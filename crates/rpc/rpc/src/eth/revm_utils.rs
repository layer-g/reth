//! utilities for working with revm

use crate::eth::error::{EthApiError, EthResult, InvalidTransactionError};
use reth_primitives::{AccessList, Address, U256};
use reth_rpc_types::{
    state::{AccountOverride, StateOverride},
    CallRequest,
};
use revm::{
    db::CacheDB,
    precompile::{Precompiles, SpecId as PrecompilesSpecId},
    primitives::{BlockEnv, CfgEnv, Env, ResultAndState, SpecId, TransactTo, TxEnv},
    Database, Inspector,
};
use revm_primitives::{db::DatabaseRef, Bytecode};
use tracing::trace;

/// Returns the addresses of the precompiles corresponding to the SpecId.
pub(crate) fn get_precompiles(spec_id: &SpecId) -> Vec<reth_primitives::H160> {
    let spec = match spec_id {
        SpecId::FRONTIER | SpecId::FRONTIER_THAWING => return vec![],
        SpecId::HOMESTEAD | SpecId::DAO_FORK | SpecId::TANGERINE | SpecId::SPURIOUS_DRAGON => {
            PrecompilesSpecId::HOMESTEAD
        }
        SpecId::BYZANTIUM | SpecId::CONSTANTINOPLE | SpecId::PETERSBURG => {
            PrecompilesSpecId::BYZANTIUM
        }
        SpecId::ISTANBUL | SpecId::MUIR_GLACIER => PrecompilesSpecId::ISTANBUL,
        SpecId::BERLIN |
        SpecId::LONDON |
        SpecId::ARROW_GLACIER |
        SpecId::GRAY_GLACIER |
        SpecId::MERGE |
        SpecId::SHANGHAI |
        SpecId::CANCUN => PrecompilesSpecId::BERLIN,
        SpecId::LATEST => PrecompilesSpecId::LATEST,
    };
    Precompiles::new(spec).addresses().into_iter().map(Address::from).collect()
}

/// Executes the [Env] against the given [Database] without committing state changes.
pub(crate) fn transact<S>(db: S, env: Env) -> EthResult<(ResultAndState, Env)>
where
    S: Database,
    <S as Database>::Error: Into<EthApiError>,
{
    let mut evm = revm::EVM::with_env(env);
    evm.database(db);
    let res = evm.transact()?;
    Ok((res, evm.env))
}

/// Executes the [Env] against the given [Database] without committing state changes.
pub(crate) fn inspect<S, I>(db: S, env: Env, inspector: I) -> EthResult<(ResultAndState, Env)>
where
    S: Database,
    <S as Database>::Error: Into<EthApiError>,
    I: Inspector<S>,
{
    let mut evm = revm::EVM::with_env(env);
    evm.database(db);
    let res = evm.inspect(inspector)?;
    Ok((res, evm.env))
}

/// Prepares the [Env] for execution.
///
/// Does not commit any changes to the underlying database.
pub(crate) fn prepare_call_env<DB>(
    mut cfg: CfgEnv,
    block: BlockEnv,
    request: CallRequest,
    db: &mut CacheDB<DB>,
    state_overrides: Option<StateOverride>,
) -> EthResult<Env>
where
    DB: DatabaseRef,
    EthApiError: From<<DB as DatabaseRef>::Error>,
{
    // we want to disable this in eth_call, since this is common practice used by other node
    // impls and providers <https://github.com/foundry-rs/foundry/issues/4388>
    cfg.disable_block_gas_limit = true;

    // Disabled because eth_call is sometimes used with eoa senders
    // See <https://github.com/paradigmxyz/reth/issues/1959>
    cfg.disable_eip3607 = true;

    // The basefee should be ignored for eth_call
    // See:
    // <https://github.com/ethereum/go-ethereum/blob/ee8e83fa5f6cb261dad2ed0a7bbcde4930c41e6c/internal/ethapi/api.go#L985>
    cfg.disable_base_fee = true;

    let request_gas = request.gas;

    let mut env = build_call_evm_env(cfg, block, request)?;

    // apply state overrides
    if let Some(state_overrides) = state_overrides {
        apply_state_overrides(state_overrides, db)?;
    }

    if request_gas.is_none() && env.tx.gas_price > U256::ZERO {
        trace!(target: "rpc::eth::call", ?env, "Applying gas limit cap");
        // no gas limit was provided in the request, so we need to cap the request's gas limit
        cap_tx_gas_limit_with_caller_allowance(db, &mut env.tx)?;
    }

    Ok(env)
}

/// Creates a new [Env] to be used for executing the [CallRequest] in `eth_call`.
///
/// Note: this does _not_ access the Database to check the sender.
pub(crate) fn build_call_evm_env(
    cfg: CfgEnv,
    block: BlockEnv,
    request: CallRequest,
) -> EthResult<Env> {
    let tx = create_txn_env(&block, request)?;
    Ok(Env { cfg, block, tx })
}

/// Configures a new [TxEnv]  for the [CallRequest]
///
/// All [TxEnv] fields are derived from the given [CallRequest], if fields are `None`, they fall
/// back to the [BlockEnv]'s settings.
pub(crate) fn create_txn_env(block_env: &BlockEnv, request: CallRequest) -> EthResult<TxEnv> {
    let CallRequest {
        from,
        to,
        gas_price,
        max_fee_per_gas,
        max_priority_fee_per_gas,
        gas,
        value,
        data,
        nonce,
        access_list,
        chain_id,
    } = request;

    let CallFees { max_priority_fee_per_gas, gas_price } = CallFees::ensure_fees(
        gas_price,
        max_fee_per_gas,
        max_priority_fee_per_gas,
        block_env.basefee,
    )?;

    let gas_limit = gas.unwrap_or(block_env.gas_limit.min(U256::from(u64::MAX)));

    let env = TxEnv {
        gas_limit: gas_limit.try_into().map_err(|_| InvalidTransactionError::GasUintOverflow)?,
        nonce: nonce
            .map(|n| n.try_into().map_err(|_| InvalidTransactionError::NonceTooHigh))
            .transpose()?,
        caller: from.unwrap_or_default(),
        gas_price,
        gas_priority_fee: max_priority_fee_per_gas,
        transact_to: to.map(TransactTo::Call).unwrap_or_else(TransactTo::create),
        value: value.unwrap_or_default(),
        data: data.map(|data| data.0).unwrap_or_default(),
        chain_id: chain_id.map(|c| c.as_u64()),
        access_list: access_list.map(AccessList::flattened).unwrap_or_default(),
        category: Default::default(),
    };

    Ok(env)
}

/// Caps the configured [TxEnv] `gas_limit` with the allowance of the caller.
///
/// Returns an error if the caller has insufficient funds
pub(crate) fn cap_tx_gas_limit_with_caller_allowance<DB>(
    mut db: DB,
    env: &mut TxEnv,
) -> EthResult<()>
where
    DB: Database,
    EthApiError: From<<DB as Database>::Error>,
{
    let mut allowance = db.basic(env.caller)?.map(|acc| acc.balance).unwrap_or_default();

    // subtract transferred value
    allowance = allowance
        .checked_sub(env.value)
        .ok_or_else(|| InvalidTransactionError::InsufficientFunds)?;

    // cap the gas limit
    if let Ok(gas_limit) = allowance.checked_div(env.gas_price).unwrap_or_default().try_into() {
        env.gas_limit = gas_limit;
    }

    Ok(())
}

/// Helper type for representing the fees of a [CallRequest]
pub(crate) struct CallFees {
    /// EIP-1559 priority fee
    max_priority_fee_per_gas: Option<U256>,
    /// Unified gas price setting
    ///
    /// Will be the configured `basefee` if unset in the request
    ///
    /// `gasPrice` for legacy,
    /// `maxFeePerGas` for EIP-1559
    gas_price: U256,
}

// === impl CallFees ===

impl CallFees {
    /// Ensures the fields of a [CallRequest] are not conflicting.
    ///
    /// If no `gasPrice` or `maxFeePerGas` is set, then the `gas_price` in the response will
    /// fallback to the given `basefee`.
    fn ensure_fees(
        call_gas_price: Option<U256>,
        call_max_fee: Option<U256>,
        call_priority_fee: Option<U256>,
        base_fee: U256,
    ) -> EthResult<CallFees> {
        match (call_gas_price, call_max_fee, call_priority_fee) {
            (None, None, None) => {
                // when none are specified, they are all set to zero
                Ok(CallFees { gas_price: U256::ZERO, max_priority_fee_per_gas: None })
            }
            (gas_price, None, None) => {
                // request for a legacy transaction
                // set everything to zero
                let gas_price = gas_price.unwrap_or(base_fee);
                Ok(CallFees { gas_price, max_priority_fee_per_gas: None })
            }
            (None, max_fee_per_gas, max_priority_fee_per_gas) => {
                // request for eip-1559 transaction
                let max_fee = max_fee_per_gas.unwrap_or(base_fee);

                if let Some(max_priority) = max_priority_fee_per_gas {
                    if max_priority > max_fee {
                        // Fail early
                        return Err(
                            // `max_priority_fee_per_gas` is greater than the `max_fee_per_gas`
                            InvalidTransactionError::TipAboveFeeCap.into(),
                        )
                    }
                }
                Ok(CallFees { gas_price: max_fee, max_priority_fee_per_gas })
            }
            _ => Err(EthApiError::ConflictingFeeFieldsInRequest),
        }
    }
}

/// Applies the given state overrides (a set of [AccountOverride]) to the [CacheDB].
fn apply_state_overrides<DB>(overrides: StateOverride, db: &mut CacheDB<DB>) -> EthResult<()>
where
    DB: DatabaseRef,
    EthApiError: From<<DB as DatabaseRef>::Error>,
{
    for (account, account_overrides) in overrides {
        apply_account_override(account, account_overrides, db)?;
    }
    Ok(())
}

/// Applies a single [AccountOverride] to the [CacheDB].
fn apply_account_override<DB>(
    account: Address,
    account_override: AccountOverride,
    db: &mut CacheDB<DB>,
) -> EthResult<()>
where
    DB: DatabaseRef,
    EthApiError: From<<DB as DatabaseRef>::Error>,
{
    let mut account_info = db.basic(account)?.unwrap_or_default();

    if let Some(nonce) = account_override.nonce {
        account_info.nonce = nonce.as_u64();
    }
    if let Some(code) = account_override.code {
        account_info.code = Some(Bytecode::new_raw(code.0));
    }
    if let Some(balance) = account_override.balance {
        account_info.balance = balance;
    }

    db.insert_account_info(account, account_info);

    // We ensure that not both state and state_diff are set.
    // If state is set, we must mark the account as "NewlyCreated", so that the old storage
    // isn't read from
    match (account_override.state, account_override.state_diff) {
        (Some(_), Some(_)) => return Err(EthApiError::BothStateAndStateDiffInOverride(account)),
        (None, None) => {
            // nothing to do
        }
        (Some(new_account_state), None) => {
            db.replace_account_storage(
                account,
                new_account_state
                    .into_iter()
                    .map(|(slot, value)| {
                        (U256::from_be_bytes(slot.0), U256::from_be_bytes(value.0))
                    })
                    .collect(),
            )?;
        }
        (None, Some(account_state_diff)) => {
            for (slot, value) in account_state_diff {
                db.insert_account_storage(
                    account,
                    U256::from_be_bytes(slot.0),
                    U256::from_be_bytes(value.0),
                )?;
            }
        }
    };

    Ok(())
}
