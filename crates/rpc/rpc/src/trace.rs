use crate::{
    eth::{
        cache::EthStateCache,
        error::{EthApiError, EthResult},
        revm_utils::{inspect, prepare_call_env},
        utils::recover_raw_transaction,
        EthTransactions,
    },
    result::internal_rpc_err,
    TracingCallGuard,
};
use async_trait::async_trait;
use jsonrpsee::core::RpcResult as Result;
use reth_primitives::{BlockId, BlockNumberOrTag, Bytes, H256};
use reth_provider::{BlockProvider, EvmEnvProvider, StateProviderFactory};
use reth_revm::{
    database::{State, SubState},
    env::tx_env_with_recovered,
    tracing::{TracingInspector, TracingInspectorConfig},
};
use reth_rpc_api::TraceApiServer;
use reth_rpc_types::{
    trace::{filter::TraceFilter, parity::*},
    BlockError, CallRequest, Index, TransactionInfo,
};
use revm::primitives::Env;
use revm_primitives::ResultAndState;
use std::collections::HashSet;
use tokio::sync::{AcquireError, OwnedSemaphorePermit};

/// `trace` API implementation.
///
/// This type provides the functionality for handling `trace` related requests.
#[derive(Clone)]
pub struct TraceApi<Client, Eth> {
    /// The client that can interact with the chain.
    client: Client,
    /// Access to commonly used code of the `eth` namespace
    eth_api: Eth,
    /// The async cache frontend for eth related data
    eth_cache: EthStateCache,
    // restrict the number of concurrent calls to `trace_*`
    tracing_call_guard: TracingCallGuard,
}

// === impl TraceApi ===

impl<Client, Eth> TraceApi<Client, Eth> {
    /// Create a new instance of the [TraceApi]
    pub fn new(
        client: Client,
        eth_api: Eth,
        eth_cache: EthStateCache,
        tracing_call_guard: TracingCallGuard,
    ) -> Self {
        Self { client, eth_api, eth_cache, tracing_call_guard }
    }

    /// Acquires a permit to execute a tracing call.
    async fn acquire_trace_permit(
        &self,
    ) -> std::result::Result<OwnedSemaphorePermit, AcquireError> {
        self.tracing_call_guard.clone().acquire_owned().await
    }
}

// === impl TraceApi ===

impl<Client, Eth> TraceApi<Client, Eth>
where
    Client: BlockProvider + StateProviderFactory + EvmEnvProvider + 'static,
    Eth: EthTransactions + 'static,
{
    /// Executes the given call and returns a number of possible traces for it.
    pub async fn trace_call(
        &self,
        call: CallRequest,
        trace_types: HashSet<TraceType>,
        block_id: Option<BlockId>,
    ) -> EthResult<TraceResults> {
        let _permit = self.acquire_trace_permit().await;
        let at = block_id.unwrap_or(BlockId::Number(BlockNumberOrTag::Latest));
        let config = tracing_config(&trace_types);
        let mut inspector = TracingInspector::new(config);

        let (res, _) = self.eth_api.inspect_call_at(call, at, None, &mut inspector).await?;

        let trace_res =
            inspector.into_parity_builder().into_trace_results(res.result, &trace_types);
        Ok(trace_res)
    }

    /// Traces a call to `eth_sendRawTransaction` without making the call, returning the traces.
    pub async fn trace_raw_transaction(
        &self,
        tx: Bytes,
        trace_types: HashSet<TraceType>,
        block_id: Option<BlockId>,
    ) -> EthResult<TraceResults> {
        let _permit = self.acquire_trace_permit().await;
        let tx = recover_raw_transaction(tx)?;

        let (cfg, block, at) = self
            .eth_api
            .evm_env_at(block_id.unwrap_or(BlockId::Number(BlockNumberOrTag::Latest)))
            .await?;
        let tx = tx_env_with_recovered(&tx);
        let env = Env { cfg, block, tx };

        let config = tracing_config(&trace_types);

        self.eth_api.trace_at(env, config, at, |inspector, res| {
            let trace_res =
                inspector.into_parity_builder().into_trace_results(res.result, &trace_types);
            Ok(trace_res)
        })
    }

    /// Performs multiple call traces on top of the same block. i.e. transaction n will be executed
    /// on top of a pending block with all n-1 transactions applied (traced) first.
    ///
    /// Note: Allows to trace dependent transactions, hence all transactions are traced in sequence
    pub async fn trace_call_many(
        &self,
        calls: Vec<(CallRequest, HashSet<TraceType>)>,
        block_id: Option<BlockId>,
    ) -> EthResult<Vec<TraceResults>> {
        let at = block_id.unwrap_or(BlockId::Number(BlockNumberOrTag::Pending));
        let (cfg, block_env, at) = self.eth_api.evm_env_at(at).await?;

        // execute all transactions on top of each other and record the traces
        self.eth_api.with_state_at(at, move |state| {
            let mut results = Vec::with_capacity(calls.len());
            let mut db = SubState::new(State::new(state));

            for (call, trace_types) in calls {
                let env = prepare_call_env(cfg.clone(), block_env.clone(), call, &mut db, None)?;
                let config = tracing_config(&trace_types);
                let mut inspector = TracingInspector::new(config);
                let (res, _) = inspect(&mut db, env, &mut inspector)?;
                let trace_res =
                    inspector.into_parity_builder().into_trace_results(res.result, &trace_types);
                results.push(trace_res);
            }

            Ok(results)
        })
    }

    /// Replays a transaction, returning the traces.
    pub async fn replay_transaction(
        &self,
        hash: H256,
        trace_types: HashSet<TraceType>,
    ) -> EthResult<TraceResults> {
        let config = tracing_config(&trace_types);
        self.eth_api
            .trace_transaction(hash, config, |_, inspector, res| {
                let trace_res =
                    inspector.into_parity_builder().into_trace_results(res.result, &trace_types);
                Ok(trace_res)
            })
            .await
            .transpose()
            .ok_or_else(|| EthApiError::TransactionNotFound)?
    }

    /// Returns transaction trace with the given address.
    pub async fn trace_get(
        &self,
        hash: H256,
        trace_address: Vec<usize>,
    ) -> EthResult<Option<LocalizedTransactionTrace>> {
        let _permit = self.acquire_trace_permit().await;

        match self.trace_transaction(hash).await? {
            None => Ok(None),
            Some(traces) => {
                let trace =
                    traces.into_iter().find(|trace| trace.trace.trace_address == trace_address);
                Ok(trace)
            }
        }
    }

    /// Returns all traces for the given transaction hash
    pub async fn trace_transaction(
        &self,
        hash: H256,
    ) -> EthResult<Option<Vec<LocalizedTransactionTrace>>> {
        let _permit = self.acquire_trace_permit().await;

        self.eth_api
            .trace_transaction(
                hash,
                TracingInspectorConfig::default_parity(),
                |tx_info, inspector, _| {
                    let traces =
                        inspector.into_parity_builder().into_localized_transaction_traces(tx_info);
                    Ok(traces)
                },
            )
            .await
    }

    /// Executes all transactions of a block and returns a list of callback results.
    async fn trace_block_with<F, R>(
        &self,
        block_id: BlockId,
        config: TracingInspectorConfig,
        f: F,
    ) -> EthResult<Option<Vec<R>>>
    where
        F: Fn(TransactionInfo, TracingInspector, ResultAndState) -> EthResult<R> + Send,
    {
        let block_hash = match self.client.block_hash_for_id(block_id)? {
            Some(hash) => hash,
            None => return Ok(None),
        };

        let ((cfg, block_env, at), transactions) = futures::try_join!(
            self.eth_api.evm_env_at(block_hash.into()),
            self.eth_api.transactions_by_block(block_hash),
        )?;
        let transactions = transactions.ok_or_else(|| EthApiError::UnknownBlockNumber)?;

        // replay all transactions of the block
        self.eth_api
            .with_state_at(at, move |state| {
                let mut results = Vec::with_capacity(transactions.len());
                let mut db = SubState::new(State::new(state));

                for (idx, tx) in transactions.into_iter().enumerate() {
                    let tx = tx.into_ecrecovered().ok_or(BlockError::InvalidSignature)?;
                    let tx_info = TransactionInfo {
                        hash: Some(tx.hash),
                        index: Some(idx as u64),
                        block_hash: Some(block_hash),
                        block_number: Some(block_env.number.try_into().unwrap_or(u64::MAX)),
                    };

                    let tx = tx_env_with_recovered(&tx);
                    let env = Env { cfg: cfg.clone(), block: block_env.clone(), tx };

                    let mut inspector = TracingInspector::new(config);
                    let (res, _) = inspect(&mut db, env, &mut inspector)?;
                    results.push(f(tx_info, inspector, res)?);
                }

                Ok(results)
            })
            .map(Some)
    }

    /// Returns traces created at given block.
    pub async fn trace_block(
        &self,
        block_id: BlockId,
    ) -> EthResult<Option<Vec<LocalizedTransactionTrace>>> {
        let traces = self
            .trace_block_with(
                block_id,
                TracingInspectorConfig::default_parity(),
                |tx_info, inspector, _| {
                    let traces =
                        inspector.into_parity_builder().into_localized_transaction_traces(tx_info);
                    Ok(traces)
                },
            )
            .await?
            .map(|traces| traces.into_iter().flatten().collect());
        Ok(traces)
    }

    /// Replays all transaction in a block
    pub async fn replay_block_transactions(
        &self,
        block_id: BlockId,
        trace_types: HashSet<TraceType>,
    ) -> EthResult<Option<Vec<TraceResultsWithTransactionHash>>> {
        self.trace_block_with(block_id, tracing_config(&trace_types), |tx_info, inspector, res| {
            let full_trace =
                inspector.into_parity_builder().into_trace_results(res.result, &trace_types);
            let trace = TraceResultsWithTransactionHash {
                transaction_hash: tx_info.hash.expect("tx hash is set"),
                full_trace,
            };
            Ok(trace)
        })
        .await
    }
}

#[async_trait]
impl<Client, Eth> TraceApiServer for TraceApi<Client, Eth>
where
    Client: BlockProvider + StateProviderFactory + EvmEnvProvider + 'static,
    Eth: EthTransactions + 'static,
{
    /// Executes the given call and returns a number of possible traces for it.
    ///
    /// Handler for `trace_call`
    async fn trace_call(
        &self,
        call: CallRequest,
        trace_types: HashSet<TraceType>,
        block_id: Option<BlockId>,
    ) -> Result<TraceResults> {
        Ok(TraceApi::trace_call(self, call, trace_types, block_id).await?)
    }

    /// Handler for `trace_callMany`
    async fn trace_call_many(
        &self,
        calls: Vec<(CallRequest, HashSet<TraceType>)>,
        block_id: Option<BlockId>,
    ) -> Result<Vec<TraceResults>> {
        Ok(TraceApi::trace_call_many(self, calls, block_id).await?)
    }

    /// Handler for `trace_rawTransaction`
    async fn trace_raw_transaction(
        &self,
        data: Bytes,
        trace_types: HashSet<TraceType>,
        block_id: Option<BlockId>,
    ) -> Result<TraceResults> {
        Ok(TraceApi::trace_raw_transaction(self, data, trace_types, block_id).await?)
    }

    /// Handler for `trace_replayBlockTransactions`
    async fn replay_block_transactions(
        &self,
        block_id: BlockId,
        trace_types: HashSet<TraceType>,
    ) -> Result<Option<Vec<TraceResultsWithTransactionHash>>> {
        Ok(TraceApi::replay_block_transactions(self, block_id, trace_types).await?)
    }

    /// Handler for `trace_replayTransaction`
    async fn replay_transaction(
        &self,
        transaction: H256,
        trace_types: HashSet<TraceType>,
    ) -> Result<TraceResults> {
        Ok(TraceApi::replay_transaction(self, transaction, trace_types).await?)
    }

    /// Handler for `trace_block`
    async fn trace_block(
        &self,
        block_id: BlockId,
    ) -> Result<Option<Vec<LocalizedTransactionTrace>>> {
        Ok(TraceApi::trace_block(self, block_id).await?)
    }

    /// Handler for `trace_filter`
    async fn trace_filter(&self, _filter: TraceFilter) -> Result<Vec<LocalizedTransactionTrace>> {
        Err(internal_rpc_err("unimplemented"))
    }

    /// Returns transaction trace at given index.
    /// Handler for `trace_get`
    async fn trace_get(
        &self,
        hash: H256,
        indices: Vec<Index>,
    ) -> Result<Option<LocalizedTransactionTrace>> {
        Ok(TraceApi::trace_get(self, hash, indices.into_iter().map(Into::into).collect()).await?)
    }

    /// Handler for `trace_transaction`
    async fn trace_transaction(
        &self,
        hash: H256,
    ) -> Result<Option<Vec<LocalizedTransactionTrace>>> {
        Ok(TraceApi::trace_transaction(self, hash).await?)
    }
}

impl<Client, Eth> std::fmt::Debug for TraceApi<Client, Eth> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TraceApi").finish_non_exhaustive()
    }
}

/// Returns the [TracingInspectorConfig] depending on the enabled [TraceType]s
fn tracing_config(trace_types: &HashSet<TraceType>) -> TracingInspectorConfig {
    TracingInspectorConfig::default_parity()
        .set_state_diffs(trace_types.contains(&TraceType::StateDiff))
        .set_steps(trace_types.contains(&TraceType::VmTrace))
}
