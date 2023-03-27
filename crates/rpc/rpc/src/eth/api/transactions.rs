//! Contains RPC handler implementations specific to transactions
use crate::{
    eth::{
        error::{EthApiError, EthResult, SignError},
        utils::recover_raw_transaction,
    },
    EthApi,
};
use async_trait::async_trait;
use reth_primitives::{
    Address, BlockId, BlockNumberOrTag, Bytes, FromRecoveredTransaction, IntoRecoveredTransaction,
    Receipt, Transaction as PrimitiveTransaction,
    TransactionKind::{Call, Create},
    TransactionMeta, TransactionSigned, TransactionSignedEcRecovered, TxEip1559, TxEip2930,
    TxLegacy, H256, U128, U256, U64,
};
use reth_provider::{providers::ChainState, BlockProvider, EvmEnvProvider, StateProviderFactory};
use reth_rpc_types::{
    Index, Log, Transaction, TransactionInfo, TransactionReceipt, TransactionRequest,
    TypedTransactionRequest,
};
use reth_transaction_pool::{TransactionOrigin, TransactionPool};
use revm::primitives::{BlockEnv, CfgEnv};
use revm_primitives::utilities::create_address;

/// Commonly used transaction related functions for the [EthApi] type in the `eth_` namespace
#[async_trait::async_trait]
pub trait EthTransactions: Send + Sync {
    /// Returns the state at the given [BlockId]
    fn state_at(&self, at: BlockId) -> EthResult<ChainState<'_>>;

    /// Executes the closure with the state that corresponds to the given [BlockId].
    fn with_state_at<F, T>(&self, _at: BlockId, _f: F) -> EthResult<T>
    where
        F: FnOnce(ChainState<'_>) -> EthResult<T>;

    /// Returns the revm evm env for the requested [BlockId]
    ///
    /// If the [BlockId] this will return the [BlockId::Hash] of the block the env was configured
    /// for.
    async fn evm_env_at(&self, at: BlockId) -> EthResult<(CfgEnv, BlockEnv, BlockId)>;

    /// Returns the transaction by hash.
    ///
    /// Checks the pool and state.
    ///
    /// Returns `Ok(None)` if no matching transaction was found.
    async fn transaction_by_hash(&self, hash: H256) -> EthResult<Option<TransactionSource>>;

    /// Returns the transaction by including its corresponding [BlockId]
    async fn transaction_by_hash_at(
        &self,
        hash: H256,
    ) -> EthResult<Option<(TransactionSource, BlockId)>>;

    /// Returns the transaction receipt for the given hash.
    ///
    /// Returns None if the transaction does not exist or is pending
    /// Note: The tx receipt is not available for pending transactions.
    async fn transaction_receipt(&self, hash: H256) -> EthResult<Option<TransactionReceipt>>;

    /// Decodes and recovers the transaction and submits it to the pool.
    ///
    /// Returns the hash of the transaction.
    async fn send_raw_transaction(&self, tx: Bytes) -> EthResult<H256>;

    /// Signs transaction with a matching signer, if any and submits the transaction to the pool.
    /// Returns the hash of the signed transaction.
    async fn send_transaction(&self, request: TransactionRequest) -> EthResult<H256>;
}

#[async_trait]
impl<Client, Pool, Network> EthTransactions for EthApi<Client, Pool, Network>
where
    Pool: TransactionPool + Clone + 'static,
    Client: BlockProvider + StateProviderFactory + EvmEnvProvider + 'static,
    Network: Send + Sync + 'static,
{
    fn state_at(&self, at: BlockId) -> EthResult<ChainState<'_>> {
        self.state_at_block_id(at)
    }

    fn with_state_at<F, T>(&self, at: BlockId, f: F) -> EthResult<T>
    where
        F: FnOnce(ChainState<'_>) -> EthResult<T>,
    {
        let state = self.state_at(at)?;
        f(state)
    }

    async fn evm_env_at(&self, at: BlockId) -> EthResult<(CfgEnv, BlockEnv, BlockId)> {
        // TODO handle Pending state's env
        match at {
            BlockId::Number(BlockNumberOrTag::Pending) => {
                // This should perhaps use the latest env settings and update block specific
                // settings like basefee/number
                Err(EthApiError::Unsupported("pending state not implemented yet"))
            }
            hash_or_num => {
                let block_hash = self
                    .client()
                    .block_hash_for_id(hash_or_num)?
                    .ok_or_else(|| EthApiError::UnknownBlockNumber)?;
                let (cfg, env) = self.cache().get_evm_env(block_hash).await?;
                Ok((cfg, env, block_hash.into()))
            }
        }
    }

    async fn transaction_by_hash(&self, hash: H256) -> EthResult<Option<TransactionSource>> {
        if let Some(tx) = self.pool().get(&hash).map(|tx| tx.transaction.to_recovered_transaction())
        {
            return Ok(Some(TransactionSource::Pool(tx)))
        }

        match self.client().transaction_by_hash_with_meta(hash)? {
            None => Ok(None),
            Some((tx, meta)) => {
                let transaction =
                    tx.into_ecrecovered().ok_or(EthApiError::InvalidTransactionSignature)?;

                let tx = TransactionSource::Database {
                    transaction,
                    index: meta.index,
                    block_hash: meta.block_hash,
                    block_number: meta.block_number,
                };
                Ok(Some(tx))
            }
        }
    }

    async fn transaction_by_hash_at(
        &self,
        hash: H256,
    ) -> EthResult<Option<(TransactionSource, BlockId)>> {
        match self.transaction_by_hash(hash).await? {
            None => return Ok(None),
            Some(tx) => {
                let res = match tx {
                    tx @ TransactionSource::Pool(_) => {
                        (tx, BlockId::Number(BlockNumberOrTag::Pending))
                    }
                    TransactionSource::Database {
                        transaction,
                        index,
                        block_hash,
                        block_number,
                    } => {
                        let at = BlockId::Hash(block_hash.into());
                        let tx = TransactionSource::Database {
                            transaction,
                            index,
                            block_hash,
                            block_number,
                        };
                        (tx, at)
                    }
                };
                Ok(Some(res))
            }
        }
    }

    async fn transaction_receipt(&self, hash: H256) -> EthResult<Option<TransactionReceipt>> {
        let (tx, meta) = match self.client().transaction_by_hash_with_meta(hash)? {
            Some((tx, meta)) => (tx, meta),
            None => return Ok(None),
        };

        let receipt = match self.client().receipt_by_hash(hash)? {
            Some(recpt) => recpt,
            None => return Ok(None),
        };

        self.build_transaction_receipt(tx, meta, receipt).await.map(Some)
    }

    async fn send_raw_transaction(&self, tx: Bytes) -> EthResult<H256> {
        let recovered = recover_raw_transaction(tx)?;

        let pool_transaction = <Pool::Transaction>::from_recovered_transaction(recovered);

        // submit the transaction to the pool with a `Local` origin
        let hash = self.pool().add_transaction(TransactionOrigin::Local, pool_transaction).await?;

        Ok(hash)
    }

    async fn send_transaction(&self, request: TransactionRequest) -> EthResult<H256> {
        let from = match request.from {
            Some(from) => from,
            None => return Err(SignError::NoAccount.into()),
        };
        let transaction = match request.into_typed_request() {
            Some(tx) => tx,
            None => return Err(EthApiError::ConflictingFeeFieldsInRequest),
        };

        // TODO we need to update additional settings in the transaction: nonce, gaslimit, chainid,
        // gasprice

        let signed_tx = self.sign_request(&from, transaction)?;

        let recovered =
            signed_tx.into_ecrecovered().ok_or(EthApiError::InvalidTransactionSignature)?;

        let pool_transaction = <Pool::Transaction>::from_recovered_transaction(recovered);

        // submit the transaction to the pool with a `Local` origin
        let hash = self.pool().add_transaction(TransactionOrigin::Local, pool_transaction).await?;

        Ok(hash)
    }
}

// === impl EthApi ===

impl<Client, Pool, Network> EthApi<Client, Pool, Network>
where
    Pool: TransactionPool + 'static,
    Client: BlockProvider + StateProviderFactory + EvmEnvProvider + 'static,
    Network: 'static,
{
    pub(crate) fn sign_request(
        &self,
        from: &Address,
        request: TypedTransactionRequest,
    ) -> EthResult<TransactionSigned> {
        for signer in self.inner.signers.iter() {
            if signer.is_signer_for(from) {
                return match signer.sign_transaction(request, from) {
                    Ok(tx) => Ok(tx),
                    Err(e) => Err(e.into()),
                }
            }
        }
        Err(EthApiError::InvalidTransactionSignature)
    }

    /// Get Transaction by [BlockId] and the index of the transaction within that Block.
    ///
    /// Returns `Ok(None)` if the block does not exist, or the block as fewer transactions
    pub(crate) async fn transaction_by_block_and_tx_index(
        &self,
        block_id: impl Into<BlockId>,
        index: Index,
    ) -> EthResult<Option<Transaction>> {
        let block_id = block_id.into();
        if let Some(block) = self.client().block(block_id)? {
            let block_hash = self
                .client()
                .block_hash_for_id(block_id)?
                .ok_or(EthApiError::UnknownBlockNumber)?;
            if let Some(tx_signed) = block.body.into_iter().nth(index.into()) {
                let tx =
                    tx_signed.into_ecrecovered().ok_or(EthApiError::InvalidTransactionSignature)?;
                return Ok(Some(Transaction::from_recovered_with_block_context(
                    tx,
                    block_hash,
                    block.header.number,
                    index.into(),
                )))
            }
        }

        Ok(None)
    }

    /// Helper function for `eth_getTransactionReceipt`
    ///
    /// Returns the receipt
    pub(crate) async fn build_transaction_receipt(
        &self,
        tx: TransactionSigned,
        meta: TransactionMeta,
        receipt: Receipt,
    ) -> EthResult<TransactionReceipt> {
        let transaction =
            tx.clone().into_ecrecovered().ok_or(EthApiError::InvalidTransactionSignature)?;

        // get all receipts for the block
        let all_receipts = match self.client().receipts_by_block((meta.block_number).into())? {
            Some(recpts) => recpts,
            None => return Err(EthApiError::UnknownBlockNumber),
        };

        let mut res_receipt = TransactionReceipt {
            transaction_hash: Some(meta.tx_hash),
            transaction_index: Some(U256::from(meta.index)),
            block_hash: Some(meta.block_hash),
            block_number: Some(U256::from(meta.block_number)),
            from: transaction.signer(),
            to: None,
            cumulative_gas_used: U256::from(receipt.cumulative_gas_used),
            gas_used: None,
            contract_address: None,
            logs: vec![],
            effective_gas_price: U128::from(0),
            transaction_type: U256::from(0),
            // TODO: set state root after the block
            state_root: None,
            logs_bloom: receipt.bloom_slow(),
            status_code: if receipt.success { Some(U64::from(1)) } else { Some(U64::from(0)) },
        };

        // get the previous transaction cumulative gas used
        let gas_used = if meta.index == 0 {
            receipt.cumulative_gas_used
        } else {
            let prev_tx_idx = (meta.index - 1) as usize;
            all_receipts
                .get(prev_tx_idx)
                .map(|prev_receipt| receipt.cumulative_gas_used - prev_receipt.cumulative_gas_used)
                .unwrap_or_default()
        };
        res_receipt.gas_used = Some(U256::from(gas_used));

        match tx.transaction.kind() {
            Create => {
                // set contract address if creation was successful
                if receipt.success {
                    res_receipt.contract_address =
                        Some(create_address(transaction.signer(), tx.transaction.nonce()));
                }
            }
            Call(addr) => {
                res_receipt.to = Some(*addr);
            }
        }

        match tx.transaction {
            PrimitiveTransaction::Legacy(TxLegacy { gas_price, .. }) => {
                res_receipt.transaction_type = U256::from(0);
                res_receipt.effective_gas_price = U128::from(gas_price);
            }
            PrimitiveTransaction::Eip2930(TxEip2930 { gas_price, .. }) => {
                res_receipt.transaction_type = U256::from(1);
                res_receipt.effective_gas_price = U128::from(gas_price);
            }
            PrimitiveTransaction::Eip1559(TxEip1559 {
                max_fee_per_gas,
                max_priority_fee_per_gas,
                ..
            }) => {
                res_receipt.transaction_type = U256::from(2);
                res_receipt.effective_gas_price =
                    U128::from(max_fee_per_gas + max_priority_fee_per_gas)
            }
        }

        // get number of logs in the block
        let mut num_logs = 0;
        for prev_receipt in all_receipts.iter().take(meta.index as usize) {
            num_logs += prev_receipt.logs.len();
        }

        for (tx_log_idx, log) in receipt.logs.into_iter().enumerate() {
            let rpclog = Log {
                address: log.address,
                topics: log.topics,
                data: log.data,
                block_hash: Some(meta.block_hash),
                block_number: Some(U256::from(meta.block_number)),
                transaction_hash: Some(meta.tx_hash),
                transaction_index: Some(U256::from(meta.index)),
                transaction_log_index: Some(U256::from(tx_log_idx)),
                log_index: Some(U256::from(num_logs + tx_log_idx)),
                removed: false,
            };
            res_receipt.logs.push(rpclog);
        }

        Ok(res_receipt)
    }
}
/// Represents from where a transaction was fetched.
#[derive(Debug, Clone, Eq, PartialEq)]
pub enum TransactionSource {
    /// Transaction exists in the pool (Pending)
    Pool(TransactionSignedEcRecovered),
    /// Transaction already executed
    Database {
        /// Transaction fetched via provider
        transaction: TransactionSignedEcRecovered,
        /// Index of the transaction in the block
        index: u64,
        /// Hash of the block.
        block_hash: H256,
        /// Number of the block.
        block_number: u64,
    },
}

// === impl TransactionSource ===

impl TransactionSource {
    /// Consumes the type and returns the wrapped transaction.
    pub fn into_recovered(self) -> TransactionSignedEcRecovered {
        self.into()
    }

    /// Returns the transaction and block related info, if not pending
    pub fn split(self) -> (TransactionSignedEcRecovered, TransactionInfo) {
        match self {
            TransactionSource::Pool(tx) => {
                let hash = tx.hash();
                (
                    tx,
                    TransactionInfo {
                        hash: Some(hash),
                        index: None,
                        block_hash: None,
                        block_number: None,
                    },
                )
            }
            TransactionSource::Database { transaction, index, block_hash, block_number } => {
                let hash = transaction.hash();
                (
                    transaction,
                    TransactionInfo {
                        hash: Some(hash),
                        index: Some(index),
                        block_hash: Some(block_hash),
                        block_number: Some(block_number),
                    },
                )
            }
        }
    }
}

impl From<TransactionSource> for TransactionSignedEcRecovered {
    fn from(value: TransactionSource) -> Self {
        match value {
            TransactionSource::Pool(tx) => tx,
            TransactionSource::Database { transaction, .. } => transaction,
        }
    }
}

impl From<TransactionSource> for Transaction {
    fn from(value: TransactionSource) -> Self {
        match value {
            TransactionSource::Pool(tx) => Transaction::from_recovered(tx),
            TransactionSource::Database { transaction, index, block_hash, block_number } => {
                Transaction::from_recovered_with_block_context(
                    transaction,
                    block_hash,
                    block_number,
                    U256::from(index),
                )
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{eth::cache::EthStateCache, EthApi};
    use reth_primitives::{hex_literal::hex, Bytes};
    use reth_provider::test_utils::NoopProvider;
    use reth_transaction_pool::{test_utils::testing_pool, TransactionPool};

    #[tokio::test]
    async fn send_raw_transaction() {
        let noop_provider = NoopProvider::default();

        let pool = testing_pool();

        let eth_api = EthApi::new(
            noop_provider,
            pool.clone(),
            (),
            EthStateCache::spawn(NoopProvider::default(), Default::default()),
        );

        // https://etherscan.io/tx/0xa694b71e6c128a2ed8e2e0f6770bddbe52e3bb8f10e8472f9a79ab81497a8b5d
        let tx_1 = Bytes::from(hex!("02f871018303579880850555633d1b82520894eee27662c2b8eba3cd936a23f039f3189633e4c887ad591c62bdaeb180c080a07ea72c68abfb8fca1bd964f0f99132ed9280261bdca3e549546c0205e800f7d0a05b4ef3039e9c9b9babc179a1878fb825b5aaf5aed2fa8744854150157b08d6f3"));

        let tx_1_result = eth_api.send_raw_transaction(tx_1).await.unwrap();
        assert_eq!(
            pool.len(),
            1,
            "expect 1 transactions in the pool, but pool size is {}",
            pool.len()
        );

        // https://etherscan.io/tx/0x48816c2f32c29d152b0d86ff706f39869e6c1f01dc2fe59a3c1f9ecf39384694
        let tx_2 = Bytes::from(hex!("02f9043c018202b7843b9aca00850c807d37a08304d21d94ef1c6e67703c7bd7107eed8303fbe6ec2554bf6b881bc16d674ec80000b903c43593564c000000000000000000000000000000000000000000000000000000000000006000000000000000000000000000000000000000000000000000000000000000a00000000000000000000000000000000000000000000000000000000063e2d99f00000000000000000000000000000000000000000000000000000000000000030b000800000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000003000000000000000000000000000000000000000000000000000000000000006000000000000000000000000000000000000000000000000000000000000000c000000000000000000000000000000000000000000000000000000000000001e0000000000000000000000000000000000000000000000000000000000000004000000000000000000000000000000000000000000000000000000000000000020000000000000000000000000000000000000000000000001bc16d674ec80000000000000000000000000000000000000000000000000000000000000000010000000000000000000000000065717fe021ea67801d1088cc80099004b05b64600000000000000000000000000000000000000000000000001bc16d674ec80000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000a00000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000002bc02aaa39b223fe8d0a0e5c4f27ead9083c756cc20001f4a0b86991c6218b36c1d19d4a2e9eb0ce3606eb480000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000100000000000000000000000000000000000000000000000000000000000000000180000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000009e95fd5965fd1f1a6f0d4600000000000000000000000000000000000000000000000000000000000000a000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000002000000000000000000000000a0b86991c6218b36c1d19d4a2e9eb0ce3606eb48000000000000000000000000428dca9537116148616a5a3e44035af17238fe9dc080a0c6ec1e41f5c0b9511c49b171ad4e04c6bb419c74d99fe9891d74126ec6e4e879a032069a753d7a2cfa158df95421724d24c0e9501593c09905abf3699b4a4405ce"));

        let tx_2_result = eth_api.send_raw_transaction(tx_2).await.unwrap();
        assert_eq!(
            pool.len(),
            2,
            "expect 2 transactions in the pool, but pool size is {}",
            pool.len()
        );

        assert!(pool.get(&tx_1_result).is_some(), "tx1 not found in the pool");
        assert!(pool.get(&tx_2_result).is_some(), "tx2 not found in the pool");
    }
}
