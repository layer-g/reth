//! Faucet rpc endpoint

use std::{
    future::Future,
    time::{UNIX_EPOCH, SystemTime, Duration}, pin::Pin, task::{Context, Poll, ready}, sync::Arc
};

use futures::StreamExt;
use jsonrpsee::{proc_macros::rpc, core::async_trait};
use lru_time_cache::LruCache;
use reth_primitives::{Address, TxHash, TransactionSigned, FromRecoveredPooledTransaction, ChainSpec, public_key_to_address, U128, U256, Bytes, sign_message, H256, Signature};
use reth_rpc::eth::error::{EthResult, EthApiError, SignError};
use reth_rpc_types::{TypedTransactionRequest, LegacyTransactionRequest, TransactionRequest, EIP1559TransactionRequest, TransactionKind};
use reth_tasks::{TokioTaskExecutor, TaskSpawner};
use reth_transaction_pool::{TransactionPool, TransactionOrigin};
use secp256k1::KeyPair;
use tokio::sync::{mpsc::{UnboundedSender, unbounded_channel, UnboundedReceiver}, oneshot};
use tokio_stream::wrappers::UnboundedReceiverStream;
use humantime::format_duration;

/// Faucet that disperses 1 TEL every 24hours per requesting address.
#[rpc(server, namespace = "faucet")]
#[async_trait]
pub trait FaucetRpcExtApi {
    /// Transfer TEL to an address
    #[method(name = "transfer")]
    async fn transfer(&self, address: Address) -> EthResult<TxHash>;
}

/// The type that implements Faucet namespace trait.
pub struct FaucetRpcExt {
    cache: FaucetCache,
}

#[async_trait]
impl FaucetRpcExtApiServer for FaucetRpcExt {
    /// Faucet method.
    /// 
    /// The faucet checks the time-based LRU cache for the recipient's address.
    /// If the address is not found, a transaction is created to transfer TEL
    /// to the recipient. Otherwise, a time is returned indicating when the
    /// recipient's request will become valid.
    /// 
    /// Addresses are removed from the cache every 24 hours.
    async fn transfer(&self, address: Address) -> EthResult<TxHash> {
        self.cache.handle_request(address).await
    }
}

impl FaucetRpcExt {
    /// Create new instance
    fn new<Pool>(pool: Pool, config: FaucetConfig) -> Self
    where
        Pool: TransactionPool + Unpin + Clone + 'static,
    {
        // let wait_period = ::std::time::Duration::from_secs(60 * 60 * 24);
        // let transfer_amount = 5;
        // let config = FaucetConfig { wait_period, transfer_amount };
        let cache = FaucetCache::spawn(pool, config);

        Self { cache }
    }

    /// Calculate the amount of time remaining until the recipient can successfully request
    /// additional TEL.
    fn calculate_time_remaining(&self) -> EthResult<SystemTime> {
        todo!()
    }
}

/// Configure the faucet with a wait period between transfers and the amount of TEL to transfer.
pub struct FaucetConfig {
    /// The amount of time recipients must wait between transfers
    /// specified in seconds.
    pub wait_period: Duration,

    /// The amount of TEL to transfer to each recipient.
    pub transfer_amount: usize,

    /// The chain id
    pub chain_id: U64,

    /// 
}

/// Provides async access to the cached addresses.
/// 
/// This is the frontend for the async caching service which manages cached data
/// on a different task.
pub struct FaucetCache {
    /// Channel to service task.
    to_service: UnboundedSender<(Address, oneshot::Sender<EthResult<TxHash>>)>,
}

impl FaucetCache {
    /// Create and return both cache's frontend and the time bound service.
    fn create<Pool, Tasks>(
        config: FaucetConfig,
        pool: Pool,
        executor: Tasks,
        keypair: KeyPair,
        nonce: u64,
    ) -> (Self, FaucetCacheService<Pool, Tasks>) {
        let (to_service, rx) = unbounded_channel();

        // Construct an `LruCache` of `<String, SystemTime>`s, limited by 24hr expiry time
        let lru_cache = LruCache::with_expiry_duration(config.wait_period);
        let address = public_key_to_address(keypair.public_key());
        let service = FaucetCacheService {
            request_rx: UnboundedReceiverStream::new(rx),
            pool,
            lru_cache,
            config,
            executor,
            keypair,
            nonce,
        };
        let cache = FaucetCache { to_service };
        (cache, service)
    }

    /// Creates a new async LRU backed cache service task and spawns it to a new task via
    /// [tokio::spawn].
    ///
    /// See also [Self::spawn_with]
    pub fn spawn<Pool>(
        pool: Pool,
        config: FaucetConfig,
        keypair: KeyPair,
        nonce: u64,
    ) -> Self
    where
        Pool: TransactionPool + Unpin + Clone + 'static,
    {
        Self::spawn_with(pool, config, TokioTaskExecutor::default(), keypair, nonce)
    }

    /// Creates a new async LRU backed cache service task and spawns it to a new task via
    /// [tokio::spawn].
    ///
    /// See also [Self::spawn_with]
    pub fn spawn_with<Pool, Tasks>(
        pool: Pool,
        config: FaucetConfig,
        executor: Tasks,
        keypair: KeyPair,
        nonce: u64,
    ) -> Self
    where
        Pool: TransactionPool + Unpin + Clone + 'static,
        Tasks: TaskSpawner + Clone + 'static,
        {
        let (this, service) = Self::create(
            config,
            pool,
            executor.clone(),
            keypair,
            nonce,
        );

        executor.spawn_critical("fauce-cache", Box::pin(service));
        this
    }

    /// Requests a new transfer from faucet wallet to an address.
    pub(crate) async fn handle_request(
        &self,
        address: Address,
    ) -> EthResult<TxHash> {
        let (tx, rx) = oneshot::channel();
        let _ = self.to_service.send((address, tx));
        rx.await.map_err(|e| EthApiError::InvalidParams(e.to_string())).and_then(|res| res)
    }
}

pub struct FaucetCacheService<Pool, Tasks> {
    request_rx: UnboundedReceiverStream<(Address, oneshot::Sender<EthResult<TxHash>>)>,
    pool: Pool,
    lru_cache: LruCache<Address, SystemTime>,
    config: FaucetConfig,
    executor: Tasks,
    keypair: KeyPair,
    nonce: u64,
}

impl<Pool, Tasks> FaucetCacheService<Pool, Tasks>
where
    Pool: TransactionPool + Unpin + Clone + 'static,
    Tasks: TaskSpawner + Clone + 'static,

{
    /// Calculate when the wait period is over
    fn calc_wait_period(&self, last_transfer: &SystemTime) -> EthResult<Duration> {
        // calc when the address is removed from cache
        let end = last_transfer.checked_add(self.config.wait_period).ok_or(EthApiError::InternalEthError)?;

        // calc the remaining duration
        end.duration_since(SystemTime::now()).map_err(|e| EthApiError::InvalidParams(e.to_string()))
    }

    /// Create a EIP1559 transaction with max fee per gas set to 1 TEL.
    fn create_transaction(&self, to: Address) -> EthResult<TransactionSigned> {
        let request = TypedTransactionRequest::EIP1559(EIP1559TransactionRequest {
            chain_id: self.config.chain_id,
            nonce: self.nonce.into(),
            max_priority_fee_per_gas: U128::ZERO,
            max_fee_per_gas: U128::from(1_000_000_000), // 1 TEL
            gas_limit: U256::from(1_000_000_000), // 1 TEL
            kind: TransactionKind::Call(to),
            value: U256::from(self.config.transfer_amount),
            input: Default::default(),
            access_list: Default::default(),
        });
        // convert to primitive transaction
        let transaction = request.into_transaction().ok_or(SignError::InvalidTransactionRequest)?;
        let tx_signature_hash = transaction.signature_hash();
        let signature = self.sign_hash(tx_signature_hash)?;

        Ok(TransactionSigned::from_transaction_and_signature(transaction, signature))
    }

    fn sign_hash(
        &self,
        hash: H256,
    ) -> EthResult<Signature> {
        let secret = self.keypair.secret_bytes();
        let signature = sign_message(H256::from_slice(secret.as_ref()), hash);
        signature.map_err(|e| EthApiError::Signing(SignError::CouldNotSign))
    }

}

impl<Pool, Tasks> Future for FaucetCacheService<Pool, Tasks>
where
    Pool: TransactionPool + Unpin + Clone + 'static,
    Tasks: TaskSpawner + Clone + 'static,
{
    type Output = ();

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();

        loop {
            match ready!(this.request_rx.poll_next_unpin(cx)) {
                None => {
                    unreachable!("can't close")
                }
                Some((address, reply)) => {
                    // check the cache
                    if let Some(time) = this.lru_cache.peek(&address) {
                        let wait_period_over = this.calc_wait_period(time);
                        let error = match wait_period_over {
                            Ok(time) => {
                                let human_readable = format_duration(time);
                                let msg = format!("Wait period over at: {:?}", human_readable);
                                Err(EthApiError::InvalidParams(msg))
                            }
                            Err(e) => Err(e),
                        };
                        let _ = reply.send(error);
                    } else {
                        // submit tx to pool
                        let tx = this.create_transaction(address);
                        let pool = this.pool.clone();
                        this.executor.spawn_blocking(Box::pin(async move {
                            let res = submit_transaction(pool, tx).await;
                            let _ = reply.send(res);
                        }));
                    };
                }
            }
        }
    }
}

async fn submit_transaction<Pool>(
    pool: Pool,
    tx: TransactionSigned,
) -> EthResult<TxHash>
where
    Pool: TransactionPool + Clone + 'static,
{
    let recovered = tx.try_into_ecrecovered().or(Err(EthApiError::InvalidTransactionSignature))?;
    let transaction = <Pool::Transaction>::from_recovered_transaction(recovered.into());
    let hash = pool.add_transaction(TransactionOrigin::Local, transaction).await?;
    Ok(hash)
}

#[cfg(test)]
mod tests {
    use std::{sync::Arc, collections::HashMap};

    use reth_blockchain_tree::{BlockchainTree, ShareableBlockchainTree, BlockchainTreeConfig, TreeExternals};
    use reth_db::test_utils::create_test_rw_db;
    use reth_interfaces::test_utils::TestConsensus;
    use reth_primitives::{SealedBlock, BlockBody, ChainSpec, Genesis, GenesisAccount, U256, public_key_to_address};
    use reth_provider::{ProviderFactory, BlockWriter, providers::BlockchainProvider};
    use reth_revm::Factory;
    use reth_transaction_pool::{blobstore::InMemoryBlobStore, TransactionValidationTaskExecutor, PoolConfig};
    use secp256k1::{rand, KeyPair, Secp256k1};
    use crate::init::init_genesis;

    use super::*;

    #[tokio::test]
    async fn test_request() {
        // create pool
        let db = create_test_rw_db();
        let mut rng = rand::thread_rng();
        let secp = Secp256k1::new();
        let keypair = KeyPair::new(&secp, &mut rng);
        let public = keypair.public_key();
        println!("\n\npublickey: {:?}\n\n", public);

        let address = public_key_to_address(public);
        let genesis = genesis(address);
        let chain = custom_chain(genesis);
        let factory = ProviderFactory::new(db.as_ref(), Arc::clone(&chain));
        // let provider = factory.provider_rw().unwrap();
        let genesis_hash = init_genesis(db.clone(), chain.clone());
        let consensus = Arc::new(TestConsensus::default());

        // configure blockchain tree
        let tree_externals = TreeExternals::new(
            db.clone(),
            Arc::clone(&consensus),
            Factory::new(chain.clone()),
            Arc::clone(&chain),
        );
        let tree_config = BlockchainTreeConfig::default();
        // The size of the broadcast is twice the maximum reorg depth, because at maximum reorg
        // depth at least N blocks must be sent at once.
        let (canon_state_notification_sender, _receiver) =
            tokio::sync::broadcast::channel(tree_config.max_reorg_depth() as usize * 2);
        let blockchain_tree = ShareableBlockchainTree::new(
            BlockchainTree::new(
                tree_externals,
                canon_state_notification_sender.clone(),
                tree_config,
                None,
            ).unwrap()
        );

        // setup the blockchain provider
        let factory = ProviderFactory::new(Arc::clone(&db), Arc::clone(&chain));
        let blockchain_db = BlockchainProvider::new(factory, blockchain_tree.clone())?;
        let blob_store = InMemoryBlobStore::default();
        let validator = TransactionValidationTaskExecutor::eth_builder(Arc::clone(&chain))
            .with_additional_tasks(1)
            .build_with_tasks(blockchain_db.clone(), TokioTaskExecutor::default(), blob_store.clone());

        let transaction_pool =
            reth_transaction_pool::Pool::eth_pool(validator, blob_store, PoolConfig::default());

        let config = FaucetConfig {
            wait_period: Duration::from_secs(1),
            transfer_amount: 1,
        };
    }

    fn genesis(address: Address) -> Genesis {
        let accounts = HashMap::from([
            (address, GenesisAccount::default().with_balance(U256::MAX))
        ]);
        Genesis::default().extend_accounts(accounts)
    }

    fn custom_chain(genesis: Genesis) -> Arc<ChainSpec> {
        Arc::new(genesis.into())
    }
}
