//! Faucet rpc endpoint

use std::{
    future::Future,
    time::{UNIX_EPOCH, SystemTime, Duration}, pin::Pin, task::{Context, Poll, ready}
};

use futures::StreamExt;
use jsonrpsee::{proc_macros::rpc, core::async_trait};
use lru_time_cache::LruCache;
use reth_primitives::{Address, TxHash, TransactionSigned, FromRecoveredPooledTransaction};
use reth_rpc::eth::error::{EthResult, EthApiError};
use reth_tasks::{TokioTaskExecutor, TaskSpawner};
use reth_transaction_pool::{TransactionPool, TransactionOrigin};
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
    ) -> (Self, FaucetCacheService<Pool, Tasks>) {
        let (to_service, rx) = unbounded_channel();

        // Construct an `LruCache` of `<String, SystemTime>`s, limited by 24hr expiry time
        let lru_cache = LruCache::with_expiry_duration(config.wait_period);
        let service = FaucetCacheService {
            request_rx: UnboundedReceiverStream::new(rx),
            pool,
            lru_cache,
            config,
            executor,
        };
        let cache = FaucetCache { to_service };
        (cache, service)
    }

    /// Creates a new async LRU backed cache service task and spawns it to a new task via
    /// [tokio::spawn].
    ///
    /// See also [Self::spawn_with]
    pub fn spawn<Pool>(pool: Pool, config: FaucetConfig) -> Self
    where
        Pool: TransactionPool + Unpin + Clone + 'static,
    {
        Self::spawn_with(pool, config, TokioTaskExecutor::default())
    }

    /// Creates a new async LRU backed cache service task and spawns it to a new task via
    /// [tokio::spawn].
    ///
    /// See also [Self::spawn_with]
    pub fn spawn_with<Pool, Tasks>(
        pool: Pool,
        config: FaucetConfig,
        executor: Tasks,
    ) -> Self
    where
        Pool: TransactionPool + Unpin + Clone + 'static,
        Tasks: TaskSpawner + Clone + 'static,
        {
        let (this, service) = Self::create(
            config,
            pool,
            executor.clone(),
        );

        executor.spawn_critical("fauce-cache", Box::pin(service));
        this
    }

    /// Requests a new transfer from faucet wallet to an address.
    pub(crate) async fn handle_request(
        &self,
        address: Address,
    ) -> EthResult<TxHash> {
        todo!()
    }
}

pub struct FaucetCacheService<Pool, Tasks> {
    request_rx: UnboundedReceiverStream<(Address, oneshot::Sender<EthResult<TxHash>>)>,
    pool: Pool,
    lru_cache: LruCache<Address, SystemTime>,
    config: FaucetConfig,
    executor: Tasks,
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

    fn create_transaction(&self, address: Address) -> TransactionSigned {
        todo!()
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
