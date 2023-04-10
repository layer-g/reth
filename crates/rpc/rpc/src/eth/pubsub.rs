//! `eth_` PubSub RPC handler implementation
use crate::eth::{cache::EthStateCache, logs_utils};
use futures::StreamExt;
use jsonrpsee::{types::SubscriptionResult, SubscriptionSink};
use reth_network_api::NetworkInfo;
use reth_primitives::{filter::FilteredParams, TxHash};
use reth_provider::{BlockProvider, CanonStateSubscriptions, EvmEnvProvider};
use reth_rpc_api::EthPubSubApiServer;
use reth_rpc_types::{
    pubsub::{
        Params, PubSubSyncStatus, SubscriptionKind, SubscriptionResult as EthSubscriptionResult,
        SyncStatusMetadata,
    },
    Header, Log,
};
use reth_tasks::{TaskSpawner, TokioTaskExecutor};
use reth_transaction_pool::TransactionPool;
use tokio_stream::{
    wrappers::{BroadcastStream, ReceiverStream},
    Stream,
};

/// `Eth` pubsub RPC implementation.
///
/// This handles `eth_subscribe` RPC calls.
#[derive(Clone)]
pub struct EthPubSub<Client, Pool, Events, Network> {
    /// All nested fields bundled together.
    inner: EthPubSubInner<Client, Pool, Events, Network>,
    /// The type that's used to spawn subscription tasks.
    subscription_task_spawner: Box<dyn TaskSpawner>,
}

// === impl EthPubSub ===

impl<Client, Pool, Events, Network> EthPubSub<Client, Pool, Events, Network> {
    /// Creates a new, shareable instance.
    ///
    /// Subscription tasks are spawned via [tokio::task::spawn]
    pub fn new(
        client: Client,
        pool: Pool,
        chain_events: Events,
        network: Network,
        eth_cache: EthStateCache,
    ) -> Self {
        Self::with_spawner(
            client,
            pool,
            chain_events,
            network,
            eth_cache,
            Box::<TokioTaskExecutor>::default(),
        )
    }

    /// Creates a new, shareable instance.
    pub fn with_spawner(
        client: Client,
        pool: Pool,
        chain_events: Events,
        network: Network,
        eth_cache: EthStateCache,
        subscription_task_spawner: Box<dyn TaskSpawner>,
    ) -> Self {
        let inner = EthPubSubInner { client, pool, chain_events, network, eth_cache };
        Self { inner, subscription_task_spawner }
    }
}

impl<Client, Pool, Events, Network> EthPubSubApiServer for EthPubSub<Client, Pool, Events, Network>
where
    Client: BlockProvider + EvmEnvProvider + Clone + 'static,
    Pool: TransactionPool + 'static,
    Events: CanonStateSubscriptions + Clone + 'static,
    Network: NetworkInfo + Clone + 'static,
{
    /// Handler for `eth_subscribe`
    fn subscribe(
        &self,
        mut sink: SubscriptionSink,
        kind: SubscriptionKind,
        params: Option<Params>,
    ) -> SubscriptionResult {
        sink.accept()?;

        let pubsub = self.inner.clone();
        self.subscription_task_spawner.spawn(Box::pin(async move {
            handle_accepted(pubsub, sink, kind, params).await;
        }));

        Ok(())
    }
}

/// The actual handler for and accepted [`EthPubSub::subscribe`] call.
async fn handle_accepted<Client, Pool, Events, Network>(
    pubsub: EthPubSubInner<Client, Pool, Events, Network>,
    mut accepted_sink: SubscriptionSink,
    kind: SubscriptionKind,
    params: Option<Params>,
) where
    Client: BlockProvider + EvmEnvProvider + Clone + 'static,
    Pool: TransactionPool + 'static,
    Events: CanonStateSubscriptions + Clone + 'static,
    Network: NetworkInfo + Clone + 'static,
{
    match kind {
        SubscriptionKind::NewHeads => {
            let stream = pubsub
                .into_new_headers_stream()
                .map(|block| EthSubscriptionResult::Header(Box::new(block.into())));
            accepted_sink.pipe_from_stream(stream).await;
        }
        SubscriptionKind::Logs => {
            // if no params are provided, used default filter params
            let filter = match params {
                Some(Params::Logs(filter)) => FilteredParams::new(Some(*filter)),
                _ => FilteredParams::default(),
            };
            let stream =
                pubsub.into_log_stream(filter).map(|log| EthSubscriptionResult::Log(Box::new(log)));
            accepted_sink.pipe_from_stream(stream).await;
        }
        SubscriptionKind::NewPendingTransactions => {
            let stream = pubsub
                .into_pending_transaction_stream()
                .map(EthSubscriptionResult::TransactionHash);
            accepted_sink.pipe_from_stream(stream).await;
        }
        SubscriptionKind::Syncing => {
            // get new block subscription
            let mut canon_state = BroadcastStream::new(pubsub.chain_events.subscribe_canon_state());
            // get current sync status
            let mut initial_sync_status = pubsub.network.is_syncing();
            let current_sub_res = pubsub.sync_status(initial_sync_status).await;

            // send the current status immediately
            let _ = accepted_sink.send(&current_sub_res);

            while (canon_state.next().await).is_some() {
                let current_syncing = pubsub.network.is_syncing();
                // Only send a new response if the sync status has changed
                if current_syncing != initial_sync_status {
                    // Update the sync status on each new block
                    initial_sync_status = current_syncing;

                    // send a new message now that the status changed
                    let sync_status = pubsub.sync_status(current_syncing).await;
                    let _ = accepted_sink.send(&sync_status);
                }
            }
        }
    }
}

impl<Client, Pool, Events, Network> std::fmt::Debug for EthPubSub<Client, Pool, Events, Network> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("EthPubSub").finish_non_exhaustive()
    }
}

/// Container type `EthPubSub`
#[derive(Clone)]
struct EthPubSubInner<Client, Pool, Events, Network> {
    /// The transaction pool.
    pool: Pool,
    /// The client that can interact with the chain.
    client: Client,
    /// A type that allows to create new event subscriptions,
    chain_events: Events,
    /// The network.
    network: Network,
    /// The async cache frontend for eth related data
    eth_cache: EthStateCache,
}

// == impl EthPubSubInner ===

impl<Client, Pool, Events, Network> EthPubSubInner<Client, Pool, Events, Network>
where
    Client: BlockProvider + 'static,
{
    /// Returns the current sync status for the `syncing` subscription
    async fn sync_status(&self, is_syncing: bool) -> EthSubscriptionResult {
        if is_syncing {
            let current_block =
                self.client.chain_info().map(|info| info.best_number).unwrap_or_default();
            EthSubscriptionResult::SyncState(PubSubSyncStatus::Detailed(SyncStatusMetadata {
                syncing: true,
                starting_block: 0,
                current_block,
                highest_block: Some(current_block),
            }))
        } else {
            EthSubscriptionResult::SyncState(PubSubSyncStatus::Simple(false))
        }
    }
}

impl<Client, Pool, Events, Network> EthPubSubInner<Client, Pool, Events, Network>
where
    Pool: TransactionPool + 'static,
{
    /// Returns a stream that yields all transactions emitted by the txpool.
    fn into_pending_transaction_stream(self) -> impl Stream<Item = TxHash> {
        ReceiverStream::new(self.pool.pending_transactions_listener())
    }
}

impl<Client, Pool, Events, Network> EthPubSubInner<Client, Pool, Events, Network>
where
    Client: BlockProvider + EvmEnvProvider + 'static,
    Events: CanonStateSubscriptions + 'static,
    Network: NetworkInfo + 'static,
    Pool: 'static,
{
    /// Returns a stream that yields all new RPC blocks.
    fn into_new_headers_stream(self) -> impl Stream<Item = Header> {
        BroadcastStream::new(self.chain_events.subscribe_canon_state())
            .map(|new_block| {
                let new_chain = new_block.expect("new block subscription never ends; qed");
                new_chain
                    .commited()
                    .map(|c| {
                        c.blocks()
                            .iter()
                            .map(|(_, block)| {
                                Header::from_primitive_with_hash(block.header.clone())
                            })
                            .collect::<Vec<_>>()
                    })
                    .unwrap_or_default()
            })
            .flat_map(futures::stream::iter)
    }

    /// Returns a stream that yields all logs that match the given filter.
    fn into_log_stream(self, filter: FilteredParams) -> impl Stream<Item = Log> {
        BroadcastStream::new(self.chain_events.subscribe_canon_state())
            .map(move |canon_state| {
                canon_state.expect("new block subscription never ends; qed").block_receipts()
            })
            .flat_map(futures::stream::iter)
            .flat_map(move |(block_receipts, removed)| {
                let all_logs = logs_utils::matching_block_logs(
                    &filter,
                    block_receipts.block,
                    block_receipts.tx_receipts.into_iter(),
                    removed,
                );
                futures::stream::iter(all_logs)
            })
    }
}
