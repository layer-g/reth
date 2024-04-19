//! Trait to launch the node and default implementation.

use crate::{
    components::{ComponentsBuilder, NodeComponents, NodeComponentsBuilder, PoolBuilder}, exex::BoxedLaunchExEx, hooks::NodeHooks, node::FullNode, rpc::{RethRpcServerHandles, RpcContext, RpcHooks}, BuilderContext, ComponentsState, Node, NodeHandle, RethFullAdapter, RethFullBuilderState, RethFullProviderType
};
use eyre::Context;
use futures::{future, future::Either, stream, stream_select, Future, StreamExt};
use rayon::ThreadPoolBuilder;
use reth_beacon_consensus::{
    hooks::{EngineHooks, PruneHook, StaticFileHook},
    BeaconConsensusEngine,
};
use reth_blockchain_tree::{
    BlockchainTree, BlockchainTreeConfig, ShareableBlockchainTree, TreeExternals,
};
use reth_config::config::EtlConfig;
use reth_db::{
    database::Database,
    database_metrics::{DatabaseMetadata, DatabaseMetrics},
    test_utils::{create_test_rw_db, TempDatabase},
    DatabaseEnv,
};
use reth_exex::{ExExContext, ExExHandle, ExExManager, ExExManagerHandle};
use reth_interfaces::p2p::either::EitherDownloader;
use reth_network::{NetworkBuilder, NetworkConfig, NetworkEvents, NetworkHandle};
use reth_node_api::{
    FullNodeComponents, FullNodeComponentsAdapter, FullNodeTypes, FullNodeTypesAdapter, NodeTypes,
};
use reth_node_core::{
    cli::config::{PayloadBuilderConfig, RethRpcConfig, RethTransactionPoolConfig},
    dirs::{ChainPath, DataDirPath, MaybePlatformPath},
    engine_api_store::EngineApiStore,
    engine_skip_fcu::EngineApiSkipFcu,
    exit::NodeExitFuture,
    init::init_genesis,
    node_config::NodeConfig,
    primitives::{kzg::KzgSettings, Head},
    utils::write_peers_to_file,
};
use reth_node_events::{cl::ConsensusLayerHealthEvents, node};
use reth_primitives::{constants::eip4844::MAINNET_KZG_TRUSTED_SETUP, format_ether, ChainSpec};
use reth_provider::{
    providers::BlockchainProvider, CanonStateSubscriptions, ChainSpecProvider, ProviderFactory,
};
use reth_prune::PrunerBuilder;
use reth_revm::EvmProcessorFactory;
use reth_rpc_engine_api::EngineApi;
use reth_static_file::StaticFileProducer;
use reth_tasks::TaskExecutor;
use reth_tracing::tracing::{debug, error, info};
use reth_transaction_pool::{PoolConfig, TransactionPool};
use std::{cmp::max, str::FromStr, sync::Arc, thread::available_parallelism};
use tokio::sync::{mpsc::unbounded_channel, oneshot};

/// Launch the node.
pub trait LaunchNode<DB, Types, Components>
where
    DB: Database + Clone + Unpin + 'static,
    Types: NodeTypes,
    Components: NodeComponentsBuilder<RethFullAdapter<DB, Types>> + NodeTypes,
{
    /// Build components and launch the node.
    fn launch(
        self,
        args: LaunchArgs<DB, Types, Components>,
    ) -> impl Future<Output = eyre::Result<NodeHandle<
        FullNodeComponentsAdapter<RethFullAdapter<DB, Types>, Components::Pool>
>>>;
}

pub struct LaunchArgs<DB, Types: NodeTypes, Components> {
    pub config: NodeConfig,
    pub components: RethFullBuilderState<DB, Types, Components>,
    pub database: DB,
    pub executor: TaskExecutor,
    pub data_dir: ChainPath<DataDirPath>,
    pub reth_config: reth_config::Config,
}

/// Default ethererum node launch
#[derive(Debug)]
pub struct DefaultLauncher;

impl<DB, Types, Components> LaunchNode<DB, Types, Components> for DefaultLauncher
where
    DB: Database + DatabaseMetrics + DatabaseMetadata + Clone + Unpin + 'static,
    Types: NodeTypes,
    Components: NodeComponentsBuilder<
        RethFullAdapter<DB, Types>,
    > + NodeTypes,
{
    async fn launch(
        self,
        args: LaunchArgs<DB, Types, Components>,
    ) -> eyre::Result<NodeHandle<
    FullNodeComponentsAdapter<RethFullAdapter<DB, Types>, Components::Pool>
>> {
        // get config from file
        // let reth_config = self.load_config(&data_dir)?;
        // let reth_config = self.reth_config.clone();

        let LaunchArgs {
            config,
            components: ComponentsState { types, components_builder, hooks, rpc, exexs },
            database,
            executor,
            data_dir,
            reth_config,
        } = args;

        // Raise the fd limit of the process.
        // Does not do anything on windows.
        fdlimit::raise_fd_limit()?;

        // Limit the global rayon thread pool, reserving 2 cores for the rest of the system
        let _ = ThreadPoolBuilder::new()
            .num_threads(
                available_parallelism().map_or(25, |cpus| max(cpus.get().saturating_sub(2), 2)),
            )
            .build_global()
            .map_err(|e| error!("Failed to build global thread pool: {:?}", e));

        let provider_factory = ProviderFactory::new(
            database.clone(),
            Arc::clone(&config.chain),
            data_dir.static_files_path(),
        )?
        .with_static_files_metrics();
        info!(target: "reth::cli", "Database opened");

        let prometheus_handle = config.install_prometheus_recorder()?;
        config
            .start_metrics_endpoint(
                prometheus_handle,
                database.clone(),
                provider_factory.static_file_provider(),
            )
            .await?;

        debug!(target: "reth::cli", chain=%config.chain.chain, genesis=?config.chain.genesis_hash(), "Initializing genesis");

        let genesis_hash = init_genesis(provider_factory.clone())?;

        info!(target: "reth::cli", "\n{}", config.chain.display_hardforks());

        let consensus = config.consensus();

        debug!(target: "reth::cli", "Spawning stages metrics listener task");
        let (sync_metrics_tx, sync_metrics_rx) = unbounded_channel();
        let sync_metrics_listener = reth_stages::MetricsListener::new(sync_metrics_rx);
        executor.spawn_critical("stages metrics listener task", sync_metrics_listener);

        let prune_config = config.prune_config()?.or_else(|| reth_config.prune.clone());

        // Configure the blockchain tree for the node
        let evm_config = types.evm_config();
        let tree_config = BlockchainTreeConfig::default();
        let tree_externals = TreeExternals::new(
            provider_factory.clone(),
            consensus.clone(),
            EvmProcessorFactory::new(config.chain.clone(), evm_config.clone()),
        );
        let tree = BlockchainTree::new(
            tree_externals,
            tree_config,
            prune_config.as_ref().map(|config| config.segments.clone()),
        )?
        .with_sync_metrics_tx(sync_metrics_tx.clone());

        let canon_state_notification_sender = tree.canon_state_notification_sender();
        let blockchain_tree = ShareableBlockchainTree::new(tree);
        debug!(target: "reth::cli", "configured blockchain tree");

        // fetch the head block from the database
        let head =
            config.lookup_head(provider_factory.clone()).wrap_err("the head block is missing")?;

        // setup the blockchain provider
        let blockchain_db =
            BlockchainProvider::new(provider_factory.clone(), blockchain_tree.clone())?;

        let ctx = BuilderContext::new(
            head,
            blockchain_db,
            executor,
            data_dir,
            config,
            reth_config,
            evm_config.clone(),
        );

        debug!(target: "reth::cli", "creating components");
        let NodeComponents { transaction_pool, network, payload_builder } =
            components_builder.build_components(&ctx).await?;

        let BuilderContext {
            provider: blockchain_db,
            executor,
            data_dir,
            mut config,
            mut reth_config,
            ..
        } = ctx;

        let NodeHooks { on_component_initialized, on_node_started, .. } = hooks;

        let node_components = FullNodeComponentsAdapter {
            evm_config: evm_config.clone(),
            pool: transaction_pool.clone(),
            network: network.clone(),
            provider: blockchain_db.clone(),
            payload_builder: payload_builder.clone(),
            executor: executor.clone(),
        };
        debug!(target: "reth::cli", "calling on_component_initialized hook");
        on_component_initialized.on_event(node_components.clone())?;

        // spawn exexs
        let mut exex_handles = Vec::with_capacity(exexs.len());
        let mut exexs_vec = Vec::with_capacity(exexs.len());
        for (id, exex) in exexs {
            // create a new exex handle
            let (handle, events, notifications) = ExExHandle::new(id.clone());
            exex_handles.push(handle);

            // create the launch context for the exex
            let context = ExExContext {
                head,
                provider: blockchain_db.clone(),
                task_executor: executor.clone(),
                data_dir: data_dir.clone(),
                config: config.clone(),
                reth_config: reth_config.clone(),
                pool: transaction_pool.clone(),
                events,
                notifications,
            };

            let executor = executor.clone();
            exexs_vec.push(async move {
                debug!(target: "reth::cli", id, "spawning exex");
                let span = reth_tracing::tracing::info_span!("exex", id);
                let _enter = span.enter();

                // init the exex
                let exex = exex.launch(context).await.unwrap();

                // spawn it as a crit task
                executor.spawn_critical("exex", async move {
                    info!(target: "reth::cli", id, "ExEx started");
                    match exex.await {
                        Ok(_) => panic!("ExEx {id} finished. ExEx's should run indefinitely"),
                        Err(err) => panic!("ExEx {id} crashed: {err}"),
                    }
                });
            });
        }

        future::join_all(exexs_vec).await;

        // spawn exex manager
        let exex_manager_handle = if !exex_handles.is_empty() {
            debug!(target: "reth::cli", "spawning exex manager");
            // todo(onbjerg): rm magic number
            let exex_manager = ExExManager::new(exex_handles, 1024);
            let exex_manager_handle = exex_manager.handle();
            executor.spawn_critical("exex manager", async move {
                exex_manager.await.expect("exex manager crashed");
            });

            // send notifications from the blockchain tree to exex manager
            let mut canon_state_notifications = blockchain_tree.subscribe_to_canonical_state();
            let mut handle = exex_manager_handle.clone();
            executor.spawn_critical("exex manager blockchain tree notifications", async move {
                while let Ok(notification) = canon_state_notifications.recv().await {
                    handle
                        .send_async(notification)
                        .await
                        .expect("blockchain tree notification could not be sent to exex manager");
                }
            });

            info!(target: "reth::cli", "ExEx Manager started");

            Some(exex_manager_handle)
        } else {
            None
        };

        // create pipeline
        let network_client = network.fetch_client().await?;
        let (consensus_engine_tx, mut consensus_engine_rx) = unbounded_channel();

        if let Some(skip_fcu_threshold) = config.debug.skip_fcu {
            debug!(target: "reth::cli", "spawning skip FCU task");
            let (skip_fcu_tx, skip_fcu_rx) = unbounded_channel();
            let engine_skip_fcu = EngineApiSkipFcu::new(skip_fcu_threshold);
            executor.spawn_critical(
                "skip FCU interceptor",
                engine_skip_fcu.intercept(consensus_engine_rx, skip_fcu_tx),
            );
            consensus_engine_rx = skip_fcu_rx;
        }

        if let Some(store_path) = config.debug.engine_api_store.clone() {
            debug!(target: "reth::cli", "spawning engine API store");
            let (engine_intercept_tx, engine_intercept_rx) = unbounded_channel();
            let engine_api_store = EngineApiStore::new(store_path);
            executor.spawn_critical(
                "engine api interceptor",
                engine_api_store.intercept(consensus_engine_rx, engine_intercept_tx),
            );
            consensus_engine_rx = engine_intercept_rx;
        };

        let max_block = config.max_block(&network_client, provider_factory.clone()).await?;
        let mut hooks = EngineHooks::new();

        let static_file_producer = StaticFileProducer::new(
            provider_factory.clone(),
            provider_factory.static_file_provider(),
            prune_config.clone().unwrap_or_default().segments,
        );
        let static_file_producer_events = static_file_producer.lock().events();
        hooks.add(StaticFileHook::new(static_file_producer.clone(), Box::new(executor.clone())));
        info!(target: "reth::cli", "StaticFileProducer initialized");

        // Make sure ETL doesn't default to /tmp/, but to whatever datadir is set to
        if reth_config.stages.etl.dir.is_none() {
            reth_config.stages.etl.dir = Some(EtlConfig::from_datadir(&data_dir.data_dir_path()));
        }

        // Configure the pipeline
        let pipeline_exex_handle =
            exex_manager_handle.clone().unwrap_or_else(ExExManagerHandle::empty);
        let (mut pipeline, client) = if config.dev.dev {
            info!(target: "reth::cli", "Starting Reth in dev mode");

            for (idx, (address, alloc)) in config.chain.genesis.alloc.iter().enumerate() {
                info!(target: "reth::cli", "Allocated Genesis Account: {:02}. {} ({} ETH)", idx, address.to_string(), format_ether(alloc.balance));
            }

            let mining_mode = config.mining_mode(transaction_pool.pending_transactions_listener());

            let (_, client, mut task) = reth_auto_seal_consensus::AutoSealBuilder::new(
                Arc::clone(&config.chain),
                blockchain_db.clone(),
                transaction_pool.clone(),
                consensus_engine_tx.clone(),
                canon_state_notification_sender,
                mining_mode,
                evm_config.clone(),
            )
            .build();

            let mut pipeline = crate::setup::build_networked_pipeline(
                &config,
                &reth_config.stages,
                client.clone(),
                Arc::clone(&consensus),
                provider_factory.clone(),
                &executor,
                sync_metrics_tx,
                prune_config.clone(),
                max_block,
                static_file_producer,
                evm_config,
                pipeline_exex_handle,
            )
            .await?;

            let pipeline_events = pipeline.events();
            task.set_pipeline_events(pipeline_events);
            debug!(target: "reth::cli", "Spawning auto mine task");
            executor.spawn(Box::pin(task));

            (pipeline, EitherDownloader::Left(client))
        } else {
            let pipeline = crate::setup::build_networked_pipeline(
                &config,
                &reth_config.stages,
                network_client.clone(),
                Arc::clone(&consensus),
                provider_factory.clone(),
                &executor,
                sync_metrics_tx,
                prune_config.clone(),
                max_block,
                static_file_producer,
                evm_config,
                pipeline_exex_handle,
            )
            .await?;

            (pipeline, EitherDownloader::Right(network_client))
        };

        let pipeline_events = pipeline.events();

        let initial_target = config.initial_pipeline_target(genesis_hash);

        let prune_config = prune_config.unwrap_or_default();
        let mut pruner_builder = PrunerBuilder::new(prune_config.clone())
            .max_reorg_depth(tree_config.max_reorg_depth() as usize)
            .prune_delete_limit(config.chain.prune_delete_limit)
            .timeout(PrunerBuilder::DEFAULT_TIMEOUT);
        if let Some(exex_manager_handle) = &exex_manager_handle {
            pruner_builder =
                pruner_builder.finished_exex_height(exex_manager_handle.finished_height());
        }

        let mut pruner = pruner_builder.build(provider_factory.clone());

        let pruner_events = pruner.events();
        hooks.add(PruneHook::new(pruner, Box::new(executor.clone())));
        info!(target: "reth::cli", ?prune_config, "Pruner initialized");

        // Configure the consensus engine
        let (beacon_consensus_engine, beacon_engine_handle) = BeaconConsensusEngine::with_channel(
            client,
            pipeline,
            blockchain_db.clone(),
            Box::new(executor.clone()),
            Box::new(network.clone()),
            max_block,
            config.debug.continuous,
            payload_builder.clone(),
            initial_target,
            reth_beacon_consensus::MIN_BLOCKS_FOR_PIPELINE_RUN,
            consensus_engine_tx,
            consensus_engine_rx,
            hooks,
        )?;
        info!(target: "reth::cli", "Consensus engine initialized");

        let events = stream_select!(
            network.event_listener().map(Into::into),
            beacon_engine_handle.event_listener().map(Into::into),
            pipeline_events.map(Into::into),
            if config.debug.tip.is_none() && !config.dev.dev {
                Either::Left(
                    ConsensusLayerHealthEvents::new(Box::new(blockchain_db.clone()))
                        .map(Into::into),
                )
            } else {
                Either::Right(stream::empty())
            },
            pruner_events.map(Into::into),
            static_file_producer_events.map(Into::into)
        );
        executor.spawn_critical(
            "events task",
            node::handle_events(Some(network.clone()), Some(head.number), events, database.clone()),
        );

        let engine_api = EngineApi::new(
            blockchain_db.clone(),
            config.chain.clone(),
            beacon_engine_handle,
            payload_builder.into(),
            Box::new(executor.clone()),
        );
        info!(target: "reth::cli", "Engine API handler initialized");

        // extract the jwt secret from the args if possible
        let default_jwt_path = data_dir.jwt_path();
        let jwt_secret = config.rpc.auth_jwt_secret(default_jwt_path)?;

        // adjust rpc port numbers based on instance number
        config.adjust_instance_ports();

        // Start RPC servers

        let (rpc_server_handles, mut rpc_registry) = crate::rpc::launch_rpc_servers(
            node_components.clone(),
            engine_api,
            &config,
            jwt_secret,
            rpc,
        )
        .await?;

        // in dev mode we generate 20 random dev-signer accounts
        if config.dev.dev {
            rpc_registry.eth_api().with_dev_accounts();
        }

        // Run consensus engine to completion
        let (tx, rx) = oneshot::channel();
        info!(target: "reth::cli", "Starting consensus engine");
        executor.spawn_critical_blocking("consensus engine", async move {
            let res = beacon_consensus_engine.await;
            let _ = tx.send(res);
        });

        let FullNodeComponentsAdapter {
            evm_config,
            pool,
            network,
            provider,
            payload_builder,
            executor,
        } = node_components;

        let full_node = FullNode {
            evm_config,
            pool,
            network,
            provider,
            payload_builder,
            task_executor: executor,
            rpc_server_handles,
            rpc_registry,
            config,
            data_dir,
        };
        // Notify on node started
        on_node_started.on_event(full_node.clone())?;

        let handle = NodeHandle {
            node_exit_future: NodeExitFuture::new(rx, full_node.config.debug.terminate),
            node: full_node,
        };

        Ok(handle)
    }
}
