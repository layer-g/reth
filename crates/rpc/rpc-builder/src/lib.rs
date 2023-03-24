#![warn(missing_docs, unreachable_pub)]
#![deny(unused_must_use, rust_2018_idioms)]
#![doc(test(
    no_crate_inject,
    attr(deny(warnings, rust_2018_idioms), allow(dead_code, unused_variables))
))]

//! Configure reth RPC
//!
//! This crate contains several builder and config types that allow to configure the selection of
//! [RethRpcModule] specific to transports (ws, http, ipc).
//!
//! The [RpcModuleBuilder] is the main entrypoint for configuring all reth modules. It takes
//! instances of components required to start the servers, such as provider impls, network and
//! transaction pool. [RpcModuleBuilder::build] returns a [TransportRpcModules] which contains the
//! transport specific config (what APIs are available via this transport).
//!
//! The [RpcServerConfig] is used to configure the [RpcServer] type which contains all transport
//! implementations (http server, ws server, ipc server). [RpcServer::start] requires the
//! [TransportRpcModules] so it can start the servers with the configured modules.
//!
//! # Examples
//!
//! Configure only a http server with a selection of [RethRpcModule]s
//!
//! ```
//! use reth_interfaces::events::ChainEventSubscriptions;
//! use reth_network_api::{NetworkInfo, Peers};
//! use reth_provider::{BlockProvider, StateProviderFactory, EvmEnvProvider};
//! use reth_rpc_builder::{RethRpcModule, RpcModuleBuilder, RpcServerConfig, ServerBuilder, TransportRpcModuleConfig};
//! use reth_tasks::TokioTaskExecutor;
//! use reth_transaction_pool::TransactionPool;
//! pub async fn launch<Client, Pool, Network, Events>(client: Client, pool: Pool, network: Network, events: Events)
//! where
//!     Client: BlockProvider + StateProviderFactory + EvmEnvProvider + Clone + Unpin + 'static,
//!     Pool: TransactionPool + Clone + 'static,
//!     Network: NetworkInfo + Peers + Clone + 'static,
//!     Events: ChainEventSubscriptions +  Clone + 'static,
//! {
//!     // configure the rpc module per transport
//!     let transports = TransportRpcModuleConfig::default().with_http(vec![
//!         RethRpcModule::Admin,
//!         RethRpcModule::Debug,
//!         RethRpcModule::Eth,
//!         RethRpcModule::Web3,
//!     ]);
//!     let transport_modules = RpcModuleBuilder::new(client, pool, network, TokioTaskExecutor::default(), events).build(transports);
//!     let handle = RpcServerConfig::default()
//!         .with_http(ServerBuilder::default())
//!         .start(transport_modules)
//!         .await
//!         .unwrap();
//! }
//! ```

use constants::*;
use jsonrpsee::{
    core::{server::rpc_module::Methods, Error as RpcError},
    server::{IdProvider, Server, ServerHandle},
    RpcModule,
};
use reth_interfaces::events::ChainEventSubscriptions;
use reth_ipc::server::IpcServer;
use reth_network_api::{NetworkInfo, Peers};
use reth_provider::{BlockProvider, EvmEnvProvider, StateProviderFactory};
use reth_rpc::{
    eth::cache::EthStateCache, AdminApi, DebugApi, EthApi, EthFilter, EthPubSub,
    EthSubscriptionIdProvider, NetApi, TraceApi, Web3Api,
};
use reth_rpc_api::servers::*;
use reth_tasks::TaskSpawner;
use reth_transaction_pool::TransactionPool;
use serde::{Deserialize, Serialize, Serializer};
use std::{
    collections::HashMap,
    fmt,
    net::{Ipv4Addr, SocketAddr, SocketAddrV4},
    str::FromStr,
};
use strum::{AsRefStr, EnumString, EnumVariantNames, ParseError, VariantNames};
use tower::layer::util::{Identity, Stack};
use tower_http::cors::CorsLayer;
use tracing::{instrument, trace};

/// Auth server utilities.
pub mod auth;

/// Cors utilities.
mod cors;

/// Eth utils
mod eth;

/// Common RPC constants.
pub mod constants;

// re-export for convenience
pub use crate::eth::{EthConfig, EthHandlers};
pub use jsonrpsee::server::ServerBuilder;
pub use reth_ipc::server::{Builder as IpcServerBuilder, Endpoint};

/// Convenience function for starting a server in one step.
pub async fn launch<Client, Pool, Network, Tasks, Events>(
    client: Client,
    pool: Pool,
    network: Network,
    module_config: impl Into<TransportRpcModuleConfig>,
    server_config: impl Into<RpcServerConfig>,
    executor: Tasks,
    events: Events,
) -> Result<RpcServerHandle, RpcError>
where
    Client: BlockProvider + StateProviderFactory + EvmEnvProvider + Clone + Unpin + 'static,
    Pool: TransactionPool + Clone + 'static,
    Network: NetworkInfo + Peers + Clone + 'static,
    Tasks: TaskSpawner + Clone + 'static,
    Events: ChainEventSubscriptions + Clone + 'static,
{
    let module_config = module_config.into();
    let server_config = server_config.into();
    RpcModuleBuilder::new(client, pool, network, executor, events)
        .build(module_config)
        .start_server(server_config)
        .await
}

/// A builder type to configure the RPC module: See [RpcModule]
///
/// This is the main entrypoint for up RPC servers.
#[derive(Debug)]
pub struct RpcModuleBuilder<Client, Pool, Network, Tasks, Events> {
    /// The Client type to when creating all rpc handlers
    client: Client,
    /// The Pool type to when creating all rpc handlers
    pool: Pool,
    /// The Network type to when creating all rpc handlers
    network: Network,
    /// How additional tasks are spawned, for example in the eth pubsub namespace
    executor: Tasks,
    /// Provides access to chain events, such as new blocks, required by pubsub.
    events: Events,
}

// === impl RpcBuilder ===

impl<Client, Pool, Network, Tasks, Events> RpcModuleBuilder<Client, Pool, Network, Tasks, Events> {
    /// Create a new instance of the builder
    pub fn new(
        client: Client,
        pool: Pool,
        network: Network,
        executor: Tasks,
        events: Events,
    ) -> Self {
        Self { client, pool, network, executor, events }
    }

    /// Configure the client instance.
    pub fn with_client<C>(self, client: C) -> RpcModuleBuilder<C, Pool, Network, Tasks, Events>
    where
        C: BlockProvider + StateProviderFactory + EvmEnvProvider + 'static,
    {
        let Self { pool, network, executor, events, .. } = self;
        RpcModuleBuilder { client, network, pool, executor, events }
    }

    /// Configure the transaction pool instance.
    pub fn with_pool<P>(self, pool: P) -> RpcModuleBuilder<Client, P, Network, Tasks, Events>
    where
        P: TransactionPool + 'static,
    {
        let Self { client, network, executor, events, .. } = self;
        RpcModuleBuilder { client, network, pool, executor, events }
    }

    /// Configure the network instance.
    pub fn with_network<N>(self, network: N) -> RpcModuleBuilder<Client, Pool, N, Tasks, Events>
    where
        N: NetworkInfo + Peers + 'static,
    {
        let Self { client, pool, executor, events, .. } = self;
        RpcModuleBuilder { client, network, pool, executor, events }
    }

    /// Configure the task executor to use for additional tasks.
    pub fn with_executor<T>(self, executor: T) -> RpcModuleBuilder<Client, Pool, Network, T, Events>
    where
        T: TaskSpawner + 'static,
    {
        let Self { pool, network, client, events, .. } = self;
        RpcModuleBuilder { client, network, pool, executor, events }
    }

    /// Configure the event subscriber instance
    pub fn with_events<E>(self, events: E) -> RpcModuleBuilder<Client, Pool, Network, Tasks, E>
    where
        E: ChainEventSubscriptions + 'static,
    {
        let Self { client, pool, executor, network, .. } = self;
        RpcModuleBuilder { client, network, pool, executor, events }
    }
}

impl<Client, Pool, Network, Tasks, Events> RpcModuleBuilder<Client, Pool, Network, Tasks, Events>
where
    Client: BlockProvider + StateProviderFactory + EvmEnvProvider + Clone + Unpin + 'static,
    Pool: TransactionPool + Clone + 'static,
    Network: NetworkInfo + Peers + Clone + 'static,
    Tasks: TaskSpawner + Clone + 'static,
    Events: ChainEventSubscriptions + Clone + 'static,
{
    /// Configures all [RpcModule]s specific to the given [TransportRpcModuleConfig] which can be
    /// used to start the transport server(s).
    ///
    /// See also [RpcServer::start]
    pub fn build(self, module_config: TransportRpcModuleConfig) -> TransportRpcModules<()> {
        let mut modules = TransportRpcModules::default();

        let Self { client, pool, network, executor, events } = self;

        if !module_config.is_empty() {
            let TransportRpcModuleConfig { http, ws, ipc, config } = module_config;

            let mut registry = RethModuleRegistry::new(
                client,
                pool,
                network,
                executor,
                events,
                config.unwrap_or_default(),
            );

            modules.http = registry.maybe_module(http.as_ref());
            modules.ws = registry.maybe_module(ws.as_ref());
            modules.ipc = registry.maybe_module(ipc.as_ref());
        }

        modules
    }
}

impl Default for RpcModuleBuilder<(), (), (), (), ()> {
    fn default() -> Self {
        RpcModuleBuilder::new((), (), (), (), ())
    }
}

/// Bundles settings for modules
#[derive(Debug, Default, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub struct RpcModuleConfig {
    /// `eth` namespace settings
    eth: EthConfig,
}

// === impl RpcModuleConfig ===

impl RpcModuleConfig {
    /// Convenience method to create a new [RpcModuleConfigBuilder]
    pub fn builder() -> RpcModuleConfigBuilder {
        RpcModuleConfigBuilder::default()
    }
}

/// Configures [RpcModuleConfig]
#[derive(Default)]
pub struct RpcModuleConfigBuilder {
    eth: Option<EthConfig>,
}

// === impl RpcModuleConfigBuilder ===

impl RpcModuleConfigBuilder {
    /// Configures a custom eth namespace config
    pub fn eth(mut self, eth: EthConfig) -> Self {
        self.eth = Some(eth);
        self
    }

    /// Consumes the type and creates the [RpcModuleConfig]
    pub fn build(self) -> RpcModuleConfig {
        let RpcModuleConfigBuilder { eth } = self;
        RpcModuleConfig { eth: eth.unwrap_or_default() }
    }
}

/// Describes the modules that should be installed.
///
/// # Example
///
/// Create a [RpcModuleSelection] from a selection.
///
/// ```
/// use reth_rpc_builder::{RethRpcModule, RpcModuleSelection};
/// let config: RpcModuleSelection = vec![RethRpcModule::Eth].into();
/// ```
#[derive(Debug, Default, Clone, Eq, PartialEq)]
pub enum RpcModuleSelection {
    /// Use _all_ available modules.
    All,
    /// The default modules `eth`, `net`, `web3`
    #[default]
    Standard,
    /// Only use the configured modules.
    Selection(Vec<RethRpcModule>),
}

// === impl RpcModuleSelection ===

impl RpcModuleSelection {
    /// The standard modules to instantiate by default `eth`, `net`, `web3`
    pub const STANDARD_MODULES: [RethRpcModule; 3] =
        [RethRpcModule::Eth, RethRpcModule::Net, RethRpcModule::Web3];

    /// Returns a selection of [RethRpcModule] with all [RethRpcModule::VARIANTS].
    pub fn all_modules() -> Vec<RethRpcModule> {
        RpcModuleSelection::try_from_selection(RethRpcModule::VARIANTS.iter().copied())
            .expect("valid selection")
            .into_selection()
    }

    /// Creates a new [RpcModuleSelection::Selection] from the given items.
    ///
    /// # Example
    ///
    /// Create a selection from the [RethRpcModule] string identifiers
    ///
    /// ```
    ///  use reth_rpc_builder::{RethRpcModule, RpcModuleSelection};
    /// let selection = vec!["eth", "admin"];
    /// let config = RpcModuleSelection::try_from_selection(selection).unwrap();
    /// assert_eq!(config, RpcModuleSelection::Selection(vec![RethRpcModule::Eth, RethRpcModule::Admin]));
    /// ```
    pub fn try_from_selection<I, T>(selection: I) -> Result<Self, T::Error>
    where
        I: IntoIterator<Item = T>,
        T: TryInto<RethRpcModule>,
    {
        let selection =
            selection.into_iter().map(TryInto::try_into).collect::<Result<Vec<_>, _>>()?;
        Ok(RpcModuleSelection::Selection(selection))
    }

    /// Creates a new [RpcModule] based on the configured reth modules.
    ///
    /// Note: This will always create new instance of the module handlers and is therefor only
    /// recommended for launching standalone transports. If multiple transports need to be
    /// configured it's recommended to use the [RpcModuleBuilder].
    pub fn standalone_module<Client, Pool, Network, Tasks, Events>(
        &self,
        client: Client,
        pool: Pool,
        network: Network,
        executor: Tasks,
        events: Events,
        config: RpcModuleConfig,
    ) -> RpcModule<()>
    where
        Client: BlockProvider + StateProviderFactory + EvmEnvProvider + Clone + Unpin + 'static,
        Pool: TransactionPool + Clone + 'static,
        Network: NetworkInfo + Peers + Clone + 'static,
        Tasks: TaskSpawner + Clone + 'static,
        Events: ChainEventSubscriptions + Clone + 'static,
    {
        let mut registry = RethModuleRegistry::new(client, pool, network, executor, events, config);
        registry.module_for(self)
    }

    /// Returns an iterator over all configured [RethRpcModule]
    pub fn iter_selection(&self) -> Box<dyn Iterator<Item = RethRpcModule> + '_> {
        match self {
            RpcModuleSelection::All => Box::new(Self::all_modules().into_iter()),
            RpcModuleSelection::Standard => Box::new(Self::STANDARD_MODULES.iter().copied()),
            RpcModuleSelection::Selection(s) => Box::new(s.iter().copied()),
        }
    }

    /// Returns the list of configured [RethRpcModule]
    pub fn into_selection(self) -> Vec<RethRpcModule> {
        match self {
            RpcModuleSelection::All => Self::all_modules(),
            RpcModuleSelection::Selection(s) => s,
            RpcModuleSelection::Standard => Self::STANDARD_MODULES.to_vec(),
        }
    }
}

impl<I, T> From<I> for RpcModuleSelection
where
    I: IntoIterator<Item = T>,
    T: Into<RethRpcModule>,
{
    fn from(value: I) -> Self {
        RpcModuleSelection::Selection(value.into_iter().map(Into::into).collect())
    }
}

impl FromStr for RpcModuleSelection {
    type Err = ParseError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let modules = s.split(',');

        RpcModuleSelection::try_from_selection(modules)
    }
}

/// Represents RPC modules that are supported by reth
#[derive(
    Debug, Clone, Copy, Eq, PartialEq, Hash, AsRefStr, EnumVariantNames, EnumString, Deserialize,
)]
#[serde(rename_all = "snake_case")]
#[strum(serialize_all = "kebab-case")]
pub enum RethRpcModule {
    /// `admin_` module
    Admin,
    /// `debug_` module
    Debug,
    /// `eth_` module
    Eth,
    /// `net_` module
    Net,
    /// `trace_` module
    Trace,
    /// `web3_` module
    Web3,
}

impl fmt::Display for RethRpcModule {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.pad(self.as_ref())
    }
}

impl Serialize for RethRpcModule {
    fn serialize<S>(&self, s: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        s.serialize_str(self.as_ref())
    }
}

/// A Helper type the holds instances of the configured modules.
pub struct RethModuleRegistry<Client, Pool, Network, Tasks, Events> {
    client: Client,
    pool: Pool,
    network: Network,
    executor: Tasks,
    events: Events,
    /// Additional settings for handlers.
    config: RpcModuleConfig,
    /// Holds a clone of all the eth namespace handlers
    eth: Option<EthHandlers<Client, Pool, Network, Events>>,
    /// Contains the [Methods] of a module
    modules: HashMap<RethRpcModule, Methods>,
}

// === impl RethModuleRegistry ===

impl<Client, Pool, Network, Tasks, Events>
    RethModuleRegistry<Client, Pool, Network, Tasks, Events>
{
    /// Creates a new, empty instance.
    pub fn new(
        client: Client,
        pool: Pool,
        network: Network,
        executor: Tasks,
        events: Events,
        config: RpcModuleConfig,
    ) -> Self {
        Self {
            client,
            pool,
            network,
            eth: None,
            executor,
            modules: Default::default(),
            config,
            events,
        }
    }

    /// Returns all installed methods
    pub fn methods(&self) -> Vec<Methods> {
        self.modules.values().cloned().collect()
    }

    /// Returns a merged RpcModule
    pub fn module(&self) -> RpcModule<()> {
        let mut module = RpcModule::new(());
        for methods in self.modules.values().cloned() {
            module.merge(methods).expect("No conflicts");
        }
        module
    }
}

impl<Client, Pool, Network, Tasks, Events> RethModuleRegistry<Client, Pool, Network, Tasks, Events>
where
    Network: NetworkInfo + Peers + Clone + 'static,
{
    /// Register Admin Namespace
    pub fn register_admin(&mut self) -> &mut Self {
        self.modules
            .insert(RethRpcModule::Admin, AdminApi::new(self.network.clone()).into_rpc().into());
        self
    }

    /// Register Web3 Namespace
    pub fn register_web3(&mut self) -> &mut Self {
        self.modules
            .insert(RethRpcModule::Web3, Web3Api::new(self.network.clone()).into_rpc().into());
        self
    }
}

impl<Client, Pool, Network, Tasks, Events> RethModuleRegistry<Client, Pool, Network, Tasks, Events>
where
    Client: BlockProvider + StateProviderFactory + EvmEnvProvider + Clone + Unpin + 'static,
    Pool: TransactionPool + Clone + 'static,
    Network: NetworkInfo + Peers + Clone + 'static,
    Tasks: TaskSpawner + Clone + 'static,
    Events: ChainEventSubscriptions + Clone + 'static,
{
    /// Register Eth Namespace
    pub fn register_eth(&mut self) -> &mut Self {
        let eth_api = self.eth_api();
        self.modules.insert(RethRpcModule::Eth, eth_api.into_rpc().into());
        self
    }

    /// Register Debug Namespace
    pub fn register_debug(&mut self) -> &mut Self {
        let eth_api = self.eth_api();
        self.modules.insert(
            RethRpcModule::Debug,
            DebugApi::new(self.client.clone(), eth_api).into_rpc().into(),
        );
        self
    }

    /// Register Net Namespace
    pub fn register_net(&mut self) -> &mut Self {
        let eth_api = self.eth_api();
        self.modules.insert(
            RethRpcModule::Net,
            NetApi::new(self.network.clone(), eth_api).into_rpc().into(),
        );
        self
    }

    /// Helper function to create a [RpcModule] if it's not `None`
    fn maybe_module(&mut self, config: Option<&RpcModuleSelection>) -> Option<RpcModule<()>> {
        let config = config?;
        let module = self.module_for(config);
        Some(module)
    }

    /// Populates a new [RpcModule] based on the selected [RethRpcModule]s in the given
    /// [RpcModuleSelection]
    pub fn module_for(&mut self, config: &RpcModuleSelection) -> RpcModule<()> {
        let mut module = RpcModule::new(());
        let all_methods = self.reth_methods(config.iter_selection());
        for methods in all_methods {
            module.merge(methods).expect("No conflicts");
        }
        module
    }

    /// Returns the [Methods] for the given [RethRpcModule]
    ///
    /// If this is the first time the namespace is requested, a new instance of API implementation
    /// will be created.
    pub fn reth_methods(
        &mut self,
        namespaces: impl Iterator<Item = RethRpcModule>,
    ) -> Vec<Methods> {
        let EthHandlers { api: eth_api, cache: eth_cache, filter: eth_filter, pubsub: eth_pubsub } =
            self.with_eth(|eth| eth.clone());
        namespaces
            .map(|namespace| {
                self.modules
                    .entry(namespace)
                    .or_insert_with(|| match namespace {
                        RethRpcModule::Admin => {
                            AdminApi::new(self.network.clone()).into_rpc().into()
                        }
                        RethRpcModule::Debug => {
                            DebugApi::new(self.client.clone(), eth_api.clone()).into_rpc().into()
                        }
                        RethRpcModule::Eth => {
                            // merge all eth handlers
                            let mut module = eth_api.clone().into_rpc();
                            module.merge(eth_filter.clone().into_rpc()).expect("No conflicts");
                            module.merge(eth_pubsub.clone().into_rpc()).expect("No conflicts");

                            module.into()
                        }
                        RethRpcModule::Net => {
                            NetApi::new(self.network.clone(), eth_api.clone()).into_rpc().into()
                        }
                        RethRpcModule::Trace => {
                            TraceApi::new(self.client.clone(), eth_api.clone(), eth_cache.clone())
                                .into_rpc()
                                .into()
                        }
                        RethRpcModule::Web3 => Web3Api::new(self.network.clone()).into_rpc().into(),
                    })
                    .clone()
            })
            .collect::<Vec<_>>()
    }

    /// Returns the [EthStateCache] frontend
    ///
    /// This will spawn exactly one [EthStateCache] service if this is the first time the cache is
    /// requested.
    pub fn eth_cache(&mut self) -> EthStateCache {
        self.with_eth(|handlers| handlers.cache.clone())
    }

    /// Creates the [EthHandlers] type the first time this is called.
    fn with_eth<F, R>(&mut self, f: F) -> R
    where
        F: FnOnce(&EthHandlers<Client, Pool, Network, Events>) -> R,
    {
        if self.eth.is_none() {
            let cache = EthStateCache::spawn_with(
                self.client.clone(),
                self.config.eth.cache.clone(),
                self.executor.clone(),
            );
            let api = EthApi::new(
                self.client.clone(),
                self.pool.clone(),
                self.network.clone(),
                cache.clone(),
            );
            let filter = EthFilter::new(self.client.clone(), self.pool.clone());

            let pubsub = EthPubSub::new(
                self.client.clone(),
                self.pool.clone(),
                self.events.clone(),
                self.network.clone(),
            );

            let eth = EthHandlers { api, cache, filter, pubsub };
            self.eth = Some(eth);
        }
        f(self.eth.as_ref().expect("exists; qed"))
    }

    /// Returns the configured [EthApi] or creates it if it does not exist yet
    fn eth_api(&mut self) -> EthApi<Client, Pool, Network> {
        self.with_eth(|handlers| handlers.api.clone())
    }
}

/// A builder type for configuring and launching the servers that will handle RPC requests.
///
/// Supported server transports are:
///    - http
///    - ws
///    - ipc
///
/// Http and WS share the same settings: [`ServerBuilder`].
///
/// Once the [RpcModule] is built via [RpcModuleBuilder] the servers can be started, See also
/// [ServerBuilder::build] and [Server::start](jsonrpsee::server::Server::start).
#[derive(Default)]
pub struct RpcServerConfig {
    /// Configs for JSON-RPC Http.
    http_server_config: Option<ServerBuilder>,
    /// Allowed CORS Domains for http
    http_cors_domains: Option<String>,
    /// Address where to bind the http server to
    http_addr: Option<SocketAddr>,
    /// Configs for WS server
    ws_server_config: Option<ServerBuilder>,
    /// Allowed CORS Domains for ws.
    ws_cors_domains: Option<String>,
    /// Address where to bind the ws server to
    ws_addr: Option<SocketAddr>,
    /// Configs for JSON-RPC IPC server
    ipc_server_config: Option<IpcServerBuilder>,
    /// The Endpoint where to launch the ipc server
    ipc_endpoint: Option<Endpoint>,
}

impl fmt::Debug for RpcServerConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RpcServerConfig")
            .field("http_server_config", &self.http_server_config)
            .field("http_cors_domains", &self.http_cors_domains)
            .field("http_addr", &self.http_addr)
            .field("ws_server_config", &self.ws_server_config)
            .field("ws_addr", &self.ws_addr)
            .field("ipc_server_config", &self.ipc_server_config)
            .field("ipc_endpoint", &self.ipc_endpoint.as_ref().map(|endpoint| endpoint.path()))
            .finish()
    }
}

/// === impl RpcServerConfig ===

impl RpcServerConfig {
    /// Creates a new config with only http set
    pub fn http(config: ServerBuilder) -> Self {
        Self::default().with_http(config)
    }

    /// Creates a new config with only ws set
    pub fn ws(config: ServerBuilder) -> Self {
        Self::default().with_ws(config)
    }

    /// Creates a new config with only ipc set
    pub fn ipc(config: IpcServerBuilder) -> Self {
        Self::default().with_ipc(config)
    }

    /// Configures the http server
    ///
    /// Note: this always configures an [EthSubscriptionIdProvider] [IdProvider] for convenience.
    /// To set a custom [IdProvider], please use [Self::with_id_provider].
    pub fn with_http(mut self, config: ServerBuilder) -> Self {
        self.http_server_config =
            Some(config.http_only().set_id_provider(EthSubscriptionIdProvider::default()));
        self
    }

    /// Configure the cors domains for http _and_ ws
    pub fn with_cors(self, cors_domain: Option<String>) -> Self {
        self.with_http_cors(cors_domain.clone()).with_ws_cors(cors_domain)
    }

    /// Configure the cors domains for HTTP
    pub fn with_http_cors(mut self, cors_domain: Option<String>) -> Self {
        self.http_cors_domains = cors_domain;
        self
    }

    /// Configure the cors domains for WS
    pub fn with_ws_cors(mut self, cors_domain: Option<String>) -> Self {
        self.ws_cors_domains = cors_domain;
        self
    }

    /// Configures the ws server
    ///
    /// Note: this always configures an [EthSubscriptionIdProvider] [IdProvider] for convenience.
    /// To set a custom [IdProvider], please use [Self::with_id_provider].
    pub fn with_ws(mut self, config: ServerBuilder) -> Self {
        self.ws_server_config =
            Some(config.ws_only().set_id_provider(EthSubscriptionIdProvider::default()));
        self
    }

    /// Configures the [SocketAddr] of the http server
    ///
    /// Default is [Ipv4Addr::UNSPECIFIED] and [DEFAULT_HTTP_RPC_PORT]
    pub fn with_http_address(mut self, addr: SocketAddr) -> Self {
        self.http_addr = Some(addr);
        self
    }

    /// Configures the [SocketAddr] of the ws server
    ///
    /// Default is [Ipv4Addr::UNSPECIFIED] and [DEFAULT_WS_RPC_PORT]
    pub fn with_ws_address(mut self, addr: SocketAddr) -> Self {
        self.ws_addr = Some(addr);
        self
    }

    /// Configures the ipc server
    ///
    /// Note: this always configures an [EthSubscriptionIdProvider] [IdProvider] for convenience.
    /// To set a custom [IdProvider], please use [Self::with_id_provider].
    pub fn with_ipc(mut self, config: IpcServerBuilder) -> Self {
        self.ipc_server_config = Some(config.set_id_provider(EthSubscriptionIdProvider::default()));
        self
    }

    /// Sets a custom [IdProvider] for all configured transports.
    ///
    /// By default all transports use [EthSubscriptionIdProvider]
    pub fn with_id_provider<I>(mut self, id_provider: I) -> Self
    where
        I: IdProvider + Clone + 'static,
    {
        if let Some(http) = self.http_server_config {
            self.http_server_config = Some(http.set_id_provider(id_provider.clone()));
        }
        if let Some(ws) = self.ws_server_config {
            self.ws_server_config = Some(ws.set_id_provider(id_provider.clone()));
        }
        if let Some(ipc) = self.ipc_server_config {
            self.ipc_server_config = Some(ipc.set_id_provider(id_provider));
        }

        self
    }

    /// Configures the endpoint of the ipc server
    ///
    /// Default is [DEFAULT_IPC_ENDPOINT]
    pub fn with_ipc_endpoint(mut self, path: impl Into<String>) -> Self {
        self.ipc_endpoint = Some(Endpoint::new(path.into()));
        self
    }

    /// Returns the [SocketAddr] of the http server
    pub fn http_address(&self) -> Option<SocketAddr> {
        self.http_addr
    }

    /// Returns the [SocketAddr] of the ws server
    pub fn ws_address(&self) -> Option<SocketAddr> {
        self.ws_addr
    }

    /// Returns the [Endpoint] of the ipc server
    pub fn ipc_endpoint(&self) -> Option<&Endpoint> {
        self.ipc_endpoint.as_ref()
    }

    /// Convenience function to do [RpcServerConfig::build] and [RpcServer::start] in one step
    pub async fn start(
        self,
        modules: TransportRpcModules<()>,
    ) -> Result<RpcServerHandle, RpcError> {
        self.build().await?.start(modules).await
    }

    /// Finalize the configuration of the server(s).
    ///
    /// This consumes the builder and returns a server.
    ///
    /// Note: The server ist not started and does nothing unless polled, See also [RpcServer::start]
    pub async fn build(self) -> Result<RpcServer, RpcError> {
        let mut server = RpcServer::empty();

        let http_socket_addr = self.http_addr.unwrap_or(SocketAddr::V4(SocketAddrV4::new(
            Ipv4Addr::UNSPECIFIED,
            DEFAULT_HTTP_RPC_PORT,
        )));

        if let Some(builder) = self.http_server_config {
            if let Some(cors) = self.http_cors_domains.as_deref().map(cors::create_cors_layer) {
                let cors = cors.map_err(|err| RpcError::Custom(err.to_string()))?;
                let middleware = tower::ServiceBuilder::new().layer(cors);
                let http_server =
                    builder.set_middleware(middleware).build(http_socket_addr).await?;
                server.http_local_addr = http_server.local_addr().ok();
                server.http = Some(WsHttpServer::WithCors(http_server));
            } else {
                let http_server = builder.build(http_socket_addr).await?;
                server.http_local_addr = http_server.local_addr().ok();
                server.http = Some(WsHttpServer::Plain(http_server));
            }
        }

        let ws_socket_addr = self.ws_addr.unwrap_or(SocketAddr::V4(SocketAddrV4::new(
            Ipv4Addr::UNSPECIFIED,
            DEFAULT_WS_RPC_PORT,
        )));

        if let Some(builder) = self.ws_server_config {
            if let Some(cors) = self.ws_cors_domains.as_deref().map(cors::create_cors_layer) {
                let cors = cors.map_err(|err| RpcError::Custom(err.to_string()))?;
                let middleware = tower::ServiceBuilder::new().layer(cors);
                let ws_server = builder.set_middleware(middleware).build(ws_socket_addr).await?;
                server.http_local_addr = ws_server.local_addr().ok();
                server.ws = Some(WsHttpServer::WithCors(ws_server));
            } else {
                let ws_server = builder.build(ws_socket_addr).await?;
                server.ws_local_addr = ws_server.local_addr().ok();
                server.ws = Some(WsHttpServer::Plain(ws_server));
            }
        }

        if let Some(builder) = self.ipc_server_config {
            let ipc_path = self
                .ipc_endpoint
                .unwrap_or_else(|| Endpoint::new(DEFAULT_IPC_ENDPOINT.to_string()));
            let ipc = builder.build(ipc_path.path())?;
            server.ipc = Some(ipc);
        }

        Ok(server)
    }
}

/// Holds modules to be installed per transport type
///
/// # Example
///
/// Configure a http transport only
///
/// ```
/// use reth_rpc_builder::{RethRpcModule, TransportRpcModuleConfig};
///  let config = TransportRpcModuleConfig::default()
///       .with_http([RethRpcModule::Eth, RethRpcModule::Admin]);
/// ```
#[derive(Debug, Clone, Default, Eq, PartialEq)]
pub struct TransportRpcModuleConfig {
    /// http module configuration
    http: Option<RpcModuleSelection>,
    /// ws module configuration
    ws: Option<RpcModuleSelection>,
    /// ipc module configuration
    ipc: Option<RpcModuleSelection>,
    /// Config for the modules
    config: Option<RpcModuleConfig>,
}

// === impl TransportRpcModuleConfig ===

impl TransportRpcModuleConfig {
    /// Creates a new config with only http set
    pub fn set_http(http: impl Into<RpcModuleSelection>) -> Self {
        Self::default().with_http(http)
    }

    /// Creates a new config with only ws set
    pub fn set_ws(ws: impl Into<RpcModuleSelection>) -> Self {
        Self::default().with_ws(ws)
    }

    /// Creates a new config with only ipc set
    pub fn set_ipc(ipc: impl Into<RpcModuleSelection>) -> Self {
        Self::default().with_ipc(ipc)
    }

    /// Sets the [RpcModuleSelection] for the http transport.
    pub fn with_http(mut self, http: impl Into<RpcModuleSelection>) -> Self {
        self.http = Some(http.into());
        self
    }

    /// Sets the [RpcModuleSelection] for the ws transport.
    pub fn with_ws(mut self, ws: impl Into<RpcModuleSelection>) -> Self {
        self.ws = Some(ws.into());
        self
    }

    /// Sets the [RpcModuleSelection] for the http transport.
    pub fn with_ipc(mut self, ipc: impl Into<RpcModuleSelection>) -> Self {
        self.ipc = Some(ipc.into());
        self
    }

    /// Sets a custom [RpcModuleConfig] for the configured modules.
    pub fn with_config(mut self, config: RpcModuleConfig) -> Self {
        self.config = Some(config);
        self
    }

    /// Returns true if no transports are configured
    pub fn is_empty(&self) -> bool {
        self.http.is_none() && self.ws.is_none() && self.ipc.is_none()
    }

    /// Returns the [RpcModuleSelection] for the http transport
    pub fn http(&self) -> Option<&RpcModuleSelection> {
        self.http.as_ref()
    }

    /// Returns the [RpcModuleSelection] for the ws transport
    pub fn ws(&self) -> Option<&RpcModuleSelection> {
        self.ws.as_ref()
    }

    /// Returns the [RpcModuleSelection] for the ipc transport
    pub fn ipc(&self) -> Option<&RpcModuleSelection> {
        self.ipc.as_ref()
    }
}

/// Holds installed modules per transport type.
#[derive(Debug, Default)]
pub struct TransportRpcModules<Context> {
    /// rpcs module for http
    http: Option<RpcModule<Context>>,
    /// rpcs module for ws
    ws: Option<RpcModule<Context>>,
    /// rpcs module for ipc
    ipc: Option<RpcModule<Context>>,
}

// === impl TransportRpcModules ===

impl TransportRpcModules<()> {
    /// Convenience function for starting a server
    pub async fn start_server(self, builder: RpcServerConfig) -> Result<RpcServerHandle, RpcError> {
        builder.start(self).await
    }
}

/// Container type for each transport ie. http, ws, and ipc server
pub struct RpcServer {
    /// The address of the http server
    http_local_addr: Option<SocketAddr>,
    /// The address of the ws server
    ws_local_addr: Option<SocketAddr>,
    /// http server
    http: Option<WsHttpServer>,
    /// ws server
    ws: Option<WsHttpServer>,
    /// ipc server
    ipc: Option<IpcServer>,
}
/// Http Servers Enum
pub enum WsHttpServer {
    /// Http server
    Plain(Server),
    /// Http server with cors
    WithCors(Server<Stack<CorsLayer, Identity>>),
}

// === impl RpcServer ===

impl RpcServer {
    fn empty() -> RpcServer {
        RpcServer { http_local_addr: None, ws_local_addr: None, http: None, ws: None, ipc: None }
    }

    /// Returns the [`SocketAddr`] of the http server if started.
    pub fn http_local_addr(&self) -> Option<SocketAddr> {
        self.http_local_addr
    }

    /// Returns the [`SocketAddr`] of the ws server if started.
    pub fn ws_local_addr(&self) -> Option<SocketAddr> {
        self.ws_local_addr
    }

    /// Returns the [`Endpoint`] of the ipc server if started.
    pub fn ipc_endpoint(&self) -> Option<&Endpoint> {
        self.ipc.as_ref().map(|ipc| ipc.endpoint())
    }

    /// Starts the configured server by spawning the servers on the tokio runtime.
    ///
    /// This returns an [RpcServerHandle] that's connected to the server task(s) until the server is
    /// stopped or the [RpcServerHandle] is dropped.
    #[instrument(name = "start", skip_all, fields(http = ?self.http_local_addr, ws = ?self.ws_local_addr, ipc = ?self.ipc_endpoint().map(|ipc|ipc.path())), target = "rpc", level = "TRACE")]
    pub async fn start(
        self,
        modules: TransportRpcModules<()>,
    ) -> Result<RpcServerHandle, RpcError> {
        trace!(target: "rpc", "staring RPC server");
        let TransportRpcModules { http, ws, ipc } = modules;
        let mut handle = RpcServerHandle {
            http_local_addr: self.http_local_addr,
            ws_local_addr: self.ws_local_addr,
            http: None,
            ws: None,
            ipc: None,
        };

        // Start all servers
        if let Some((server, module)) =
            self.http.and_then(|server| http.map(|module| (server, module)))
        {
            match server {
                WsHttpServer::Plain(server) => {
                    handle.http = Some(server.start(module)?);
                }
                WsHttpServer::WithCors(server) => {
                    handle.http = Some(server.start(module)?);
                }
            }
        }

        if let Some((server, module)) = self.ws.and_then(|server| ws.map(|module| (server, module)))
        {
            match server {
                WsHttpServer::Plain(server) => {
                    handle.ws = Some(server.start(module)?);
                }
                WsHttpServer::WithCors(server) => {
                    handle.ws = Some(server.start(module)?);
                }
            }
        }

        if let Some((server, module)) =
            self.ipc.and_then(|server| ipc.map(|module| (server, module)))
        {
            handle.ipc = Some(server.start(module).await?);
        }

        Ok(handle)
    }
}

impl fmt::Debug for RpcServer {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RpcServer")
            .field("http", &self.http.is_some())
            .field("ws", &self.ws.is_some())
            .field("ipc", &self.ipc.is_some())
            .finish()
    }
}

/// A handle to the spawned servers.
///
/// When this type is dropped or [RpcServerHandle::stop] has been called the server will be stopped.
#[derive(Clone)]
#[must_use = "Server stop if dropped"]
pub struct RpcServerHandle {
    /// The address of the http/ws server
    http_local_addr: Option<SocketAddr>,
    ws_local_addr: Option<SocketAddr>,
    http: Option<ServerHandle>,
    ws: Option<ServerHandle>,
    ipc: Option<ServerHandle>,
}

// === impl RpcServerHandle ===

impl RpcServerHandle {
    /// Returns the [`SocketAddr`] of the http server if started.
    pub fn http_local_addr(&self) -> Option<SocketAddr> {
        self.http_local_addr
    }

    /// Returns the [`SocketAddr`] of the ws server if started.
    pub fn ws_local_addr(&self) -> Option<SocketAddr> {
        self.ws_local_addr
    }

    /// Tell the server to stop without waiting for the server to stop.
    pub fn stop(self) -> Result<(), RpcError> {
        if let Some(handle) = self.http {
            handle.stop()?
        }

        if let Some(handle) = self.ws {
            handle.stop()?
        }

        if let Some(handle) = self.ipc {
            handle.stop()?
        }

        Ok(())
    }

    /// Returns the url to the http server
    pub fn http_url(&self) -> Option<String> {
        self.http_local_addr.map(|addr| format!("http://{addr}"))
    }

    /// Returns the url to the ws server
    pub fn ws_url(&self) -> Option<String> {
        self.ws_local_addr.map(|addr| format!("ws://{addr}"))
    }

    /// Returns a http client connected to the server.
    pub fn http_client(&self) -> Option<jsonrpsee::http_client::HttpClient> {
        let url = self.http_url()?;
        let client = jsonrpsee::http_client::HttpClientBuilder::default()
            .build(url)
            .expect("Failed to create http client");
        Some(client)
    }

    /// Returns a ws client connected to the server.
    pub async fn ws_client(&self) -> Option<jsonrpsee::ws_client::WsClient> {
        let url = self.ws_url()?;
        let client = jsonrpsee::ws_client::WsClientBuilder::default()
            .build(url)
            .await
            .expect("Failed to create ws client");
        Some(client)
    }
}

impl std::fmt::Debug for RpcServerHandle {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RpcServerHandle")
            .field("http", &self.http.is_some())
            .field("ws", &self.ws.is_some())
            .field("ipc", &self.ipc.is_some())
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_rpc_module_str() {
        macro_rules! assert_rpc_module {
            ($($s:expr => $v:expr,)*) => {
                $(
                    let val: RethRpcModule  = $s.parse().unwrap();
                    assert_eq!(val, $v);
                    assert_eq!(val.to_string().as_str(), $s);
                )*
            };
        }
        assert_rpc_module!
        (
                "admin" =>  RethRpcModule::Admin,
                "debug" =>  RethRpcModule::Debug,
                "eth" =>  RethRpcModule::Eth,
                "net" =>  RethRpcModule::Net,
                "trace" =>  RethRpcModule::Trace,
                "web3" =>  RethRpcModule::Web3,
            );
    }

    #[test]
    fn test_default_selection() {
        let selection = RpcModuleSelection::Standard.into_selection();
        assert_eq!(selection, vec![RethRpcModule::Eth, RethRpcModule::Net, RethRpcModule::Web3,])
    }

    #[test]
    fn test_create_rpc_module_config() {
        let selection = vec!["eth", "admin"];
        let config = RpcModuleSelection::try_from_selection(selection).unwrap();
        assert_eq!(
            config,
            RpcModuleSelection::Selection(vec![RethRpcModule::Eth, RethRpcModule::Admin])
        );
    }

    #[test]
    fn test_configure_transport_config() {
        let config = TransportRpcModuleConfig::default()
            .with_http([RethRpcModule::Eth, RethRpcModule::Admin]);
        assert_eq!(
            config,
            TransportRpcModuleConfig {
                http: Some(RpcModuleSelection::Selection(vec![
                    RethRpcModule::Eth,
                    RethRpcModule::Admin
                ])),
                ws: None,
                ipc: None,
                config: None,
            }
        )
    }
}
