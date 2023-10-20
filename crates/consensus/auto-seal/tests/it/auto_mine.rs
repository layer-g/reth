//! auto-mine consensus integration test
use std::{sync::Arc, time::Duration};
use jsonrpsee::{http_client::HttpClientBuilder, rpc_params, core::client::ClientT};
use reth::{node::NodeCommand, runner::CliRunner, cli::{components::RethNodeComponents, ext::{RethNodeCommandConfig, NoArgs, NoArgsCliExt}}, tasks::TaskSpawner};
use reth_primitives::{ChainSpec, Genesis, hex, revm_primitives::FixedBytes};
use reth_provider::CanonStateSubscriptions;
use clap::Parser;
use reth_transaction_pool::TransactionPool;

#[derive(Debug)]
struct AutoMineConfig;

impl RethNodeCommandConfig for AutoMineConfig {
    fn on_components_initialized<Reth: RethNodeComponents>(
        &mut self,
        _components: &Reth,
    ) -> eyre::Result<()> {
        tokio::spawn(async move {
            println!("All components initialized");
        });
        Ok(())
    }
    fn on_node_started<Reth: RethNodeComponents>(
        &mut self,
        components: &Reth,
    ) -> eyre::Result<()> {
        let pool = components.pool();
        let mut canon_events = components.events().subscribe_to_canonical_state();

        components.task_executor().spawn_critical_blocking("rpc request", Box::pin(async move {
            // submit tx through rpc
            let raw_tx = "0x02f876820a28808477359400847735940082520894ab0840c0e43688012c1adb0f5e3fc665188f83d28a029d394a5d630544000080c080a0a044076b7e67b5deecc63f61a8d7913fab86ca365b344b5759d1fe3563b4c39ea019eab979dd000da04dfc72bb0377c092d30fd9e1cab5ae487de49586cc8b0090";
            let client = HttpClientBuilder::default().build("http://127.0.0.1:8545").expect("");
            let response: String = client.request("eth_sendRawTransaction", rpc_params![raw_tx]).await.expect("");
            let expected = "0xb1c6512f4fc202c04355fbda66755e0e344b152e633010e8fd75ecec09b63398";

            assert_eq!(&response, expected);

            // more than enough time for the next block
            let sleep = tokio::time::sleep(Duration::from_secs(15));
            tokio::pin!(sleep);

            // wait for canon event or timeout
            tokio::select! {
                _ = &mut sleep => {
                    panic!("Canon update took too long to arrive")
                }

                update = canon_events.recv() => {
                    let event = update.expect("canon events stream is still open");
                    let new_tip = event.tip();
                    let expected_tx_root: FixedBytes<32> = hex!("c79b5383458e63fb20c6a49d9ec7917195a59003a2af4b28a01d7c6fbbcd7e35").into();
                    assert_eq!(new_tip.transactions_root, expected_tx_root);
                    assert_eq!(new_tip.number, 1);
                    assert!(pool.pending_transactions().is_empty());
                }
            }
        }));
        Ok(())
    }

    fn on_rpc_server_started<Conf, Reth>(
        &mut self,
        config: &Conf,
        components: &Reth,
        rpc_components: reth::cli::components::RethRpcComponents<'_, Reth>,
        handles: reth::cli::components::RethRpcServerHandles,
    ) -> eyre::Result<()>
    where
        Conf: reth::cli::config::RethRpcConfig,
        Reth: RethNodeComponents,
    {
        let _ = config;
        let _ = components;
        let _ = rpc_components;
        let _ = handles;
        Ok(())
    }
}

#[test]
pub fn test_auto_mine() {
    reth_tracing::init_test_tracing();
    let temp_path = tempfile::TempDir::new()
        .expect("tempdir is okay")
        .into_path();

    let datadir = temp_path.to_str().expect("temp path is okay");

    let no_args = NoArgs::with(AutoMineConfig);
    let chain = custom_chain();
    let mut command = NodeCommand::<NoArgsCliExt<AutoMineConfig>>::parse_from([
    // let mut command = NodeCommand::<()>::parse_from([
        "reth",
        "--dev",
        "--datadir",
        datadir,
        "--debug.max-block",
        "1",
        "--debug.terminate",
    ])
    .with_ext::<NoArgsCliExt<AutoMineConfig>>(no_args);

    // use custom chain spec
    command.chain = chain;

    let runner = CliRunner::default();
    let node_command = runner.run_command_until_exit(|ctx| command.execute(ctx));
    assert!(node_command.is_ok())
}

fn custom_chain() -> Arc<ChainSpec> {
    let custom_genesis = r#"
{
    "nonce": "0x42",
    "timestamp": "0x0",
    "extraData": "0x5343",
    "gasLimit": "0x1388",
    "difficulty": "0x400000000",
    "mixHash": "0x0000000000000000000000000000000000000000000000000000000000000000",
    "coinbase": "0x0000000000000000000000000000000000000000",
    "alloc": {
        "0x6Be02d1d3665660d22FF9624b7BE0551ee1Ac91b": {
            "balance": "0x4a47e3c12448f4ad000000"
        }
    },
    "number": "0x0",
    "gasUsed": "0x0",
    "parentHash": "0x0000000000000000000000000000000000000000000000000000000000000000",
    "config": {
        "ethash": {},
        "chainId": 2600,
        "homesteadBlock": 0,
        "eip150Block": 0,
        "eip155Block": 0,
        "eip158Block": 0,
        "byzantiumBlock": 0,
        "constantinopleBlock": 0,
        "petersburgBlock": 0,
        "istanbulBlock": 0,
        "berlinBlock": 0,
        "londonBlock": 0,
        "terminalTotalDifficulty": 0,
        "terminalTotalDifficultyPassed": true,
        "shanghaiTime": 0
    }
}
"#;
    let genesis: Genesis = serde_json::from_str(custom_genesis).unwrap();
    Arc::new(genesis.into())
}
