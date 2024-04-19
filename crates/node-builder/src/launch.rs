use futures::Future;
use reth_node_api::FullNodeComponents;

use crate::NodeHandle;


/// Launch the node.
pub trait LaunchNode {
    /// Build components and launch the node.
    fn launch<Node: FullNodeComponents>() -> impl Future<Output = eyre::Result<NodeHandle<Node>>>;
}
