//! Implementation of [`BlockchainTree`]
use chain::{BlockChainId, Chain, ForkBlock};
use reth_db::{cursor::DbCursorRO, database::Database, tables, transaction::DbTx};
use reth_interfaces::{consensus::Consensus, executor::Error as ExecError, Error};
use reth_primitives::{BlockHash, BlockNumber, SealedBlock, SealedBlockWithSenders};
use reth_provider::{
    providers::ChainState, ExecutorFactory, HeaderProvider, StateProviderFactory, Transaction,
};
use std::collections::{BTreeMap, HashMap};

pub mod block_indices;
use block_indices::BlockIndices;

pub mod chain;
use chain::{ChainSplit, SplitAt};

pub mod config;
use config::BlockchainTreeConfig;

pub mod externals;
use externals::TreeExternals;

#[cfg_attr(doc, aquamarine::aquamarine)]
/// Tree of chains and its identifications.
///
/// Mermaid flowchart represent all blocks that can appear in blockchain.
/// Green blocks belong to canonical chain and are saved inside database table, they are our main
/// chain. Pending blocks and sidechains are found in memory inside [`BlockchainTree`].
/// Both pending and sidechains have same mechanisms only difference is when they got committed to
/// database. For pending it is just append operation but for sidechains they need to move current
/// canonical blocks to BlockchainTree flush sidechain to the database to become canonical chain.
/// ```mermaid
/// flowchart BT
/// subgraph canonical chain
/// CanonState:::state
/// block0canon:::canon -->block1canon:::canon -->block2canon:::canon -->block3canon:::canon --> block4canon:::canon --> block5canon:::canon
/// end
/// block5canon --> block6pending1:::pending
/// block5canon --> block6pending2:::pending
/// subgraph sidechain2
/// S2State:::state
/// block3canon --> block4s2:::sidechain --> block5s2:::sidechain
/// end
/// subgraph sidechain1
/// S1State:::state
/// block2canon --> block3s1:::sidechain --> block4s1:::sidechain --> block5s1:::sidechain --> block6s1:::sidechain
/// end
/// classDef state fill:#1882C4
/// classDef canon fill:#8AC926
/// classDef pending fill:#FFCA3A
/// classDef sidechain fill:#FF595E
/// ```
///
///
/// main functions:
/// * [BlockchainTree::insert_block]: Connect block to chain, execute it and if valid insert block
///   inside tree.
/// * [BlockchainTree::finalize_block]: Remove chains that join to now finalized block, as chain
///   becomes invalid.
/// * [BlockchainTree::make_canonical]: Check if we have the hash of block that we want to finalize
///   and commit it to db. If we dont have the block, pipeline syncing should start to fetch the
///   blocks from p2p. Do reorg in tables if canonical chain if needed.
pub struct BlockchainTree<DB: Database, C: Consensus, EF: ExecutorFactory> {
    /// chains and present data
    chains: HashMap<BlockChainId, Chain>,
    /// Static blockchain id generator
    block_chain_id_generator: u64,
    /// Indices to block and their connection.
    block_indices: BlockIndices,
    /// Tree configuration.
    config: BlockchainTreeConfig,
    /// Externals
    externals: TreeExternals<DB, C, EF>,
}

/// From Engine API spec, block inclusion can be valid, accepted or invalid.
/// Invalid case is already covered by error but we needs to make distinction
/// between if it is valid (extends canonical chain) or just accepted (is side chain).
/// If we dont know the block parent we are returning Disconnected status
/// as we can't make a claim if block is valid or not.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum BlockStatus {
    /// If block validation is valid and block extends canonical chain.
    /// In BlockchainTree sense it forks on canonical tip.
    Valid,
    /// If block validation is valid but block does not extend canonical chain
    /// (It is side chain) or hasn't been fully validated but ancestors of a payload are known.
    Accepted,
    /// If blocks is not connected to canonical chain.
    Disconnected,
}

/// Helper structure that wraps chains and indices to search for block hash accross the chains.
pub struct BlockHashes<'a> {
    /// Chains
    pub chains: &'a mut HashMap<BlockChainId, Chain>,
    /// Indices
    pub indices: &'a BlockIndices,
}

impl<DB: Database, C: Consensus, EF: ExecutorFactory> BlockchainTree<DB, C, EF> {
    /// New blockchain tree
    pub fn new(
        externals: TreeExternals<DB, C, EF>,
        config: BlockchainTreeConfig,
    ) -> Result<Self, Error> {
        let max_reorg_depth = config.max_reorg_depth();

        let last_canonical_hashes = externals
            .db
            .tx()?
            .cursor_read::<tables::CanonicalHeaders>()?
            .walk_back(None)?
            .take((max_reorg_depth + config.num_of_additional_canonical_block_hashes()) as usize)
            .collect::<Result<Vec<(BlockNumber, BlockHash)>, _>>()?;

        // TODO(rakita) save last finalized block inside database but for now just take
        // tip-max_reorg_depth
        // task: https://github.com/paradigmxyz/reth/issues/1712
        let (last_finalized_block_number, _) =
            if last_canonical_hashes.len() > max_reorg_depth as usize {
                last_canonical_hashes[max_reorg_depth as usize]
            } else {
                // it is in reverse order from tip to N
                last_canonical_hashes.last().cloned().unwrap_or_default()
            };

        Ok(Self {
            externals,
            block_chain_id_generator: 0,
            chains: Default::default(),
            block_indices: BlockIndices::new(
                last_finalized_block_number,
                BTreeMap::from_iter(last_canonical_hashes.into_iter()),
            ),
            config,
        })
    }

    /// Fork side chain or append the block if parent is the top of the chain
    fn fork_side_chain(
        &mut self,
        block: SealedBlockWithSenders,
        chain_id: BlockChainId,
    ) -> Result<BlockStatus, Error> {
        let block_hashes = self.all_chain_hashes(chain_id);

        // get canonical fork.
        let canonical_fork =
            self.canonical_fork(chain_id).ok_or(ExecError::BlockChainIdConsistency { chain_id })?;

        // get chain that block needs to join to.
        let parent_chain = self
            .chains
            .get_mut(&chain_id)
            .ok_or(ExecError::BlockChainIdConsistency { chain_id })?;
        let chain_tip = parent_chain.tip().hash();

        let canonical_block_hashes = self.block_indices.canonical_chain();

        // get canonical tip
        let (_, canonical_tip_hash) =
            canonical_block_hashes.last_key_value().map(|(i, j)| (*i, *j)).unwrap_or_default();

        let db = self.externals.shareable_db();
        let provider = if canonical_fork.hash == canonical_tip_hash {
            ChainState::boxed(db.latest()?)
        } else {
            ChainState::boxed(db.history_by_block_number(canonical_fork.number)?)
        };

        // append the block if it is continuing the chain.
        if chain_tip == block.parent_hash {
            let block_hash = block.hash();
            let block_number = block.number;
            parent_chain.append_block(
                block,
                block_hashes,
                canonical_block_hashes,
                &provider,
                &self.externals.consensus,
                &self.externals.executor_factory,
            )?;
            drop(provider);
            self.block_indices.insert_non_fork_block(block_number, block_hash, chain_id);
            Ok(BlockStatus::Valid)
        } else {
            let chain = parent_chain.new_chain_fork(
                block,
                block_hashes,
                canonical_block_hashes,
                &provider,
                &self.externals.consensus,
                &self.externals.executor_factory,
            )?;
            // release the lifetime with a drop
            drop(provider);
            self.insert_chain(chain);
            Ok(BlockStatus::Accepted)
        }
    }

    /// Fork canonical chain by creating new chain
    fn fork_canonical_chain(
        &mut self,
        block: SealedBlockWithSenders,
    ) -> Result<BlockStatus, Error> {
        let canonical_block_hashes = self.block_indices.canonical_chain();
        let (_, canonical_tip) =
            canonical_block_hashes.last_key_value().map(|(i, j)| (*i, *j)).unwrap_or_default();

        // create state provider
        let db = self.externals.shareable_db();
        let parent_header = db
            .header(&block.parent_hash)?
            .ok_or(ExecError::CanonicalChain { block_hash: block.parent_hash })?;

        let block_status;
        let provider = if block.parent_hash == canonical_tip {
            block_status = BlockStatus::Valid;
            ChainState::boxed(db.latest()?)
        } else {
            block_status = BlockStatus::Accepted;
            ChainState::boxed(db.history_by_block_number(block.number - 1)?)
        };

        let parent_header = parent_header.seal(block.parent_hash);
        let chain = Chain::new_canonical_fork(
            &block,
            &parent_header,
            canonical_block_hashes,
            &provider,
            &self.externals.consensus,
            &self.externals.executor_factory,
        )?;
        drop(provider);
        self.insert_chain(chain);
        Ok(block_status)
    }

    /// Get all block hashes from chain that are not canonical. This is one time operation per
    /// block. Reason why this is not caches is to save memory.
    fn all_chain_hashes(&self, chain_id: BlockChainId) -> BTreeMap<BlockNumber, BlockHash> {
        // find chain and iterate over it,
        let mut chain_id = chain_id;
        let mut hashes = BTreeMap::new();
        loop {
            let Some(chain) = self.chains.get(&chain_id) else { return hashes };
            hashes.extend(chain.blocks().values().map(|b| (b.number, b.hash())));

            let fork_block = chain.fork_block_hash();
            if let Some(next_chain_id) = self.block_indices.get_blocks_chain_id(&fork_block) {
                chain_id = next_chain_id;
            } else {
                // if there is no fork block that point to other chains, break the loop.
                // it means that this fork joins to canonical block.
                break
            }
        }
        hashes
    }

    /// Getting the canonical fork would tell use what kind of Provider we should execute block on.
    /// If it is latest state provider or history state provider
    /// Return None if chain_id is not known.
    fn canonical_fork(&self, chain_id: BlockChainId) -> Option<ForkBlock> {
        let mut chain_id = chain_id;
        let mut fork;
        loop {
            // chain fork block
            fork = self.chains.get(&chain_id)?.fork_block();
            // get fork block chain
            if let Some(fork_chain_id) = self.block_indices.get_blocks_chain_id(&fork.hash) {
                chain_id = fork_chain_id;
                continue
            }
            break
        }
        if self.block_indices.canonical_hash(&fork.number) == Some(fork.hash) {
            Some(fork)
        } else {
            None
        }
    }

    /// Insert chain to tree and ties the blocks to it.
    /// Helper function that handles indexing and inserting.
    fn insert_chain(&mut self, chain: Chain) -> BlockChainId {
        let chain_id = self.block_chain_id_generator;
        self.block_chain_id_generator += 1;
        self.block_indices.insert_chain(chain_id, &chain);
        // add chain_id -> chain index
        self.chains.insert(chain_id, chain);
        chain_id
    }

    /// Insert block inside tree. recover transaction signers and
    /// internaly call [`BlockchainTree::insert_block_with_senders`] fn.
    pub fn insert_block(&mut self, block: SealedBlock) -> Result<BlockStatus, Error> {
        let block = block.seal_with_senders().ok_or(ExecError::SenderRecoveryError)?;
        self.insert_block_with_senders(&block)
    }

    /// Insert block with senders inside tree.
    /// Returns `true` if:
    /// 1. It is part of the blockchain tree
    /// 2. It is part of the canonical chain
    /// 3. Its parent is part of the blockchain tree and we can fork at the parent
    /// 4. Its parent is part of the canonical chain and we can fork at the parent
    /// Otherwise will return `false`, indicating that neither the block nor its parent
    /// is part of the chain or any sidechains. This means that if block becomes canonical
    /// we need to fetch the missing blocks over p2p.
    pub fn insert_block_with_senders(
        &mut self,
        block: &SealedBlockWithSenders,
    ) -> Result<BlockStatus, Error> {
        // check if block number is inside pending block slide
        let last_finalized_block = self.block_indices.last_finalized_block();
        if block.number <= last_finalized_block {
            return Err(ExecError::PendingBlockIsFinalized {
                block_number: block.number,
                block_hash: block.hash(),
                last_finalized: last_finalized_block,
            }
            .into())
        }

        // we will not even try to insert blocks that are too far in future.
        if block.number > last_finalized_block + self.config.max_blocks_in_chain() {
            return Err(ExecError::PendingBlockIsInFuture {
                block_number: block.number,
                block_hash: block.hash(),
                last_finalized: last_finalized_block,
            }
            .into())
        }

        // check if block known and is already inside Tree
        if let Some(chain_id) = self.block_indices.get_blocks_chain_id(&block.hash()) {
            let canonical_fork = self.canonical_fork(chain_id).expect("Chain id is valid");
            // if block chain extends canonical chain
            if canonical_fork == self.block_indices.canonical_tip() {
                return Ok(BlockStatus::Valid)
            } else {
                return Ok(BlockStatus::Accepted)
            }
        }

        // check if block is part of canonical chain
        if self.block_indices.canonical_hash(&block.number) == Some(block.hash()) {
            // block is part of canonical chain
            return Ok(BlockStatus::Valid)
        }

        // check if block parent can be found in Tree
        if let Some(parent_chain) = self.block_indices.get_blocks_chain_id(&block.parent_hash) {
            return self.fork_side_chain(block.clone(), parent_chain)
            // TODO save pending block to database
            // https://github.com/paradigmxyz/reth/issues/1713
        }

        // if not found, check if the parent can be found inside canonical chain.
        if Some(block.parent_hash) == self.block_indices.canonical_hash(&(block.number - 1)) {
            // create new chain that points to that block
            return self.fork_canonical_chain(block.clone())
            // TODO save pending block to database
            // https://github.com/paradigmxyz/reth/issues/1713
        }
        // NOTE: Block doesn't have a parent, and if we receive this block in `make_canonical`
        // function this could be a trigger to initiate p2p syncing, as we are missing the
        // parent.
        Ok(BlockStatus::Disconnected)
    }

    /// Do finalization of blocks. Remove them from tree
    pub fn finalize_block(&mut self, finalized_block: BlockNumber) {
        let mut remove_chains = self.block_indices.finalize_canonical_blocks(
            finalized_block,
            self.config.num_of_additional_canonical_block_hashes(),
        );

        while let Some(chain_id) = remove_chains.pop_first() {
            if let Some(chain) = self.chains.remove(&chain_id) {
                remove_chains.extend(self.block_indices.remove_chain(&chain));
            }
        }
    }

    /// Update canonical hashes. Reads last N canonical blocks from database and update all indices.
    pub fn restore_canonical_hashes(
        &mut self,
        last_finalized_block: BlockNumber,
    ) -> Result<(), Error> {
        self.finalize_block(last_finalized_block);

        let num_of_canonical_hashes =
            self.config.max_reorg_depth() + self.config.num_of_additional_canonical_block_hashes();

        let last_canonical_hashes = self
            .externals
            .db
            .tx()?
            .cursor_read::<tables::CanonicalHeaders>()?
            .walk_back(None)?
            .take(num_of_canonical_hashes as usize)
            .collect::<Result<BTreeMap<BlockNumber, BlockHash>, _>>()?;

        let mut remove_chains = self.block_indices.update_block_hashes(last_canonical_hashes);

        // remove all chains that got discarded
        while let Some(chain_id) = remove_chains.first() {
            if let Some(chain) = self.chains.remove(chain_id) {
                remove_chains.extend(self.block_indices.remove_chain(&chain));
            }
        }

        Ok(())
    }

    /// Split chain and return canonical part of it. Pending part is reinserted inside tree
    /// with same chain_id.
    fn split_chain(&mut self, chain_id: BlockChainId, chain: Chain, split_at: SplitAt) -> Chain {
        match chain.split(split_at) {
            ChainSplit::Split { canonical, pending } => {
                // rest of splited chain is inserted back with same chain_id.
                self.block_indices.insert_chain(chain_id, &pending);
                self.chains.insert(chain_id, pending);
                canonical
            }
            ChainSplit::NoSplitCanonical(canonical) => canonical,
            ChainSplit::NoSplitPending(_) => {
                panic!("Should not happen as block indices guarantee structure of blocks")
            }
        }
    }

    /// Make block and its parent canonical. Unwind chains to database if necessary.
    ///
    /// If block is already part of canonical chain return Ok.
    pub fn make_canonical(&mut self, block_hash: &BlockHash) -> Result<(), Error> {
        let chain_id = if let Some(chain_id) = self.block_indices.get_blocks_chain_id(block_hash) {
            chain_id
        } else {
            // If block is already canonical don't return error.
            if self.block_indices.is_block_hash_canonical(block_hash) {
                return Ok(())
            }
            return Err(ExecError::BlockHashNotFoundInChain { block_hash: *block_hash }.into())
        };
        let chain = self.chains.remove(&chain_id).expect("To be present");

        // we are spliting chain as there is possibility that only part of chain get canonicalized.
        let canonical = self.split_chain(chain_id, chain, SplitAt::Hash(*block_hash));

        let mut block_fork = canonical.fork_block();
        let mut block_fork_number = canonical.fork_block_number();
        let mut chains_to_promote = vec![canonical];

        // loop while fork blocks are found in Tree.
        while let Some(chain_id) = self.block_indices.get_blocks_chain_id(&block_fork.hash) {
            let chain = self.chains.remove(&chain_id).expect("To fork to be present");
            block_fork = chain.fork_block();
            let canonical = self.split_chain(chain_id, chain, SplitAt::Number(block_fork_number));
            block_fork_number = canonical.fork_block_number();
            chains_to_promote.push(canonical);
        }

        let old_tip = self.block_indices.canonical_tip();
        // Merge all chain into one chain.
        let mut new_canon_chain = chains_to_promote.pop().expect("There is at least one block");
        for chain in chains_to_promote.into_iter().rev() {
            new_canon_chain.append_chain(chain).expect("We have just build the chain.");
        }

        // update canonical index
        self.block_indices.canonicalize_blocks(new_canon_chain.blocks());

        // if joins to the tip
        if new_canon_chain.fork_block_hash() == old_tip.hash {
            // append to database
            self.commit_canonical(new_canon_chain)?;
        } else {
            // it forks to canonical block that is not the tip.

            let canon_fork = new_canon_chain.fork_block();
            // sanity check
            if self.block_indices.canonical_hash(&canon_fork.number) != Some(canon_fork.hash) {
                unreachable!("all chains should point to canonical chain.");
            }

            let old_canon_chain = self.revert_canonical(canon_fork.number)?;
            // commit new canonical chain.
            self.commit_canonical(new_canon_chain)?;
            // insert old canon chain
            self.insert_chain(old_canon_chain);
        }

        Ok(())
    }

    /// Commit chain for it to become canonical. Assume we are doing pending operation to db.
    fn commit_canonical(&mut self, chain: Chain) -> Result<(), Error> {
        let mut tx = Transaction::new(&self.externals.db)?;

        let new_tip = chain.tip().number;
        let (blocks, changesets, _) = chain.into_inner();
        for item in blocks.into_iter().zip(changesets.into_iter()) {
            let ((_, block), changeset) = item;
            tx.insert_block(block, self.externals.chain_spec.as_ref(), changeset)
                .map_err(|e| ExecError::CanonicalCommit { inner: e.to_string() })?;
        }
        // update pipeline progress.
        tx.update_pipeline_stages(new_tip)
            .map_err(|e| ExecError::PipelineStatusUpdate { inner: e.to_string() })?;

        tx.commit()?;

        Ok(())
    }

    /// Unwind tables and put it inside state
    pub fn unwind(&mut self, unwind_to: BlockNumber) -> Result<(), Error> {
        // nothing to be done if unwind_to is higher then the tip
        if self.block_indices.canonical_tip().number <= unwind_to {
            return Ok(())
        }
        // revert `N` blocks from current canonical chain and put them inside BlockchanTree
        let old_canon_chain = self.revert_canonical(unwind_to)?;

        // check if there is block in chain
        if old_canon_chain.blocks().is_empty() {
            return Ok(())
        }
        self.block_indices.unwind_canonical_chain(unwind_to);
        // insert old canonical chain to BlockchainTree.
        self.insert_chain(old_canon_chain);

        Ok(())
    }

    /// Revert canonical blocks from database and insert them to pending table
    /// Revert should be non inclusive, and revert_until should stay in db.
    /// Return the chain that represent reverted canonical blocks.
    fn revert_canonical(&mut self, revert_until: BlockNumber) -> Result<Chain, Error> {
        // read data that is needed for new sidechain

        let mut tx = Transaction::new(&self.externals.db)?;

        // read block and execution result from database. and remove traces of block from tables.
        let blocks_and_execution = tx
            .take_block_and_execution_range(
                self.externals.chain_spec.as_ref(),
                (revert_until + 1)..,
            )
            .map_err(|e| ExecError::CanonicalRevert { inner: e.to_string() })?;

        // update pipeline progress.
        tx.update_pipeline_stages(revert_until)
            .map_err(|e| ExecError::PipelineStatusUpdate { inner: e.to_string() })?;

        tx.commit()?;

        let chain = Chain::new(blocks_and_execution);

        Ok(chain)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use parking_lot::Mutex;
    use reth_db::{
        mdbx::{test_utils::create_test_rw_db, Env, WriteMap},
        transaction::DbTxMut,
    };
    use reth_interfaces::test_utils::TestConsensus;
    use reth_primitives::{
        hex_literal::hex, proofs::EMPTY_ROOT, ChainSpec, ChainSpecBuilder, H256, MAINNET,
    };
    use reth_provider::{
        execution_result::ExecutionResult, insert_block, test_utils::blocks::BlockChainTestData,
        BlockExecutor, StateProvider,
    };
    use std::{collections::HashSet, sync::Arc};

    #[derive(Clone)]
    struct TestFactory {
        exec_result: Arc<Mutex<Vec<ExecutionResult>>>,
        chain_spec: Arc<ChainSpec>,
    }

    impl TestFactory {
        fn new(chain_spec: Arc<ChainSpec>) -> Self {
            Self { exec_result: Arc::new(Mutex::new(Vec::new())), chain_spec }
        }

        fn extend(&self, exec_res: Vec<ExecutionResult>) {
            self.exec_result.lock().extend(exec_res.into_iter());
        }
    }

    struct TestExecutor(Option<ExecutionResult>);

    impl<SP: StateProvider> BlockExecutor<SP> for TestExecutor {
        fn execute(
            &mut self,
            _block: &reth_primitives::Block,
            _total_difficulty: reth_primitives::U256,
            _senders: Option<Vec<reth_primitives::Address>>,
        ) -> Result<ExecutionResult, ExecError> {
            self.0.clone().ok_or(ExecError::VerificationFailed)
        }

        fn execute_and_verify_receipt(
            &mut self,
            _block: &reth_primitives::Block,
            _total_difficulty: reth_primitives::U256,
            _senders: Option<Vec<reth_primitives::Address>>,
        ) -> Result<ExecutionResult, ExecError> {
            self.0.clone().ok_or(ExecError::VerificationFailed)
        }
    }

    impl ExecutorFactory for TestFactory {
        type Executor<T: StateProvider> = TestExecutor;

        fn with_sp<SP: StateProvider>(&self, _sp: SP) -> Self::Executor<SP> {
            let exec_res = self.exec_result.lock().pop();
            TestExecutor(exec_res)
        }

        fn chain_spec(&self) -> &ChainSpec {
            self.chain_spec.as_ref()
        }
    }

    fn setup_externals(
        exec_res: Vec<ExecutionResult>,
    ) -> TreeExternals<Arc<Env<WriteMap>>, Arc<TestConsensus>, TestFactory> {
        let db = create_test_rw_db();
        let consensus = Arc::new(TestConsensus::default());
        let chain_spec = Arc::new(
            ChainSpecBuilder::default()
                .chain(MAINNET.chain)
                .genesis(MAINNET.genesis.clone())
                .shanghai_activated()
                .build(),
        );
        let executor_factory = TestFactory::new(chain_spec.clone());
        executor_factory.extend(exec_res);

        TreeExternals::new(db, consensus, executor_factory, chain_spec)
    }

    fn setup_genesis<DB: Database>(db: DB, mut genesis: SealedBlock) {
        // insert genesis to db.

        genesis.header.header.number = 10;
        genesis.header.header.state_root = EMPTY_ROOT;
        let tx_mut = db.tx_mut().unwrap();

        insert_block(&tx_mut, genesis, None, false, Some((0, 0))).unwrap();

        // insert first 10 blocks
        for i in 0..10 {
            tx_mut.put::<tables::CanonicalHeaders>(i, H256([100 + i as u8; 32])).unwrap();
        }
        tx_mut.commit().unwrap();
    }

    /// Test data structure that will check tree internals
    #[derive(Default, Debug)]
    struct TreeTester {
        /// Number of chains
        chain_num: Option<usize>,
        /// Check block to chain index
        block_to_chain: Option<HashMap<BlockHash, BlockChainId>>,
        /// Check fork to child index
        fork_to_child: Option<HashMap<BlockHash, HashSet<BlockHash>>>,
    }

    impl TreeTester {
        fn with_chain_num(mut self, chain_num: usize) -> Self {
            self.chain_num = Some(chain_num);
            self
        }
        fn with_block_to_chain(mut self, block_to_chain: HashMap<BlockHash, BlockChainId>) -> Self {
            self.block_to_chain = Some(block_to_chain);
            self
        }
        fn with_fork_to_child(
            mut self,
            fork_to_child: HashMap<BlockHash, HashSet<BlockHash>>,
        ) -> Self {
            self.fork_to_child = Some(fork_to_child);
            self
        }

        fn assert<DB: Database, C: Consensus, EF: ExecutorFactory>(
            self,
            tree: &BlockchainTree<DB, C, EF>,
        ) {
            if let Some(chain_num) = self.chain_num {
                assert_eq!(tree.chains.len(), chain_num);
            }
            if let Some(block_to_chain) = self.block_to_chain {
                assert_eq!(*tree.block_indices.blocks_to_chain(), block_to_chain);
            }
            if let Some(fork_to_child) = self.fork_to_child {
                assert_eq!(*tree.block_indices.fork_to_child(), fork_to_child);
            }
        }
    }

    #[test]
    fn sanity_path() {
        let data = BlockChainTestData::default();
        let (mut block1, exec1) = data.blocks[0].clone();
        block1.number = 11;
        block1.state_root =
            H256(hex!("5d035ccb3e75a9057452ff060b773b213ec1fc353426174068edfc3971a0b6bd"));
        let (mut block2, exec2) = data.blocks[1].clone();
        block2.number = 12;
        block2.state_root =
            H256(hex!("90101a13dd059fa5cca99ed93d1dc23657f63626c5b8f993a2ccbdf7446b64f8"));

        // test pops execution results from vector, so order is from last to first.ß
        let externals = setup_externals(vec![exec2.clone(), exec1.clone(), exec2, exec1]);

        // last finalized block would be number 9.
        setup_genesis(externals.db.clone(), data.genesis);

        // make tree
        let config = BlockchainTreeConfig::new(1, 2, 3);
        let mut tree = BlockchainTree::new(externals, config).expect("failed to create tree");

        // genesis block 10 is already canonical
        assert_eq!(tree.make_canonical(&H256::zero()), Ok(()));

        // insert block2 hits max chain size
        assert_eq!(
            tree.insert_block_with_senders(&block2),
            Err(ExecError::PendingBlockIsInFuture {
                block_number: block2.number,
                block_hash: block2.hash(),
                last_finalized: 9,
            }
            .into())
        );

        // make genesis block 10 as finalized
        tree.finalize_block(10);

        // block 2 parent is not known.
        assert_eq!(tree.insert_block_with_senders(&block2), Ok(BlockStatus::Disconnected));

        // insert block1
        assert_eq!(tree.insert_block_with_senders(&block1), Ok(BlockStatus::Valid));
        // already inserted block will return true.
        assert_eq!(tree.insert_block_with_senders(&block1), Ok(BlockStatus::Valid));

        // insert block2
        assert_eq!(tree.insert_block_with_senders(&block2), Ok(BlockStatus::Valid));

        // Trie state:
        //      b2 (pending block)
        //      |
        //      |
        //      b1 (pending block)
        //    /
        //  /
        // g1 (canonical blocks)
        // |
        TreeTester::default()
            .with_chain_num(1)
            .with_block_to_chain(HashMap::from([(block1.hash, 0), (block2.hash, 0)]))
            .with_fork_to_child(HashMap::from([(block1.parent_hash, HashSet::from([block1.hash]))]))
            .assert(&tree);

        // make block1 canonical
        assert_eq!(tree.make_canonical(&block1.hash()), Ok(()));
        // make block2 canonical
        assert_eq!(tree.make_canonical(&block2.hash()), Ok(()));

        // Trie state:
        // b2 (canonical block)
        // |
        // |
        // b1 (canonical block)
        // |
        // |
        // g1 (canonical blocks)
        // |
        TreeTester::default()
            .with_chain_num(0)
            .with_block_to_chain(HashMap::from([]))
            .with_fork_to_child(HashMap::from([]))
            .assert(&tree);

        let mut block1a = block1.clone();
        let block1a_hash = H256([0x33; 32]);
        block1a.hash = block1a_hash;
        let mut block2a = block2.clone();
        let block2a_hash = H256([0x34; 32]);
        block2a.hash = block2a_hash;

        // reinsert two blocks that point to canonical chain
        assert_eq!(tree.insert_block_with_senders(&block1a), Ok(BlockStatus::Accepted));

        TreeTester::default()
            .with_chain_num(1)
            .with_block_to_chain(HashMap::from([(block1a_hash, 1)]))
            .with_fork_to_child(HashMap::from([(
                block1.parent_hash,
                HashSet::from([block1a_hash]),
            )]))
            .assert(&tree);

        assert_eq!(tree.insert_block_with_senders(&block2a), Ok(BlockStatus::Accepted));
        // Trie state:
        // b2   b2a (side chain)
        // |   /
        // | /
        // b1  b1a (side chain)
        // |  /
        // |/
        // g1 (10)
        // |
        TreeTester::default()
            .with_chain_num(2)
            .with_block_to_chain(HashMap::from([(block1a_hash, 1), (block2a_hash, 2)]))
            .with_fork_to_child(HashMap::from([
                (block1.parent_hash, HashSet::from([block1a_hash])),
                (block1.hash(), HashSet::from([block2a_hash])),
            ]))
            .assert(&tree);

        // make b2a canonical
        assert_eq!(tree.make_canonical(&block2a_hash), Ok(()));
        // Trie state:
        // b2a   b2 (side chain)
        // |   /
        // | /
        // b1  b1a (side chain)
        // |  /
        // |/
        // g1 (10)
        // |
        TreeTester::default()
            .with_chain_num(2)
            .with_block_to_chain(HashMap::from([(block1a_hash, 1), (block2.hash, 3)]))
            .with_fork_to_child(HashMap::from([
                (block1.parent_hash, HashSet::from([block1a_hash])),
                (block1.hash(), HashSet::from([block2.hash])),
            ]))
            .assert(&tree);

        assert_eq!(tree.make_canonical(&block1a_hash), Ok(()));
        // Trie state:
        //       b2a   b2 (side chain)
        //       |   /
        //       | /
        // b1a  b1 (side chain)
        // |  /
        // |/
        // g1 (10)
        // |
        TreeTester::default()
            .with_chain_num(2)
            .with_block_to_chain(HashMap::from([
                (block1.hash, 4),
                (block2a_hash, 4),
                (block2.hash, 3),
            ]))
            .with_fork_to_child(HashMap::from([
                (block1.parent_hash, HashSet::from([block1.hash])),
                (block1.hash(), HashSet::from([block2.hash])),
            ]))
            .assert(&tree);

        // make b2 canonical
        assert_eq!(tree.make_canonical(&block2.hash()), Ok(()));
        // Trie state:
        // b2   b2a (side chain)
        // |   /
        // | /
        // b1  b1a (side chain)
        // |  /
        // |/
        // g1 (10)
        // |
        TreeTester::default()
            .with_chain_num(2)
            .with_block_to_chain(HashMap::from([(block1a_hash, 5), (block2a_hash, 4)]))
            .with_fork_to_child(HashMap::from([
                (block1.parent_hash, HashSet::from([block1a_hash])),
                (block1.hash(), HashSet::from([block2a_hash])),
            ]))
            .assert(&tree);

        // finalize b1 that would make b1a removed from tree
        tree.finalize_block(11);
        // Trie state:
        // b2   b2a (side chain)
        // |   /
        // | /
        // b1 (canon)
        // |
        // g1 (10)
        // |
        TreeTester::default()
            .with_chain_num(1)
            .with_block_to_chain(HashMap::from([(block2a_hash, 4)]))
            .with_fork_to_child(HashMap::from([(block1.hash(), HashSet::from([block2a_hash]))]))
            .assert(&tree);

        // unwind canonical
        assert_eq!(tree.unwind(block1.number), Ok(()));
        // Trie state:
        //    b2   b2a (pending block)
        //   /    /
        //  /   /
        // /  /
        // b1 (canonical block)
        // |
        // |
        // g1 (canonical blocks)
        // |
        TreeTester::default()
            .with_chain_num(2)
            .with_block_to_chain(HashMap::from([(block2a_hash, 4), (block2.hash, 6)]))
            .with_fork_to_child(HashMap::from([(
                block1.hash(),
                HashSet::from([block2a_hash, block2.hash]),
            )]))
            .assert(&tree);

        // commit b2a
        assert_eq!(tree.make_canonical(&block2.hash), Ok(()));
        // Trie state:
        // b2   b2a (side chain)
        // |   /
        // | /
        // b1 (canon)
        // |
        // g1 (10)
        // |
        TreeTester::default()
            .with_chain_num(1)
            .with_block_to_chain(HashMap::from([(block2a_hash, 4)]))
            .with_fork_to_child(HashMap::from([(block1.hash(), HashSet::from([block2a_hash]))]))
            .assert(&tree);

        // update canonical block to b2, this would make b2a be removed
        assert_eq!(tree.restore_canonical_hashes(12), Ok(()));
        // Trie state:
        // b2 (canon)
        // |
        // b1 (canon)
        // |
        // g1 (10)
        // |
        TreeTester::default()
            .with_chain_num(0)
            .with_block_to_chain(HashMap::from([]))
            .with_fork_to_child(HashMap::from([]))
            .assert(&tree);
    }
}
