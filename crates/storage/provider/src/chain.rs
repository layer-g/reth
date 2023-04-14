//! Contains [Chain], a chain of blocks and their final state.

use crate::PostState;
use reth_interfaces::{executor::Error as ExecError, Error};
use reth_primitives::{
    BlockHash, BlockNumHash, BlockNumber, ForkBlock, Receipt, SealedBlock, SealedBlockWithSenders,
    TransitionId, TxHash,
};
use std::collections::BTreeMap;

/// A chain of blocks and their final state.
///
/// The chain contains the state of accounts after execution of its blocks,
/// changesets for those blocks (and their transactions), as well as the blocks themselves.
///
/// Used inside the BlockchainTree.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Chain {
    /// The state of accounts after execution of the blocks in this chain.
    ///
    /// This state also contains the individual changes that lead to the current state.
    pub state: PostState,
    /// The blocks in this chain.
    pub blocks: BTreeMap<BlockNumber, SealedBlockWithSenders>,
    /// A mapping of each block number in the chain to the highest transition ID in the chain's
    /// state after execution of the block.
    ///
    /// This is used to revert changes in the state until a certain block number when the chain is
    /// split.
    pub block_transitions: BTreeMap<BlockNumber, TransitionId>,
}

impl Chain {
    /// Get the blocks in this chain.
    pub fn blocks(&self) -> &BTreeMap<BlockNumber, SealedBlockWithSenders> {
        &self.blocks
    }

    /// Get post state of this chain
    pub fn state(&self) -> &PostState {
        &self.state
    }

    /// Return block number of the block hash.
    pub fn block_number(&self, block_hash: BlockHash) -> Option<BlockNumber> {
        self.blocks.iter().find_map(|(num, block)| (block.hash() == block_hash).then_some(*num))
    }

    /// Return block inner transition ids.
    pub fn block_transitions(&self) -> &BTreeMap<BlockNumber, TransitionId> {
        &self.block_transitions
    }

    /// Returns the block with matching hash.
    pub fn block(&self, block_hash: BlockHash) -> Option<&SealedBlock> {
        self.blocks
            .iter()
            .find_map(|(_num, block)| (block.hash() == block_hash).then_some(&block.block))
    }

    /// Return post state of the block at the `block_number` or None if block is not known
    pub fn state_at_block(&self, block_number: BlockNumber) -> Option<PostState> {
        let mut state = self.state.clone();
        if self.tip().number == block_number {
            return Some(state)
        }

        if let Some(&transition_id) = self.block_transitions.get(&block_number) {
            state.revert_to(transition_id);
            return Some(state)
        }

        None
    }

    /// Destructure the chain into its inner components, the blocks and the state.
    pub fn into_inner(self) -> (BTreeMap<BlockNumber, SealedBlockWithSenders>, PostState) {
        (self.blocks, self.state)
    }

    /// Get the block at which this chain forked.
    pub fn fork_block(&self) -> ForkBlock {
        let tip = self.first();
        ForkBlock { number: tip.number.saturating_sub(1), hash: tip.parent_hash }
    }

    /// Get the block number at which this chain forked.
    pub fn fork_block_number(&self) -> BlockNumber {
        self.first().number.saturating_sub(1)
    }

    /// Get the block hash at which this chain forked.
    pub fn fork_block_hash(&self) -> BlockHash {
        self.first().parent_hash
    }

    /// Get the first block in this chain.
    pub fn first(&self) -> &SealedBlockWithSenders {
        self.blocks.first_key_value().expect("Chain has at least one block for first").1
    }

    /// Get the tip of the chain.
    ///
    /// # Note
    ///
    /// Chains always have at least one block.
    pub fn tip(&self) -> &SealedBlockWithSenders {
        self.blocks.last_key_value().expect("Chain should have at least one block").1
    }

    /// Create new chain with given blocks and post state.
    pub fn new(blocks: Vec<(SealedBlockWithSenders, PostState)>) -> Self {
        let mut state = PostState::default();
        let mut block_transitions = BTreeMap::new();
        let mut block_num_hash = BTreeMap::new();
        for (block, block_state) in blocks.into_iter() {
            state.extend(block_state);
            block_transitions.insert(block.number, state.transitions_count());
            block_num_hash.insert(block.number, block);
        }

        Self { state, block_transitions, blocks: block_num_hash }
    }

    /// Merge two chains by appending the given chain into the current one.
    ///
    /// The state of accounts for this chain is set to the state of the newest chain.
    pub fn append_chain(&mut self, chain: Chain) -> Result<(), Error> {
        let chain_tip = self.tip();
        if chain_tip.hash != chain.fork_block_hash() {
            return Err(ExecError::AppendChainDoesntConnect {
                chain_tip: chain_tip.num_hash(),
                other_chain_fork: chain.fork_block().into_components(),
            }
            .into())
        }

        // Insert blocks from other chain
        self.blocks.extend(chain.blocks.into_iter());
        let current_transition_count = self.state.transitions_count();
        self.state.extend(chain.state);

        // Update the block transition mapping, shifting the transition ID by the current number of
        // transitions in *this* chain
        for (block_number, transition_id) in chain.block_transitions.iter() {
            self.block_transitions.insert(*block_number, transition_id + current_transition_count);
        }
        Ok(())
    }

    /// Get all receipts with attachment.
    ///
    /// Attachment includes block number, block hash, transaction hash and transaction index.
    pub fn receipts_with_attachment(&self) -> Vec<BlockReceipts> {
        let mut receipt_attch = Vec::new();
        let mut receipts = self.state().receipts().iter();
        for (block_num, block) in self.blocks().iter() {
            let block_num_hash = BlockNumHash::new(*block_num, block.hash());
            let mut tx_receipts = Vec::new();
            for tx in block.body.iter() {
                if let Some(receipt) = receipts.next() {
                    tx_receipts.push((tx.hash(), receipt.clone()));
                }
            }
            receipt_attch.push(BlockReceipts { block: block_num_hash, tx_receipts });
        }
        receipt_attch
    }

    /// Split this chain at the given block.
    ///
    /// The given block will be the first block in the first returned chain.
    ///
    /// If the given block is not found, [`ChainSplit::NoSplitPending`] is returned.
    /// Split chain at the number or hash, block with given number will be included at first chain.
    /// If any chain is empty (Does not have blocks) None will be returned.
    ///
    /// # Note
    ///
    /// The block number to transition ID mapping is only found in the second chain, making it
    /// impossible to perform any state reverts on the first chain.
    ///
    /// The second chain only contains the changes that were reverted on the first chain; however,
    /// it retains the up to date state as if the chains were one, i.e. the second chain is an
    /// extension of the first.
    pub fn split(mut self, split_at: SplitAt) -> ChainSplit {
        let chain_tip = *self.blocks.last_entry().expect("chain is never empty").key();
        let block_number = match split_at {
            SplitAt::Hash(block_hash) => {
                let Some(block_number) = self.block_number(block_hash) else { return ChainSplit::NoSplitPending(self)};
                // If block number is same as tip whole chain is becoming canonical.
                if block_number == chain_tip {
                    return ChainSplit::NoSplitCanonical(self)
                }
                block_number
            }
            SplitAt::Number(block_number) => {
                if block_number >= chain_tip {
                    return ChainSplit::NoSplitCanonical(self)
                }
                if block_number < *self.blocks.first_entry().expect("chain is never empty").key() {
                    return ChainSplit::NoSplitPending(self)
                }
                block_number
            }
        };

        let higher_number_blocks = self.blocks.split_off(&(block_number + 1));

        let mut canonical_state = std::mem::take(&mut self.state);
        let new_state = canonical_state.split_at(
            *self.block_transitions.get(&block_number).expect("Unknown block transition ID"),
        );
        self.state = new_state;

        ChainSplit::Split {
            canonical: Chain {
                state: canonical_state,
                block_transitions: BTreeMap::new(),
                blocks: self.blocks,
            },
            pending: Chain {
                state: self.state,
                block_transitions: self.block_transitions,
                blocks: higher_number_blocks,
            },
        }
    }
}

/// Used to hold receipts and their attachment.
#[derive(Default, Clone, Debug)]
pub struct BlockReceipts {
    /// Block identifier
    pub block: BlockNumHash,
    /// Transaction identifier and receipt.
    pub tx_receipts: Vec<(TxHash, Receipt)>,
}

/// Used in spliting the chain.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SplitAt {
    /// Split at block number.
    Number(BlockNumber),
    /// Split at block hash.
    Hash(BlockHash),
}

/// Result of spliting chain.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ChainSplit {
    /// Chain is not splited. Pending chain is returned.
    /// Given block split is higher than last block.
    /// Or in case of split by hash when hash is unknown.
    NoSplitPending(Chain),
    /// Chain is not splited. Canonical chain is returned.
    /// Given block split is lower than first block.
    NoSplitCanonical(Chain),
    /// Chain is splited in two.
    /// Given block split is contained in first chain.
    Split {
        /// Left contains lower block number that get canonicalized.
        /// And substate is empty and not usable.
        canonical: Chain,
        /// Right contains higher block number, that is still pending.
        /// And substate from original chain is moved here.
        pending: Chain,
    },
}

#[cfg(test)]
mod tests {
    use super::*;
    use reth_primitives::{Account, H160, H256};

    #[test]
    fn chain_append() {
        let block = SealedBlockWithSenders::default();
        let block1_hash = H256([0x01; 32]);
        let block2_hash = H256([0x02; 32]);
        let block3_hash = H256([0x03; 32]);
        let block4_hash = H256([0x04; 32]);

        let mut block1 = block.clone();
        let mut block2 = block.clone();
        let mut block3 = block.clone();
        let mut block4 = block;

        block1.block.header.hash = block1_hash;
        block2.block.header.hash = block2_hash;
        block3.block.header.hash = block3_hash;
        block4.block.header.hash = block4_hash;

        block3.block.header.header.parent_hash = block2_hash;

        let mut chain1 =
            Chain { blocks: BTreeMap::from([(1, block1), (2, block2)]), ..Default::default() };

        let chain2 =
            Chain { blocks: BTreeMap::from([(3, block3), (4, block4)]), ..Default::default() };

        assert_eq!(chain1.append_chain(chain2.clone()), Ok(()));

        // chain1 got changed so this will fail
        assert!(chain1.append_chain(chain2).is_err());
    }

    #[test]
    fn test_number_split() {
        let mut base_state = PostState::default();
        let account = Account { nonce: 10, ..Default::default() };
        base_state.create_account(H160([1; 20]), account);
        base_state.finish_transition();

        let mut block_state1 = PostState::default();
        block_state1.create_account(H160([2; 20]), Account::default());
        block_state1.finish_transition();

        let mut block_state2 = PostState::default();
        block_state2.create_account(H160([3; 20]), Account::default());
        block_state2.finish_transition();

        let mut block1 = SealedBlockWithSenders::default();
        let block1_hash = H256([15; 32]);
        block1.number = 1;
        block1.hash = block1_hash;
        block1.senders.push(H160([4; 20]));

        let mut block2 = SealedBlockWithSenders::default();
        let block2_hash = H256([16; 32]);
        block2.number = 2;
        block2.hash = block2_hash;
        block2.senders.push(H160([4; 20]));

        let chain = Chain::new(vec![
            (block1.clone(), block_state1.clone()),
            (block2.clone(), block_state2.clone()),
        ]);

        let mut split1_state = chain.state.clone();
        let split2_state = split1_state.split_at(*chain.block_transitions.get(&1).unwrap());

        let chain_split1 = Chain {
            state: split1_state,
            block_transitions: BTreeMap::new(),
            blocks: BTreeMap::from([(1, block1.clone())]),
        };

        let chain_split2 = Chain {
            state: split2_state,
            block_transitions: chain.block_transitions.clone(),
            blocks: BTreeMap::from([(2, block2.clone())]),
        };

        // return tip state
        assert_eq!(chain.state_at_block(block2.number), Some(chain.state.clone()));
        assert_eq!(chain.state_at_block(block1.number), Some(chain_split1.state.clone()));
        // state at unknown block
        assert_eq!(chain.state_at_block(100), None);

        // split in two
        assert_eq!(
            chain.clone().split(SplitAt::Hash(block1_hash)),
            ChainSplit::Split { canonical: chain_split1, pending: chain_split2 }
        );

        // split at unknown block hash
        assert_eq!(
            chain.clone().split(SplitAt::Hash(H256([100; 32]))),
            ChainSplit::NoSplitPending(chain.clone())
        );

        // split at higher number
        assert_eq!(
            chain.clone().split(SplitAt::Number(10)),
            ChainSplit::NoSplitCanonical(chain.clone())
        );

        // split at lower number
        assert_eq!(chain.clone().split(SplitAt::Number(0)), ChainSplit::NoSplitPending(chain));
    }
}
