use crate::{BlockIdProvider, HeaderProvider, ReceiptProvider, TransactionsProvider};
use reth_interfaces::Result;
use reth_primitives::{Block, BlockId, BlockNumberOrTag, Header, H256};

/// Api trait for fetching `Block` related data.
#[auto_impl::auto_impl(&, Arc)]
pub trait BlockProvider:
    BlockIdProvider + HeaderProvider + TransactionsProvider + ReceiptProvider + Send + Sync
{
    /// Returns the block.
    ///
    /// Returns `None` if block is not found.
    fn block(&self, id: BlockId) -> Result<Option<Block>>;

    /// Returns the ommers/uncle headers of the given block.
    ///
    /// Returns `None` if block is not found.
    fn ommers(&self, id: BlockId) -> Result<Option<Vec<Header>>>;

    /// Returns the block. Returns `None` if block is not found.
    fn block_by_hash(&self, hash: H256) -> Result<Option<Block>> {
        self.block(hash.into())
    }

    /// Returns the block. Returns `None` if block is not found.
    fn block_by_number_or_tag(&self, num: BlockNumberOrTag) -> Result<Option<Block>> {
        self.block(num.into())
    }

    /// Returns the block. Returns `None` if block is not found.
    fn block_by_number(&self, num: u64) -> Result<Option<Block>> {
        self.block(num.into())
    }
}
