use super::PrefixSet;
use crate::Nibbles;
use derive_more::Deref;
use reth_db::{
    cursor::DbCursorRO,
    models::{AccountBeforeTx, TransitionIdAddress},
    tables,
    transaction::DbTx,
    Error,
};
use reth_primitives::{keccak256, Address, StorageEntry, TransitionId, H256};
use std::{collections::HashMap, ops::Range};

/// A wrapper around a database transaction that loads prefix sets within a given transition range.
#[derive(Deref)]
pub struct PrefixSetLoader<'a, TX>(&'a TX);

impl<'a, TX> PrefixSetLoader<'a, TX> {
    /// Create a new loader.
    pub fn new(tx: &'a TX) -> Self {
        Self(tx)
    }
}

impl<'a, 'b, TX> PrefixSetLoader<'a, TX>
where
    TX: DbTx<'b>,
{
    /// Load all account and storage changes for the given transition id range.
    pub fn load(
        self,
        tid_range: Range<TransitionId>,
    ) -> Result<(PrefixSet, HashMap<H256, PrefixSet>), Error> {
        // Initialize prefix sets.
        let mut account_prefix_set = PrefixSet::default();
        let mut storage_prefix_set: HashMap<H256, PrefixSet> = HashMap::default();

        // Walk account changeset and insert account prefixes.
        let mut account_cursor = self.cursor_read::<tables::AccountChangeSet>()?;
        for account_entry in account_cursor.walk_range(tid_range.clone())? {
            let (_, AccountBeforeTx { address, .. }) = account_entry?;
            account_prefix_set.insert(Nibbles::unpack(keccak256(address)));
        }

        // Walk storage changeset and insert storage prefixes as well as account prefixes if missing
        // from the account prefix set.
        let mut storage_cursor = self.cursor_dup_read::<tables::StorageChangeSet>()?;
        let start = TransitionIdAddress((tid_range.start, Address::zero()));
        let end = TransitionIdAddress((tid_range.end, Address::zero()));
        for storage_entry in storage_cursor.walk_range(start..end)? {
            let (TransitionIdAddress((_, address)), StorageEntry { key, .. }) = storage_entry?;
            let hashed_address = keccak256(address);
            account_prefix_set.insert(Nibbles::unpack(hashed_address));
            storage_prefix_set
                .entry(hashed_address)
                .or_default()
                .insert(Nibbles::unpack(keccak256(key)));
        }

        Ok((account_prefix_set, storage_prefix_set))
    }
}
