use crate::{ExecInput, ExecOutput, Stage, StageError, StageId, UnwindInput, UnwindOutput};
use num_traits::Zero;
use reth_codecs::Compact;
use reth_db::{
    cursor::DbDupCursorRO,
    database::Database,
    models::TransitionIdAddress,
    tables,
    transaction::{DbTx, DbTxMut},
};
use reth_primitives::{keccak256, Address, StorageEntry, StorageHashingCheckpoint};
use reth_provider::Transaction;
use std::{collections::BTreeMap, fmt::Debug};
use tracing::*;

/// The [`StageId`] of the storage hashing stage.
pub const STORAGE_HASHING: StageId = StageId("StorageHashing");

/// Storage hashing stage hashes plain storage.
/// This is preparation before generating intermediate hashes and calculating Merkle tree root.
#[derive(Debug)]
pub struct StorageHashingStage {
    /// The threshold (in number of state transitions) for switching between incremental
    /// hashing and full storage hashing.
    pub clean_threshold: u64,
    /// The maximum number of slots to process before committing.
    pub commit_threshold: u64,
}

impl Default for StorageHashingStage {
    fn default() -> Self {
        Self { clean_threshold: 500_000, commit_threshold: 100_000 }
    }
}

impl StorageHashingStage {
    /// Saves the hashing progress
    pub fn save_checkpoint<DB: Database>(
        &mut self,
        tx: &Transaction<'_, DB>,
        checkpoint: StorageHashingCheckpoint,
    ) -> Result<(), StageError> {
        debug!(target: "sync::stages::storage_hashing::exec", checkpoint = ?checkpoint, "Saving inner storage hashing checkpoint");

        let mut buf = vec![];
        checkpoint.to_compact(&mut buf);

        Ok(tx.put::<tables::SyncStageProgress>(STORAGE_HASHING.0.into(), buf)?)
    }

    /// Gets the hashing progress
    pub fn get_checkpoint<DB: Database>(
        &self,
        tx: &Transaction<'_, DB>,
    ) -> Result<StorageHashingCheckpoint, StageError> {
        let buf =
            tx.get::<tables::SyncStageProgress>(STORAGE_HASHING.0.into())?.unwrap_or_default();

        if buf.is_empty() {
            return Ok(StorageHashingCheckpoint::default())
        }

        let (checkpoint, _) = StorageHashingCheckpoint::from_compact(&buf, buf.len());

        if checkpoint.address.is_some() {
            debug!(target: "sync::stages::storage_hashing::exec", checkpoint = ?checkpoint, "Continuing inner storage hashing checkpoint");
        }

        Ok(checkpoint)
    }
}

#[async_trait::async_trait]
impl<DB: Database> Stage<DB> for StorageHashingStage {
    /// Return the id of the stage
    fn id(&self) -> StageId {
        STORAGE_HASHING
    }

    /// Execute the stage.
    async fn execute(
        &mut self,
        tx: &mut Transaction<'_, DB>,
        input: ExecInput,
    ) -> Result<ExecOutput, StageError> {
        let stage_progress = input.stage_progress.unwrap_or_default();
        let previous_stage_progress = input.previous_stage_progress();

        // read storage changeset, merge it into one changeset and calculate storage hashes.
        let from_transition = tx.get_block_transition(stage_progress)?;
        let to_transition = tx.get_block_transition(previous_stage_progress)?;

        // if there are more blocks then threshold it is faster to go over Plain state and hash all
        // account otherwise take changesets aggregate the sets and apply hashing to
        // AccountHashing table. Also, if we start from genesis, we need to hash from scratch, as
        // genesis accounts are not in changeset, along with their storages.
        if to_transition - from_transition > self.clean_threshold || stage_progress == 0 {
            let mut checkpoint = self.get_checkpoint(tx)?;

            if checkpoint.address.is_none() ||
                // Checkpoint is no longer valid if the range of transitions changed. 
                // An already hashed storage may have been changed with the new range, and therefore should be hashed again. 
                checkpoint.to != to_transition ||
                checkpoint.from != from_transition
            {
                tx.clear::<tables::HashedStorage>()?;

                checkpoint = StorageHashingCheckpoint::default();
                self.save_checkpoint(tx, checkpoint)?;
            }

            let mut current_key = checkpoint.address.take();
            let mut current_subkey = checkpoint.storage.take();
            let mut keccak_address = None;

            let mut hashed_batch = BTreeMap::new();
            let mut remaining = self.commit_threshold as usize;
            {
                let mut storage = tx.cursor_dup_read::<tables::PlainStorageState>()?;
                while !remaining.is_zero() {
                    hashed_batch.extend(
                        storage
                            .walk_dup(current_key, current_subkey)?
                            .take(remaining)
                            .map(|res| {
                                res.map(|(address, slot)| {
                                    // Address caching for the first iteration when current_key
                                    // is None
                                    let keccak_address =
                                        if let Some(keccak_address) = keccak_address {
                                            keccak_address
                                        } else {
                                            keccak256(address)
                                        };

                                    // TODO cache map keccak256(slot.key) ?
                                    ((keccak_address, keccak256(slot.key)), slot.value)
                                })
                            })
                            .collect::<Result<BTreeMap<_, _>, _>>()?,
                    );

                    remaining = self.commit_threshold as usize - hashed_batch.len();

                    if let Some((address, slot)) = storage.next_dup()? {
                        // There's still some remaining elements on this key, so we need to save
                        // the cursor position for the next
                        // iteration

                        current_key = Some(address);
                        current_subkey = Some(slot.key);
                    } else {
                        // Go to the next key
                        current_key = storage.next_no_dup()?.map(|(key, _)| key);
                        current_subkey = None;

                        // Cache keccak256(address) for the next key if it exists
                        if let Some(address) = current_key {
                            keccak_address = Some(keccak256(address));
                        } else {
                            // We have reached the end of table
                            break
                        }
                    }
                }
            }

            // iterate and put presorted hashed slots
            hashed_batch.into_iter().try_for_each(|((addr, key), value)| {
                tx.put::<tables::HashedStorage>(addr, StorageEntry { key, value })
            })?;

            if let Some(address) = &current_key {
                checkpoint.address = Some(*address);
                checkpoint.storage = current_subkey;
                checkpoint.from = from_transition;
                checkpoint.to = to_transition;
            }

            self.save_checkpoint(tx, checkpoint)?;

            if current_key.is_some() {
                return Ok(ExecOutput { stage_progress, done: false })
            }
        } else {
            // Aggregate all transition changesets and and make list of storages that have been
            // changed.
            let lists =
                tx.get_addresses_and_keys_of_changed_storages(from_transition, to_transition)?;
            // iterate over plain state and get newest storage value.
            // Assumption we are okay with is that plain state represent
            // `previous_stage_progress` state.
            let storages = tx.get_plainstate_storages(lists.into_iter())?;
            tx.insert_storage_for_hashing(storages.into_iter())?;
        }

        info!(target: "sync::stages::hashing_storage", "Stage finished");
        Ok(ExecOutput { stage_progress: input.previous_stage_progress(), done: true })
    }

    /// Unwind the stage.
    async fn unwind(
        &mut self,
        tx: &mut Transaction<'_, DB>,
        input: UnwindInput,
    ) -> Result<UnwindOutput, StageError> {
        let from_transition_rev = tx.get_block_transition(input.unwind_to)?;
        let to_transition_rev = tx.get_block_transition(input.stage_progress)?;

        tx.unwind_storage_hashing(
            TransitionIdAddress((from_transition_rev, Address::zero()))..
                TransitionIdAddress((to_transition_rev, Address::zero())),
        )?;

        Ok(UnwindOutput { stage_progress: input.unwind_to })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_utils::{
        stage_test_suite_ext, ExecuteStageTestRunner, StageTestRunner, TestRunnerError,
        TestTransaction, UnwindStageTestRunner, PREV_STAGE_ID,
    };
    use reth_db::{
        cursor::{DbCursorRO, DbCursorRW},
        mdbx::{tx::Tx, WriteMap, RW},
        models::{StoredBlockBodyIndices, TransitionIdAddress},
    };
    use reth_interfaces::test_utils::generators::{
        random_block_range, random_contract_account_range,
    };
    use reth_primitives::{SealedBlock, StorageEntry, H256, U256};

    stage_test_suite_ext!(StorageHashingTestRunner, storage_hashing);

    /// Execute with low clean threshold so as to hash whole storage
    #[tokio::test]
    async fn execute_clean_storage_hashing() {
        let (previous_stage, stage_progress) = (500, 100);

        // Set up the runner
        let mut runner = StorageHashingTestRunner::default();

        // set low clean threshold so we hash the whole storage
        runner.set_clean_threshold(1);

        // set low commit threshold so we force each entry to be a tx.commit and make sure we don't
        // hang on one key. Seed execution inserts more than one storage entry per address.
        runner.set_commit_threshold(1);

        let input = ExecInput {
            previous_stage: Some((PREV_STAGE_ID, previous_stage)),
            stage_progress: Some(stage_progress),
        };

        runner.seed_execution(input).expect("failed to seed execution");

        loop {
            if let Ok(result) = runner.execute(input).await.unwrap() {
                if !result.done {
                    // Continue from checkpoint
                    continue
                } else {
                    assert!(result.stage_progress == previous_stage);

                    // Validate the stage execution
                    assert!(
                        runner.validate_execution(input, Some(result)).is_ok(),
                        "execution validation"
                    );

                    break
                }
            }
            panic!("Failed execution");
        }
    }

    struct StorageHashingTestRunner {
        tx: TestTransaction,
        commit_threshold: u64,
        clean_threshold: u64,
    }

    impl Default for StorageHashingTestRunner {
        fn default() -> Self {
            Self { tx: TestTransaction::default(), commit_threshold: 1000, clean_threshold: 1000 }
        }
    }

    impl StageTestRunner for StorageHashingTestRunner {
        type S = StorageHashingStage;

        fn tx(&self) -> &TestTransaction {
            &self.tx
        }

        fn stage(&self) -> Self::S {
            Self::S {
                commit_threshold: self.commit_threshold,
                clean_threshold: self.clean_threshold,
            }
        }
    }

    #[async_trait::async_trait]
    impl ExecuteStageTestRunner for StorageHashingTestRunner {
        type Seed = Vec<SealedBlock>;

        fn seed_execution(&mut self, input: ExecInput) -> Result<Self::Seed, TestRunnerError> {
            let stage_progress = input.stage_progress.unwrap_or_default();
            let end = input.previous_stage_progress() + 1;

            let n_accounts = 31;
            let mut accounts = random_contract_account_range(&mut (0..n_accounts));

            let blocks = random_block_range(stage_progress..end, H256::zero(), 0..3);

            self.tx.insert_headers(blocks.iter().map(|block| &block.header))?;

            let iter = blocks.iter();
            let (mut next_transition_id, mut next_tx_num) = (0, 0);
            let mut first_transition_id = next_transition_id;
            let mut first_tx_num = next_tx_num;
            for progress in iter {
                // Insert last progress data
                self.tx.commit(|tx| {
                    progress.body.iter().try_for_each(|transaction| {
                        tx.put::<tables::TxHashNumber>(transaction.hash(), next_tx_num)?;
                        tx.put::<tables::Transactions>(next_tx_num, transaction.clone())?;

                        let (addr, _) = accounts
                            .get_mut(rand::random::<usize>() % n_accounts as usize)
                            .unwrap();

                        for _ in 0..2 {
                            let new_entry = StorageEntry {
                                key: keccak256([rand::random::<u8>()]),
                                value: U256::from(rand::random::<u8>() % 30 + 1),
                            };
                            self.insert_storage_entry(
                                tx,
                                (next_transition_id, *addr).into(),
                                new_entry,
                                progress.header.number == stage_progress,
                            )?;
                        }

                        next_tx_num += 1;
                        next_transition_id += 1;
                        Ok(())
                    })?;

                    // Randomize rewards
                    let has_reward: bool = rand::random();
                    if has_reward {
                        self.insert_storage_entry(
                            tx,
                            (next_transition_id, Address::random()).into(),
                            StorageEntry {
                                key: keccak256("mining"),
                                value: U256::from(rand::random::<u32>()),
                            },
                            progress.header.number == stage_progress,
                        )?;
                        next_transition_id += 1;
                    }

                    let body = StoredBlockBodyIndices {
                        first_tx_num,
                        first_transition_id,
                        tx_count: progress.body.len() as u64,
                        has_block_change: has_reward,
                    };

                    first_transition_id = next_transition_id;
                    first_tx_num = next_tx_num;

                    tx.put::<tables::BlockBodyIndices>(progress.number, body)
                })?;
            }

            Ok(blocks)
        }

        fn validate_execution(
            &self,
            input: ExecInput,
            output: Option<ExecOutput>,
        ) -> Result<(), TestRunnerError> {
            if let Some(output) = output {
                let start_block = input.stage_progress.unwrap_or_default() + 1;
                let end_block = output.stage_progress;
                if start_block > end_block {
                    return Ok(())
                }
            }
            self.check_hashed_storage()
        }
    }

    impl UnwindStageTestRunner for StorageHashingTestRunner {
        fn validate_unwind(&self, input: UnwindInput) -> Result<(), TestRunnerError> {
            self.unwind_storage(input)?;
            self.check_hashed_storage()
        }
    }

    impl StorageHashingTestRunner {
        fn set_clean_threshold(&mut self, threshold: u64) {
            self.clean_threshold = threshold;
        }

        fn set_commit_threshold(&mut self, threshold: u64) {
            self.commit_threshold = threshold;
        }

        fn check_hashed_storage(&self) -> Result<(), TestRunnerError> {
            self.tx
                .query(|tx| {
                    let mut storage_cursor = tx.cursor_dup_read::<tables::PlainStorageState>()?;
                    let mut hashed_storage_cursor =
                        tx.cursor_dup_read::<tables::HashedStorage>()?;

                    let mut expected = 0;

                    while let Some((address, entry)) = storage_cursor.next()? {
                        let key = keccak256(entry.key);
                        let got =
                            hashed_storage_cursor.seek_by_key_subkey(keccak256(address), key)?;
                        assert_eq!(
                            got,
                            Some(StorageEntry { key, ..entry }),
                            "{expected}: {address:?}"
                        );
                        expected += 1;
                    }
                    let count = tx.cursor_dup_read::<tables::HashedStorage>()?.walk(None)?.count();

                    assert_eq!(count, expected);
                    Ok(())
                })
                .map_err(|e| e.into())
        }

        fn insert_storage_entry(
            &self,
            tx: &Tx<'_, RW, WriteMap>,
            tid_address: TransitionIdAddress,
            entry: StorageEntry,
            hash: bool,
        ) -> Result<(), reth_db::Error> {
            let mut storage_cursor = tx.cursor_dup_write::<tables::PlainStorageState>()?;
            let prev_entry =
                match storage_cursor.seek_by_key_subkey(tid_address.address(), entry.key)? {
                    Some(e) if e.key == entry.key => {
                        tx.delete::<tables::PlainStorageState>(tid_address.address(), Some(e))
                            .expect("failed to delete entry");
                        e
                    }
                    _ => StorageEntry { key: entry.key, value: U256::from(0) },
                };
            tx.put::<tables::PlainStorageState>(tid_address.address(), entry)?;

            if hash {
                let hashed_address = keccak256(tid_address.address());
                let hashed_entry = StorageEntry { key: keccak256(entry.key), value: entry.value };

                if let Some(e) = tx
                    .cursor_dup_write::<tables::HashedStorage>()?
                    .seek_by_key_subkey(hashed_address, hashed_entry.key)?
                    .filter(|e| e.key == hashed_entry.key)
                {
                    tx.delete::<tables::HashedStorage>(hashed_address, Some(e))
                        .expect("failed to delete entry");
                }

                tx.put::<tables::HashedStorage>(hashed_address, hashed_entry)?;
            }

            tx.put::<tables::StorageChangeSet>(tid_address, prev_entry)?;
            Ok(())
        }

        fn unwind_storage(&self, input: UnwindInput) -> Result<(), TestRunnerError> {
            tracing::debug!("unwinding storage...");
            let target_transition = self
                .tx
                .inner()
                .get_block_transition(input.unwind_to)
                .map_err(|e| TestRunnerError::Internal(Box::new(e)))?;

            self.tx.commit(|tx| {
                let mut storage_cursor = tx.cursor_dup_write::<tables::PlainStorageState>()?;
                let mut changeset_cursor = tx.cursor_dup_read::<tables::StorageChangeSet>()?;

                let mut rev_changeset_walker = changeset_cursor.walk_back(None)?;

                while let Some((tid_address, entry)) = rev_changeset_walker.next().transpose()? {
                    if tid_address.transition_id() < target_transition {
                        break
                    }

                    if storage_cursor
                        .seek_by_key_subkey(tid_address.address(), entry.key)?
                        .filter(|e| e.key == entry.key)
                        .is_some()
                    {
                        storage_cursor.delete_current()?;
                    }

                    if entry.value != U256::ZERO {
                        storage_cursor.upsert(tid_address.address(), entry)?;
                    }
                }
                Ok(())
            })?;
            Ok(())
        }
    }
}
