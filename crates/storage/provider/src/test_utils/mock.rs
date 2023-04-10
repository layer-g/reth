use crate::{
    traits::ReceiptProvider, AccountProvider, BlockHashProvider, BlockIdProvider, BlockProvider,
    EvmEnvProvider, HeaderProvider, PostStateDataProvider, StateProvider, StateProviderBox,
    StateProviderFactory, TransactionsProvider,
};
use parking_lot::Mutex;
use reth_interfaces::Result;
use reth_primitives::{
    keccak256, Account, Address, Block, BlockHash, BlockId, BlockNumber, BlockNumberOrTag,
    Bytecode, Bytes, ChainInfo, Header, Receipt, StorageKey, StorageValue, TransactionMeta,
    TransactionSigned, TxHash, TxNumber, H256, U256,
};
use reth_revm_primitives::primitives::{BlockEnv, CfgEnv};
use std::{
    collections::{BTreeMap, HashMap},
    ops::RangeBounds,
    sync::Arc,
};

/// A mock implementation for Provider interfaces.
#[derive(Debug, Clone, Default)]
pub struct MockEthProvider {
    /// Local block store
    pub blocks: Arc<Mutex<HashMap<H256, Block>>>,
    /// Local header store
    pub headers: Arc<Mutex<HashMap<H256, Header>>>,
    /// Local account store
    pub accounts: Arc<Mutex<HashMap<Address, ExtendedAccount>>>,
}

/// An extended account for local store
#[derive(Debug, Clone)]
pub struct ExtendedAccount {
    account: Account,
    bytecode: Option<Bytecode>,
    storage: HashMap<StorageKey, StorageValue>,
}

impl ExtendedAccount {
    /// Create new instance of extended account
    pub fn new(nonce: u64, balance: U256) -> Self {
        Self {
            account: Account { nonce, balance, bytecode_hash: None },
            bytecode: None,
            storage: Default::default(),
        }
    }

    /// Set bytecode and bytecode hash on the extended account
    pub fn with_bytecode(mut self, bytecode: Bytes) -> Self {
        let hash = keccak256(&bytecode);
        self.account.bytecode_hash = Some(hash);
        self.bytecode = Some(Bytecode::new_raw(bytecode.into()));
        self
    }
}

impl MockEthProvider {
    /// Add block to local block store
    pub fn add_block(&self, hash: H256, block: Block) {
        self.add_header(hash, block.header.clone());
        self.blocks.lock().insert(hash, block);
    }

    /// Add multiple blocks to local block store
    pub fn extend_blocks(&self, iter: impl IntoIterator<Item = (H256, Block)>) {
        for (hash, block) in iter.into_iter() {
            self.add_header(hash, block.header.clone());
            self.add_block(hash, block)
        }
    }

    /// Add header to local header store
    pub fn add_header(&self, hash: H256, header: Header) {
        self.headers.lock().insert(hash, header);
    }

    /// Add multiple headers to local header store
    pub fn extend_headers(&self, iter: impl IntoIterator<Item = (H256, Header)>) {
        for (hash, header) in iter.into_iter() {
            self.add_header(hash, header)
        }
    }

    /// Add account to local account store
    pub fn add_account(&self, address: Address, account: ExtendedAccount) {
        self.accounts.lock().insert(address, account);
    }

    /// Add account to local account store
    pub fn extend_accounts(&self, iter: impl IntoIterator<Item = (Address, ExtendedAccount)>) {
        for (address, account) in iter.into_iter() {
            self.add_account(address, account)
        }
    }
}

impl HeaderProvider for MockEthProvider {
    fn header(&self, block_hash: &BlockHash) -> Result<Option<Header>> {
        let lock = self.headers.lock();
        Ok(lock.get(block_hash).cloned())
    }

    fn header_by_number(&self, num: u64) -> Result<Option<Header>> {
        let lock = self.headers.lock();
        Ok(lock.values().find(|h| h.number == num).cloned())
    }

    fn header_td(&self, hash: &BlockHash) -> Result<Option<U256>> {
        let lock = self.headers.lock();
        Ok(lock.get(hash).map(|target| {
            lock.values()
                .filter(|h| h.number < target.number)
                .fold(target.difficulty, |td, h| td + h.difficulty)
        }))
    }

    fn header_td_by_number(&self, number: BlockNumber) -> Result<Option<U256>> {
        let lock = self.headers.lock();
        let sum = lock
            .values()
            .filter(|h| h.number <= number)
            .fold(U256::ZERO, |td, h| td + h.difficulty);
        Ok(Some(sum))
    }

    fn headers_range(
        &self,
        range: impl RangeBounds<reth_primitives::BlockNumber>,
    ) -> Result<Vec<Header>> {
        let lock = self.headers.lock();

        let mut headers: Vec<_> =
            lock.values().filter(|header| range.contains(&header.number)).cloned().collect();
        headers.sort_by_key(|header| header.number);

        Ok(headers)
    }
}

impl TransactionsProvider for MockEthProvider {
    fn transaction_id(&self, _tx_hash: TxHash) -> Result<Option<TxNumber>> {
        todo!()
    }

    fn transaction_by_id(&self, _id: TxNumber) -> Result<Option<TransactionSigned>> {
        Ok(None)
    }

    fn transaction_by_hash(&self, hash: TxHash) -> Result<Option<TransactionSigned>> {
        Ok(self
            .blocks
            .lock()
            .iter()
            .find_map(|(_, block)| block.body.iter().find(|tx| tx.hash == hash).cloned()))
    }

    fn transaction_by_hash_with_meta(
        &self,
        _hash: TxHash,
    ) -> Result<Option<(TransactionSigned, TransactionMeta)>> {
        Ok(None)
    }

    fn transaction_block(&self, _id: TxNumber) -> Result<Option<BlockNumber>> {
        unimplemented!()
    }

    fn transactions_by_block(&self, id: BlockId) -> Result<Option<Vec<TransactionSigned>>> {
        Ok(self.block(id)?.map(|b| b.body))
    }

    fn transactions_by_block_range(
        &self,
        range: impl RangeBounds<reth_primitives::BlockNumber>,
    ) -> Result<Vec<Vec<TransactionSigned>>> {
        // init btreemap so we can return in order
        let mut map = BTreeMap::new();
        for (_, block) in self.blocks.lock().iter() {
            if range.contains(&block.number) {
                map.insert(block.number, block.body.clone());
            }
        }

        Ok(map.into_values().collect())
    }
}

impl ReceiptProvider for MockEthProvider {
    fn receipt(&self, _id: TxNumber) -> Result<Option<Receipt>> {
        Ok(None)
    }

    fn receipt_by_hash(&self, _hash: TxHash) -> Result<Option<Receipt>> {
        Ok(None)
    }

    fn receipts_by_block(&self, _block: BlockId) -> Result<Option<Vec<Receipt>>> {
        Ok(None)
    }
}

impl BlockHashProvider for MockEthProvider {
    fn block_hash(&self, number: u64) -> Result<Option<H256>> {
        let lock = self.blocks.lock();

        let hash = lock.iter().find_map(|(hash, b)| (b.number == number).then_some(*hash));
        Ok(hash)
    }

    fn canonical_hashes_range(&self, start: BlockNumber, end: BlockNumber) -> Result<Vec<H256>> {
        let range = start..end;
        let lock = self.blocks.lock();

        let mut hashes: Vec<_> =
            lock.iter().filter(|(_, block)| range.contains(&block.number)).collect();
        hashes.sort_by_key(|(_, block)| block.number);

        Ok(hashes.into_iter().map(|(hash, _)| *hash).collect())
    }
}

impl BlockIdProvider for MockEthProvider {
    fn chain_info(&self) -> Result<ChainInfo> {
        let lock = self.headers.lock();
        Ok(lock
            .iter()
            .max_by_key(|h| h.1.number)
            .map(|(hash, header)| ChainInfo {
                best_hash: *hash,
                best_number: header.number,
                last_finalized: None,
                safe_finalized: None,
            })
            .expect("provider is empty"))
    }

    fn block_number(&self, hash: H256) -> Result<Option<reth_primitives::BlockNumber>> {
        let lock = self.blocks.lock();
        let num = lock.iter().find_map(|(h, b)| (*h == hash).then_some(b.number));
        Ok(num)
    }
}

impl BlockProvider for MockEthProvider {
    fn block(&self, id: BlockId) -> Result<Option<Block>> {
        let lock = self.blocks.lock();
        match id {
            BlockId::Hash(hash) => Ok(lock.get(hash.as_ref()).cloned()),
            BlockId::Number(BlockNumberOrTag::Number(num)) => {
                Ok(lock.values().find(|b| b.number == num).cloned())
            }
            _ => {
                unreachable!("unused in network tests")
            }
        }
    }

    fn ommers(&self, _id: BlockId) -> Result<Option<Vec<Header>>> {
        Ok(None)
    }
}

impl AccountProvider for MockEthProvider {
    fn basic_account(&self, address: Address) -> Result<Option<Account>> {
        Ok(self.accounts.lock().get(&address).cloned().map(|a| a.account))
    }
}

impl StateProvider for MockEthProvider {
    fn storage(&self, account: Address, storage_key: StorageKey) -> Result<Option<StorageValue>> {
        let lock = self.accounts.lock();
        Ok(lock.get(&account).and_then(|account| account.storage.get(&storage_key)).cloned())
    }

    fn bytecode_by_hash(&self, code_hash: H256) -> Result<Option<Bytecode>> {
        let lock = self.accounts.lock();
        Ok(lock.values().find_map(|account| {
            match (account.account.bytecode_hash.as_ref(), account.bytecode.as_ref()) {
                (Some(bytecode_hash), Some(bytecode)) if *bytecode_hash == code_hash => {
                    Some(bytecode.clone())
                }
                _ => None,
            }
        }))
    }

    fn proof(
        &self,
        _address: Address,
        _keys: &[H256],
    ) -> Result<(Vec<Bytes>, H256, Vec<Vec<Bytes>>)> {
        todo!()
    }
}

impl EvmEnvProvider for MockEthProvider {
    fn fill_env_at(
        &self,
        _cfg: &mut CfgEnv,
        _block_env: &mut BlockEnv,
        _at: BlockId,
    ) -> Result<()> {
        unimplemented!()
    }

    fn fill_env_with_header(
        &self,
        _cfg: &mut CfgEnv,
        _block_env: &mut BlockEnv,
        _header: &Header,
    ) -> Result<()> {
        unimplemented!()
    }

    fn fill_block_env_at(&self, _block_env: &mut BlockEnv, _at: BlockId) -> Result<()> {
        unimplemented!()
    }

    fn fill_block_env_with_header(
        &self,
        _block_env: &mut BlockEnv,
        _header: &Header,
    ) -> Result<()> {
        unimplemented!()
    }

    fn fill_cfg_env_at(&self, _cfg: &mut CfgEnv, _at: BlockId) -> Result<()> {
        unimplemented!()
    }

    fn fill_cfg_env_with_header(&self, _cfg: &mut CfgEnv, _header: &Header) -> Result<()> {
        unimplemented!()
    }
}

impl StateProviderFactory for MockEthProvider {
    fn latest(&self) -> Result<StateProviderBox<'_>> {
        Ok(Box::new(self.clone()))
    }

    fn history_by_block_number(
        &self,
        _block: reth_primitives::BlockNumber,
    ) -> Result<StateProviderBox<'_>> {
        todo!()
    }

    fn history_by_block_hash(&self, _block: BlockHash) -> Result<StateProviderBox<'_>> {
        todo!()
    }

    fn pending<'a>(
        &'a self,
        _post_state_data: Box<dyn PostStateDataProvider + 'a>,
    ) -> Result<StateProviderBox<'a>> {
        todo!()
    }
}

impl StateProviderFactory for Arc<MockEthProvider> {
    fn latest(&self) -> Result<StateProviderBox<'_>> {
        Ok(Box::new(self.clone()))
    }

    fn history_by_block_number(
        &self,
        _block: reth_primitives::BlockNumber,
    ) -> Result<StateProviderBox<'_>> {
        todo!()
    }

    fn history_by_block_hash(&self, _block: BlockHash) -> Result<StateProviderBox<'_>> {
        todo!()
    }

    fn pending<'a>(
        &'a self,
        _post_state_data: Box<dyn PostStateDataProvider + 'a>,
    ) -> Result<StateProviderBox<'a>> {
        todo!()
    }
}
