use reth_interfaces::Error;
use reth_primitives::{H160, H256, KECCAK_EMPTY, U256};
use reth_provider::StateProvider;
use revm::{
    db::{CacheDB, DatabaseRef},
    interpreter::analysis::to_analysed,
    primitives::{AccountInfo, Bytecode},
};
use schnellru::{ByLength, LruMap};
use std::sync::{Arc, RwLock};

/// SubState of database. Uses revm internal cache with binding to reth StateProvider trait.
pub type SubState<DB> = CacheDB<State<DB>>;

/// Wrapper around StateProvider that implements revm database trait
pub struct State<DB: StateProvider> {
    inner: DB,
    lru: Arc<RwLock<LruMap<H256, Bytecode>>>,
}

impl<DB: StateProvider> State<DB> {
    /// Create new State with generic StateProvider.
    pub fn new(db: DB) -> Self {
        Self { inner: db, lru: Arc::new(RwLock::new(LruMap::new(ByLength::new(1024)))) }
    }

    pub fn new_with_lru(db: DB, lru: Arc<RwLock<LruMap<H256, Bytecode>>>) -> Self {
        Self { inner: db, lru }
    }

    /// Return inner state reference
    pub fn state(&self) -> &DB {
        &self.inner
    }

    /// Return inner state mutable reference
    pub fn state_mut(&mut self) -> &mut DB {
        &mut self.inner
    }

    /// Consume State and return inner StateProvider.
    pub fn into_inner(self) -> DB {
        self.inner
    }
}

impl<DB: StateProvider> DatabaseRef for State<DB> {
    type Error = Error;

    fn basic(&self, address: H160) -> Result<Option<AccountInfo>, Self::Error> {
        Ok(self.inner.basic_account(address)?.map(|account| AccountInfo {
            balance: account.balance,
            nonce: account.nonce,
            code_hash: account.bytecode_hash.unwrap_or(KECCAK_EMPTY),
            code: None,
        }))
    }

    fn code_by_hash(&self, code_hash: H256) -> Result<Bytecode, Self::Error> {
        let mut lru = self.lru.write().unwrap();
        if let Some(code) = lru.get(&code_hash) {
            Ok(code.clone())
        } else {
            // SAFETY: We are requesting the code by its hash, so it is almost tautological why this
            // would be safe. If the bytecode is not found, we return an empty bytecode with the
            // appropriate hash.
            //
            // In an ideal world we would return analysed bytecode here, but analysed bytecode in
            // revm depends on the current active hard fork, since it calculates gas
            // blocks...
            let bytecode = self.inner.bytecode_by_hash(code_hash)?;
            let bytecode = if let Some(bytecode) = bytecode {
                unsafe { to_analysed(Bytecode::new_raw_with_hash(bytecode.0, code_hash)) }
            } else {
                // NOTE(onbjerg): This corresponds to an empty analysed bytecode with a hash of
                // `KECCAK_EMPTY`. In the case where the bytecode is not found, we would
                // return empty bytes anyway: this simply skips the hashing and analysis steps,
                // which would otherwise be present if we simply did an
                // `.unwrap_or_default()` above.
                Bytecode::new()
            };

            lru.insert(code_hash, bytecode.clone());
            Ok(bytecode)
        }
    }

    fn storage(&self, address: H160, index: U256) -> Result<U256, Self::Error> {
        let index = H256(index.to_be_bytes());
        let ret = self.inner.storage(address, index)?.unwrap_or_default();
        Ok(ret)
    }

    fn block_hash(&self, number: U256) -> Result<H256, Self::Error> {
        Ok(self.inner.block_hash(number)?.unwrap_or_default())
    }
}
