//! Wrapper around revms state.

use reth_db::{
    cursor::{DbCursorRO, DbCursorRW, DbDupCursorRO, DbDupCursorRW},
    tables,
    transaction::{DbTx, DbTxMut},
};
use reth_interfaces::db::DatabaseError;
use reth_primitives::{Bytecode, StorageEntry, H256, U256};
use reth_revm_primitives::{db::states::StateChangeset as RevmChange, to_reth_acc};

/// A change to the state of the world.
#[derive(Default)]
pub struct StateChange(pub RevmChange);

impl From<RevmChange> for StateChange {
    fn from(revm: RevmChange) -> Self {
        Self(revm)
    }
}

impl StateChange {
    /// Write the post state to the database.
    pub fn write_to_db<'a, TX: DbTxMut<'a> + DbTx<'a>>(self, tx: &TX) -> Result<(), DatabaseError> {
        // Write new storage state
        tracing::trace!(target: "provider::post_state", len = self.0.storage.len(), "Writing new storage state");
        let mut storages_cursor = tx.cursor_dup_write::<tables::PlainStorageState>()?;
        for (address, (wipped, storage)) in self.0.storage.into_iter() {
            // If the storage was wiped at least once, remove all previous entries from the
            // database.
            if wipped {
                tracing::trace!(target: "provider::post_state", ?address, "Wiping storage from plain state");
                if storages_cursor.seek_exact(address)?.is_some() {
                    storages_cursor.delete_current_duplicates()?;
                }
            }

            for (key, value) in storage.into_iter() {
                tracing::trace!(target: "provider::post_state", ?address, ?key, "Updating plain state storage");
                let key: H256 = key.into();
                if let Some(entry) = storages_cursor.seek_by_key_subkey(address, key)? {
                    if entry.key == key {
                        storages_cursor.delete_current()?;
                    }
                }

                if value != U256::ZERO {
                    storages_cursor.upsert(address, StorageEntry { key, value })?;
                }
            }
        }

        // Write new account state
        tracing::trace!(target: "provider::post_state", len = self.0.accounts.len(), "Writing new account state");
        let mut accounts_cursor = tx.cursor_write::<tables::PlainAccountState>()?;
        for (address, account) in self.0.accounts.into_iter() {
            if let Some(account) = account {
                tracing::trace!(target: "provider::post_state", ?address, "Updating plain state account");
                accounts_cursor.upsert(address, to_reth_acc(&account))?;
            } else if accounts_cursor.seek_exact(address)?.is_some() {
                tracing::trace!(target: "provider::post_state", ?address, "Deleting plain state account");
                accounts_cursor.delete_current()?;
            }
        }

        // Write bytecode
        tracing::trace!(target: "provider::post_state", len = self.0.contracts.len(), "Writing bytecodes");
        let mut bytecodes_cursor = tx.cursor_write::<tables::Bytecodes>()?;
        for (hash, bytecode) in self.0.contracts.into_iter() {
            bytecodes_cursor.upsert(hash, Bytecode(bytecode))?;
        }

        // Write the receipts of the transactions
        // tracing::trace!(target: "provider::post_state", len = self.receipts.len(), "Writing
        // receipts"); let mut bodies_cursor =
        // tx.cursor_read::<tables::BlockBodyIndices>()?; let mut receipts_cursor =
        // tx.cursor_write::<tables::Receipts>()?; for (block, receipts) in self.receipts {
        //     let (_, body_indices) = bodies_cursor.seek_exact(block)?.expect("body indices
        // exist");     let tx_range = body_indices.tx_num_range();
        //     assert_eq!(receipts.len(), tx_range.clone().count(), "Receipt length mismatch");
        //     for (tx_num, receipt) in tx_range.zip(receipts) {
        //         receipts_cursor.append(tx_num, receipt)?;
        //     }
        // }
        Ok(())
    }
}
