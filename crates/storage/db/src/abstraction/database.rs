use crate::{
    common::{Bounds, Sealed},
    implementation::mdbx::test_utils::create_test_rw_db,
    table::{Table, TableImporter},
    tables,
    transaction::{DbTx, DbTxGAT, DbTxMut},
    Error,
};
use std::{cell::Cell, sync::Arc};

/// Implements the GAT method from:
/// <https://sabrinajewson.org/blog/the-better-alternative-to-lifetime-gats#the-better-gats>.
///
/// Sealed trait which cannot be implemented by 3rd parties, exposed only for implementers
pub trait DatabaseGAT<'a, __ImplicitBounds: Sealed = Bounds<&'a Self>>: Send + Sync {
    /// RO database transaction
    type TX: DbTx<'a> + Send + Sync;
    /// RW database transaction
    type TXMut: DbTxMut<'a> + DbTx<'a> + TableImporter<'a> + Send + Sync;
}

/// Main Database trait that spawns transactions to be executed.
pub trait Database: for<'a> DatabaseGAT<'a> {
    /// Create read only transaction.
    fn tx(&self) -> Result<<Self as DatabaseGAT<'_>>::TX, Error>;

    /// Create read write transaction only possible if database is open with write access.
    fn tx_mut(&self) -> Result<<Self as DatabaseGAT<'_>>::TXMut, Error>;

    /// Takes a function and passes a read-only transaction into it, making sure it's closed in the
    /// end of the execution.
    fn view<T, F>(&self, f: F) -> Result<T, Error>
    where
        F: FnOnce(&<Self as DatabaseGAT<'_>>::TX) -> T,
    {
        let tx = self.tx()?;

        let res = f(&tx);
        tx.commit()?;

        Ok(res)
    }

    /// Takes a function and passes a write-read transaction into it, making sure it's committed in
    /// the end of the execution.
    fn update<T, F>(&self, f: F) -> Result<T, Error>
    where
        F: FnOnce(&<Self as DatabaseGAT<'_>>::TXMut) -> T,
    {
        let tx = self.tx_mut()?;

        let res = f(&tx);
        tx.commit()?;

        Ok(res)
    }
}

// Generic over Arc
impl<'a, DB: Database> DatabaseGAT<'a> for Arc<DB> {
    type TX = <DB as DatabaseGAT<'a>>::TX;
    type TXMut = <DB as DatabaseGAT<'a>>::TXMut;
}

impl<DB: Database> Database for Arc<DB> {
    fn tx(&self) -> Result<<Self as DatabaseGAT<'_>>::TX, Error> {
        <DB as Database>::tx(self)
    }

    fn tx_mut(&self) -> Result<<Self as DatabaseGAT<'_>>::TXMut, Error> {
        <DB as Database>::tx_mut(self)
    }
}

// Generic over reference
impl<'a, DB: Database> DatabaseGAT<'a> for &DB {
    type TX = <DB as DatabaseGAT<'a>>::TX;
    type TXMut = <DB as DatabaseGAT<'a>>::TXMut;
}

impl<DB: Database> Database for &DB {
    fn tx(&self) -> Result<<Self as DatabaseGAT<'_>>::TX, Error> {
        <DB as Database>::tx(self)
    }

    fn tx_mut(&self) -> Result<<Self as DatabaseGAT<'_>>::TXMut, Error> {
        <DB as Database>::tx_mut(self)
    }
}

#[cfg(test)]
mod tests {

    use reth_primitives::H256;

    use crate::cursor::DbCursorRO;

    use super::*;

    pub type CursorT<'a, TX: DbTx<'a>, T: Table> =
        <TX as DbTxGAT<'a>>::Cursor<T>;

    pub struct DatabaseGATWrapper<'a, TX: DbTx<'a>> {
        tx: TX,
        c1_taken: Cell<bool>,
        c2_taken: Cell<bool>,
        c1: Cell<Option<Box<CursorT<'a, TX, tables::CanonicalHeaders>>>>,
        c2: Cell<Option<Box<CursorT<'a, TX, tables::HeaderNumbers>>>>,
    }

    pub enum CursorState<'a, TX: DbTx<'a>, T: Table> {
        Taken,
        Free(Box<CursorT<'a, TX, T>>),
        NotInitialized,
    }

    impl<'a, TX: DbTx<'a>> DatabaseGATWrapper<'a, TX> {
        pub fn first<'b>(&'b self) -> DropVar<'b, 'a, TX, tables::CanonicalHeaders> {
            if self.c1_taken.get() == true {
                panic!("Cursor 1 is already taken");
            }
            self.c1_taken.set(true);
            let cursor = self.c1.take();
            DropVar { drop_flag: &self.c1_taken, cursor_cell: &self.c1,cursor  }
        }

        pub fn second<'b>(&'b self) -> DropVar<'b, 'a, TX, tables::HeaderNumbers> {
            if self.c2_taken.get() == true {
                panic!("Cursor 1 is already taken");
            }
            self.c2_taken.set(true);
            let cursor = self.c2.take();
            DropVar { drop_flag: &self.c2_taken, cursor_cell: &self.c2, cursor}
        }
    }

    pub struct DropVar<'a, 'b, TX: DbTx<'b>, T: Table> {
        drop_flag: &'a Cell<bool>,
        cursor_cell: &'a Cell<Option<Box<CursorT<'b, TX, T>>>>,
        cursor: Option<Box<CursorT<'b, TX, T>>>,
    }

    impl<'a, 'b, TX: DbTx<'b>, T: Table> Drop for DropVar<'a, 'b, TX, T> {
        fn drop(&mut self) {
            self.cursor_cell.set(self.cursor.take());
            self.drop_flag.set(false);
        }
    }

    #[test]
    pub fn temp() {
        let db = create_test_rw_db();
        db.update(|tx| {
            tx.put::<tables::CanonicalHeaders>(1, [0x11; 32].into())?;
            tx.put::<tables::CanonicalHeaders>(2, [0x22; 32].into())?;
            tx.put::<tables::HeaderNumbers>([0x33; 32].into(), 1)?;
            tx.put::<tables::HeaderNumbers>([0x44; 32].into(), 1)
        })
        .unwrap()
        .unwrap();

        let tx = db.tx().unwrap();
        let c1 = Cell::new(Some(Box::new(tx.cursor_read::<tables::CanonicalHeaders>().unwrap())));
        let c2 = Cell::new(Some(Box::new(tx.cursor_read::<tables::HeaderNumbers>().unwrap())));

        let mut wrap = DatabaseGATWrapper {
            //db: &db,
            tx,
            c1_taken: Cell::new(false),
            c2_taken: Cell::new(false),
            c1,
            c2,
        };
        let mut first = wrap.first();
        let mut second = wrap.second();

        println!("{:?}",first.cursor.as_mut().unwrap().seek(1).unwrap());
        println!("{:?}",second.cursor.as_mut().unwrap().seek(H256([0x33;32])).unwrap());

        drop(first);
        let mut first = wrap.first();
        println!("{:?}",first.cursor.as_mut().unwrap().seek(1).unwrap());
        //let second = wrap.second();
        // panics
        //let first = wrap.first();

        drop(first);
    }
}
