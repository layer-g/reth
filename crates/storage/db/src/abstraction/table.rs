use crate::{
    abstraction::cursor::{DbCursorRO, DbCursorRW, DbDupCursorRO, DbDupCursorRW},
    transaction::{DbTx, DbTxMut},
    Error,
};

use serde::Serialize;
use std::{
    fmt::Debug,
    marker::{Send, Sync},
};

/// Trait that will transform the data to be saved in the DB in a (ideally) compressed format
pub trait Compress: Send + Sync + Sized + Debug {
    /// Compressed type.
    type Compressed: AsRef<[u8]> + Send + Sync;

    /// Compresses data going into the database.
    fn compress(self) -> Self::Compressed;
}

/// Trait that will transform the data to be read from the DB.
pub trait Decompress: Send + Sync + Sized + Debug {
    /// Decompresses data coming from the database.
    fn decompress<B: AsRef<[u8]>>(value: B) -> Result<Self, Error>;
}

/// Trait that will transform the data to be saved in the DB.
pub trait Encode: Send + Sync + Sized + Debug {
    /// Encoded type.
    type Encoded: AsRef<[u8]> + Send + Sync;

    /// Encodes data going into the database.
    fn encode(self) -> Self::Encoded;
}

/// Trait that will transform the data to be read from the DB.
pub trait Decode: Send + Sync + Sized + Debug {
    /// Decodes data coming from the database.
    fn decode<B: AsRef<[u8]>>(key: B) -> Result<Self, Error>;
}

/// Generic trait that enforces the database key to implement [`Encode`] and [`Decode`].
pub trait Key: Encode + Decode + Ord + Clone {}

impl<T> Key for T where T: Encode + Decode + Ord + Clone {}

/// Generic trait that enforces the database value to implement [`Compress`] and [`Decompress`].
pub trait Value: Compress + Decompress + Serialize {}

impl<T> Value for T where T: Compress + Decompress + Serialize {}

/// Generic trait that a database table should follow.
///
/// The [`Table::Key`] and [`Table::Value`] types should implement [`Encode`] and
/// [`Decode`] when appropriate. These traits define how the data is stored and read from the
/// database.
///
/// It allows for the use of codecs. See [`crate::models::ShardedKey`] for a custom
/// implementation.
pub trait Table: Send + Sync + Debug + 'static {
    /// Return table name as it is present inside the MDBX.
    const NAME: &'static str;
    /// Key element of `Table`.
    ///
    /// Sorting should be taken into account when encoding this.
    type Key: Key;
    /// Value element of `Table`.
    type Value: Value;
}

/// DupSort allows for keys to be repeated in the database.
///
/// Upstream docs: <https://libmdbx.dqdkfa.ru/usage.html#autotoc_md48>
pub trait DupSort: Table {
    /// The table subkey. This type must implement [`Encode`] and [`Decode`].
    ///
    /// Sorting should be taken into account when encoding this.
    ///
    /// Upstream docs: <https://libmdbx.dqdkfa.ru/usage.html#autotoc_md48>
    type SubKey: Key;
}

/// Allows duplicating tables across databases
pub trait TableImporter<'tx>: for<'a> DbTxMut<'a> {
    /// Imports all table data from another transaction.
    fn import_table<T: Table, R: DbTx<'tx>>(&self, source_tx: &R) -> Result<(), Error> {
        let mut destination_cursor = self.cursor_write::<T>()?;

        for kv in source_tx.cursor_read::<T>()?.walk(None)? {
            let (k, v) = kv?;
            destination_cursor.append(k, v)?;
        }

        Ok(())
    }

    /// Imports table data from another transaction within a range.
    fn import_table_with_range<T: Table, R: DbTx<'tx>>(
        &self,
        source_tx: &R,
        from: Option<<T as Table>::Key>,
        to: <T as Table>::Key,
    ) -> Result<(), Error>
    where
        T::Key: Default,
    {
        let mut destination_cursor = self.cursor_write::<T>()?;
        let mut source_cursor = source_tx.cursor_read::<T>()?;

        let source_range = match from {
            Some(from) => source_cursor.walk_range(from..=to),
            None => source_cursor.walk_range(..=to),
        };
        for row in source_range? {
            let (key, value) = row?;
            destination_cursor.append(key, value)?;
        }

        Ok(())
    }

    /// Imports all dupsort data from another transaction.
    fn import_dupsort<T: DupSort, R: DbTx<'tx>>(&self, source_tx: &R) -> Result<(), Error> {
        let mut destination_cursor = self.cursor_dup_write::<T>()?;
        let mut cursor = source_tx.cursor_dup_read::<T>()?;

        while let Some((k, _)) = cursor.next_no_dup()? {
            for kv in cursor.walk_dup(Some(k), None)? {
                let (k, v) = kv?;
                destination_cursor.append_dup(k, v)?;
            }
        }

        Ok(())
    }
}
