use crate::traits::PoolTransaction;
use reth_primitives::U256;
use std::{fmt, marker::PhantomData};

/// Transaction ordering trait to determine the order of transactions.
///
/// Decides how transactions should be ordered within the pool, depending on a `Priority` value.
///
/// The returned priority must reflect [total order](https://en.wikipedia.org/wiki/Total_order).
pub trait TransactionOrdering: Send + Sync + 'static {
    /// Priority of a transaction.
    ///
    /// Higher is better.
    type Priority: Ord + Clone + Default + fmt::Debug + Send + Sync;

    /// The transaction type to determine the priority of.
    type Transaction: PoolTransaction;

    /// Returns the priority score for the given transaction.
    fn priority(&self, transaction: &Self::Transaction) -> Self::Priority;
}

/// Default ordering for the pool.
///
/// The transactions are ordered by their cost. The higher the cost,
/// the higher the priority of this transaction is.
#[derive(Debug, Default)]
#[non_exhaustive]
pub struct CostOrdering<T>(PhantomData<T>);

impl<T> TransactionOrdering for CostOrdering<T>
where
    T: PoolTransaction + 'static,
{
    type Priority = U256;
    type Transaction = T;

    fn priority(&self, transaction: &Self::Transaction) -> Self::Priority {
        transaction.cost()
    }
}
