#![warn(missing_docs, unreachable_pub)]
#![deny(unused_must_use, rust_2018_idioms)]
#![doc(test(
    no_crate_inject,
    attr(deny(warnings, rust_2018_idioms), allow(dead_code, unused_variables))
))]

//! Reth executor executes transaction in block of data.

pub mod eth_dao_fork;
pub mod substate;

/// Execution result types.
pub use reth_provider::post_state;

pub mod blockchain_tree;

/// Executor
pub mod executor;

/// ExecutorFactory impl
pub mod factory;
pub use factory::Factory;

#[cfg(any(test, feature = "test-utils"))]
/// Common test helpers for mocking out executor and executor factory
pub mod test_utils;
