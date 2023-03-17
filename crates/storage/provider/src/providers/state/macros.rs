//! Helper macros for implementing traits for various [StateProvider](crate::StateProvider)
//! implementations

/// A macro that delegates trait implementations to the `as_ref` function of the type.
///
/// Used to implement provider traits.
macro_rules! delegate_impls_to_as_ref {
    (for $target:ty => $($trait:ident $(where [$($generics:tt)*])? {  $(fn $func:ident$(<$($generic_arg:ident: $generic_arg_ty:path),*>)?(&self, $($arg:ident: $argty:ty),*) -> $ret:path;)* })* ) => {

        $(
          impl<'a, $($($generics)*)?> $trait for $target {
              $(
                  fn $func$(<$($generic_arg: $generic_arg_ty),*>)?(&self, $($arg: $argty),*) -> $ret {
                    self.as_ref().$func($($arg),*)
                  }
              )*
          }
        )*
    };
}

pub(crate) use delegate_impls_to_as_ref;

/// Delegates the provider trait implementations to the `as_ref` function of the type:
///
/// [AccountProvider](crate::AccountProvider)
/// [BlockHashProvider](crate::BlockHashProvider)
/// [StateProvider](crate::StateProvider)
macro_rules! delegate_provider_impls {
    ($target:ty $(where [$($generics:tt)*])?) => {
        $crate::providers::state::macros::delegate_impls_to_as_ref!(
            for $target =>
            AccountProvider $(where [$($generics)*])? {
                fn basic_account(&self, address: reth_primitives::Address) -> reth_interfaces::Result<Option<reth_primitives::Account>>;
            }
            BlockHashProvider $(where [$($generics)*])? {
                fn block_hash(&self, number: u64) -> reth_interfaces::Result<Option<reth_primitives::H256>>;
                fn canonical_hashes_range(&self, start: reth_primitives::BlockNumber, end: reth_primitives::BlockNumber) -> reth_interfaces::Result<Vec<reth_primitives::H256>>;
            }
            StateProvider $(where [$($generics)*])?{
                fn storage(&self, account: reth_primitives::Address, storage_key: reth_primitives::StorageKey) -> reth_interfaces::Result<Option<reth_primitives::StorageValue>>;
                fn proof(&self, address: reth_primitives::Address, keys: &[reth_primitives::H256]) -> reth_interfaces::Result<(Vec<reth_primitives::Bytes>, reth_primitives::H256, Vec<Vec<reth_primitives::Bytes>>)>;
                fn bytecode_by_hash(&self, code_hash: reth_primitives::H256) -> reth_interfaces::Result<Option<reth_primitives::Bytecode>>;
            }
        );
    }
}

pub(crate) use delegate_provider_impls;
