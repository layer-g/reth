use crate::{
    cursor::{CursorSubNode, TrieCursor},
    prefix_set::PrefixSet,
    Nibbles,
};
use reth_db::{table::Key, Error};
use reth_primitives::{trie::BranchNodeCompact, H256};
use std::marker::PhantomData;

/// `TrieWalker` is a structure that enables traversal of a Merkle trie.
/// It allows moving through the trie in a depth-first manner, skipping certain branches if the .
pub struct TrieWalker<'a, K, C> {
    /// A mutable reference to a trie cursor instance used for navigating the trie.
    pub cursor: &'a mut C,
    /// A vector containing the trie nodes that have been visited.
    pub stack: Vec<CursorSubNode>,
    /// A flag indicating whether the current node can be skipped when traversing the trie. This
    /// is determined by whether the current key's prefix is included in the prefix set and if the
    /// hash flag is set.
    pub can_skip_current_node: bool,
    /// A `PrefixSet` representing the changes to be applied to the trie.
    pub changes: PrefixSet,
    __phantom: PhantomData<K>,
}

impl<'a, K: Key + From<Vec<u8>>, C: TrieCursor<K>> TrieWalker<'a, K, C> {
    /// Constructs a new TrieWalker, setting up the initial state of the stack and cursor.
    pub fn new(cursor: &'a mut C, changes: PrefixSet) -> Self {
        // Initialize the walker with a single empty stack element.
        let mut this = Self {
            cursor,
            changes,
            can_skip_current_node: false,
            stack: vec![CursorSubNode::default()],
            __phantom: PhantomData::default(),
        };

        // Set up the root node of the trie in the stack, if it exists.
        if let Some((key, value)) = this.node(true).unwrap() {
            this.stack[0] = CursorSubNode::new(key, Some(value));
        }

        // Update the skip state for the root node.
        this.update_skip_node();
        this
    }

    /// Prints the current stack of trie nodes.
    pub fn print_stack(&self) {
        println!("====================== STACK ======================");
        for node in &self.stack {
            println!("{node:?}");
        }
        println!("====================== END STACK ======================\n");
    }

    /// Advances the walker to the next trie node and updates the skip node flag.
    ///
    /// # Returns
    ///
    /// * `Result<Option<Nibbles>, Error>` - The next key in the trie or an error.
    pub fn advance(&mut self) -> Result<Option<Nibbles>, Error> {
        if let Some(last) = self.stack.last() {
            if !self.can_skip_current_node && self.children_are_in_trie() {
                // If we can't skip the current node and the children are in the trie,
                // either consume the next node or move to the next sibling.
                match last.nibble {
                    -1 => self.move_to_next_sibling(true)?,
                    _ => self.consume_node()?,
                }
            } else {
                // If we can skip the current node, move to the next sibling.
                self.move_to_next_sibling(false)?;
            }

            // Update the skip node flag based on the new position in the trie.
            self.update_skip_node();
        }

        // Return the current key.
        Ok(self.key())
    }

    /// Retrieves the current root node from the DB, seeking either the exact node or the next one.
    fn node(&mut self, exact: bool) -> Result<Option<(Nibbles, BranchNodeCompact)>, Error> {
        let key = self.key().expect("key must exist");
        let entry = if exact {
            self.cursor.seek_exact(key.hex_data.into())?
        } else {
            self.cursor.seek(key.hex_data.into())?
        };

        if let Some((_, node)) = &entry {
            assert!(!node.state_mask.is_empty());
        }

        Ok(entry.map(|(k, v)| (Nibbles::from(k), v)))
    }

    /// Consumes the next node in the trie, updating the stack.
    fn consume_node(&mut self) -> Result<(), Error> {
        let Some((key, node)) = self.node(false)? else {
            // If no next node is found, clear the stack.
            self.stack.clear();
            return Ok(());
        };

        // Overwrite the root node's first nibble
        // We need to sync the stack with the trie structure when consuming a new node. This is
        // necessary for proper traversal and accurately representing the trie in the stack.
        if !key.is_empty() && !self.stack.is_empty() {
            self.stack[0].nibble = key[0] as i8;
        }

        // Create a new CursorSubNode and push it to the stack.
        let subnode = CursorSubNode::new(key, Some(node));
        let nibble = subnode.nibble;
        self.stack.push(subnode);
        self.update_skip_node();

        // Delete the current node if it's included in the prefix set or it doesn't contain the root
        // hash.
        if !self.can_skip_current_node || nibble != -1 {
            self.cursor.delete_current()?;
        }

        Ok(())
    }

    /// Moves to the next sibling node in the trie, updating the stack.
    fn move_to_next_sibling(&mut self, allow_root_to_child_nibble: bool) -> Result<(), Error> {
        let Some(subnode) = self.stack.last_mut() else {
            return Ok(());
        };

        // Check if the walker needs to backtrack to the previous level in the trie during its
        // traversal.
        if subnode.nibble >= 15 || (subnode.nibble < 0 && !allow_root_to_child_nibble) {
            self.stack.pop();
            self.move_to_next_sibling(false)?;
            return Ok(())
        }

        subnode.nibble += 1;

        if subnode.node.is_none() {
            return self.consume_node()
        }

        // Find the next sibling with state.
        while subnode.nibble < 16 {
            if subnode.state_flag() {
                return Ok(())
            }
            subnode.nibble += 1;
        }

        // Pop the current node and move to the next sibling.
        self.stack.pop();
        self.move_to_next_sibling(false)?;

        Ok(())
    }

    /// Returns the current key in the trie.
    pub fn key(&self) -> Option<Nibbles> {
        self.stack.last().map(|n| n.full_key())
    }

    /// Returns the current hash in the trie if any.
    pub fn hash(&self) -> Option<H256> {
        self.stack.last().and_then(|n| n.hash())
    }

    /// Indicates whether the children of the current node are present in the trie.
    pub fn children_are_in_trie(&self) -> bool {
        self.stack.last().map_or(false, |n| n.tree_flag())
    }

    /// Returns the next unprocessed key in the trie.
    pub fn next_unprocessed_key(&self) -> Option<H256> {
        self.key()
            .as_ref()
            .and_then(|key| {
                if self.can_skip_current_node {
                    key.increment().map(|inc| inc.pack())
                } else {
                    Some(key.pack())
                }
            })
            .map(|mut key| {
                key.resize(32, 0);
                H256::from_slice(key.as_slice())
            })
    }

    fn update_skip_node(&mut self) {
        self.can_skip_current_node = if let Some(key) = self.key() {
            let contains_prefix = self.changes.contains(key);
            let hash_flag = self.stack.last().unwrap().hash_flag();
            !contains_prefix && hash_flag
        } else {
            false
        };
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cursor::{AccountTrieCursor, StorageTrieCursor};
    use reth_db::{mdbx::test_utils::create_test_rw_db, tables, transaction::DbTxMut};
    use reth_provider::Transaction;

    #[test]
    fn walk_nodes_with_common_prefix() {
        let inputs = vec![
            (vec![0x5u8], BranchNodeCompact::new(0b1_0000_0101, 0b1_0000_0100, 0, vec![], None)),
            (vec![0x5u8, 0x2, 0xC], BranchNodeCompact::new(0b1000_0111, 0, 0, vec![], None)),
            (vec![0x5u8, 0x8], BranchNodeCompact::new(0b0110, 0b0100, 0, vec![], None)),
        ];
        let expected = vec![
            vec![0x5, 0x0],
            // The [0x5, 0x2] prefix is shared by the first 2 nodes, however:
            // 1. 0x2 for the first node points to the child node path
            // 2. 0x2 for the second node is a key.
            // So to proceed to add 1 and 3, we need to push the sibling first (0xC).
            vec![0x5, 0x2],
            vec![0x5, 0x2, 0xC, 0x0],
            vec![0x5, 0x2, 0xC, 0x1],
            vec![0x5, 0x2, 0xC, 0x2],
            vec![0x5, 0x2, 0xC, 0x7],
            vec![0x5, 0x8],
            vec![0x5, 0x8, 0x1],
            vec![0x5, 0x8, 0x2],
        ];

        let db = create_test_rw_db();
        let tx = Transaction::new(db.as_ref()).unwrap();
        let account_trie =
            AccountTrieCursor::new(tx.cursor_write::<tables::AccountsTrie>().unwrap());
        test_cursor(account_trie, &inputs, &expected);

        let storage_trie = StorageTrieCursor::new(
            tx.cursor_dup_write::<tables::StoragesTrie>().unwrap(),
            H256::random(),
        );
        test_cursor(storage_trie, &inputs, &expected);
    }

    fn test_cursor<K, T>(mut trie: T, inputs: &[(Vec<u8>, BranchNodeCompact)], expected: &[Vec<u8>])
    where
        K: Key + From<Vec<u8>>,
        T: TrieCursor<K>,
    {
        for (k, v) in inputs {
            trie.upsert(k.clone().into(), v.clone()).unwrap();
        }

        let mut walker = TrieWalker::new(&mut trie, Default::default());
        assert!(walker.key().unwrap().is_empty());

        // We're traversing the path in lexigraphical order.
        for expected in expected {
            let got = walker.advance().unwrap();
            assert_eq!(got.unwrap(), Nibbles::from(&expected[..]));
        }

        // There should be 8 paths traversed in total from 3 branches.
        let got = walker.advance().unwrap();
        assert!(got.is_none());
    }

    #[test]
    fn cursor_rootnode_with_changesets() {
        let db = create_test_rw_db();
        let tx = Transaction::new(db.as_ref()).unwrap();

        let mut trie = StorageTrieCursor::new(
            tx.cursor_dup_write::<tables::StoragesTrie>().unwrap(),
            H256::random(),
        );

        let nodes = vec![
            (
                vec![],
                BranchNodeCompact::new(
                    // 2 and 4 are set
                    0b10100,
                    0b00100,
                    0,
                    vec![],
                    Some(H256::random()),
                ),
            ),
            (
                vec![0x2],
                BranchNodeCompact::new(
                    // 1 is set
                    0b00010,
                    0,
                    0b00010,
                    vec![H256::random()],
                    None,
                ),
            ),
        ];

        for (k, v) in nodes {
            trie.upsert(k.into(), v).unwrap();
        }

        // No changes
        let mut cursor = TrieWalker::new(&mut trie, Default::default());
        assert_eq!(cursor.key(), Some(Nibbles::from(vec![]))); // root
        assert!(cursor.can_skip_current_node); // due to root_hash
        cursor.advance().unwrap(); // skips to the end of trie
        assert_eq!(cursor.key(), None);

        // We insert something that's not part of the existing trie/prefix.
        let mut changed = PrefixSet::default();
        changed.insert(&[0xF, 0x1]);
        let mut cursor = TrieWalker::new(&mut trie, changed);

        // Root node
        assert_eq!(cursor.key(), Some(Nibbles::from(vec![])));
        // Should not be able to skip state due to the changed values
        assert!(!cursor.can_skip_current_node);
        cursor.advance().unwrap();
        assert_eq!(cursor.key(), Some(Nibbles::from(vec![0x2])));
        cursor.advance().unwrap();
        assert_eq!(cursor.key(), Some(Nibbles::from(vec![0x2, 0x1])));
        cursor.advance().unwrap();
        assert_eq!(cursor.key(), Some(Nibbles::from(vec![0x4])));

        cursor.advance().unwrap();
        assert_eq!(cursor.key(), None); // the end of trie
    }
}
