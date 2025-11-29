use crate::record::RecordId;

use super::BPlusKey;

/// Node identifier (index into node storage)
pub type NodeId = usize;

/// Internal node: stores keys and child pointers
///
/// In this B+ tree variant:
/// - keys[i] is the maximum key in the subtree rooted at children[i]
/// - keys.len() == children.len()
#[derive(Debug, Clone)]
pub struct InternalNode {
    /// Maximum key of each child subtree
    pub keys: Vec<BPlusKey>,
    /// Child node IDs
    pub children: Vec<NodeId>,
}

impl InternalNode {
    /// Create a new internal node with given keys and children
    pub fn new(keys: Vec<BPlusKey>, children: Vec<NodeId>) -> Self {
        debug_assert_eq!(keys.len(), children.len());
        Self { keys, children }
    }

    /// Number of children
    pub fn len(&self) -> usize {
        self.children.len()
    }

    /// Check if node is empty
    pub fn is_empty(&self) -> bool {
        self.children.is_empty()
    }

    /// Find the child index for a given key
    /// Returns the index of the first key >= search key, or the last index if none found
    pub fn find_child_index(&self, key: BPlusKey) -> usize {
        // Find first key >= search key
        for (i, &k) in self.keys.iter().enumerate() {
            if k >= key {
                return i;
            }
        }
        // If no key >= search key, go to rightmost child
        self.keys.len().saturating_sub(1)
    }

    /// Insert a new key and child at the appropriate position
    /// Used after a child split
    pub fn insert_child(&mut self, key: BPlusKey, child: NodeId) {
        // Find insertion position
        let pos = self
            .keys
            .iter()
            .position(|&k| k >= key)
            .unwrap_or(self.keys.len());
        self.keys.insert(pos, key);
        self.children.insert(pos, child);
    }

    /// Update the key at a given position
    pub fn update_key(&mut self, index: usize, new_key: BPlusKey) {
        if index < self.keys.len() {
            self.keys[index] = new_key;
        }
    }

    /// Get the maximum key in this node
    pub fn max_key(&self) -> Option<BPlusKey> {
        self.keys.last().copied()
    }
}

/// Leaf node: stores key-value pairs, linked to next leaf
#[derive(Debug, Clone)]
pub struct LeafNode {
    /// Keys (sorted)
    pub keys: Vec<BPlusKey>,
    /// Values (RecordIds) corresponding to keys
    pub values: Vec<RecordId>,
    /// Link to next leaf for range queries
    pub next: Option<NodeId>,
}

impl LeafNode {
    /// Create a new empty leaf node
    pub fn new() -> Self {
        Self {
            keys: Vec::new(),
            values: Vec::new(),
            next: None,
        }
    }

    /// Create a leaf node with given entries
    pub fn with_entries(keys: Vec<BPlusKey>, values: Vec<RecordId>) -> Self {
        debug_assert_eq!(keys.len(), values.len());
        Self {
            keys,
            values,
            next: None,
        }
    }

    /// Number of entries
    pub fn len(&self) -> usize {
        self.keys.len()
    }

    /// Check if leaf is empty
    pub fn is_empty(&self) -> bool {
        self.keys.is_empty()
    }

    /// Insert a key-value pair in sorted order
    /// Allows duplicate keys
    pub fn insert(&mut self, key: BPlusKey, value: RecordId) {
        // Find insertion position (insert after existing keys with same value for stability)
        let pos = self
            .keys
            .iter()
            .position(|&k| k > key)
            .unwrap_or(self.keys.len());
        self.keys.insert(pos, key);
        self.values.insert(pos, value);
    }

    /// Search for a key, return the first matching RecordId
    pub fn search(&self, key: BPlusKey) -> Option<RecordId> {
        for (i, &k) in self.keys.iter().enumerate() {
            if k == key {
                return Some(self.values[i]);
            }
            if k > key {
                break;
            }
        }
        None
    }

    /// Search for all entries with the given key
    pub fn search_all(&self, key: BPlusKey) -> Vec<RecordId> {
        let mut results = Vec::new();
        for (i, &k) in self.keys.iter().enumerate() {
            match k.cmp(&key) {
                std::cmp::Ordering::Equal => results.push(self.values[i]),
                std::cmp::Ordering::Greater => break,
                std::cmp::Ordering::Less => {}
            }
        }
        results
    }

    /// Delete the first entry with the given key
    /// Returns true if an entry was deleted
    pub fn delete(&mut self, key: BPlusKey) -> bool {
        for i in 0..self.keys.len() {
            if self.keys[i] == key {
                self.keys.remove(i);
                self.values.remove(i);
                return true;
            }
            if self.keys[i] > key {
                break;
            }
        }
        false
    }

    /// Delete a specific key-value pair
    /// Returns true if the entry was found and deleted
    pub fn delete_entry(&mut self, key: BPlusKey, rid: RecordId) -> bool {
        for i in 0..self.keys.len() {
            if self.keys[i] == key && self.values[i] == rid {
                self.keys.remove(i);
                self.values.remove(i);
                return true;
            }
            if self.keys[i] > key {
                break;
            }
        }
        false
    }

    /// Get the maximum key in this leaf
    pub fn max_key(&self) -> Option<BPlusKey> {
        self.keys.last().copied()
    }

    /// Get the minimum key in this leaf
    pub fn min_key(&self) -> Option<BPlusKey> {
        self.keys.first().copied()
    }

    /// Split this leaf node, returning the new right sibling
    /// This node keeps the first half, new node gets the second half
    pub fn split(&mut self) -> LeafNode {
        let mid = self.keys.len() / 2;

        let right_keys = self.keys.split_off(mid);
        let right_values = self.values.split_off(mid);

        let mut right = LeafNode::with_entries(right_keys, right_values);
        right.next = self.next.take();

        right
    }
}

impl Default for LeafNode {
    fn default() -> Self {
        Self::new()
    }
}

/// B+ tree node (either internal or leaf)
#[derive(Debug, Clone)]
pub enum BPlusNode {
    Internal(InternalNode),
    Leaf(LeafNode),
}

impl BPlusNode {
    /// Check if this is a leaf node
    pub fn is_leaf(&self) -> bool {
        matches!(self, BPlusNode::Leaf(_))
    }

    /// Check if this is an internal node
    pub fn is_internal(&self) -> bool {
        matches!(self, BPlusNode::Internal(_))
    }

    /// Get the maximum key in this node
    pub fn max_key(&self) -> Option<BPlusKey> {
        match self {
            BPlusNode::Internal(node) => node.max_key(),
            BPlusNode::Leaf(node) => node.max_key(),
        }
    }

    /// Get as internal node reference
    pub fn as_internal(&self) -> Option<&InternalNode> {
        match self {
            BPlusNode::Internal(node) => Some(node),
            BPlusNode::Leaf(_) => None,
        }
    }

    /// Get as internal node mutable reference
    pub fn as_internal_mut(&mut self) -> Option<&mut InternalNode> {
        match self {
            BPlusNode::Internal(node) => Some(node),
            BPlusNode::Leaf(_) => None,
        }
    }

    /// Get as leaf node reference
    pub fn as_leaf(&self) -> Option<&LeafNode> {
        match self {
            BPlusNode::Internal(_) => None,
            BPlusNode::Leaf(node) => Some(node),
        }
    }

    /// Get as leaf node mutable reference
    pub fn as_leaf_mut(&mut self) -> Option<&mut LeafNode> {
        match self {
            BPlusNode::Internal(_) => None,
            BPlusNode::Leaf(node) => Some(node),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_leaf_node_insert() {
        let mut leaf = LeafNode::new();

        leaf.insert(5, RecordId::new(1, 0));
        leaf.insert(3, RecordId::new(1, 1));
        leaf.insert(7, RecordId::new(1, 2));
        leaf.insert(3, RecordId::new(1, 3)); // Duplicate key

        assert_eq!(leaf.len(), 4);
        assert_eq!(leaf.keys, vec![3, 3, 5, 7]);
    }

    #[test]
    fn test_leaf_node_search() {
        let mut leaf = LeafNode::new();

        leaf.insert(3, RecordId::new(1, 0));
        leaf.insert(5, RecordId::new(1, 1));
        leaf.insert(7, RecordId::new(1, 2));

        assert_eq!(leaf.search(5), Some(RecordId::new(1, 1)));
        assert_eq!(leaf.search(4), None);
        assert_eq!(leaf.search(10), None);
    }

    #[test]
    fn test_leaf_node_search_all() {
        let mut leaf = LeafNode::new();

        leaf.insert(5, RecordId::new(1, 0));
        leaf.insert(5, RecordId::new(1, 1));
        leaf.insert(5, RecordId::new(1, 2));
        leaf.insert(7, RecordId::new(1, 3));

        let results = leaf.search_all(5);
        assert_eq!(results.len(), 3);
    }

    #[test]
    fn test_leaf_node_delete() {
        let mut leaf = LeafNode::new();

        leaf.insert(3, RecordId::new(1, 0));
        leaf.insert(5, RecordId::new(1, 1));
        leaf.insert(7, RecordId::new(1, 2));

        assert!(leaf.delete(5));
        assert_eq!(leaf.len(), 2);
        assert_eq!(leaf.search(5), None);

        assert!(!leaf.delete(5)); // Already deleted
    }

    #[test]
    fn test_leaf_node_delete_entry() {
        let mut leaf = LeafNode::new();

        leaf.insert(5, RecordId::new(1, 0));
        leaf.insert(5, RecordId::new(1, 1));
        leaf.insert(5, RecordId::new(1, 2));

        assert!(leaf.delete_entry(5, RecordId::new(1, 1)));
        assert_eq!(leaf.len(), 2);

        let results = leaf.search_all(5);
        assert_eq!(results, vec![RecordId::new(1, 0), RecordId::new(1, 2)]);
    }

    #[test]
    fn test_leaf_node_split() {
        let mut leaf = LeafNode::new();

        for i in 0..6 {
            leaf.insert(i, RecordId::new(1, i as usize));
        }

        let right = leaf.split();

        assert_eq!(leaf.len(), 3); // 0, 1, 2
        assert_eq!(right.len(), 3); // 3, 4, 5
        assert_eq!(leaf.max_key(), Some(2));
        assert_eq!(right.min_key(), Some(3));
    }

    #[test]
    fn test_internal_node_find_child() {
        let node = InternalNode::new(vec![3, 7, 12], vec![0, 1, 2]);

        assert_eq!(node.find_child_index(1), 0); // <= 3, go to child 0
        assert_eq!(node.find_child_index(3), 0); // == 3, go to child 0
        assert_eq!(node.find_child_index(5), 1); // <= 7, go to child 1
        assert_eq!(node.find_child_index(7), 1); // == 7, go to child 1
        assert_eq!(node.find_child_index(10), 2); // <= 12, go to child 2
        assert_eq!(node.find_child_index(15), 2); // > all, go to last child
    }

    #[test]
    fn test_internal_node_insert_child() {
        let mut node = InternalNode::new(vec![3, 12], vec![0, 2]);

        node.insert_child(7, 1);

        assert_eq!(node.keys, vec![3, 7, 12]);
        assert_eq!(node.children, vec![0, 1, 2]);
    }
}
