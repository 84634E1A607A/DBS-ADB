//! B+ Tree implementation for database indexing
//!
//! This module provides a standard B+ tree data structure optimized for
//! database index operations. It supports:
//! - Duplicate keys (for non-unique indexes)
//! - Range queries (via linked leaf nodes)
//! - i64 keys with RecordId values
//!
//! The tree is currently in-memory; disk persistence will be added
//! when integrating with the index file layer.

mod error;
mod node;

pub use error::{BPlusTreeError, BPlusTreeResult};
pub use node::{BPlusNode, InternalNode, LeafNode, NodeId};

use crate::record::RecordId;

/// Key type for B+ tree (i64 for INT columns)
pub type BPlusKey = i64;

/// Default B+ tree order optimized for 8KB pages
/// - Leaf node: 499 entries (order - 1) = 499 * 16 bytes + 16 byte header = 8000 bytes
/// - Internal node: 500 children = 500 * 12 bytes + 16 byte header = 6016 bytes
/// This ensures one node fits comfortably in one 8KB page
pub const DEFAULT_ORDER: usize = 500;

/// B+ Tree data structure
///
/// Order `m` means:
/// - Internal nodes have at most `m` children
/// - Internal nodes (except root) have at least `ceil(m/2)` children
/// - Leaf nodes have at most `m-1` entries
/// - Leaf nodes (except root) have at least `ceil((m-1)/2)` entries
#[derive(Debug)]
pub struct BPlusTree {
    /// Root node ID (None if tree is empty)
    root: Option<NodeId>,

    /// Tree order (max children per internal node)
    order: usize,

    /// Node storage
    nodes: Vec<Option<BPlusNode>>,

    /// Free list for recycling deleted nodes
    free_list: Vec<NodeId>,

    /// First leaf node (for full range scans)
    first_leaf: Option<NodeId>,

    /// Total number of entries in the tree
    entry_count: usize,
}

impl BPlusTree {
    /// Create a new empty B+ tree with the given order
    ///
    /// # Arguments
    /// * `order` - The tree order (must be >= 3)
    ///
    /// # Returns
    /// * `Ok(BPlusTree)` - A new empty B+ tree
    /// * `Err(BPlusTreeError)` - If order is invalid
    pub fn new(order: usize) -> BPlusTreeResult<Self> {
        if order < 3 {
            return Err(BPlusTreeError::InvalidOrder(order));
        }

        Ok(Self {
            root: None,
            order,
            nodes: Vec::new(),
            free_list: Vec::new(),
            first_leaf: None,
            entry_count: 0,
        })
    }

    /// Create a new B+ tree with default order (500, optimized for 8KB pages)
    pub fn default_order() -> Self {
        Self::new(DEFAULT_ORDER).expect("Default order is valid")
    }

    /// Get the tree order
    pub fn order(&self) -> usize {
        self.order
    }

    /// Check if tree is empty
    pub fn is_empty(&self) -> bool {
        self.root.is_none()
    }

    /// Get number of entries in the tree
    pub fn len(&self) -> usize {
        self.entry_count
    }

    /// Get tree height (1 for single leaf, 2+ for internal nodes)
    pub fn height(&self) -> usize {
        match self.root {
            None => 0,
            Some(root_id) => {
                let mut height = 1;
                let mut current = root_id;

                while let Some(BPlusNode::Internal(node)) = self.get_node(current) {
                    if let Some(&child_id) = node.children.first() {
                        current = child_id;
                        height += 1;
                    } else {
                        break;
                    }
                }

                height
            }
        }
    }

    /// Maximum entries in a leaf node
    fn max_leaf_entries(&self) -> usize {
        self.order - 1
    }

    /// Minimum entries in a leaf node (except root)
    fn min_leaf_entries(&self) -> usize {
        (self.order - 1).div_ceil(2) // ceil((m-1)/2)
    }

    /// Maximum children in an internal node
    fn max_internal_children(&self) -> usize {
        self.order
    }

    /// Minimum children in an internal node (except root)
    fn min_internal_children(&self) -> usize {
        self.order.div_ceil(2) // ceil(m/2)
    }

    // ========== Node Management ==========

    /// Allocate a new node, returning its ID
    fn allocate_node(&mut self, node: BPlusNode) -> NodeId {
        if let Some(id) = self.free_list.pop() {
            self.nodes[id] = Some(node);
            id
        } else {
            let id = self.nodes.len();
            self.nodes.push(Some(node));
            id
        }
    }

    /// Get a reference to a node by ID (public for index layer)
    pub fn get_node(&self, id: NodeId) -> Option<&BPlusNode> {
        self.nodes.get(id).and_then(|n| n.as_ref())
    }

    /// Get a mutable reference to a node by ID
    fn get_node_mut(&mut self, id: NodeId) -> Option<&mut BPlusNode> {
        self.nodes.get_mut(id).and_then(|n| n.as_mut())
    }

    /// Get the root node ID
    pub fn root_node_id(&self) -> Option<NodeId> {
        self.root
    }

    /// Get the first leaf node ID
    pub fn first_leaf_id(&self) -> Option<NodeId> {
        self.first_leaf
    }

    /// Get the total number of nodes
    pub fn node_count(&self) -> usize {
        self.nodes.len()
    }

    /// Free a node, adding it to the free list
    fn free_node(&mut self, id: NodeId) {
        if id < self.nodes.len() {
            self.nodes[id] = None;
            self.free_list.push(id);
        }
    }

    // ========== Search Operations ==========

    /// Search for a key, returning the first matching RecordId
    pub fn search(&self, key: BPlusKey) -> Option<RecordId> {
        let leaf_id = self.find_leaf(key)?;
        let leaf = self.get_node(leaf_id)?.as_leaf()?;
        leaf.search(key)
    }

    /// Search for all entries with the given key
    pub fn search_all(&self, key: BPlusKey) -> Vec<RecordId> {
        let mut results = Vec::new();

        let leaf_id = match self.find_leaf(key) {
            Some(id) => id,
            None => return results,
        };

        let mut current_id = Some(leaf_id);

        // Scan through leaf nodes (key might span multiple leaves for duplicates)
        while let Some(id) = current_id {
            let leaf = match self.get_node(id).and_then(|n| n.as_leaf()) {
                Some(l) => l,
                None => break,
            };

            // Check if we've passed the key
            if let Some(min_key) = leaf.min_key() {
                if min_key > key {
                    break;
                }
            }

            // Collect matching entries
            for (i, &k) in leaf.keys.iter().enumerate() {
                if k == key {
                    results.push(leaf.values[i]);
                } else if k > key {
                    return results;
                }
            }

            current_id = leaf.next;
        }

        results
    }

    /// Range search: return all entries where lower <= key <= upper
    pub fn range_search(&self, lower: BPlusKey, upper: BPlusKey) -> Vec<(BPlusKey, RecordId)> {
        let mut results = Vec::new();

        if lower > upper {
            return results;
        }

        // Find the leaf containing the lower bound
        let leaf_id = match self.find_leaf(lower) {
            Some(id) => id,
            None => return results,
        };

        let mut current_id = Some(leaf_id);

        // Scan through leaf nodes
        while let Some(id) = current_id {
            let leaf = match self.get_node(id).and_then(|n| n.as_leaf()) {
                Some(l) => l,
                None => break,
            };

            // Collect entries in range
            for (i, &k) in leaf.keys.iter().enumerate() {
                if k > upper {
                    return results;
                }
                if k >= lower {
                    results.push((k, leaf.values[i]));
                }
            }

            current_id = leaf.next;
        }

        results
    }

    /// Find the leaf node that should contain the given key
    fn find_leaf(&self, key: BPlusKey) -> Option<NodeId> {
        let mut current = self.root?;

        loop {
            match self.get_node(current)? {
                BPlusNode::Leaf(_) => return Some(current),
                BPlusNode::Internal(node) => {
                    let child_idx = node.find_child_index(key);
                    current = node.children[child_idx];
                }
            }
        }
    }

    /// Find the leaf node and the path from root to it
    fn find_leaf_with_path(&self, key: BPlusKey) -> Option<(NodeId, Vec<(NodeId, usize)>)> {
        let mut current = self.root?;
        let mut path = Vec::new();

        loop {
            match self.get_node(current)? {
                BPlusNode::Leaf(_) => return Some((current, path)),
                BPlusNode::Internal(node) => {
                    let child_idx = node.find_child_index(key);
                    path.push((current, child_idx));
                    current = node.children[child_idx];
                }
            }
        }
    }

    // ========== Insert Operations ==========

    /// Insert a key-value pair into the tree
    pub fn insert(&mut self, key: BPlusKey, rid: RecordId) -> BPlusTreeResult<()> {
        if self.root.is_none() {
            // Create first leaf as root
            let mut leaf = LeafNode::new();
            leaf.insert(key, rid);
            let leaf_id = self.allocate_node(BPlusNode::Leaf(leaf));
            self.root = Some(leaf_id);
            self.first_leaf = Some(leaf_id);
            self.entry_count = 1;
            return Ok(());
        }

        // Find the leaf and path
        let (leaf_id, path) = self
            .find_leaf_with_path(key)
            .ok_or_else(|| BPlusTreeError::InvalidState("Could not find leaf".to_string()))?;

        // Insert into leaf
        {
            let leaf = self
                .get_node_mut(leaf_id)
                .and_then(|n| n.as_leaf_mut())
                .ok_or(BPlusTreeError::NodeNotFound(leaf_id))?;
            leaf.insert(key, rid);
        }

        self.entry_count += 1;

        // Check for overflow and split if needed
        let leaf_len = self
            .get_node(leaf_id)
            .and_then(|n| n.as_leaf())
            .map(|l| l.len())
            .unwrap_or(0);

        if leaf_len > self.max_leaf_entries() {
            self.split_leaf(leaf_id, path)?;
        } else {
            // Update keys in ancestors if max key changed
            self.update_ancestor_keys(leaf_id, &path)?;
        }

        Ok(())
    }

    /// Split an overflowing leaf node
    fn split_leaf(&mut self, leaf_id: NodeId, path: Vec<(NodeId, usize)>) -> BPlusTreeResult<()> {
        // Split the leaf
        let (left_max_key, right_max_key, right_id) = {
            let leaf = self
                .get_node_mut(leaf_id)
                .and_then(|n| n.as_leaf_mut())
                .ok_or(BPlusTreeError::NodeNotFound(leaf_id))?;

            let mut right = leaf.split();
            let left_max = leaf.max_key().unwrap_or(0);
            let right_max = right.max_key().unwrap_or(0);

            // Link the new leaf
            let right_id = self.allocate_node(BPlusNode::Leaf(LeafNode::new()));

            // Update the leaf's next pointer
            let leaf = self
                .get_node_mut(leaf_id)
                .and_then(|n| n.as_leaf_mut())
                .ok_or(BPlusTreeError::NodeNotFound(leaf_id))?;
            right.next = leaf.next.take();
            leaf.next = Some(right_id);

            // Store the right leaf
            self.nodes[right_id] = Some(BPlusNode::Leaf(right));

            (left_max, right_max, right_id)
        };

        // Insert into parent
        self.insert_into_parent(path, leaf_id, left_max_key, right_id, right_max_key)?;

        Ok(())
    }

    /// Insert a new child into the parent after a split
    fn insert_into_parent(
        &mut self,
        path: Vec<(NodeId, usize)>,
        left_id: NodeId,
        left_key: BPlusKey,
        right_id: NodeId,
        right_key: BPlusKey,
    ) -> BPlusTreeResult<()> {
        if path.is_empty() {
            // Split the root - create new root
            let new_root = InternalNode::new(vec![left_key, right_key], vec![left_id, right_id]);
            let new_root_id = self.allocate_node(BPlusNode::Internal(new_root));
            self.root = Some(new_root_id);
            return Ok(());
        }

        // Get parent info
        let (parent_id, child_idx) = path[path.len() - 1];
        let parent_path = path[..path.len() - 1].to_vec();

        // Update parent
        {
            let parent = self
                .get_node_mut(parent_id)
                .and_then(|n| n.as_internal_mut())
                .ok_or(BPlusTreeError::NodeNotFound(parent_id))?;

            // Update the key for the left child
            parent.keys[child_idx] = left_key;

            // Insert the right child after the left child
            parent.keys.insert(child_idx + 1, right_key);
            parent.children.insert(child_idx + 1, right_id);
        }

        // Check for overflow in parent
        let parent_len = self
            .get_node(parent_id)
            .and_then(|n| n.as_internal())
            .map(|n| n.len())
            .unwrap_or(0);

        if parent_len > self.max_internal_children() {
            self.split_internal(parent_id, parent_path)?;
        }

        Ok(())
    }

    /// Split an overflowing internal node
    fn split_internal(
        &mut self,
        node_id: NodeId,
        path: Vec<(NodeId, usize)>,
    ) -> BPlusTreeResult<()> {
        // Split the internal node
        let (left_max_key, right_max_key, right_id) = {
            let node = self
                .get_node_mut(node_id)
                .and_then(|n| n.as_internal_mut())
                .ok_or(BPlusTreeError::NodeNotFound(node_id))?;

            let mid = node.len() / 2;

            let right_keys = node.keys.split_off(mid);
            let right_children = node.children.split_off(mid);

            let left_max = node.keys.last().copied().unwrap_or(0);
            let right_max = *right_keys.last().unwrap_or(&0);

            let right_node = InternalNode::new(right_keys, right_children);
            let right_id = self.allocate_node(BPlusNode::Internal(right_node));

            (left_max, right_max, right_id)
        };

        // Insert into parent
        self.insert_into_parent(path, node_id, left_max_key, right_id, right_max_key)?;

        Ok(())
    }

    /// Update ancestor keys after an insertion
    fn update_ancestor_keys(
        &mut self,
        node_id: NodeId,
        path: &[(NodeId, usize)],
    ) -> BPlusTreeResult<()> {
        // Update keys in ancestors from bottom to top
        // Each level needs to be updated with the actual max of that subtree
        let mut current_node = node_id;

        for &(parent_id, child_idx) in path.iter().rev() {
            let max_key = self
                .get_node(current_node)
                .and_then(|n| n.max_key())
                .ok_or(BPlusTreeError::NodeNotFound(current_node))?;

            let parent = self
                .get_node_mut(parent_id)
                .and_then(|n| n.as_internal_mut())
                .ok_or(BPlusTreeError::NodeNotFound(parent_id))?;

            // Only update if the key actually changed
            if parent.keys[child_idx] != max_key {
                parent.keys[child_idx] = max_key;
                current_node = parent_id;
            } else {
                // If the key didn't change, no need to update higher levels
                break;
            }
        }

        Ok(())
    }

    // ========== Delete Operations ==========

    /// Delete the first entry with the given key
    /// Returns true if an entry was deleted
    pub fn delete(&mut self, key: BPlusKey) -> BPlusTreeResult<bool> {
        if self.root.is_none() {
            return Ok(false);
        }

        // Find the leaf and path
        let (leaf_id, path) = match self.find_leaf_with_path(key) {
            Some(result) => result,
            None => return Ok(false),
        };

        // Delete from leaf
        let deleted = {
            let leaf = self
                .get_node_mut(leaf_id)
                .and_then(|n| n.as_leaf_mut())
                .ok_or(BPlusTreeError::NodeNotFound(leaf_id))?;
            leaf.delete(key)
        };

        if !deleted {
            return Ok(false);
        }

        self.entry_count -= 1;

        // Handle underflow
        self.handle_leaf_underflow(leaf_id, path)?;

        Ok(true)
    }

    /// Delete a specific key-value pair
    /// Returns true if the entry was found and deleted
    pub fn delete_entry(&mut self, key: BPlusKey, rid: RecordId) -> BPlusTreeResult<bool> {
        if self.root.is_none() {
            return Ok(false);
        }

        // Find the leaf and path
        let (leaf_id, path) = match self.find_leaf_with_path(key) {
            Some(result) => result,
            None => return Ok(false),
        };

        // Delete from leaf
        let deleted = {
            let leaf = self
                .get_node_mut(leaf_id)
                .and_then(|n| n.as_leaf_mut())
                .ok_or(BPlusTreeError::NodeNotFound(leaf_id))?;
            leaf.delete_entry(key, rid)
        };

        if !deleted {
            return Ok(false);
        }

        self.entry_count -= 1;

        // Handle underflow
        self.handle_leaf_underflow(leaf_id, path)?;

        Ok(true)
    }

    /// Handle underflow in a leaf node after deletion
    fn handle_leaf_underflow(
        &mut self,
        leaf_id: NodeId,
        path: Vec<(NodeId, usize)>,
    ) -> BPlusTreeResult<()> {
        let leaf_len = self
            .get_node(leaf_id)
            .and_then(|n| n.as_leaf())
            .map(|l| l.len())
            .unwrap_or(0);

        // Check if leaf is root
        if path.is_empty() {
            if leaf_len == 0 {
                // Tree is now empty
                self.free_node(leaf_id);
                self.root = None;
                self.first_leaf = None;
            }
            return Ok(());
        }

        // Check if underflow
        if leaf_len >= self.min_leaf_entries() {
            // No underflow, but update ancestor keys
            self.update_ancestor_keys(leaf_id, &path)?;
            return Ok(());
        }

        // Get parent and sibling info
        let (parent_id, child_idx) = path[path.len() - 1];
        let parent_path = path[..path.len() - 1].to_vec();

        let (sibling_id, sibling_is_left) = {
            let parent = self
                .get_node(parent_id)
                .and_then(|n| n.as_internal())
                .ok_or(BPlusTreeError::NodeNotFound(parent_id))?;

            if child_idx > 0 {
                (parent.children[child_idx - 1], true)
            } else if child_idx < parent.len() - 1 {
                (parent.children[child_idx + 1], false)
            } else {
                // No sibling - shouldn't happen in valid tree
                return Err(BPlusTreeError::InvalidState("No sibling found".to_string()));
            }
        };

        let sibling_len = self
            .get_node(sibling_id)
            .and_then(|n| n.as_leaf())
            .map(|l| l.len())
            .unwrap_or(0);

        if sibling_len > self.min_leaf_entries() {
            // Redistribute from sibling
            self.redistribute_leaves(leaf_id, sibling_id, sibling_is_left, parent_id, child_idx)?;
        } else {
            // Merge with sibling
            self.merge_leaves(
                leaf_id,
                sibling_id,
                sibling_is_left,
                parent_id,
                child_idx,
                parent_path,
            )?;
        }

        Ok(())
    }

    /// Redistribute entries between a leaf and its sibling
    fn redistribute_leaves(
        &mut self,
        leaf_id: NodeId,
        sibling_id: NodeId,
        sibling_is_left: bool,
        parent_id: NodeId,
        child_idx: usize,
    ) -> BPlusTreeResult<()> {
        if sibling_is_left {
            // Borrow from left sibling (take its last entry)
            let (key, value) = {
                let sibling = self
                    .get_node_mut(sibling_id)
                    .and_then(|n| n.as_leaf_mut())
                    .ok_or(BPlusTreeError::NodeNotFound(sibling_id))?;

                let key = sibling.keys.pop().unwrap();
                let value = sibling.values.pop().unwrap();
                (key, value)
            };

            // Add to current leaf at the beginning
            {
                let leaf = self
                    .get_node_mut(leaf_id)
                    .and_then(|n| n.as_leaf_mut())
                    .ok_or(BPlusTreeError::NodeNotFound(leaf_id))?;

                leaf.keys.insert(0, key);
                leaf.values.insert(0, value);
            }

            // Update parent key for left sibling
            let sibling_max = self
                .get_node(sibling_id)
                .and_then(|n| n.max_key())
                .unwrap_or(0);

            let parent = self
                .get_node_mut(parent_id)
                .and_then(|n| n.as_internal_mut())
                .ok_or(BPlusTreeError::NodeNotFound(parent_id))?;

            parent.keys[child_idx - 1] = sibling_max;
        } else {
            // Borrow from right sibling (take its first entry)
            let (key, value) = {
                let sibling = self
                    .get_node_mut(sibling_id)
                    .and_then(|n| n.as_leaf_mut())
                    .ok_or(BPlusTreeError::NodeNotFound(sibling_id))?;

                let key = sibling.keys.remove(0);
                let value = sibling.values.remove(0);
                (key, value)
            };

            // Add to current leaf at the end
            {
                let leaf = self
                    .get_node_mut(leaf_id)
                    .and_then(|n| n.as_leaf_mut())
                    .ok_or(BPlusTreeError::NodeNotFound(leaf_id))?;

                leaf.keys.push(key);
                leaf.values.push(value);
            }

            // Update parent key for current leaf
            let leaf_max = self
                .get_node(leaf_id)
                .and_then(|n| n.max_key())
                .unwrap_or(0);

            let parent = self
                .get_node_mut(parent_id)
                .and_then(|n| n.as_internal_mut())
                .ok_or(BPlusTreeError::NodeNotFound(parent_id))?;

            parent.keys[child_idx] = leaf_max;
        }

        Ok(())
    }

    /// Merge a leaf with its sibling
    fn merge_leaves(
        &mut self,
        leaf_id: NodeId,
        sibling_id: NodeId,
        sibling_is_left: bool,
        parent_id: NodeId,
        child_idx: usize,
        parent_path: Vec<(NodeId, usize)>,
    ) -> BPlusTreeResult<()> {
        // Determine which node is left and which is right
        let (left_id, right_id, remove_idx) = if sibling_is_left {
            (sibling_id, leaf_id, child_idx)
        } else {
            (leaf_id, sibling_id, child_idx + 1)
        };

        // Merge right into left
        {
            let right_entries: Vec<(BPlusKey, RecordId)> = {
                let right = self
                    .get_node(right_id)
                    .and_then(|n| n.as_leaf())
                    .ok_or(BPlusTreeError::NodeNotFound(right_id))?;

                right
                    .keys
                    .iter()
                    .copied()
                    .zip(right.values.iter().copied())
                    .collect()
            };

            let right_next = self
                .get_node(right_id)
                .and_then(|n| n.as_leaf())
                .and_then(|l| l.next);

            let left = self
                .get_node_mut(left_id)
                .and_then(|n| n.as_leaf_mut())
                .ok_or(BPlusTreeError::NodeNotFound(left_id))?;

            for (k, v) in right_entries {
                left.keys.push(k);
                left.values.push(v);
            }
            left.next = right_next;
        }

        // Update first_leaf if needed - when right node is being removed and was first_leaf
        // This shouldn't normally happen since left should come before right
        // But handle it just in case: if right_id is first_leaf, left should be new first
        // (though this is technically a bug in tree structure)
        if self.first_leaf == Some(right_id) {
            // This is wrong - if right was first_leaf, left should have been before it
            // Just set to left to avoid pointing to freed node
            self.first_leaf = Some(left_id);
        }

        // Free the right node
        self.free_node(right_id);

        // Remove from parent
        self.remove_from_parent(parent_id, remove_idx, left_id, parent_path)?;

        Ok(())
    }

    /// Remove a child from parent after merge
    fn remove_from_parent(
        &mut self,
        parent_id: NodeId,
        remove_idx: usize,
        _remaining_child_id: NodeId,
        parent_path: Vec<(NodeId, usize)>,
    ) -> BPlusTreeResult<()> {
        // Remove from parent and get the remaining child ID to update
        let remaining_child_id_to_update = {
            let parent = self
                .get_node_mut(parent_id)
                .and_then(|n| n.as_internal_mut())
                .ok_or(BPlusTreeError::NodeNotFound(parent_id))?;

            parent.keys.remove(remove_idx);
            parent.children.remove(remove_idx);

            // Get the remaining child that needs key update
            let remaining_idx = if remove_idx > 0 { remove_idx - 1 } else { 0 };
            if remaining_idx < parent.children.len() {
                Some((remaining_idx, parent.children[remaining_idx]))
            } else {
                None
            }
        };

        // Update the key for the remaining child
        if let Some((remaining_idx, child_id)) = remaining_child_id_to_update {
            let child_max = self
                .get_node(child_id)
                .and_then(|n| n.max_key())
                .unwrap_or(0);

            if let Some(parent) = self
                .get_node_mut(parent_id)
                .and_then(|n| n.as_internal_mut())
            {
                if remaining_idx < parent.keys.len() {
                    parent.keys[remaining_idx] = child_max;
                }
            }
        }

        // Check if parent is root with only one child
        let parent_len = self
            .get_node(parent_id)
            .and_then(|n| n.as_internal())
            .map(|n| n.len())
            .unwrap_or(0);

        if parent_path.is_empty() && parent_len == 1 {
            // Parent is root with one child - make child the new root
            let new_root = {
                let parent = self
                    .get_node(parent_id)
                    .and_then(|n| n.as_internal())
                    .ok_or(BPlusTreeError::NodeNotFound(parent_id))?;
                parent.children[0]
            };

            self.free_node(parent_id);
            self.root = Some(new_root);
            return Ok(());
        }

        // Check for underflow in parent
        if !parent_path.is_empty() && parent_len < self.min_internal_children() {
            self.handle_internal_underflow(parent_id, parent_path)?;
        } else if !parent_path.is_empty() {
            // Update ancestor keys
            self.update_ancestor_keys(parent_id, &parent_path)?;
        }

        Ok(())
    }

    /// Handle underflow in an internal node
    fn handle_internal_underflow(
        &mut self,
        node_id: NodeId,
        path: Vec<(NodeId, usize)>,
    ) -> BPlusTreeResult<()> {
        let (parent_id, child_idx) = path[path.len() - 1];
        let parent_path = path[..path.len() - 1].to_vec();

        let (sibling_id, sibling_is_left) = {
            let parent = self
                .get_node(parent_id)
                .and_then(|n| n.as_internal())
                .ok_or(BPlusTreeError::NodeNotFound(parent_id))?;

            if child_idx > 0 {
                (parent.children[child_idx - 1], true)
            } else if child_idx < parent.len() - 1 {
                (parent.children[child_idx + 1], false)
            } else {
                return Err(BPlusTreeError::InvalidState("No sibling found".to_string()));
            }
        };

        let sibling_len = self
            .get_node(sibling_id)
            .and_then(|n| n.as_internal())
            .map(|n| n.len())
            .unwrap_or(0);

        if sibling_len > self.min_internal_children() {
            // Redistribute from sibling
            self.redistribute_internal(node_id, sibling_id, sibling_is_left, parent_id, child_idx)?;
        } else {
            // Merge with sibling
            self.merge_internal(
                node_id,
                sibling_id,
                sibling_is_left,
                parent_id,
                child_idx,
                parent_path,
            )?;
        }

        Ok(())
    }

    /// Redistribute entries between an internal node and its sibling
    fn redistribute_internal(
        &mut self,
        node_id: NodeId,
        sibling_id: NodeId,
        sibling_is_left: bool,
        parent_id: NodeId,
        child_idx: usize,
    ) -> BPlusTreeResult<()> {
        if sibling_is_left {
            // Borrow from left sibling (take its last child)
            let (key, child) = {
                let sibling = self
                    .get_node_mut(sibling_id)
                    .and_then(|n| n.as_internal_mut())
                    .ok_or(BPlusTreeError::NodeNotFound(sibling_id))?;

                let key = sibling.keys.pop().unwrap();
                let child = sibling.children.pop().unwrap();
                (key, child)
            };

            // Add to current node at the beginning
            {
                let node = self
                    .get_node_mut(node_id)
                    .and_then(|n| n.as_internal_mut())
                    .ok_or(BPlusTreeError::NodeNotFound(node_id))?;

                node.keys.insert(0, key);
                node.children.insert(0, child);
            }

            // Update parent key for left sibling
            let sibling_max = self
                .get_node(sibling_id)
                .and_then(|n| n.max_key())
                .unwrap_or(0);

            let parent = self
                .get_node_mut(parent_id)
                .and_then(|n| n.as_internal_mut())
                .ok_or(BPlusTreeError::NodeNotFound(parent_id))?;

            parent.keys[child_idx - 1] = sibling_max;
        } else {
            // Borrow from right sibling (take its first child)
            let (key, child) = {
                let sibling = self
                    .get_node_mut(sibling_id)
                    .and_then(|n| n.as_internal_mut())
                    .ok_or(BPlusTreeError::NodeNotFound(sibling_id))?;

                let key = sibling.keys.remove(0);
                let child = sibling.children.remove(0);
                (key, child)
            };

            // Add to current node at the end
            {
                let node = self
                    .get_node_mut(node_id)
                    .and_then(|n| n.as_internal_mut())
                    .ok_or(BPlusTreeError::NodeNotFound(node_id))?;

                node.keys.push(key);
                node.children.push(child);
            }

            // Update parent key for current node
            let node_max = self
                .get_node(node_id)
                .and_then(|n| n.max_key())
                .unwrap_or(0);

            let parent = self
                .get_node_mut(parent_id)
                .and_then(|n| n.as_internal_mut())
                .ok_or(BPlusTreeError::NodeNotFound(parent_id))?;

            parent.keys[child_idx] = node_max;
        }

        Ok(())
    }

    /// Merge an internal node with its sibling
    fn merge_internal(
        &mut self,
        node_id: NodeId,
        sibling_id: NodeId,
        sibling_is_left: bool,
        parent_id: NodeId,
        child_idx: usize,
        parent_path: Vec<(NodeId, usize)>,
    ) -> BPlusTreeResult<()> {
        // Determine which node is left and which is right
        let (left_id, right_id, remove_idx) = if sibling_is_left {
            (sibling_id, node_id, child_idx)
        } else {
            (node_id, sibling_id, child_idx + 1)
        };

        // Merge right into left
        {
            let (right_keys, right_children): (Vec<BPlusKey>, Vec<NodeId>) = {
                let right = self
                    .get_node(right_id)
                    .and_then(|n| n.as_internal())
                    .ok_or(BPlusTreeError::NodeNotFound(right_id))?;

                (right.keys.clone(), right.children.clone())
            };

            let left = self
                .get_node_mut(left_id)
                .and_then(|n| n.as_internal_mut())
                .ok_or(BPlusTreeError::NodeNotFound(left_id))?;

            left.keys.extend(right_keys);
            left.children.extend(right_children);
        }

        // Free the right node
        self.free_node(right_id);

        // Remove from parent
        self.remove_from_parent(parent_id, remove_idx, left_id, parent_path)?;

        Ok(())
    }

    // ========== Iterator ==========

    /// Iterate over all entries in key order
    pub fn iter(&self) -> BPlusTreeIter<'_> {
        BPlusTreeIter::new(self)
    }
}

/// Iterator over B+ tree entries
pub struct BPlusTreeIter<'a> {
    tree: &'a BPlusTree,
    current_leaf: Option<NodeId>,
    current_idx: usize,
}

impl<'a> BPlusTreeIter<'a> {
    fn new(tree: &'a BPlusTree) -> Self {
        Self {
            tree,
            current_leaf: tree.first_leaf,
            current_idx: 0,
        }
    }
}

impl Iterator for BPlusTreeIter<'_> {
    type Item = (BPlusKey, RecordId);

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            let leaf_id = self.current_leaf?;
            let leaf = self.tree.get_node(leaf_id)?.as_leaf()?;

            if self.current_idx < leaf.len() {
                let key = leaf.keys[self.current_idx];
                let value = leaf.values[self.current_idx];
                self.current_idx += 1;
                return Some((key, value));
            }

            // Move to next leaf
            self.current_leaf = leaf.next;
            self.current_idx = 0;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn rid(page: usize, slot: usize) -> RecordId {
        RecordId::new(page, slot)
    }

    #[test]
    fn test_new_tree() {
        let tree = BPlusTree::new(4).unwrap();
        assert!(tree.is_empty());
        assert_eq!(tree.len(), 0);
        assert_eq!(tree.height(), 0);
        assert_eq!(tree.order(), 4);
    }

    #[test]
    fn test_invalid_order() {
        assert!(BPlusTree::new(2).is_err());
        assert!(BPlusTree::new(1).is_err());
        assert!(BPlusTree::new(0).is_err());
    }

    #[test]
    fn test_single_insert_and_search() {
        let mut tree = BPlusTree::new(4).unwrap();

        tree.insert(42, rid(1, 0)).unwrap();

        assert!(!tree.is_empty());
        assert_eq!(tree.len(), 1);
        assert_eq!(tree.height(), 1);
        assert_eq!(tree.search(42), Some(rid(1, 0)));
        assert_eq!(tree.search(41), None);
    }

    #[test]
    fn test_multiple_inserts_no_split() {
        let mut tree = BPlusTree::new(4).unwrap();

        // Order 4 means max 3 entries per leaf
        tree.insert(10, rid(1, 0)).unwrap();
        tree.insert(20, rid(1, 1)).unwrap();
        tree.insert(30, rid(1, 2)).unwrap();

        assert_eq!(tree.len(), 3);
        assert_eq!(tree.height(), 1);
        assert_eq!(tree.search(10), Some(rid(1, 0)));
        assert_eq!(tree.search(20), Some(rid(1, 1)));
        assert_eq!(tree.search(30), Some(rid(1, 2)));
    }

    #[test]
    fn test_leaf_split() {
        let mut tree = BPlusTree::new(4).unwrap();

        // Insert 4 entries to trigger split (max 3 per leaf)
        tree.insert(10, rid(1, 0)).unwrap();
        tree.insert(20, rid(1, 1)).unwrap();
        tree.insert(30, rid(1, 2)).unwrap();
        tree.insert(40, rid(1, 3)).unwrap();

        assert_eq!(tree.len(), 4);
        assert_eq!(tree.height(), 2);

        // All entries should still be searchable
        assert_eq!(tree.search(10), Some(rid(1, 0)));
        assert_eq!(tree.search(20), Some(rid(1, 1)));
        assert_eq!(tree.search(30), Some(rid(1, 2)));
        assert_eq!(tree.search(40), Some(rid(1, 3)));
    }

    #[test]
    fn test_multiple_splits() {
        let mut tree = BPlusTree::new(4).unwrap();

        // Insert enough entries to cause multiple splits
        for i in 0..20 {
            tree.insert(i * 10, rid(1, i as usize)).unwrap();
        }

        assert_eq!(tree.len(), 20);
        assert!(tree.height() >= 2);

        // All entries should be searchable
        for i in 0..20 {
            assert_eq!(tree.search(i * 10), Some(rid(1, i as usize)));
        }
    }

    #[test]
    fn test_duplicate_keys() {
        let mut tree = BPlusTree::new(4).unwrap();

        tree.insert(10, rid(1, 0)).unwrap();
        tree.insert(10, rid(1, 1)).unwrap();
        tree.insert(10, rid(1, 2)).unwrap();

        assert_eq!(tree.len(), 3);

        let results = tree.search_all(10);
        assert_eq!(results.len(), 3);
        assert!(results.contains(&rid(1, 0)));
        assert!(results.contains(&rid(1, 1)));
        assert!(results.contains(&rid(1, 2)));
    }

    #[test]
    fn test_range_search() {
        let mut tree = BPlusTree::new(4).unwrap();

        for i in 0..10 {
            tree.insert(i * 10, rid(1, i as usize)).unwrap();
        }

        let results = tree.range_search(25, 55);
        assert_eq!(results.len(), 3);
        assert!(results.contains(&(30, rid(1, 3))));
        assert!(results.contains(&(40, rid(1, 4))));
        assert!(results.contains(&(50, rid(1, 5))));
    }

    #[test]
    fn test_range_search_empty() {
        let mut tree = BPlusTree::new(4).unwrap();

        for i in 0..10 {
            tree.insert(i * 10, rid(1, i as usize)).unwrap();
        }

        // Range with no entries
        let results = tree.range_search(100, 200);
        assert!(results.is_empty());

        // Reversed range
        let results = tree.range_search(50, 20);
        assert!(results.is_empty());
    }

    #[test]
    fn test_delete_single() {
        let mut tree = BPlusTree::new(4).unwrap();

        tree.insert(10, rid(1, 0)).unwrap();
        tree.insert(20, rid(1, 1)).unwrap();
        tree.insert(30, rid(1, 2)).unwrap();

        assert!(tree.delete(20).unwrap());
        assert_eq!(tree.len(), 2);
        assert_eq!(tree.search(20), None);
        assert_eq!(tree.search(10), Some(rid(1, 0)));
        assert_eq!(tree.search(30), Some(rid(1, 2)));
    }

    #[test]
    fn test_delete_not_found() {
        let mut tree = BPlusTree::new(4).unwrap();

        tree.insert(10, rid(1, 0)).unwrap();

        assert!(!tree.delete(20).unwrap());
        assert_eq!(tree.len(), 1);
    }

    #[test]
    fn test_delete_entry() {
        let mut tree = BPlusTree::new(4).unwrap();

        tree.insert(10, rid(1, 0)).unwrap();
        tree.insert(10, rid(1, 1)).unwrap();
        tree.insert(10, rid(1, 2)).unwrap();

        assert!(tree.delete_entry(10, rid(1, 1)).unwrap());
        assert_eq!(tree.len(), 2);

        let results = tree.search_all(10);
        assert_eq!(results.len(), 2);
        assert!(results.contains(&rid(1, 0)));
        assert!(results.contains(&rid(1, 2)));
    }

    #[test]
    fn test_delete_until_empty() {
        let mut tree = BPlusTree::new(4).unwrap();

        tree.insert(10, rid(1, 0)).unwrap();
        tree.insert(20, rid(1, 1)).unwrap();
        tree.insert(30, rid(1, 2)).unwrap();

        tree.delete(10).unwrap();
        tree.delete(20).unwrap();
        tree.delete(30).unwrap();

        assert!(tree.is_empty());
        assert_eq!(tree.len(), 0);
        assert_eq!(tree.height(), 0);
    }

    #[test]
    fn test_delete_with_redistribution() {
        let mut tree = BPlusTree::new(4).unwrap();

        // Insert enough entries to create multiple leaves
        for i in 0..8 {
            tree.insert(i * 10, rid(1, i as usize)).unwrap();
        }

        // Delete entries to trigger redistribution
        for i in 0..4 {
            tree.delete(i * 10).unwrap();
        }

        assert_eq!(tree.len(), 4);

        // Remaining entries should be searchable
        for i in 4..8 {
            assert_eq!(tree.search(i * 10), Some(rid(1, i as usize)));
        }
    }

    #[test]
    fn test_delete_with_merge() {
        let mut tree = BPlusTree::new(4).unwrap();

        // Insert entries
        for i in 0..10 {
            tree.insert(i * 10, rid(1, i as usize)).unwrap();
        }

        let initial_height = tree.height();

        // Delete entries to trigger merges
        for i in 0..7 {
            tree.delete(i * 10).unwrap();
        }

        assert_eq!(tree.len(), 3);
        assert!(tree.height() <= initial_height);

        // Remaining entries should be searchable
        for i in 7..10 {
            assert_eq!(tree.search(i * 10), Some(rid(1, i as usize)));
        }
    }

    #[test]
    fn test_iterator() {
        let mut tree = BPlusTree::new(4).unwrap();

        // Insert in random order
        for i in [5, 2, 8, 1, 9, 3, 7, 4, 6, 0] {
            tree.insert(i, rid(1, i as usize)).unwrap();
        }

        // Iterator should return in sorted order
        let entries: Vec<_> = tree.iter().collect();
        assert_eq!(entries.len(), 10);

        for i in 0..10 {
            assert_eq!(entries[i], (i as i64, rid(1, i)));
        }
    }

    #[test]
    fn test_negative_keys() {
        let mut tree = BPlusTree::new(4).unwrap();

        tree.insert(-10, rid(1, 0)).unwrap();
        tree.insert(0, rid(1, 1)).unwrap();
        tree.insert(10, rid(1, 2)).unwrap();

        assert_eq!(tree.search(-10), Some(rid(1, 0)));
        assert_eq!(tree.search(0), Some(rid(1, 1)));
        assert_eq!(tree.search(10), Some(rid(1, 2)));

        let results = tree.range_search(-15, 5);
        assert_eq!(results.len(), 2);
    }

    #[test]
    fn test_stress_insert_delete() {
        let mut tree = BPlusTree::new(4).unwrap();

        // Insert 100 entries
        for i in 0..100 {
            tree.insert(i, rid(1, i as usize)).unwrap();
        }

        assert_eq!(tree.len(), 100);

        // Delete every other entry
        for i in (0..100).step_by(2) {
            tree.delete(i).unwrap();
        }

        assert_eq!(tree.len(), 50);

        // Verify remaining entries
        for i in 0..100 {
            if i % 2 == 0 {
                assert_eq!(tree.search(i), None);
            } else {
                assert_eq!(tree.search(i), Some(rid(1, i as usize)));
            }
        }
    }

    #[test]
    fn test_delete_stress_debug() {
        // This test verifies that search works correctly after multiple deletions
        // which trigger tree rebalancing (redistribution/merge operations)
        let mut tree = BPlusTree::new(4).unwrap();

        // Insert 20 entries to create a multi-level tree
        for i in 0..20 {
            tree.insert(i, rid(1, i as usize)).unwrap();
        }
        assert_eq!(tree.len(), 20);
        assert!(tree.height() >= 2);

        // Delete every other entry starting from 0
        for i in (0..20).step_by(2) {
            tree.delete(i).unwrap();

            // Verify all remaining entries are still searchable
            for j in 0..20 {
                let should_exist = j % 2 != 0 || j > i;
                let found = tree.search(j).is_some();
                assert_eq!(
                    found, should_exist,
                    "After delete({}): Key {} should_exist={} but found={}",
                    i, j, should_exist, found
                );
            }
        }

        assert_eq!(tree.len(), 10);
    }

    #[test]
    fn test_large_tree() {
        let mut tree = BPlusTree::new(4).unwrap();

        // Insert 1000 entries
        for i in 0..1000 {
            tree.insert(i, rid(i as usize / 100, i as usize % 100))
                .unwrap();
        }

        assert_eq!(tree.len(), 1000);
        assert!(tree.height() >= 3);

        // Verify all entries are searchable
        for i in 0..1000 {
            assert_eq!(
                tree.search(i),
                Some(rid(i as usize / 100, i as usize % 100))
            );
        }

        // Test range query
        let results = tree.range_search(500, 510);
        assert_eq!(results.len(), 11);
    }

    #[test]
    fn test_first_leaf_tracking() {
        let mut tree = BPlusTree::new(4).unwrap();

        // Insert entries
        for i in [50, 30, 70, 20, 40, 60, 80, 10] {
            tree.insert(i, rid(1, i as usize)).unwrap();
        }

        // First entry from iterator should be minimum
        let first = tree.iter().next().unwrap();
        assert_eq!(first.0, 10);
    }
}
