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
///
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

    pub(crate) fn from_persistent_state(
        order: usize,
        root: Option<NodeId>,
        first_leaf: Option<NodeId>,
        entry_count: usize,
        nodes: Vec<Option<BPlusNode>>,
    ) -> BPlusTreeResult<Self> {
        if order < 3 {
            return Err(BPlusTreeError::InvalidOrder(order));
        }

        let mut free_list = Vec::new();
        for (idx, node) in nodes.iter().enumerate() {
            if node.is_none() {
                free_list.push(idx);
            }
        }

        Ok(Self {
            root,
            order,
            nodes,
            free_list,
            first_leaf,
            entry_count,
        })
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

    /// Calculate the ideal tree depth based on number of entries and tree order
    ///
    /// # Arguments
    /// * `entry_count` - Number of entries that will be stored
    /// * `order` - Tree order (max children per internal node)
    ///
    /// # Returns
    /// Optimal tree depth (1 for all entries in one leaf, 2+ for multi-level trees)
    pub fn calculate_optimal_depth(entry_count: usize, order: usize) -> usize {
        if entry_count == 0 {
            return 0;
        }

        let max_leaf_entries = order - 1;
        if entry_count <= max_leaf_entries {
            return 1; // All entries fit in a single leaf
        }

        // Calculate minimum number of leaf nodes needed
        let leaf_count = entry_count.div_ceil(max_leaf_entries);

        // Calculate depth: each internal level can have up to 'order' children
        let mut depth = 1; // Start with leaf level
        let mut nodes_at_level = leaf_count;

        while nodes_at_level > 1 {
            nodes_at_level = nodes_at_level.div_ceil(order);
            depth += 1;
        }

        depth
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
            if let Some(min_key) = leaf.min_key()
                && min_key > key
            {
                break;
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

    /// Efficiently build a B+ tree from pre-sorted entries
    ///
    /// This method is significantly faster than repeated individual inserts when loading
    /// a large amount of data. It requires that entries are already sorted by key.
    ///
    /// # Arguments
    /// * `entries` - Iterator of (key, value) pairs in ascending key order
    ///
    /// # Performance
    /// Time complexity: O(n) where n is the number of entries
    /// Space complexity: O(d * m) where d is tree depth and m is order
    ///
    /// # Panics
    /// If entries are not sorted in ascending order
    ///
    /// # Example
    /// ```ignore
    /// let mut tree = BPlusTree::new(500)?;
    /// let mut entries: Vec<(i64, RecordId)> = /* collect data */;
    /// entries.sort_by_key(|e| e.0); // MUST sort first
    /// tree.bulk_load(entries.into_iter())?;
    /// ```
    pub fn bulk_load<I>(&mut self, entries: I) -> BPlusTreeResult<()>
    where
        I: Iterator<Item = (BPlusKey, RecordId)>,
    {
        // Collect entries into a Vec - we need random access for bulk loading
        let all_entries: Vec<(BPlusKey, RecordId)> = entries.collect();
        self.bulk_load_from_slice(&all_entries)
    }

    /// Bulk load from a pre-sorted slice (more memory efficient)
    pub fn bulk_load_from_slice(
        &mut self,
        all_entries: &[(BPlusKey, RecordId)],
    ) -> BPlusTreeResult<()> {
        // Clear existing tree
        self.root = None;
        self.nodes.clear();
        self.free_list.clear();
        self.first_leaf = None;
        self.entry_count = 0;

        let max_leaf_entries = self.max_leaf_entries();
        let _min_leaf_entries = self.min_leaf_entries();
        let max_internal_children = self.max_internal_children();

        if all_entries.is_empty() {
            return Ok(());
        }

        // Verify sorted order
        for i in 1..all_entries.len() {
            if all_entries[i].0 < all_entries[i - 1].0 {
                return Err(BPlusTreeError::InvalidState(
                    "Bulk load requires sorted entries".to_string(),
                ));
            }
        }

        // Distribute entries into leaf nodes
        let mut leaves: Vec<NodeId> = Vec::new();
        let total_entries = all_entries.len();
        let mut entry_idx = 0;

        // Calculate number of leaves needed
        // We want to fill leaves as much as possible while respecting minimum constraints
        let ideal_leaf_count = total_entries.div_ceil(max_leaf_entries);
        let actual_leaf_count = ideal_leaf_count.max(1);

        // Distribute entries evenly across leaves
        for leaf_num in 0..actual_leaf_count {
            let mut current_leaf = LeafNode::new();

            // Calculate how many entries this leaf should get
            let remaining_entries = total_entries - entry_idx;
            let remaining_leaves = actual_leaf_count - leaf_num;

            // Try to distribute evenly, but respect max constraint
            let entries_for_this_leaf = if remaining_leaves == 1 {
                // Last leaf - take all remaining
                remaining_entries
            } else {
                // Distribute evenly among remaining leaves
                let avg = remaining_entries.div_ceil(remaining_leaves);
                avg.min(max_leaf_entries)
            };

            // Add entries to this leaf
            for _ in 0..entries_for_this_leaf {
                if entry_idx >= total_entries {
                    break;
                }
                let (key, value) = all_entries[entry_idx];
                current_leaf.keys.push(key);
                current_leaf.values.push(value);
                self.entry_count += 1;
                entry_idx += 1;
            }

            if !current_leaf.is_empty() {
                let leaf_id = self.allocate_node(BPlusNode::Leaf(current_leaf));
                leaves.push(leaf_id);
            }
        }

        // Link leaves together
        for i in 0..leaves.len() - 1 {
            let leaf = self
                .get_node_mut(leaves[i])
                .and_then(|n| n.as_leaf_mut())
                .ok_or(BPlusTreeError::NodeNotFound(leaves[i]))?;
            leaf.next = Some(leaves[i + 1]);
        }

        // Track first leaf
        self.first_leaf = Some(leaves[0]);

        // Build internal levels bottom-up
        let mut current_level = leaves;

        while current_level.len() > 1 {
            let mut next_level: Vec<NodeId> = Vec::new();
            let mut i = 0;
            let min_internal_children = self.min_internal_children();

            while i < current_level.len() {
                let mut keys = Vec::new();
                let mut children = Vec::new();

                // Calculate how many children to put in this node
                let remaining = current_level.len() - i;

                let num_children = if remaining <= max_internal_children {
                    // Last node in this level - take all remaining
                    remaining
                } else {
                    // Check if taking max_internal_children would leave too few for next node
                    let would_remain = remaining - max_internal_children;

                    if would_remain > 0 && would_remain < min_internal_children {
                        // Would violate minimum constraint on next node
                        // Split more evenly: give some to this node, some to next
                        // Ensure both nodes meet minimum constraint
                        let total_for_last_two = remaining;
                        // Try to distribute evenly while respecting min/max constraints
                        let first_half = total_for_last_two.div_ceil(2); // Round up
                        first_half
                            .min(max_internal_children)
                            .max(min_internal_children)
                    } else {
                        max_internal_children
                    }
                };

                // Collect children
                let end = i + num_children;
                for &child_id in &current_level[i..end] {
                    let child_max_key = self
                        .get_node(child_id)
                        .and_then(|n| n.max_key())
                        .ok_or(BPlusTreeError::NodeNotFound(child_id))?;

                    keys.push(child_max_key);
                    children.push(child_id);
                }

                // Create internal node
                let internal = InternalNode::new(keys, children);
                let internal_id = self.allocate_node(BPlusNode::Internal(internal));
                next_level.push(internal_id);

                i = end;
            }

            current_level = next_level;
        }

        // Set root
        self.root = Some(current_level[0]);

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
                && remaining_idx < parent.keys.len()
            {
                parent.keys[remaining_idx] = child_max;
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

    /// Comprehensive B+ tree structure validator
    ///
    /// Validates all B+ tree constraints:
    /// 1. Node size constraints (min/max children/entries)
    /// 2. Key ordering within nodes
    /// 3. Parent-child key relationships
    /// 4. Leaf linkage
    /// 5. All leaves at same depth
    fn validate_btree_structure(tree: &BPlusTree) -> Result<(), String> {
        if tree.is_empty() {
            return Ok(());
        }

        let root_id = tree.root_node_id().ok_or("Tree has no root")?;

        // Track all leaf depths to ensure they're all the same
        let mut leaf_depths = Vec::new();

        // Validate recursively from root
        validate_node(tree, root_id, None, 0, &mut leaf_depths)?;

        // Check all leaves are at same depth
        if !leaf_depths.is_empty() {
            let first_depth = leaf_depths[0];
            if !leaf_depths.iter().all(|&d| d == first_depth) {
                return Err(format!("Leaves at different depths: {:?}", leaf_depths));
            }
        }

        // Validate leaf linkage
        validate_leaf_chain(tree)?;

        Ok(())
    }

    fn validate_node(
        tree: &BPlusTree,
        node_id: NodeId,
        parent_id: Option<NodeId>,
        depth: usize,
        leaf_depths: &mut Vec<usize>,
    ) -> Result<(), String> {
        let node = tree
            .get_node(node_id)
            .ok_or(format!("Node {} not found", node_id))?;

        match node {
            BPlusNode::Leaf(leaf) => {
                // Record depth
                leaf_depths.push(depth);

                // Check size constraints
                let is_root = parent_id.is_none();
                if !is_root && leaf.len() < tree.min_leaf_entries() {
                    return Err(format!(
                        "Leaf node {} has {} entries, min is {}",
                        node_id,
                        leaf.len(),
                        tree.min_leaf_entries()
                    ));
                }
                if leaf.len() > tree.max_leaf_entries() {
                    return Err(format!(
                        "Leaf node {} has {} entries, max is {}",
                        node_id,
                        leaf.len(),
                        tree.max_leaf_entries()
                    ));
                }

                // Check key ordering
                for i in 1..leaf.keys.len() {
                    if leaf.keys[i] < leaf.keys[i - 1] {
                        return Err(format!(
                            "Leaf node {} has unsorted keys: {:?}",
                            node_id, leaf.keys
                        ));
                    }
                }

                Ok(())
            }
            BPlusNode::Internal(internal) => {
                // Check size constraints
                let is_root = parent_id.is_none();
                if !is_root && internal.len() < tree.min_internal_children() {
                    return Err(format!(
                        "Internal node {} has {} children, min is {}",
                        node_id,
                        internal.len(),
                        tree.min_internal_children()
                    ));
                }
                if is_root && internal.len() < 2 && !tree.is_empty() {
                    return Err(format!(
                        "Root internal node {} has {} children, min is 2",
                        node_id,
                        internal.len()
                    ));
                }
                if internal.len() > tree.max_internal_children() {
                    return Err(format!(
                        "Internal node {} has {} children, max is {}",
                        node_id,
                        internal.len(),
                        tree.max_internal_children()
                    ));
                }

                // Check keys match children count
                if internal.keys.len() != internal.children.len() {
                    return Err(format!(
                        "Internal node {} has {} keys but {} children",
                        node_id,
                        internal.keys.len(),
                        internal.children.len()
                    ));
                }

                // Check key ordering
                for i in 1..internal.keys.len() {
                    if internal.keys[i] < internal.keys[i - 1] {
                        return Err(format!(
                            "Internal node {} has unsorted keys: {:?}",
                            node_id, internal.keys
                        ));
                    }
                }

                // Validate each child and check parent-child key relationship
                for (i, &child_id) in internal.children.iter().enumerate() {
                    // Recursively validate child
                    validate_node(tree, child_id, Some(node_id), depth + 1, leaf_depths)?;

                    // Check that parent's key matches child's max key
                    let child_max = tree
                        .get_node(child_id)
                        .and_then(|n| n.max_key())
                        .ok_or(format!("Child {} has no max key", child_id))?;

                    if internal.keys[i] != child_max {
                        return Err(format!(
                            "Internal node {} key[{}] = {}, but child {} max = {}",
                            node_id, i, internal.keys[i], child_id, child_max
                        ));
                    }
                }

                Ok(())
            }
        }
    }

    fn validate_leaf_chain(tree: &BPlusTree) -> Result<(), String> {
        let mut current_id = tree.first_leaf_id();
        let mut visited = std::collections::HashSet::new();
        let mut prev_max_key: Option<i64> = None;

        while let Some(id) = current_id {
            // Check for cycles
            if visited.contains(&id) {
                return Err(format!("Cycle detected in leaf chain at node {}", id));
            }
            visited.insert(id);

            let leaf = tree
                .get_node(id)
                .and_then(|n| n.as_leaf())
                .ok_or(format!("Leaf {} not found or not a leaf", id))?;

            // Check ordering between leaves
            if let Some(prev_max) = prev_max_key {
                if let Some(curr_min) = leaf.min_key() {
                    if curr_min < prev_max {
                        return Err(format!(
                            "Leaf chain out of order: prev max = {}, curr min = {}",
                            prev_max, curr_min
                        ));
                    }
                }
            }

            prev_max_key = leaf.max_key();
            current_id = leaf.next;
        }

        Ok(())
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

    #[test]
    fn test_calculate_optimal_depth() {
        // Empty tree
        assert_eq!(BPlusTree::calculate_optimal_depth(0, 500), 0);

        // Single leaf (order=500, max_leaf_entries=499)
        assert_eq!(BPlusTree::calculate_optimal_depth(1, 500), 1);
        assert_eq!(BPlusTree::calculate_optimal_depth(100, 500), 1);
        assert_eq!(BPlusTree::calculate_optimal_depth(499, 500), 1);

        // Two levels (1 leaf + 1 internal)
        assert_eq!(BPlusTree::calculate_optimal_depth(500, 500), 2);
        assert_eq!(BPlusTree::calculate_optimal_depth(1000, 500), 2);

        // Three levels
        // 500 leaves * 499 entries = 249,500 entries max with 2 levels
        // So 249,501 entries needs 3 levels
        assert_eq!(BPlusTree::calculate_optimal_depth(250_000, 500), 3);

        // Smaller tree for easier verification
        // Order 4: max_leaf_entries = 3
        // 1 leaf: up to 3 entries (depth 1)
        // 2-4 leaves: up to 12 entries (depth 2)
        // 5-16 leaves: up to 48 entries (depth 3)
        assert_eq!(BPlusTree::calculate_optimal_depth(3, 4), 1);
        assert_eq!(BPlusTree::calculate_optimal_depth(4, 4), 2);
        assert_eq!(BPlusTree::calculate_optimal_depth(12, 4), 2);
        assert_eq!(BPlusTree::calculate_optimal_depth(13, 4), 3);
    }

    #[test]
    fn test_bulk_load_empty() {
        let mut tree = BPlusTree::new(4).unwrap();
        let entries: Vec<(i64, RecordId)> = vec![];

        tree.bulk_load(entries.into_iter()).unwrap();

        assert!(tree.is_empty());
        assert_eq!(tree.len(), 0);
        assert_eq!(tree.height(), 0);
    }

    #[test]
    fn test_bulk_load_single() {
        let mut tree = BPlusTree::new(4).unwrap();
        let entries = vec![(10, rid(1, 0))];

        tree.bulk_load(entries.into_iter()).unwrap();

        assert_eq!(tree.len(), 1);
        assert_eq!(tree.height(), 1);
        assert_eq!(tree.search(10), Some(rid(1, 0)));
    }

    #[test]
    fn test_bulk_load_small() {
        let mut tree = BPlusTree::new(4).unwrap();
        let mut entries: Vec<(i64, RecordId)> =
            (0..10).map(|i| (i * 10, rid(1, i as usize))).collect();

        tree.bulk_load(entries.into_iter()).unwrap();

        assert_eq!(tree.len(), 10);

        // Verify all entries are searchable
        for i in 0..10 {
            assert_eq!(tree.search(i * 10), Some(rid(1, i as usize)));
        }

        // Verify iterator returns sorted order
        let iter_entries: Vec<_> = tree.iter().collect();
        for i in 0..10 {
            assert_eq!(iter_entries[i], ((i * 10) as i64, rid(1, i as usize)));
        }
    }

    #[test]
    fn test_bulk_load_large() {
        let mut tree = BPlusTree::new(500).unwrap();
        let entries: Vec<(i64, RecordId)> = (0..10000)
            .map(|i| (i, rid(i as usize / 100, i as usize % 100)))
            .collect();

        tree.bulk_load(entries.into_iter()).unwrap();

        assert_eq!(tree.len(), 10000);

        // Verify tree structure is correct
        let depth = tree.height();
        let expected_depth = BPlusTree::calculate_optimal_depth(10000, 500);
        assert_eq!(depth, expected_depth);

        // Spot check some entries
        assert_eq!(tree.search(0), Some(rid(0, 0)));
        assert_eq!(tree.search(5000), Some(rid(50, 0)));
        assert_eq!(tree.search(9999), Some(rid(99, 99)));

        // Verify all entries via iterator
        let iter_entries: Vec<_> = tree.iter().collect();
        assert_eq!(iter_entries.len(), 10000);
        for i in 0..10000 {
            assert_eq!(iter_entries[i].0, i as i64);
        }
    }

    #[test]
    fn test_bulk_load_with_duplicates() {
        let mut tree = BPlusTree::new(4).unwrap();
        let entries = vec![
            (10, rid(1, 0)),
            (10, rid(1, 1)),
            (20, rid(2, 0)),
            (20, rid(2, 1)),
            (20, rid(2, 2)),
            (30, rid(3, 0)),
        ];

        tree.bulk_load(entries.into_iter()).unwrap();

        assert_eq!(tree.len(), 6);

        let results_10 = tree.search_all(10);
        assert_eq!(results_10.len(), 2);
        assert!(results_10.contains(&rid(1, 0)));
        assert!(results_10.contains(&rid(1, 1)));

        let results_20 = tree.search_all(20);
        assert_eq!(results_20.len(), 3);
    }

    #[test]
    fn test_bulk_load_unsorted_fails() {
        let mut tree = BPlusTree::new(4).unwrap();
        let entries = vec![
            (30, rid(1, 0)),
            (10, rid(1, 1)), // Out of order!
            (20, rid(1, 2)),
        ];

        let result = tree.bulk_load(entries.into_iter());
        assert!(result.is_err());

        // Tree should be empty after failed bulk load
        assert!(tree.is_empty());
    }

    #[test]
    fn test_bulk_load_vs_individual_inserts() {
        // Create two trees: one with bulk load, one with individual inserts
        let mut tree_bulk = BPlusTree::new(500).unwrap();
        let mut tree_individual = BPlusTree::new(500).unwrap();

        let entries: Vec<(i64, RecordId)> = (0..1000)
            .map(|i| (i, rid(i as usize / 100, i as usize % 100)))
            .collect();

        // Bulk load
        tree_bulk.bulk_load(entries.iter().copied()).unwrap();

        // Individual inserts
        for &(key, rid) in &entries {
            tree_individual.insert(key, rid).unwrap();
        }

        // Both should have same entries
        assert_eq!(tree_bulk.len(), tree_individual.len());
        assert_eq!(tree_bulk.len(), 1000);

        // Verify all entries are identical
        for i in 0..1000 {
            assert_eq!(tree_bulk.search(i), tree_individual.search(i));
        }

        // Iterators should produce same results
        let bulk_entries: Vec<_> = tree_bulk.iter().collect();
        let individual_entries: Vec<_> = tree_individual.iter().collect();
        assert_eq!(bulk_entries, individual_entries);
    }

    #[test]
    fn test_bulk_load_replaces_existing_tree() {
        let mut tree = BPlusTree::new(4).unwrap();

        // Insert some entries
        for i in 0..5 {
            tree.insert(i * 10, rid(1, i as usize)).unwrap();
        }

        assert_eq!(tree.len(), 5);

        // Bulk load new data (should replace existing)
        let new_entries: Vec<(i64, RecordId)> = (0..10).map(|i| (i, rid(2, i as usize))).collect();

        tree.bulk_load(new_entries.into_iter()).unwrap();

        // Old entries should be gone
        assert_eq!(tree.len(), 10);
        assert_eq!(tree.search(0), Some(rid(2, 0)));
        assert_eq!(tree.search(40), None);
    }

    #[test]
    fn test_bulk_load_negative_keys() {
        let mut tree = BPlusTree::new(4).unwrap();
        let entries = vec![
            (-50, rid(1, 0)),
            (-30, rid(1, 1)),
            (-10, rid(1, 2)),
            (0, rid(1, 3)),
            (10, rid(1, 4)),
            (30, rid(1, 5)),
        ];

        tree.bulk_load(entries.into_iter()).unwrap();

        assert_eq!(tree.len(), 6);
        assert_eq!(tree.search(-50), Some(rid(1, 0)));
        assert_eq!(tree.search(0), Some(rid(1, 3)));
        assert_eq!(tree.search(30), Some(rid(1, 5)));

        // Verify sorted order via iterator
        let first = tree.iter().next().unwrap();
        assert_eq!(first.0, -50);
    }

    #[test]
    fn test_btree_constraints_after_bulk_load_small() {
        let mut tree = BPlusTree::new(4).unwrap();
        let entries: Vec<(i64, RecordId)> = (0..10).map(|i| (i * 10, rid(1, i as usize))).collect();

        tree.bulk_load(entries.into_iter()).unwrap();

        // Validate all B+ tree constraints
        validate_btree_structure(&tree).expect("Tree structure validation failed");
    }

    #[test]
    fn test_btree_constraints_after_bulk_load_medium() {
        let mut tree = BPlusTree::new(10).unwrap();
        let entries: Vec<(i64, RecordId)> = (0..100)
            .map(|i| (i, rid(i as usize / 10, i as usize % 10)))
            .collect();

        tree.bulk_load(entries.into_iter()).unwrap();

        // Validate all B+ tree constraints
        validate_btree_structure(&tree).expect("Tree structure validation failed");
    }

    #[test]
    fn test_btree_constraints_after_bulk_load_large() {
        let mut tree = BPlusTree::new(50).unwrap();
        let entries: Vec<(i64, RecordId)> = (0..1000)
            .map(|i| (i, rid(i as usize / 100, i as usize % 100)))
            .collect();

        tree.bulk_load(entries.into_iter()).unwrap();

        // Validate all B+ tree constraints
        validate_btree_structure(&tree).expect("Tree structure validation failed");

        // Additional checks
        assert!(tree.height() >= 2, "Should be multi-level tree");
    }

    #[test]
    fn test_btree_constraints_after_bulk_load_very_large() {
        let mut tree = BPlusTree::new(500).unwrap();
        let entries: Vec<(i64, RecordId)> = (0..10000)
            .map(|i| (i, rid(i as usize / 100, i as usize % 100)))
            .collect();

        tree.bulk_load(entries.into_iter()).unwrap();

        // Validate all B+ tree constraints
        validate_btree_structure(&tree).expect("Tree structure validation failed");

        // Additional checks
        assert!(tree.height() >= 2, "Should be multi-level tree");
        assert_eq!(tree.len(), 10000);
    }

    #[test]
    fn test_btree_constraints_exact_leaf_capacity() {
        // Test with exactly max_leaf_entries
        let mut tree = BPlusTree::new(4).unwrap();
        let max_entries = tree.max_leaf_entries(); // 3 for order 4

        let entries: Vec<(i64, RecordId)> =
            (0..max_entries).map(|i| (i as i64, rid(1, i))).collect();

        tree.bulk_load(entries.into_iter()).unwrap();

        validate_btree_structure(&tree).expect("Tree structure validation failed");

        // Should fit in single leaf
        assert_eq!(tree.height(), 1);
    }

    #[test]
    fn test_btree_constraints_one_over_leaf_capacity() {
        // Test with max_leaf_entries + 1 (forces split)
        let mut tree = BPlusTree::new(4).unwrap();
        let max_entries = tree.max_leaf_entries(); // 3 for order 4

        let entries: Vec<(i64, RecordId)> =
            (0..=max_entries).map(|i| (i as i64, rid(1, i))).collect();

        tree.bulk_load(entries.into_iter()).unwrap();

        validate_btree_structure(&tree).expect("Tree structure validation failed");

        // Should have 2 levels
        assert_eq!(tree.height(), 2);
    }

    #[test]
    fn test_btree_constraints_with_duplicates() {
        let mut tree = BPlusTree::new(5).unwrap();
        let mut entries = Vec::new();

        // Add duplicates at various points
        for i in 0..20 {
            entries.push((i, rid(i as usize, 0)));
            if i % 5 == 0 {
                entries.push((i, rid(i as usize, 1)));
                entries.push((i, rid(i as usize, 2)));
            }
        }

        tree.bulk_load(entries.into_iter()).unwrap();

        validate_btree_structure(&tree).expect("Tree structure validation failed");
    }

    #[test]
    fn test_btree_constraints_after_individual_inserts() {
        // Compare: bulk load should produce same constraints as individual inserts
        let mut tree = BPlusTree::new(10).unwrap();

        // Individual inserts
        for i in 0..50 {
            tree.insert(i, rid(1, i as usize)).unwrap();
        }

        validate_btree_structure(&tree).expect("Tree structure validation failed after inserts");
    }

    #[test]
    fn test_btree_constraints_boundary_cases() {
        // Test various tree sizes around boundary conditions
        for order in [4, 5, 10, 50] {
            for size in [1, 2, 10, 50, 100] {
                let mut tree = BPlusTree::new(order).unwrap();
                let entries: Vec<(i64, RecordId)> =
                    (0..size).map(|i| (i, rid(1, i as usize))).collect();

                tree.bulk_load(entries.into_iter()).unwrap();

                validate_btree_structure(&tree).unwrap_or_else(|e| {
                    panic!(
                        "Validation failed for order={}, size={}: {}",
                        order, size, e
                    )
                });
            }
        }
    }

    #[test]
    fn test_node_size_constraints_maintained() {
        // Specifically test that no node violates size constraints
        let mut tree = BPlusTree::new(10).unwrap();
        let entries: Vec<(i64, RecordId)> = (0..200).map(|i| (i, rid(1, i as usize))).collect();

        tree.bulk_load(entries.into_iter()).unwrap();

        // Walk through all nodes and verify constraints
        let root_id = tree.root_node_id().unwrap();
        let mut to_check = vec![root_id];
        let mut visited = std::collections::HashSet::new();

        while let Some(node_id) = to_check.pop() {
            if visited.contains(&node_id) {
                continue;
            }
            visited.insert(node_id);

            let node = tree.get_node(node_id).unwrap();

            match node {
                BPlusNode::Leaf(leaf) => {
                    // Leaf must have <= max_leaf_entries
                    assert!(
                        leaf.len() <= tree.max_leaf_entries(),
                        "Leaf has {} entries, max is {}",
                        leaf.len(),
                        tree.max_leaf_entries()
                    );

                    // Non-root leaves must have >= min_leaf_entries
                    if node_id != root_id {
                        assert!(
                            leaf.len() >= tree.min_leaf_entries(),
                            "Non-root leaf has {} entries, min is {}",
                            leaf.len(),
                            tree.min_leaf_entries()
                        );
                    }
                }
                BPlusNode::Internal(internal) => {
                    // Internal must have <= max_internal_children
                    assert!(
                        internal.len() <= tree.max_internal_children(),
                        "Internal has {} children, max is {}",
                        internal.len(),
                        tree.max_internal_children()
                    );

                    // Root must have >= 2 children (if not a leaf)
                    if node_id == root_id {
                        assert!(
                            internal.len() >= 2,
                            "Root internal has {} children, min is 2",
                            internal.len()
                        );
                    } else {
                        // Non-root internal must have >= min_internal_children
                        assert!(
                            internal.len() >= tree.min_internal_children(),
                            "Non-root internal has {} children, min is {}",
                            internal.len(),
                            tree.min_internal_children()
                        );
                    }

                    // Add children to check
                    for &child_id in &internal.children {
                        to_check.push(child_id);
                    }
                }
            }
        }
    }
}
