//! Persistent B+ Tree backed by disk pages

use std::collections::HashSet;

use crate::btree::{BPlusNode, BPlusTree, NodeId};
use crate::file::{BufferManager, FileHandle, PagedFileManager};
use crate::record::RecordId;

use super::error::{IndexError, IndexResult};
use super::serialization::{
    BPlusTreeMetadata, deserialize_metadata, deserialize_node, serialize_metadata, serialize_node,
};

/// Persistent B+ Tree backed by disk pages
pub struct PersistentBPlusTree {
    /// In-memory B+ tree
    tree: BPlusTree,

    /// File handle for the index file
    file_handle: FileHandle,

    /// Dirty pages that need to be written back
    dirty_pages: HashSet<usize>,

    /// Whether metadata page (page 0) is dirty
    metadata_dirty: bool,
}

impl PersistentBPlusTree {
    /// Create a new index file
    pub fn create(
        buffer_mgr: &mut BufferManager,
        path: &str,
        order: usize,
    ) -> IndexResult<Self> {
        // Create the file
        buffer_mgr.file_manager_mut().create_file(path)?;
        let file_handle = buffer_mgr.file_manager_mut().open_file(path)?;

        // Create empty tree
        let tree = BPlusTree::new(order)?;

        // Write metadata to page 0
        let metadata = BPlusTreeMetadata {
            order,
            root_node_id: None,
            first_leaf_id: None,
            entry_count: 0,
            tree_height: 0,
            next_free_page: 1, // Page 1 is the first available data page
        };

        let metadata_bytes = serialize_metadata(&metadata);
        let page = buffer_mgr.get_page_mut(file_handle, 0)?;
        page.copy_from_slice(&metadata_bytes);

        Ok(Self {
            tree,
            file_handle,
            dirty_pages: HashSet::new(),
            metadata_dirty: false,
        })
    }

    /// Open an existing index file
    pub fn open(
        buffer_mgr: &mut BufferManager,
        path: &str,
    ) -> IndexResult<Self> {
        // Open the file
        let file_handle = buffer_mgr.file_manager_mut().open_file(path)?;

        // Read metadata from page 0
        let metadata_bytes = buffer_mgr.get_page(file_handle, 0)?;
        let metadata = deserialize_metadata(metadata_bytes)?;

        // Reconstruct the tree from disk
        let tree = BPlusTree::new(metadata.order)?;
        let mut nodes = Vec::new();

        // We need to load all nodes from disk
        // The challenge is: we don't know how many pages there are
        // Solution: Load nodes based on tree structure starting from root

        if let Some(root_id) = metadata.root_node_id {
            // Load all nodes recursively
            Self::load_tree_nodes(buffer_mgr, file_handle, root_id, &mut nodes)?;
        }

        // Set the tree's internal state
        // Note: This requires access to private fields of BPlusTree
        // We'll need to add a method to BPlusTree to set its state
        // For now, we'll rebuild by inserting all entries

        let mut persistent_tree = Self {
            tree,
            file_handle,
            dirty_pages: HashSet::new(),
            metadata_dirty: false,
        };

        // Store loaded nodes into tree's node storage
        persistent_tree.tree = Self::reconstruct_tree(metadata, nodes)?;

        Ok(persistent_tree)
    }

    /// Load all tree nodes recursively from disk
    fn load_tree_nodes(
        buffer_mgr: &mut BufferManager,
        file_handle: FileHandle,
        node_id: NodeId,
        nodes: &mut Vec<Option<BPlusNode>>,
    ) -> IndexResult<()> {
        // Ensure nodes vec is large enough
        while nodes.len() <= node_id {
            nodes.push(None);
        }

        // Node is stored at page (node_id + 1) because page 0 is metadata
        let page_id = node_id + 1;
        let page_bytes = buffer_mgr.get_page(file_handle, page_id)?;
        let node = deserialize_node(page_bytes)?;

        // If internal node, recursively load children
        if let BPlusNode::Internal(ref internal) = node {
            for &child_id in &internal.children {
                if child_id < nodes.len() && nodes[child_id].is_some() {
                    continue; // Already loaded
                }
                Self::load_tree_nodes(buffer_mgr, file_handle, child_id, nodes)?;
            }
        }

        nodes[node_id] = Some(node);
        Ok(())
    }

    /// Reconstruct B+ tree from loaded nodes
    fn reconstruct_tree(
        metadata: BPlusTreeMetadata,
        nodes: Vec<Option<BPlusNode>>,
    ) -> IndexResult<BPlusTree> {
        // Create a new tree with the correct order
        let mut tree = BPlusTree::new(metadata.order)?;

        // We need to insert all entries from the leaf nodes
        // Collect all entries by scanning leaf nodes
        let mut entries = Vec::new();

        if let Some(first_leaf_id) = metadata.first_leaf_id {
            let mut current_leaf_id = Some(first_leaf_id);

            while let Some(leaf_id) = current_leaf_id {
                if leaf_id >= nodes.len() {
                    break;
                }

                if let Some(BPlusNode::Leaf(ref leaf)) = nodes[leaf_id] {
                    for i in 0..leaf.len() {
                        entries.push((leaf.keys[i], leaf.values[i]));
                    }
                    current_leaf_id = leaf.next;
                } else {
                    break;
                }
            }
        }

        // Insert all entries into the new tree
        for (key, value) in entries {
            tree.insert(key, value)?;
        }

        Ok(tree)
    }

    /// Flush all dirty pages to disk
    pub fn flush(&mut self, buffer_mgr: &mut BufferManager) -> IndexResult<()> {
        // Write metadata if dirty
        if self.metadata_dirty {
            let metadata = BPlusTreeMetadata {
                order: self.tree.order(),
                root_node_id: self.get_root_node_id(),
                first_leaf_id: self.get_first_leaf_id(),
                entry_count: self.tree.len(),
                tree_height: self.tree.height(),
                next_free_page: 1, // Simplified: we don't track free pages yet
            };

            let metadata_bytes = serialize_metadata(&metadata);
            let page = buffer_mgr.get_page_mut(self.file_handle, 0)?;
            page.copy_from_slice(&metadata_bytes);
            self.metadata_dirty = false;
        }

        // Write all dirty node pages
        for &node_id in &self.dirty_pages {
            if let Some(node) = self.tree.get_node(node_id) {
                let node_bytes = serialize_node(node)?;
                let page_id = node_id + 1; // Page 0 is metadata
                let page = buffer_mgr.get_page_mut(self.file_handle, page_id)?;
                page.copy_from_slice(&node_bytes);
            }
        }

        self.dirty_pages.clear();

        // Ensure buffer manager flushes to disk
        buffer_mgr.flush_all()?;

        Ok(())
    }

    /// Close the index file (flushes automatically)
    pub fn close(mut self, buffer_mgr: &mut BufferManager) -> IndexResult<()> {
        self.flush(buffer_mgr)?;
        Ok(())
    }

    /// Insert a key-value pair
    pub fn insert(&mut self, key: i64, value: RecordId) -> IndexResult<()> {
        // Perform the insert
        self.tree.insert(key, value)?;

        // Mark metadata as dirty
        self.metadata_dirty = true;

        // Mark affected nodes as dirty
        // For simplicity, mark all nodes as dirty (can optimize later)
        self.mark_all_nodes_dirty();

        Ok(())
    }

    /// Delete all entries with the given key
    /// Returns whether any entries were deleted
    pub fn delete(&mut self, key: i64) -> IndexResult<bool> {
        let deleted = self.tree.delete(key)?;

        if deleted {
            self.metadata_dirty = true;
            self.mark_all_nodes_dirty();
        }

        Ok(deleted)
    }

    /// Delete a specific key-value pair
    pub fn delete_entry(&mut self, key: i64, value: RecordId) -> IndexResult<bool> {
        let deleted = self.tree.delete_entry(key, value)?;

        if deleted {
            self.metadata_dirty = true;
            self.mark_all_nodes_dirty();
        }

        Ok(deleted)
    }

    /// Search for a key (returns first match)
    pub fn search(&self, key: i64) -> Option<RecordId> {
        self.tree.search(key)
    }

    /// Search for all entries with the given key
    pub fn search_all(&self, key: i64) -> Vec<RecordId> {
        self.tree.search_all(key)
    }

    /// Range search [lower, upper]
    pub fn range_search(&self, lower: i64, upper: i64) -> Vec<(i64, RecordId)> {
        self.tree.range_search(lower, upper)
    }

    /// Update a specific entry
    pub fn update(
        &mut self,
        old_key: i64,
        old_value: RecordId,
        new_key: i64,
        new_value: RecordId,
    ) -> IndexResult<()> {
        // Delete old entry
        let deleted = self.tree.delete_entry(old_key, old_value)?;
        if !deleted {
            return Err(IndexError::DeserializationError(
                "Entry not found for update".to_string(),
            ));
        }

        // Insert new entry
        self.tree.insert(new_key, new_value)?;

        self.metadata_dirty = true;
        self.mark_all_nodes_dirty();

        Ok(())
    }

    /// Get number of entries
    pub fn len(&self) -> usize {
        self.tree.len()
    }

    /// Check if empty
    pub fn is_empty(&self) -> bool {
        self.tree.is_empty()
    }

    /// Get file handle
    pub fn file_handle(&self) -> FileHandle {
        self.file_handle
    }

    // Helper methods

    /// Mark all nodes as dirty (simplified approach)
    fn mark_all_nodes_dirty(&mut self) {
        for i in 0..self.tree.node_count() {
            if self.tree.get_node(i).is_some() {
                self.dirty_pages.insert(i);
            }
        }
    }

    /// Get root node ID from tree
    fn get_root_node_id(&self) -> Option<NodeId> {
        self.tree.root_node_id()
    }

    /// Get first leaf ID from tree
    fn get_first_leaf_id(&self) -> Option<NodeId> {
        self.tree.first_leaf_id()
    }
}

// We need to add some accessor methods to BPlusTree
// Let's check what methods we need to add
