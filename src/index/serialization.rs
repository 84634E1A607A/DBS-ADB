//! Serialization and deserialization for B+ tree nodes

use crate::btree::{BPlusNode, InternalNode, LeafNode};
use crate::file::PAGE_SIZE;
use crate::record::RecordId;

use super::error::{IndexError, IndexResult};

/// Magic number for index files: "BTRE" in ASCII
pub const MAGIC_NUMBER: u32 = 0x42545245;

/// Current index file version
pub const VERSION: u32 = 1;

/// Metadata stored in page 0
#[derive(Debug, Clone)]
pub struct BPlusTreeMetadata {
    pub order: usize,
    pub root_node_id: Option<usize>,
    pub first_leaf_id: Option<usize>,
    pub entry_count: usize,
    pub tree_height: usize,
    pub next_free_page: u32,
}

/// Serialize metadata to page 0
pub fn serialize_metadata(metadata: &BPlusTreeMetadata) -> Vec<u8> {
    let mut buf = vec![0u8; PAGE_SIZE];
    let mut offset = 0;

    // Magic number (4 bytes)
    buf[offset..offset + 4].copy_from_slice(&MAGIC_NUMBER.to_le_bytes());
    offset += 4;

    // Version (4 bytes)
    buf[offset..offset + 4].copy_from_slice(&VERSION.to_le_bytes());
    offset += 4;

    // Order (4 bytes)
    buf[offset..offset + 4].copy_from_slice(&(metadata.order as u32).to_le_bytes());
    offset += 4;

    // Root node ID (4 bytes, 0xFFFFFFFF = None)
    let root_id = metadata
        .root_node_id
        .map(|id| id as u32)
        .unwrap_or(u32::MAX);
    buf[offset..offset + 4].copy_from_slice(&root_id.to_le_bytes());
    offset += 4;

    // First leaf ID (4 bytes, 0xFFFFFFFF = None)
    let first_leaf = metadata
        .first_leaf_id
        .map(|id| id as u32)
        .unwrap_or(u32::MAX);
    buf[offset..offset + 4].copy_from_slice(&first_leaf.to_le_bytes());
    offset += 4;

    // Entry count (8 bytes)
    buf[offset..offset + 8].copy_from_slice(&(metadata.entry_count as u64).to_le_bytes());
    offset += 8;

    // Tree height (4 bytes)
    buf[offset..offset + 4].copy_from_slice(&(metadata.tree_height as u32).to_le_bytes());
    offset += 4;

    // Next free page (4 bytes)
    buf[offset..offset + 4].copy_from_slice(&metadata.next_free_page.to_le_bytes());

    buf
}

/// Deserialize metadata from page 0
pub fn deserialize_metadata(buf: &[u8]) -> IndexResult<BPlusTreeMetadata> {
    if buf.len() < PAGE_SIZE {
        return Err(IndexError::DeserializationError(
            "Buffer too small for metadata".to_string(),
        ));
    }

    let mut offset = 0;

    // Magic number
    let magic = u32::from_le_bytes([
        buf[offset],
        buf[offset + 1],
        buf[offset + 2],
        buf[offset + 3],
    ]);
    if magic != MAGIC_NUMBER {
        return Err(IndexError::InvalidMagic);
    }
    offset += 4;

    // Version
    let version = u32::from_le_bytes([
        buf[offset],
        buf[offset + 1],
        buf[offset + 2],
        buf[offset + 3],
    ]);
    if version != VERSION {
        return Err(IndexError::UnsupportedVersion(version));
    }
    offset += 4;

    // Order
    let order = u32::from_le_bytes([
        buf[offset],
        buf[offset + 1],
        buf[offset + 2],
        buf[offset + 3],
    ]) as usize;
    offset += 4;

    // Root node ID
    let root_id = u32::from_le_bytes([
        buf[offset],
        buf[offset + 1],
        buf[offset + 2],
        buf[offset + 3],
    ]);
    let root_node_id = if root_id == u32::MAX {
        None
    } else {
        Some(root_id as usize)
    };
    offset += 4;

    // First leaf ID
    let first_leaf = u32::from_le_bytes([
        buf[offset],
        buf[offset + 1],
        buf[offset + 2],
        buf[offset + 3],
    ]);
    let first_leaf_id = if first_leaf == u32::MAX {
        None
    } else {
        Some(first_leaf as usize)
    };
    offset += 4;

    // Entry count
    let entry_count = u64::from_le_bytes([
        buf[offset],
        buf[offset + 1],
        buf[offset + 2],
        buf[offset + 3],
        buf[offset + 4],
        buf[offset + 5],
        buf[offset + 6],
        buf[offset + 7],
    ]) as usize;
    offset += 8;

    // Tree height
    let tree_height = u32::from_le_bytes([
        buf[offset],
        buf[offset + 1],
        buf[offset + 2],
        buf[offset + 3],
    ]) as usize;
    offset += 4;

    // Next free page
    let next_free_page = u32::from_le_bytes([
        buf[offset],
        buf[offset + 1],
        buf[offset + 2],
        buf[offset + 3],
    ]);

    Ok(BPlusTreeMetadata {
        order,
        root_node_id,
        first_leaf_id,
        entry_count,
        tree_height,
        next_free_page,
    })
}

/// Serialize an internal node to bytes
pub fn serialize_internal_node(node: &InternalNode) -> IndexResult<Vec<u8>> {
    let mut buf = vec![0u8; PAGE_SIZE];
    let mut offset = 0;

    // Node type (1 byte): 0 = Internal
    buf[offset] = 0;
    offset += 1;

    // Entry count (2 bytes)
    let entry_count = node.len() as u16;
    buf[offset..offset + 2].copy_from_slice(&entry_count.to_le_bytes());
    offset += 2;

    // Reserved (13 bytes)
    offset += 13;

    // Now at offset 16, write keys and children
    for i in 0..node.len() {
        // Key (8 bytes)
        buf[offset..offset + 8].copy_from_slice(&node.keys[i].to_le_bytes());
        offset += 8;

        // Child ID (4 bytes)
        buf[offset..offset + 4].copy_from_slice(&(node.children[i] as u32).to_le_bytes());
        offset += 4;
    }

    Ok(buf)
}

/// Serialize a leaf node to bytes
pub fn serialize_leaf_node(node: &LeafNode) -> IndexResult<Vec<u8>> {
    let mut buf = vec![0u8; PAGE_SIZE];
    let mut offset = 0;

    // Node type (1 byte): 1 = Leaf
    buf[offset] = 1;
    offset += 1;

    // Entry count (2 bytes)
    let entry_count = node.len() as u16;
    buf[offset..offset + 2].copy_from_slice(&entry_count.to_le_bytes());
    offset += 2;

    // Next leaf (4 bytes, 0xFFFFFFFF = None)
    let next = node.next.map(|id| id as u32).unwrap_or(u32::MAX);
    buf[offset..offset + 4].copy_from_slice(&next.to_le_bytes());
    offset += 4;

    // Reserved (9 bytes)
    offset += 9;

    // Now at offset 16, write key-value pairs
    for i in 0..node.len() {
        // Key (8 bytes)
        buf[offset..offset + 8].copy_from_slice(&node.keys[i].to_le_bytes());
        offset += 8;

        // Value: RecordId (page_id: 4 bytes, slot_id: 4 bytes)
        buf[offset..offset + 4].copy_from_slice(&(node.values[i].page_id as u32).to_le_bytes());
        offset += 4;
        buf[offset..offset + 4].copy_from_slice(&(node.values[i].slot_id as u32).to_le_bytes());
        offset += 4;
    }

    Ok(buf)
}

/// Serialize a B+ tree node to bytes
pub fn serialize_node(node: &BPlusNode) -> IndexResult<Vec<u8>> {
    match node {
        BPlusNode::Internal(internal) => serialize_internal_node(internal),
        BPlusNode::Leaf(leaf) => serialize_leaf_node(leaf),
    }
}

/// Deserialize an internal node from bytes
pub fn deserialize_internal_node(buf: &[u8]) -> IndexResult<InternalNode> {
    if buf.len() < PAGE_SIZE {
        return Err(IndexError::DeserializationError(
            "Buffer too small for node".to_string(),
        ));
    }

    let mut offset = 0;

    // Node type (should be 0)
    let node_type = buf[offset];
    if node_type != 0 {
        return Err(IndexError::InvalidNodeType(node_type));
    }
    offset += 1;

    // Entry count
    let entry_count = u16::from_le_bytes([buf[offset], buf[offset + 1]]) as usize;
    offset += 2;

    // Skip reserved
    offset += 13;

    // Read keys and children
    let mut keys = Vec::with_capacity(entry_count);
    let mut children = Vec::with_capacity(entry_count);

    for _ in 0..entry_count {
        // Key
        let key = i64::from_le_bytes([
            buf[offset],
            buf[offset + 1],
            buf[offset + 2],
            buf[offset + 3],
            buf[offset + 4],
            buf[offset + 5],
            buf[offset + 6],
            buf[offset + 7],
        ]);
        offset += 8;

        // Child ID
        let child = u32::from_le_bytes([
            buf[offset],
            buf[offset + 1],
            buf[offset + 2],
            buf[offset + 3],
        ]) as usize;
        offset += 4;

        keys.push(key);
        children.push(child);
    }

    Ok(InternalNode::new(keys, children))
}

/// Deserialize a leaf node from bytes
pub fn deserialize_leaf_node(buf: &[u8]) -> IndexResult<LeafNode> {
    if buf.len() < PAGE_SIZE {
        return Err(IndexError::DeserializationError(
            "Buffer too small for node".to_string(),
        ));
    }

    let mut offset = 0;

    // Node type (should be 1)
    let node_type = buf[offset];
    if node_type != 1 {
        return Err(IndexError::InvalidNodeType(node_type));
    }
    offset += 1;

    // Entry count
    let entry_count = u16::from_le_bytes([buf[offset], buf[offset + 1]]) as usize;
    offset += 2;

    // Next leaf
    let next = u32::from_le_bytes([
        buf[offset],
        buf[offset + 1],
        buf[offset + 2],
        buf[offset + 3],
    ]);
    let next_leaf = if next == u32::MAX {
        None
    } else {
        Some(next as usize)
    };
    offset += 4;

    // Skip reserved
    offset += 9;

    // Read key-value pairs
    let mut keys = Vec::with_capacity(entry_count);
    let mut values = Vec::with_capacity(entry_count);

    for _ in 0..entry_count {
        // Key
        let key = i64::from_le_bytes([
            buf[offset],
            buf[offset + 1],
            buf[offset + 2],
            buf[offset + 3],
            buf[offset + 4],
            buf[offset + 5],
            buf[offset + 6],
            buf[offset + 7],
        ]);
        offset += 8;

        // Value: RecordId
        let page_id = u32::from_le_bytes([
            buf[offset],
            buf[offset + 1],
            buf[offset + 2],
            buf[offset + 3],
        ]) as usize;
        offset += 4;
        let slot_id = u32::from_le_bytes([
            buf[offset],
            buf[offset + 1],
            buf[offset + 2],
            buf[offset + 3],
        ]) as usize;
        offset += 4;

        keys.push(key);
        values.push(RecordId { page_id, slot_id });
    }

    let mut leaf = LeafNode::with_entries(keys, values);
    leaf.next = next_leaf;

    Ok(leaf)
}

/// Deserialize a B+ tree node from bytes
pub fn deserialize_node(buf: &[u8]) -> IndexResult<BPlusNode> {
    if buf.is_empty() {
        return Err(IndexError::DeserializationError("Empty buffer".to_string()));
    }

    let node_type = buf[0];
    match node_type {
        0 => Ok(BPlusNode::Internal(deserialize_internal_node(buf)?)),
        1 => Ok(BPlusNode::Leaf(deserialize_leaf_node(buf)?)),
        _ => Err(IndexError::InvalidNodeType(node_type)),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_metadata_serialization() {
        let metadata = BPlusTreeMetadata {
            order: 500,
            root_node_id: Some(1),
            first_leaf_id: Some(5),
            entry_count: 1000,
            tree_height: 3,
            next_free_page: 10,
        };

        let serialized = serialize_metadata(&metadata);
        let deserialized = deserialize_metadata(&serialized).unwrap();

        assert_eq!(deserialized.order, metadata.order);
        assert_eq!(deserialized.root_node_id, metadata.root_node_id);
        assert_eq!(deserialized.first_leaf_id, metadata.first_leaf_id);
        assert_eq!(deserialized.entry_count, metadata.entry_count);
        assert_eq!(deserialized.tree_height, metadata.tree_height);
        assert_eq!(deserialized.next_free_page, metadata.next_free_page);
    }

    #[test]
    fn test_metadata_none_values() {
        let metadata = BPlusTreeMetadata {
            order: 500,
            root_node_id: None,
            first_leaf_id: None,
            entry_count: 0,
            tree_height: 0,
            next_free_page: 0,
        };

        let serialized = serialize_metadata(&metadata);
        let deserialized = deserialize_metadata(&serialized).unwrap();

        assert_eq!(deserialized.root_node_id, None);
        assert_eq!(deserialized.first_leaf_id, None);
    }

    #[test]
    fn test_leaf_node_serialization() {
        let keys = vec![10, 20, 30];
        let values = vec![
            RecordId {
                page_id: 1,
                slot_id: 0,
            },
            RecordId {
                page_id: 1,
                slot_id: 1,
            },
            RecordId {
                page_id: 2,
                slot_id: 0,
            },
        ];
        let mut leaf = LeafNode::with_entries(keys.clone(), values.clone());
        leaf.next = Some(5);

        let serialized = serialize_leaf_node(&leaf).unwrap();
        let deserialized = deserialize_leaf_node(&serialized).unwrap();

        assert_eq!(deserialized.keys, keys);
        assert_eq!(deserialized.values, values);
        assert_eq!(deserialized.next, Some(5));
    }

    #[test]
    fn test_internal_node_serialization() {
        let keys = vec![100, 200, 300, 400];
        let children = vec![1, 2, 3, 4];
        let internal = InternalNode::new(keys.clone(), children.clone());

        let serialized = serialize_internal_node(&internal).unwrap();
        let deserialized = deserialize_internal_node(&serialized).unwrap();

        assert_eq!(deserialized.keys, keys);
        assert_eq!(deserialized.children, children);
    }

    #[test]
    fn test_node_serialization() {
        // Test leaf node
        let leaf = LeafNode::with_entries(
            vec![1, 2, 3],
            vec![
                RecordId {
                    page_id: 0,
                    slot_id: 0,
                },
                RecordId {
                    page_id: 0,
                    slot_id: 1,
                },
                RecordId {
                    page_id: 0,
                    slot_id: 2,
                },
            ],
        );
        let node = BPlusNode::Leaf(leaf);
        let serialized = serialize_node(&node).unwrap();
        let deserialized = deserialize_node(&serialized).unwrap();

        if let BPlusNode::Leaf(deserialized_leaf) = deserialized {
            assert_eq!(deserialized_leaf.keys, vec![1, 2, 3]);
        } else {
            panic!("Expected leaf node");
        }

        // Test internal node
        let internal = InternalNode::new(vec![10, 20], vec![1, 2]);
        let node = BPlusNode::Internal(internal);
        let serialized = serialize_node(&node).unwrap();
        let deserialized = deserialize_node(&serialized).unwrap();

        if let BPlusNode::Internal(deserialized_internal) = deserialized {
            assert_eq!(deserialized_internal.keys, vec![10, 20]);
            assert_eq!(deserialized_internal.children, vec![1, 2]);
        } else {
            panic!("Expected internal node");
        }
    }
}
