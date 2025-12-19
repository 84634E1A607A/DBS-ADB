//! Integration tests for the index layer

#[cfg(test)]
mod tests {
    use crate::btree::DEFAULT_ORDER;
    use crate::file::{BufferManager, PagedFileManager};
    use crate::index::IndexManager;
    use crate::record::RecordId;
    use tempfile::TempDir;

    #[test]
    fn test_index_manager_basic() {
        let temp_dir = TempDir::new().unwrap();
        let db_path = temp_dir.path().to_str().unwrap();

        let file_manager = PagedFileManager::new();
        let buffer_mgr = BufferManager::new(file_manager);
        let mut manager = IndexManager::new(buffer_mgr);

        // Create an index
        manager.create_index(db_path, "students", "id").unwrap();

        // Insert data
        manager
            .insert(
                "students",
                "id",
                1,
                RecordId {
                    page_id: 0,
                    slot_id: 0,
                },
            )
            .unwrap();
        manager
            .insert(
                "students",
                "id",
                2,
                RecordId {
                    page_id: 0,
                    slot_id: 1,
                },
            )
            .unwrap();

        // Search
        assert_eq!(
            manager.search("students", "id", 1),
            Some(RecordId {
                page_id: 0,
                slot_id: 0
            })
        );
        assert_eq!(
            manager.search("students", "id", 2),
            Some(RecordId {
                page_id: 0,
                slot_id: 1
            })
        );

        // Drop index
        manager.drop_index(db_path, "students", "id").unwrap();

        // Verify file deleted
        let index_path = format!("{}/students_id.idx", db_path);
        assert!(!std::path::Path::new(&index_path).exists());
    }

    #[test]
    fn test_persistence() {
        let temp_dir = TempDir::new().unwrap();
        let db_path = temp_dir.path().to_str().unwrap();

        // First session: create and insert
        {
            let file_manager = PagedFileManager::new();
            let buffer_mgr = BufferManager::new(file_manager);
            let mut manager = IndexManager::new(buffer_mgr);

            manager.create_index(db_path, "test", "col").unwrap();

            for i in 0..100 {
                manager
                    .insert(
                        "test",
                        "col",
                        i,
                        RecordId {
                            page_id: i as usize,
                            slot_id: 0,
                        },
                    )
                    .unwrap();
            }

            manager.close_index("test", "col").unwrap();
        }

        // Second session: reopen and verify
        {
            let file_manager = PagedFileManager::new();
            let buffer_mgr = BufferManager::new(file_manager);
            let mut manager = IndexManager::new(buffer_mgr);

            manager.open_index(db_path, "test", "col").unwrap();

            for i in (0..100).step_by(10) {
                assert_eq!(
                    manager.search("test", "col", i),
                    Some(RecordId {
                        page_id: i as usize,
                        slot_id: 0
                    })
                );
            }
        }
    }

    #[test]
    fn test_range_search() {
        let temp_dir = TempDir::new().unwrap();
        let db_path = temp_dir.path().to_str().unwrap();

        let file_manager = PagedFileManager::new();
        let buffer_mgr = BufferManager::new(file_manager);
        let mut manager = IndexManager::new(buffer_mgr);

        manager.create_index(db_path, "test", "col").unwrap();

        // Insert entries with gaps
        for i in 0..50 {
            manager
                .insert(
                    "test",
                    "col",
                    i * 2,
                    RecordId {
                        page_id: i as usize,
                        slot_id: 0,
                    },
                )
                .unwrap();
        }

        // Range search
        let results = manager.range_search("test", "col", 10, 20);
        assert_eq!(results.len(), 6); // 10, 12, 14, 16, 18, 20

        let keys: Vec<i64> = results.iter().map(|(k, _)| *k).collect();
        assert_eq!(keys, vec![10, 12, 14, 16, 18, 20]);
    }

    #[test]
    fn test_delete() {
        let temp_dir = TempDir::new().unwrap();
        let db_path = temp_dir.path().to_str().unwrap();

        let file_manager = PagedFileManager::new();
        let buffer_mgr = BufferManager::new(file_manager);
        let mut manager = IndexManager::new(buffer_mgr);

        manager.create_index(db_path, "test", "col").unwrap();

        // Insert entries
        for i in 0..10 {
            manager
                .insert(
                    "test",
                    "col",
                    i,
                    RecordId {
                        page_id: i as usize,
                        slot_id: 0,
                    },
                )
                .unwrap();
        }

        // Delete an entry
        let deleted = manager.delete("test", "col", 5).unwrap();
        assert!(deleted);

        // Verify deletion
        assert_eq!(manager.search("test", "col", 5), None);

        // Try to delete again
        let deleted = manager.delete("test", "col", 5).unwrap();
        assert!(!deleted);
    }

    #[test]
    fn test_duplicate_keys() {
        let temp_dir = TempDir::new().unwrap();
        let db_path = temp_dir.path().to_str().unwrap();

        let file_manager = PagedFileManager::new();
        let buffer_mgr = BufferManager::new(file_manager);
        let mut manager = IndexManager::new(buffer_mgr);

        manager.create_index(db_path, "test", "col").unwrap();

        // Insert duplicate keys
        manager
            .insert(
                "test",
                "col",
                10,
                RecordId {
                    page_id: 0,
                    slot_id: 0,
                },
            )
            .unwrap();
        manager
            .insert(
                "test",
                "col",
                10,
                RecordId {
                    page_id: 0,
                    slot_id: 1,
                },
            )
            .unwrap();
        manager
            .insert(
                "test",
                "col",
                10,
                RecordId {
                    page_id: 1,
                    slot_id: 0,
                },
            )
            .unwrap();

        // Search for all duplicates
        let results = manager.search_all("test", "col", 10);
        assert_eq!(results.len(), 3);
    }

    #[test]
    fn test_large_dataset() {
        let temp_dir = TempDir::new().unwrap();
        let db_path = temp_dir.path().to_str().unwrap();

        let file_manager = PagedFileManager::new();
        let buffer_mgr = BufferManager::new(file_manager);
        let mut manager = IndexManager::new(buffer_mgr);

        manager.create_index(db_path, "test", "col").unwrap();

        // Insert 5000 entries
        for i in 0..5000 {
            manager
                .insert(
                    "test",
                    "col",
                    i,
                    RecordId {
                        page_id: (i / 100) as usize,
                        slot_id: (i % 100) as usize,
                    },
                )
                .unwrap();
        }

        // Verify random samples
        for i in (0..5000).step_by(100) {
            let result = manager.search("test", "col", i);
            assert_eq!(
                result,
                Some(RecordId {
                    page_id: (i / 100) as usize,
                    slot_id: (i % 100) as usize
                })
            );
        }

        // Range search
        let results = manager.range_search("test", "col", 2500, 2510);
        assert_eq!(results.len(), 11);
    }
}
