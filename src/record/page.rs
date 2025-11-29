use super::error::{RecordError, RecordResult};
use super::record::SlotId;
use crate::file::{PAGE_SIZE, PageId};

/// Page header stored at the beginning of each page
#[derive(Debug, Clone, Copy)]
pub struct PageHeader {
    pub next_page: u32,   // 4 bytes - Link to next page (0 = no next)
    pub slot_count: u16,  // 2 bytes - Max slots in this page
    pub free_slots: u16,  // 2 bytes - Number of free slots
    pub record_size: u16, // 2 bytes - Size of each record
    _padding: [u8; 6],    // 6 bytes - Padding to 16 bytes
}

impl PageHeader {
    const SIZE: usize = 16;

    pub fn new(slot_count: u16, record_size: u16) -> Self {
        Self {
            next_page: 0,
            slot_count,
            free_slots: slot_count,
            record_size,
            _padding: [0; 6],
        }
    }

    pub fn serialize(&self) -> [u8; Self::SIZE] {
        let mut result = [0u8; Self::SIZE];
        result[0..4].copy_from_slice(&self.next_page.to_le_bytes());
        result[4..6].copy_from_slice(&self.slot_count.to_le_bytes());
        result[6..8].copy_from_slice(&self.free_slots.to_le_bytes());
        result[8..10].copy_from_slice(&self.record_size.to_le_bytes());
        result
    }

    pub fn deserialize(data: &[u8]) -> RecordResult<Self> {
        if data.len() < Self::SIZE {
            return Err(RecordError::Deserialization(format!(
                "Not enough data for page header: {} bytes",
                data.len()
            )));
        }

        let next_page = u32::from_le_bytes([data[0], data[1], data[2], data[3]]);
        let slot_count = u16::from_le_bytes([data[4], data[5]]);
        let free_slots = u16::from_le_bytes([data[6], data[7]]);
        let record_size = u16::from_le_bytes([data[8], data[9]]);

        Ok(Self {
            next_page,
            slot_count,
            free_slots,
            record_size,
            _padding: [0; 6],
        })
    }
}

/// In-memory representation of a page
pub struct Page {
    header: PageHeader,
    slot_bitmap: Vec<u8>, // Bitmap tracking used/free slots
    data: Vec<u8>,        // Record data area
}

impl Page {
    /// Calculate maximum number of slots for a given record size
    pub fn calculate_slot_count(record_size: usize) -> usize {
        if record_size == 0 || record_size > PAGE_SIZE {
            return 0;
        }

        let available = PAGE_SIZE - PageHeader::SIZE;

        // We need: bitmap_size + (slot_count * record_size) <= available
        // bitmap_size = ⌈slot_count / 8⌉
        // Solving: slot_count / 8 + slot_count * record_size <= available
        // slot_count * (1/8 + record_size) <= available
        // slot_count <= available / (1/8 + record_size)

        let max_slots = (available * 8) / (1 + record_size * 8);
        max_slots.min(u16::MAX as usize)
    }

    /// Create a new empty page
    pub fn new(record_size: usize) -> RecordResult<Self> {
        let slot_count = Self::calculate_slot_count(record_size);
        if slot_count == 0 {
            return Err(RecordError::InvalidRecord(format!(
                "Record size {} is too large for page",
                record_size
            )));
        }

        let bitmap_size = slot_count.div_ceil(8);
        let data_size = slot_count * record_size;

        Ok(Self {
            header: PageHeader::new(slot_count as u16, record_size as u16),
            slot_bitmap: vec![0u8; bitmap_size],
            data: vec![0u8; data_size],
        })
    }

    /// Deserialize page from buffer
    pub fn from_bytes(buffer: &[u8]) -> RecordResult<Self> {
        if buffer.len() != PAGE_SIZE {
            return Err(RecordError::Deserialization(format!(
                "Invalid page size: {} bytes",
                buffer.len()
            )));
        }

        let header = PageHeader::deserialize(&buffer[..PageHeader::SIZE])?;

        let slot_count = header.slot_count as usize;
        let record_size = header.record_size as usize;
        let bitmap_size = slot_count.div_ceil(8);

        let bitmap_start = PageHeader::SIZE;
        let bitmap_end = bitmap_start + bitmap_size;
        let data_start = bitmap_end;
        let data_end = data_start + (slot_count * record_size);

        if data_end > PAGE_SIZE {
            return Err(RecordError::Deserialization(
                "Page layout exceeds page size".to_string(),
            ));
        }

        Ok(Self {
            header,
            slot_bitmap: buffer[bitmap_start..bitmap_end].to_vec(),
            data: buffer[data_start..data_end].to_vec(),
        })
    }

    /// Serialize page to buffer
    pub fn to_bytes(&self) -> Vec<u8> {
        let mut result = vec![0u8; PAGE_SIZE];

        // Write header
        result[..PageHeader::SIZE].copy_from_slice(&self.header.serialize());

        // Write bitmap
        let bitmap_start = PageHeader::SIZE;
        let bitmap_end = bitmap_start + self.slot_bitmap.len();
        result[bitmap_start..bitmap_end].copy_from_slice(&self.slot_bitmap);

        // Write data
        let data_start = bitmap_end;
        let data_end = data_start + self.data.len();
        result[data_start..data_end].copy_from_slice(&self.data);

        result
    }

    /// Find a free slot, returns None if page is full
    pub fn find_free_slot(&self) -> Option<SlotId> {
        if self.header.free_slots == 0 {
            return None;
        }

        (0..self.header.slot_count as usize).find(|&slot_id| !self.is_slot_used(slot_id))
    }

    /// Check if a slot is used
    pub fn is_slot_used(&self, slot_id: SlotId) -> bool {
        let byte_idx = slot_id / 8;
        let bit_idx = slot_id % 8;
        if byte_idx >= self.slot_bitmap.len() {
            return false;
        }
        (self.slot_bitmap[byte_idx] & (1 << bit_idx)) != 0
    }

    /// Check if a slot is free
    pub fn is_slot_free(&self, slot_id: SlotId) -> bool {
        !self.is_slot_used(slot_id)
    }

    /// Mark a slot as used
    pub fn mark_slot_used(&mut self, slot_id: SlotId) -> RecordResult<()> {
        if slot_id >= self.header.slot_count as usize {
            return Err(RecordError::InvalidSlot(0, slot_id));
        }

        let byte_idx = slot_id / 8;
        let bit_idx = slot_id % 8;

        if !self.is_slot_used(slot_id) {
            self.slot_bitmap[byte_idx] |= 1 << bit_idx;
            self.header.free_slots = self.header.free_slots.saturating_sub(1);
        }

        Ok(())
    }

    /// Mark a slot as free
    pub fn mark_slot_free(&mut self, slot_id: SlotId) -> RecordResult<()> {
        if slot_id >= self.header.slot_count as usize {
            return Err(RecordError::InvalidSlot(0, slot_id));
        }

        let byte_idx = slot_id / 8;
        let bit_idx = slot_id % 8;

        if self.is_slot_used(slot_id) {
            self.slot_bitmap[byte_idx] &= !(1 << bit_idx);
            self.header.free_slots = (self.header.free_slots + 1).min(self.header.slot_count);
        }

        Ok(())
    }

    /// Get record data from a slot
    pub fn get_record(&self, slot_id: SlotId) -> RecordResult<&[u8]> {
        if slot_id >= self.header.slot_count as usize {
            return Err(RecordError::InvalidSlot(0, slot_id));
        }

        if !self.is_slot_used(slot_id) {
            return Err(RecordError::InvalidSlot(0, slot_id));
        }

        let record_size = self.header.record_size as usize;
        let start = slot_id * record_size;
        let end = start + record_size;

        Ok(&self.data[start..end])
    }

    /// Set record data in a slot
    pub fn set_record(&mut self, slot_id: SlotId, data: &[u8]) -> RecordResult<()> {
        if slot_id >= self.header.slot_count as usize {
            return Err(RecordError::InvalidSlot(0, slot_id));
        }

        let record_size = self.header.record_size as usize;
        if data.len() != record_size {
            return Err(RecordError::InvalidRecord(format!(
                "Record size mismatch: expected {}, got {}",
                record_size,
                data.len()
            )));
        }

        let start = slot_id * record_size;
        let end = start + record_size;
        self.data[start..end].copy_from_slice(data);

        Ok(())
    }

    /// Get the number of slots in this page
    pub fn slot_count(&self) -> usize {
        self.header.slot_count as usize
    }

    /// Get the number of free slots
    pub fn free_slot_count(&self) -> usize {
        self.header.free_slots as usize
    }

    /// Get the next page ID (0 means no next page)
    pub fn next_page(&self) -> PageId {
        self.header.next_page as PageId
    }

    /// Set the next page ID
    pub fn set_next_page(&mut self, page_id: PageId) {
        self.header.next_page = page_id as u32;
    }

    /// Check if page is full
    pub fn is_full(&self) -> bool {
        self.header.free_slots == 0
    }

    /// Check if page is empty
    pub fn is_empty(&self) -> bool {
        self.header.free_slots == self.header.slot_count
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_calculate_slot_count() {
        // For a 23-byte record (from example in plan)
        let slot_count = Page::calculate_slot_count(23);
        assert!(slot_count > 300 && slot_count < 400);

        // Small record
        let slot_count = Page::calculate_slot_count(10);
        assert!(slot_count > 700);

        // Large record
        let slot_count = Page::calculate_slot_count(1000);
        assert!(slot_count > 5 && slot_count < 10);
    }

    #[test]
    fn test_page_creation() {
        let page = Page::new(23).unwrap();
        assert_eq!(page.slot_count(), Page::calculate_slot_count(23));
        assert_eq!(page.free_slot_count(), page.slot_count());
        assert!(page.is_empty());
        assert!(!page.is_full());
    }

    #[test]
    fn test_slot_operations() {
        let mut page = Page::new(23).unwrap();

        // Initially all slots are free
        assert!(page.is_slot_free(0));
        assert!(page.is_slot_free(1));

        // Mark slot 0 as used
        page.mark_slot_used(0).unwrap();
        assert!(page.is_slot_used(0));
        assert!(page.is_slot_free(1));
        assert_eq!(page.free_slot_count(), page.slot_count() - 1);

        // Mark slot 1 as used
        page.mark_slot_used(1).unwrap();
        assert!(page.is_slot_used(1));

        // Mark slot 0 as free again
        page.mark_slot_free(0).unwrap();
        assert!(page.is_slot_free(0));
        assert!(page.is_slot_used(1));
    }

    #[test]
    fn test_find_free_slot() {
        let mut page = Page::new(23).unwrap();

        // First free slot should be 0
        assert_eq!(page.find_free_slot(), Some(0));

        // Use slot 0
        page.mark_slot_used(0).unwrap();
        assert_eq!(page.find_free_slot(), Some(1));

        // Use slot 1
        page.mark_slot_used(1).unwrap();
        assert_eq!(page.find_free_slot(), Some(2));
    }

    #[test]
    fn test_record_operations() {
        let mut page = Page::new(10).unwrap();

        let data = vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10];

        // Set record in slot 0
        page.set_record(0, &data).unwrap();
        page.mark_slot_used(0).unwrap();

        // Get record from slot 0
        let retrieved = page.get_record(0).unwrap();
        assert_eq!(retrieved, &data[..]);
    }

    #[test]
    fn test_serialization() {
        let mut page = Page::new(23).unwrap();

        // Mark some slots as used
        page.mark_slot_used(0).unwrap();
        page.mark_slot_used(5).unwrap();
        page.set_next_page(42);

        // Serialize
        let bytes = page.to_bytes();
        assert_eq!(bytes.len(), PAGE_SIZE);

        // Deserialize
        let restored = Page::from_bytes(&bytes).unwrap();
        assert_eq!(restored.slot_count(), page.slot_count());
        assert_eq!(restored.free_slot_count(), page.free_slot_count());
        assert_eq!(restored.next_page(), 42);
        assert!(restored.is_slot_used(0));
        assert!(restored.is_slot_used(5));
        assert!(restored.is_slot_free(1));
    }

    #[test]
    fn test_page_full() {
        let mut page = Page::new(100).unwrap();
        let slot_count = page.slot_count();

        // Fill all slots
        for i in 0..slot_count {
            assert!(!page.is_full());
            page.mark_slot_used(i).unwrap();
        }

        assert!(page.is_full());
        assert_eq!(page.find_free_slot(), None);
    }
}
