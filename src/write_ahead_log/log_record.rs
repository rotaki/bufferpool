use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize)]
pub enum LogRecord<'a> {
    // User transaction operations
    TxnBegin {
        txn_id: u64,
    },
    TxnCommit {
        txn_id: u64,
    },
    TxnAbort {
        txn_id: u64,
    },
    PageInsert {
        txn_id: u64,
        page_id: u32,
        key: &'a [u8],
        value: &'a [u8],
    },
    PageUpdate {
        txn_id: u64,
        page_id: u32,
        key: &'a [u8],
        before_value: &'a [u8],
        after_value: &'a [u8],
    },
    PageDelete {
        txn_id: u64,
        page_id: u32,
        key: &'a [u8],
        value: &'a [u8],
    },

    // System transaction operations
    SysTxnAllocPage {
        txn_id: u64,
        page_id: u32,
    },
    SysTxnSplit {
        txn_id: u64,
        page_id: u32,
        new_page_id: u32,
        key_values: Vec<(&'a [u8], &'a [u8])>,
    },
}

impl LogRecord<'_> {
    pub fn get_page_ids(&self) -> Vec<u32> {
        match self {
            LogRecord::PageInsert { page_id, .. } => vec![*page_id],
            LogRecord::PageUpdate { page_id, .. } => vec![*page_id],
            LogRecord::PageDelete { page_id, .. } => vec![*page_id],
            LogRecord::SysTxnAllocPage { page_id, .. } => vec![*page_id],
            LogRecord::SysTxnSplit {
                page_id,
                new_page_id,
                ..
            } => vec![*page_id, *new_page_id],
            _ => vec![],
        }
    }

    pub fn to_bytes(&self) -> Vec<u8> {
        bincode::serialize(self).unwrap()
    }

    pub fn from_bytes(bytes: &[u8]) -> Self {
        bincode::deserialize(bytes).unwrap()
    }
}
