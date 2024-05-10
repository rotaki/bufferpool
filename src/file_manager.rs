use super::page::{Page, PageId, PAGE_SIZE};
use crate::{log, log_trace};
use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Mutex;

#[derive(Debug, PartialEq)]
pub enum FMStatus {
    OpenError,
    SeekError,
    ReadError,
    WriteError,
    FlushError,
}

impl std::fmt::Display for FMStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            FMStatus::OpenError => write!(f, "[FM] Error opening file"),
            FMStatus::SeekError => write!(f, "[FM] Error seeking in file"),
            FMStatus::ReadError => write!(f, "[FM] Error reading from file"),
            FMStatus::WriteError => write!(f, "[FM] Error writing to file"),
            FMStatus::FlushError => write!(f, "[FM] Error flushing file"),
        }
    }
}

pub struct FileManager {
    path: String,
    file: Mutex<File>,
    num_pages: AtomicU32,
}

impl FileManager {
    pub fn new<P: AsRef<std::path::Path>>(path: P) -> Result<Self, FMStatus> {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(&path)
            .map_err(|_| FMStatus::OpenError)?;
        let num_pages = file.metadata().unwrap().len() as usize / PAGE_SIZE;
        Ok(FileManager {
            path: path.as_ref().to_str().unwrap().to_string(),
            file: Mutex::new(file),
            num_pages: AtomicU32::new(num_pages as PageId),
        })
    }

    pub fn fetch_add_page_id(&self) -> PageId {
        self.num_pages.fetch_add(1, Ordering::AcqRel)
    }

    pub fn fetch_sub_page_id(&self) -> PageId {
        self.num_pages.fetch_sub(1, Ordering::AcqRel)
    }

    pub fn get_num_pages(&self) -> PageId {
        self.num_pages.load(Ordering::Acquire)
    }

    pub fn read_page(&self, page_id: PageId, page: &mut Page) -> Result<(), FMStatus> {
        let mut file = self.file.lock().unwrap();
        log_trace!("Reading page: {} from file: {:?}", page_id, self.path);
        file.seek(SeekFrom::Start((page_id * PAGE_SIZE as PageId) as u64))
            .map_err(|_| FMStatus::SeekError)?;
        file.read_exact(page.get_raw_bytes_mut())
            .map_err(|_| FMStatus::ReadError)?;
        debug_assert!(page.get_id() == page_id, "Page id mismatch");
        Ok(())
    }

    pub fn write_page(&self, page_id: PageId, page: &Page) -> Result<(), FMStatus> {
        let mut file = self.file.lock().unwrap();
        log_trace!("Writing page: {} to file: {:?}", page_id, self.path);
        file.seek(SeekFrom::Start((page_id * PAGE_SIZE as PageId) as u64))
            .map_err(|_| FMStatus::SeekError)?;
        file.write_all(page.get_raw_bytes())
            .map_err(|_| FMStatus::WriteError)?;
        Ok(())
    }

    pub fn flush(&mut self) -> Result<(), FMStatus> {
        let mut file = self.file.lock().unwrap();
        log_trace!("Flushing file: {:?}", self.path);
        file.flush().map_err(|_| FMStatus::FlushError)
    }
}
