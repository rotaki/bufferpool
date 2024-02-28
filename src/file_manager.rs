use log::trace;

use crate::page::{Page, PAGE_SIZE};
use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::sync::atomic::AtomicUsize;
use std::sync::Mutex;

pub struct FileManager {
    path: String,
    pub file: Mutex<File>,
    pub num_pages: AtomicUsize,
}

impl FileManager {
    pub fn new<P: AsRef<std::path::Path>>(path: P) -> Self {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(&path)
            .unwrap();
        let num_pages = file.metadata().unwrap().len() as usize / PAGE_SIZE;
        FileManager {
            path: path.as_ref().to_str().unwrap().to_string(),
            file: Mutex::new(file),
            num_pages: AtomicUsize::new(num_pages),
        }
    }

    pub fn get_new_page_id(&self) -> usize {
        self.num_pages.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
    }

    pub fn read_page(&self, page_id: usize) -> Page {
        let mut page = Page::new();
        let mut file = self.file.lock().unwrap();
        trace!("Reading page: {} from file: {:?}", page_id, self.path);
        file.seek(SeekFrom::Start((page_id * PAGE_SIZE) as u64))
            .unwrap();
        file.read_exact(&mut page.0).unwrap();
        page
    }

    pub fn write_page(&self, page_id: usize, page: &Page) {
        let mut file = self.file.lock().unwrap();
        trace!("Writing page: {} to file: {:?}", page_id, self.path);
        file.seek(SeekFrom::Start((page_id * PAGE_SIZE) as u64))
            .unwrap();
        file.write_all(&page.0).unwrap();
    }

    pub fn flush(&mut self) {
        let mut file = self.file.lock().unwrap();
        trace!("Flushing file: {:?}", self.path);
        file.flush().unwrap();
    }
}
