use crate::page::{Page, PAGE_SIZE};
use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::sync::atomic::AtomicUsize;
use std::sync::Mutex;

pub struct FileManager {
    pub file: Mutex<File>,
    pub num_pages: AtomicUsize,
}

impl FileManager {
    pub fn new<P: AsRef<std::path::Path>>(path: P) -> Self {
        // open if exists and figure out the number of pages
        // otherwise create a new file
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(path)
            .unwrap();
        let num_pages = file.metadata().unwrap().len() as usize / PAGE_SIZE;
        FileManager {
            file: Mutex::new(file),
            num_pages: AtomicUsize::new(num_pages),
        }
    }

    pub fn new_page(&self) -> usize {
        let page_id = self
            .num_pages
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        self.write_page(page_id, &Page::new());
        page_id
    }

    pub fn read_page(&self, page_id: usize) -> Page {
        let mut page = Page::new();
        let mut file = self.file.lock().unwrap();
        file.seek(SeekFrom::Start((page_id * PAGE_SIZE) as u64))
            .unwrap();
        file.read_exact(&mut page.0).unwrap();
        page
    }

    pub fn write_page(&self, page_id: usize, page: &Page) {
        let mut file = self.file.lock().unwrap();
        file.seek(SeekFrom::Start((page_id * PAGE_SIZE) as u64))
            .unwrap();
        file.write_all(&page.0).unwrap();
    }

    pub fn flush(&mut self) {
        let mut file = self.file.lock().unwrap();
        file.flush().unwrap();
    }
}
