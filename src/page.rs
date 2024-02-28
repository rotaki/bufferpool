use log::trace;
use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::ops::{Deref, DerefMut};
use std::sync::atomic::AtomicUsize;
use std::sync::Mutex;

pub const PAGE_SIZE: usize = 4096;
const BASE_PAGE_HEADER_SIZE: usize = 12;

pub struct Page([u8; PAGE_SIZE]);

impl Page {
    pub fn new() -> Self {
        Page([0; PAGE_SIZE])
    }

    pub fn copy(&mut self, other: &Page) {
        self.0.copy_from_slice(&other.0);
    }

    fn base_header(&self) -> BasePageHeader {
        BasePageHeader::from_bytes(&self.0[0..BASE_PAGE_HEADER_SIZE].try_into().unwrap())
    }

    pub fn get_id(&self) -> u32 {
        self.base_header().id
    }

    pub fn set_id(&mut self, id: u32) {
        let mut header = self.base_header();
        header.id = id;
        self.0[0..BASE_PAGE_HEADER_SIZE].copy_from_slice(&header.to_bytes());
    }

    pub fn get_lsn(&self) -> u64 {
        self.base_header().lsn
    }

    pub fn set_lsn(&mut self, lsn: u64) {
        let mut header = self.base_header();
        header.lsn = lsn;
        self.0[0..BASE_PAGE_HEADER_SIZE].copy_from_slice(&header.to_bytes());
    }
}

struct BasePageHeader {
    id: u32,
    lsn: u64,
}

impl BasePageHeader {
    fn from_bytes(bytes: &[u8; BASE_PAGE_HEADER_SIZE]) -> Self {
        let id = u32::from_be_bytes(bytes[0..4].try_into().unwrap());
        let lsn = u64::from_be_bytes(bytes[4..12].try_into().unwrap());
        BasePageHeader { id, lsn }
    }

    fn to_bytes(&self) -> [u8; BASE_PAGE_HEADER_SIZE] {
        let id_bytes = self.id.to_be_bytes();
        let lsn_bytes = self.lsn.to_be_bytes();
        let mut bytes = [0; BASE_PAGE_HEADER_SIZE];
        bytes[0..4].copy_from_slice(&id_bytes);
        bytes[4..12].copy_from_slice(&lsn_bytes);
        bytes
    }
}

impl Deref for Page {
    type Target = [u8];

    fn deref(&self) -> &Self::Target {
        &self.0[BASE_PAGE_HEADER_SIZE..]
    }
}

impl DerefMut for Page {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0[BASE_PAGE_HEADER_SIZE..]
    }
}

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
        self.num_pages
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed)
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
