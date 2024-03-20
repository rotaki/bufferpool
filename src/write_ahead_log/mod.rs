mod log_buffer;
mod log_record;

use log_buffer::LogBuffer;
pub type LogBufferRef = std::sync::Arc<LogBuffer>;

pub mod prelude {
    pub use super::log_buffer::{LogBuffer, Lsn, LsnSize};
    pub use super::log_record::LogRecord;
    pub use super::LogBufferRef;
}
