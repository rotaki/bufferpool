use std::io::Write;

pub fn init_logger() {
    let _ = env_logger::builder()
        .filter_level(log::LevelFilter::Error) // Default to only show errors
        .filter_module("bp::buffer_pool", log::LevelFilter::Error)
        .format(|buf, record| {
            writeln!(
                buf,
                "{} [{}] - {}:{} - {}",
                record.level(),
                record.target(),
                record.file().unwrap_or("<unknown>"),
                record.line().unwrap_or(0),
                record.args()
            )
        })
        .try_init();
}

#[allow(dead_code)]
#[cfg(test)]
pub fn init_test_logger() {
    let _ = env_logger::builder()
        .is_test(true)
        .filter_level(log::LevelFilter::Info) // Default to only show info
        .filter_module("bp::buffer_pool", log::LevelFilter::Trace) // Per module debugging
        .filter_module("bp::heap_page", log::LevelFilter::Trace)
        .filter_module("bp::foster_btree", log::LevelFilter::Trace)
        .format(|buf, record| {
            writeln!(
                buf,
                "{} [{}] - {}:{} - {}",
                record.level(),
                record.target(),
                record.file().unwrap_or("<unknown>"),
                record.line().unwrap_or(0),
                record.args()
            )
        })
        .try_init();
}
