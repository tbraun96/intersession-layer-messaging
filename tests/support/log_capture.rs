//! Captures the crate's log lines so a test can count them.
//!
//! `log::set_boxed_logger` is process-wide and refuses a second call, so each
//! test binary that uses this holds exactly one logging test.

use std::sync::{Arc, Mutex};

#[derive(Clone)]
pub struct LogCapture {
    lines: Arc<Mutex<Vec<(log::Level, String)>>>,
}

struct Sink(LogCapture);

impl log::Log for Sink {
    fn enabled(&self, _: &log::Metadata) -> bool {
        true
    }
    fn log(&self, record: &log::Record) {
        self.0
            .lines
            .lock()
            .unwrap()
            .push((record.level(), format!("{}", record.args())));
    }
    fn flush(&self) {}
}

impl LogCapture {
    pub fn install(max_level: log::LevelFilter) -> Self {
        let capture = Self {
            lines: Arc::new(Mutex::new(Vec::new())),
        };
        log::set_boxed_logger(Box::new(Sink(capture.clone())))
            .expect("another logger was installed first in this test binary");
        log::set_max_level(max_level);
        capture
    }

    /// Lines at `level` or more severe that contain `needle`.
    pub fn count(&self, level: log::Level, needle: &str) -> usize {
        self.lines
            .lock()
            .unwrap()
            .iter()
            .filter(|(l, text)| *l <= level && text.contains(needle))
            .count()
    }
}
