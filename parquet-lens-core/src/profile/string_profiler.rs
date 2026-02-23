use regex::Regex;
use serde::{Deserialize, Serialize};
use std::sync::OnceLock;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PatternStats {
    pub all_numeric_pct: f64,
    pub email_like_pct: f64,
    pub uuid_like_pct: f64,
    pub iso_date_like_pct: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StringProfile {
    pub count: u64,
    pub empty_count: u64,
    pub whitespace_only_count: u64,
    pub min_length: usize,
    pub max_length: usize,
    pub mean_length: f64,
    pub patterns: PatternStats,
}

static RE_NUMERIC: OnceLock<Regex> = OnceLock::new();
static RE_EMAIL: OnceLock<Regex> = OnceLock::new();
static RE_UUID: OnceLock<Regex> = OnceLock::new();
static RE_ISODATE: OnceLock<Regex> = OnceLock::new();

fn re_numeric() -> &'static Regex { RE_NUMERIC.get_or_init(|| Regex::new(r"^\d+(\.\d+)?$").unwrap()) }
fn re_email() -> &'static Regex { RE_EMAIL.get_or_init(|| Regex::new(r"^[^@\s]+@[^@\s]+\.[^@\s]+$").unwrap()) }
fn re_uuid() -> &'static Regex { RE_UUID.get_or_init(|| Regex::new(r"^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$").unwrap()) }
fn re_isodate() -> &'static Regex { RE_ISODATE.get_or_init(|| Regex::new(r"^\d{4}-\d{2}-\d{2}(T\d{2}:\d{2}:\d{2})?").unwrap()) }

pub struct StringAccumulator {
    count: u64,
    empty: u64,
    whitespace: u64,
    min_len: usize,
    max_len: usize,
    total_len: u64,
    numeric: u64,
    email: u64,
    uuid: u64,
    isodate: u64,
}

impl StringAccumulator {
    pub fn new() -> Self {
        Self { count:0,empty:0,whitespace:0,min_len:usize::MAX,max_len:0,
               total_len:0,numeric:0,email:0,uuid:0,isodate:0 }
    }
    pub fn add(&mut self, s: &str) {
        self.count += 1;
        let len = s.len();
        if len == 0 { self.empty += 1; }
        else if s.chars().all(|c| c.is_whitespace()) { self.whitespace += 1; }
        if len < self.min_len { self.min_len = len; }
        if len > self.max_len { self.max_len = len; }
        self.total_len += len as u64;
        if re_numeric().is_match(s) { self.numeric += 1; }
        if re_email().is_match(s) { self.email += 1; }
        if re_uuid().is_match(s) { self.uuid += 1; }
        if re_isodate().is_match(s) { self.isodate += 1; }
    }
    pub fn finish(self) -> StringProfile {
        let n = self.count as f64;
        let pct = |x: u64| if self.count > 0 { x as f64 / n * 100.0 } else { 0.0 };
        StringProfile {
            count: self.count,
            empty_count: self.empty,
            whitespace_only_count: self.whitespace,
            min_length: if self.min_len == usize::MAX { 0 } else { self.min_len },
            max_length: self.max_len,
            mean_length: if self.count > 0 { self.total_len as f64 / n } else { 0.0 },
            patterns: PatternStats {
                all_numeric_pct: pct(self.numeric),
                email_like_pct: pct(self.email),
                uuid_like_pct: pct(self.uuid),
                iso_date_like_pct: pct(self.isodate),
            },
        }
    }
}
