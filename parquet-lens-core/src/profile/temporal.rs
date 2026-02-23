use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TemporalProfile {
    pub count: u64,
    pub null_count: u64,
    pub min_timestamp_ms: Option<i64>,
    pub max_timestamp_ms: Option<i64>,
    pub range_days: Option<f64>,
    pub year_distribution: Vec<(i32, u64)>,
}

pub struct TemporalAccumulator {
    count: u64,
    null_count: u64,
    min: Option<i64>,
    max: Option<i64>,
    year_counts: std::collections::HashMap<i32, u64>,
}

impl TemporalAccumulator {
    pub fn new() -> Self { Self { count:0,null_count:0,min:None,max:None,year_counts:std::collections::HashMap::new() } }
    pub fn add_ms(&mut self, ts_ms: i64) {
        self.count += 1;
        self.min = Some(self.min.map_or(ts_ms, |m| m.min(ts_ms)));
        self.max = Some(self.max.map_or(ts_ms, |m| m.max(ts_ms)));
        // approximate year from ms: 1970 + ms / (365.25 * 86400 * 1000)
        let year = 1970 + (ts_ms as f64 / (365.25 * 86400.0 * 1000.0)) as i32;
        *self.year_counts.entry(year).or_insert(0) += 1;
    }
    pub fn add_null(&mut self) { self.null_count += 1; }
    pub fn finish(self) -> TemporalProfile {
        let range_days = match (self.min, self.max) {
            (Some(mn), Some(mx)) => Some((mx - mn) as f64 / (86400.0 * 1000.0)),
            _ => None,
        };
        let mut year_distribution: Vec<(i32, u64)> = self.year_counts.into_iter().collect();
        year_distribution.sort_by_key(|(y, _)| *y);
        TemporalProfile { count:self.count,null_count:self.null_count,
            min_timestamp_ms:self.min,max_timestamp_ms:self.max,range_days,year_distribution }
    }
}
