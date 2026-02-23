use serde::{Deserialize, Serialize};
use std::collections::HashMap;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FrequencyEntry {
    pub value: String,
    pub count: u64,
    pub percentage: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FrequencyResult {
    pub top_values: Vec<FrequencyEntry>,
    pub total_count: u64,
}

pub struct FrequencyCounter {
    map: HashMap<String, u64>,
    total: u64,
}

impl FrequencyCounter {
    pub fn new() -> Self {
        Self {
            map: HashMap::new(),
            total: 0,
        }
    }
    pub fn add(&mut self, val: String) {
        *self.map.entry(val).or_insert(0) += 1;
        self.total += 1;
    }
    pub fn top_n(self, n: usize) -> FrequencyResult {
        let total = self.total;
        let mut entries: Vec<(String, u64)> = self.map.into_iter().collect();
        entries.sort_by(|a, b| b.1.cmp(&a.1));
        let top_values = entries
            .into_iter()
            .take(n)
            .map(|(v, c)| FrequencyEntry {
                percentage: if total > 0 {
                    c as f64 / total as f64 * 100.0
                } else {
                    0.0
                },
                value: v,
                count: c,
            })
            .collect();
        FrequencyResult {
            top_values,
            total_count: total,
        }
    }
}

impl Default for FrequencyCounter {
    fn default() -> Self { Self::new() }
}
