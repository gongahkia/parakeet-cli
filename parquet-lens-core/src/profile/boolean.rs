use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BooleanProfile {
    pub true_count: u64,
    pub false_count: u64,
    pub null_count: u64,
    pub true_percentage: f64,
}

pub struct BooleanAccumulator {
    true_count: u64,
    false_count: u64,
    null_count: u64,
}

impl BooleanAccumulator {
    pub fn new() -> Self {
        Self {
            true_count: 0,
            false_count: 0,
            null_count: 0,
        }
    }
    pub fn add(&mut self, v: Option<bool>) {
        match v {
            Some(true) => self.true_count += 1,
            Some(false) => self.false_count += 1,
            None => self.null_count += 1,
        }
    }
    pub fn finish(self) -> BooleanProfile {
        let total = self.true_count + self.false_count;
        let true_percentage = if total > 0 {
            self.true_count as f64 / total as f64 * 100.0
        } else {
            0.0
        };
        BooleanProfile {
            true_count: self.true_count,
            false_count: self.false_count,
            null_count: self.null_count,
            true_percentage,
        }
    }
}

impl Default for BooleanAccumulator {
    fn default() -> Self {
        Self::new()
    }
}
