use hyperloglog::HyperLogLog;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CardinalityEstimate {
    pub approximate_distinct: u64,
    pub error_rate: f64,
}

pub struct HllEstimator {
    hll: HyperLogLog,
}

impl HllEstimator {
    pub fn new() -> Self {
        // error rate ~0.8%
        Self {
            hll: HyperLogLog::new(0.00813),
        }
    }
    pub fn add_bytes(&mut self, val: &[u8]) {
        self.hll.insert(&val);
    }
    pub fn estimate(&self) -> CardinalityEstimate {
        CardinalityEstimate {
            approximate_distinct: self.hll.len().round() as u64,
            error_rate: 0.00813,
        }
    }
}

impl Default for HllEstimator {
    fn default() -> Self {
        Self::new()
    }
}
