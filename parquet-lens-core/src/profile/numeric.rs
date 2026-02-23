use serde::{Deserialize, Serialize};
use tdigest::TDigest;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NumericProfile {
    pub mean: f64,
    pub stddev: f64,
    pub min: f64,
    pub max: f64,
    pub p1: f64,
    pub p5: f64,
    pub p25: f64,
    pub p50: f64,
    pub p75: f64,
    pub p95: f64,
    pub p99: f64,
    pub skewness: f64,
    pub kurtosis: f64,
    pub count: u64,
}

pub struct NumericAccumulator {
    digest: TDigest,
    sum: f64,
    sum_sq: f64,
    sum_cube: f64,
    sum_quad: f64,
    min: f64,
    max: f64,
    count: u64,
    values_buf: Vec<f64>,
}

impl NumericAccumulator {
    pub fn new() -> Self {
        Self {
            digest: TDigest::new_with_size(100),
            sum: 0.0,
            sum_sq: 0.0,
            sum_cube: 0.0,
            sum_quad: 0.0,
            min: f64::MAX,
            max: f64::MIN,
            count: 0,
            values_buf: Vec::new(),
        }
    }
    pub fn add(&mut self, v: f64) {
        self.values_buf.push(v);
        self.sum += v;
        self.sum_sq += v * v;
        self.sum_cube += v * v * v;
        self.sum_quad += v * v * v * v;
        if v < self.min {
            self.min = v;
        }
        if v > self.max {
            self.max = v;
        }
        self.count += 1;
        if self.values_buf.len() >= 10000 {
            self.flush();
        }
    }
    fn flush(&mut self) {
        if self.values_buf.is_empty() {
            return;
        }
        // merge_unsorted is a method on &self returning a new TDigest
        let merged = self
            .digest
            .merge_unsorted(self.values_buf.drain(..).collect());
        self.digest = merged;
    }
    pub fn finish(mut self) -> NumericProfile {
        self.flush();
        let n = self.count as f64;
        if n == 0.0 {
            return NumericProfile {
                mean: 0.0,
                stddev: 0.0,
                min: 0.0,
                max: 0.0,
                p1: 0.0,
                p5: 0.0,
                p25: 0.0,
                p50: 0.0,
                p75: 0.0,
                p95: 0.0,
                p99: 0.0,
                skewness: 0.0,
                kurtosis: 0.0,
                count: 0,
            };
        }
        let mean = self.sum / n;
        let variance = (self.sum_sq / n) - mean * mean;
        let stddev = variance.sqrt();
        let skewness = if stddev > 0.0 {
            ((self.sum_cube / n) - 3.0 * mean * variance - mean.powi(3)) / stddev.powi(3)
        } else {
            0.0
        };
        let kurtosis = if variance > 0.0 {
            ((self.sum_quad / n) - 4.0 * mean * (self.sum_cube / n)
                + 6.0 * mean * mean * (self.sum_sq / n)
                - 3.0 * mean.powi(4))
                / variance.powi(2)
                - 3.0
        } else {
            0.0
        };
        NumericProfile {
            mean,
            stddev,
            min: self.min,
            max: self.max,
            p1: self.digest.estimate_quantile(0.01),
            p5: self.digest.estimate_quantile(0.05),
            p25: self.digest.estimate_quantile(0.25),
            p50: self.digest.estimate_quantile(0.50),
            p75: self.digest.estimate_quantile(0.75),
            p95: self.digest.estimate_quantile(0.95),
            p99: self.digest.estimate_quantile(0.99),
            skewness,
            kurtosis,
            count: self.count,
        }
    }
}

impl Default for NumericAccumulator {
    fn default() -> Self { Self::new() }
}
