use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HistogramBin {
    pub range_start: f64,
    pub range_end: f64,
    pub count: u64,
}

pub fn build_histogram(values: &[f64], bins: usize) -> Vec<HistogramBin> {
    if values.is_empty() || bins == 0 { return Vec::new(); }
    let min = values.iter().cloned().fold(f64::INFINITY, f64::min);
    let max = values.iter().cloned().fold(f64::NEG_INFINITY, f64::max);
    if (max - min).abs() < f64::EPSILON {
        return vec![HistogramBin { range_start: min, range_end: max, count: values.len() as u64 }];
    }
    let width = (max - min) / bins as f64;
    let mut counts = vec![0u64; bins];
    for &v in values {
        let idx = ((v - min) / width) as usize;
        let idx = idx.min(bins - 1);
        counts[idx] += 1;
    }
    counts.iter().enumerate().map(|(i, &c)| HistogramBin {
        range_start: min + i as f64 * width,
        range_end: min + (i + 1) as f64 * width,
        count: c,
    }).collect()
}
