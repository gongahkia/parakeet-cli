use crate::schema::{extract_schema, ColumnSchema};
use parquet_lens_common::Result;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::PathBuf;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum InconsistencyKind {
    ColumnAdded {
        column: String,
    },
    ColumnRemoved {
        column: String,
    },
    TypeChanged {
        column: String,
        from: String,
        to: String,
    },
    LogicalTypeChanged {
        column: String,
        from: Option<String>,
        to: Option<String>,
    },
    RepetitionChanged {
        column: String,
        from: String,
        to: String,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SchemaInconsistency {
    pub file: PathBuf,
    pub baseline_file: PathBuf,
    pub kind: InconsistencyKind,
    pub description: String,
}

pub fn check_schema_consistency(paths: &[PathBuf]) -> Result<Vec<SchemaInconsistency>> {
    if paths.len() < 2 {
        return Ok(Vec::new());
    }
    let baseline_path = &paths[0];
    let baseline = extract_schema(baseline_path)?;
    let baseline_map: HashMap<&str, &ColumnSchema> =
        baseline.iter().map(|c| (c.name.as_str(), c)).collect();
    let mut issues = Vec::new();
    for path in &paths[1..] {
        let cols = extract_schema(path)?;
        let col_map: HashMap<&str, &ColumnSchema> =
            cols.iter().map(|c| (c.name.as_str(), c)).collect();
        // check removals (in baseline, not in file)
        for (name, base_col) in &baseline_map {
            if let Some(col) = col_map.get(name) {
                if col.physical_type != base_col.physical_type {
                    issues.push(SchemaInconsistency {
                        file: path.clone(),
                        baseline_file: baseline_path.clone(),
                        kind: InconsistencyKind::TypeChanged {
                            column: name.to_string(),
                            from: base_col.physical_type.clone(),
                            to: col.physical_type.clone(),
                        },
                        description: format!(
                            "{name}: physical_type {} -> {}",
                            base_col.physical_type, col.physical_type
                        ),
                    });
                }
                if col.logical_type != base_col.logical_type {
                    issues.push(SchemaInconsistency {
                        file: path.clone(),
                        baseline_file: baseline_path.clone(),
                        kind: InconsistencyKind::LogicalTypeChanged {
                            column: name.to_string(),
                            from: base_col.logical_type.clone(),
                            to: col.logical_type.clone(),
                        },
                        description: format!(
                            "{name}: logical_type {:?} -> {:?}",
                            base_col.logical_type, col.logical_type
                        ),
                    });
                }
                if col.repetition != base_col.repetition {
                    issues.push(SchemaInconsistency {
                        file: path.clone(),
                        baseline_file: baseline_path.clone(),
                        kind: InconsistencyKind::RepetitionChanged {
                            column: name.to_string(),
                            from: base_col.repetition.clone(),
                            to: col.repetition.clone(),
                        },
                        description: format!(
                            "{name}: repetition {} -> {}",
                            base_col.repetition, col.repetition
                        ),
                    });
                }
            } else {
                issues.push(SchemaInconsistency {
                    file: path.clone(),
                    baseline_file: baseline_path.clone(),
                    kind: InconsistencyKind::ColumnRemoved {
                        column: name.to_string(),
                    },
                    description: format!("{name}: column removed from baseline"),
                });
            }
        }
        // check additions (in file, not in baseline)
        for name in col_map.keys() {
            if !baseline_map.contains_key(name) {
                issues.push(SchemaInconsistency {
                    file: path.clone(),
                    baseline_file: baseline_path.clone(),
                    kind: InconsistencyKind::ColumnAdded {
                        column: name.to_string(),
                    },
                    description: format!("{name}: column added vs baseline"),
                });
            }
        }
    }
    Ok(issues)
}
