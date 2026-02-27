use arrow::array::{Int32Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use parquet::arrow::ArrowWriter;
use parquet_lens_core::{
    aggregate_column_stats, open_parquet_file, read_column_stats, score_column,
};
use std::sync::Arc;
use tempfile::NamedTempFile;

fn write_fixture() -> NamedTempFile {
    let tmp = tempfile::Builder::new()
        .suffix(".parquet")
        .tempfile()
        .unwrap();
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("name", DataType::Utf8, true),
    ]));
    let ids = Arc::new(Int32Array::from(vec![1, 2, 3]));
    let names = Arc::new(StringArray::from(vec![Some("alice"), Some("bob"), None]));
    let batch = RecordBatch::try_new(schema.clone(), vec![ids, names]).unwrap();
    let mut writer = ArrowWriter::try_new(tmp.as_file(), schema, None).unwrap();
    writer.write(&batch).unwrap();
    writer.close().unwrap();
    tmp
}

#[test]
fn open_parquet_file_returns_info() {
    let tmp = write_fixture();
    let (info, _meta) = open_parquet_file(tmp.path()).unwrap();
    assert_eq!(info.row_count, 3);
    assert_eq!(info.schema_fields.len(), 2);
    assert_eq!(info.schema_fields[0].name, "id");
    assert_eq!(info.schema_fields[1].name, "name");
}

#[test]
fn read_column_stats_returns_entries() {
    let tmp = write_fixture();
    let (_info, meta) = open_parquet_file(tmp.path()).unwrap();
    let stats = read_column_stats(&meta);
    assert!(!stats.is_empty());
    let id_stat = stats.iter().find(|s| s.column_name == "id").unwrap();
    assert_eq!(id_stat.row_group_index, 0);
}

#[test]
fn aggregate_column_stats_sums_correctly() {
    let tmp = write_fixture();
    let (_info, meta) = open_parquet_file(tmp.path()).unwrap();
    let stats = read_column_stats(&meta);
    let agg = aggregate_column_stats(&stats, 3);
    let name_agg = agg.iter().find(|a| a.column_name == "name").unwrap();
    assert_eq!(name_agg.total_null_count, 1); // one None in fixture
    assert!((name_agg.null_percentage - 33.33).abs() < 1.0);
}

#[test]
fn score_column_on_fixture_col() {
    let tmp = write_fixture();
    let (_info, meta) = open_parquet_file(tmp.path()).unwrap();
    let stats = read_column_stats(&meta);
    let agg = aggregate_column_stats(&stats, 3);
    let name_agg = agg.iter().find(|a| a.column_name == "name").unwrap();
    let qs = score_column(
        &name_agg.column_name,
        name_agg.null_percentage,
        name_agg.total_distinct_count_estimate,
        3,
        false,
    );
    assert_eq!(qs.column_name, "name");
    assert!(qs.score <= 100);
}
