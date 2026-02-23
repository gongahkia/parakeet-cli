use std::fs::File;
use std::path::Path;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::file::reader::{FileReader, SerializedFileReader};
use parquet::file::metadata::RowGroupMetaData;
use parquet::file::statistics::Statistics;
use arrow::array::{Array, ArrayRef, BooleanArray, Int32Array, Int64Array, Float32Array, Float64Array, StringArray, BooleanBuilder};
use arrow::record_batch::RecordBatch;
use serde::{Deserialize, Serialize};

// --- AST ---

#[derive(Debug, Clone)]
pub enum CmpOp { Eq, Ne, Lt, Le, Gt, Ge }

#[derive(Debug, Clone)]
pub enum Value { Int(i64), Float(f64), Str(String), Bool(bool), Null }

#[derive(Debug, Clone)]
pub enum Predicate {
    Comparison { col: String, op: CmpOp, val: Value },
    IsNull(String),
    IsNotNull(String),
    In { col: String, vals: Vec<Value> },
    Like { col: String, pattern: String },
    And(Box<Predicate>, Box<Predicate>),
    Or(Box<Predicate>, Box<Predicate>),
    Not(Box<Predicate>),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FilterResult {
    pub matched_rows: u64,
    pub scanned_rows: u64,
    pub skipped_rgs: usize,
    pub total_rgs: usize,
}

// --- recursive descent parser ---

struct Parser { tokens: Vec<String>, pos: usize }

impl Parser {
    fn new(input: &str) -> Self {
        Parser { tokens: tokenize(input), pos: 0 }
    }
    fn peek(&self) -> Option<&str> { self.tokens.get(self.pos).map(|s| s.as_str()) }
    fn peek_upper(&self) -> Option<String> { self.peek().map(|s| s.to_uppercase()) }
    fn consume(&mut self) -> Option<&str> {
        let t = self.tokens.get(self.pos).map(|s| s.as_str());
        if t.is_some() { self.pos += 1; }
        t
    }
    fn expect(&mut self, s: &str) -> Result<(), String> {
        match self.consume() {
            Some(t) if t.eq_ignore_ascii_case(s) => Ok(()),
            Some(t) => Err(format!("expected '{s}', got '{t}'")),
            None => Err(format!("expected '{s}', got EOF")),
        }
    }
    fn parse(&mut self) -> Result<Predicate, String> {
        let p = self.parse_or()?;
        if self.peek().is_some() { return Err(format!("unexpected token: '{}'", self.peek().unwrap())); }
        Ok(p)
    }
    fn parse_or(&mut self) -> Result<Predicate, String> {
        let mut left = self.parse_and()?;
        while self.peek_upper().as_deref() == Some("OR") {
            self.consume();
            let right = self.parse_and()?;
            left = Predicate::Or(Box::new(left), Box::new(right));
        }
        Ok(left)
    }
    fn parse_and(&mut self) -> Result<Predicate, String> {
        let mut left = self.parse_not()?;
        while self.peek_upper().as_deref() == Some("AND") {
            self.consume();
            let right = self.parse_not()?;
            left = Predicate::And(Box::new(left), Box::new(right));
        }
        Ok(left)
    }
    fn parse_not(&mut self) -> Result<Predicate, String> {
        if self.peek_upper().as_deref() == Some("NOT") {
            self.consume();
            let inner = self.parse_not()?;
            return Ok(Predicate::Not(Box::new(inner)));
        }
        self.parse_atom()
    }
    fn parse_atom(&mut self) -> Result<Predicate, String> {
        // parenthesized expression
        if self.peek() == Some("(") {
            self.consume();
            let inner = self.parse_or()?;
            self.expect(")")?;
            return Ok(inner);
        }
        let col = match self.consume() {
            Some(t) => t.to_string(),
            None => return Err("expected column name, got EOF".into()),
        };
        // IS NULL / IS NOT NULL
        if self.peek_upper().as_deref() == Some("IS") {
            self.consume();
            if self.peek_upper().as_deref() == Some("NOT") {
                self.consume();
                self.expect("NULL")?;
                return Ok(Predicate::IsNotNull(col));
            }
            self.expect("NULL")?;
            return Ok(Predicate::IsNull(col));
        }
        // IN (...)
        if self.peek_upper().as_deref() == Some("IN") {
            self.consume();
            self.expect("(")?;
            let mut vals = Vec::new();
            loop {
                vals.push(self.parse_value()?);
                match self.peek() {
                    Some(",") => { self.consume(); }
                    Some(")") => { self.consume(); break; }
                    Some(t) => return Err(format!("expected ',' or ')' in IN list, got '{t}'")),
                    None => return Err("unexpected EOF in IN list".into()),
                }
            }
            return Ok(Predicate::In { col, vals });
        }
        // LIKE
        if self.peek_upper().as_deref() == Some("LIKE") {
            self.consume();
            let pattern = match self.consume() {
                Some(t) => strip_quotes(t),
                None => return Err("expected pattern after LIKE".into()),
            };
            return Ok(Predicate::Like { col, pattern });
        }
        // comparison op
        let op = match self.consume() {
            Some("=") => CmpOp::Eq,
            Some("!=") | Some("<>") => CmpOp::Ne,
            Some("<") => CmpOp::Lt,
            Some("<=") => CmpOp::Le,
            Some(">") => CmpOp::Gt,
            Some(">=") => CmpOp::Ge,
            Some(t) => return Err(format!("expected comparison operator, got '{t}'")),
            None => return Err("expected comparison operator, got EOF".into()),
        };
        let val = self.parse_value()?;
        Ok(Predicate::Comparison { col, op, val })
    }
    fn parse_value(&mut self) -> Result<Value, String> {
        match self.consume() {
            None => Err("expected value, got EOF".into()),
            Some("NULL") | Some("null") => Ok(Value::Null),
            Some("true") | Some("TRUE") => Ok(Value::Bool(true)),
            Some("false") | Some("FALSE") => Ok(Value::Bool(false)),
            Some(t) => {
                if t.starts_with('\'') || t.starts_with('"') {
                    Ok(Value::Str(strip_quotes(t)))
                } else if let Ok(i) = t.parse::<i64>() {
                    Ok(Value::Int(i))
                } else if let Ok(f) = t.parse::<f64>() {
                    Ok(Value::Float(f))
                } else {
                    Ok(Value::Str(t.to_string()))
                }
            }
        }
    }
}

fn strip_quotes(s: &str) -> String {
    if (s.starts_with('\'') && s.ends_with('\'')) || (s.starts_with('"') && s.ends_with('"')) {
        s[1..s.len()-1].to_string()
    } else {
        s.to_string()
    }
}

fn tokenize(input: &str) -> Vec<String> {
    let mut tokens = Vec::new();
    let mut chars = input.chars().peekable();
    while let Some(&c) = chars.peek() {
        if c.is_whitespace() { chars.next(); continue; }
        if c == '\'' || c == '"' { // string literal
            let q = c;
            let mut s = String::from(c);
            chars.next();
            loop {
                match chars.next() {
                    Some(ch) if ch == q => { s.push(ch); break; }
                    Some(ch) => s.push(ch),
                    None => break,
                }
            }
            tokens.push(s);
            continue;
        }
        if c == '<' || c == '>' || c == '!' || c == '=' {
            let mut op = String::from(c);
            chars.next();
            if let Some(&next) = chars.peek() {
                if (c == '<' || c == '>' || c == '!') && next == '=' {
                    op.push(next);
                    chars.next();
                }
            }
            tokens.push(op);
            continue;
        }
        if c == '(' || c == ')' || c == ',' {
            tokens.push(c.to_string());
            chars.next();
            continue;
        }
        // identifier or number
        let mut word = String::new();
        while let Some(&ch) = chars.peek() {
            if ch.is_whitespace() || "(),='\"<>!".contains(ch) { break; }
            word.push(ch);
            chars.next();
        }
        if !word.is_empty() { tokens.push(word); }
    }
    tokens
}

pub fn parse_predicate(expr: &str) -> Result<Predicate, String> {
    Parser::new(expr).parse()
}

// --- row group pushdown ---

pub fn can_skip_row_group(pred: &Predicate, rg: &RowGroupMetaData) -> bool {
    match pred {
        Predicate::And(a, b) => can_skip_row_group(a, rg) || can_skip_row_group(b, rg), // skip if EITHER side definitely false
        Predicate::Or(a, b) => can_skip_row_group(a, rg) && can_skip_row_group(b, rg),  // skip only if BOTH sides definitely false
        Predicate::Not(_) => false, // conservative: don't skip on NOT
        Predicate::Comparison { col, op, val } => {
            let stats = find_col_stats(col, rg);
            stats.map(|s| stat_can_skip(s, op, val)).unwrap_or(false)
        }
        Predicate::IsNull(col) => {
            // skip if null_count == 0 for all row groups (no nulls possible)
            let stats = find_col_stats(col, rg);
            stats.map(|s| s.null_count_opt().map(|nc| nc == 0).unwrap_or(false)).unwrap_or(false)
        }
        Predicate::IsNotNull(_) | Predicate::In { .. } | Predicate::Like { .. } => false,
    }
}

fn find_col_stats<'a>(col: &str, rg: &'a RowGroupMetaData) -> Option<&'a Statistics> {
    for i in 0..rg.num_columns() {
        let cm = rg.column(i);
        if cm.column_descr().name() == col {
            return cm.statistics();
        }
    }
    None
}

fn stat_can_skip(stats: &Statistics, op: &CmpOp, val: &Value) -> bool {
    // only handle numeric comparisons where we have typed min/max
    match (stats, val) {
        (Statistics::Int32(s), Value::Int(v)) => {
            let (min, max) = match (s.min_opt(), s.max_opt()) {
                (Some(mn), Some(mx)) => (*mn as i64, *mx as i64),
                _ => return false,
            };
            match op {
                CmpOp::Eq => *v < min || *v > max,
                CmpOp::Lt => *v <= min, // all values >= min, need val > min to have any match
                CmpOp::Le => *v < min,
                CmpOp::Gt => *v >= max,
                CmpOp::Ge => *v > max,
                CmpOp::Ne => false,
            }
        }
        (Statistics::Int64(s), Value::Int(v)) => {
            let (min, max) = match (s.min_opt(), s.max_opt()) {
                (Some(mn), Some(mx)) => (*mn, *mx),
                _ => return false,
            };
            match op {
                CmpOp::Eq => *v < min || *v > max,
                CmpOp::Lt => *v <= min,
                CmpOp::Le => *v < min,
                CmpOp::Gt => *v >= max,
                CmpOp::Ge => *v > max,
                CmpOp::Ne => false,
            }
        }
        (Statistics::Float(s), Value::Float(v)) => {
            let (min, max) = match (s.min_opt(), s.max_opt()) {
                (Some(mn), Some(mx)) => (*mn as f64, *mx as f64),
                _ => return false,
            };
            match op {
                CmpOp::Eq => *v < min || *v > max,
                CmpOp::Lt => *v <= min,
                CmpOp::Le => *v < min,
                CmpOp::Gt => *v >= max,
                CmpOp::Ge => *v > max,
                CmpOp::Ne => false,
            }
        }
        (Statistics::Double(s), Value::Float(v)) => {
            let (min, max) = match (s.min_opt(), s.max_opt()) {
                (Some(mn), Some(mx)) => (*mn, *mx),
                _ => return false,
            };
            match op {
                CmpOp::Eq => *v < min || *v > max,
                CmpOp::Lt => *v <= min,
                CmpOp::Le => *v < min,
                CmpOp::Gt => *v >= max,
                CmpOp::Ge => *v > max,
                CmpOp::Ne => false,
            }
        }
        (Statistics::Int32(s), Value::Float(v)) => {
            let (min, max) = match (s.min_opt(), s.max_opt()) {
                (Some(mn), Some(mx)) => (*mn as f64, *mx as f64),
                _ => return false,
            };
            match op {
                CmpOp::Eq => *v < min || *v > max,
                CmpOp::Lt => *v <= min, CmpOp::Le => *v < min,
                CmpOp::Gt => *v >= max, CmpOp::Ge => *v > max,
                CmpOp::Ne => false,
            }
        }
        (Statistics::Int64(s), Value::Float(v)) => {
            let (min, max) = match (s.min_opt(), s.max_opt()) {
                (Some(mn), Some(mx)) => (*mn as f64, *mx as f64),
                _ => return false,
            };
            match op {
                CmpOp::Eq => *v < min || *v > max,
                CmpOp::Lt => *v <= min, CmpOp::Le => *v < min,
                CmpOp::Gt => *v >= max, CmpOp::Ge => *v > max,
                CmpOp::Ne => false,
            }
        }
        _ => false,
    }
}

// --- filter evaluation on RecordBatch ---

fn eval_predicate_batch(pred: &Predicate, batch: &RecordBatch) -> BooleanArray {
    let n = batch.num_rows();
    match pred {
        Predicate::And(a, b) => {
            let ma = eval_predicate_batch(a, batch);
            let mb = eval_predicate_batch(b, batch);
            arrow::compute::and(&ma, &mb).unwrap_or_else(|_| BooleanArray::from(vec![false; n]))
        }
        Predicate::Or(a, b) => {
            let ma = eval_predicate_batch(a, batch);
            let mb = eval_predicate_batch(b, batch);
            arrow::compute::or(&ma, &mb).unwrap_or_else(|_| BooleanArray::from(vec![false; n]))
        }
        Predicate::Not(inner) => {
            let m = eval_predicate_batch(inner, batch);
            arrow::compute::not(&m).unwrap_or_else(|_| BooleanArray::from(vec![false; n]))
        }
        Predicate::IsNull(col) => {
            match batch.schema().index_of(col) {
                Ok(i) => arrow::compute::is_null(batch.column(i)).unwrap_or_else(|_| BooleanArray::from(vec![false; n])),
                Err(_) => BooleanArray::from(vec![false; n]),
            }
        }
        Predicate::IsNotNull(col) => {
            match batch.schema().index_of(col) {
                Ok(i) => arrow::compute::is_not_null(batch.column(i)).unwrap_or_else(|_| BooleanArray::from(vec![false; n])),
                Err(_) => BooleanArray::from(vec![false; n]),
            }
        }
        Predicate::Comparison { col, op, val } => eval_comparison(col, op, val, batch),
        Predicate::In { col, vals } => eval_in(col, vals, batch),
        Predicate::Like { col, pattern } => eval_like(col, pattern, batch),
    }
}

fn eval_comparison(col: &str, op: &CmpOp, val: &Value, batch: &RecordBatch) -> BooleanArray {
    let n = batch.num_rows();
    let false_arr = || BooleanArray::from(vec![false; n]);
    let idx = match batch.schema().index_of(col) {
        Ok(i) => i,
        Err(_) => return false_arr(),
    };
    let arr = batch.column(idx);
    build_mask(arr, op, val, n)
}

fn build_mask(arr: &ArrayRef, op: &CmpOp, val: &Value, n: usize) -> BooleanArray {
    let false_arr = BooleanArray::from(vec![false; n]);
    // try i32
    if let Some(a) = arr.as_any().downcast_ref::<Int32Array>() {
        let cmp_val: Option<i64> = match val {
            Value::Int(v) => Some(*v),
            Value::Float(v) => Some(*v as i64),
            _ => None,
        };
        if let Some(cv) = cmp_val {
            let mut b = BooleanBuilder::with_capacity(n);
            for i in 0..n {
                if a.is_null(i) { b.append_value(false); continue; }
                let v = a.value(i) as i64;
                b.append_value(cmp_i64(v, op, cv));
            }
            return b.finish();
        }
        return false_arr;
    }
    // try i64
    if let Some(a) = arr.as_any().downcast_ref::<Int64Array>() {
        let cmp_val: Option<i64> = match val {
            Value::Int(v) => Some(*v),
            Value::Float(v) => Some(*v as i64),
            _ => None,
        };
        if let Some(cv) = cmp_val {
            let mut b = BooleanBuilder::with_capacity(n);
            for i in 0..n {
                if a.is_null(i) { b.append_value(false); continue; }
                b.append_value(cmp_i64(a.value(i), op, cv));
            }
            return b.finish();
        }
        return false_arr;
    }
    // try f32
    if let Some(a) = arr.as_any().downcast_ref::<Float32Array>() {
        let cmp_val: Option<f64> = match val {
            Value::Float(v) => Some(*v),
            Value::Int(v) => Some(*v as f64),
            _ => None,
        };
        if let Some(cv) = cmp_val {
            let mut b = BooleanBuilder::with_capacity(n);
            for i in 0..n {
                if a.is_null(i) { b.append_value(false); continue; }
                b.append_value(cmp_f64(a.value(i) as f64, op, cv));
            }
            return b.finish();
        }
        return false_arr;
    }
    // try f64
    if let Some(a) = arr.as_any().downcast_ref::<Float64Array>() {
        let cmp_val: Option<f64> = match val {
            Value::Float(v) => Some(*v),
            Value::Int(v) => Some(*v as f64),
            _ => None,
        };
        if let Some(cv) = cmp_val {
            let mut b = BooleanBuilder::with_capacity(n);
            for i in 0..n {
                if a.is_null(i) { b.append_value(false); continue; }
                b.append_value(cmp_f64(a.value(i), op, cv));
            }
            return b.finish();
        }
        return false_arr;
    }
    // try string
    if let Some(a) = arr.as_any().downcast_ref::<StringArray>() {
        if let Value::Str(sv) = val {
            let mut b = BooleanBuilder::with_capacity(n);
            for i in 0..n {
                if a.is_null(i) { b.append_value(false); continue; }
                let v = a.value(i);
                let matched = match op {
                    CmpOp::Eq => v == sv.as_str(),
                    CmpOp::Ne => v != sv.as_str(),
                    CmpOp::Lt => v < sv.as_str(),
                    CmpOp::Le => v <= sv.as_str(),
                    CmpOp::Gt => v > sv.as_str(),
                    CmpOp::Ge => v >= sv.as_str(),
                };
                b.append_value(matched);
            }
            return b.finish();
        }
        return false_arr;
    }
    // try bool
    if let Some(a) = arr.as_any().downcast_ref::<BooleanArray>() {
        if let Value::Bool(bv) = val {
            let mut b = BooleanBuilder::with_capacity(n);
            for i in 0..n {
                if a.is_null(i) { b.append_value(false); continue; }
                let matched = match op {
                    CmpOp::Eq => a.value(i) == *bv,
                    CmpOp::Ne => a.value(i) != *bv,
                    _ => false,
                };
                b.append_value(matched);
            }
            return b.finish();
        }
        return false_arr;
    }
    false_arr
}

fn cmp_i64(v: i64, op: &CmpOp, cv: i64) -> bool {
    match op { CmpOp::Eq => v == cv, CmpOp::Ne => v != cv, CmpOp::Lt => v < cv, CmpOp::Le => v <= cv, CmpOp::Gt => v > cv, CmpOp::Ge => v >= cv }
}

fn cmp_f64(v: f64, op: &CmpOp, cv: f64) -> bool {
    match op { CmpOp::Eq => v == cv, CmpOp::Ne => v != cv, CmpOp::Lt => v < cv, CmpOp::Le => v <= cv, CmpOp::Gt => v > cv, CmpOp::Ge => v >= cv }
}

fn eval_in(col: &str, vals: &[Value], batch: &RecordBatch) -> BooleanArray {
    let n = batch.num_rows();
    let false_arr = BooleanArray::from(vec![false; n]);
    let idx = match batch.schema().index_of(col) {
        Ok(i) => i,
        Err(_) => return false_arr,
    };
    let arr = batch.column(idx);
    // build OR of equality masks
    if vals.is_empty() { return false_arr; }
    let mut result = BooleanArray::from(vec![false; n]);
    for v in vals {
        let mask = build_mask(arr, &CmpOp::Eq, v, n);
        result = arrow::compute::or(&result, &mask).unwrap_or_else(|_| BooleanArray::from(vec![false; n]));
    }
    result
}

fn eval_like(col: &str, pattern: &str, batch: &RecordBatch) -> BooleanArray {
    let n = batch.num_rows();
    let false_arr = BooleanArray::from(vec![false; n]);
    let idx = match batch.schema().index_of(col) {
        Ok(i) => i,
        Err(_) => return false_arr,
    };
    let arr = batch.column(idx);
    let Some(a) = arr.as_any().downcast_ref::<StringArray>() else { return false_arr; };
    let re = like_to_regex(pattern);
    let mut b = BooleanBuilder::with_capacity(n);
    for i in 0..n {
        if a.is_null(i) { b.append_value(false); continue; }
        b.append_value(like_match(a.value(i), &re));
    }
    b.finish()
}

// convert SQL LIKE pattern to simple match segments: % = any, _ = one char
fn like_to_regex(pattern: &str) -> Vec<LikePart> {
    let mut parts = Vec::new();
    let mut literal = String::new();
    let mut chars = pattern.chars().peekable();
    while let Some(c) = chars.next() {
        match c {
            '%' => { if !literal.is_empty() { parts.push(LikePart::Literal(std::mem::take(&mut literal))); } parts.push(LikePart::Any); }
            '_' => { if !literal.is_empty() { parts.push(LikePart::Literal(std::mem::take(&mut literal))); } parts.push(LikePart::One); }
            c => literal.push(c),
        }
    }
    if !literal.is_empty() { parts.push(LikePart::Literal(literal)); }
    parts
}

#[derive(Debug)]
enum LikePart { Literal(String), Any, One }

fn like_match(s: &str, parts: &[LikePart]) -> bool {
    like_match_at(s, parts)
}

fn like_match_at(s: &str, parts: &[LikePart]) -> bool {
    if parts.is_empty() { return s.is_empty(); }
    match &parts[0] {
        LikePart::Literal(lit) => {
            if s.starts_with(lit.as_str()) { like_match_at(&s[lit.len()..], &parts[1..]) } else { false }
        }
        LikePart::One => {
            let mut chars = s.chars();
            if chars.next().is_some() { like_match_at(chars.as_str(), &parts[1..]) } else { false }
        }
        LikePart::Any => {
            // try matching rest of parts at every position in s
            for i in 0..=s.len() {
                // ensure i is on char boundary
                if !s.is_char_boundary(i) { continue; }
                if like_match_at(&s[i..], &parts[1..]) { return true; }
            }
            false
        }
    }
}

/// collect all column names referenced in predicate
fn predicate_columns(pred: &Predicate) -> Vec<&str> {
    match pred {
        Predicate::Comparison { col, .. } | Predicate::IsNull(col) | Predicate::IsNotNull(col)
        | Predicate::In { col, .. } | Predicate::Like { col, .. } => vec![col.as_str()],
        Predicate::And(a, b) | Predicate::Or(a, b) => {
            let mut cols = predicate_columns(a);
            cols.extend(predicate_columns(b));
            cols
        }
        Predicate::Not(inner) => predicate_columns(inner),
    }
}

// --- main filter_count entry point ---

pub fn filter_count(path: &Path, predicate: &Predicate) -> Result<FilterResult, String> {
    let file = File::open(path).map_err(|e| e.to_string())?;
    let reader = SerializedFileReader::new(file).map_err(|e| e.to_string())?;
    let meta = reader.metadata();
    // bounds check: verify all referenced columns exist in schema
    let schema = meta.file_metadata().schema_descr();
    let schema_names: Vec<String> = (0..schema.num_columns()).map(|i| schema.column(i).name().to_owned()).collect();
    for col in predicate_columns(predicate) {
        if !schema_names.iter().any(|n| n == col) {
            return Err(format!("column '{}' not found in schema (available: {})", col, schema_names.join(", ")));
        }
    }
    let total_rgs = meta.num_row_groups();
    let mut skipped_rgs = 0usize;
    let mut rgs_to_scan: Vec<usize> = Vec::new();
    for rg_idx in 0..total_rgs {
        let rg = meta.row_group(rg_idx);
        if can_skip_row_group(predicate, rg) {
            skipped_rgs += 1;
        } else {
            rgs_to_scan.push(rg_idx);
        }
    }
    let mut matched_rows = 0u64;
    let mut scanned_rows = 0u64;
    if !rgs_to_scan.is_empty() {
        let file2 = File::open(path).map_err(|e| e.to_string())?;
        let builder = ParquetRecordBatchReaderBuilder::try_new(file2).map_err(|e| e.to_string())?;
        // read all row groups (we can't easily select specific ones without schema issues)
        // build reader for all, then skip by tracking which rg we're in
        let reader = builder.build().map_err(|e| e.to_string())?;
        let _rg_cursor = 0usize; // tracks which RG we think we're in (approx)
        let _rows_in_rg = 0i64;
        // load rg row counts for tracking
        let rg_row_counts: Vec<i64> = (0..total_rgs).map(|i| meta.row_group(i).num_rows()).collect();
        let mut rg_idx = 0usize;
        let mut rows_seen_in_current_rg = 0i64;
        for batch_result in reader {
            let batch = batch_result.map_err(|e| e.to_string())?;
            let batch_rows = batch.num_rows() as i64;
            // determine if this batch belongs to a skipped rg
            // advance rg_idx to consume batch rows
            let mut batch_remaining = batch_rows;
            let mut batch_offset = 0i64;
            while batch_remaining > 0 && rg_idx < total_rgs {
                let rg_remaining = rg_row_counts[rg_idx] - rows_seen_in_current_rg;
                let take = batch_remaining.min(rg_remaining);
                let is_skipped = skipped_rgs > 0 && !rgs_to_scan.contains(&rg_idx);
                if !is_skipped {
                    // slice the batch for this rg portion
                    let slice = batch.slice(batch_offset as usize, take as usize);
                    scanned_rows += take as u64;
                    let mask = eval_predicate_batch(predicate, &slice);
                    matched_rows += mask.true_count() as u64;
                }
                rows_seen_in_current_rg += take;
                batch_offset += take;
                batch_remaining -= take;
                if rows_seen_in_current_rg >= rg_row_counts[rg_idx] {
                    rg_idx += 1;
                    rows_seen_in_current_rg = 0;
                }
            }
        }
    }
    Ok(FilterResult { matched_rows, scanned_rows, skipped_rgs, total_rgs })
}
