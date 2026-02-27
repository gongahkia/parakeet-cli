use arrow::array::{
    Array, ArrayRef, BooleanArray, BooleanBuilder, Float32Array, Float64Array, Int32Array,
    Int64Array, StringArray,
};
use arrow::record_batch::RecordBatch;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::file::metadata::RowGroupMetaData;
use parquet::file::metadata::ParquetMetaData;
use parquet::file::statistics::Statistics;
use serde::{Deserialize, Serialize};
use std::fs::File;
use std::path::Path;

// --- AST ---

#[derive(Debug, Clone)]
pub enum CmpOp {
    Eq,
    Ne,
    Lt,
    Le,
    Gt,
    Ge,
}

#[derive(Debug, Clone)]
pub enum Value {
    Int(i64),
    Float(f64),
    Str(String),
    Bool(bool),
    Null,
}

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
    pub sample_headers: Vec<String>,   // schema column names
    pub sample_rows: Vec<Vec<String>>, // up to 10 matching rows as strings
}

// --- recursive descent parser ---

struct Parser {
    tokens: Vec<String>,
    pos: usize,
}

impl Parser {
    fn new(input: &str) -> Self {
        Parser {
            tokens: tokenize(input),
            pos: 0,
        }
    }
    fn peek(&self) -> Option<&str> {
        self.tokens.get(self.pos).map(|s| s.as_str())
    }
    fn peek_upper(&self) -> Option<String> {
        self.peek().map(|s| s.to_uppercase())
    }
    fn consume(&mut self) -> Option<&str> {
        let t = self.tokens.get(self.pos).map(|s| s.as_str());
        if t.is_some() {
            self.pos += 1;
        }
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
        if self.peek().is_some() {
            return Err(format!("unexpected token: '{}'", self.peek().unwrap()));
        }
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
                    Some(",") => {
                        self.consume();
                    }
                    Some(")") => {
                        self.consume();
                        break;
                    }
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
        s[1..s.len() - 1].to_string()
    } else {
        s.to_string()
    }
}

fn tokenize(input: &str) -> Vec<String> {
    let mut tokens = Vec::new();
    let mut chars = input.chars().peekable();
    while let Some(&c) = chars.peek() {
        if c.is_whitespace() {
            chars.next();
            continue;
        }
        if c == '\'' || c == '"' {
            // string literal with escape sequence support (\' and \" don't end the token)
            let q = c;
            let mut s = String::from(c);
            chars.next();
            loop {
                match chars.next() {
                    Some('\\') => {
                        // consume backslash; if next char is a quote push it literally
                        if let Some(&next) = chars.peek() {
                            if next == '\'' || next == '"' {
                                s.push(chars.next().unwrap());
                                continue;
                            }
                        }
                        s.push('\\');
                    }
                    Some(ch) if ch == q => {
                        s.push(ch);
                        break;
                    }
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
            if ch.is_whitespace() || "(),='\"<>!".contains(ch) {
                break;
            }
            word.push(ch);
            chars.next();
        }
        if !word.is_empty() {
            tokens.push(word);
        }
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
        Predicate::Or(a, b) => can_skip_row_group(a, rg) && can_skip_row_group(b, rg), // skip only if BOTH sides definitely false
        Predicate::Not(_) => false, // conservative: don't skip on NOT
        Predicate::Comparison { col, op, val } => {
            let stats = find_col_stats(col, rg);
            stats.map(|s| stat_can_skip(s, op, val)).unwrap_or(false)
        }
        Predicate::IsNull(col) => {
            // skip if null_count == 0 for all row groups (no nulls possible)
            let stats = find_col_stats(col, rg);
            stats
                .map(|s| s.null_count_opt().map(|nc| nc == 0).unwrap_or(false))
                .unwrap_or(false)
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
                CmpOp::Lt => *v <= min,
                CmpOp::Le => *v < min,
                CmpOp::Gt => *v >= max,
                CmpOp::Ge => *v > max,
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
                CmpOp::Lt => *v <= min,
                CmpOp::Le => *v < min,
                CmpOp::Gt => *v >= max,
                CmpOp::Ge => *v > max,
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
        Predicate::IsNull(col) => match batch.schema().index_of(col) {
            Ok(i) => arrow::compute::is_null(batch.column(i))
                .unwrap_or_else(|_| BooleanArray::from(vec![false; n])),
            Err(_) => BooleanArray::from(vec![false; n]),
        },
        Predicate::IsNotNull(col) => match batch.schema().index_of(col) {
            Ok(i) => arrow::compute::is_not_null(batch.column(i))
                .unwrap_or_else(|_| BooleanArray::from(vec![false; n])),
            Err(_) => BooleanArray::from(vec![false; n]),
        },
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
                if a.is_null(i) {
                    b.append_value(false);
                    continue;
                }
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
                if a.is_null(i) {
                    b.append_value(false);
                    continue;
                }
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
                if a.is_null(i) {
                    b.append_value(false);
                    continue;
                }
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
                if a.is_null(i) {
                    b.append_value(false);
                    continue;
                }
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
                if a.is_null(i) {
                    b.append_value(false);
                    continue;
                }
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
                if a.is_null(i) {
                    b.append_value(false);
                    continue;
                }
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
    match op {
        CmpOp::Eq => v == cv,
        CmpOp::Ne => v != cv,
        CmpOp::Lt => v < cv,
        CmpOp::Le => v <= cv,
        CmpOp::Gt => v > cv,
        CmpOp::Ge => v >= cv,
    }
}

fn cmp_f64(v: f64, op: &CmpOp, cv: f64) -> bool {
    match op {
        CmpOp::Eq => v == cv,
        CmpOp::Ne => v != cv,
        CmpOp::Lt => v < cv,
        CmpOp::Le => v <= cv,
        CmpOp::Gt => v > cv,
        CmpOp::Ge => v >= cv,
    }
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
    if vals.is_empty() {
        return false_arr;
    }
    let mut result = BooleanArray::from(vec![false; n]);
    for v in vals {
        let mask = build_mask(arr, &CmpOp::Eq, v, n);
        result = arrow::compute::or(&result, &mask)
            .unwrap_or_else(|_| BooleanArray::from(vec![false; n]));
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
    let Some(a) = arr.as_any().downcast_ref::<StringArray>() else {
        return false_arr;
    };
    let re = like_to_regex(pattern);
    let mut b = BooleanBuilder::with_capacity(n);
    for i in 0..n {
        if a.is_null(i) {
            b.append_value(false);
            continue;
        }
        b.append_value(like_match(a.value(i), &re));
    }
    b.finish()
}

// convert SQL LIKE pattern to simple match segments: % = any, _ = one char
fn like_to_regex(pattern: &str) -> Vec<LikePart> {
    let mut parts = Vec::new();
    let mut literal = String::new();
    let chars = pattern.chars().peekable();
    for c in chars {
        match c {
            '%' => {
                if !literal.is_empty() {
                    parts.push(LikePart::Literal(std::mem::take(&mut literal)));
                }
                parts.push(LikePart::Any);
            }
            '_' => {
                if !literal.is_empty() {
                    parts.push(LikePart::Literal(std::mem::take(&mut literal)));
                }
                parts.push(LikePart::One);
            }
            c => literal.push(c),
        }
    }
    if !literal.is_empty() {
        parts.push(LikePart::Literal(literal));
    }
    parts
}

#[derive(Debug)]
enum LikePart {
    Literal(String),
    Any,
    One,
}

fn like_match(s: &str, parts: &[LikePart]) -> bool {
    like_match_at(s, parts)
}

fn like_match_at(s: &str, parts: &[LikePart]) -> bool {
    if parts.is_empty() {
        return s.is_empty();
    }
    match &parts[0] {
        LikePart::Literal(lit) => {
            if s.starts_with(lit.as_str()) {
                like_match_at(&s[lit.len()..], &parts[1..])
            } else {
                false
            }
        }
        LikePart::One => {
            let mut chars = s.chars();
            if chars.next().is_some() {
                like_match_at(chars.as_str(), &parts[1..])
            } else {
                false
            }
        }
        LikePart::Any => {
            // try matching rest of parts at every position in s
            for i in 0..=s.len() {
                // ensure i is on char boundary
                if !s.is_char_boundary(i) {
                    continue;
                }
                if like_match_at(&s[i..], &parts[1..]) {
                    return true;
                }
            }
            false
        }
    }
}

/// collect all column names referenced in predicate
fn predicate_columns(pred: &Predicate) -> Vec<&str> {
    match pred {
        Predicate::Comparison { col, .. }
        | Predicate::IsNull(col)
        | Predicate::IsNotNull(col)
        | Predicate::In { col, .. }
        | Predicate::Like { col, .. } => vec![col.as_str()],
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
    let builder = ParquetRecordBatchReaderBuilder::try_new(file).map_err(|e| e.to_string())?;
    let meta: std::sync::Arc<ParquetMetaData> = builder.metadata().clone(); // single open
    // bounds check: verify all referenced columns exist in schema
    let schema = meta.file_metadata().schema_descr();
    let schema_names: Vec<String> = (0..schema.num_columns())
        .map(|i| schema.column(i).name().to_owned())
        .collect();
    for col in predicate_columns(predicate) {
        if !schema_names.iter().any(|n| n == col) {
            return Err(format!(
                "column '{}' not found in schema (available: {})",
                col,
                schema_names.join(", ")
            ));
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
    let mut sample_headers: Vec<String> = Vec::new();
    let mut sample_rows: Vec<Vec<String>> = Vec::new();
    if !rgs_to_scan.is_empty() {
        let selection = parquet::arrow::arrow_reader::RowSelection::from(
            (0..total_rgs)
                .map(|i| {
                    let count = meta.row_group(i).num_rows() as usize;
                    if rgs_to_scan.contains(&i) {
                        parquet::arrow::arrow_reader::RowSelector::select(count)
                    } else {
                        parquet::arrow::arrow_reader::RowSelector::skip(count)
                    }
                })
                .collect::<Vec<_>>(),
        );
        let reader = builder
            .with_row_selection(selection)
            .build()
            .map_err(|e| e.to_string())?;
        for batch_result in reader {
            let batch = batch_result.map_err(|e| e.to_string())?;
            scanned_rows += batch.num_rows() as u64;
            let mask = eval_predicate_batch(predicate, &batch);
            matched_rows += mask.true_count() as u64;
            // collect up to 10 sample rows from first matching batch
            if sample_headers.is_empty() && mask.true_count() > 0 {
                sample_headers = batch.schema().fields().iter().map(|f| f.name().clone()).collect();
                for row in 0..batch.num_rows() {
                    if mask.value(row) {
                        let vals: Vec<String> = batch.columns().iter().map(|col| col_val_str(col, row)).collect();
                        sample_rows.push(vals);
                        if sample_rows.len() >= 10 { break; }
                    }
                }
            }
        }
    }
    Ok(FilterResult {
        matched_rows,
        scanned_rows,
        skipped_rgs,
        total_rgs,
        sample_headers,
        sample_rows,
    })
}

#[cfg(test)]
mod tests_parse_predicate {
    use super::*;

    fn p(s: &str) -> Predicate { parse_predicate(s).expect(s) }

    #[test] fn eq_int() { assert!(matches!(p("age = 30"), Predicate::Comparison { op: CmpOp::Eq, .. })); }
    #[test] fn ne_int() { assert!(matches!(p("age != 30"), Predicate::Comparison { op: CmpOp::Ne, .. })); }
    #[test] fn lt_int() { assert!(matches!(p("age < 30"), Predicate::Comparison { op: CmpOp::Lt, .. })); }
    #[test] fn le_int() { assert!(matches!(p("age <= 30"), Predicate::Comparison { op: CmpOp::Le, .. })); }
    #[test] fn gt_int() { assert!(matches!(p("age > 30"), Predicate::Comparison { op: CmpOp::Gt, .. })); }
    #[test] fn ge_int() { assert!(matches!(p("age >= 30"), Predicate::Comparison { op: CmpOp::Ge, .. })); }
    #[test] fn is_null() { assert!(matches!(p("name IS NULL"), Predicate::IsNull(_))); }
    #[test] fn is_not_null() { assert!(matches!(p("name IS NOT NULL"), Predicate::IsNotNull(_))); }
    #[test] fn in_list() { assert!(matches!(p("city IN ('A','B')"), Predicate::In { .. })); }
    #[test] fn like_pat() { assert!(matches!(p("name LIKE 'foo%'"), Predicate::Like { .. })); }
    #[test] fn and_combo() { assert!(matches!(p("a = 1 AND b = 2"), Predicate::And(_, _))); }
    #[test] fn or_combo() { assert!(matches!(p("a = 1 OR b = 2"), Predicate::Or(_, _))); }
    #[test] fn not_combo() { assert!(matches!(p("NOT a = 1"), Predicate::Not(_))); }
    #[test] fn malformed_empty() { assert!(parse_predicate("").is_err()); }
    #[test] fn malformed_dangling() { assert!(parse_predicate("a =").is_err()); }
}

fn col_val_str(col: &dyn arrow::array::Array, row: usize) -> String {
    if col.is_null(row) {
        return "NULL".into();
    }
    match col.data_type() {
        arrow::datatypes::DataType::Int32 => col.as_any().downcast_ref::<Int32Array>().map(|a| a.value(row).to_string()).unwrap_or_default(),
        arrow::datatypes::DataType::Int64 => col.as_any().downcast_ref::<Int64Array>().map(|a| a.value(row).to_string()).unwrap_or_default(),
        arrow::datatypes::DataType::Float32 => col.as_any().downcast_ref::<Float32Array>().map(|a| a.value(row).to_string()).unwrap_or_default(),
        arrow::datatypes::DataType::Float64 => col.as_any().downcast_ref::<Float64Array>().map(|a| a.value(row).to_string()).unwrap_or_default(),
        arrow::datatypes::DataType::Utf8 => col.as_any().downcast_ref::<StringArray>().map(|a| a.value(row).to_string()).unwrap_or_default(),
        arrow::datatypes::DataType::Boolean => col.as_any().downcast_ref::<BooleanArray>().map(|a| a.value(row).to_string()).unwrap_or_default(),
        _ => format!("{:?}", col.data_type()),
    }
}
