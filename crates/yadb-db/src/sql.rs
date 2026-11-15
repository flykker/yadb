use std::cmp::Ordering;
use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

use crate::{DbError, QueryResult};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ColumnType {
    Int,
    Text,
    Bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ColumnDef {
    pub name: String,
    pub col_type: ColumnType,
    pub primary_key: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum SqlValue {
    Null,
    Bool(bool),
    Int(i64),
    Text(String),
}

impl SqlValue {
    pub fn check_type(&self, expected: ColumnType, allow_null: bool) -> bool {
        match (self, expected) {
            (SqlValue::Null, _) => allow_null,
            (SqlValue::Int(_), ColumnType::Int) => true,
            (SqlValue::Text(_), ColumnType::Text) => true,
            (SqlValue::Bool(_), ColumnType::Bool) => true,
            _ => false,
        }
    }

    /// Encodes a non-null SQL value into a byte-lexicographically order-preserving key!
    /// Signed 64-bit integers flip the sign bit (`u64::MIN` ^= `1<<63`) and serialize big-endian
    /// so `-100 < -1 < 0 < 42` in raw byte order inside BTreeMaps, SSTables, and Secondary Indexes.
    pub fn encode_order_preserving_key(&self) -> Result<Vec<u8>, DbError> {
        match self {
            SqlValue::Int(v) => {
                let sortable = (*v as u64) ^ 0x8000_0000_0000_0000;
                let mut out = Vec::with_capacity(9);
                out.push(0x01);
                out.extend_from_slice(&sortable.to_be_bytes());
                Ok(out)
            }
            SqlValue::Text(s) => {
                let mut out = Vec::with_capacity(1 + s.len());
                out.push(0x02);
                out.extend_from_slice(s.as_bytes());
                Ok(out)
            }
            SqlValue::Bool(b) => Ok(vec![0x03, u8::from(*b)]),
            SqlValue::Null => Err(DbError::InvalidQuery("KEY value cannot be NULL".into())),
        }
    }

    pub fn sql_cmp(&self, other: &Self) -> Option<Ordering> {
        match (self, other) {
            (SqlValue::Int(a), SqlValue::Int(b)) => Some(a.cmp(b)),
            (SqlValue::Text(a), SqlValue::Text(b)) => Some(a.cmp(b)),
            (SqlValue::Bool(a), SqlValue::Bool(b)) => Some(a.cmp(b)),
            (SqlValue::Null, SqlValue::Null) => Some(Ordering::Equal),
            (SqlValue::Null, _) => Some(Ordering::Less),
            (_, SqlValue::Null) => Some(Ordering::Greater),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CmpOp {
    Eq,
    Ne,
    Lt,
    Le,
    Gt,
    Ge,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Predicate {
    pub column: String,
    pub op: CmpOp,
    pub value: SqlValue,
}

impl Predicate {
    pub fn eval(&self, cell: &SqlValue) -> bool {
        if matches!(cell, SqlValue::Null) || matches!(self.value, SqlValue::Null) {
            return false;
        }
        let Some(ord) = cell.sql_cmp(&self.value) else {
            return false;
        };
        match self.op {
            CmpOp::Eq => ord == Ordering::Equal,
            CmpOp::Ne => ord != Ordering::Equal,
            CmpOp::Lt => ord == Ordering::Less,
            CmpOp::Le => ord != Ordering::Greater,
            CmpOp::Gt => ord == Ordering::Greater,
            CmpOp::Ge => ord != Ordering::Less,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AggFunc {
    Count,
    Sum,
    Min,
    Max,
    Avg,
}

impl AggFunc {
    pub fn name(&self) -> &'static str {
        match self {
            AggFunc::Count => "COUNT",
            AggFunc::Sum => "SUM",
            AggFunc::Min => "MIN",
            AggFunc::Max => "MAX",
            AggFunc::Avg => "AVG",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SelectItem {
    Wildcard,
    Column {
        name: String,
        alias: Option<String>,
    },
    Aggregate {
        func: AggFunc,
        column: Option<String>, // `None` means `COUNT(*)`
        alias: Option<String>,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Projection {
    All,
    Columns(Vec<String>),
    Items(Vec<SelectItem>),
}

/// The closed set of scalar functions a `SELECT` without `FROM` may evaluate.
///
/// This is deliberately not an expression language: every variant is either a
/// constant or a value the daemon already knows, so nothing here can read or
/// write table data and no clause of a `SELECT ... FROM` can be affected.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ScalarFunc {
    Now,
    Version,
    CurrentDatabase,
    CurrentSchema,
    CurrentUser,
    CurrentSetting { name: String, missing_ok: bool },
}

impl ScalarFunc {
    pub fn name(&self) -> &'static str {
        match self {
            ScalarFunc::Now => "now",
            ScalarFunc::Version => "version",
            ScalarFunc::CurrentDatabase => "current_database",
            ScalarFunc::CurrentSchema => "current_schema",
            ScalarFunc::CurrentUser => "current_user",
            ScalarFunc::CurrentSetting { .. } => "current_setting",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ScalarExpr {
    Literal(SqlValue),
    Func(ScalarFunc),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SelectExpr {
    pub expr: ScalarExpr,
    pub alias: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ArithOp {
    Add,
    Sub,
    Mul,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AssignExpr {
    Literal(SqlValue),
    Column(String),
    BinOp {
        column: String,
        op: ArithOp,
        rhs: i64,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SqlStmt {
    Begin,
    Commit,
    Rollback,
    ShowTables,
    ShowIndexes {
        table: String,
    },
    CreateTable {
        table: String,
        if_not_exists: bool,
        columns: Vec<ColumnDef>,
    },
    CreateIndex {
        index_name: String,
        if_not_exists: bool,
        table: String,
        column: String,
    },
    DropIndex {
        index_name: String,
        if_exists: bool,
        table: String,
    },
    Insert {
        table: String,
        upsert: bool,
        columns: Option<Vec<String>>,
        rows: Vec<Vec<SqlValue>>,
    },
    Update {
        table: String,
        assignments: Vec<(String, AssignExpr)>,
        predicates: Vec<Predicate>,
    },
    Select {
        table: String,
        projection: Projection,
        predicates: Vec<Predicate>,
        group_by: Vec<String>,
        order_by: Option<(String, bool)>,
        limit: Option<usize>,
    },
    /// `SELECT <constants and scalar functions>` with no `FROM` clause.
    ///
    /// Kept as a separate variant so the `SELECT ... FROM` path is byte-for-byte
    /// the same code it was before: a statement containing a top-level `FROM` is
    /// never routed here.
    SelectNoFrom {
        items: Vec<SelectExpr>,
    },
    /// `SET <name> { TO | = } <value>`; the value is stored verbatim.
    Set {
        name: String,
        value: String,
    },
    /// `SET <name> { TO | = } DEFAULT`, which restores the documented default.
    ///
    /// Distinct from `Reset` only so the command tag can be `SET`, matching
    /// PostgreSQL, while `RESET <name>` still answers `RESET`.
    SetDefault {
        name: String,
    },
    /// `RESET <name>` or `RESET ALL`.
    Reset {
        name: Option<String>,
    },
    /// `SHOW <name>` for a single configuration parameter.
    ShowSetting {
        name: String,
    },
    /// `SHOW ALL`, which is refused rather than faked.
    ShowAll,
    Delete {
        table: String,
        predicates: Vec<Predicate>,
    },
    FlushTable {
        table: String,
    },
    CompactTable {
        table: String,
    },
}

impl SqlStmt {
    pub fn is_write(&self) -> bool {
        match self {
            SqlStmt::ShowTables
            | SqlStmt::ShowIndexes { .. }
            | SqlStmt::Select { .. }
            | SqlStmt::SelectNoFrom { .. }
            | SqlStmt::Set { .. }
            | SqlStmt::SetDefault { .. }
            | SqlStmt::Reset { .. }
            | SqlStmt::ShowSetting { .. }
            | SqlStmt::ShowAll => false,
            SqlStmt::Begin
            | SqlStmt::Commit
            | SqlStmt::Rollback
            | SqlStmt::CreateTable { .. }
            | SqlStmt::CreateIndex { .. }
            | SqlStmt::DropIndex { .. }
            | SqlStmt::Insert { .. }
            | SqlStmt::Update { .. }
            | SqlStmt::Delete { .. }
            | SqlStmt::FlushTable { .. }
            | SqlStmt::CompactTable { .. } => true,
        }
    }
}

pub fn script_has_writes(stmts: &[SqlStmt]) -> bool {
    stmts.iter().any(SqlStmt::is_write)
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum Tok {
    Ident(String),
    Int(i64),
    Str(String),
    LParen,
    RParen,
    Comma,
    Star,
    Plus,
    Minus,
    Dot,
    Semi,
    Op(CmpOp),
}

fn tokenize(input: &str) -> Result<Vec<Tok>, DbError> {
    let chars: Vec<char> = input.chars().collect();
    let mut i = 0usize;
    let mut out = Vec::new();

    while i < chars.len() {
        let c = chars[i];
        if c.is_whitespace() {
            i += 1;
            continue;
        }
        if c == '-' && i + 1 < chars.len() && chars[i + 1] == '-' {
            while i < chars.len() && chars[i] != '\n' {
                i += 1;
            }
            continue;
        }
        match c {
            '(' => {
                out.push(Tok::LParen);
                i += 1;
            }
            ')' => {
                out.push(Tok::RParen);
                i += 1;
            }
            ',' => {
                out.push(Tok::Comma);
                i += 1;
            }
            '*' => {
                out.push(Tok::Star);
                i += 1;
            }
            '+' => {
                out.push(Tok::Plus);
                i += 1;
            }
            '-' => {
                out.push(Tok::Minus);
                i += 1;
            }
            '.' => {
                out.push(Tok::Dot);
                i += 1;
            }
            ';' => {
                out.push(Tok::Semi);
                i += 1;
            }
            '=' => {
                out.push(Tok::Op(CmpOp::Eq));
                i += 1;
            }
            '!' if i + 1 < chars.len() && chars[i + 1] == '=' => {
                out.push(Tok::Op(CmpOp::Ne));
                i += 2;
            }
            '<' if i + 1 < chars.len() && chars[i + 1] == '>' => {
                out.push(Tok::Op(CmpOp::Ne));
                i += 2;
            }
            '<' if i + 1 < chars.len() && chars[i + 1] == '=' => {
                out.push(Tok::Op(CmpOp::Le));
                i += 2;
            }
            '<' => {
                out.push(Tok::Op(CmpOp::Lt));
                i += 1;
            }
            '>' if i + 1 < chars.len() && chars[i + 1] == '=' => {
                out.push(Tok::Op(CmpOp::Ge));
                i += 2;
            }
            '>' => {
                out.push(Tok::Op(CmpOp::Gt));
                i += 1;
            }
            '\'' => {
                i += 1;
                let mut s = String::new();
                let mut closed = false;
                while i < chars.len() {
                    if chars[i] == '\'' {
                        if i + 1 < chars.len() && chars[i + 1] == '\'' {
                            s.push('\'');
                            i += 2;
                        } else {
                            i += 1;
                            closed = true;
                            break;
                        }
                    } else {
                        s.push(chars[i]);
                        i += 1;
                    }
                }
                if !closed {
                    return Err(DbError::InvalidQuery("unterminated string literal".into()));
                }
                out.push(Tok::Str(s));
            }
            _ if c.is_ascii_digit() => {
                let start = i;
                i += 1;
                while i < chars.len() && chars[i].is_ascii_digit() {
                    i += 1;
                }
                let s: String = chars[start..i].iter().collect();
                let n: i64 = s
                    .parse()
                    .map_err(|_| DbError::InvalidQuery(format!("invalid integer `{s}`")))?;
                out.push(Tok::Int(n));
            }
            _ if c.is_ascii_alphabetic() || c == '_' => {
                let start = i;
                i += 1;
                while i < chars.len() && (chars[i].is_ascii_alphanumeric() || chars[i] == '_') {
                    i += 1;
                }
                let ident: String = chars[start..i].iter().collect();
                out.push(Tok::Ident(ident));
            }
            other => {
                return Err(DbError::InvalidQuery(format!(
                    "unexpected character `{other}` in SQL"
                )));
            }
        }
    }

    Ok(out)
}

struct Parser {
    toks: Vec<Tok>,
    pos: usize,
}

impl Parser {
    fn peek(&self) -> Option<&Tok> {
        self.toks.get(self.pos)
    }

    fn peek_next(&self) -> Option<&Tok> {
        self.toks.get(self.pos + 1)
    }

    fn next(&mut self) -> Option<Tok> {
        let t = self.toks.get(self.pos).cloned();
        if t.is_some() {
            self.pos += 1;
        }
        t
    }

    fn eat_kw(&mut self, kw: &str) -> bool {
        if let Some(Tok::Ident(s)) = self.peek() {
            if s.eq_ignore_ascii_case(kw) {
                self.pos += 1;
                return true;
            }
        }
        false
    }

    fn expect_kw(&mut self, kw: &str) -> Result<(), DbError> {
        if self.eat_kw(kw) {
            Ok(())
        } else {
            Err(DbError::InvalidQuery(format!(
                "expected keyword `{kw}`, found {:?}",
                self.peek()
            )))
        }
    }

    fn expect_ident(&mut self) -> Result<String, DbError> {
        match self.next() {
            Some(Tok::Ident(s)) => Ok(s),
            other => Err(DbError::InvalidQuery(format!(
                "expected identifier, found {other:?}"
            ))),
        }
    }

    /// Reads a configuration parameter name, allowing PostgreSQL's dotted
    /// custom-GUC form (`myapp.tenant`). The result is lower-cased because
    /// parameter names are case-insensitive.
    fn expect_param_name(&mut self) -> Result<String, DbError> {
        let mut name = self.expect_ident()?.to_ascii_lowercase();
        while self.peek() == Some(&Tok::Dot) {
            self.next(); // consume `.`
            name.push('.');
            name.push_str(&self.expect_ident()?.to_ascii_lowercase());
        }
        Ok(name)
    }

    fn expect_tok(&mut self, expected: Tok) -> Result<(), DbError> {
        let got = self.next();
        if got.as_ref() == Some(&expected) {
            Ok(())
        } else {
            Err(DbError::InvalidQuery(format!(
                "expected {expected:?}, found {got:?}"
            )))
        }
    }

    fn parse_signed_int(&mut self) -> Result<i64, DbError> {
        let neg = if self.peek() == Some(&Tok::Minus) {
            self.next();
            true
        } else if self.peek() == Some(&Tok::Plus) {
            self.next();
            false
        } else {
            false
        };
        match self.next() {
            Some(Tok::Int(n)) => Ok(if neg { -n } else { n }),
            other => Err(DbError::InvalidQuery(format!(
                "expected integer literal, found {other:?}"
            ))),
        }
    }

    fn parse_value(&mut self) -> Result<SqlValue, DbError> {
        if self.peek() == Some(&Tok::Minus) || self.peek() == Some(&Tok::Plus) {
            return Ok(SqlValue::Int(self.parse_signed_int()?));
        }
        match self.next() {
            Some(Tok::Int(n)) => Ok(SqlValue::Int(n)),
            Some(Tok::Str(s)) => Ok(SqlValue::Text(s)),
            Some(Tok::Ident(s)) if s.eq_ignore_ascii_case("true") => Ok(SqlValue::Bool(true)),
            Some(Tok::Ident(s)) if s.eq_ignore_ascii_case("false") => Ok(SqlValue::Bool(false)),
            Some(Tok::Ident(s)) if s.eq_ignore_ascii_case("null") => Ok(SqlValue::Null),
            other => Err(DbError::InvalidQuery(format!(
                "expected SQL literal value, found {other:?}"
            ))),
        }
    }

    fn parse_assign_expr(&mut self) -> Result<AssignExpr, DbError> {
        if let Some(Tok::Ident(s)) = self.peek().cloned() {
            let is_lit = s.eq_ignore_ascii_case("true")
                || s.eq_ignore_ascii_case("false")
                || s.eq_ignore_ascii_case("null");
            if !is_lit {
                self.next();
                let op_opt = match self.peek() {
                    Some(Tok::Plus) => Some(ArithOp::Add),
                    Some(Tok::Minus) => Some(ArithOp::Sub),
                    Some(Tok::Star) => Some(ArithOp::Mul),
                    _ => None,
                };
                if let Some(op) = op_opt {
                    self.next();
                    let rhs = self.parse_signed_int()?;
                    return Ok(AssignExpr::BinOp { column: s, op, rhs });
                }
                return Ok(AssignExpr::Column(s));
            }
        }
        Ok(AssignExpr::Literal(self.parse_value()?))
    }

    fn parse_where(&mut self) -> Result<Vec<Predicate>, DbError> {
        if !self.eat_kw("WHERE") {
            return Ok(Vec::new());
        }
        let mut preds = Vec::new();
        loop {
            let column = self.expect_ident()?;
            let op = match self.next() {
                Some(Tok::Op(op)) => op,
                other => {
                    return Err(DbError::InvalidQuery(format!(
                        "expected comparison operator in WHERE, found {other:?}"
                    )))
                }
            };
            let value = self.parse_value()?;
            preds.push(Predicate { column, op, value });
            if !self.eat_kw("AND") {
                break;
            }
        }
        Ok(preds)
    }

    fn parse_optional_alias(&mut self) -> Result<Option<String>, DbError> {
        if self.eat_kw("AS") {
            return Ok(Some(self.expect_ident()?));
        }
        Ok(None)
    }

    /// True when a `FROM` keyword appears at paren depth zero before the end of
    /// this statement. `SELECT` statements that contain one always take the
    /// legacy `SELECT ... FROM` path, so a column named like a scalar function
    /// still parses exactly as it did before.
    fn has_top_level_from(&self) -> bool {
        let mut depth = 0i32;
        for tok in &self.toks[self.pos..] {
            match tok {
                Tok::LParen => depth += 1,
                Tok::RParen => depth -= 1,
                Tok::Semi => break,
                Tok::Ident(s) if depth == 0 && s.eq_ignore_ascii_case("FROM") => return true,
                _ => {}
            }
        }
        false
    }

    fn parse_scalar_select_items(&mut self) -> Result<Vec<SelectExpr>, DbError> {
        let mut items = Vec::new();
        loop {
            items.push(SelectExpr {
                expr: self.parse_scalar_expr()?,
                alias: self.parse_optional_alias()?,
            });
            if self.peek() == Some(&Tok::Comma) {
                self.next();
            } else {
                break;
            }
        }
        if !matches!(self.peek(), None | Some(Tok::Semi)) {
            return Err(DbError::InvalidQuery(format!(
                "a SELECT without FROM supports only a list of constants and scalar functions, found {:?}",
                self.peek()
            )));
        }
        Ok(items)
    }

    fn parse_scalar_expr(&mut self) -> Result<ScalarExpr, DbError> {
        if matches!(self.peek(), Some(Tok::Minus) | Some(Tok::Plus)) {
            return Ok(ScalarExpr::Literal(SqlValue::Int(self.parse_signed_int()?)));
        }
        match self.peek().cloned() {
            Some(Tok::Int(_)) | Some(Tok::Str(_)) => Ok(ScalarExpr::Literal(self.parse_value()?)),
            Some(Tok::Ident(word)) => {
                if matches!(
                    word.to_ascii_lowercase().as_str(),
                    "true" | "false" | "null"
                ) {
                    return Ok(ScalarExpr::Literal(self.parse_value()?));
                }
                self.parse_scalar_func()
            }
            other => Err(DbError::InvalidQuery(format!(
                "a SELECT without FROM supports only constants and scalar functions, found {other:?}"
            ))),
        }
    }

    fn parse_scalar_func(&mut self) -> Result<ScalarExpr, DbError> {
        let mut name = self.expect_ident()?;
        if name.eq_ignore_ascii_case("pg_catalog") && self.peek() == Some(&Tok::Dot) {
            self.next();
            name = self.expect_ident()?;
        }
        let lower = name.to_ascii_lowercase();
        let call = self.peek() == Some(&Tok::LParen);
        let func = match lower.as_str() {
            "now" | "version" => {
                if !call {
                    return Err(DbError::InvalidQuery(format!(
                        "`{lower}` requires parentheses in a SELECT without FROM"
                    )));
                }
                self.next();
                self.expect_tok(Tok::RParen)?;
                if lower == "now" {
                    ScalarFunc::Now
                } else {
                    ScalarFunc::Version
                }
            }
            "current_database" | "current_schema" | "current_user" | "session_user" | "user" => {
                if call {
                    self.next();
                    self.expect_tok(Tok::RParen)?;
                }
                match lower.as_str() {
                    "current_database" => ScalarFunc::CurrentDatabase,
                    "current_schema" => ScalarFunc::CurrentSchema,
                    _ => ScalarFunc::CurrentUser,
                }
            }
            "current_setting" => {
                if !call {
                    return Err(DbError::InvalidQuery(
                        "`current_setting` requires parentheses in a SELECT without FROM".into(),
                    ));
                }
                self.next();
                let setting = match self.next() {
                    Some(Tok::Str(s)) => s.to_ascii_lowercase(),
                    other => {
                        return Err(DbError::InvalidQuery(format!(
                            "current_setting needs a string literal name, found {other:?}"
                        )))
                    }
                };
                let mut missing_ok = false;
                if self.peek() == Some(&Tok::Comma) {
                    self.next();
                    missing_ok = match self.parse_value()? {
                        SqlValue::Bool(b) => b,
                        other => {
                            return Err(DbError::InvalidQuery(format!(
                                "current_setting missing_ok must be a boolean, found {other:?}"
                            )))
                        }
                    };
                }
                self.expect_tok(Tok::RParen)?;
                ScalarFunc::CurrentSetting {
                    name: setting,
                    missing_ok,
                }
            }
            other => {
                return Err(DbError::InvalidQuery(format!(
                    "unsupported scalar function `{other}` in a SELECT without FROM"
                )))
            }
        };
        Ok(ScalarExpr::Func(func))
    }

    /// `SET [SESSION|LOCAL] <name> { TO | = } <value>`.
    fn parse_set(&mut self) -> Result<SqlStmt, DbError> {
        if !self.eat_kw("SESSION") {
            let _ = self.eat_kw("LOCAL");
        }
        let name = self.expect_param_name()?;
        if name.eq_ignore_ascii_case("TRANSACTION")
            || name.eq_ignore_ascii_case("CONSTRAINTS")
            || name.eq_ignore_ascii_case("CHARACTERISTICS")
            || name.eq_ignore_ascii_case("ROLE")
            || name.eq_ignore_ascii_case("SESSION_AUTHORIZATION")
        {
            return Err(DbError::InvalidQuery(format!(
                "SET {name} is not supported; only SET <parameter> = <value> is implemented"
            )));
        }
        if name.eq_ignore_ascii_case("TIME") {
            return Err(DbError::InvalidQuery(
                "SET TIME ZONE is not supported; use SET TimeZone = 'UTC'".into(),
            ));
        }
        let to = self.eat_kw("TO");
        if !to && self.peek() != Some(&Tok::Op(CmpOp::Eq)) {
            return Err(DbError::InvalidQuery(format!(
                "expected `TO` or `=` after SET {name}, found {:?}",
                self.peek()
            )));
        }
        if !to {
            self.next(); // consume `=`
        }
        let mut parts: Vec<String> = Vec::new();
        loop {
            let value = match self.peek().cloned() {
                Some(Tok::Ident(word))
                    if !matches!(
                        word.to_ascii_lowercase().as_str(),
                        "true" | "false" | "null"
                    ) =>
                {
                    self.next();
                    if word.eq_ignore_ascii_case("DEFAULT") {
                        if parts.is_empty() {
                            return Ok(SqlStmt::SetDefault { name });
                        }
                        return Err(DbError::InvalidQuery(
                            "DEFAULT cannot be mixed with other SET values".into(),
                        ));
                    }
                    word
                }
                Some(Tok::Int(_)) | Some(Tok::Str(_)) | Some(Tok::Minus) | Some(Tok::Plus) => {
                    match self.parse_value()? {
                        SqlValue::Null => {
                            return Err(DbError::InvalidQuery("SET value cannot be NULL".into()))
                        }
                        SqlValue::Int(v) => v.to_string(),
                        SqlValue::Bool(b) => if b { "on" } else { "off" }.to_string(),
                        SqlValue::Text(v) => v,
                    }
                }
                Some(Tok::Ident(_)) => match self.parse_value()? {
                    SqlValue::Bool(b) => if b { "on" } else { "off" }.to_string(),
                    SqlValue::Null => {
                        return Err(DbError::InvalidQuery("SET value cannot be NULL".into()))
                    }
                    other => {
                        return Err(DbError::InvalidQuery(format!(
                            "unsupported SET value {other:?}"
                        )))
                    }
                },
                other => {
                    return Err(DbError::InvalidQuery(format!(
                        "expected a SET value, found {other:?}"
                    )))
                }
            };
            parts.push(value);
            if self.peek() == Some(&Tok::Comma) {
                self.next();
                continue;
            }
            break;
        }
        Ok(SqlStmt::Set {
            name: name.to_ascii_lowercase(),
            value: parts.join(", "),
        })
    }

    /// `RESET <name>` or `RESET ALL`.
    fn parse_reset(&mut self) -> Result<SqlStmt, DbError> {
        let name = self.expect_param_name()?;
        if name.eq_ignore_ascii_case("ALL") {
            return Ok(SqlStmt::Reset { name: None });
        }
        Ok(SqlStmt::Reset { name: Some(name) })
    }

    /// `SHOW <name>`, `SHOW ALL`, `SHOW TRANSACTION ISOLATION [LEVEL]`,
    /// `SHOW TIME ZONE` and `SHOW SESSION AUTHORIZATION`.
    fn parse_show_setting(&mut self) -> Result<SqlStmt, DbError> {
        let name = self.expect_param_name()?;
        let lower = name;
        if lower == "all" {
            return Ok(SqlStmt::ShowAll);
        }
        if lower == "transaction" {
            self.expect_kw("ISOLATION")?;
            let _ = self.eat_kw("LEVEL");
            return Ok(SqlStmt::ShowSetting {
                name: "transaction_isolation".into(),
            });
        }
        if lower == "time" {
            self.expect_kw("ZONE")?;
            return Ok(SqlStmt::ShowSetting {
                name: "timezone".into(),
            });
        }
        if lower == "session" {
            self.expect_kw("AUTHORIZATION")?;
            return Ok(SqlStmt::ShowSetting {
                name: "session_authorization".into(),
            });
        }
        Ok(SqlStmt::ShowSetting { name: lower })
    }

    fn parse_select_item(&mut self) -> Result<SelectItem, DbError> {
        if self.peek() == Some(&Tok::Star) {
            self.next();
            return Ok(SelectItem::Wildcard);
        }
        if let Some(Tok::Ident(fn_cand)) = self.peek().cloned() {
            if self.peek_next() == Some(&Tok::LParen) {
                let agg_fn = match fn_cand.to_ascii_uppercase().as_str() {
                    "COUNT" => Some(AggFunc::Count),
                    "SUM" => Some(AggFunc::Sum),
                    "MIN" => Some(AggFunc::Min),
                    "MAX" => Some(AggFunc::Max),
                    "AVG" => Some(AggFunc::Avg),
                    _ => None,
                };
                if let Some(func) = agg_fn {
                    self.next(); // function name
                    self.next(); // '('
                    let column = if self.peek() == Some(&Tok::Star) {
                        if func != AggFunc::Count {
                            return Err(DbError::InvalidQuery(format!(
                                "`*` is only valid inside COUNT(*), not {}(*)",
                                func.name()
                            )));
                        }
                        self.next();
                        None
                    } else {
                        Some(self.expect_ident()?)
                    };
                    self.expect_tok(Tok::RParen)?;
                    let alias = self.parse_optional_alias()?;
                    return Ok(SelectItem::Aggregate {
                        func,
                        column,
                        alias,
                    });
                }
            }
        }
        let name = self.expect_ident()?;
        let alias = self.parse_optional_alias()?;
        Ok(SelectItem::Column { name, alias })
    }

    fn parse_stmt(&mut self) -> Result<SqlStmt, DbError> {
        if self.eat_kw("BEGIN") {
            let _ = self.eat_kw("TRANSACTION");
            return Ok(SqlStmt::Begin);
        }
        if self.eat_kw("COMMIT") {
            let _ = self.eat_kw("TRANSACTION");
            return Ok(SqlStmt::Commit);
        }
        if self.eat_kw("ROLLBACK") {
            let _ = self.eat_kw("TRANSACTION");
            return Ok(SqlStmt::Rollback);
        }

        if self.eat_kw("SET") {
            return self.parse_set();
        }

        if self.eat_kw("RESET") {
            return self.parse_reset();
        }

        if self.eat_kw("SHOW") {
            if self.eat_kw("TABLES") {
                return Ok(SqlStmt::ShowTables);
            }
            if self.eat_kw("INDEXES") {
                if !self.eat_kw("FROM") {
                    self.expect_kw("ON")?;
                }
                let table = self.expect_ident()?;
                return Ok(SqlStmt::ShowIndexes { table });
            }
            return self.parse_show_setting();
        }

        if self.eat_kw("FLUSH") {
            let _ = self.eat_kw("TABLE");
            let table = self.expect_ident()?;
            return Ok(SqlStmt::FlushTable { table });
        }

        if self.eat_kw("COMPACT") {
            let _ = self.eat_kw("TABLE");
            let table = self.expect_ident()?;
            return Ok(SqlStmt::CompactTable { table });
        }

        if self.eat_kw("CREATE") {
            if self.eat_kw("INDEX") {
                let if_not_exists = if self.eat_kw("IF") {
                    self.expect_kw("NOT")?;
                    self.expect_kw("EXISTS")?;
                    true
                } else {
                    false
                };
                let index_name = self.expect_ident()?;
                self.expect_kw("ON")?;
                let table = self.expect_ident()?;
                self.expect_tok(Tok::LParen)?;
                let column = self.expect_ident()?;
                self.expect_tok(Tok::RParen)?;
                return Ok(SqlStmt::CreateIndex {
                    index_name,
                    if_not_exists,
                    table,
                    column,
                });
            }

            self.expect_kw("TABLE")?;
            let if_not_exists = if self.eat_kw("IF") {
                self.expect_kw("NOT")?;
                self.expect_kw("EXISTS")?;
                true
            } else {
                false
            };
            let table = self.expect_ident()?;
            self.expect_tok(Tok::LParen)?;
            let mut columns = Vec::new();
            loop {
                let name = self.expect_ident()?;
                let ty_raw = self.expect_ident()?;
                let col_type = match ty_raw.to_ascii_uppercase().as_str() {
                    "INT" | "INTEGER" | "BIGINT" | "INT64" => ColumnType::Int,
                    "TEXT" | "VARCHAR" | "STRING" => ColumnType::Text,
                    "BOOL" | "BOOLEAN" => ColumnType::Bool,
                    other => {
                        return Err(DbError::InvalidQuery(format!(
                            "unsupported column type `{other}` (supported: INT, TEXT, BOOL)"
                        )))
                    }
                };
                let mut primary_key = false;
                if self.eat_kw("PRIMARY") {
                    self.expect_kw("KEY")?;
                    primary_key = true;
                }
                columns.push(ColumnDef {
                    name,
                    col_type,
                    primary_key,
                });
                if self.peek() == Some(&Tok::Comma) {
                    self.next();
                } else {
                    break;
                }
            }
            self.expect_tok(Tok::RParen)?;
            return Ok(SqlStmt::CreateTable {
                table,
                if_not_exists,
                columns,
            });
        }

        if self.eat_kw("DROP") {
            self.expect_kw("INDEX")?;
            let if_exists = if self.eat_kw("IF") {
                self.expect_kw("EXISTS")?;
                true
            } else {
                false
            };
            let index_name = self.expect_ident()?;
            self.expect_kw("ON")?;
            let table = self.expect_ident()?;
            return Ok(SqlStmt::DropIndex {
                index_name,
                if_exists,
                table,
            });
        }

        let is_insert = self.eat_kw("INSERT");
        let is_upsert = if !is_insert {
            self.eat_kw("UPSERT")
        } else {
            false
        };
        if is_insert || is_upsert {
            self.expect_kw("INTO")?;
            let table = self.expect_ident()?;
            let columns = if self.peek() == Some(&Tok::LParen) {
                self.next();
                let mut cols = Vec::new();
                loop {
                    cols.push(self.expect_ident()?);
                    if self.peek() == Some(&Tok::Comma) {
                        self.next();
                    } else {
                        break;
                    }
                }
                self.expect_tok(Tok::RParen)?;
                Some(cols)
            } else {
                None
            };

            self.expect_kw("VALUES")?;
            let mut rows = Vec::new();
            loop {
                self.expect_tok(Tok::LParen)?;
                let mut vals = Vec::new();
                loop {
                    vals.push(self.parse_value()?);
                    if self.peek() == Some(&Tok::Comma) {
                        self.next();
                    } else {
                        break;
                    }
                }
                self.expect_tok(Tok::RParen)?;
                rows.push(vals);
                if self.peek() == Some(&Tok::Comma) {
                    self.next();
                } else {
                    break;
                }
            }
            return Ok(SqlStmt::Insert {
                table,
                upsert: is_upsert,
                columns,
                rows,
            });
        }

        if self.eat_kw("UPDATE") {
            let table = self.expect_ident()?;
            self.expect_kw("SET")?;
            let mut assignments = Vec::new();
            loop {
                let col = self.expect_ident()?;
                self.expect_tok(Tok::Op(CmpOp::Eq))?;
                let expr = self.parse_assign_expr()?;
                assignments.push((col, expr));
                if self.peek() == Some(&Tok::Comma) {
                    self.next();
                } else {
                    break;
                }
            }
            let predicates = self.parse_where()?;
            return Ok(SqlStmt::Update {
                table,
                assignments,
                predicates,
            });
        }

        if self.eat_kw("SELECT") {
            if !self.has_top_level_from() {
                return Ok(SqlStmt::SelectNoFrom {
                    items: self.parse_scalar_select_items()?,
                });
            }
            let mut items = Vec::new();
            loop {
                items.push(self.parse_select_item()?);
                if self.peek() == Some(&Tok::Comma) {
                    self.next();
                } else {
                    break;
                }
            }
            let projection = if items.len() == 1 && items[0] == SelectItem::Wildcard {
                Projection::All
            } else {
                Projection::Items(items)
            };

            self.expect_kw("FROM")?;
            let table = self.expect_ident()?;
            let predicates = self.parse_where()?;

            let mut group_by = Vec::new();
            if self.eat_kw("GROUP") {
                self.expect_kw("BY")?;
                loop {
                    group_by.push(self.expect_ident()?);
                    if self.peek() == Some(&Tok::Comma) {
                        self.next();
                    } else {
                        break;
                    }
                }
            }

            let order_by = if self.eat_kw("ORDER") {
                self.expect_kw("BY")?;
                let col = self.expect_ident()?;
                let desc = if self.eat_kw("DESC") {
                    true
                } else {
                    let _ = self.eat_kw("ASC");
                    false
                };
                Some((col, desc))
            } else {
                None
            };

            let limit = if self.eat_kw("LIMIT") {
                match self.next() {
                    Some(Tok::Int(n)) if n >= 0 => Some(n as usize),
                    other => {
                        return Err(DbError::InvalidQuery(format!(
                            "expected non-negative integer after LIMIT, found {other:?}"
                        )))
                    }
                }
            } else {
                None
            };

            return Ok(SqlStmt::Select {
                table,
                projection,
                predicates,
                group_by,
                order_by,
                limit,
            });
        }

        if self.eat_kw("DELETE") {
            self.expect_kw("FROM")?;
            let table = self.expect_ident()?;
            let predicates = self.parse_where()?;
            return Ok(SqlStmt::Delete { table, predicates });
        }

        Err(DbError::InvalidQuery(format!(
            "unsupported SQL statement starting with {:?}",
            self.peek()
        )))
    }
}

pub fn parse_sql_script(sql: &str) -> Result<Vec<SqlStmt>, DbError> {
    let toks = tokenize(sql)?;
    let mut parser = Parser { toks, pos: 0 };
    let mut stmts = Vec::new();
    while parser.peek() == Some(&Tok::Semi) {
        parser.next();
    }
    while parser.peek().is_some() {
        stmts.push(parser.parse_stmt()?);
        if parser.peek().is_some() {
            if parser.peek() == Some(&Tok::Semi) {
                while parser.peek() == Some(&Tok::Semi) {
                    parser.next();
                }
            } else {
                return Err(DbError::InvalidQuery(format!(
                    "expected `;` between SQL statements, found {:?}",
                    parser.peek()
                )));
            }
        }
    }
    if stmts.is_empty() {
        return Err(DbError::InvalidQuery("empty SQL query".into()));
    }
    Ok(stmts)
}

pub fn parse_sql(sql: &str) -> Result<SqlStmt, DbError> {
    let mut stmts = parse_sql_script(sql)?;
    if stmts.len() > 1 {
        return Err(DbError::InvalidQuery(
            "multiple SQL statements passed to single-statement parser (use parse_sql_script)"
                .into(),
        ));
    }
    Ok(stmts.remove(0))
}

/// Parses exactly one SQL literal, rejecting any trailing text.
///
/// Used where a value has to be typed without a full statement around it, such
/// as the arguments of `EXECUTE name(1, 'x')`. Anything that is not a literal
/// (an expression, a subquery, a column reference) is refused instead of being
/// passed through to the engine.
pub fn parse_sql_value(text: &str) -> Result<SqlValue, DbError> {
    let toks = tokenize(text)?;
    let mut parser = Parser { toks, pos: 0 };
    let value = parser.parse_value()?;
    if let Some(trailing) = parser.peek() {
        return Err(DbError::InvalidQuery(format!(
            "expected a single SQL literal, found {trailing:?}"
        )));
    }
    Ok(value)
}

/// Documented defaults for the configuration parameters this server knows.
///
/// These are the values `SHOW <name>` answers with before any `SET`, and they
/// match the `ParameterStatus` messages sent in the startup phase. A parameter
/// that is not in this table and was never `SET` on the session is reported as
/// unrecognised, exactly like PostgreSQL does.
pub fn setting_default(name: &str) -> Option<&'static str> {
    Some(match name.to_ascii_lowercase().as_str() {
        "server_version" => "14.0",
        "server_version_num" => "140000",
        "server_encoding" => "UTF8",
        "client_encoding" => "UTF8",
        "datestyle" => "ISO, MDY",
        "intervalstyle" => "postgres",
        "integer_datetimes" => "on",
        "standard_conforming_strings" => "on",
        "application_name" => "yadb",
        "search_path" => "public",
        "extra_float_digits" => "1",
        "statement_timeout" => "0",
        "lock_timeout" => "0",
        "idle_in_transaction_session_timeout" => "0",
        "timezone" => "UTC",
        "transaction_isolation" => "read committed",
        "default_transaction_isolation" => "read committed",
        "transaction_read_only" => "off",
        "default_transaction_read_only" => "off",
        "is_superuser" => "off",
        "row_security" => "off",
        "bytea_output" => "hex",
        "client_min_messages" => "notice",
        "max_identifier_length" => "63",
        _ => return None,
    })
}

/// Everything a `SELECT` without `FROM` may read besides its own literals.
///
/// The engine has no per-connection state, so the caller supplies the identity
/// and the session parameters. `settings` is empty for stateless callers, which
/// makes `current_setting` report a parameter as unrecognised rather than
/// inventing a value.
pub struct ScalarContext<'a> {
    pub database: &'a str,
    pub user: &'a str,
    pub settings: &'a BTreeMap<String, String>,
}

/// Evaluates `SELECT <constants and scalar functions>` (no `FROM`) to one row.
///
/// Column names follow PostgreSQL: the alias when one was written, otherwise
/// the function name, and `?column?` for a bare constant.
pub fn eval_scalar_select(
    items: &[SelectExpr],
    cx: &ScalarContext<'_>,
) -> Result<QueryResult, DbError> {
    let mut columns = Vec::with_capacity(items.len());
    let mut row = Vec::with_capacity(items.len());
    for item in items {
        columns.push(match (&item.alias, &item.expr) {
            (Some(alias), _) => alias.clone(),
            (None, ScalarExpr::Func(func)) => func.name().to_string(),
            (None, ScalarExpr::Literal(_)) => "?column?".to_string(),
        });
        row.push(match &item.expr {
            ScalarExpr::Literal(value) => value.clone(),
            ScalarExpr::Func(func) => eval_scalar_func(func, cx)?,
        });
    }
    Ok(QueryResult {
        columns,
        rows: vec![row],
        rows_affected: 0,
    })
}

fn eval_scalar_func(func: &ScalarFunc, cx: &ScalarContext<'_>) -> Result<SqlValue, DbError> {
    Ok(match func {
        ScalarFunc::Now => SqlValue::Text(timestamp_now()),
        // The startup packet already advertises server_version 14.0; the
        // leading PostgreSQL version lets drivers that parse `version()` work.
        ScalarFunc::Version => SqlValue::Text(format!(
            "PostgreSQL 14.0 (YADB {})",
            env!("CARGO_PKG_VERSION")
        )),
        ScalarFunc::CurrentDatabase => SqlValue::Text(cx.database.to_string()),
        ScalarFunc::CurrentSchema => SqlValue::Text("public".into()),
        ScalarFunc::CurrentUser => SqlValue::Text(cx.user.to_string()),
        ScalarFunc::CurrentSetting { name, missing_ok } => {
            let name = name.to_ascii_lowercase();
            let value = cx.settings.get(&name).map(String::as_str).or_else(|| {
                if name == "session_authorization" {
                    Some(cx.user)
                } else {
                    setting_default(&name)
                }
            });
            match value {
                Some(value) => SqlValue::Text(value.to_string()),
                None if *missing_ok => SqlValue::Null,
                None => return Err(DbError::UnrecognizedSetting(name)),
            }
        }
    })
}

fn timestamp_now() -> String {
    let secs = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0);
    format_timestamp(secs)
}

/// PostgreSQL's default `timestamptz` text output: `YYYY-MM-DD HH:MM:SS+00`.
///
/// The server only has whole-second resolution, so the fractional part is
/// omitted exactly as PostgreSQL omits a zero fraction.
fn format_timestamp(secs: u64) -> String {
    let days = (secs / 86_400) as i64;
    let rem = secs % 86_400;
    let (year, month, day) = civil_from_days(days);
    format!(
        "{year:04}-{month:02}-{day:02} {:02}:{:02}:{:02}+00",
        rem / 3600,
        (rem % 3600) / 60,
        rem % 60
    )
}

/// Howard Hinnant's days-to-civil algorithm, valid for any `i64` day count.
fn civil_from_days(days: i64) -> (i64, u32, u32) {
    let z = days + 719_468;
    let era = if z >= 0 { z } else { z - 146_096 } / 146_097;
    let doe = (z - era * 146_097) as u64;
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let year = yoe as i64 + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let day = (doy - (153 * mp + 2) / 5 + 1) as u32;
    let month = if mp < 10 { mp + 3 } else { mp - 9 } as u32;
    (if month <= 2 { year + 1 } else { year }, month, day)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ctx<'a>(settings: &'a BTreeMap<String, String>) -> ScalarContext<'a> {
        ScalarContext {
            database: "ledger",
            user: "sa:billing-backend",
            settings,
        }
    }

    fn one_row(sql: &str, settings: &BTreeMap<String, String>) -> QueryResult {
        let stmt = parse_sql(sql).unwrap();
        eval_scalar_select(
            match &stmt {
                SqlStmt::SelectNoFrom { items } => items,
                other => panic!("not a no-FROM select: {other:?}"),
            },
            &ctx(settings),
        )
        .unwrap()
    }

    #[test]
    fn select_without_from_evaluates_constants() {
        let settings = BTreeMap::new();
        let res = one_row("SELECT 1, 'text', true, NULL, -7", &settings);
        assert_eq!(
            res.columns,
            vec!["?column?", "?column?", "?column?", "?column?", "?column?"]
        );
        assert_eq!(
            res.rows,
            vec![vec![
                SqlValue::Int(1),
                SqlValue::Text("text".into()),
                SqlValue::Bool(true),
                SqlValue::Null,
                SqlValue::Int(-7),
            ]]
        );
        assert_eq!(res.rows_affected, 0);
    }

    #[test]
    fn select_without_from_uses_aliases_and_function_names_as_columns() {
        let settings = BTreeMap::new();
        let res = one_row("SELECT 1 AS one, version(), current_schema()", &settings);
        assert_eq!(res.columns, vec!["one", "version", "current_schema"]);
        let version = match &res.rows[0][1] {
            SqlValue::Text(v) => v.clone(),
            other => panic!("version() is not text: {other:?}"),
        };
        assert!(
            version.starts_with("PostgreSQL 14.0"),
            "drivers parse the leading version: {version}"
        );
        assert_eq!(res.rows[0][2], SqlValue::Text("public".into()));
    }

    #[test]
    fn select_without_from_answers_identity_and_time() {
        let settings = BTreeMap::new();
        let res = one_row(
            "SELECT current_database(), current_user, user, session_user, now()",
            &settings,
        );
        assert_eq!(res.rows[0][0], SqlValue::Text("ledger".into()));
        for idx in 1..=3 {
            assert_eq!(
                res.rows[0][idx],
                SqlValue::Text("sa:billing-backend".into())
            );
        }
        let SqlValue::Text(now) = &res.rows[0][4] else {
            panic!("now() is not text");
        };
        assert!(
            now.len() == 22 && now.ends_with("+00") && now.as_bytes()[10] == b' ',
            "now() must use PostgreSQL's timestamptz text form: {now}"
        );
    }

    #[test]
    fn select_without_from_reads_session_settings_then_defaults() {
        let mut settings = BTreeMap::new();
        settings.insert("client_encoding".to_string(), "LATIN1".to_string());
        let res = one_row(
            "SELECT current_setting('client_encoding'), current_setting('server_version'), current_setting('session_authorization')",
            &settings,
        );
        assert_eq!(res.rows[0][0], SqlValue::Text("LATIN1".into()));
        assert_eq!(res.rows[0][1], SqlValue::Text("14.0".into()));
        assert_eq!(res.rows[0][2], SqlValue::Text("sa:billing-backend".into()));
    }

    #[test]
    fn unknown_setting_is_refused_unless_missing_ok() {
        let settings = BTreeMap::new();
        let stmt = parse_sql("SELECT current_setting('no_such_guc')").unwrap();
        let SqlStmt::SelectNoFrom { items } = &stmt else {
            panic!("expected a no-FROM select");
        };
        let err = eval_scalar_select(items, &ctx(&settings)).unwrap_err();
        assert!(matches!(err, DbError::UnrecognizedSetting(ref name) if name == "no_such_guc"));

        let stmt = parse_sql("SELECT current_setting('no_such_guc', true)").unwrap();
        let SqlStmt::SelectNoFrom { items } = &stmt else {
            panic!("expected a no-FROM select");
        };
        assert_eq!(
            eval_scalar_select(items, &ctx(&settings)).unwrap().rows[0][0],
            SqlValue::Null
        );
    }

    #[test]
    fn pg_catalog_qualified_scalar_functions_parse() {
        let settings = BTreeMap::new();
        let res = one_row(
            "SELECT pg_catalog.version(), pg_catalog.current_schema()",
            &settings,
        );
        assert_eq!(res.columns, vec!["version", "current_schema"]);
    }

    #[test]
    fn select_with_from_is_untouched() {
        // Every statement with a top-level FROM keeps using the legacy parser,
        // including one that selects a column named like a scalar function.
        for (sql, expect_table) in [
            ("SELECT * FROM t", "t"),
            ("SELECT version FROM t WHERE id = 1", "t"),
            ("SELECT count(*) FROM t", "t"),
        ] {
            match parse_sql(sql).unwrap() {
                SqlStmt::Select { table, .. } => assert_eq!(table, expect_table),
                other => panic!("{sql} left the SELECT ... FROM path: {other:?}"),
            }
        }
        // A constant select list with a FROM is still refused, as before.
        assert!(parse_sql("SELECT 1 FROM t").is_err());
    }

    #[test]
    fn select_without_from_refuses_what_it_does_not_implement() {
        for sql in [
            "SELECT 1 + 1",
            "SELECT *",
            "SELECT 1 FROM",
            "SELECT upper('x')",
            "SELECT now",
            "SELECT current_setting(1)",
            "SELECT 1 AS",
        ] {
            assert!(
                matches!(parse_sql(sql), Err(DbError::InvalidQuery(_))),
                "{sql} was accepted"
            );
        }
    }

    #[test]
    fn set_reset_and_show_parse_into_session_statements() {
        assert_eq!(
            parse_sql("SET extra_float_digits = 3").unwrap(),
            SqlStmt::Set {
                name: "extra_float_digits".into(),
                value: "3".into()
            }
        );
        assert_eq!(
            parse_sql("SET client_encoding TO 'UTF8'").unwrap(),
            SqlStmt::Set {
                name: "client_encoding".into(),
                value: "UTF8".into()
            }
        );
        assert_eq!(
            parse_sql("SET SESSION application_name = myapp").unwrap(),
            SqlStmt::Set {
                name: "application_name".into(),
                value: "myapp".into()
            }
        );
        assert_eq!(
            parse_sql("SET search_path TO public, pg_catalog").unwrap(),
            SqlStmt::Set {
                name: "search_path".into(),
                value: "public, pg_catalog".into()
            }
        );
        assert_eq!(
            parse_sql("SET standard_conforming_strings = on").unwrap(),
            SqlStmt::Set {
                name: "standard_conforming_strings".into(),
                value: "on".into()
            }
        );
        assert_eq!(
            parse_sql("SET statement_timeout = DEFAULT").unwrap(),
            SqlStmt::SetDefault {
                name: "statement_timeout".into()
            }
        );
        assert_eq!(
            parse_sql("RESET ALL").unwrap(),
            SqlStmt::Reset { name: None }
        );
        assert_eq!(
            parse_sql("RESET client_encoding").unwrap(),
            SqlStmt::Reset {
                name: Some("client_encoding".into())
            }
        );
        assert_eq!(
            parse_sql("SHOW client_encoding").unwrap(),
            SqlStmt::ShowSetting {
                name: "client_encoding".into()
            }
        );
        assert_eq!(
            parse_sql("SHOW TRANSACTION ISOLATION LEVEL").unwrap(),
            SqlStmt::ShowSetting {
                name: "transaction_isolation".into()
            }
        );
        assert_eq!(parse_sql("SHOW ALL").unwrap(), SqlStmt::ShowAll);
        // Custom GUCs use PostgreSQL's dotted namespace form.
        assert_eq!(
            parse_sql("SET myapp.tenant = 'acme'").unwrap(),
            SqlStmt::Set {
                name: "myapp.tenant".into(),
                value: "acme".into()
            }
        );
        assert_eq!(
            parse_sql("SHOW myapp.tenant").unwrap(),
            SqlStmt::ShowSetting {
                name: "myapp.tenant".into()
            }
        );
        assert_eq!(
            parse_sql("RESET myapp.tenant").unwrap(),
            SqlStmt::Reset {
                name: Some("myapp.tenant".into())
            }
        );
        // Legacy SHOW forms are untouched.
        assert_eq!(parse_sql("SHOW TABLES").unwrap(), SqlStmt::ShowTables);
        assert_eq!(
            parse_sql("SHOW INDEXES ON t").unwrap(),
            SqlStmt::ShowIndexes { table: "t".into() }
        );
        // UPDATE ... SET is still the update statement.
        assert!(matches!(
            parse_sql("UPDATE t SET v = 1 WHERE id = 2").unwrap(),
            SqlStmt::Update { .. }
        ));
        for sql in [
            "SET TIME ZONE 'UTC'",
            "SET TRANSACTION ISOLATION LEVEL SERIALIZABLE",
        ] {
            assert!(
                matches!(parse_sql(sql), Err(DbError::InvalidQuery(_))),
                "{sql} was accepted"
            );
        }
    }

    #[test]
    fn set_reset_and_show_are_not_writes() {
        for sql in [
            "SET x = 1",
            "SET x = DEFAULT",
            "RESET x",
            "SHOW x",
            "SHOW ALL",
            "SELECT 1",
            "SELECT version()",
        ] {
            assert!(!parse_sql(sql).unwrap().is_write(), "{sql}");
        }
    }

    #[test]
    fn execute_arguments_are_parsed_as_single_literals() {
        assert_eq!(parse_sql_value("1").unwrap(), SqlValue::Int(1));
        assert_eq!(parse_sql_value("-3").unwrap(), SqlValue::Int(-3));
        assert_eq!(
            parse_sql_value("'it''s'").unwrap(),
            SqlValue::Text("it's".into())
        );
        assert_eq!(parse_sql_value("true").unwrap(), SqlValue::Bool(true));
        assert_eq!(parse_sql_value("NULL").unwrap(), SqlValue::Null);
        for bad in ["1 + 1", "a", "'x' 'y'"] {
            assert!(parse_sql_value(bad).is_err(), "{bad}");
        }
    }

    #[test]
    fn documented_defaults_are_answered_and_unknown_ones_are_not() {
        assert_eq!(setting_default("client_encoding"), Some("UTF8"));
        assert_eq!(setting_default("DateStyle"), Some("ISO, MDY"));
        assert_eq!(setting_default("no_such_guc"), None);
    }

    #[test]
    fn timestamps_are_rfc3339_utc() {
        assert_eq!(format_timestamp(0), "1970-01-01 00:00:00+00");
        assert_eq!(format_timestamp(1_700_000_000), "2023-11-14 22:13:20+00");
    }
}
