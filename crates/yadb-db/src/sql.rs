use std::cmp::Ordering;

use serde::{Deserialize, Serialize};

use crate::DbError;

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
            SqlValue::Null => Err(DbError::InvalidQuery(
                "KEY value cannot be NULL".into(),
            )),
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
            SqlStmt::ShowTables | SqlStmt::ShowIndexes { .. } | SqlStmt::Select { .. } => false,
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
                    return Ok(AssignExpr::BinOp {
                        column: s,
                        op,
                        rhs,
                    });
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
            return Err(DbError::InvalidQuery(
                "expected TABLES or INDEXES after SHOW".into(),
            ));
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
            "multiple SQL statements passed to single-statement parser (use parse_sql_script)".into(),
        ));
    }
    Ok(stmts.remove(0))
}
