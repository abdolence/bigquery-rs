//! The SQL front end: BigQuery SQL text to a DataFusion plan over tables pinned at one snapshot.
//!
//! ```text
//! text ─► sqlparser (BigQuery dialect) ─► classify ─► AST pass ─► DataFusion plan
//!                                                        │
//!                                                        ├─ table names → `project`.`dataset`.`table`
//!                                                        ├─ `@name` and `?` → placeholders
//!                                                        └─ unnamed select items → `f<k>_`
//! ```
//!
//! Exactly one statement is planned per request. The AST pass rewrites every table name to
//! DataFusion's three-part name, project as catalog and dataset as schema, and collects the
//! tables, so that the caller can load each at one snapshot and register it before planning.
//! Parameters become placeholders, so no parameter text ever enters the SQL text; their values
//! are bound on the plan as typed scalars. Every select item that BigQuery would leave anonymous
//! gets its `f<k>_` alias here, since DataFusion names an expression by its display text.

mod params;
mod tables;

pub(crate) use params::Parameters;

use crate::config::ByteSize;
use crate::error::EngineError;
use crate::ids::{DatasetId, DatasetRef, ProjectId, TableId, TableRef};
use datafusion::common::DFSchema;
use datafusion::execution::disk_manager::{DiskManagerBuilder, DiskManagerMode};
use datafusion::execution::memory_pool::FairSpillPool;
use datafusion::execution::runtime_env::{RuntimeEnv, RuntimeEnvBuilder};
use datafusion::logical_expr::{Expr, LogicalPlan};
use datafusion::prelude::{SessionConfig, SessionContext};
use datafusion::sql::parser::{DFParserBuilder, Statement as ParsedStatement};
use datafusion::sql::sqlparser::ast::{
    self, Ident, ObjectName, ObjectNamePart, Query, SelectItem, SetExpr, TableFactor, Value,
    VisitMut, VisitorMut,
};
use datafusion::sql::sqlparser::dialect::BigQueryDialect;
use std::collections::BTreeSet;
use std::ops::ControlFlow;
use std::path::Path;
use std::str::FromStr;
use std::sync::Arc;

/// The DataFusion configuration and the memory pool every statement shares.
#[derive(Debug)]
pub(crate) struct SqlEngine {
    runtime: Arc<RuntimeEnv>,
    config: SessionConfig,
}

impl SqlEngine {
    /// An engine whose statements share `memory_budget`, spilling to `spill_dir`, or to the
    /// system's temporary directory when it is `None`.
    pub(crate) fn new(
        memory_budget: ByteSize,
        spill_dir: Option<&Path>,
    ) -> Result<Self, EngineError> {
        let budget = usize::try_from(memory_budget.bytes()).unwrap_or(usize::MAX);
        let disk = match spill_dir {
            Some(dir) => DiskManagerBuilder::default()
                .with_mode(DiskManagerMode::Directories(vec![dir.to_path_buf()])),
            None => DiskManagerBuilder::default().with_mode(DiskManagerMode::OsTmpDirectory),
        };
        let runtime = RuntimeEnvBuilder::new()
            .with_memory_pool(Arc::new(FairSpillPool::new(budget)))
            .with_disk_manager_builder(disk)
            .build_arc()?;
        let mut config = SessionConfig::new().with_information_schema(false);
        let parser = &mut config.options_mut().sql_parser;
        parser.dialect = datafusion::config::Dialect::BigQuery;
        // Identifiers keep their case, as BigQuery's dataset, table and column names do.
        parser.enable_ident_normalization = false;
        // BigQuery sorts NULL first in ascending order.
        parser.default_null_ordering = "nulls_min".to_string();
        parser.map_string_types_to_utf8view = false;
        Ok(Self { runtime, config })
    }

    /// A fresh session for one statement, sharing the memory pool.
    pub(crate) fn session(&self) -> SessionContext {
        SessionContext::new_with_config_rt(self.config.clone(), self.runtime.clone())
    }
}

/// What a statement is, as BigQuery's `statement_type` names it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum StatementKind {
    Select,
}

impl StatementKind {
    pub(crate) fn name(self) -> &'static str {
        match self {
            StatementKind::Select => "SELECT",
        }
    }
}

/// One statement of BigQuery SQL, parsed and classified.
#[derive(Debug)]
pub(crate) struct Statement {
    parsed: Box<ast::Statement>,
    kind: StatementKind,
}

/// Parses exactly one statement and classifies it.
///
/// # Errors
/// [`EngineError::InvalidQuery`] for text that does not parse, and [`EngineError::Unsupported`]
/// naming a script of several statements or a statement the engine does not run.
impl FromStr for Statement {
    type Err = EngineError;

    fn from_str(sql: &str) -> Result<Self, Self::Err> {
        let mut statements = DFParserBuilder::new(sql)
            .with_dialect(&BigQueryDialect {})
            .build()?
            .parse_statements()?;
        if statements.len() > 1 {
            return Err(EngineError::unsupported(
                "SCRIPT",
                "a request runs exactly one statement",
            ));
        }
        let parsed = statements
            .pop_front()
            .ok_or_else(|| EngineError::InvalidQuery {
                detail: "the request holds no statement".to_string(),
            })?;
        let (parsed, kind) = match parsed {
            ParsedStatement::Statement(statement) => match statement.as_ref() {
                ast::Statement::Query(_) => (statement, StatementKind::Select),
                other => {
                    return Err(EngineError::unsupported(
                        statement_name(other),
                        "this release runs SELECT statements",
                    ))
                }
            },
            ParsedStatement::Explain(_) => {
                return Err(EngineError::unsupported(
                    "EXPLAIN",
                    "not a BigQuery statement",
                ))
            }
            ParsedStatement::CopyTo(_) => {
                return Err(EngineError::unsupported("COPY", "not a BigQuery statement"))
            }
            ParsedStatement::CreateExternalTable(_) => {
                return Err(EngineError::unsupported(
                    "CREATE EXTERNAL TABLE",
                    "this release runs SELECT statements",
                ))
            }
            ParsedStatement::Reset(_) => {
                return Err(EngineError::unsupported(
                    "RESET",
                    "not a BigQuery statement",
                ))
            }
        };
        Ok(Self { parsed, kind })
    }
}

/// The statement's name as an `Unimplemented` answer names it: its leading keywords.
fn statement_name(statement: &ast::Statement) -> String {
    let text = statement.to_string();
    let mut words = text.split_whitespace();
    let first = words.next().unwrap_or_default().to_ascii_uppercase();
    match first.as_str() {
        "CREATE" | "DROP" | "ALTER" | "TRUNCATE" => {
            let mut name = first;
            for word in words.take_while(|word| {
                [
                    "OR",
                    "REPLACE",
                    "TABLE",
                    "SCHEMA",
                    "VIEW",
                    "MATERIALIZED",
                    "FUNCTION",
                    "PROCEDURE",
                    "TEMP",
                    "TEMPORARY",
                    "EXTERNAL",
                    "SNAPSHOT",
                    "SEARCH",
                    "INDEX",
                    "MODEL",
                ]
                .contains(&word.to_ascii_uppercase().as_str())
            }) {
                name.push(' ');
                name.push_str(&word.to_ascii_uppercase());
            }
            name
        }
        _ => first,
    }
}

/// Where unqualified table names resolve: `dataset.table` in the request's project, `table` in
/// the default dataset.
#[derive(Debug, Clone)]
pub(crate) struct NameScope {
    pub project: ProjectId,
    pub default_dataset: Option<DatasetRef>,
}

/// How a statement's parameters are named.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub(crate) enum ParameterStyle {
    /// The statement takes none.
    #[default]
    None,
    /// `@name`.
    Named,
    /// `?`, numbered in source order.
    Positional,
}

/// A statement after the AST pass, ready to plan once its tables are registered.
#[derive(Debug)]
pub(crate) struct Prepared {
    parsed: Box<ast::Statement>,
    kind: StatementKind,
    tables: Vec<TableRef>,
    parameters: ParameterStyle,
}

impl Statement {
    /// Runs the AST pass.
    ///
    /// # Errors
    /// [`EngineError::Invalid`] for a table name that names no table of BigQuery's form, a
    /// statement that mixes `@name` and `?`, and [`EngineError::Unsupported`] for the name forms
    /// the engine refuses.
    pub(crate) fn prepare(mut self, scope: &NameScope) -> Result<Prepared, EngineError> {
        let mut ctes = CteNames::default();
        let _ = self.parsed.visit(&mut ctes);
        let mut pass = AstPass {
            scope: Some(scope),
            ctes: ctes.names,
            tables: Vec::new(),
            parameters: ParameterStyle::None,
            positional: 0,
        };
        if let ControlFlow::Break(err) = self.parsed.visit(&mut pass) {
            return Err(err);
        }
        Ok(Prepared {
            parameters: pass.parameters,
            tables: pass.tables,
            parsed: self.parsed,
            kind: self.kind,
        })
    }
}

impl Prepared {
    pub(crate) fn kind(&self) -> StatementKind {
        self.kind
    }

    /// The tables the statement names, each once, in order of first mention.
    pub(crate) fn tables(&self) -> &[TableRef] {
        &self.tables
    }

    /// The logical plan over the tables `session` has registered, with `parameters` bound.
    ///
    /// # Errors
    /// [`EngineError::InvalidQuery`] for a statement that does not plan, such as an unknown
    /// column, and for a placeholder without a parameter.
    pub(crate) async fn plan(
        self,
        session: &SessionContext,
        parameters: &Parameters,
    ) -> Result<LogicalPlan, EngineError> {
        let plan = session
            .state()
            .statement_to_plan(ParsedStatement::Statement(self.parsed))
            .await?;
        Ok(parameters.bind(plan, self.parameters)?)
    }
}

/// Collects the names of every CTE a statement declares, which a one-part table name may name
/// instead of a table.
#[derive(Default)]
struct CteNames {
    names: BTreeSet<String>,
}

impl VisitorMut for CteNames {
    type Break = ();

    fn pre_visit_query(&mut self, query: &mut Query) -> ControlFlow<Self::Break> {
        if let Some(with) = &query.with {
            for cte in &with.cte_tables {
                self.names.insert(cte.alias.name.value.clone());
            }
        }
        ControlFlow::Continue(())
    }
}

/// The AST pass: table names, parameters and anonymous select items. Without a scope, as for a
/// row restriction, no table name is allowed.
struct AstPass<'s> {
    scope: Option<&'s NameScope>,
    ctes: BTreeSet<String>,
    tables: Vec<TableRef>,
    parameters: ParameterStyle,
    positional: usize,
}

impl VisitorMut for AstPass<'_> {
    type Break = EngineError;

    fn pre_visit_query(&mut self, query: &mut Query) -> ControlFlow<Self::Break> {
        name_select_items(&mut query.body);
        ControlFlow::Continue(())
    }

    fn pre_visit_table_factor(&mut self, factor: &mut TableFactor) -> ControlFlow<Self::Break> {
        let TableFactor::Table {
            name,
            args: None,
            version,
            ..
        } = factor
        else {
            return ControlFlow::Continue(());
        };
        if version.is_some() {
            return ControlFlow::Break(EngineError::unsupported(
                "FOR SYSTEM_TIME AS OF",
                "time travel in SQL is not served by this release",
            ));
        }
        match self.table(name) {
            Ok(Some(table)) => {
                *name = ObjectName::from(vec![
                    Ident::with_quote('`', table.project.to_string()),
                    Ident::with_quote('`', table.dataset.to_string()),
                    Ident::with_quote('`', table.table.to_string()),
                ]);
                if !self.tables.contains(&table) {
                    self.tables.push(table);
                }
                ControlFlow::Continue(())
            }
            Ok(None) => ControlFlow::Continue(()),
            Err(err) => ControlFlow::Break(err),
        }
    }

    fn pre_visit_expr(&mut self, expr: &mut ast::Expr) -> ControlFlow<Self::Break> {
        // sqlparser's BigQuery dialect reads `@name` as an identifier, which DataFusion would
        // plan as a session variable.
        if let ast::Expr::Identifier(ident) = expr {
            if is_named_parameter(ident) {
                if let Err(err) = self.uses(ParameterStyle::Named) {
                    return ControlFlow::Break(err);
                }
                *expr = ast::Expr::value(Value::Placeholder(ident.value.clone()));
            }
        }
        ControlFlow::Continue(())
    }

    fn pre_visit_value(&mut self, value: &mut Value) -> ControlFlow<Self::Break> {
        // DataFusion looks positional placeholders up by the number after their first
        // character, which `?` does not have.
        if let Value::Placeholder(placeholder) = value {
            if placeholder == "?" {
                if let Err(err) = self.uses(ParameterStyle::Positional) {
                    return ControlFlow::Break(err);
                }
                self.positional += 1;
                *placeholder = format!("${}", self.positional);
            }
        }
        ControlFlow::Continue(())
    }
}

impl AstPass<'_> {
    /// Records that the statement uses `style`, refusing a mix of the two.
    fn uses(&mut self, style: ParameterStyle) -> Result<(), EngineError> {
        match self.parameters {
            ParameterStyle::None => {
                self.parameters = style;
                Ok(())
            }
            current if current == style => Ok(()),
            _ => Err(EngineError::invalid(
                "query parameters",
                "a statement uses either named (@name) or positional (?) parameters, not both",
            )),
        }
    }

    /// The table a relation name names, or `None` for the name of a CTE: `project.dataset.table`,
    /// `dataset.table` in the request's project, `table` in the default dataset, each part
    /// backticked or the whole name in one pair of backticks.
    fn table(&self, name: &ObjectName) -> Result<Option<TableRef>, EngineError> {
        let mut parts = Vec::new();
        for part in &name.0 {
            let ObjectNamePart::Identifier(ident) = part else {
                return Err(EngineError::invalid(
                    format!("table name {name}"),
                    "a table name is made of identifiers",
                ));
            };
            parts.push(ident.value.clone());
        }
        if let [single] = parts.as_slice() {
            if self.ctes.contains(single) {
                return Ok(None);
            }
        }
        if parts
            .iter()
            .any(|part| part.eq_ignore_ascii_case("INFORMATION_SCHEMA"))
        {
            return Err(EngineError::unsupported(
                "INFORMATION_SCHEMA",
                format!("named by {name}"),
            ));
        }
        if parts.last().is_some_and(|part| part.ends_with('*')) {
            return Err(EngineError::unsupported(
                "wildcard tables",
                format!("named by {name}"),
            ));
        }
        let invalid = |detail: &str| EngineError::invalid(format!("table name {name}"), detail);
        let scope = self
            .scope
            .ok_or_else(|| invalid("a row restriction names no table"))?;
        let (project, dataset, table) = match parts.as_slice() {
            [table] => {
                let dataset = scope
                    .default_dataset
                    .clone()
                    .ok_or_else(|| invalid("an unqualified table name needs a default dataset"))?;
                return Ok(Some(dataset.table(TableId::from_str(table)?)));
            }
            [dataset, table] => (scope.project.clone(), dataset, table),
            // A domain-scoped project, as in `example.com:project`, holds dots of its own.
            [project @ .., dataset, table] if project.iter().any(|part| part.contains(':')) => {
                (ProjectId::from_str(&project.join("."))?, dataset, table)
            }
            [project, dataset, table] => (ProjectId::from_str(project)?, dataset, table),
            _ => return Err(invalid("a table name has at most three parts")),
        };
        Ok(Some(
            DatasetRef {
                project,
                dataset: DatasetId::from_str(dataset)?,
            }
            .table(TableId::from_str(table)?),
        ))
    }
}

/// Whether `ident` is a named parameter, `@name`, which sqlparser's BigQuery dialect reads as an
/// identifier; `@@name` is a system variable.
fn is_named_parameter(ident: &Ident) -> bool {
    ident.quote_style.is_none() && ident.value.starts_with('@') && !ident.value.starts_with("@@")
}

/// Gives every select item that BigQuery leaves anonymous its `f<k>_` alias, counting such items
/// from 0 in select-list order; a column reference keeps the column's name and a field access
/// the field's. In a set operation the leftmost select names the columns, but every select is
/// named, so that each nested query's columns have their BigQuery names too.
fn name_select_items(body: &mut SetExpr) {
    match body {
        SetExpr::Select(select) => {
            let mut anonymous = 0usize;
            for item in &mut select.projection {
                let SelectItem::UnnamedExpr(expr) = item else {
                    continue;
                };
                let alias = match expr {
                    ast::Expr::Identifier(ident) if !is_named_parameter(ident) => continue,
                    ast::Expr::CompoundIdentifier(parts) => match parts.last() {
                        Some(last) => Ident::new(last.value.clone()),
                        None => continue,
                    },
                    ast::Expr::CompoundFieldAccess { access_chain, .. } => {
                        match access_chain.last() {
                            Some(ast::AccessExpr::Dot(ast::Expr::Identifier(field))) => {
                                Ident::new(field.value.clone())
                            }
                            _ => {
                                let alias = Ident::new(format!("f{anonymous}_"));
                                anonymous += 1;
                                alias
                            }
                        }
                    }
                    _ => {
                        let alias = Ident::new(format!("f{anonymous}_"));
                        anonymous += 1;
                        alias
                    }
                };
                let expr = std::mem::replace(expr, ast::Expr::value(Value::Null));
                *item = SelectItem::ExprWithAlias { expr, alias };
            }
        }
        SetExpr::SetOperation { left, right, .. } => {
            name_select_items(left);
            name_select_items(right);
        }
        SetExpr::Query(query) => name_select_items(&mut query.body),
        _ => {}
    }
}

/// A Storage Read `row_restriction`: a BigQuery boolean expression over `schema`'s columns.
///
/// # Errors
/// [`EngineError::Invalid`] whose message contains `selected fields do not exist`, the text the
/// client matches, for a column the table does not have, and [`EngineError::InvalidQuery`] for
/// an expression that does not parse or plan.
pub(crate) fn restriction(
    session: &SessionContext,
    text: &str,
    schema: &DFSchema,
) -> Result<Expr, EngineError> {
    let state = session.state();
    let mut parsed = state.sql_to_expr_with_alias(text, &datafusion::config::Dialect::BigQuery)?;
    let mut pass = AstPass {
        scope: None,
        ctes: BTreeSet::new(),
        tables: Vec::new(),
        parameters: ParameterStyle::None,
        positional: 0,
    };
    if let ControlFlow::Break(err) = parsed.expr.visit(&mut pass) {
        return Err(err);
    }
    if pass.parameters != ParameterStyle::None {
        return Err(EngineError::invalid(
            "row_restriction",
            "a row restriction takes no parameters",
        ));
    }
    state
        .create_logical_expr_from_sql_expr(parsed, schema)
        .map_err(|err| match err.find_root() {
            datafusion::error::DataFusionError::SchemaError(schema_error, _) => {
                match schema_error.as_ref() {
                    datafusion::common::SchemaError::FieldNotFound { field, .. } => {
                        EngineError::invalid(
                            "row_restriction",
                            format!("selected fields do not exist: {}", field.name),
                        )
                    }
                    _ => EngineError::from(err),
                }
            }
            _ => EngineError::from(err),
        })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn scope() -> NameScope {
        NameScope {
            project: "lite-project".parse().expect("a valid project"),
            default_dataset: Some(DatasetRef {
                project: "lite-project".parse().expect("a valid project"),
                dataset: "shop".parse().expect("a valid dataset"),
            }),
        }
    }

    fn table(project: &str, dataset: &str, table: &str) -> TableRef {
        TableRef::parse(project, dataset, table).expect("a valid table")
    }

    #[test]
    fn every_table_name_form_resolves_to_one_table() {
        let orders = table("lite-project", "shop", "orders");
        for sql in [
            "SELECT * FROM orders",
            "SELECT * FROM shop.orders",
            "SELECT * FROM `lite-project.shop.orders`",
            "SELECT * FROM `lite-project`.shop.orders",
            "SELECT * FROM shop.orders o JOIN orders p ON o.id = p.id",
        ] {
            let prepared = Statement::from_str(sql)
                .and_then(|statement| statement.prepare(&scope()))
                .expect(sql);
            assert_eq!(prepared.tables(), std::slice::from_ref(&orders), "{sql}");
        }
        let domain = Statement::from_str("SELECT * FROM `example.com:lite-project.shop.orders`")
            .and_then(|statement| statement.prepare(&scope()))
            .expect("a domain-scoped project");
        assert_eq!(
            domain.tables(),
            [table("example.com:lite-project", "shop", "orders")]
        );
    }

    #[test]
    fn a_cte_name_is_not_a_table() {
        let prepared =
            Statement::from_str("WITH recent AS (SELECT * FROM orders) SELECT * FROM recent")
                .and_then(|statement| statement.prepare(&scope()))
                .expect("a CTE");
        assert_eq!(prepared.tables(), [table("lite-project", "shop", "orders")]);
    }

    #[test]
    fn a_script_and_dml_are_refused_by_name() {
        for (sql, construct) in [
            ("SELECT 1; SELECT 2", "SCRIPT"),
            ("UPDATE orders SET a = 1 WHERE true", "UPDATE"),
            ("DELETE FROM orders WHERE true", "DELETE"),
            ("CREATE TABLE shop.t (a INT64)", "CREATE TABLE"),
        ] {
            let refused = Statement::from_str(sql);
            assert!(
                matches!(&refused, Err(EngineError::Unsupported { construct: c, .. }) if c == construct),
                "{sql}: {refused:?}"
            );
        }
    }

    #[test]
    fn named_and_positional_parameters_do_not_mix() {
        let mixed = Statement::from_str("SELECT * FROM orders WHERE a = @a AND b = ?")
            .and_then(|statement| statement.prepare(&scope()));
        assert!(
            matches!(mixed, Err(EngineError::Invalid { .. })),
            "{mixed:?}"
        );
    }
}
