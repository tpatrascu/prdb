//! Bounded SQL DDL accepted by the authenticated administrative client.
//!
//! This parser only translates a deliberately small CREATE TABLE grammar to
//! the existing typed control operation. Authorization and catalog validation
//! remain server-side.

mod ast;
mod binding;
mod lexer;
mod parser;
mod statements_identity_policy;
mod statements_schema_table;
#[cfg(test)]
mod tests_ddl;
#[cfg(test)]
mod tests_expressions;

pub use ast::*;
pub use binding::{bind_check_expression, bind_default_literal, bind_policy_expression};
pub use statements_identity_policy::*;
pub use statements_schema_table::*;

pub(super) const MAX_DDL_BYTES: usize = 65_536;
pub(super) const MAX_CHECK_IN_VALUES: usize = 64;
