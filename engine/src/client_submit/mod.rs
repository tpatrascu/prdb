//! Authenticated TCP upload client for prepared query packages.

mod admin_ddl;
mod admin_discovery;
mod admin_identity;
mod admin_sql;
mod codec;
mod metadata;
mod response;
mod submission;
#[cfg(test)]
mod tests;
mod types;

#[cfg(feature = "protocol-fuzzing")]
#[doc(hidden)]
pub use response::fuzz_decode_bounded_response_json;
pub(crate) use response::{ResponseValue, parse_bounded_json_value};
pub use submission::{read_package, write_start_receipt};
pub use types::*;

pub(super) const MAX_PACKAGE_ARCHIVE_BYTES: u64 = crate::supervisor::MAX_ARTIFACT_BYTES;
pub(super) const CLIENT_NAME: &str = "prdb-rust-sdk";
pub(super) const DEFAULT_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(30);
