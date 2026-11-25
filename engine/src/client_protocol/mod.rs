//! Bounded framing primitives for client network protocol version 1.0.

mod admin_decode;
#[cfg(test)]
mod client_v2_frame_tests;
mod control_decode;
mod frame;
mod json;
#[cfg(test)]
mod runtime_wait_tests;
mod session;
mod session_admin_prepare;
mod session_admin_read;
mod session_admin_start;
mod types;

pub use admin_decode::{decode_client_admin_command, decode_client_admin_read};
pub use control_decode::ClientStartActorCredentials;
pub use control_decode::{
    decode_client_chunk, decode_client_credentials, decode_client_hello, decode_client_prepare,
    decode_client_prepare_for_protocol, decode_client_seal, decode_client_start_for_protocol,
};
pub use frame::{
    ClientFrameDecoder, ClientRequestIds, ClientResponseSequences, ClientV2PackageState,
    ClientV2Request, ClientV2Response, ClientV2TransactionState,
};
pub(crate) use json::{JsonStringObjectParser, decode_table_scalar_type};
pub use session::ClientConnectionSession;
pub use types::*;

pub const CLIENT_FRAME_HEADER_BYTES: usize =
    crate::query_abi::CLIENT_FRAME_HEADER_RECORD_HEADER_BYTES as usize;
pub const CLIENT_MAX_PAYLOAD_BYTES: usize = 1024 * 1024;
pub const CLIENT_MAX_CONTROL_BYTES: usize = 64 * 1024;
pub const CLIENT_MAX_CHUNK_BYTES: usize = 256 * 1024;
pub const CLIENT_ERROR_KIND: u16 = 101;
pub const CLIENT_V2_TX_BEGIN_KIND: u16 = 13;
pub const CLIENT_V2_TX_ATTACH_KIND: u16 = 14;
pub const CLIENT_V2_TX_COMMIT_KIND: u16 = 15;
pub const CLIENT_V2_TX_ROLLBACK_KIND: u16 = 16;
pub const CLIENT_V2_TX_STATUS_KIND: u16 = 17;
pub const CLIENT_V2_PACKAGE_STATUS_KIND: u16 = 18;
pub const CLIENT_SDK_LIBRARY_CHUNK_KIND: u16 = 19;
pub(super) const CLIENT_MAGIC: [u8; 4] = *b"PDBN";
pub(super) const CLIENT_METADATA_CURSOR_KIND: u8 = 0xd3;
pub const CLIENT_METADATA_MAX_PAGE: usize = 128;
