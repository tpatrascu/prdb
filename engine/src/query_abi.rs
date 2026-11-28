//! Versioned C entry-point contracts for query libraries. V1 unifies
//! parameters, bounded role-bound IPC/results, sticky statement-failure
//! latching, bounded optimizer submission, and bounded spill-stream requests
//! while retaining the versioned descriptor/symbol mechanism for future ABIs.

use std::ffi::c_int;
include!("query_abi_constants.rs");

/// Generic bounded callback for one request on a role-bound IPC channel.
/// The channel determines authority; the operation remains subject to the
/// server-side role allowlist and payload decoder.
pub type QueryIpcRequestV1 = unsafe extern "C" fn(
    context: *mut std::ffi::c_void,
    operation: u16,
    request: *const u8,
    request_len: u64,
    response: *mut u8,
    response_capacity: u64,
    response_len: *mut u64,
) -> i32;

/// Bounded provisional application output callback. This never emits a
/// storage terminal decision.
pub type QueryWriteResultV1 =
    unsafe extern "C" fn(context: *mut std::ffi::c_void, bytes: *const u8, bytes_len: u64) -> i32;

/// Sticky notification that a SQL statement failed. The host records this
/// independently of the query entry-point return value.
pub type QueryStatementFailureV1 =
    unsafe extern "C" fn(context: *mut std::ffi::c_void, failure_code: u32) -> i32;

/// Bounded optimizer callback for a caller-selected operation. Callers
/// can submit SELECT or DELETE; the host still enforces the optimizer role's
/// operation allowlist and validates each request independently.
pub type QueryOptimizerRequestV1 = unsafe extern "C" fn(
    context: *mut std::ffi::c_void,
    operation: u16,
    request: *const u8,
    request_len: u64,
    response: *mut u8,
    response_capacity: u64,
    response_len: *mut u64,
) -> i32;

/// Bounded opaque spill-stream operation callback. Authority is bound to the
/// dedicated supervisor-provisioned worker channel; requests carry no path or
/// invocation, package, actor, or workload identity.
pub type QuerySpillRequestV1 = unsafe extern "C" fn(
    context: *mut std::ffi::c_void,
    operation: u16,
    request: *const u8,
    request_len: u64,
    response: *mut u8,
    response_capacity: u64,
    response_len: *mut u64,
) -> i32;

include!("query_abi_records.rs");

pub type QueryMainV1 = unsafe extern "C" fn(*const HostApiV1, *const InvocationV1) -> c_int;

/// `prdb_sdk_sql_batch_statement_v1` — one PSQ1 statement slice, borrowed
/// from the decoded `sql_data` payload.
#[repr(C)]
pub struct SqlBatchStatementV1 {
    pub sql: *const u8,
    pub sql_len: u64,
    pub parameters: *const u8,
    pub parameters_len: u64,
}

/// Callback shape required by `prdb_sdk_sql_run_batch_v1` to run one PSQ1
/// statement. Matches `prdb_sdk_sql_batch_statement_runner_v1_fn`.
pub type QuerySqlBatchStatementRunnerV1 = unsafe extern "C" fn(
    host: *const HostApiV1,
    schema: *const u8,
    schema_len: u32,
    statement: *const SqlBatchStatementV1,
) -> i32;

/// `prdb_sdk_sql_run_batch_v1`, exported with default visibility from the SQL
/// engine release library. Decodes PSQ1 `sql_data`, runs each statement
/// through the supplied runner in order, and latches on the first failure.
pub type QuerySqlRunBatchV1 = unsafe extern "C" fn(
    host: *const HostApiV1,
    invocation: *const InvocationV1,
    run_statement: QuerySqlBatchStatementRunnerV1,
) -> c_int;

/// `prdb_sdk_sql_run_statement_v1`, exported with default visibility from the
/// SQL engine release library. Runs one supported query/DML statement.
pub type QuerySqlRunStatementV1 = unsafe extern "C" fn(
    host: *const HostApiV1,
    schema: *const u8,
    schema_len: u32,
    sql: *const u8,
    sql_len: u64,
    parameters: *const u8,
    parameters_len: u64,
) -> c_int;

pub const QUERY_SQL_RUN_BATCH_SYMBOL: &str = "prdb_sdk_sql_run_batch_v1";
pub const QUERY_SQL_RUN_STATEMENT_SYMBOL: &str = "prdb_sdk_sql_run_statement_v1";

include!("query_abi_layout_assertions.rs");

/// Invocation record whose parameter and SQL data pointers are tied to the
/// borrowed slices. Keep this value alive for the complete C entry-point call.
#[must_use = "the invocation record must remain alive while the C entry point uses it"]
#[derive(Debug)]
pub struct BorrowedInvocationV1<'a> {
    record: InvocationV1,
    _parameters: std::marker::PhantomData<&'a [u8]>,
    _sql_data: std::marker::PhantomData<&'a [u8]>,
}

impl<'a> BorrowedInvocationV1<'a> {
    pub fn new(parameters: &'a [u8], sql_data: &'a [u8]) -> Result<Self, ParameterError> {
        let parameters_len =
            u64::try_from(parameters.len()).map_err(|_| ParameterError::TooLarge)?;
        if parameters_len > MAX_PARAMETER_BYTES {
            return Err(ParameterError::TooLarge);
        }
        let sql_data_len = u64::try_from(sql_data.len()).map_err(|_| ParameterError::TooLarge)?;
        if sql_data_len > MAX_SQL_DATA_BYTES {
            return Err(ParameterError::TooLarge);
        }
        let parameters_pointer = if parameters.is_empty() {
            std::ptr::null()
        } else {
            parameters.as_ptr()
        };
        let sql_data_pointer = if sql_data.is_empty() {
            std::ptr::null()
        } else {
            sql_data.as_ptr()
        };
        Ok(Self {
            record: InvocationV1 {
                struct_size: std::mem::size_of::<InvocationV1>() as u32,
                abi_version: QUERY_ABI_VERSION,
                parameters: parameters_pointer,
                parameters_len,
                sql_data: sql_data_pointer,
                sql_data_len,
            },
            _parameters: std::marker::PhantomData,
            _sql_data: std::marker::PhantomData,
        })
    }

    pub fn as_ptr(&self) -> *const InvocationV1 {
        &self.record
    }

    pub fn as_record(&self) -> &InvocationV1 {
        &self.record
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ParameterError {
    TooLarge,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DescriptorError {
    StructSize,
    AbiVersion,
    HostApiSize,
    InvocationSize,
}

/// Check the descriptor exported by a query library against this host's v1
/// layouts before looking up or calling the entry point.
pub fn validate_descriptor_v1(descriptor: &QueryDescriptorV1) -> Result<(), DescriptorError> {
    if descriptor.struct_size != std::mem::size_of::<QueryDescriptorV1>() as u32 {
        return Err(DescriptorError::StructSize);
    }
    if descriptor.abi_version != QUERY_ABI_VERSION {
        return Err(DescriptorError::AbiVersion);
    }
    if descriptor.host_api_size != std::mem::size_of::<HostApiV1>() as u32 {
        return Err(DescriptorError::HostApiSize);
    }
    if descriptor.invocation_size != std::mem::size_of::<InvocationV1>() as u32 {
        return Err(DescriptorError::InvocationSize);
    }
    Ok(())
}

#[cfg(test)]
mod generated_payload_tests {
    use super::{
        decode_supervisor_admin_cancel_request, decode_supervisor_cancel_result_simple,
        decode_supervisor_cancel_result_terminal, decode_supervisor_invocation_control_request,
        decode_supervisor_status_terminal_response, encode_supervisor_admin_cancel_request,
        encode_supervisor_cancel_result_simple, encode_supervisor_cancel_result_terminal,
        encode_supervisor_invocation_control_request, encode_supervisor_status_terminal_response,
    };

    #[test]
    fn generated_planner_population_codec_matches_wire_example() {
        let values = [
            [0x0102_0304_0506_0708, 9, 10],
            [11, 12, 0xfedc_ba98_7654_3210],
        ];
        let encoded = super::encode_planner_estimate_scan_response_column_populations(&values)
            .expect("two population triples fit");
        let expected = [
            1, 2, 3, 4, 5, 6, 7, 8, 0, 0, 0, 0, 0, 0, 0, 9, 0, 0, 0, 0, 0, 0, 0, 10, 0, 0, 0, 0, 0,
            0, 0, 11, 0, 0, 0, 0, 0, 0, 0, 12, 0xfe, 0xdc, 0xba, 0x98, 0x76, 0x54, 0x32, 0x10,
        ];
        assert_eq!(encoded, expected);
        assert_eq!(
            super::decode_planner_estimate_scan_response_column_populations(&encoded, 2),
            Some(values.to_vec())
        );
        assert_eq!(
            super::decode_planner_estimate_scan_response_column_populations(&encoded[..47], 2),
            None
        );
        assert_eq!(
            super::decode_planner_estimate_scan_response_column_populations(&encoded, 1),
            None
        );
    }

    #[test]
    fn generated_planner_column_id_codec_matches_wire_example() {
        let values = [[0x00; 16], [0xff; 16]];
        let encoded = super::encode_planner_estimate_scan_request_column_ids(&values)
            .expect("two column IDs fit");
        assert_eq!(encoded, [vec![0x00; 16], vec![0xff; 16]].concat());
        assert_eq!(
            super::decode_planner_estimate_scan_request_column_ids(&encoded, 2),
            Some(values.to_vec())
        );
        assert_eq!(
            super::decode_planner_estimate_scan_request_column_ids(&encoded[..31], 2),
            None
        );
        assert_eq!(
            super::decode_planner_estimate_scan_request_column_ids(&encoded, 1),
            None
        );
    }

    #[test]
    fn generated_supervisor_control_codec_matches_wire_example() {
        let encoded = encode_supervisor_invocation_control_request([0x5a; 16]);
        assert_eq!(
            encoded,
            [
                0x00, 0x01, 0x5a, 0x5a, 0x5a, 0x5a, 0x5a, 0x5a, 0x5a, 0x5a, 0x5a, 0x5a, 0x5a, 0x5a,
                0x5a, 0x5a, 0x5a, 0x5a
            ]
        );
        assert_eq!(
            decode_supervisor_invocation_control_request(&encoded),
            Some([0x5a; 16])
        );

        let mut wrong_version = encoded;
        wrong_version[1] = 2;
        assert_eq!(
            decode_supervisor_invocation_control_request(&wrong_version),
            None
        );
        assert_eq!(
            decode_supervisor_invocation_control_request(&encoded[..encoded.len() - 1]),
            None
        );
    }

    #[test]
    fn generated_admin_cancel_codec_matches_wire_example() {
        let encoded = encode_supervisor_admin_cancel_request([0x44; 16], 0x1234);
        assert_eq!(
            encoded,
            [
                0x00, 0x01, 0x44, 0x44, 0x44, 0x44, 0x44, 0x44, 0x44, 0x44, 0x44, 0x44, 0x44, 0x44,
                0x44, 0x44, 0x44, 0x44, 0x12, 0x34
            ]
        );
        assert_eq!(
            decode_supervisor_admin_cancel_request(&encoded),
            Some(([0x44; 16], 0x1234))
        );
        assert_eq!(
            decode_supervisor_admin_cancel_request(&encoded[..encoded.len() - 1]),
            None
        );
    }

    #[test]
    fn generated_simple_cancel_result_codec_matches_wire_example() {
        let encoded = encode_supervisor_cancel_result_simple(3);
        assert_eq!(encoded, [0x00, 0x01, 0x03]);
        assert_eq!(decode_supervisor_cancel_result_simple(&encoded), Some(3));
        let mut invalid_status = encoded;
        invalid_status[2] = 0xff;
        assert_eq!(
            decode_supervisor_cancel_result_simple(&invalid_status),
            Some(0xff)
        );
        assert_eq!(decode_supervisor_cancel_result_simple(&encoded[..2]), None);
    }

    #[test]
    fn generated_terminal_cancel_result_uses_signed_retention_time() {
        let encoded = encode_supervisor_cancel_result_terminal(2, 1, 7, -2);
        assert_eq!(
            encoded,
            [
                0x00, 0x01, 0x02, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x07, 0xff, 0xff,
                0xff, 0xff, 0xff, 0xff, 0xff, 0xfe
            ]
        );
        assert_eq!(
            decode_supervisor_cancel_result_terminal(&encoded),
            Some((2, 1, 7, -2))
        );
    }

    #[test]
    fn generated_terminal_status_codec_preserves_signed_times() {
        let encoded = encode_supervisor_status_terminal_response(4, -1, 7, -2);
        assert_eq!(
            encoded,
            [
                0x00, 0x01, 0x04, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0x00, 0x00, 0x00,
                0x00, 0x00, 0x00, 0x00, 0x07, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xfe
            ]
        );
        assert_eq!(
            decode_supervisor_status_terminal_response(&encoded),
            Some((4, -1, 7, -2))
        );
    }
}

#[cfg(test)]
mod spill_abi_tests {
    use super::*;

    #[test]
    fn unified_host_table_validates_v1_descriptor() {
        let mut descriptor = QueryDescriptorV1 {
            struct_size: std::mem::size_of::<QueryDescriptorV1>() as u32,
            abi_version: QUERY_ABI_VERSION,
            host_api_size: std::mem::size_of::<HostApiV1>() as u32,
            invocation_size: std::mem::size_of::<InvocationV1>() as u32,
        };
        assert_eq!(validate_descriptor_v1(&descriptor), Ok(()));

        descriptor.host_api_size -= 1;
        assert_eq!(
            validate_descriptor_v1(&descriptor),
            Err(DescriptorError::HostApiSize)
        );
        descriptor.host_api_size = std::mem::size_of::<HostApiV1>() as u32;
        descriptor.abi_version = QUERY_ABI_VERSION + 1;
        assert_eq!(
            validate_descriptor_v1(&descriptor),
            Err(DescriptorError::AbiVersion)
        );
    }
}
