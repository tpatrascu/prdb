//! Versioned C entry-point contracts for query libraries. V1 carries only
//! parameters; v2 adds bounded role-bound IPC/results, and v3 adds a sticky
//! statement-failure callback, and v4 adds bounded optimizer submission.

use std::ffi::c_int;
include!("query_abi_constants.rs");

/// Generic bounded callback for one request on a role-bound IPC channel.
/// The channel determines authority; the operation remains subject to the
/// server-side role allowlist and payload decoder.
pub type QueryIpcRequestV2 = unsafe extern "C" fn(
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
pub type QueryWriteResultV2 =
    unsafe extern "C" fn(context: *mut std::ffi::c_void, bytes: *const u8, bytes_len: u64) -> i32;

/// Sticky notification that a SQL statement failed. The host records this
/// independently of the query entry-point return value.
pub type QueryStatementFailureV3 =
    unsafe extern "C" fn(context: *mut std::ffi::c_void, failure_code: u32) -> i32;

/// Dedicated bounded callback for a SELECT request on the optimizer channel.
/// The operation is fixed to OptimizeSelect; query code cannot choose another
/// operation or use this endpoint for storage authority.
pub type QueryOptimizerRequestV4 = unsafe extern "C" fn(
    context: *mut std::ffi::c_void,
    request: *const u8,
    request_len: u64,
    response: *mut u8,
    response_capacity: u64,
    response_len: *mut u64,
) -> i32;

include!("query_abi_records.rs");

pub type QueryMainV2 = unsafe extern "C" fn(*const HostApiV2, *const InvocationV2) -> c_int;
pub type QueryMainV3 = unsafe extern "C" fn(*const HostApiV3, *const InvocationV3) -> c_int;
pub type QueryMainV4 = unsafe extern "C" fn(*const HostApiV4, *const InvocationV4) -> c_int;

pub type QueryMainV1 = unsafe extern "C" fn(*const HostApiV1, *const InvocationV1) -> c_int;

include!("query_abi_layout_assertions.rs");

/// Invocation record whose parameter pointer is tied to the borrowed slice.
/// Keep this value alive for the complete C entry-point call.
#[must_use = "the invocation record must remain alive while the C entry point uses it"]
#[derive(Debug)]
pub struct BorrowedInvocationV1<'a> {
    record: InvocationV1,
    _parameters: std::marker::PhantomData<&'a [u8]>,
}

impl<'a> BorrowedInvocationV1<'a> {
    pub fn new(parameters: &'a [u8]) -> Result<Self, ParameterError> {
        let parameters_len =
            u64::try_from(parameters.len()).map_err(|_| ParameterError::TooLarge)?;
        if parameters_len > MAX_PARAMETER_BYTES {
            return Err(ParameterError::TooLarge);
        }
        let parameters_pointer = if parameters.is_empty() {
            std::ptr::null()
        } else {
            parameters.as_ptr()
        };
        Ok(Self {
            record: InvocationV1 {
                struct_size: std::mem::size_of::<InvocationV1>() as u32,
                abi_version: QUERY_ABI_VERSION,
                parameters: parameters_pointer,
                parameters_len,
            },
            _parameters: std::marker::PhantomData,
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

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DescriptorV2Error {
    StructSize,
    AbiVersion,
    HostApiSize,
    InvocationSize,
}

pub type DescriptorV3Error = DescriptorV2Error;
pub type DescriptorV4Error = DescriptorV2Error;

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

/// Check the exported v2 descriptor before resolving its entry point.
pub fn validate_descriptor_v2(descriptor: &QueryDescriptorV2) -> Result<(), DescriptorV2Error> {
    if descriptor.struct_size != std::mem::size_of::<QueryDescriptorV2>() as u32 {
        return Err(DescriptorV2Error::StructSize);
    }
    if descriptor.abi_version != QUERY_ABI_V2_VERSION {
        return Err(DescriptorV2Error::AbiVersion);
    }
    if descriptor.host_api_size != std::mem::size_of::<HostApiV2>() as u32 {
        return Err(DescriptorV2Error::HostApiSize);
    }
    if descriptor.invocation_size != std::mem::size_of::<InvocationV2>() as u32 {
        return Err(DescriptorV2Error::InvocationSize);
    }
    Ok(())
}

/// Check the exported v3 descriptor before resolving its entry point.
pub fn validate_descriptor_v3(descriptor: &QueryDescriptorV3) -> Result<(), DescriptorV3Error> {
    if descriptor.struct_size != std::mem::size_of::<QueryDescriptorV3>() as u32 {
        return Err(DescriptorV2Error::StructSize);
    }
    if descriptor.abi_version != QUERY_ABI_V3_VERSION {
        return Err(DescriptorV2Error::AbiVersion);
    }
    if descriptor.host_api_size != std::mem::size_of::<HostApiV3>() as u32 {
        return Err(DescriptorV2Error::HostApiSize);
    }
    if descriptor.invocation_size != std::mem::size_of::<InvocationV3>() as u32 {
        return Err(DescriptorV2Error::InvocationSize);
    }
    Ok(())
}

/// Check the exported v4 descriptor before resolving its entry point.
pub fn validate_descriptor_v4(descriptor: &QueryDescriptorV4) -> Result<(), DescriptorV4Error> {
    if descriptor.struct_size != std::mem::size_of::<QueryDescriptorV4>() as u32 {
        return Err(DescriptorV2Error::StructSize);
    }
    if descriptor.abi_version != QUERY_ABI_V4_VERSION {
        return Err(DescriptorV2Error::AbiVersion);
    }
    if descriptor.host_api_size != std::mem::size_of::<HostApiV4>() as u32 {
        return Err(DescriptorV2Error::HostApiSize);
    }
    if descriptor.invocation_size != std::mem::size_of::<InvocationV4>() as u32 {
        return Err(DescriptorV2Error::InvocationSize);
    }
    Ok(())
}

#[cfg(test)]
mod generated_payload_tests {
    use super::{
        decode_supervisor_admin_cancel_request, decode_supervisor_cancel_result_simple,
        decode_supervisor_cancel_result_terminal, decode_supervisor_invocation_control_request,
        encode_supervisor_admin_cancel_request, encode_supervisor_cancel_result_simple,
        encode_supervisor_cancel_result_terminal, encode_supervisor_invocation_control_request,
    };

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
}
