//! Initial versioned C entry-point contract for query libraries.
//!
//! This ABI currently carries invocation parameters only. Storage, planning,
//! result, and cancellation callbacks will be added by a later ABI version.

use std::ffi::c_int;

pub const QUERY_ABI_VERSION: u32 = 1;
pub const QUERY_DESCRIPTOR_SYMBOL: &str = "prdb_query_descriptor_v1";
pub const QUERY_ENTRY_SYMBOL: &str = "prdb_query_main_v1";
pub const MAX_PARAMETER_BYTES: u64 = 1024 * 1024;

#[repr(C)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct HostApiV1 {
    pub struct_size: u32,
    pub abi_version: u32,
}

#[repr(C)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct InvocationV1 {
    pub struct_size: u32,
    pub abi_version: u32,
    pub parameters: *const u8,
    pub parameters_len: u64,
}

#[repr(C)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct QueryDescriptorV1 {
    pub struct_size: u32,
    pub abi_version: u32,
    pub host_api_size: u32,
    pub invocation_size: u32,
}

pub type QueryMainV1 = unsafe extern "C" fn(*const HostApiV1, *const InvocationV1) -> c_int;

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
