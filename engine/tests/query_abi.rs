#![cfg(all(target_os = "linux", target_arch = "x86_64"))]

use prdb_engine::query_abi::{
    BorrowedInvocationV1, HostApiV1, HostApiV2, InvocationV1, InvocationV2, ParameterError,
    QUERY_ABI_V2_VERSION, QUERY_ABI_VERSION, QueryDescriptorV1, QueryDescriptorV2,
    validate_descriptor_v1, validate_descriptor_v2,
};

#[test]
fn query_abi_v1_layout_matches_the_x86_64_c_contract() {
    assert_eq!(std::mem::size_of::<HostApiV1>(), 8);
    assert_eq!(std::mem::align_of::<HostApiV1>(), 4);
    assert_eq!(std::mem::size_of::<QueryDescriptorV1>(), 16);
    assert_eq!(std::mem::align_of::<QueryDescriptorV1>(), 4);
    assert_eq!(std::mem::size_of::<InvocationV1>(), 24);
    assert_eq!(std::mem::align_of::<InvocationV1>(), 8);
    assert_eq!(std::mem::offset_of!(InvocationV1, parameters), 8);
    assert_eq!(std::mem::offset_of!(InvocationV1, parameters_len), 16);
}

#[test]
fn query_abi_v2_layout_matches_the_x86_64_c_contract() {
    assert_eq!(std::mem::size_of::<HostApiV2>(), 40);
    assert_eq!(std::mem::align_of::<HostApiV2>(), 8);
    assert_eq!(std::mem::offset_of!(HostApiV2, context), 8);
    assert_eq!(std::mem::offset_of!(HostApiV2, executor_request), 16);
    assert_eq!(std::mem::offset_of!(HostApiV2, planner_request), 24);
    assert_eq!(std::mem::offset_of!(HostApiV2, write_result), 32);
    assert_eq!(std::mem::size_of::<QueryDescriptorV2>(), 16);
    assert_eq!(std::mem::align_of::<QueryDescriptorV2>(), 4);
    assert_eq!(std::mem::size_of::<InvocationV2>(), 24);
    assert_eq!(std::mem::align_of::<InvocationV2>(), 8);
    assert_eq!(std::mem::offset_of!(InvocationV2, parameters), 8);
    assert_eq!(std::mem::offset_of!(InvocationV2, parameters_len), 16);
}

#[test]
fn query_invocation_parameters_are_borrowed_bounded_and_null_only_when_empty() {
    let empty_owner = BorrowedInvocationV1::new(&[]).unwrap();
    let empty = empty_owner.as_record();
    assert!(empty.parameters.is_null());
    assert_eq!(empty.parameters_len, 0);

    let parameters = [7, 8, 9];
    let invocation_owner = BorrowedInvocationV1::new(&parameters).unwrap();
    let invocation = invocation_owner.as_record();
    assert_eq!(invocation.parameters, parameters.as_ptr());
    assert_eq!(invocation.parameters_len, parameters.len() as u64);

    let oversized = vec![0; prdb_engine::query_abi::MAX_PARAMETER_BYTES as usize + 1];
    assert_eq!(
        BorrowedInvocationV1::new(&oversized).unwrap_err(),
        ParameterError::TooLarge
    );
}

#[test]
fn query_abi_v1_descriptor_rejects_incompatible_sizes_and_versions() {
    let valid = QueryDescriptorV1 {
        struct_size: std::mem::size_of::<QueryDescriptorV1>() as u32,
        abi_version: QUERY_ABI_VERSION,
        host_api_size: std::mem::size_of::<HostApiV1>() as u32,
        invocation_size: std::mem::size_of::<InvocationV1>() as u32,
    };
    assert!(validate_descriptor_v1(&valid).is_ok());

    let mut incompatible = valid;
    incompatible.abi_version += 1;
    assert!(validate_descriptor_v1(&incompatible).is_err());
    incompatible = valid;
    incompatible.host_api_size -= 1;
    assert!(validate_descriptor_v1(&incompatible).is_err());
    incompatible = valid;
    incompatible.invocation_size -= 1;
    assert!(validate_descriptor_v1(&incompatible).is_err());
    incompatible = valid;
    incompatible.struct_size -= 1;
    assert!(validate_descriptor_v1(&incompatible).is_err());
}

#[test]
fn query_abi_v2_descriptor_rejects_incompatible_sizes_and_versions() {
    let valid = QueryDescriptorV2 {
        struct_size: std::mem::size_of::<QueryDescriptorV2>() as u32,
        abi_version: QUERY_ABI_V2_VERSION,
        host_api_size: std::mem::size_of::<HostApiV2>() as u32,
        invocation_size: std::mem::size_of::<InvocationV2>() as u32,
    };
    assert!(validate_descriptor_v2(&valid).is_ok());

    let mut incompatible = valid;
    incompatible.abi_version = QUERY_ABI_VERSION;
    assert!(validate_descriptor_v2(&incompatible).is_err());
    incompatible = valid;
    incompatible.host_api_size -= 1;
    assert!(validate_descriptor_v2(&incompatible).is_err());
    incompatible = valid;
    incompatible.invocation_size -= 1;
    assert!(validate_descriptor_v2(&incompatible).is_err());
    incompatible = valid;
    incompatible.struct_size -= 1;
    assert!(validate_descriptor_v2(&incompatible).is_err());
}
