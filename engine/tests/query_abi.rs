#![cfg(all(target_os = "linux", target_arch = "x86_64"))]

use prdb_engine::query_abi::{
    BorrowedInvocationV1, HostApiV1, InvocationV1, ParameterError, QUERY_ABI_VERSION,
    QueryDescriptorV1, validate_descriptor_v1,
};

#[test]
fn generated_nested_record_codecs_preserve_wire_bytes_and_boundaries() {
    let name =
        prdb_engine::query_abi::encode_executor_resolve_relation_available_response_relation_name(
            "items",
        )
        .unwrap();
    assert_eq!(name, [0, 5, b'i', b't', b'e', b'm', b's']);

    let mut tail = name.clone();
    tail.extend_from_slice(
        &prdb_engine::query_abi::encode_executor_resolve_relation_available_response_column_count(
            2,
        ),
    );
    let (decoded_name, consumed) =
        prdb_engine::query_abi::decode_executor_resolve_relation_available_response_relation_name(
            &tail,
        )
        .unwrap();
    assert_eq!(decoded_name, "items");
    assert_eq!(consumed, name.len());
    assert_eq!(
        prdb_engine::query_abi::decode_executor_resolve_relation_available_response_column_count(
            &tail[consumed..]
        ),
        Some(2)
    );
    assert!(
        prdb_engine::query_abi::decode_executor_resolve_relation_available_response_relation_name(
            &tail[..6]
        )
        .is_none()
    );

    let planner_name =
        prdb_engine::query_abi::encode_planner_describe_relation_available_response_relation_name(
            "items",
        )
        .unwrap();
    assert_eq!(
        prdb_engine::query_abi::decode_planner_describe_relation_available_response_relation_name(
            &planner_name
        ),
        Some(("items", planner_name.len()))
    );
    let plan_name = prdb_engine::query_abi::encode_planner_describe_plan_relation_available_response_relation_name(
        "items",
    )
    .unwrap();
    assert_eq!(
        prdb_engine::query_abi::decode_planner_describe_plan_relation_available_response_relation_name(
            &plan_name
        ),
        Some(("items", plan_name.len()))
    );
    assert_eq!(
        prdb_engine::query_abi::decode_planner_describe_plan_relation_available_response_column_count(
            &[0, 3],
        ),
        Some(3)
    );

    let column =
        prdb_engine::query_abi::encode_visible_column_record([0x42; 16], 0x0102_0304, 1, 3, [0; 4]);
    assert_eq!(
        column,
        [[0x42; 16].as_slice(), &[1, 2, 3, 4, 1, 3, 0, 0, 0, 0],]
            .concat()
            .as_slice()
    );
    assert_eq!(
        prdb_engine::query_abi::decode_visible_column_record(&column),
        Some(([0x42; 16], 0x0102_0304, 1, 3, [0; 4]))
    );

    let value_header = prdb_engine::query_abi::encode_scalar_value_record(3, 4);
    assert_eq!(value_header, [3, 0, 0, 0, 4]);
    assert_eq!(
        prdb_engine::query_abi::decode_scalar_value_record(&value_header),
        Some((3, 4))
    );
    let value_bytes =
        prdb_engine::query_abi::encode_scalar_value_record_value_bytes(&[0xff, 0, 1, 2]).unwrap();
    assert_eq!(
        prdb_engine::query_abi::decode_scalar_value_record_value_bytes(value_bytes, 4),
        Some([0xff, 0, 1, 2].as_slice())
    );
    assert!(
        prdb_engine::query_abi::decode_scalar_value_record_value_bytes(&value_bytes[..3], 4)
            .is_none()
    );

    let patch_ordinal = prdb_engine::query_abi::encode_patch_ordinal_record(0x0102_0304);
    assert_eq!(patch_ordinal, [1, 2, 3, 4]);
    assert_eq!(
        prdb_engine::query_abi::decode_patch_ordinal_record(&patch_ordinal),
        Some(0x0102_0304)
    );
    assert!(prdb_engine::query_abi::decode_patch_ordinal_record(&patch_ordinal[..3]).is_none());

    let row_handle = [8; 16];
    let batch_row = prdb_engine::query_abi::encode_executor_batch_row_record(row_handle, 2);
    assert_eq!(batch_row.len(), 18);
    assert_eq!(&batch_row[..16], &row_handle);
    assert_eq!(&batch_row[16..], &[0, 2]);
    assert_eq!(
        prdb_engine::query_abi::decode_executor_batch_row_record(
            &[batch_row.as_slice(), &[0, 1]].concat()
        ),
        Some((row_handle, 2))
    );
    assert!(prdb_engine::query_abi::decode_executor_batch_row_record(&batch_row[..17]).is_none());

    for state in 0..=2 {
        let state_record = prdb_engine::query_abi::encode_sanitized_cell_tag_record(state);
        assert_eq!(state_record, [state]);
        assert_eq!(
            prdb_engine::query_abi::decode_sanitized_cell_tag_record(&state_record),
            Some(state)
        );
    }
    assert!(prdb_engine::query_abi::decode_sanitized_cell_tag_record(&[]).is_none());

    for state in 0..=2 {
        let state_record = prdb_engine::query_abi::encode_mutation_cell_tag_record(state);
        assert_eq!(state_record, [state]);
        assert_eq!(
            prdb_engine::query_abi::decode_mutation_cell_tag_record(&state_record),
            Some(state)
        );
    }
    assert!(prdb_engine::query_abi::decode_mutation_cell_tag_record(&[]).is_none());
}

#[test]
fn query_abi_v1_layout_matches_the_x86_64_c_contract() {
    assert_eq!(std::mem::size_of::<HostApiV1>(), 64);
    assert_eq!(std::mem::align_of::<HostApiV1>(), 8);
    assert_eq!(std::mem::offset_of!(HostApiV1, context), 8);
    assert_eq!(std::mem::offset_of!(HostApiV1, executor_request), 16);
    assert_eq!(std::mem::offset_of!(HostApiV1, planner_request), 24);
    assert_eq!(std::mem::offset_of!(HostApiV1, write_result), 32);
    assert_eq!(std::mem::offset_of!(HostApiV1, statement_failure), 40);
    assert_eq!(std::mem::offset_of!(HostApiV1, optimizer_request), 48);
    assert_eq!(std::mem::offset_of!(HostApiV1, spill_request), 56);
    assert_eq!(std::mem::size_of::<QueryDescriptorV1>(), 16);
    assert_eq!(std::mem::align_of::<QueryDescriptorV1>(), 4);
    assert_eq!(std::mem::size_of::<InvocationV1>(), 40);
    assert_eq!(std::mem::align_of::<InvocationV1>(), 8);
    assert_eq!(std::mem::offset_of!(InvocationV1, parameters), 8);
    assert_eq!(std::mem::offset_of!(InvocationV1, parameters_len), 16);
    assert_eq!(std::mem::offset_of!(InvocationV1, sql_data), 24);
    assert_eq!(std::mem::offset_of!(InvocationV1, sql_data_len), 32);
}

#[test]
fn query_invocation_parameters_are_borrowed_bounded_and_null_only_when_empty() {
    let empty_owner = BorrowedInvocationV1::new(&[], &[]).unwrap();
    let empty = empty_owner.as_record();
    assert!(empty.parameters.is_null());
    assert_eq!(empty.parameters_len, 0);
    assert!(empty.sql_data.is_null());
    assert_eq!(empty.sql_data_len, 0);

    let parameters = [7, 8, 9];
    let sql_data = [1, 2, 3, 4];
    let invocation_owner = BorrowedInvocationV1::new(&parameters, &sql_data).unwrap();
    let invocation = invocation_owner.as_record();
    assert_eq!(invocation.parameters, parameters.as_ptr());
    assert_eq!(invocation.parameters_len, parameters.len() as u64);
    assert_eq!(invocation.sql_data, sql_data.as_ptr());
    assert_eq!(invocation.sql_data_len, sql_data.len() as u64);

    let oversized = vec![0; prdb_engine::query_abi::MAX_PARAMETER_BYTES as usize + 1];
    assert_eq!(
        BorrowedInvocationV1::new(&oversized, &[]).unwrap_err(),
        ParameterError::TooLarge
    );
    let oversized_sql = vec![0; prdb_engine::query_abi::MAX_SQL_DATA_BYTES as usize + 1];
    assert_eq!(
        BorrowedInvocationV1::new(&[], &oversized_sql).unwrap_err(),
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
