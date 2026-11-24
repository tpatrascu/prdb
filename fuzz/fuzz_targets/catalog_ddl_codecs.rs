#![no_main]

use libfuzzer_sys::fuzz_target;
use prdb_engine::catalog::{
    MAX_CATALOG_ENVELOPE_BYTES, decode_policy_target, decode_registered_envelope, family_schema,
};
use prdb_engine::expression::{decode_catalog_expression, decode_expression};
use prdb_engine::optimizer_plan::{decode_delete_plan_v1, decode_plan_v1, decode_update_plan_v1};
use prdb_engine::optimizer_request::{
    decode_composite_group_aggregate_request, decode_composite_group_count_request,
    decode_delete_request, decode_filtered_composite_group_aggregate_request,
    decode_group_aggregate_request, decode_group_count_request, decode_join_select_request,
    decode_ordered_composite_group_aggregate_request, decode_ordered_composite_group_count_request,
    decode_ordered_group_aggregate_request, decode_ordered_group_count_request,
    decode_ordered_select_request, decode_select_request, decode_update_request,
};

fuzz_target!(|data: &[u8]| {
    if data.len() > MAX_CATALOG_ENVELOPE_BYTES {
        return;
    }

    let _ = decode_registered_envelope(data);
    let _ = decode_policy_target(data);
    let _ = decode_catalog_expression(data);
    let _ = decode_expression(data);

    if let Some(family_bytes) = data.get(6..8) {
        let family = u16::from_le_bytes(family_bytes.try_into().expect("fixed header slice"));
        if let Some(schema) = family_schema(family) {
            let _ = prdb_engine::catalog::decode_envelope(&schema, data);
        }
    }

    let _ = decode_select_request(data);
    let _ = decode_ordered_select_request(data);
    let _ = decode_join_select_request(data);
    let _ = decode_group_count_request(data);
    let _ = decode_group_aggregate_request(data);
    let _ = decode_ordered_group_aggregate_request(data);
    let _ = decode_ordered_group_count_request(data);
    let _ = decode_ordered_composite_group_count_request(data);
    let _ = decode_composite_group_count_request(data);
    let _ = decode_delete_request(data);
    let _ = decode_update_request(data);

    let _ = decode_plan_v1(data);
    let _ = prdb_engine::optimizer_plan::decode_join_select_plan_v1(data);
    let _ = prdb_engine::optimizer_plan::decode_group_count_plan_v1(data);
    let _ = prdb_engine::optimizer_plan::decode_group_aggregate_plan_v1(data);
    let _ = prdb_engine::optimizer_plan::decode_ordered_group_aggregate_plan_v1(data);
    let _ = prdb_engine::optimizer_plan::decode_ordered_group_count_plan_v1(data);
    let _ = prdb_engine::optimizer_plan::decode_ordered_composite_group_count_plan_v1(data);
    let _ = prdb_engine::optimizer_plan::decode_composite_group_count_plan_v1(data);
    let _ = prdb_engine::optimizer_plan::decode_composite_group_aggregate_plan_v1(data);
    let _ = prdb_engine::optimizer_plan::decode_filtered_composite_group_aggregate_plan_v1(data);
    let _ = prdb_engine::optimizer_plan::decode_ordered_composite_group_aggregate_plan_v1(data);
    let _ = prdb_engine::optimizer_plan::decode_ordered_select_plan_v1(data);
    let _ = decode_delete_plan_v1(data);
    let _ = decode_update_plan_v1(data);

    let _ = decode_composite_group_aggregate_request(data);
    let _ = decode_filtered_composite_group_aggregate_request(data);
    let _ = decode_ordered_composite_group_aggregate_request(data);
});
