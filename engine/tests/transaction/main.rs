mod helpers;
macro_rules! forward_tests {
    ($mod:ident => { $($name:ident -> $ret:ty),* $(,)? }) => {
        $(
            #[test]
            fn $name() -> $ret {
                $mod::$name()
            }
        )*
    };
}

mod suite_01;
forward_tests!(suite_01 => {
    authorized_relation_listing_uses_schema_usage_and_data_grants -> Result<(), Box<dyn std::error::Error>>,
    relation_listing_rejects_grants_not_present_in_the_pinned_catalog -> Result<(), Box<dyn std::error::Error>>,
    schema_listing_includes_the_public_namespace_and_usage_grants -> Result<(), Box<dyn std::error::Error>>,
    schema_cursor_rejects_modified_wire_tokens -> Result<(), Box<dyn std::error::Error>>,
    authorized_schema_resolution_matches_only_visible_exact_names -> Result<(), Box<dyn std::error::Error>>,
    schema_access_loader_uses_pinned_principal_schema_usage_grants -> Result<(), Box<dyn std::error::Error>>,
    relation_discovery_grant_loader_filters_to_one_schema -> Result<(), Box<dyn std::error::Error>>,
    catalog_authorized_relation_listing_derives_the_invocation_scope -> Result<(), Box<dyn std::error::Error>>,
    catalog_authorized_schema_listing_derives_the_invocation_scope -> Result<(), Box<dyn std::error::Error>>,
    information_schema_schemata_exposes_public_and_authorized_names -> Result<(), Box<dyn std::error::Error>>,
    information_schema_builtin_relations_have_public_self_descriptions -> Result<(), Box<dyn std::error::Error>>,
    information_schema_tables_projects_authorized_schema_pages -> Result<(), Box<dyn std::error::Error>>,
    information_schema_tables_scan_walks_visible_schema_pages -> Result<(), Box<dyn std::error::Error>>,
    information_schema_tables_cursor_retains_renamed_relation_snapshot -> Result<(), Box<dyn std::error::Error>>,
    information_schema_tables_cursor_retains_grant_snapshot_across_revocation -> Result<(), Box<dyn std::error::Error>>,
    information_schema_columns_projects_declared_shape_in_pages -> Result<(), Box<dyn std::error::Error>>,
    information_schema_columns_scan_walks_visible_relation_pages -> Result<(), Box<dyn std::error::Error>>,
    information_schema_scans_respect_each_principals_grants -> Result<(), Box<dyn std::error::Error>>,
    executor_scan_reads_filtered_information_schema_schemata -> Result<(), Box<dyn std::error::Error>>,
    column_grants_can_discover_a_relation_and_pages_are_snapshot_scoped -> Result<(), Box<dyn std::error::Error>>,
    authorized_column_description_returns_only_declared_shape -> Result<(), Box<dyn std::error::Error>>,
    authorized_relation_resolution_requires_visibility_and_exact_name -> Result<(), Box<dyn std::error::Error>>,
    catalog_authorized_qualified_relation_resolution_uses_pinned_grants -> Result<(), Box<dyn std::error::Error>>,
    executor_relation_handles_are_connection_scoped_and_revalidated -> Result<(), Box<dyn std::error::Error>>,
});

mod suite_02;
forward_tests!(suite_02 => {
    executor_resolve_relation_ipc_dispatch_returns_only_authorized_handle -> Result<(), Box<dyn std::error::Error>>,
    executor_relation_socket_loop_ends_session_without_finalizing -> Result<(), Box<dyn std::error::Error>>,
    information_schema_virtual_scans_round_trip_through_executor_ipc -> Result<(), Box<dyn std::error::Error>>,
    catalog_authorized_scan_builds_its_projection_from_pinned_records -> Result<(), Box<dyn std::error::Error>>,
});

mod suite_03;
forward_tests!(suite_03 => {
    authorized_column_description_pages_bind_the_snapshot_and_order -> Result<(), Box<dyn std::error::Error>>,
    invocation_reads_are_projected_after_snapshot_selection -> Result<(), Box<dyn std::error::Error>>,
    authorization_evaluation_error_aborts_and_persists_terminal_status -> Result<(), Box<dyn std::error::Error>>,
    authorization_context_cannot_change_the_admitted_principal -> Result<(), Box<dyn std::error::Error>>,
    latest_catalog_authority_change_blocks_a_stale_invocation_commit -> Result<(), Box<dyn std::error::Error>>,
    racing_catalog_authority_change_linearizes_before_or_after_pending_write -> Result<(), Box<dyn std::error::Error>>,
    catalog_absence_guard_blocks_a_phantom_authority_record -> Result<(), Box<dyn std::error::Error>>,
    catalog_expression_reads_are_decoded_and_guarded_at_commit -> Result<(), Box<dyn std::error::Error>>,
    invocation_sees_own_writes_and_commits_against_its_pinned_snapshot -> Result<(), Box<dyn std::error::Error>>,
    invocation_snapshot_hides_private_writes_and_keeps_repeated_reads_stable -> Result<(), Box<dyn std::error::Error>>,
    retained_invocation_snapshot_survives_a_durable_write_burst -> Result<(), Box<dyn std::error::Error>>,
    retained_invocation_snapshot_survives_concurrent_writer_pressure -> Result<(), Box<dyn std::error::Error>>,
    managed_cancellation_releases_snapshot_admission_during_write_pressure -> Result<(), Box<dyn std::error::Error>>,
    authorized_row_scan_pages_over_the_pinned_snapshot_and_private_overlay -> Result<(), Box<dyn std::error::Error>>,
    managed_scan_work_budget_is_cumulative_and_aborts_private_writes -> Result<(), Box<dyn std::error::Error>>,
});

mod suite_04;
forward_tests!(suite_04 => {
    authorized_scan_estimate_counts_sanitized_cells_and_private_writes -> Result<(), Box<dyn std::error::Error>>,
    constant_predicate_reuses_fresh_population_without_reading_rows -> Result<(), Box<dyn std::error::Error>>,
    authorized_predicate_estimate_compares_readable_decimal_columns -> Result<(), Box<dyn std::error::Error>>,
    authorized_predicate_estimate_compares_readable_scalar_columns -> Result<(), Box<dyn std::error::Error>>,
});

mod suite_05;
forward_tests!(suite_05 => {
    authorized_join_estimate_counts_duplicate_matches_and_private_overlay -> Result<(), Box<dyn std::error::Error>>,
    authorized_join_estimate_returns_unknown_at_row_and_key_byte_limits -> Result<(), Box<dyn std::error::Error>>,
    authorized_scan_estimate_returns_unknown_at_candidate_budget -> Result<(), Box<dyn std::error::Error>>,
    invocation_auto_indexes_row_writes -> Result<(), Box<dyn std::error::Error>>,
});

mod suite_06;
forward_tests!(suite_06 => {
    unique_constraint_uses_its_backing_index_for_invocation_writes -> Result<(), Box<dyn std::error::Error>>,
});

mod suite_07;
forward_tests!(suite_07 => {
    authorized_composite_integer_varchar_unique_probe_matches_reference -> Result<(), Box<dyn std::error::Error>>,
    catalog_foreign_keys_check_child_and_parent_mutations -> Result<(), Box<dyn std::error::Error>>,
    catalog_schema_validates_rows_without_secondary_indexes -> Result<(), Box<dyn std::error::Error>>,
    failed_commit_validation_records_aborted_outcome -> Result<(), Box<dyn std::error::Error>>,
    fenced_invocation_commit_waits_for_ddl_and_aborts_on_timeout -> Result<(), Box<dyn std::error::Error>>,
    replacing_new_index_membership_discards_the_intermediate_key -> Result<(), Box<dyn std::error::Error>>,
    missing_index_removal_aborts_with_a_durable_outcome -> Result<(), Box<dyn std::error::Error>>,
    deleting_new_row_releases_index_overlay_budget -> Result<(), Box<dyn std::error::Error>>,
    index_overlay_budget_aborts_the_entire_invocation -> Result<(), Box<dyn std::error::Error>>,
    unsupported_database_versions_reject_transactions_and_commits -> Result<(), Box<dyn std::error::Error>>,
    deleting_same_snapshot_row_twice_aborts_invocation -> Result<(), Box<dyn std::error::Error>>,
    stale_invocation_aborts_and_insert_then_delete_commits_as_noop -> Result<(), Box<dyn std::error::Error>>,
});

mod suite_08;
forward_tests!(suite_08 => {
    statement_error_aborts_earlier_writes_and_prevents_finalization -> Result<(), Box<dyn std::error::Error>>,
    admission_requires_ready_metadata_and_a_fresh_valid_invocation_id -> Result<(), Box<dyn std::error::Error>>,
    concurrent_same_row_writers_have_one_commit_and_one_durable_conflict -> Result<(), Box<dyn std::error::Error>>,
    concurrent_authorized_updates_have_one_commit_and_one_durable_conflict -> Result<(), Box<dyn std::error::Error>>,
    snapshot_isolation_allows_documented_disjoint_row_write_skew -> Result<(), Box<dyn std::error::Error>>,
    field_patch_merges_with_pinned_tuple_and_preserves_untouched_values -> Result<(), Box<dyn std::error::Error>>,
    authorized_patch_checks_changed_cells_and_freezes_policy_dependencies -> Result<(), Box<dyn std::error::Error>>,
    authorized_delete_requires_visibility_and_an_explicit_delete_grant -> Result<(), Box<dyn std::error::Error>>,
    authorized_insert_materializes_defaults_before_policy_and_check_validation -> Result<(), Box<dyn std::error::Error>>,
    authorized_insert_implicit_null_requires_column_insert_grant -> Result<(), Box<dyn std::error::Error>>,
    catalog_insert_allocates_generated_identity_and_rejects_caller_override -> Result<(), Box<dyn std::error::Error>>,
});

mod suite_09;
forward_tests!(suite_09 => {
    composite_index_prefix_scan_reconciles_snapshot_and_private_overlay -> Result<(), Box<dyn std::error::Error>>,
    schema_loader_uses_each_columns_type_parameters -> Result<(), Box<dyn std::error::Error>>,
    catalog_rejects_deleting_expression_used_by_existing_check -> Result<(), Box<dyn std::error::Error>>,
});

mod suite_10;
forward_tests!(suite_10 => {
    invocation_loads_and_guards_catalog_table_schema -> Result<(), Box<dyn std::error::Error>>,
    catalog_authority_rejects_disabled_or_role_invocation_principals -> Result<(), Box<dyn std::error::Error>>,
    table_epochs_cannot_move_back_after_catalog_changes -> Result<(), Box<dyn std::error::Error>>,
    new_membership_cannot_be_hidden_by_restoring_principal_epoch -> Result<(), Box<dyn std::error::Error>>,
    invocation_loads_and_guards_transitive_catalog_role_closure -> Result<(), Box<dyn std::error::Error>>,
    authorized_insert_checks_non_generated_fields_and_allows_generated_fields -> Result<(), Box<dyn std::error::Error>>,
});

mod suite_11;
forward_tests!(suite_11 => {
    authorized_write_revalidates_projection_catalog_dependencies -> Result<(), Box<dyn std::error::Error>>,
});
