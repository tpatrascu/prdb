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
    storage_manager_creates_schema_atomically_for_privileged_actor -> Result<(), Box<dyn std::error::Error>>,
    schema_drop_racing_table_creation_never_leaves_an_orphan -> Result<(), Box<dyn std::error::Error>>,
    schema_rename_racing_table_creation_preserves_catalog_identity -> Result<(), Box<dyn std::error::Error>>,
    storage_manager_creates_typed_table_with_atomic_check_constraint -> Result<(), Box<dyn std::error::Error>>,
});

mod suite_02;
forward_tests!(suite_02 => {
    storage_manager_drop_table_removes_owned_catalog_and_rows -> Result<(), Box<dyn std::error::Error>>,
    storage_manager_drop_table_removes_owned_primary_and_unique_indexes -> Result<(), Box<dyn std::error::Error>>,
    unique_constraint_backfill_racing_duplicate_insert_is_atomic -> Result<(), Box<dyn std::error::Error>>,
    storage_manager_drop_table_racing_invocation_leaves_no_orphan_row -> Result<(), Box<dyn std::error::Error>>,
    storage_manager_drop_table_byte_limit_aborts_atomically -> Result<(), Box<dyn std::error::Error>>,
    storage_manager_drop_table_index_byte_limit_boundary_is_atomic -> Result<(), Box<dyn std::error::Error>>,
});

mod suite_03;
forward_tests!(suite_03 => {
    storage_manager_drop_table_index_entry_limit_aborts_atomically -> Result<(), Box<dyn std::error::Error>>,
    storage_manager_renames_tables_and_drop_column_rewrites_rows -> Result<(), Box<dyn std::error::Error>>,
    storage_manager_sets_and_drops_not_null_with_existing_row_validation -> Result<(), Box<dyn std::error::Error>>,
    set_not_null_racing_a_staged_null_insert_is_atomic -> Result<(), Box<dyn std::error::Error>>,
});

mod suite_04;
forward_tests!(suite_04 => {
    drop_not_null_racing_a_staged_writer_is_atomic -> Result<(), Box<dyn std::error::Error>>,
    storage_manager_widens_varchar_atomically_and_keeps_index_current -> Result<(), Box<dyn std::error::Error>>,
    storage_manager_truncates_rows_and_index_memberships_atomically -> Result<(), Box<dyn std::error::Error>>,
    storage_manager_truncate_restricts_real_incoming_foreign_key -> Result<(), Box<dyn std::error::Error>>,
    storage_manager_sets_replaces_and_drops_column_defaults -> Result<(), Box<dyn std::error::Error>>,
});

mod suite_05;
forward_tests!(suite_05 => {
    column_default_change_racing_column_drop_preserves_catalog_integrity -> Result<(), Box<dyn std::error::Error>>,
    column_default_and_not_null_alterations_linearize_atomically -> Result<(), Box<dyn std::error::Error>>,
    storage_manager_adds_column_and_backfills_existing_rows -> Result<(), Box<dyn std::error::Error>>,
    catalog_commit_rejects_unresolved_schema_search_path_entry -> Result<(), Box<dyn std::error::Error>>,
    principal_name_index_tracks_renames_per_snapshot -> Result<(), Box<dyn std::error::Error>>,
    schema_name_index_tracks_renames_per_snapshot -> Result<(), Box<dyn std::error::Error>>,
});

mod suite_06;
forward_tests!(suite_06 => {
    schema_create_drop_pressure_preserves_pinned_name_index_snapshots -> Result<(), Box<dyn std::error::Error>>,
    membership_indexes_track_both_directions_per_snapshot -> Result<(), Box<dyn std::error::Error>>,
    grant_indexes_track_grantee_and_object_per_snapshot -> Result<(), Box<dyn std::error::Error>>,
    catalog_commit_rejects_malformed_constraint_semantics -> Result<(), Box<dyn std::error::Error>>,
    catalog_commit_rejects_schema_mismatched_check_expression -> Result<(), Box<dyn std::error::Error>>,
    catalog_commit_rejects_nullable_primary_key_columns -> Result<(), Box<dyn std::error::Error>>,
    catalog_commit_rejects_constraints_for_missing_tables -> Result<(), Box<dyn std::error::Error>>,
    catalog_commit_rejects_dangling_dependency_edges_and_records_abort -> Result<(), Box<dyn std::error::Error>>,
});

mod suite_07;
forward_tests!(suite_07 => {
    storage_manager_authorizes_constraint_ddl_by_table_alter -> Result<(), Box<dyn std::error::Error>>,
    catalog_commit_checks_dependency_endpoints_in_the_final_catalog -> Result<(), Box<dyn std::error::Error>>,
    catalog_commit_rejects_table_parent_schema_mismatch -> Result<(), Box<dyn std::error::Error>>,
    catalog_commit_rejects_column_parent_mismatch_with_typed_table -> Result<(), Box<dyn std::error::Error>>,
    catalog_commit_requires_owned_child_parent_edge_consistency -> Result<(), Box<dyn std::error::Error>>,
    storage_manager_drop_table_restricts_external_foreign_key_dependencies -> Result<(), Box<dyn std::error::Error>>,
    foreign_key_creation_racing_referenced_table_drop_is_atomic -> Result<(), Box<dyn std::error::Error>>,
    foreign_key_creation_racing_referenced_primary_key_drop_is_atomic -> Result<(), Box<dyn std::error::Error>>,
    foreign_key_removal_racing_referenced_table_drop_is_atomic -> Result<(), Box<dyn std::error::Error>>,
    storage_manager_drops_foreign_key_and_advances_both_table_epochs -> Result<(), Box<dyn std::error::Error>>,
    catalog_rejects_foreign_key_deletion_without_identity_tombstone -> Result<(), Box<dyn std::error::Error>>,
    storage_manager_drops_self_referential_foreign_key_once_per_table -> Result<(), Box<dyn std::error::Error>>,
    storage_manager_creates_foreign_key_and_advances_both_table_epochs -> Result<(), Box<dyn std::error::Error>>,
});

mod suite_08;
forward_tests!(suite_08 => {
    foreign_key_creation_racing_a_staged_child_write_is_atomic -> Result<(), Box<dyn std::error::Error>>,
    foreign_key_drop_racing_a_staged_child_write_is_atomic -> Result<(), Box<dyn std::error::Error>>,
    storage_manager_atomically_creates_table_with_foreign_key_to_existing_key -> Result<(), Box<dyn std::error::Error>>,
    foreign_key_creation_racing_orphan_child_insert_is_atomic -> Result<(), Box<dyn std::error::Error>>,
    foreign_key_creation_racing_row_policy_creation_is_atomic -> Result<(), Box<dyn std::error::Error>>,
    storage_manager_creates_named_unique_index_and_backfills_atomically -> Result<(), Box<dyn std::error::Error>>,
});

mod suite_09;
forward_tests!(suite_09 => {
    storage_manager_creates_unique_constraint_and_backfills_existing_rows -> Result<(), Box<dyn std::error::Error>>,
    storage_manager_creates_primary_key_with_owned_unique_index -> Result<(), Box<dyn std::error::Error>>,
    storage_manager_refuses_to_drop_primary_key_referenced_by_foreign_key -> Result<(), Box<dyn std::error::Error>>,
    storage_manager_creates_and_drops_check_constraint -> Result<(), Box<dyn std::error::Error>>,
    storage_manager_drop_referencing_table_removes_owned_foreign_key -> Result<(), Box<dyn std::error::Error>>,
    storage_manager_drop_table_removes_self_referential_foreign_key -> Result<(), Box<dyn std::error::Error>>,
    catalog_commit_requires_foreign_key_target_dependency_edges -> Result<(), Box<dyn std::error::Error>>,
    adding_foreign_key_rejects_preexisting_orphan_rows -> Result<(), Box<dyn std::error::Error>>,
    adding_foreign_key_accepts_preexisting_matching_rows -> Result<(), Box<dyn std::error::Error>>,
    storage_manager_routes_invocations_and_catalog_commits_through_shared_fences -> Result<(), Box<dyn std::error::Error>>,
});

mod suite_10;
forward_tests!(suite_10 => {
    storage_manager_admission_requires_a_login_enabled_catalog_user -> Result<(), Box<dyn std::error::Error>>,
    authenticated_role_closure_budget_ignores_unreachable_memberships -> Result<(), Box<dyn std::error::Error>>,
    catalog_grant_loading_budget_ignores_unrelated_grants -> Result<(), Box<dyn std::error::Error>>,
    storage_manager_bounds_active_invocation_snapshots_across_clones -> Result<(), Box<dyn std::error::Error>>,
    storage_manager_expires_idle_invocations_and_releases_the_snapshot -> Result<(), Box<dyn std::error::Error>>,
    storage_manager_resolves_only_retained_outcomes_for_the_authenticated_owner -> Result<(), Box<dyn std::error::Error>>,
    storage_manager_reports_live_and_durable_invocation_status_to_the_owner -> Result<(), Box<dyn std::error::Error>>,
    storage_manager_lists_only_owner_live_invocations_with_generation_bound_pages -> Result<(), Box<dyn std::error::Error>>,
});

mod suite_11;
forward_tests!(suite_11 => {
    storage_manager_runtime_and_metadata_reads_enforce_authorization -> Result<(), Box<dyn std::error::Error>>,
});

mod suite_12;
forward_tests!(suite_12 => {
    denied_monitoring_pressure_does_not_starve_commit_or_cancellation -> Result<(), Box<dyn std::error::Error>>,
    storage_manager_audits_cancellation_before_commit_and_preserves_terminal_outcomes -> Result<(), Box<dyn std::error::Error>>,
    admin_cancellation_sync_failure_quarantines_until_reopen -> Result<(), Box<dyn std::error::Error>>,
    schema_create_sync_failure_recovers_atomic_catalog_audit_and_outcome -> Result<(), Box<dyn std::error::Error>>,
    identity_drop_sync_failure_recovers_tombstone_and_token_atomically -> Result<(), Box<dyn std::error::Error>>,
    role_drop_sync_failure_recovers_tombstone_audit_and_outcome_atomically -> Result<(), Box<dyn std::error::Error>>,
    storage_manager_rejects_invalid_recovered_images_before_admission -> Result<(), Box<dyn std::error::Error>>,
    startup_rejects_missing_bootstrap_descriptor_before_admission -> Result<(), Box<dyn std::error::Error>>,
    startup_rejects_principal_without_matching_identity_object -> Result<(), Box<dyn std::error::Error>>,
    catalog_commit_requires_backing_index_dependency_edges -> Result<(), Box<dyn std::error::Error>>,
    catalog_commit_rejects_not_null_when_an_existing_row_is_null -> Result<(), Box<dyn std::error::Error>>,
    catalog_commit_requires_expression_reference_dependency_edges -> Result<(), Box<dyn std::error::Error>>,
    catalog_commit_rejects_unknown_dependency_edge_kinds -> Result<(), Box<dyn std::error::Error>>,
    catalog_commit_requires_object_owner_principals -> Result<(), Box<dyn std::error::Error>>,
    catalog_commit_requires_index_object_identity_and_kind -> Result<(), Box<dyn std::error::Error>>,
    catalog_commit_rejects_indexes_with_missing_or_mistabled_columns -> Result<(), Box<dyn std::error::Error>>,
});

mod suite_13;
forward_tests!(suite_13 => {
    catalog_index_changes_reject_tables_with_existing_rows -> Result<(), Box<dyn std::error::Error>>,
    storage_manager_builds_and_drops_populated_indexes_atomically -> Result<(), Box<dyn std::error::Error>>,
    storage_manager_rebuilds_populated_indexes_atomically -> Result<(), Box<dyn std::error::Error>>,
    storage_manager_rejects_unique_rebuild_with_duplicate_values -> Result<(), Box<dyn std::error::Error>>,
    storage_manager_checks_index_ddl_authority_for_current_actor -> Result<(), Box<dyn std::error::Error>>,
    column_default_ddl_revalidates_after_table_owner_transfer -> Result<(), Box<dyn std::error::Error>>,
    index_ddl_racing_table_owner_transfer_rechecks_authority_at_commit -> Result<(), Box<dyn std::error::Error>>,
    policy_ddl_racing_table_owner_transfer_rechecks_authority_at_commit -> Result<(), Box<dyn std::error::Error>>,
});

mod suite_14;
forward_tests!(suite_14 => {
    system_privilege_dependency_survives_until_the_last_grant_is_revoked -> Result<(), Box<dyn std::error::Error>>,
    storage_manager_authorizes_membership_ddl_by_system_privilege_and_admin_role -> Result<(), Box<dyn std::error::Error>>,
    membership_ddl_requires_an_unexpired_credential_for_a_final_admin -> Result<(), Box<dyn std::error::Error>>,
    concurrent_admin_membership_revocations_preserve_a_usable_administrator -> Result<(), Box<dyn std::error::Error>>,
    storage_manager_restricts_system_privilege_ddl_to_database_admins -> Result<(), Box<dyn std::error::Error>>,
    identity_ddl_refuses_to_reuse_a_tombstoned_id -> Result<(), Box<dyn std::error::Error>>,
    identity_drop_aborts_a_role_authorized_staged_update -> Result<(), Box<dyn std::error::Error>>,
    identity_login_disable_survives_process_exit_after_commit -> Result<(), Box<dyn std::error::Error>>,
});

mod suite_15;
forward_tests!(suite_15 => {
    identity_ddl_drops_unreferenced_user_and_revokes_tokens_atomically -> Result<(), Box<dyn std::error::Error>>,
    token_rotation_racing_a_staged_write_linearizes -> Result<(), Box<dyn std::error::Error>>,
    identity_ddl_restricts_user_drop_when_a_grant_references_the_user -> Result<(), Box<dyn std::error::Error>>,
    storage_manager_authorizes_identity_ddl_and_preserves_a_login_admin -> Result<(), Box<dyn std::error::Error>>,
    login_reenable_racing_token_issue_only_publishes_after_enable -> Result<(), Box<dyn std::error::Error>>,
    identity_login_reenable_does_not_revive_revoked_credentials -> Result<(), Box<dyn std::error::Error>>,
    concurrent_user_and_role_creation_share_one_name_namespace -> Result<(), Box<dyn std::error::Error>>,
    identity_rename_preserves_the_shared_user_role_name_namespace -> Result<(), Box<dyn std::error::Error>>,
    login_disable_racing_token_admission_linearizes_before_or_after_start -> Result<(), Box<dyn std::error::Error>>,
    identity_ddl_cannot_disable_the_last_credentialed_admin -> Result<(), Box<dyn std::error::Error>>,
});

mod suite_16;
forward_tests!(suite_16 => {
    storage_manager_authorizes_object_grants_and_linearizes_revocation_races -> Result<(), Box<dyn std::error::Error>>,
    object_grant_batch_racing_manage_grants_revocation_is_atomic -> Result<(), Box<dyn std::error::Error>>,
    role_membership_revocation_aborts_a_staged_role_authorized_writer -> Result<(), Box<dyn std::error::Error>>,
});

mod suite_17;
forward_tests!(suite_17 => {
    storage_manager_authorizes_policy_ddl_for_owners_and_inherited_manage_policies -> Result<(), Box<dyn std::error::Error>>,
    storage_manager_separates_foreign_key_alter_and_references_authority -> Result<(), Box<dyn std::error::Error>>,
    storage_manager_rejects_unique_index_backfill_with_duplicate_values -> Result<(), Box<dyn std::error::Error>>,
    catalog_index_backfill_limit_fails_without_publishing_catalog -> Result<(), Box<dyn std::error::Error>>,
    storage_manager_add_column_row_limit_aborts_atomically -> Result<(), Box<dyn std::error::Error>>,
    storage_manager_drop_column_row_limit_aborts_atomically -> Result<(), Box<dyn std::error::Error>>,
});

mod suite_18;
forward_tests!(suite_18 => {
    storage_manager_drop_column_byte_limit_aborts_atomically -> Result<(), Box<dyn std::error::Error>>,
    storage_manager_add_column_byte_limit_aborts_atomically -> Result<(), Box<dyn std::error::Error>>,
    catalog_commit_rejects_removing_a_mapped_builtin_type -> Result<(), Box<dyn std::error::Error>>,
    catalog_commit_rejects_unassigned_builtin_type_profiles -> Result<(), Box<dyn std::error::Error>>,
    catalog_commit_rejects_duplicate_builtin_type_kinds -> Result<(), Box<dyn std::error::Error>>,
    catalog_commit_rejects_removing_the_bootstrap_descriptor -> Result<(), Box<dyn std::error::Error>>,
    catalog_commit_rejects_renaming_the_mapped_admin_role -> Result<(), Box<dyn std::error::Error>>,
    catalog_commit_requires_typed_rows_for_every_object -> Result<(), Box<dyn std::error::Error>>,
    catalog_commit_rejects_malformed_credential_verifier_profiles -> Result<(), Box<dyn std::error::Error>>,
    token_authenticated_catalog_scans_redact_cells_per_principal -> Result<(), Box<dyn std::error::Error>>,
});

mod suite_19;
forward_tests!(suite_19 => {
    token_admission_checks_expiry_secret_revocation_and_commit_guards -> Result<(), Box<dyn std::error::Error>>,
    client_session_authenticates_and_revalidates_before_operations -> Result<(), Box<dyn std::error::Error>>,
    client_handshake_requires_hello_first_and_sequences_responses -> Result<(), Box<dyn std::error::Error>>,
    control_cancel_over_tcp_reports_too_late_during_durable_commit -> Result<(), Box<dyn std::error::Error>>,
    gateway_ipc_status_and_cancel_keep_unknown_ids_non_enumerating -> Result<(), Box<dyn std::error::Error>>,
    gateway_control_socket_serves_bounded_status_requests -> Result<(), Box<dyn std::error::Error>>,
});

mod suite_20;
forward_tests!(suite_20 => {
    gateway_service_listener_authenticates_control_admin_over_socket -> Result<(), Box<dyn std::error::Error>>,
    gateway_disconnect_after_storage_commit_is_reconcilable -> Result<(), Box<dyn std::error::Error>>,
    process_exit_after_durable_commit_preserves_reconcilable_outcome -> Result<(), Box<dyn std::error::Error>>,
    process_exit_after_atomic_row_index_commit_preserves_publication -> Result<(), Box<dyn std::error::Error>>,
    process_crash_during_authorized_index_changes_recovers_all_or_none -> Result<(), Box<dyn std::error::Error>>,
    process_crash_during_table_rename_recovers_all_or_none -> Result<(), Box<dyn std::error::Error>>,
    process_crash_during_schema_create_or_drop_recovers_all_or_none -> Result<(), Box<dyn std::error::Error>>,
});

mod suite_21;
forward_tests!(suite_21 => {
    process_crash_during_schema_owner_transfer_recovers_all_or_none -> Result<(), Box<dyn std::error::Error>>,
    process_crash_during_table_owner_transfer_recovers_all_or_none -> Result<(), Box<dyn std::error::Error>>,
    process_crash_during_drop_column_recovers_all_or_none -> Result<(), Box<dyn std::error::Error>>,
    process_crash_during_add_column_recovers_all_or_none -> Result<(), Box<dyn std::error::Error>>,
    process_crash_during_drop_table_recovers_all_or_none -> Result<(), Box<dyn std::error::Error>>,
    process_crash_during_create_table_recovers_all_or_none -> Result<(), Box<dyn std::error::Error>>,
});

mod suite_22;
forward_tests!(suite_22 => {
    process_crash_during_unique_constraint_creation_recovers_all_or_none -> Result<(), Box<dyn std::error::Error>>,
    process_crash_during_check_constraint_creation_recovers_all_or_none -> Result<(), Box<dyn std::error::Error>>,
    process_crash_during_check_constraint_drop_recovers_all_or_none -> Result<(), Box<dyn std::error::Error>>,
    process_crash_during_column_default_change_recovers_all_or_none -> Result<(), Box<dyn std::error::Error>>,
    process_crash_during_not_null_change_recovers_all_or_none -> Result<(), Box<dyn std::error::Error>>,
    process_crash_during_primary_key_creation_recovers_all_or_none -> Result<(), Box<dyn std::error::Error>>,
});

mod suite_23;
forward_tests!(suite_23 => {
    process_crash_during_key_constraint_drop_recovers_all_or_none -> Result<(), Box<dyn std::error::Error>>,
    process_exit_after_rejected_table_drop_preserves_dependencies -> Result<(), Box<dyn std::error::Error>>,
    process_crash_during_foreign_key_creation_recovers_all_or_none -> Result<(), Box<dyn std::error::Error>>,
    process_crash_during_foreign_key_drop_recovers_all_or_none -> Result<(), Box<dyn std::error::Error>>,
    supervisor_drains_both_worker_channels_before_durable_finalize -> Result<(), Box<dyn std::error::Error>>,
    supervisor_worker_exit_failure_is_reaped_and_durably_aborted -> Result<(), Box<dyn std::error::Error>>,
    sandboxed_v3_sql_error_latches_failure_and_durably_aborts_after_partial_output -> Result<(), Box<dyn std::error::Error>>,
    sandboxed_v3_shared_sql_library_resolves_loads_and_durably_aborts_on_sql_error -> Result<(), Box<dyn std::error::Error>>,
    sandboxed_repository_sdk_library_loads_and_stays_pinned_until_worker_cleanup -> Result<(), Box<dyn std::error::Error>>,
    sandboxed_shared_sql_missing_dependency_aborts_inside_executor -> Result<(), Box<dyn std::error::Error>>,
    sandboxed_v3_constructor_cannot_connect_to_host_loopback -> Result<(), Box<dyn std::error::Error>>,
    sandboxed_direct_executor_ipc_rejects_forged_relation_handle -> Result<(), Box<dyn std::error::Error>>,
    sandboxed_direct_executor_ipc_rejects_forged_row_handle_mutation -> Result<(), Box<dyn std::error::Error>>,
});

mod suite_24;
forward_tests!(suite_24 => {
    sandboxed_optimizer_metadata_submission_cancels_over_tcp -> Result<(), Box<dyn std::error::Error>>,
});

mod suite_25;
forward_tests!(suite_25 => {
    sandboxed_v3_sql_dml_sequence_is_visible_and_commits -> Result<(), Box<dyn std::error::Error>>,
});

mod suite_26;
forward_tests!(suite_26 => {
    supervisor_cancellation_and_deadline_during_worker_wait_abort -> Result<(), Box<dyn std::error::Error>>,
    supervisor_cancellation_cleans_descendants_from_worker_cgroup -> Result<(), Box<dyn std::error::Error>>,
    cancellation_during_optimizer_predicate_estimate_kills_process_tree_without_fallback -> Result<(), Box<dyn std::error::Error>>,
    deadline_during_optimizer_predicate_estimate_kills_process_tree_without_fallback -> Result<(), Box<dyn std::error::Error>>,
    sealed_start_admits_only_current_credentials_and_enforces_request_deadline -> Result<(), Box<dyn std::error::Error>>,
    storage_manager_issues_and_revokes_tokens_for_manage_users -> Result<(), Box<dyn std::error::Error>>,
    catalog_commit_requires_principal_object_identity_and_kind -> Result<(), Box<dyn std::error::Error>>,
    catalog_commit_requires_matching_column_and_principal_names -> Result<(), Box<dyn std::error::Error>>,
    catalog_commit_rejects_duplicate_user_and_role_names -> Result<(), Box<dyn std::error::Error>>,
    catalog_commit_checks_principal_name_uniqueness_in_the_final_view -> Result<(), Box<dyn std::error::Error>>,
    catalog_commit_requires_schema_object_identity_and_kind -> Result<(), Box<dyn std::error::Error>>,
    catalog_commit_rejects_database_identity_as_schema_object -> Result<(), Box<dyn std::error::Error>>,
    policy_target_limit_checks_final_set_and_allows_replacements -> Result<(), Box<dyn std::error::Error>>,
    policy_column_targets_require_matching_cell_policy_columns -> Result<(), Box<dyn std::error::Error>>,
    membership_limit_applies_to_final_graph_independent_of_write_order -> Result<(), Box<dyn std::error::Error>>,
});

mod suite_27;
forward_tests!(suite_27 => {
    membership_catalog_commits_reject_cycles_and_record_aborted_outcomes -> Result<(), Box<dyn std::error::Error>>,
    catalog_commit_requires_exact_membership_authority_edges -> Result<(), Box<dyn std::error::Error>>,
    database_scoped_authority_grants_use_the_bootstrap_database_id -> Result<(), Box<dyn std::error::Error>>,
    concurrent_membership_commits_cannot_publish_a_cycle -> Result<(), Box<dyn std::error::Error>>,
    catalog_audit_metadata_and_outcome_commit_together -> Result<(), Box<dyn std::error::Error>>,
    one_redb_commit_can_publish_rows_indexes_catalog_audit_and_outcome -> Result<(), Box<dyn std::error::Error>>,
    oversized_index_key_is_rejected_before_publication -> Result<(), Box<dyn std::error::Error>>,
    row_updates_refresh_retained_index_locator_versions -> Result<(), Box<dyn std::error::Error>>,
    row_and_index_locators_publish_and_remove_atomically -> Result<(), Box<dyn std::error::Error>>,
    serialized_unique_index_check_rejects_duplicates_and_exempts_null_tuples -> Result<(), Box<dyn std::error::Error>>,
    foreign_key_commit_checks_child_and_parent_changes_against_final_rows -> Result<(), Box<dyn std::error::Error>>,
    foreign_key_row_visit_limit_aborts_the_whole_commit -> Result<(), Box<dyn std::error::Error>>,
    foreign_key_comparison_key_budget_aborts_large_keys_atomically -> Result<(), Box<dyn std::error::Error>>,
    dependency_graph_scan_limit_aborts_catalog_publication_atomically -> Result<(), Box<dyn std::error::Error>>,
    concurrent_foreign_key_insert_and_parent_delete_cannot_both_commit -> Result<(), Box<dyn std::error::Error>>,
    multirow_commit_assigns_one_sequence_and_conflict_aborts_the_whole_set -> Result<(), Box<dyn std::error::Error>>,
});

mod suite_28;
forward_tests!(suite_28 => {
    planner_stream_estimate_allowance_falls_back_without_aborting_invocation -> Result<(), Box<dyn std::error::Error>>,
});

mod suite_29;
forward_tests!(suite_29 => {
    planner_stream_estimates_same_typed_decimal_column_predicate -> Result<(), Box<dyn std::error::Error>>,
    catalog_backed_executor_generates_uuid_identity_and_rejects_override_and_update -> Result<(), Box<dyn std::error::Error>>,
});
