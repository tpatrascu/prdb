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
    admin_create_table_decoder_validates_constraints_and_check_expressions -> (),
    admin_create_table_v2_decoder_accepts_same_statement_foreign_key_target -> (),
    admin_client_lists_bounded_secret_free_token_pages -> (),
    admin_client_decodes_bounded_audit_pages_and_cursor -> (),
    admin_client_lists_administratively_visible_schema_pages -> (),
    admin_client_lists_sdk_libraries_with_compatibility_metadata -> (),
    admin_client_follows_opaque_sdk_library_page_cursors -> (),
    admin_client_sends_typed_sdk_library_remove_request -> (),
    admin_client_reads_owner_scoped_sdk_library_operation_status -> (),
    repository_profile_does_not_fall_back_when_peer_omits_capability -> (),
    admin_client_streams_sdk_library_install_over_control_chunks -> (),
    admin_client_lists_runtime_invocation_pages_with_best_effort_metadata -> (),
    admin_client_lists_logical_index_definitions_without_physical_details -> (),
    admin_client_lists_constraints_and_accepts_redacted_foreign_key_targets -> (),
    admin_client_lists_direct_table_grants_with_provenance -> (),
    admin_client_lists_secret_free_policy_summaries -> (),
    admin_client_lists_visible_dependency_edges -> (),
    submission_client_decodes_cancellation_acknowledgements -> (),
    admin_client_cancels_invocations_on_control_mode -> (),
    admin_client_sends_typed_index_prepare_start_and_status_requests -> (),
    admin_client_creates_index_from_qualified_sql_names -> (),
    admin_client_drops_index_from_schema_qualified_sql_name -> (),
    admin_client_creates_user_and_role_from_sql_names -> (),
    admin_client_returns_operation_id_when_start_reply_is_lost -> (),
    admin_client_sends_typed_user_and_role_lifecycle_requests -> (),
    admin_client_issues_one_time_token_and_revokes_tokens -> (),
    admin_client_grants_and_revokes_database_system_privileges -> (),
});

mod suite_02;
forward_tests!(suite_02 => {
    admin_client_creates_and_drops_schemas -> (),
    admin_client_executes_schema_rename_and_drop_sql_through_control_protocol -> (),
    admin_client_executes_table_rename_and_drop_sql_through_control_protocol -> (),
    admin_client_adds_key_constraint_from_qualified_sql_names -> (),
    admin_client_binds_foreign_key_sql_to_visible_columns_and_unique_target -> (),
    admin_client_drops_only_a_foreign_key_attached_to_the_selected_table -> (),
    admin_client_drops_primary_key_from_generated_constraint_sql -> (),
    admin_client_binds_check_sql_names_to_eir_dependencies -> (),
    admin_client_binds_nullability_sql_to_metadata_ids -> (),
    admin_client_binds_column_default_sql_to_metadata_type -> (),
    admin_client_adds_typed_column_from_sql_names -> (),
    admin_client_renames_column_from_qualified_sql_names -> (),
    admin_client_drops_column_from_qualified_sql_names -> (),
    admin_client_renames_and_drops_tables -> (),
    admin_client_creates_table_with_typed_columns_and_defaults -> (),
    admin_client_executes_create_table_sql_on_authenticated_control_channel -> (),
    admin_client_executes_self_referencing_create_table_sql_with_v2_payload -> (),
    admin_client_adds_renames_and_drops_columns -> (),
    admin_client_creates_table_with_foreign_key_in_one_typed_operation -> (),
});

mod suite_03;
forward_tests!(suite_03 => {
    admin_client_changes_table_owner_nullability_and_defaults -> (),
    admin_client_sends_typed_constraint_and_foreign_key_operations -> (),
    configured_sdk_library_install_and_remove_commands_use_typed_gateway_operations -> (),
});
