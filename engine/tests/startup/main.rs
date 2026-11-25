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
    rejects_schema_search_path_entries_without_schema_records -> (),
    stale_principal_name_index_is_rebuilt_from_catalog -> (),
    missing_principal_name_index_is_rebuilt_from_catalog -> (),
    stale_schema_name_index_is_rebuilt_from_catalog -> (),
    missing_schema_name_index_is_rebuilt_from_catalog -> (),
    identity_tombstones_are_valid_and_reject_live_id_reuse -> Result<(), Box<dyn std::error::Error>>,
    database_identity_cannot_have_an_object_record -> (),
    validates_ready_image_records_and_references -> Result<(), Box<dyn std::error::Error>>,
    rejects_missing_membership_for_catalog_index -> (),
    rejects_index_key_that_does_not_match_its_row_tuple -> (),
    rejects_duplicate_non_null_keys_in_persisted_unique_indexes -> (),
    accepts_repeated_null_keys_in_persisted_unique_indexes -> Result<(), Box<dyn std::error::Error>>,
    rejects_ready_image_without_database_bootstrap_record -> (),
    rejects_bootstrap_versions_that_disagree_with_engine_metadata -> (),
    rejects_noncanonical_bootstrap_mappings -> (),
    accepts_credential_verifiers_with_the_supported_profile -> (),
    rejects_malformed_or_disabled_user_credential_verifiers -> (),
    rejects_unsupported_credential_algorithms_and_profile_versions -> (),
    mapped_builtin_ids_resolve_to_their_expected_typed_records -> Result<(), Box<dyn std::error::Error>>,
    rejects_objects_without_matching_typed_records -> Result<(), Box<dyn std::error::Error>>,
    rejects_builtin_type_records_with_unassigned_profiles -> Result<(), Box<dyn std::error::Error>>,
    rejects_dependency_edges_with_missing_catalog_objects -> (),
    validates_foreign_key_target_edges_against_constraint_records -> Result<(), Box<dyn std::error::Error>>,
    validates_backing_index_edges_against_constraint_and_index_records -> (),
    validates_expression_reference_edges_against_typed_catalog_records -> (),
    rejects_objects_with_missing_owner_principals -> (),
    accepts_owned_child_dependency_with_catalog_object_endpoints -> Result<(), Box<dyn std::error::Error>>,
    rejects_owned_child_edges_that_disagree_with_object_parents -> (),
    rejects_column_object_parent_that_disagrees_with_typed_table -> (),
    rejects_index_object_parent_that_disagrees_with_typed_table_schema -> (),
    read_only_startup_rejects_table_object_parent_that_disagrees_with_typed_schema -> (),
    rejects_owned_child_parent_cycles -> (),
    rejects_unknown_dependency_edge_kinds -> (),
    rejects_typed_catalog_records_without_a_matching_object_record -> (),
    rejects_typed_catalog_records_with_a_different_object_kind -> (),
    rejects_principal_records_with_missing_or_mismatched_objects -> (),
    rejects_column_and_principal_names_that_disagree_with_objects -> (),
    rejects_duplicate_names_across_users_and_roles -> (),
    rejects_row_versions_beyond_the_commit_frontier -> (),
    rejects_rows_with_invalid_or_mismatched_typed_payloads -> (),
    rejects_rows_without_a_catalog_table_schema -> (),
    rejects_zero_object_ids_in_row_keys -> (),
    rejects_index_without_its_catalog_definition -> (),
    rejects_index_without_its_referenced_row -> (),
    rejects_a_locator_whose_version_differs_from_its_row -> (),
    rejects_catalog_record_stored_under_a_different_key -> (),
    rejects_a_committed_outcome_beyond_the_commit_frontier -> (),
    initializing_database_is_not_ready_for_normal_startup -> (),
    rejects_outcome_key_value_mismatch -> (),
});

mod suite_02;
forward_tests!(suite_02 => {
    accepts_pinned_acyclic_membership_graph -> Result<(), Box<dyn std::error::Error>>,
    authority_dependencies_must_exactly_match_membership_records -> (),
    authority_dependencies_cover_grants_and_policy_recipients -> Result<(), Box<dyn std::error::Error>>,
    policy_column_targets_resolve_to_cell_policy_table_columns -> Result<(), Box<dyn std::error::Error>>,
    constraints_resolve_to_source_columns_on_their_table -> Result<(), Box<dyn std::error::Error>>,
    rejects_nullable_primary_key_columns_at_startup -> (),
    indexes_resolve_to_columns_on_their_table -> Result<(), Box<dyn std::error::Error>>,
    rejects_cycles_and_invalid_membership_endpoints_at_startup -> (),
});
