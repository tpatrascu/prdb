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
    init_help_lists_required_bootstrap_inputs_cleanly -> (),
    maintenance_help_lists_offline_audit_archival -> (),
    maintenance_inspection_is_read_only_bounded_and_requires_offline_lock -> (),
    migration_inspection_validates_active_and_staged_images_without_writing_them -> (),
    migration_stage_discard_rejects_an_unrelated_verified_backup -> (),
    migration_inspection_reports_database_identity_mismatch -> (),
    maintenance_backup_is_private_validated_and_checksum_verified -> (),
    unsupported_catalog_versions_are_rejected_before_derived_index_rebuild -> (),
    pinned_redb_rejects_file_format_v2_read_only_without_changing_the_fixture -> (),
    pinned_redb_opens_and_reads_fixture_after_redb_v2_to_v3_upgrade -> (),
    pinned_redb_rejects_populated_prdb_v2_without_mutating_the_file -> (),
    populated_prdb_logical_records_survive_redb_v2_to_v3_upgrade -> (),
    maintenance_redb_v2_upgrade_backs_up_and_atomically_publishes_ready_instance -> (),
    maintenance_redb_physical_upgrade_refuses_a_live_instance_lock -> (),
    maintenance_redb_physical_upgrade_recovers_after_process_exit_at_exchange -> (),
    maintenance_redb_physical_upgrade_recovers_after_directory_sync_failure -> (),
    maintenance_redb_physical_upgrade_recovers_after_backup_directory_sync_failure -> (),
    maintenance_redb_physical_upgrade_rebuilds_stage_after_conversion_failure -> (),
    maintenance_redb_physical_upgrade_rebuilds_valid_stage_after_process_exit -> (),
    v1_source_validator_checks_legacy_ready_images_without_admitting_them -> (),
});

mod suite_02;
forward_tests!(suite_02 => {
    migration_apply_requires_complete_history_ack_and_publishes_validated_v2 -> (),
    migration_apply_resumes_a_fully_validated_v2_staging_image -> (),
    migration_apply_keeps_the_published_target_after_process_exit_at_exchange -> (),
    migration_apply_recovers_after_directory_sync_failure_at_exchange -> (),
    restore_requires_data_loss_ack_and_recovers_an_unreadable_newer_active_image -> (),
    restore_refuses_to_overwrite_an_unrelated_staging_image -> (),
    startup_uses_active_image_and_tolerates_only_known_private_generation_files -> (),
    staged_generation_publication_atomically_exchanges_names_and_retains_source -> (),
    staged_generation_publication_rejects_different_database_identity -> (),
    staged_generation_publication_requires_a_matching_verified_backup -> (),
    maintenance_verifies_pra1_archives_and_rejects_corruption -> Result<(), Box<dyn std::error::Error>>,
    init_delivers_and_persists_one_time_credential_before_ready -> (),
    init_resumes_interrupted_bootstrap_and_rotates_the_credential -> (),
    maintenance_compaction_requires_exclusive_instance_lock -> (),
    audit_archive_is_durable_before_events_are_removed_and_refuses_overwrite -> Result<(), Box<dyn std::error::Error>>,
    audit_archive_requires_the_exclusive_instance_lock_and_preserves_sentinel -> Result<(), Box<dyn std::error::Error>>,
    audit_archive_completes_pruning_from_a_matching_preexisting_archive -> Result<(), Box<dyn std::error::Error>>,
    init_refuses_to_overwrite_credential_files_before_creating_database -> (),
});
