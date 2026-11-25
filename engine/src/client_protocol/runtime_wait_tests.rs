use super::json::encode_fence_waits;
use super::{ClientAdminCommand, decode_client_admin_command};
use crate::ddl_fence::{DdlFenceWaitCategory, DdlFenceWaitStatus};

#[test]
fn runtime_list_versions_one_through_eight_decode_and_later_versions_reject() {
    for version in 1..=8 {
        let request = format!(
            "{{\"phase\":\"read\",\"operation_version\":{version},\"operation\":\"runtime.invocations.list\",\"arguments\":{{\"limit\":\"1\"}}}}"
        );
        assert!(matches!(
            decode_client_admin_command(request.as_bytes()).unwrap(),
            ClientAdminCommand::ListRuntimeInvocations {
                response_version,
                page_limit: 1,
                cursor: None,
            } if response_version == version
        ));
    }
    let unsupported = br#"{"phase":"read","operation_version":9,"operation":"runtime.invocations.list","arguments":{"limit":"1"}}"#;
    assert!(decode_client_admin_command(unsupported).is_err());
}

#[test]
fn sdk_library_list_is_a_strict_authenticated_read_command() {
    let request = br#"{"phase":"read","operation_version":1,"operation":"sdk_libraries.list","arguments":{"limit":"4"}}"#;
    assert_eq!(
        decode_client_admin_command(request).unwrap(),
        ClientAdminCommand::ListSdkLibrariesPage {
            page_limit: 4,
            cursor: None,
        }
    );
    let unexpected_argument = br#"{"phase":"read","operation_version":1,"operation":"sdk_libraries.list","arguments":{"limit":"4","path":"/tmp"}}"#;
    assert!(decode_client_admin_command(unexpected_argument).is_err());
    let unsupported_version = br#"{"phase":"read","operation_version":2,"operation":"sdk_libraries.list","arguments":{"limit":"4"}}"#;
    assert!(decode_client_admin_command(unsupported_version).is_err());
}

#[test]
fn sdk_library_remove_is_a_typed_write_command() {
    let request = br#"{"phase":"write","operation_version":1,"operation":"sdk_libraries.remove","arguments":{"operation_id":"11111111111111111111111111111111","release":"7"}}"#;
    assert_eq!(
        decode_client_admin_command(request).unwrap(),
        ClientAdminCommand::RemoveSdkLibrary {
            operation_id: [0x11; 16],
            sdk_release: 7,
        }
    );
    let zero = br#"{"phase":"write","operation_version":1,"operation":"sdk_libraries.remove","arguments":{"operation_id":"11111111111111111111111111111111","release":"0"}}"#;
    assert!(decode_client_admin_command(zero).is_err());
    let unknown = br#"{"phase":"write","operation_version":1,"operation":"sdk_libraries.remove","arguments":{"operation_id":"11111111111111111111111111111111","release":"7","abi":"5"}}"#;
    assert!(decode_client_admin_command(unknown).is_err());
}

#[test]
fn sdk_library_install_prepare_and_finish_decode_strictly() {
    let begin = br#"{"phase":"prepare","operation_version":1,"operation":"sdk_libraries.install","arguments":{"operation_id":"11111111111111111111111111111111","metadata_bytes":"128","library_bytes":"4096","library_sha256":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"}}"#;
    assert_eq!(
        decode_client_admin_command(begin).unwrap(),
        ClientAdminCommand::BeginSdkLibraryInstall {
            operation_id: [0x11; 16],
            metadata_bytes: 128,
            library_bytes: 4096,
            library_sha256: [0xaa; 32],
        }
    );
    let finish = br#"{"phase":"write","operation_version":1,"operation":"sdk_libraries.install.finish","arguments":{"operation_id":"11111111111111111111111111111111"}}"#;
    assert_eq!(
        decode_client_admin_command(finish).unwrap(),
        ClientAdminCommand::FinishSdkLibraryInstall {
            operation_id: [0x11; 16],
        }
    );
    let excessive = br#"{"phase":"prepare","operation_version":1,"operation":"sdk_libraries.install","arguments":{"operation_id":"11111111111111111111111111111111","metadata_bytes":"128","library_bytes":"134217729","library_sha256":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"}}"#;
    assert!(decode_client_admin_command(excessive).is_err());
    let status = br#"{"phase":"read","operation_version":1,"operation":"sdk_libraries.operation_status","arguments":{"operation_id":"11111111111111111111111111111111"}}"#;
    assert_eq!(
        decode_client_admin_command(status).unwrap(),
        ClientAdminCommand::SdkLibraryOperationStatus {
            operation_id: [0x11; 16],
        }
    );
}

#[test]
fn runtime_wait_json_uses_opaque_ids_and_explicit_truncation_flags() {
    let wait = DdlFenceWaitStatus {
        wait_id: [1; 16],
        category: DdlFenceWaitCategory::Ddl,
        waited_us: 23,
        resource_ids: vec![[2; 16]],
        resources_truncated: true,
        blocker_ids: vec![[3; 16]],
        blockers_truncated: false,
    };
    let mut output = String::from("{\"invocations\":[");
    encode_fence_waits(
        &mut output,
        super::json::RuntimeInvocationSummary {
            pinned_snapshot_count: 2,
            oldest_snapshot_pin_age_us: 23,
            invocations_admitted_total: 7,
            invocation_conflicts_total: 1,
            admin_cancellations_accepted_total: 1,
            admin_cancellations_denied_total: 2,
            invocation_policy_denials_total: 3,
            invocation_staged_value_bytes_total: 8,
            invocation_commit_latency_buckets: [1, 2, 3, 4, 5, 6],
            invocation_redb_write_acquisition_buckets: [6, 5, 4, 3, 2, 1],
            include_write_lock_wait: true,
            include_outcome_maintenance: true,
            outcome_maintenance_passes_total: 7,
            outcome_maintenance_failures_total: 8,
            outcomes_pruned_total: 9,
            waits: &[wait],
            truncated: true,
        },
    );
    assert_eq!(
        output,
        format!(
            "{{\"invocations\":[],\"pinned_snapshot_count\":2,\"oldest_snapshot_pin_age_us\":23,\"invocations_admitted_total\":7,\"invocation_conflicts_total\":1,\"admin_cancellations_accepted_total\":1,\"admin_cancellations_denied_total\":2,\"invocation_policy_denials_total\":3,\"invocation_staged_value_bytes_total\":8,\"invocation_commit_latency_buckets\":[1,2,3,4,5,6],\"invocation_redb_write_acquisition_buckets\":[6,5,4,3,2,1],\"outcome_maintenance_passes_total\":7,\"outcome_maintenance_failures_total\":8,\"outcomes_pruned_total\":9,\"fence_waits\":[{{\"wait_id\":\"{}\",\"category\":\"ddl\",\"waited_us\":23 ,\"resource_ids\":[\"{}\"],\"resources_truncated\":true ,\"blocker_ids\":[\"{}\"],\"blockers_truncated\":false}}],\"fence_waits_truncated\":true",
            "01".repeat(16),
            "02".repeat(16),
            "03".repeat(16)
        )
    );
    let mut legacy_output = String::from("{\"invocations\":[");
    encode_fence_waits(
        &mut legacy_output,
        super::json::RuntimeInvocationSummary {
            pinned_snapshot_count: 0,
            oldest_snapshot_pin_age_us: 0,
            invocations_admitted_total: 0,
            invocation_conflicts_total: 0,
            admin_cancellations_accepted_total: 0,
            admin_cancellations_denied_total: 0,
            invocation_policy_denials_total: 0,
            invocation_staged_value_bytes_total: 0,
            invocation_commit_latency_buckets: [0; 6],
            invocation_redb_write_acquisition_buckets: [0; 6],
            include_write_lock_wait: false,
            include_outcome_maintenance: false,
            outcome_maintenance_passes_total: 9,
            outcome_maintenance_failures_total: 2,
            outcomes_pruned_total: 7,
            waits: &[],
            truncated: false,
        },
    );
    assert!(!legacy_output.contains("outcome_maintenance_passes_total"));
    assert!(legacy_output.contains("\"fence_waits\":[]"));
}
