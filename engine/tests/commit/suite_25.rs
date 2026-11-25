use prdb_engine::audit::AuditAction;
use prdb_engine::catalog::{CatalogField, WireType, encode_envelope, family_schema};
use prdb_engine::commit::{CatalogWrite, RowWrite, commit_rows};
use prdb_engine::row_record::RowRecord;
use prdb_engine::schema::{CATALOG, ROWS};
use prdb_engine::storage_manager::StorageManager;
use prdb_engine::tuple::{ColumnSchema, FieldPatch, decode_tuple, encode_tuple};
use redb::ReadableDatabase;
use std::time::Duration;

use super::helpers::*;

#[cfg(all(target_os = "linux", target_arch = "x86_64"))]
pub(super) fn sandboxed_v3_sql_dml_sequence_is_visible_and_commits()
-> Result<(), Box<dyn std::error::Error>> {
    use prdb_engine::client_protocol::{ClientConnectionSession, ClientFrame, ClientMode};
    use prdb_engine::ipc::{Operation, read_frame};
    use prdb_engine::outcome::OutcomeStatus;
    use prdb_engine::storage_manager::{
        CreatePolicyRequest, CreateTableRequest, PolicyKind, StorageManagerConfig,
        TableColumnDefault, TableColumnDefinition,
    };
    use prdb_engine::submission_manifest::{
        ArtifactCompatibilityProfile, VersionedSubmissionMetadataValidator, encode_manifest_v1,
        manifest_v1_from_elf,
    };
    use prdb_engine::supervisor::{
        ChunkKind, PrepareSubmission, StagingLimits, StagingStore, SubmissionMetadata,
        SubmissionMetadataValidator, WorkloadClass,
    };
    use prdb_engine::supervisor_ipc::{
        GatewayAbortReason, GatewayAdmissionConfig, GatewayExecutorConfig, GatewayTerminalDecision,
        finalize_gateway_invocation_and_send_terminal, observe_executor_relay_and_finalize_gateway,
        send_gateway_result_data_event, send_gateway_terminal_for_confirmed_outcome,
    };
    use prdb_engine::tuple::{ScalarType, ScalarValue};
    use sha2::{Digest, Sha256};
    use std::os::unix::net::UnixStream;
    use std::path::PathBuf;
    use std::sync::Arc;
    use std::time::{SystemTime, UNIX_EPOCH};

    let count_bigint_only = std::env::var_os("PRDB_TEST_COUNT_BIGINT_ONLY").is_some();
    let indexed_join_asymmetric_benchmark_only =
        std::env::var_os("PRDB_TEST_INDEXED_JOIN_ASYMMETRIC_BENCHMARK_ONLY").is_some();
    let indexed_join_skew_benchmark_only =
        std::env::var_os("PRDB_TEST_INDEXED_JOIN_SKEW_BENCHMARK_ONLY").is_some();
    let indexed_join_fanout_four_benchmark_only =
        std::env::var_os("PRDB_TEST_INDEXED_JOIN_FANOUT4_BENCHMARK_ONLY").is_some();
    let indexed_join_fanout_five_benchmark_only =
        std::env::var_os("PRDB_TEST_INDEXED_JOIN_FANOUT5_BENCHMARK_ONLY").is_some();
    let indexed_join_fanout_two_benchmark_only =
        std::env::var_os("PRDB_TEST_INDEXED_JOIN_FANOUT2_BENCHMARK_ONLY").is_some();
    let indexed_join_fanout_three_benchmark_only =
        std::env::var_os("PRDB_TEST_INDEXED_JOIN_FANOUT3_BENCHMARK_ONLY").is_some();
    let indexed_join_fanout_nine_ratio8_benchmark_only =
        std::env::var_os("PRDB_TEST_INDEXED_JOIN_FANOUT9_RATIO8_BENCHMARK_ONLY").is_some();
    let indexed_join_fanout_sixteen_benchmark_only =
        std::env::var_os("PRDB_TEST_INDEXED_JOIN_FANOUT16_BENCHMARK_ONLY").is_some();
    let indexed_join_fanout_seventeen_benchmark_only =
        std::env::var_os("PRDB_TEST_INDEXED_JOIN_FANOUT17_BENCHMARK_ONLY").is_some();
    let indexed_join_fanout_eighteen_ratio7_benchmark_only =
        std::env::var_os("PRDB_TEST_INDEXED_JOIN_FANOUT18_RATIO7_BENCHMARK_ONLY").is_some();
    let indexed_join_fanout_eighteen_threshold_benchmark_only =
        std::env::var_os("PRDB_TEST_INDEXED_JOIN_FANOUT18_THRESHOLD_BENCHMARK_ONLY").is_some();
    let indexed_join_fanout_eighteen_over_threshold_benchmark_only =
        std::env::var_os("PRDB_TEST_INDEXED_JOIN_FANOUT18_OVER_THRESHOLD_BENCHMARK_ONLY").is_some();
    let indexed_join_fanout_nineteen_ratio7_benchmark_only =
        std::env::var_os("PRDB_TEST_INDEXED_JOIN_FANOUT19_RATIO7_BENCHMARK_ONLY").is_some();
    let indexed_join_fanout_nineteen_over_cutoff_benchmark_only =
        std::env::var_os("PRDB_TEST_INDEXED_JOIN_FANOUT19_OVER_CUTOFF_BENCHMARK_ONLY").is_some();
    let indexed_join_fanout_seventeen_ratio7_benchmark_only =
        std::env::var_os("PRDB_TEST_INDEXED_JOIN_FANOUT17_RATIO7_BENCHMARK_ONLY").is_some();
    let indexed_join_fanout_seventeen_threshold_benchmark_only =
        std::env::var_os("PRDB_TEST_INDEXED_JOIN_FANOUT17_THRESHOLD_BENCHMARK_ONLY").is_some();
    let indexed_join_fanout_seventeen_over_threshold_benchmark_only =
        std::env::var_os("PRDB_TEST_INDEXED_JOIN_FANOUT17_OVER_THRESHOLD_BENCHMARK_ONLY").is_some();
    let indexed_join_fanout_seventeen_large_inner_benchmark_only =
        std::env::var_os("PRDB_TEST_INDEXED_JOIN_FANOUT17_2048_BENCHMARK_ONLY").is_some();
    let indexed_join_fanout_seventeen_cap_inner_benchmark_only =
        std::env::var_os("PRDB_TEST_INDEXED_JOIN_FANOUT17_4096_BENCHMARK_ONLY").is_some();
    let indexed_join_fanout_seventeen_4097_benchmark_only =
        std::env::var_os("PRDB_TEST_INDEXED_JOIN_FANOUT17_4097_BENCHMARK_ONLY").is_some();
    let indexed_join_skewed_fanout_benchmark_only =
        std::env::var_os("PRDB_TEST_INDEXED_JOIN_SKEWED_FANOUT_BENCHMARK_ONLY").is_some();
    let indexed_join_skewed_high_fanout_benchmark_only =
        std::env::var_os("PRDB_TEST_INDEXED_JOIN_SKEWED_HIGH_FANOUT_BENCHMARK_ONLY").is_some();
    let indexed_join_fanout_benchmark_only =
        std::env::var_os("PRDB_TEST_INDEXED_JOIN_FANOUT_BENCHMARK_ONLY").is_some()
            || indexed_join_fanout_four_benchmark_only
            || indexed_join_fanout_five_benchmark_only
            || indexed_join_fanout_two_benchmark_only
            || indexed_join_fanout_three_benchmark_only
            || indexed_join_fanout_nine_ratio8_benchmark_only
            || indexed_join_fanout_sixteen_benchmark_only
            || indexed_join_fanout_seventeen_benchmark_only
            || indexed_join_fanout_eighteen_ratio7_benchmark_only
            || indexed_join_fanout_eighteen_threshold_benchmark_only
            || indexed_join_fanout_eighteen_over_threshold_benchmark_only
            || indexed_join_fanout_nineteen_ratio7_benchmark_only
            || indexed_join_fanout_nineteen_over_cutoff_benchmark_only
            || indexed_join_fanout_seventeen_ratio7_benchmark_only
            || indexed_join_fanout_seventeen_threshold_benchmark_only
            || indexed_join_fanout_seventeen_over_threshold_benchmark_only
            || indexed_join_fanout_seventeen_large_inner_benchmark_only
            || indexed_join_fanout_seventeen_cap_inner_benchmark_only
            || indexed_join_fanout_seventeen_4097_benchmark_only
            || indexed_join_skewed_fanout_benchmark_only
            || indexed_join_skewed_high_fanout_benchmark_only;
    let indexed_join_fanout_outer_rows: u16 = if indexed_join_fanout_nineteen_ratio7_benchmark_only
        || indexed_join_fanout_nineteen_over_cutoff_benchmark_only
    {
        19
    } else if indexed_join_fanout_eighteen_ratio7_benchmark_only
        || indexed_join_fanout_eighteen_threshold_benchmark_only
        || indexed_join_fanout_eighteen_over_threshold_benchmark_only
    {
        18
    } else if indexed_join_fanout_seventeen_benchmark_only
        || indexed_join_fanout_seventeen_ratio7_benchmark_only
        || indexed_join_fanout_seventeen_threshold_benchmark_only
        || indexed_join_fanout_seventeen_over_threshold_benchmark_only
        || indexed_join_fanout_seventeen_large_inner_benchmark_only
        || indexed_join_fanout_seventeen_cap_inner_benchmark_only
        || indexed_join_fanout_seventeen_4097_benchmark_only
        || indexed_join_skewed_fanout_benchmark_only
        || indexed_join_skewed_high_fanout_benchmark_only
    {
        17
    } else if indexed_join_fanout_sixteen_benchmark_only {
        16
    } else {
        9
    };
    let indexed_join_fanout: u16 = if indexed_join_fanout_eighteen_threshold_benchmark_only
        || indexed_join_fanout_eighteen_over_threshold_benchmark_only
        || indexed_join_fanout_seventeen_threshold_benchmark_only
        || indexed_join_fanout_seventeen_over_threshold_benchmark_only
        || indexed_join_fanout_nine_ratio8_benchmark_only
    {
        4
    } else if indexed_join_fanout_two_benchmark_only {
        2
    } else if indexed_join_fanout_three_benchmark_only {
        3
    } else if indexed_join_fanout_four_benchmark_only {
        4
    } else if indexed_join_fanout_five_benchmark_only
        || indexed_join_fanout_nineteen_over_cutoff_benchmark_only
    {
        5
    } else {
        1
    };
    assert!(
        [
            std::env::var_os("PRDB_TEST_INDEXED_JOIN_FANOUT_BENCHMARK_ONLY").is_some(),
            indexed_join_fanout_four_benchmark_only,
            indexed_join_fanout_five_benchmark_only,
            indexed_join_fanout_two_benchmark_only,
            indexed_join_fanout_three_benchmark_only,
            indexed_join_fanout_nine_ratio8_benchmark_only,
            indexed_join_fanout_sixteen_benchmark_only,
            indexed_join_fanout_seventeen_benchmark_only,
            indexed_join_fanout_eighteen_ratio7_benchmark_only,
            indexed_join_fanout_eighteen_threshold_benchmark_only,
            indexed_join_fanout_eighteen_over_threshold_benchmark_only,
            indexed_join_fanout_nineteen_ratio7_benchmark_only,
            indexed_join_fanout_nineteen_over_cutoff_benchmark_only,
            indexed_join_fanout_seventeen_ratio7_benchmark_only,
            indexed_join_fanout_seventeen_threshold_benchmark_only,
            indexed_join_fanout_seventeen_over_threshold_benchmark_only,
            indexed_join_fanout_seventeen_large_inner_benchmark_only,
            indexed_join_fanout_seventeen_cap_inner_benchmark_only,
            indexed_join_fanout_seventeen_4097_benchmark_only,
            indexed_join_skewed_fanout_benchmark_only,
            indexed_join_skewed_high_fanout_benchmark_only,
        ]
        .into_iter()
        .filter(|enabled| *enabled)
        .count()
            <= 1,
        "select exactly one indexed-join fanout benchmark mode"
    );
    let indexed_join_fanout_inner_rows: u16 = if indexed_join_fanout_nineteen_ratio7_benchmark_only
        || indexed_join_fanout_nineteen_over_cutoff_benchmark_only
    {
        133
    } else if indexed_join_fanout_nine_ratio8_benchmark_only {
        72
    } else if indexed_join_fanout_seventeen_cap_inner_benchmark_only {
        4096
    } else if indexed_join_fanout_seventeen_4097_benchmark_only {
        4097
    } else if indexed_join_fanout_seventeen_large_inner_benchmark_only {
        2048
    } else if indexed_join_fanout_eighteen_ratio7_benchmark_only
        || indexed_join_fanout_eighteen_threshold_benchmark_only
        || indexed_join_fanout_eighteen_over_threshold_benchmark_only
    {
        126
    } else if indexed_join_fanout_seventeen_ratio7_benchmark_only
        || indexed_join_fanout_seventeen_threshold_benchmark_only
        || indexed_join_fanout_seventeen_over_threshold_benchmark_only
    {
        119
    } else {
        128
    };
    let indexed_join_benchmark_only = indexed_join_asymmetric_benchmark_only
        || indexed_join_skew_benchmark_only
        || indexed_join_fanout_benchmark_only
        || std::env::var_os("PRDB_TEST_INDEXED_JOIN_BENCHMARK_ONLY").is_some();
    let spill_quota_only = std::env::var_os("PRDB_TEST_SPILL_QUOTA_ONLY").is_some();
    let spill_only = std::env::var_os("PRDB_TEST_SPILL_ONLY").is_some();
    let spill_limit_only = std::env::var_os("PRDB_TEST_SPILL_LIMIT_ONLY").is_some();
    let spill_disabled_only = std::env::var_os("PRDB_TEST_SPILL_DISABLED_ONLY").is_some();
    let spill_sort_only = std::env::var_os("PRDB_TEST_SPILL_SORT_ONLY").is_some();
    let spill_decimal_only = std::env::var_os("PRDB_TEST_SPILL_DECIMAL_ONLY").is_some();
    let spill_redacted_only = std::env::var_os("PRDB_TEST_SPILL_REDACTED_ONLY").is_some();
    let spill_join_only = std::env::var_os("PRDB_TEST_SPILL_JOIN_ONLY").is_some();
    let spill_join_decimal_only = std::env::var_os("PRDB_TEST_SPILL_JOIN_DECIMAL_ONLY").is_some();
    let spill_join_filtered_only = std::env::var_os("PRDB_TEST_SPILL_JOIN_FILTERED_ONLY").is_some();
    let spill_join_parameter_only =
        std::env::var_os("PRDB_TEST_SPILL_JOIN_PARAMETER_ONLY").is_some();
    let spill_join_bigint_only = std::env::var_os("PRDB_TEST_SPILL_JOIN_BIGINT_ONLY").is_some();
    let spill_join_oversize_only = std::env::var_os("PRDB_TEST_SPILL_JOIN_OVERSIZE_ONLY").is_some();
    let spill_join_text_only = std::env::var_os("PRDB_TEST_SPILL_JOIN_TEXT_ONLY").is_some();
    let spill_crash_only = std::env::var_os("PRDB_TEST_SPILL_CRASH_ONLY").is_some();
    let spill_pressure_only = std::env::var_os("PRDB_TEST_SPILL_PRESSURE_ONLY").is_some();
    let spill_pressure_cancel_only =
        std::env::var_os("PRDB_TEST_SPILL_PRESSURE_CANCEL_ONLY").is_some();
    let spill_disk_full_only = std::env::var_os("PRDB_TEST_SPILL_DISK_FULL_ONLY").is_some();
    let sql_in_indexed_only = std::env::var_os("PRDB_TEST_SQL_IN_ONLY").is_some();
    let sql_in_fallback_only = std::env::var_os("PRDB_TEST_SQL_IN_FALLBACK_ONLY").is_some();
    let sql_in_null_left_only = std::env::var_os("PRDB_TEST_SQL_IN_NULL_LEFT_ONLY").is_some();
    let sql_in_cancel_only = std::env::var_os("PRDB_TEST_SQL_IN_CANCEL_ONLY").is_some();
    let sql_in_denied_key_only = std::env::var_os("PRDB_TEST_SQL_IN_DENIED_KEY_ONLY").is_some();
    let sql_in_over_budget_only =
        std::env::var_os("PRDB_TEST_SQL_IN_OVER_BUDGET_FALLBACK_ONLY").is_some();
    let sql_in_elevated_budget_only =
        std::env::var_os("PRDB_TEST_SQL_IN_ELEVATED_BUDGET_ONLY").is_some();
    let elevated_result_only = std::env::var_os("PRDB_TEST_ELEVATED_RESULT_ONLY").is_some();
    let sql_in_only = sql_in_indexed_only
        || sql_in_fallback_only
        || sql_in_null_left_only
        || sql_in_over_budget_only
        || sql_in_elevated_budget_only;
    let sql_in_any_only = sql_in_only || sql_in_cancel_only || sql_in_denied_key_only;
    assert!(
        [
            sql_in_indexed_only,
            sql_in_fallback_only,
            sql_in_null_left_only,
            sql_in_cancel_only,
            sql_in_denied_key_only,
            sql_in_over_budget_only,
            sql_in_elevated_budget_only,
            elevated_result_only,
        ]
        .into_iter()
        .filter(|enabled| *enabled)
        .count()
            <= 1,
        "select only one SQL IN or elevated-result acceptance mode"
    );
    let spill_enabled_only = spill_quota_only
        || spill_only
        || spill_limit_only
        || spill_sort_only
        || spill_decimal_only
        || spill_redacted_only
        || spill_join_only
        || spill_join_decimal_only
        || spill_join_filtered_only
        || spill_join_parameter_only
        || spill_join_bigint_only
        || spill_join_oversize_only
        || spill_join_text_only
        || spill_crash_only
        || spill_pressure_only
        || spill_pressure_cancel_only
        || spill_disk_full_only;
    let spill_query_mode = spill_enabled_only || spill_disabled_only;

    struct AcceptFixture {
        requested_scope: Vec<u8>,
        inner: Arc<VersionedSubmissionMetadataValidator>,
    }
    impl SubmissionMetadataValidator for AcceptFixture {
        fn supports_versions(&self, manifest_version: u32, scope_version: u32) -> bool {
            self.inner
                .supports_versions(manifest_version, scope_version)
        }

        fn validate(&self, metadata: &SubmissionMetadata) -> bool {
            self.inner.validate(metadata) && metadata.requested_scope == self.requested_scope
        }

        fn validate_artifact(&self, metadata: &SubmissionMetadata, artifact: &[u8]) -> bool {
            self.inner.validate_artifact(metadata, artifact)
                && metadata.requested_scope == self.requested_scope
        }

        fn resolve_sdk_release(
            &self,
            metadata: &SubmissionMetadata,
            artifact: &[u8],
        ) -> Result<
            Option<prdb_engine::sdk_registry::SdkLibraryPin>,
            prdb_engine::submission_manifest::ManifestError,
        > {
            self.inner.resolve_sdk_release(metadata, artifact)
        }
    }

    fn required_paths(name: &str) -> Result<Vec<PathBuf>, Box<dyn std::error::Error>> {
        let value = std::env::var_os(name)
            .ok_or_else(|| format!("{name} must list the sandbox runtime paths"))?;
        if name == "PRDB_TEST_RUNTIME_READ_ONLY" && value.is_empty() {
            return Ok(Vec::new());
        }
        let paths = std::env::split_paths(&value).collect::<Vec<_>>();
        if paths.is_empty() || paths.iter().any(|path| !path.is_absolute()) {
            return Err(format!("{name} must contain absolute paths").into());
        }
        Ok(paths)
    }

    fn worker_cgroup_memory(pid: u32) -> std::io::Result<(u64, u64)> {
        let membership = std::fs::read_to_string(format!("/proc/{pid}/cgroup"))?;
        let relative_path = membership
            .lines()
            .find_map(|line| line.strip_prefix("0::"))
            .ok_or_else(|| {
                std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    "missing cgroup v2 membership",
                )
            })?;
        let relative_path = relative_path.strip_prefix('/').ok_or_else(|| {
            std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "cgroup path is not absolute",
            )
        })?;
        let cgroup = PathBuf::from("/sys/fs/cgroup").join(relative_path);
        let read_counter = |name: &str| -> std::io::Result<u64> {
            std::fs::read_to_string(cgroup.join(name))?
                .trim()
                .parse()
                .map_err(|error| std::io::Error::new(std::io::ErrorKind::InvalidData, error))
        };
        Ok((
            read_counter("memory.current")?,
            read_counter("memory.peak")?,
        ))
    }

    let cgroup_parent = std::env::var_os("PRDB_TEST_CGROUP_PARENT")
        .map(PathBuf::from)
        .ok_or("PRDB_TEST_CGROUP_PARENT must name a delegated cgroup v2 parent")?;
    let artifact_path = if spill_query_mode {
        std::env::var_os("PRDB_TEST_SPILL_QUERY_ARTIFACT")
            .map(PathBuf::from)
            .ok_or("PRDB_TEST_SPILL_QUERY_ARTIFACT must name sql-v6-spill.so")?
    } else {
        std::env::var_os("PRDB_TEST_WRITE_READ_QUERY_ARTIFACT")
            .map(PathBuf::from)
            .ok_or("PRDB_TEST_WRITE_READ_QUERY_ARTIFACT must name sql-v3-write-read.so")?
    };
    let restricted_artifact_path = std::env::var_os("PRDB_TEST_RESTRICTED_METADATA_QUERY_ARTIFACT")
        .map(PathBuf::from)
        .ok_or(
            "PRDB_TEST_RESTRICTED_METADATA_QUERY_ARTIFACT must name sql-v3-restricted-metadata.so",
        )?;
    if !cgroup_parent.is_absolute()
        || !artifact_path.is_absolute()
        || !restricted_artifact_path.is_absolute()
    {
        return Err("sandbox test paths must be absolute".into());
    }
    let artifact = std::fs::read(&artifact_path)?;
    let restricted_artifact = std::fs::read(&restricted_artifact_path)?;
    let fixture = Fixture::new();
    let database = initialize(&fixture.path())?;
    let sdk_release_dir = std::env::var_os("PRDB_TEST_INSTALLED_SDK_RELEASE_DIR")
        .map(PathBuf::from)
        .ok_or("PRDB_TEST_INSTALLED_SDK_RELEASE_DIR must name the installed SDK")?;
    let sdk_repository = Arc::new(
        prdb_engine::sdk_registry::SdkLibraryRepository::open(
            &fixture.0.join("sdk-libraries"),
            "linux-x86_64".to_owned(),
            "x86_64-v1".to_owned(),
            "nixos-x86_64-glibc-2.42".to_owned(),
            ["libc.so.6".to_owned()].into_iter().collect(),
            128 * 1024 * 1024,
        )
        .map_err(|error| format!("open SQL SDK repository: {error:?}"))?,
    );
    sdk_repository
        .install(
            &std::fs::read(sdk_release_dir.join("sdk-release.json"))?,
            &std::fs::read(sdk_release_dir.join("libprdb_sql.so.1"))?,
            &["libc.so.6".to_owned()].into_iter().collect(),
        )
        .map_err(|error| format!("install SQL SDK repository release: {error:?}"))?;
    let manifest = encode_manifest_v1(
        &manifest_v1_from_elf(
            &artifact,
            if spill_enabled_only || spill_disabled_only {
                6
            } else {
                4
            },
            1,
            "x86_64-v1".to_owned(),
            "nixos-x86_64-glibc-2.42".to_owned(),
            if spill_query_mode {
                "TinyCC ABI v6 spill fixture".to_owned()
            } else {
                "TinyCC DML integration fixture".to_owned()
            },
        )
        .map_err(|error| format!("derive DML manifest: {error:?}"))?,
    )
    .map_err(|error| format!("encode DML manifest: {error:?}"))?;
    let profile = ArtifactCompatibilityProfile::for_repository(
        "x86_64-v1".to_owned(),
        "nixos-x86_64-glibc-2.42".to_owned(),
        ["libc.so.6".to_owned()].into_iter().collect(),
    )
    .map_err(|error| format!("build DML compatibility profile: {error:?}"))?;
    let base_validator = Arc::new(
        VersionedSubmissionMetadataValidator::new(profile)
            .with_sdk_registry(Arc::clone(&sdk_repository))
            .map_err(|error| format!("configure DML SDK repository: {error:?}"))?,
    );
    let validator_for_scope = |requested_scope| -> Arc<dyn SubmissionMetadataValidator> {
        Arc::new(AcceptFixture {
            requested_scope,
            inner: Arc::clone(&base_validator),
        })
    };
    let now = i64::try_from(SystemTime::now().duration_since(UNIX_EPOCH)?.as_micros())?;
    let user_id = id(0x7a);
    let token_id = id(0x7b);
    let secret = [0x42; 32];
    let restricted_user_id = id(0xb0);
    let restricted_token_id = id(0xb1);
    let restricted_secret = [0x43; 32];
    let admin_role = builtin_owner_id();
    let mut setup_audit = identity_audit_event(0x7c, admin_role, user_id);
    setup_audit.action = AuditAction::AuthorityChange;
    setup_audit.correlation_id = id(0x7c);
    let setup_invocation = id(0x7c);
    StorageManager::new(&database, Duration::from_millis(100))?.commit_catalog_and_audit(
        setup_invocation,
        admin_role,
        now + 3_600_000_000,
        &[
            principal_write_with_name(user_id, 1, 1, "sandbox_sql_roundtrip"),
            object_write_with_name(user_id, 8, "sandbox_sql_roundtrip"),
            credential_verifier_write(
                token_id,
                user_id,
                &secret,
                now - 1_000_000,
                now + 60_000_000,
                None,
                1,
            ),
            principal_write_with_name(restricted_user_id, 1, 1, "sandbox_sql_restricted"),
            object_write_with_name(restricted_user_id, 8, "sandbox_sql_restricted"),
            credential_verifier_write(
                restricted_token_id,
                restricted_user_id,
                &restricted_secret,
                now - 1_000_000,
                now + 60_000_000,
                None,
                1,
            ),
            membership_write(user_id, admin_role),
            dependency_write(user_id, admin_role, 5),
        ],
        &[setup_audit],
    )?;
    let storage = StorageManager::with_config(
        &database,
        StorageManagerConfig {
            max_active_analytical_invocations: 8,
            max_invocation_scan_work: prdb_engine::transaction::MAX_INVOCATION_SCAN_WORK,
            max_authorized_index_range_candidates: 128,
            max_analytical_authorized_index_range_candidates: if sql_in_elevated_budget_only {
                Some(2_048)
            } else if indexed_join_benchmark_only {
                Some(512)
            } else {
                None
            },
            max_authorized_index_range_bytes: 16 * 1024 * 1024,
            max_analytical_authorized_index_range_bytes: sql_in_elevated_budget_only
                .then_some(32 * 1024 * 1024),
            max_analytical_invocation_scan_work: indexed_join_benchmark_only.then_some(1_048_576),
            max_planner_estimate_requests: if sql_in_elevated_budget_only { 1 } else { 16 },
            max_analytical_planner_estimate_requests: sql_in_elevated_budget_only.then_some(64),
            max_planner_estimate_candidates: if sql_in_elevated_budget_only {
                1
            } else {
                prdb_engine::statistics::DEFAULT_ESTIMATE_CANDIDATES
            },
            max_analytical_planner_estimate_candidates: sql_in_elevated_budget_only
                .then_some(2_048),
            ..StorageManagerConfig::default()
        },
    )?;
    let schema_invocation = id(0x7d);
    let mut schema_audit = identity_audit_event(0x7e, user_id, id(0x7f));
    schema_audit.correlation_id = schema_invocation;
    let schema = storage.create_schema(
        schema_invocation,
        user_id,
        "sql_roundtrip",
        user_id,
        now + 3_600_000_000,
        schema_audit,
    )?;
    let empty_schema_invocation = id(0xa2);
    let mut empty_schema_audit = identity_audit_event(0xa3, user_id, id(0xa4));
    empty_schema_audit.correlation_id = empty_schema_invocation;
    let empty_schema = storage.create_schema(
        empty_schema_invocation,
        user_id,
        "empty_roundtrip",
        user_id,
        now + 3_600_000_000,
        empty_schema_audit,
    )?;
    let hidden_schema_invocation = id(0xa5);
    let mut hidden_schema_audit = identity_audit_event(0xa6, user_id, id(0xa7));
    hidden_schema_audit.correlation_id = hidden_schema_invocation;
    let hidden_schema = storage.create_schema(
        hidden_schema_invocation,
        user_id,
        "hidden_roundtrip",
        user_id,
        now + 3_600_000_000,
        hidden_schema_audit,
    )?;
    let table_invocation = id(0x80);
    let mut table_audit = identity_audit_event(0x81, user_id, id(0x82));
    table_audit.correlation_id = table_invocation;
    let records_table = storage.create_table(CreateTableRequest {
        invocation_id: table_invocation,
        authenticated_actor_id: user_id,
        schema_id: schema.schema_id,
        name: "records".to_owned(),
        columns: vec![TableColumnDefinition {
            name: "id".to_owned(),
            scalar_type: ScalarType::Integer,
            nullable: true,
            generated_uuid_identity: false,
            default: None,
        }],
        retention_deadline_us: now + 3_600_000_000,
        audit_event: table_audit,
    })?;
    let identity_table_invocation = id(0xe0);
    let mut identity_table_audit = identity_audit_event(0xe0, user_id, schema.schema_id);
    identity_table_audit.correlation_id = identity_table_invocation;
    let identity_table = storage.create_table(CreateTableRequest {
        invocation_id: identity_table_invocation,
        authenticated_actor_id: user_id,
        schema_id: schema.schema_id,
        name: "identity_records".to_owned(),
        columns: vec![
            TableColumnDefinition {
                name: "id".to_owned(),
                scalar_type: ScalarType::Uuid,
                nullable: false,
                generated_uuid_identity: true,
                default: None,
            },
            TableColumnDefinition {
                name: "label".to_owned(),
                scalar_type: ScalarType::Text,
                nullable: false,
                generated_uuid_identity: false,
                default: Some(TableColumnDefault::Literal(Some(ScalarValue::Text(
                    "generated-label".to_owned(),
                )))),
            },
        ],
        retention_deadline_us: now + 3_600_000_000,
        audit_event: identity_table_audit,
    })?;
    let joined_table_invocation = id(0xc1);
    let mut joined_table_audit = identity_audit_event(0xc2, user_id, id(0xc3));
    joined_table_audit.correlation_id = joined_table_invocation;
    let joined_table = storage.create_table(CreateTableRequest {
        invocation_id: joined_table_invocation,
        authenticated_actor_id: user_id,
        schema_id: schema.schema_id,
        name: "linked_records".to_owned(),
        columns: vec![
            TableColumnDefinition {
                name: "id".to_owned(),
                scalar_type: ScalarType::Integer,
                nullable: true,
                generated_uuid_identity: false,
                default: None,
            },
            TableColumnDefinition {
                name: "match_key".to_owned(),
                scalar_type: ScalarType::Integer,
                nullable: true,
                generated_uuid_identity: false,
                default: None,
            },
        ],
        retention_deadline_us: now + 3_600_000_000,
        audit_event: joined_table_audit,
    })?;
    let composite_table_invocation = id(0xd0);
    let mut composite_table_audit = identity_audit_event(0xd1, user_id, schema.schema_id);
    composite_table_audit.correlation_id = composite_table_invocation;
    let mut composite_table_columns = vec![
        TableColumnDefinition {
            name: "left_key".to_owned(),
            scalar_type: ScalarType::Integer,
            nullable: sql_in_null_left_only,
            generated_uuid_identity: false,
            default: None,
        },
        TableColumnDefinition {
            name: "right_key".to_owned(),
            scalar_type: ScalarType::Integer,
            nullable: false,
            generated_uuid_identity: false,
            default: None,
        },
    ];
    if sql_in_elevated_budget_only {
        composite_table_columns.push(TableColumnDefinition {
            name: "large_payload".to_owned(),
            scalar_type: ScalarType::Text,
            nullable: false,
            generated_uuid_identity: false,
            default: None,
        });
    }
    let composite_table = storage.create_table(CreateTableRequest {
        invocation_id: composite_table_invocation,
        authenticated_actor_id: user_id,
        schema_id: schema.schema_id,
        name: "grouped_records".to_owned(),
        columns: composite_table_columns,
        retention_deadline_us: now + 3_600_000_000,
        audit_event: composite_table_audit,
    })?;
    let decimal_table_invocation = id(0xf0);
    let mut decimal_table_audit = identity_audit_event(0xf1, user_id, schema.schema_id);
    decimal_table_audit.correlation_id = decimal_table_invocation;
    let mut decimal_columns = vec![
        TableColumnDefinition {
            name: "left_key".to_owned(),
            scalar_type: ScalarType::Integer,
            nullable: spill_query_mode,
            generated_uuid_identity: false,
            default: None,
        },
        TableColumnDefinition {
            name: "right_key".to_owned(),
            scalar_type: ScalarType::Integer,
            nullable: false,
            generated_uuid_identity: false,
            default: None,
        },
        TableColumnDefinition {
            name: "decimal_value".to_owned(),
            scalar_type: ScalarType::Decimal {
                precision: 4,
                scale: 2,
            },
            nullable: true,
            generated_uuid_identity: false,
            default: None,
        },
    ];
    if count_bigint_only {
        decimal_columns.push(TableColumnDefinition {
            name: "bigint_value".to_owned(),
            scalar_type: ScalarType::BigInt,
            nullable: true,
            generated_uuid_identity: false,
            default: None,
        });
    }
    if spill_join_oversize_only || spill_join_text_only || elevated_result_only {
        decimal_columns.push(TableColumnDefinition {
            name: "large_payload".to_owned(),
            scalar_type: ScalarType::Text,
            nullable: true,
            generated_uuid_identity: false,
            default: None,
        });
    }
    let decimal_table = storage.create_table(CreateTableRequest {
        invocation_id: decimal_table_invocation,
        authenticated_actor_id: user_id,
        schema_id: schema.schema_id,
        name: "decimal_grouped_records".to_owned(),
        columns: decimal_columns,
        retention_deadline_us: now + 3_600_000_000,
        audit_event: decimal_table_audit,
    })?;
    let pressure_table_invocation = id(0xe1);
    let mut pressure_table_audit = identity_audit_event(0xe2, user_id, schema.schema_id);
    pressure_table_audit.correlation_id = pressure_table_invocation;
    let pressure_table = storage.create_table(CreateTableRequest {
        invocation_id: pressure_table_invocation,
        authenticated_actor_id: user_id,
        schema_id: schema.schema_id,
        name: "analytics_pressure_records".to_owned(),
        columns: vec![
            TableColumnDefinition {
                name: "group_key".to_owned(),
                scalar_type: ScalarType::Text,
                nullable: false,
                generated_uuid_identity: false,
                default: None,
            },
            TableColumnDefinition {
                name: "measure".to_owned(),
                scalar_type: ScalarType::Integer,
                nullable: false,
                generated_uuid_identity: false,
                default: None,
            },
        ],
        retention_deadline_us: now + 3_600_000_000,
        audit_event: pressure_table_audit,
    })?;
    let mut composite_schema = vec![
        ColumnSchema {
            scalar_type: ScalarType::Integer,
            nullable: sql_in_null_left_only,
        },
        ColumnSchema {
            scalar_type: ScalarType::Integer,
            nullable: false,
        },
    ];
    if sql_in_elevated_budget_only {
        composite_schema.push(ColumnSchema {
            scalar_type: ScalarType::Text,
            nullable: false,
        });
    }
    // Keep the authorized-view population above the optimizer's small-scan
    // cutoff so the confined range acceptance exercises ScanIndexRange.
    let composite_seed_rows =
        if indexed_join_fanout_benchmark_only || indexed_join_skewed_fanout_benchmark_only {
            (0..u8::try_from(indexed_join_fanout_outer_rows)?)
                .map(|index| {
                    let key = i32::from(index);
                    (0xd3 + index, key, key)
                })
                .collect::<Vec<_>>()
        } else if indexed_join_asymmetric_benchmark_only || indexed_join_skew_benchmark_only {
            (0..if indexed_join_skew_benchmark_only {
                8u8
            } else {
                4u8
            })
                .map(|index| (0xd3 + index, 0, 0))
                .collect::<Vec<_>>()
        } else if indexed_join_benchmark_only {
            (0..64u16)
                .map(|index| {
                    let row_id_byte = if index == 0 {
                        0xd3
                    } else {
                        u8::try_from(index).unwrap()
                    };
                    (row_id_byte, i32::from(index / 8), i32::from(index / 8))
                })
                .collect::<Vec<_>>()
        } else {
            vec![
                (0xd3, 1, 7),
                (0xd4, 1, 7),
                (0xd5, 2, 9),
                (0xd6, 2, 9),
                (0xd7, 2, 9),
            ]
        };
    let mut composite_rows = composite_seed_rows
        .into_iter()
        .map(|(row_id, left, right)| {
            let mut values = vec![
                Some(ScalarValue::Integer(left)),
                Some(ScalarValue::Integer(right)),
            ];
            if sql_in_elevated_budget_only {
                values.push(Some(ScalarValue::Text(String::new())));
            }
            Ok(RowWrite {
                key: table_row_key(composite_table.table_id, id(row_id)),
                expected_version: None,
                payload: Some(encode_tuple(&composite_schema, 1, &values)?),
            })
        })
        .collect::<Result<Vec<_>, Box<dyn std::error::Error>>>()?;
    if sql_in_over_budget_only || sql_in_elevated_budget_only {
        let duplicate_rows = if sql_in_elevated_budget_only {
            1_023u16
        } else {
            124u16
        };
        let large_range_payload = "x".repeat(17 * 1024);
        let mut large_range_bytes = 0usize;
        for row_id in 1..=duplicate_rows {
            let mut row_id_bytes = [0; 16];
            row_id_bytes[14..].copy_from_slice(&row_id.to_be_bytes());
            let mut values = vec![Some(ScalarValue::Integer(1)), Some(ScalarValue::Integer(7))];
            if sql_in_elevated_budget_only {
                values.push(Some(ScalarValue::Text(large_range_payload.clone())));
            }
            let payload = encode_tuple(&composite_schema, 1, &values)?;
            if sql_in_elevated_budget_only {
                large_range_bytes = large_range_bytes.saturating_add(payload.len());
            }
            composite_rows.push(RowWrite {
                key: table_row_key(composite_table.table_id, row_id_bytes),
                expected_version: None,
                payload: Some(payload),
            });
        }
        if sql_in_elevated_budget_only {
            assert!(large_range_bytes > 16 * 1024 * 1024);
        }
    }
    commit_rows(
        &database,
        id(0xd6),
        builtin_owner_id(),
        now + 3_600_000_000,
        &composite_rows,
    )?;
    if sql_in_null_left_only {
        let null_left_row = RowWrite {
            key: table_row_key(composite_table.table_id, id(0xd9)),
            expected_version: None,
            payload: Some(encode_tuple(
                &composite_schema,
                1,
                &[None, Some(ScalarValue::Integer(13))],
            )?),
        };
        commit_rows(
            &database,
            id(0xda),
            builtin_owner_id(),
            now + 3_600_000_000,
            &[null_left_row],
        )?;
    }
    let mut decimal_schema = vec![
        ColumnSchema {
            scalar_type: ScalarType::Integer,
            nullable: spill_query_mode,
        },
        ColumnSchema {
            scalar_type: ScalarType::Integer,
            nullable: false,
        },
        ColumnSchema {
            scalar_type: ScalarType::Decimal {
                precision: 4,
                scale: 2,
            },
            nullable: true,
        },
    ];
    if count_bigint_only {
        decimal_schema.push(ColumnSchema {
            scalar_type: ScalarType::BigInt,
            nullable: true,
        });
    }
    if spill_join_oversize_only || spill_join_text_only || elevated_result_only {
        decimal_schema.push(ColumnSchema {
            scalar_type: ScalarType::Text,
            nullable: true,
        });
    }
    let decimal_rows = [
        (0xf3, 1, 7, Some(100), Some(2)),
        (0xf4, 1, 7, None, None),
        (0xf5, 2, 9, Some(100), Some(6)),
        (0xf6, 2, 9, Some(125), Some(9)),
        (0xf7, 2, 9, Some(225), Some(12)),
        (0xf8, 3, 11, None, None),
    ]
    .into_iter()
    .map(|(row_id, left, right, decimal_value, bigint_value)| {
        Ok(RowWrite {
            key: table_row_key(decimal_table.table_id, id(row_id)),
            expected_version: None,
            payload: Some(encode_tuple(&decimal_schema, 1, &{
                let mut values = vec![
                    Some(ScalarValue::Integer(left)),
                    Some(ScalarValue::Integer(right)),
                    decimal_value.map(ScalarValue::Decimal),
                ];
                if count_bigint_only {
                    values.push(bigint_value.map(ScalarValue::BigInt));
                }
                if spill_join_oversize_only || spill_join_text_only || elevated_result_only {
                    let payload = if elevated_result_only {
                        None
                    } else if spill_join_oversize_only {
                        (row_id == 0xf8).then(|| "x".repeat(1024))
                    } else {
                        match row_id {
                            0xf3 => Some("alpha".to_owned()),
                            0xf5 => Some("echo".to_owned()),
                            0xf6 => Some("charlie".to_owned()),
                            0xf7 => Some("delta".to_owned()),
                            _ => None,
                        }
                    };
                    values.push(payload.map(ScalarValue::Text));
                }
                values
            })?),
        })
    })
    .collect::<Result<Vec<_>, Box<dyn std::error::Error>>>()?;
    let mut decimal_rows = decimal_rows;
    if spill_query_mode {
        for row_id in [0xf9, 0xfa] {
            let mut values = vec![None, Some(ScalarValue::Integer(13)), None];
            if count_bigint_only {
                values.push(None);
            }
            if spill_join_oversize_only || spill_join_text_only || elevated_result_only {
                values.push(None);
            }
            decimal_rows.push(RowWrite {
                key: table_row_key(decimal_table.table_id, id(row_id)),
                expected_version: None,
                payload: Some(encode_tuple(&decimal_schema, 1, &values)?),
            });
        }
    }
    if spill_pressure_only || spill_pressure_cancel_only {
        for index in 0..128u16 {
            let left_key = i32::from(index % 3 + 1);
            let mut values = vec![
                Some(ScalarValue::Integer(left_key)),
                Some(ScalarValue::Integer(17)),
                None,
            ];
            if count_bigint_only {
                values.push(None);
            }
            let mut row_id = [0; 16];
            row_id[14..].copy_from_slice(&(index + 1).to_be_bytes());
            decimal_rows.push(RowWrite {
                key: table_row_key(decimal_table.table_id, row_id),
                expected_version: None,
                payload: Some(encode_tuple(&decimal_schema, 1, &values)?),
            });
        }
    }
    commit_rows(
        &database,
        id(0xf2),
        builtin_owner_id(),
        now + 3_600_000_000,
        &decimal_rows,
    )?;
    if elevated_result_only {
        const ROWS: u16 = 8192;
        const ROWS_PER_COMMIT: u16 = 64;
        let payload = "x".repeat(16_350);
        for batch in 0..ROWS / ROWS_PER_COMMIT {
            let first = batch * ROWS_PER_COMMIT;
            let rows = (first..first + ROWS_PER_COMMIT)
                .map(|row_number| {
                    let mut row_id = [0; 16];
                    row_id[14..].copy_from_slice(&(row_number + 1).to_be_bytes());
                    Ok(RowWrite {
                        key: table_row_key(decimal_table.table_id, row_id),
                        expected_version: None,
                        payload: Some(encode_tuple(
                            &decimal_schema,
                            1,
                            &[
                                Some(ScalarValue::Integer(99)),
                                Some(ScalarValue::Integer(1)),
                                None,
                                Some(ScalarValue::Text(payload.clone())),
                            ],
                        )?),
                    })
                })
                .collect::<Result<Vec<_>, Box<dyn std::error::Error>>>()?;
            let mut invocation_id = id(0xc6);
            invocation_id[14..].copy_from_slice(&(batch + 1).to_be_bytes());
            commit_rows(
                &database,
                invocation_id,
                builtin_owner_id(),
                now + 3_600_000_000,
                &rows,
            )?;
        }
    }
    let pressure_schema = [
        ColumnSchema {
            scalar_type: ScalarType::Text,
            nullable: false,
        },
        ColumnSchema {
            scalar_type: ScalarType::Integer,
            nullable: false,
        },
    ];
    let pressure_key = "a".repeat(8_168);
    let pressure_rows = (0..128u16)
        .map(|index| {
            let group = index / 2;
            let value = format!("{group:04}{pressure_key}");
            let mut row_id = id(0);
            row_id[14..].copy_from_slice(&(index + 1).to_be_bytes());
            Ok(RowWrite {
                key: table_row_key(pressure_table.table_id, row_id),
                expected_version: None,
                payload: Some(encode_tuple(
                    &pressure_schema,
                    1,
                    &[
                        Some(ScalarValue::Text(value)),
                        Some(ScalarValue::Integer(if index % 2 == 0 {
                            100 + i32::from(group)
                        } else {
                            200 + i32::from(group)
                        })),
                    ],
                )?),
            })
        })
        .collect::<Result<Vec<_>, Box<dyn std::error::Error>>>()?;
    commit_rows(
        &database,
        id(0xe3),
        builtin_owner_id(),
        now + 3_600_000_000,
        &pressure_rows,
    )?;
    let index_pressure_table_invocation = id(0xe8);
    let mut index_pressure_table_audit = identity_audit_event(0xe9, user_id, schema.schema_id);
    index_pressure_table_audit.correlation_id = index_pressure_table_invocation;
    let index_pressure_table = storage.create_table(CreateTableRequest {
        invocation_id: index_pressure_table_invocation,
        authenticated_actor_id: user_id,
        schema_id: schema.schema_id,
        name: "analytics_index_pressure_records".to_owned(),
        columns: vec![TableColumnDefinition {
            name: "group_key".to_owned(),
            scalar_type: ScalarType::Text,
            nullable: false,
            generated_uuid_identity: false,
            default: None,
        }],
        retention_deadline_us: now + 3_600_000_000,
        audit_event: index_pressure_table_audit,
    })?;
    let range_benchmark_table_invocation = id(0xb2);
    let mut range_benchmark_table_audit = identity_audit_event(0xb3, user_id, schema.schema_id);
    range_benchmark_table_audit.correlation_id = range_benchmark_table_invocation;
    let range_benchmark_table = storage.create_table(CreateTableRequest {
        invocation_id: range_benchmark_table_invocation,
        authenticated_actor_id: user_id,
        schema_id: schema.schema_id,
        name: "analytics_range_benchmark_records".to_owned(),
        columns: vec![TableColumnDefinition {
            name: "range_key".to_owned(),
            scalar_type: ScalarType::Integer,
            nullable: false,
            generated_uuid_identity: false,
            default: None,
        }],
        retention_deadline_us: now + 3_600_000_000,
        audit_event: range_benchmark_table_audit,
    })?;
    let operational_pressure_table_invocation = id(0xeb);
    let mut operational_pressure_table_audit =
        identity_audit_event(0xec, user_id, schema.schema_id);
    operational_pressure_table_audit.correlation_id = operational_pressure_table_invocation;
    let operational_pressure_table = storage.create_table(CreateTableRequest {
        invocation_id: operational_pressure_table_invocation,
        authenticated_actor_id: user_id,
        schema_id: schema.schema_id,
        name: "operational_pressure_records".to_owned(),
        columns: vec![
            TableColumnDefinition {
                name: "worker_key".to_owned(),
                scalar_type: ScalarType::Integer,
                nullable: false,
                generated_uuid_identity: false,
                default: None,
            },
            TableColumnDefinition {
                name: "value".to_owned(),
                scalar_type: ScalarType::Integer,
                nullable: false,
                generated_uuid_identity: false,
                default: None,
            },
        ],
        retention_deadline_us: now + 3_600_000_000,
        audit_event: operational_pressure_table_audit,
    })?;
    let operational_pressure_table_id = operational_pressure_table.table_id;
    let operational_pressure_column_ids = [
        operational_pressure_table.column_ids[0],
        operational_pressure_table.column_ids[1],
    ];
    let index_pressure_schema = [ColumnSchema {
        scalar_type: ScalarType::Text,
        nullable: false,
    }];
    let index_pressure_key = "a".repeat(8_188);
    let index_pressure_rows = (0..1024u16)
        .map(|index| {
            let value = format!("{index:04}{index_pressure_key}");
            let mut row_id = id(0);
            row_id[14..].copy_from_slice(&(index + 1).to_be_bytes());
            Ok(RowWrite {
                key: table_row_key(index_pressure_table.table_id, row_id),
                expected_version: None,
                payload: Some(encode_tuple(
                    &index_pressure_schema,
                    1,
                    &[Some(ScalarValue::Text(value))],
                )?),
            })
        })
        .collect::<Result<Vec<_>, Box<dyn std::error::Error>>>()?;
    commit_rows(
        &database,
        id(0xea),
        builtin_owner_id(),
        now + 3_600_000_000,
        &index_pressure_rows,
    )?;
    let range_benchmark_schema = [ColumnSchema {
        scalar_type: ScalarType::Integer,
        nullable: false,
    }];
    let range_benchmark_rows = (0..1024u16)
        .map(|index| {
            let mut row_id = id(0);
            row_id[14..].copy_from_slice(&(index + 1).to_be_bytes());
            Ok(RowWrite {
                key: table_row_key(range_benchmark_table.table_id, row_id),
                expected_version: None,
                payload: Some(encode_tuple(
                    &range_benchmark_schema,
                    1,
                    &[Some(ScalarValue::Integer(i32::from(index / 8)))],
                )?),
            })
        })
        .collect::<Result<Vec<_>, Box<dyn std::error::Error>>>()?;
    commit_rows(
        &database,
        id(0xb4),
        builtin_owner_id(),
        now + 3_600_000_000,
        &range_benchmark_rows,
    )?;
    let operational_pressure_schema = [
        ColumnSchema {
            scalar_type: ScalarType::Integer,
            nullable: false,
        },
        ColumnSchema {
            scalar_type: ScalarType::Integer,
            nullable: false,
        },
    ];
    let operational_pressure_rows = (0..4u8)
        .map(|index| {
            Ok(RowWrite {
                key: table_row_key(operational_pressure_table.table_id, id(0xed + index)),
                expected_version: None,
                payload: Some(encode_tuple(
                    &operational_pressure_schema,
                    1,
                    &[
                        Some(ScalarValue::Integer(i32::from(index))),
                        Some(ScalarValue::Integer(0)),
                    ],
                )?),
            })
        })
        .collect::<Result<Vec<_>, Box<dyn std::error::Error>>>()?;
    commit_rows(
        &database,
        id(0x4c),
        builtin_owner_id(),
        now + 3_600_000_000,
        &operational_pressure_rows,
    )?;
    for row_id in if indexed_join_benchmark_only {
        vec![0xd3]
    } else {
        vec![0xd3, 0xd4, 0xd5, 0xd6, 0xd7]
    } {
        assert!(
            row(
                &database,
                &table_row_key(composite_table.table_id, id(row_id))
            )?
            .is_some()
        );
    }
    let joined_schema = [
        ColumnSchema {
            scalar_type: ScalarType::Integer,
            nullable: true,
        },
        ColumnSchema {
            scalar_type: ScalarType::Integer,
            nullable: true,
        },
    ];
    let joined_seed_rows = if indexed_join_skewed_high_fanout_benchmark_only {
        (0..128u16)
            .map(|index| {
                let mut row_id = [0; 16];
                row_id[14..].copy_from_slice(&(index + 1).to_be_bytes());
                (row_id, 0, 0)
            })
            .collect::<Vec<_>>()
    } else if indexed_join_skewed_fanout_benchmark_only {
        (0..128u16)
            .map(|index| {
                let key = if index < 16 {
                    0
                } else {
                    1000 + i32::from(index)
                };
                let mut row_id = [0; 16];
                row_id[14..].copy_from_slice(&(index + 1).to_be_bytes());
                (row_id, key, key)
            })
            .collect::<Vec<_>>()
    } else if indexed_join_fanout_benchmark_only {
        (0..indexed_join_fanout_inner_rows)
            .map(|index| {
                let matching_rows = indexed_join_fanout_outer_rows * indexed_join_fanout
                    + u16::from(
                        indexed_join_fanout_seventeen_over_threshold_benchmark_only
                            || indexed_join_fanout_eighteen_over_threshold_benchmark_only,
                    );
                let key = if index < matching_rows {
                    if (indexed_join_fanout_seventeen_over_threshold_benchmark_only
                        || indexed_join_fanout_eighteen_over_threshold_benchmark_only)
                        && index == indexed_join_fanout_outer_rows * indexed_join_fanout
                    {
                        0
                    } else {
                        i32::from(index / indexed_join_fanout)
                    }
                } else {
                    1000 + i32::from(index)
                };
                let mut row_id = [0; 16];
                row_id[14..].copy_from_slice(&(index + 1).to_be_bytes());
                (row_id, key, key)
            })
            .collect::<Vec<_>>()
    } else if indexed_join_asymmetric_benchmark_only || indexed_join_skew_benchmark_only {
        (0..128u16)
            .map(|index| {
                let mut row_id = [0; 16];
                row_id[14..].copy_from_slice(&(index + 1).to_be_bytes());
                (row_id, 0, 0)
            })
            .collect::<Vec<_>>()
    } else if indexed_join_benchmark_only {
        (0..64u16)
            .map(|index| {
                let mut row_id = [0; 16];
                row_id[14..].copy_from_slice(&(index + 1).to_be_bytes());
                (row_id, i32::from(index / 8), i32::from(index / 8))
            })
            .collect::<Vec<_>>()
    } else {
        vec![(id(0xc5), 42, 7), (id(0xca), 1, 7)]
    };
    let joined_rows = joined_seed_rows
        .into_iter()
        .map(|(row_id, id_value, match_key)| {
            Ok(RowWrite {
                key: table_row_key(joined_table.table_id, row_id),
                expected_version: None,
                payload: Some(encode_tuple(
                    &joined_schema,
                    1,
                    &[
                        Some(ScalarValue::Integer(id_value)),
                        Some(ScalarValue::Integer(match_key)),
                    ],
                )?),
            })
        })
        .collect::<Result<Vec<_>, Box<dyn std::error::Error>>>()?;
    commit_rows(
        &database,
        id(0xc4),
        builtin_owner_id(),
        now + 3_600_000_000,
        &joined_rows,
    )?;
    let private_table_invocation = id(0xa0);
    let mut private_table_audit = identity_audit_event(0xa1, user_id, schema.schema_id);
    private_table_audit.correlation_id = private_table_invocation;
    let _private_table = storage
        .create_table(CreateTableRequest {
            invocation_id: private_table_invocation,
            authenticated_actor_id: user_id,
            schema_id: schema.schema_id,
            name: "private_records".to_owned(),
            columns: vec![TableColumnDefinition {
                name: "secret".to_owned(),
                scalar_type: ScalarType::Integer,
                nullable: true,
                generated_uuid_identity: false,
                default: None,
            }],
            retention_deadline_us: now + 3_600_000_000,
            audit_event: private_table_audit,
        })
        .map_err(|error| format!("private metadata fixture table creation failed: {error:?}"))?;
    let hidden_table_invocation = id(0xa8);
    let mut hidden_table_audit = identity_audit_event(0xa9, user_id, hidden_schema.schema_id);
    hidden_table_audit.correlation_id = hidden_table_invocation;
    let _hidden_table = storage.create_table(CreateTableRequest {
        invocation_id: hidden_table_invocation,
        authenticated_actor_id: user_id,
        schema_id: hidden_schema.schema_id,
        name: "hidden_records".to_owned(),
        columns: vec![TableColumnDefinition {
            name: "secret".to_owned(),
            scalar_type: ScalarType::Integer,
            nullable: true,
            generated_uuid_identity: false,
            default: None,
        }],
        retention_deadline_us: now + 3_600_000_000,
        audit_event: hidden_table_audit,
    })?;

    let authority_invocation = id(0x83);
    let mut authority_audit = identity_audit_event(0x84, user_id, user_id);
    authority_audit.action = AuditAction::AuthorityChange;
    authority_audit.correlation_id = authority_invocation;
    let mut authority_writes = vec![
        grant_write(user_id, schema.schema_id, 1, 1, schema.schema_id, user_id),
        grant_write(
            user_id,
            empty_schema.schema_id,
            1,
            1,
            empty_schema.schema_id,
            user_id,
        ),
        grant_write(
            restricted_user_id,
            empty_schema.schema_id,
            1,
            1,
            empty_schema.schema_id,
            user_id,
        ),
        dependency_write(user_id, schema.schema_id, 5),
        dependency_write(user_id, empty_schema.schema_id, 5),
        dependency_write(restricted_user_id, empty_schema.schema_id, 5),
        dependency_write(user_id, records_table.table_id, 5),
        dependency_write(user_id, records_table.column_ids[0], 5),
        dependency_write(user_id, identity_table.table_id, 5),
        dependency_write(user_id, identity_table.column_ids[1], 5),
        dependency_write(user_id, joined_table.table_id, 5),
        dependency_write(user_id, joined_table.column_ids[0], 5),
        dependency_write(user_id, joined_table.column_ids[1], 5),
        dependency_write(user_id, composite_table.table_id, 5),
        dependency_write(user_id, composite_table.column_ids[0], 5),
        dependency_write(user_id, composite_table.column_ids[1], 5),
        dependency_write(user_id, decimal_table.table_id, 5),
        dependency_write(user_id, decimal_table.column_ids[0], 5),
        dependency_write(user_id, decimal_table.column_ids[1], 5),
        dependency_write(user_id, decimal_table.column_ids[2], 5),
        dependency_write(user_id, pressure_table.table_id, 5),
        dependency_write(user_id, pressure_table.column_ids[0], 5),
        dependency_write(user_id, pressure_table.column_ids[1], 5),
        dependency_write(user_id, range_benchmark_table.table_id, 5),
        dependency_write(user_id, range_benchmark_table.column_ids[0], 5),
        dependency_write(user_id, operational_pressure_table.table_id, 5),
        dependency_write(user_id, operational_pressure_table.column_ids[0], 5),
        dependency_write(user_id, operational_pressure_table.column_ids[1], 5),
    ];
    if count_bigint_only {
        authority_writes.push(dependency_write(user_id, decimal_table.column_ids[3], 5));
    }
    if sql_in_elevated_budget_only {
        authority_writes.push(dependency_write(user_id, composite_table.column_ids[2], 5));
    }
    if spill_join_oversize_only || spill_join_text_only || elevated_result_only {
        let payload_column_index = if count_bigint_only { 4 } else { 3 };
        authority_writes.push(dependency_write(
            user_id,
            decimal_table.column_ids[payload_column_index],
            5,
        ));
    }
    authority_writes.extend((2..=5).map(|privilege| {
        grant_write(
            user_id,
            records_table.table_id,
            privilege,
            1,
            records_table.table_id,
            user_id,
        )
    }));
    authority_writes.push(grant_write(
        user_id,
        identity_table.table_id,
        3,
        1,
        identity_table.table_id,
        user_id,
    ));
    authority_writes.push(grant_write(
        user_id,
        identity_table.table_id,
        3,
        2,
        identity_table.column_ids[1],
        user_id,
    ));
    authority_writes.extend((2..=4).map(|privilege| {
        grant_write(
            user_id,
            records_table.table_id,
            privilege,
            2,
            records_table.column_ids[0],
            user_id,
        )
    }));
    authority_writes.extend([
        grant_write(
            user_id,
            joined_table.table_id,
            2,
            1,
            joined_table.table_id,
            user_id,
        ),
        grant_write(
            user_id,
            joined_table.table_id,
            2,
            2,
            joined_table.column_ids[0],
            user_id,
        ),
        grant_write(
            user_id,
            joined_table.table_id,
            2,
            2,
            joined_table.column_ids[1],
            user_id,
        ),
        grant_write(
            user_id,
            composite_table.table_id,
            2,
            1,
            composite_table.table_id,
            user_id,
        ),
        grant_write(
            user_id,
            decimal_table.table_id,
            2,
            1,
            decimal_table.table_id,
            user_id,
        ),
        grant_write(
            user_id,
            pressure_table.table_id,
            2,
            1,
            pressure_table.table_id,
            user_id,
        ),
        grant_write(
            user_id,
            pressure_table.table_id,
            2,
            2,
            pressure_table.column_ids[0],
            user_id,
        ),
        grant_write(
            user_id,
            pressure_table.table_id,
            2,
            2,
            pressure_table.column_ids[1],
            user_id,
        ),
        grant_write(
            user_id,
            range_benchmark_table.table_id,
            2,
            1,
            range_benchmark_table.table_id,
            user_id,
        ),
        grant_write(
            user_id,
            range_benchmark_table.table_id,
            2,
            2,
            range_benchmark_table.column_ids[0],
            user_id,
        ),
    ]);
    authority_writes.extend([
        grant_write(
            user_id,
            operational_pressure_table.table_id,
            2,
            1,
            operational_pressure_table.table_id,
            user_id,
        ),
        grant_write(
            user_id,
            operational_pressure_table.table_id,
            4,
            1,
            operational_pressure_table.table_id,
            user_id,
        ),
    ]);
    authority_writes.extend(
        operational_pressure_table
            .column_ids
            .iter()
            .flat_map(|column_id| {
                [2, 4].into_iter().map(|privilege| {
                    grant_write(
                        user_id,
                        operational_pressure_table.table_id,
                        privilege,
                        2,
                        *column_id,
                        user_id,
                    )
                })
            }),
    );
    authority_writes.push(grant_write(
        user_id,
        joined_table.table_id,
        4,
        1,
        joined_table.table_id,
        user_id,
    ));
    authority_writes.extend(
        joined_table.column_ids.iter().map(|column_id| {
            grant_write(user_id, joined_table.table_id, 4, 2, *column_id, user_id)
        }),
    );
    authority_writes.extend(composite_table.column_ids.iter().map(|column_id| {
        grant_write(user_id, composite_table.table_id, 2, 2, *column_id, user_id)
    }));
    authority_writes.extend(
        decimal_table.column_ids.iter().map(|column_id| {
            grant_write(user_id, decimal_table.table_id, 2, 2, *column_id, user_id)
        }),
    );
    authority_writes.push(grant_write(
        user_id,
        composite_table.table_id,
        4,
        1,
        composite_table.table_id,
        user_id,
    ));
    authority_writes.extend(composite_table.column_ids.iter().map(|column_id| {
        grant_write(user_id, composite_table.table_id, 4, 2, *column_id, user_id)
    }));
    authority_writes.extend(
        decimal_table.column_ids.iter().map(|column_id| {
            grant_write(user_id, decimal_table.table_id, 4, 2, *column_id, user_id)
        }),
    );
    let (
        old_user_record,
        old_token_record,
        old_restricted_user_record,
        old_restricted_token_record,
    ) = {
        let read = database.begin_read()?;
        let catalog = read.open_table(CATALOG)?;
        (
            catalog
                .get(id_catalog_key(10, user_id).as_slice())?
                .ok_or("sandbox SQL user record missing")?
                .value()
                .to_vec(),
            catalog
                .get(id_catalog_key(16, token_id).as_slice())?
                .ok_or("sandbox SQL token record missing")?
                .value()
                .to_vec(),
            catalog
                .get(id_catalog_key(10, restricted_user_id).as_slice())?
                .ok_or("restricted sandbox SQL user record missing")?
                .value()
                .to_vec(),
            catalog
                .get(id_catalog_key(16, restricted_token_id).as_slice())?
                .ok_or("restricted sandbox SQL token record missing")?
                .value()
                .to_vec(),
        )
    };
    let mut bumped_user = principal_write_with_name(user_id, 1, 2, "sandbox_sql_roundtrip");
    bumped_user.expected_value = Some(old_user_record);
    authority_writes.push(bumped_user);
    let mut refreshed_token = credential_verifier_write(
        token_id,
        user_id,
        &secret,
        now - 1_000_000,
        now + 60_000_000,
        None,
        2,
    );
    refreshed_token.expected_value = Some(old_token_record);
    authority_writes.push(refreshed_token);
    let mut bumped_restricted_user =
        principal_write_with_name(restricted_user_id, 1, 2, "sandbox_sql_restricted");
    bumped_restricted_user.expected_value = Some(old_restricted_user_record);
    authority_writes.push(bumped_restricted_user);
    let mut refreshed_restricted_token = credential_verifier_write(
        restricted_token_id,
        restricted_user_id,
        &restricted_secret,
        now - 1_000_000,
        now + 60_000_000,
        None,
        2,
    );
    refreshed_restricted_token.expected_value = Some(old_restricted_token_record);
    authority_writes.push(refreshed_restricted_token);
    storage
        .commit_catalog_and_audit(
            authority_invocation,
            user_id,
            now + 3_600_000_000,
            &authority_writes,
            &[authority_audit],
        )
        .map_err(|error| format!("metadata fixture grants failed: {error:?}"))?;

    let insert_policy_id = id(0x85);
    let insert_policy_expression_id = id(0x86);
    let insert_cell_policy_id = id(0x89);
    let insert_policy_key = id_catalog_key(13, insert_policy_id);
    let insert_policy_payload = encode_envelope(
        &family_schema(13).unwrap(),
        &insert_policy_key,
        &[
            CatalogField {
                tag: 1,
                wire_type: WireType::Id,
                payload: records_table.table_id.to_vec(),
            },
            CatalogField {
                tag: 2,
                wire_type: WireType::U16,
                payload: 1u16.to_le_bytes().to_vec(),
            },
            CatalogField {
                tag: 3,
                wire_type: WireType::U16,
                payload: 2u16.to_le_bytes().to_vec(),
            },
            CatalogField {
                tag: 4,
                wire_type: WireType::U16,
                payload: 1u16.to_le_bytes().to_vec(),
            },
            CatalogField {
                tag: 6,
                wire_type: WireType::Id,
                payload: insert_policy_expression_id.to_vec(),
            },
            CatalogField {
                tag: 7,
                wire_type: WireType::U64,
                payload: 1u64.to_le_bytes().to_vec(),
            },
        ],
    )?;
    let policy_target_payload =
        prdb_engine::catalog::encode_policy_target(&prdb_engine::catalog::PolicyTarget {
            policy_id: insert_policy_id,
            kind: 1,
            target_id: user_id,
        })?;
    let cell_policy_key = id_catalog_key(13, insert_cell_policy_id);
    let cell_policy_payload = encode_envelope(
        &family_schema(13).unwrap(),
        &cell_policy_key,
        &[
            CatalogField {
                tag: 1,
                wire_type: WireType::Id,
                payload: records_table.table_id.to_vec(),
            },
            CatalogField {
                tag: 2,
                wire_type: WireType::U16,
                payload: 2u16.to_le_bytes().to_vec(),
            },
            CatalogField {
                tag: 3,
                wire_type: WireType::U16,
                payload: 2u16.to_le_bytes().to_vec(),
            },
            CatalogField {
                tag: 4,
                wire_type: WireType::U16,
                payload: 1u16.to_le_bytes().to_vec(),
            },
            CatalogField {
                tag: 6,
                wire_type: WireType::Id,
                payload: insert_policy_expression_id.to_vec(),
            },
            CatalogField {
                tag: 7,
                wire_type: WireType::U64,
                payload: 1u64.to_le_bytes().to_vec(),
            },
        ],
    )?;
    let cell_policy_recipient =
        prdb_engine::catalog::encode_policy_target(&prdb_engine::catalog::PolicyTarget {
            policy_id: insert_cell_policy_id,
            kind: 1,
            target_id: user_id,
        })?;
    let cell_policy_column =
        prdb_engine::catalog::encode_policy_target(&prdb_engine::catalog::PolicyTarget {
            policy_id: insert_cell_policy_id,
            kind: 3,
            target_id: records_table.column_ids[0],
        })?;
    let insert_policy_expression_key = id_catalog_key(7, insert_policy_expression_id);
    let insert_policy_expression_ir =
        prdb_engine::expression::encode_expression(&prdb_engine::authorization::Expr::Literal(
            Some(prdb_engine::tuple::ScalarValue::Boolean(true)),
        ))?;
    let insert_policy_expression = CatalogWrite {
        key: insert_policy_expression_key.clone(),
        expected_value: None,
        payload: Some(encode_envelope(
            &family_schema(7).unwrap(),
            &insert_policy_expression_key,
            &[
                CatalogField {
                    tag: 1,
                    wire_type: WireType::U16,
                    payload: 4u16.to_le_bytes().to_vec(),
                },
                CatalogField {
                    tag: 2,
                    wire_type: WireType::U32,
                    payload: 1u32.to_le_bytes().to_vec(),
                },
                CatalogField {
                    tag: 3,
                    wire_type: WireType::Id,
                    payload: builtin_id(101).to_vec(),
                },
                CatalogField {
                    tag: 4,
                    wire_type: WireType::Bytes,
                    payload: vec![0; 8],
                },
                CatalogField {
                    tag: 5,
                    wire_type: WireType::Bytes,
                    payload: insert_policy_expression_ir,
                },
                CatalogField {
                    tag: 6,
                    wire_type: WireType::Ids,
                    payload: 0u32.to_le_bytes().to_vec(),
                },
            ],
        )?),
    };
    let mut insert_policy_table_epoch =
        table_write_with_versions(records_table.table_id, schema.schema_id, 1, 2);
    insert_policy_table_epoch.expected_value = Some(
        database
            .begin_read()?
            .open_table(CATALOG)?
            .get(id_catalog_key(5, records_table.table_id).as_slice())?
            .ok_or("sandbox SQL table record missing")?
            .value()
            .to_vec(),
    );
    let mut joined_policy_table_epoch =
        table_write_with_versions(joined_table.table_id, schema.schema_id, 1, 2);
    joined_policy_table_epoch.expected_value = Some(
        database
            .begin_read()?
            .open_table(CATALOG)?
            .get(id_catalog_key(5, joined_table.table_id).as_slice())?
            .ok_or("sandbox joined table record missing")?
            .value()
            .to_vec(),
    );
    let mut composite_policy_table_epoch =
        table_write_with_versions(composite_table.table_id, schema.schema_id, 1, 2);
    composite_policy_table_epoch.expected_value = Some(
        database
            .begin_read()?
            .open_table(CATALOG)?
            .get(id_catalog_key(5, composite_table.table_id).as_slice())?
            .ok_or("sandbox composite table record missing")?
            .value()
            .to_vec(),
    );
    let mut decimal_policy_table_epoch =
        table_write_with_versions(decimal_table.table_id, schema.schema_id, 1, 2);
    decimal_policy_table_epoch.expected_value = Some(
        database
            .begin_read()?
            .open_table(CATALOG)?
            .get(id_catalog_key(5, decimal_table.table_id).as_slice())?
            .ok_or("sandbox Decimal table record missing")?
            .value()
            .to_vec(),
    );
    let mut pressure_policy_table_epoch =
        table_write_with_versions(pressure_table.table_id, schema.schema_id, 1, 2);
    pressure_policy_table_epoch.expected_value = Some(
        database
            .begin_read()?
            .open_table(CATALOG)?
            .get(id_catalog_key(5, pressure_table.table_id).as_slice())?
            .ok_or("sandbox analytical pressure table record missing")?
            .value()
            .to_vec(),
    );
    let mut range_benchmark_policy_table_epoch =
        table_write_with_versions(range_benchmark_table.table_id, schema.schema_id, 1, 2);
    range_benchmark_policy_table_epoch.expected_value = Some(
        database
            .begin_read()?
            .open_table(CATALOG)?
            .get(id_catalog_key(5, range_benchmark_table.table_id).as_slice())?
            .ok_or("sandbox range benchmark table record missing")?
            .value()
            .to_vec(),
    );
    let select_using_id = id(0x8a);
    let update_using_id = id(0x8b);
    let mut policy_writes = vec![
        CatalogWrite {
            key: insert_policy_key,
            expected_value: None,
            payload: Some(insert_policy_payload),
        },
        CatalogWrite {
            key: cell_policy_key,
            expected_value: None,
            payload: Some(cell_policy_payload),
        },
        object_write_with_parent_and_owner_and_name(
            insert_policy_id,
            10,
            None,
            user_id,
            "sandbox_insert_policy",
        ),
        object_write_with_parent_and_owner_and_name(
            insert_cell_policy_id,
            10,
            None,
            user_id,
            "sandbox_insert_cell_policy",
        ),
        insert_policy_expression,
        object_write_with_parent_and_owner_and_name(
            insert_policy_expression_id,
            5,
            None,
            user_id,
            "sandbox_insert_policy_check",
        ),
        CatalogWrite {
            key: policy_target_payload[32..].to_vec(),
            expected_value: None,
            payload: Some(policy_target_payload),
        },
        CatalogWrite {
            key: cell_policy_recipient[32..].to_vec(),
            expected_value: None,
            payload: Some(cell_policy_recipient),
        },
        CatalogWrite {
            key: cell_policy_column[32..].to_vec(),
            expected_value: None,
            payload: Some(cell_policy_column),
        },
        dependency_write(insert_policy_id, insert_policy_expression_id, 2),
        dependency_write(insert_policy_id, user_id, 5),
        dependency_write(insert_cell_policy_id, insert_policy_expression_id, 2),
        dependency_write(insert_cell_policy_id, user_id, 5),
        insert_policy_table_epoch,
        joined_policy_table_epoch,
        composite_policy_table_epoch,
        decimal_policy_table_epoch,
        pressure_policy_table_epoch,
        range_benchmark_policy_table_epoch,
    ];
    let make_using_expression = |expression_id: [u8; 16], name: &str| {
        let key = id_catalog_key(7, expression_id);
        let ir = prdb_engine::expression::encode_expression(
            &prdb_engine::authorization::Expr::Literal(Some(
                prdb_engine::tuple::ScalarValue::Boolean(true),
            )),
        )?;
        let expression = CatalogWrite {
            key: key.clone(),
            expected_value: None,
            payload: Some(encode_envelope(
                &family_schema(7).unwrap(),
                &key,
                &[
                    CatalogField {
                        tag: 1,
                        wire_type: WireType::U16,
                        payload: 3u16.to_le_bytes().to_vec(),
                    },
                    CatalogField {
                        tag: 2,
                        wire_type: WireType::U32,
                        payload: 1u32.to_le_bytes().to_vec(),
                    },
                    CatalogField {
                        tag: 3,
                        wire_type: WireType::Id,
                        payload: builtin_id(101).to_vec(),
                    },
                    CatalogField {
                        tag: 4,
                        wire_type: WireType::Bytes,
                        payload: vec![0; 8],
                    },
                    CatalogField {
                        tag: 5,
                        wire_type: WireType::Bytes,
                        payload: ir,
                    },
                    CatalogField {
                        tag: 6,
                        wire_type: WireType::Ids,
                        payload: 0u32.to_le_bytes().to_vec(),
                    },
                ],
            )?),
        };
        Ok::<_, Box<dyn std::error::Error>>((
            expression,
            object_write_with_parent_and_owner_and_name(expression_id, 5, None, user_id, name),
        ))
    };
    let (select_using, select_using_object) =
        make_using_expression(select_using_id, "sandbox_select_using")?;
    let (update_using, update_using_object) =
        make_using_expression(update_using_id, "sandbox_update_using")?;
    policy_writes.extend([
        select_using,
        select_using_object,
        update_using,
        update_using_object,
    ]);

    let mut add_table_policy = |table_id: [u8; 16],
                                column_id: [u8; 16],
                                policy_id: [u8; 16],
                                operation: u16,
                                using_id: Option<[u8; 16]>,
                                check_id: Option<[u8; 16]>,
                                column_target: bool,
                                name: &str|
     -> Result<(), Box<dyn std::error::Error>> {
        let key = id_catalog_key(13, policy_id);
        let mut fields = vec![
            CatalogField {
                tag: 1,
                wire_type: WireType::Id,
                payload: table_id.to_vec(),
            },
            CatalogField {
                tag: 2,
                wire_type: WireType::U16,
                payload: if column_target { 2u16 } else { 1u16 }
                    .to_le_bytes()
                    .to_vec(),
            },
            CatalogField {
                tag: 3,
                wire_type: WireType::U16,
                payload: operation.to_le_bytes().to_vec(),
            },
            CatalogField {
                tag: 4,
                wire_type: WireType::U16,
                payload: 1u16.to_le_bytes().to_vec(),
            },
        ];
        if let Some(expression_id) = using_id {
            fields.push(CatalogField {
                tag: 5,
                wire_type: WireType::Id,
                payload: expression_id.to_vec(),
            });
        }
        if let Some(expression_id) = check_id {
            fields.push(CatalogField {
                tag: 6,
                wire_type: WireType::Id,
                payload: expression_id.to_vec(),
            });
        }
        fields.push(CatalogField {
            tag: 7,
            wire_type: WireType::U64,
            payload: 1u64.to_le_bytes().to_vec(),
        });
        policy_writes.extend([
            CatalogWrite {
                key: key.clone(),
                expected_value: None,
                payload: Some(encode_envelope(&family_schema(13).unwrap(), &key, &fields)?),
            },
            object_write_with_parent_and_owner_and_name(policy_id, 10, None, user_id, name),
        ]);
        let recipient =
            prdb_engine::catalog::encode_policy_target(&prdb_engine::catalog::PolicyTarget {
                policy_id,
                kind: 1,
                target_id: user_id,
            })?;
        policy_writes.push(CatalogWrite {
            key: recipient[32..].to_vec(),
            expected_value: None,
            payload: Some(recipient),
        });
        if column_target {
            let column =
                prdb_engine::catalog::encode_policy_target(&prdb_engine::catalog::PolicyTarget {
                    policy_id,
                    kind: 3,
                    target_id: column_id,
                })?;
            policy_writes.push(CatalogWrite {
                key: column[32..].to_vec(),
                expected_value: None,
                payload: Some(column),
            });
        }
        if let Some(expression_id) = using_id {
            policy_writes.push(dependency_write(policy_id, expression_id, 2));
        }
        if let Some(expression_id) = check_id {
            policy_writes.push(dependency_write(policy_id, expression_id, 2));
        }
        policy_writes.push(dependency_write(policy_id, user_id, 5));
        Ok(())
    };
    add_table_policy(
        records_table.table_id,
        records_table.column_ids[0],
        id(0x8c),
        1,
        Some(select_using_id),
        None,
        false,
        "sandbox_select_policy",
    )?;
    add_table_policy(
        records_table.table_id,
        records_table.column_ids[0],
        id(0x8d),
        1,
        Some(select_using_id),
        None,
        true,
        "sandbox_select_cell_policy",
    )?;
    add_table_policy(
        records_table.table_id,
        records_table.column_ids[0],
        id(0x8e),
        3,
        Some(update_using_id),
        Some(insert_policy_expression_id),
        false,
        "sandbox_update_policy",
    )?;
    add_table_policy(
        records_table.table_id,
        records_table.column_ids[0],
        id(0x8f),
        3,
        Some(update_using_id),
        Some(insert_policy_expression_id),
        true,
        "sandbox_update_cell_policy",
    )?;
    add_table_policy(
        records_table.table_id,
        records_table.column_ids[0],
        id(0x90),
        4,
        Some(select_using_id),
        None,
        false,
        "sandbox_delete_policy",
    )?;
    add_table_policy(
        joined_table.table_id,
        joined_table.column_ids[0],
        id(0xc6),
        1,
        Some(select_using_id),
        None,
        false,
        "sandbox_join_select_policy",
    )?;
    add_table_policy(
        joined_table.table_id,
        joined_table.column_ids[0],
        id(0xc7),
        1,
        Some(select_using_id),
        None,
        true,
        "sandbox_join_select_cell_policy",
    )?;
    add_table_policy(
        joined_table.table_id,
        joined_table.column_ids[1],
        id(0xcb),
        1,
        Some(select_using_id),
        None,
        true,
        "sandbox_join_match_key_select_cell_policy",
    )?;
    add_table_policy(
        composite_table.table_id,
        composite_table.column_ids[0],
        id(0xd7),
        1,
        Some(select_using_id),
        None,
        false,
        "sandbox_composite_select_policy",
    )?;
    add_table_policy(
        composite_table.table_id,
        composite_table.column_ids[0],
        id(0xd8),
        1,
        Some(select_using_id),
        None,
        true,
        "sandbox_composite_left_select_policy",
    )?;
    add_table_policy(
        composite_table.table_id,
        composite_table.column_ids[1],
        id(0xd9),
        1,
        Some(select_using_id),
        None,
        true,
        "sandbox_composite_right_select_policy",
    )?;
    add_table_policy(
        decimal_table.table_id,
        decimal_table.column_ids[0],
        id(0xfb),
        1,
        Some(select_using_id),
        None,
        false,
        "sandbox_decimal_group_select_policy",
    )?;
    let mut decimal_cell_policies = vec![
        (
            decimal_table.column_ids[0],
            id(0xfc),
            "sandbox_decimal_group_left_cell_policy",
        ),
        (
            decimal_table.column_ids[1],
            id(0xfd),
            "sandbox_decimal_group_right_cell_policy",
        ),
        (
            decimal_table.column_ids[2],
            id(0xfe),
            "sandbox_decimal_group_value_cell_policy",
        ),
    ];
    if count_bigint_only {
        decimal_cell_policies.push((
            decimal_table.column_ids[3],
            id(0xc0),
            "sandbox_decimal_group_bigint_cell_policy",
        ));
    }
    if spill_join_oversize_only || spill_join_text_only || elevated_result_only {
        let payload_column_index = if count_bigint_only { 4 } else { 3 };
        decimal_cell_policies.push((
            decimal_table.column_ids[payload_column_index],
            id(0xc1),
            "sandbox_decimal_group_payload_cell_policy",
        ));
    }
    for (column_id, policy_id, name) in decimal_cell_policies {
        add_table_policy(
            decimal_table.table_id,
            column_id,
            policy_id,
            1,
            Some(select_using_id),
            None,
            true,
            name,
        )?;
    }
    add_table_policy(
        pressure_table.table_id,
        pressure_table.column_ids[0],
        id(0xe4),
        1,
        Some(select_using_id),
        None,
        false,
        "sandbox_analytics_pressure_select_policy",
    )?;
    add_table_policy(
        pressure_table.table_id,
        pressure_table.column_ids[0],
        id(0xe5),
        1,
        Some(select_using_id),
        None,
        true,
        "sandbox_analytics_pressure_cell_select_policy",
    )?;
    add_table_policy(
        pressure_table.table_id,
        pressure_table.column_ids[1],
        id(0xa8),
        1,
        Some(select_using_id),
        None,
        false,
        "sandbox_analytics_pressure_measure_select_policy",
    )?;
    add_table_policy(
        pressure_table.table_id,
        pressure_table.column_ids[1],
        id(0xa9),
        1,
        Some(select_using_id),
        None,
        true,
        "sandbox_analytics_pressure_measure_cell_select_policy",
    )?;
    add_table_policy(
        range_benchmark_table.table_id,
        range_benchmark_table.column_ids[0],
        id(0xb7),
        1,
        Some(select_using_id),
        None,
        false,
        "sandbox_range_benchmark_select_policy",
    )?;
    add_table_policy(
        range_benchmark_table.table_id,
        range_benchmark_table.column_ids[0],
        id(0xb8),
        1,
        Some(select_using_id),
        None,
        true,
        "sandbox_range_benchmark_cell_select_policy",
    )?;
    add_table_policy(
        composite_table.table_id,
        composite_table.column_ids[0],
        id(0xda),
        3,
        Some(update_using_id),
        Some(insert_policy_expression_id),
        false,
        "sandbox_composite_update_policy",
    )?;
    for (column_id, policy_id, name) in [
        (
            composite_table.column_ids[0],
            id(0xdb),
            "sandbox_composite_left_update_policy",
        ),
        (
            composite_table.column_ids[1],
            id(0xdc),
            "sandbox_composite_right_update_policy",
        ),
    ] {
        add_table_policy(
            composite_table.table_id,
            column_id,
            policy_id,
            3,
            Some(update_using_id),
            Some(insert_policy_expression_id),
            true,
            name,
        )?;
    }
    let policy_invocation = id(0x87);
    let mut policy_audit = identity_audit_event(0x88, user_id, insert_policy_id);
    policy_audit.correlation_id = policy_invocation;
    storage
        .commit_authorized_policy_ddl(
            policy_invocation,
            user_id,
            now + 3_600_000_000,
            &policy_writes,
            &[policy_audit],
        )
        .map_err(|error| format!("metadata fixture policies failed: {error:?}"))?;
    if spill_redacted_only {
        let redacted_policy_invocation = id(0xac);
        let mut redacted_policy_audit =
            identity_audit_event(0xad, user_id, decimal_table.column_ids[2]);
        redacted_policy_audit.correlation_id = redacted_policy_invocation;
        storage
            .create_policy(CreatePolicyRequest {
                invocation_id: redacted_policy_invocation,
                authenticated_actor_id: user_id,
                table_id: decimal_table.table_id,
                name: "sandbox_spill_redacted_sort_key_policy".to_owned(),
                kind: PolicyKind::Cell,
                operation: prdb_engine::authorization::Operation::Select,
                effect: prdb_engine::authorization::Effect::Restrict,
                recipients: vec![prdb_engine::authorization::Subject::Principal(user_id)],
                target_column_ids: vec![decimal_table.column_ids[2]],
                using_expression: Some(prdb_engine::authorization::Expr::Literal(Some(
                    ScalarValue::Boolean(false),
                ))),
                with_check_expression: None,
                retention_deadline_us: now + 3_600_000_000,
                audit_event: redacted_policy_audit,
            })
            .map_err(|error| format!("create spill redaction policy: {error:?}"))?;
    }
    if sql_in_denied_key_only {
        let denied_key_policy_invocation = id(0xae);
        let mut denied_key_policy_audit =
            identity_audit_event(0xaf, user_id, composite_table.column_ids[0]);
        denied_key_policy_audit.correlation_id = denied_key_policy_invocation;
        storage
            .create_policy(CreatePolicyRequest {
                invocation_id: denied_key_policy_invocation,
                authenticated_actor_id: user_id,
                table_id: composite_table.table_id,
                name: "sandbox_sql_in_denied_index_key_policy".to_owned(),
                kind: PolicyKind::Cell,
                operation: prdb_engine::authorization::Operation::Select,
                effect: prdb_engine::authorization::Effect::Restrict,
                recipients: vec![prdb_engine::authorization::Subject::Principal(user_id)],
                target_column_ids: vec![composite_table.column_ids[0]],
                using_expression: Some(prdb_engine::authorization::Expr::Literal(Some(
                    ScalarValue::Boolean(false),
                ))),
                with_check_expression: None,
                retention_deadline_us: now + 3_600_000_000,
                audit_event: denied_key_policy_audit,
            })
            .map_err(|error| format!("create SQL IN denied-key policy: {error:?}"))?;
    }
    let identity_policy_invocation = id(0xfa);
    let mut identity_policy_audit = identity_audit_event(0xfa, user_id, identity_table.table_id);
    identity_policy_audit.correlation_id = identity_policy_invocation;
    storage.create_policy(CreatePolicyRequest {
        invocation_id: identity_policy_invocation,
        authenticated_actor_id: user_id,
        table_id: identity_table.table_id,
        name: "sandbox_generated_identity_insert_policy".to_owned(),
        kind: PolicyKind::Row,
        operation: prdb_engine::authorization::Operation::Insert,
        effect: prdb_engine::authorization::Effect::Allow,
        recipients: vec![prdb_engine::authorization::Subject::Principal(user_id)],
        target_column_ids: Vec::new(),
        using_expression: None,
        with_check_expression: Some(prdb_engine::authorization::Expr::Literal(Some(
            ScalarValue::Boolean(true),
        ))),
        retention_deadline_us: now + 3_600_000_000,
        audit_event: identity_policy_audit,
    })?;
    let identity_cell_policy_invocation = id(0xfb);
    let mut identity_cell_policy_audit =
        identity_audit_event(0xfc, user_id, identity_table.column_ids[1]);
    identity_cell_policy_audit.correlation_id = identity_cell_policy_invocation;
    storage.create_policy(CreatePolicyRequest {
        invocation_id: identity_cell_policy_invocation,
        authenticated_actor_id: user_id,
        table_id: identity_table.table_id,
        name: "sandbox_generated_label_insert_policy".to_owned(),
        kind: PolicyKind::Cell,
        operation: prdb_engine::authorization::Operation::Insert,
        effect: prdb_engine::authorization::Effect::Allow,
        recipients: vec![prdb_engine::authorization::Subject::Principal(user_id)],
        target_column_ids: vec![identity_table.column_ids[1]],
        using_expression: None,
        with_check_expression: Some(prdb_engine::authorization::Expr::Literal(Some(
            ScalarValue::Boolean(true),
        ))),
        retention_deadline_us: now + 3_600_000_000,
        audit_event: identity_cell_policy_audit,
    })?;
    for (invocation_byte, audit_byte, operation, using_expression, with_check_expression, name) in [
        (
            0x40,
            0x41,
            prdb_engine::authorization::Operation::Select,
            Some(prdb_engine::authorization::Expr::Literal(Some(
                ScalarValue::Boolean(true),
            ))),
            None,
            "sandbox_operational_pressure_select_policy",
        ),
        (
            0x42,
            0x43,
            prdb_engine::authorization::Operation::Update,
            Some(prdb_engine::authorization::Expr::Literal(Some(
                ScalarValue::Boolean(true),
            ))),
            Some(prdb_engine::authorization::Expr::Literal(Some(
                ScalarValue::Boolean(true),
            ))),
            "sandbox_operational_pressure_update_policy",
        ),
    ] {
        let invocation_id = id(invocation_byte);
        let mut audit_event =
            identity_audit_event(audit_byte, user_id, operational_pressure_table.table_id);
        audit_event.correlation_id = invocation_id;
        storage
            .create_policy(CreatePolicyRequest {
                invocation_id,
                authenticated_actor_id: user_id,
                table_id: operational_pressure_table.table_id,
                name: name.to_owned(),
                kind: PolicyKind::Row,
                operation,
                effect: prdb_engine::authorization::Effect::Allow,
                recipients: vec![prdb_engine::authorization::Subject::Principal(user_id)],
                target_column_ids: Vec::new(),
                using_expression,
                with_check_expression,
                retention_deadline_us: now + 3_600_000_000,
                audit_event,
            })
            .map_err(|error| format!("create operational pressure policy {name}: {error:?}"))?;
    }
    for (column_index, column_id) in operational_pressure_column_ids.into_iter().enumerate() {
        let operations = if column_index == 1 {
            vec![
                (
                    0x44,
                    0x45,
                    prdb_engine::authorization::Operation::Select,
                    Some(prdb_engine::authorization::Expr::Literal(Some(
                        ScalarValue::Boolean(true),
                    ))),
                    None,
                ),
                (
                    0x46,
                    0x47,
                    prdb_engine::authorization::Operation::Update,
                    Some(prdb_engine::authorization::Expr::Literal(Some(
                        ScalarValue::Boolean(true),
                    ))),
                    Some(prdb_engine::authorization::Expr::Literal(Some(
                        ScalarValue::Boolean(true),
                    ))),
                ),
            ]
        } else {
            vec![(
                0x48,
                0x49,
                prdb_engine::authorization::Operation::Select,
                Some(prdb_engine::authorization::Expr::Literal(Some(
                    ScalarValue::Boolean(true),
                ))),
                None,
            )]
        };
        for (invocation_byte, audit_byte, operation, using_expression, with_check_expression) in
            operations
        {
            let invocation_id = id(invocation_byte);
            let mut audit_event =
                identity_audit_event(audit_byte, user_id, operational_pressure_table_id);
            audit_event.correlation_id = invocation_id;
            storage.create_policy(CreatePolicyRequest {
                invocation_id,
                authenticated_actor_id: user_id,
                table_id: operational_pressure_table_id,
                name: format!("sandbox_operational_pressure_column_{column_index}_policy_{invocation_byte:02x}"),
                kind: PolicyKind::Cell,
                operation,
                effect: prdb_engine::authorization::Effect::Allow,
                recipients: vec![prdb_engine::authorization::Subject::Principal(user_id)],
                target_column_ids: vec![column_id],
                using_expression,
                with_check_expression,
                retention_deadline_us: now + 3_600_000_000,
                audit_event,
            }).map_err(|error| format!("create operational pressure cell policy {column_index}/{operation:?}: {error:?}"))?;
        }
    }
    let range_index_invocation = id(0xd7);
    let mut range_index_audit = identity_audit_event(0xd8, user_id, composite_table.table_id);
    range_index_audit.correlation_id = range_index_invocation;
    storage.create_index(prdb_engine::storage_manager::CreateIndexRequest {
        invocation_id: range_index_invocation,
        authenticated_actor_id: user_id,
        table_id: composite_table.table_id,
        name: "grouped_left_key_range".to_owned(),
        column_ids: vec![composite_table.column_ids[0]],
        unique: false,
        retention_deadline_us: now + 3_600_000_000,
        audit_event: range_index_audit,
    })?;
    let composite_join_index_invocation = id(0xb7);
    let mut composite_join_index_audit =
        identity_audit_event(0xb8, user_id, composite_table.table_id);
    composite_join_index_audit.correlation_id = composite_join_index_invocation;
    storage.create_index(prdb_engine::storage_manager::CreateIndexRequest {
        invocation_id: composite_join_index_invocation,
        authenticated_actor_id: user_id,
        table_id: composite_table.table_id,
        name: "grouped_left_right_composite".to_owned(),
        column_ids: composite_table.column_ids[..2].to_vec(),
        unique: false,
        retention_deadline_us: now + 3_600_000_000,
        audit_event: composite_join_index_audit,
    })?;
    let range_benchmark_index_invocation = id(0xb5);
    let mut range_benchmark_index_audit =
        identity_audit_event(0xb6, user_id, range_benchmark_table.table_id);
    range_benchmark_index_audit.correlation_id = range_benchmark_index_invocation;
    storage.create_index(prdb_engine::storage_manager::CreateIndexRequest {
        invocation_id: range_benchmark_index_invocation,
        authenticated_actor_id: user_id,
        table_id: range_benchmark_table.table_id,
        name: "analytics_range_benchmark_key".to_owned(),
        column_ids: vec![range_benchmark_table.column_ids[0]],
        unique: false,
        retention_deadline_us: now + 3_600_000_000,
        audit_event: range_benchmark_index_audit,
    })?;
    let records_index_invocation = id(0xf8);
    let mut records_index_audit = identity_audit_event(0xf9, user_id, records_table.table_id);
    records_index_audit.correlation_id = records_index_invocation;
    storage.create_index(prdb_engine::storage_manager::CreateIndexRequest {
        invocation_id: records_index_invocation,
        authenticated_actor_id: user_id,
        table_id: records_table.table_id,
        name: "records_id_range".to_owned(),
        column_ids: vec![records_table.column_ids[0]],
        unique: false,
        retention_deadline_us: now + 3_600_000_000,
        audit_event: records_index_audit,
    })?;
    if indexed_join_asymmetric_benchmark_only
        || indexed_join_skew_benchmark_only
        || indexed_join_fanout_benchmark_only
        || indexed_join_skewed_fanout_benchmark_only
    {
        let join_inner_index_invocation = id(0xc8);
        let mut join_inner_index_audit = identity_audit_event(0xc9, user_id, joined_table.table_id);
        join_inner_index_audit.correlation_id = join_inner_index_invocation;
        storage.create_index(prdb_engine::storage_manager::CreateIndexRequest {
            invocation_id: join_inner_index_invocation,
            authenticated_actor_id: user_id,
            table_id: joined_table.table_id,
            name: "linked_match_key_asymmetric_benchmark".to_owned(),
            column_ids: vec![joined_table.column_ids[1]],
            unique: false,
            retention_deadline_us: now + 3_600_000_000,
            audit_event: join_inner_index_audit,
        })?;
    }
    let (mut estimate_invocation, _) = storage.begin_token_authenticated_invocation(
        id(0xca),
        token_id,
        &secret,
        now + 3_600_000_000,
        128,
        1_048_576,
    )?;
    let joined_pairs = estimate_invocation
        .open_planner_metadata_connection()
        .estimate_join_cardinality_for_plan(
            "sql_roundtrip",
            "grouped_records",
            "linked_records",
            &composite_table.column_ids[..2],
            &joined_table.column_ids[..2],
            prdb_engine::statistics::MAX_JOIN_ESTIMATE_ROWS,
        )?;
    let expected_join_pairs = if indexed_join_skewed_high_fanout_benchmark_only {
        128
    } else if indexed_join_skewed_fanout_benchmark_only {
        16
    } else if indexed_join_fanout_benchmark_only {
        u64::from(
            indexed_join_fanout_outer_rows * indexed_join_fanout
                + u16::from(
                    indexed_join_fanout_seventeen_over_threshold_benchmark_only
                        || indexed_join_fanout_eighteen_over_threshold_benchmark_only,
                ),
        )
    } else if indexed_join_skew_benchmark_only {
        1024
    } else if indexed_join_asymmetric_benchmark_only || indexed_join_benchmark_only {
        512
    } else {
        2
    };
    assert_eq!(
        joined_pairs,
        if sql_in_denied_key_only || sql_in_elevated_budget_only {
            None
        } else if sql_in_over_budget_only {
            Some(126)
        } else {
            Some(expected_join_pairs)
        },
        "the authorized join estimate must return its exact pair count within the configured bound, or remain unknown when an indexed key is restricted"
    );
    estimate_invocation.abort()?;
    let mut requested_scope_records = Vec::<Vec<u8>>::new();
    for schema_id in [schema.schema_id, empty_schema.schema_id] {
        let mut record = vec![1, 1];
        record.extend_from_slice(&schema_id);
        requested_scope_records.push(record);
    }
    for operation in 2u8..=5 {
        let mut tables = if operation == 2 {
            vec![
                &records_table,
                &joined_table,
                &composite_table,
                &decimal_table,
                &pressure_table,
                &range_benchmark_table,
            ]
        } else if operation == 4 {
            vec![&records_table, &joined_table, &composite_table]
        } else {
            vec![&records_table]
        };
        tables.sort_by_key(|table| table.table_id);
        for table in &tables {
            let mut record = vec![operation, 2];
            record.extend_from_slice(&table.table_id);
            requested_scope_records.push(record);
        }
        if operation <= 4 {
            for table in &tables {
                for column_id in &table.column_ids {
                    let mut record = vec![operation, 3];
                    record.extend_from_slice(&table.table_id);
                    record.extend_from_slice(column_id);
                    requested_scope_records.push(record);
                }
            }
        }
    }
    requested_scope_records.sort();
    requested_scope_records.dedup();
    let scope_count = u16::try_from(requested_scope_records.len())?;
    let mut requested_scope = b"PSC1".to_vec();
    requested_scope.extend_from_slice(&scope_count.to_be_bytes());
    for record in requested_scope_records {
        requested_scope.extend_from_slice(&record);
    }

    let mut client = ClientConnectionSession::default();
    client.accept_hello(ClientMode::Query, "sandbox-sql-write-read")?;
    client.authenticate(&storage, token_id, &secret)?;
    let owner = client.staging_owner()?;
    let staging = StagingStore::new(
        StagingLimits {
            max_active_analytical: 9,
            max_active_analytical_per_principal: 8,
            ..StagingLimits::default()
        },
        validator_for_scope(requested_scope.clone()),
    )?;
    let reservation = staging.prepare(
        owner,
        PrepareSubmission {
            manifest_version: 1,
            manifest: manifest.clone(),
            artifact_bytes: u64::try_from(artifact.len())?,
            artifact_sha256: Sha256::digest(&artifact).into(),
            scope_version: 1,
            requested_scope: requested_scope.clone(),
            parameter_bytes: 0,
            parameter_sha256: Sha256::digest([]).into(),
            deadline_ms: 120_000,
            workload_class: WorkloadClass::Analytical,
        },
    )?;
    let mut artifact_offset = 0;
    for chunk in artifact.chunks(prdb_engine::supervisor::MAX_CHUNK_BYTES) {
        artifact_offset = staging
            .upload_chunk(
                owner,
                reservation.invocation_id,
                ChunkKind::Artifact,
                artifact_offset,
                chunk,
            )
            .map_err(|error| format!("SQL artifact chunk upload: {error:?}"))?;
    }
    staging.seal(owner, reservation.invocation_id)?;
    let (mut operational_only_point_reads, mut operational_only_commits) =
        run_operational_read_commit_samples(
            &storage,
            composite_table.table_id,
            if indexed_join_skew_benchmark_only {
                8
            } else if indexed_join_fanout_benchmark_only {
                usize::from(indexed_join_fanout_outer_rows)
            } else if indexed_join_asymmetric_benchmark_only {
                4
            } else if indexed_join_benchmark_only {
                64
            } else if sql_in_elevated_budget_only {
                1_028
            } else if sql_in_over_budget_only {
                129
            } else if sql_in_null_left_only {
                6
            } else {
                5
            },
            token_id,
            secret,
            now + 3_600_000_000,
            0xd0,
        )?;
    operational_only_point_reads.sort_unstable();
    operational_only_commits.sort_unstable();

    let invocation_hex = reservation
        .invocation_id
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect::<String>();
    let spill_test_root = std::env::var_os("PRDB_TEST_SPILL_TEST_ROOT").map(PathBuf::from);
    if spill_disk_full_only && spill_test_root.is_none() {
        return Err("PRDB_TEST_SPILL_TEST_ROOT is required for the disk-full acceptance".into());
    }
    let spill_root = spill_test_root.unwrap_or_else(|| fixture.0.clone());
    let spill_parent = if spill_enabled_only {
        use std::os::unix::fs::PermissionsExt;

        if !spill_root.is_absolute() || !spill_root.is_dir() {
            return Err(
                "PRDB_TEST_SPILL_TEST_ROOT must name an existing absolute directory".into(),
            );
        }
        let path = spill_root.join("spill-parent");
        std::fs::create_dir(&path)?;
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o700))?;
        Some(path)
    } else {
        None
    };
    let spill_broker = if let Some(parent) = spill_parent.as_ref() {
        Some(
            prdb_engine::spill::SpillBroker::open(prdb_engine::spill::SpillConfig {
                parent: parent.clone(),
                max_bytes_per_invocation: if spill_quota_only { 1 } else { 8 * 1024 * 1024 },
                max_io_bytes_per_invocation: if spill_pressure_only || spill_pressure_cancel_only {
                    1024 * 1024 * 1024
                } else {
                    32 * 1024 * 1024
                },
                max_streams_per_invocation: if spill_join_only
                    || spill_join_decimal_only
                    || spill_join_filtered_only
                    || spill_join_parameter_only
                    || spill_join_bigint_only
                    || spill_join_oversize_only
                    || spill_join_text_only
                    || spill_redacted_only
                {
                    17
                } else {
                    16
                },
                max_operations_per_invocation: 4096,
                max_response_bytes: 64 * 1024,
                max_service_bytes: 16 * 1024 * 1024,
            })
            .map_err(|error| format!("open spill test broker: {error:?}"))?,
        )
    } else {
        None
    };
    let spill_disk_full = if spill_disk_full_only {
        let parent = spill_parent
            .as_ref()
            .ok_or("disk-full acceptance must have a spill parent")?;
        Some(SpillDiskFullFixture::new(&spill_root, parent))
    } else {
        None
    };
    let runtime = GatewayExecutorConfig {
        cgroup_parent,
        resource_limits: prdb_engine::linux_sandbox::WorkerResourceLimits {
            cpu_quota_micros: 500_000,
            cpu_period_micros: 100_000,
            memory_max_bytes: 256 * 1024 * 1024,
            max_processes: 32,
        },
        analytical_resource_limits: None,
        launch: prdb_engine::executor_launcher::ExecutorLaunchConfig {
            executor_path: PathBuf::from(env!("CARGO_BIN_EXE_prdb-executor")),
            optimizer_path: PathBuf::from(env!("CARGO_BIN_EXE_prdb-optimizer")),
            scratch_parent: std::env::temp_dir(),
            runtime_read_execute: required_paths("PRDB_TEST_RUNTIME_READ_EXECUTE")?,
            runtime_read_only: required_paths("PRDB_TEST_RUNTIME_READ_ONLY")?,
            spill_broker,
            max_total_result_bytes: if sql_in_elevated_budget_only {
                1
            } else {
                prdb_engine::query_abi::MAX_TOTAL_RESULT_BYTES
            },
            analytical_max_total_result_bytes: if elevated_result_only {
                Some(192 * 1024 * 1024)
            } else {
                sql_in_elevated_budget_only
                    .then_some(prdb_engine::query_abi::MAX_TOTAL_RESULT_BYTES)
            },
            max_sql_index_probe_candidates:
                prdb_engine::query_abi::DEFAULT_SQL_INDEX_PROBE_CANDIDATES,
            analytical_max_sql_index_probe_candidates: if sql_in_elevated_budget_only {
                Some(2_048)
            } else if indexed_join_asymmetric_benchmark_only
                || std::env::var_os("PRDB_TEST_INDEXED_JOIN_BENCHMARK_ONLY").is_some()
            {
                Some(512)
            } else {
                None
            },
            cleanup_timeout: Duration::from_secs(2),
        },
    };
    if let Some(spill_disk_full) = spill_disk_full.as_ref() {
        spill_disk_full.fill_until_below_eight_kibibytes()?;
    }
    let filtered_composite_only = std::env::var_os("PRDB_TEST_FILTERED_COMPOSITE_ONLY").is_some();
    let count_column_only = std::env::var_os("PRDB_TEST_COUNT_COLUMN_ONLY").is_some();
    if std::env::var_os("PRDB_TEST_ORDERED_ANALYTICAL_ONLY").is_some()
        || indexed_join_benchmark_only
        || filtered_composite_only
        || count_column_only
        || count_bigint_only
        || spill_quota_only
        || spill_only
        || spill_disabled_only
        || spill_sort_only
        || spill_decimal_only
        || spill_redacted_only
        || spill_join_only
        || spill_join_decimal_only
        || spill_join_filtered_only
        || spill_join_parameter_only
        || spill_join_bigint_only
        || spill_join_oversize_only
        || spill_join_text_only
        || spill_crash_only
        || spill_pressure_only
        || spill_pressure_cancel_only
        || spill_disk_full_only
        || sql_in_any_only
        || elevated_result_only
    {
        let mut ordered_client = ClientConnectionSession::default();
        ordered_client.accept_hello(ClientMode::Query, "sandbox-ordered-analytical-sum")?;
        ordered_client.authenticate(&storage, token_id, &secret)?;
        let ordered_owner = ordered_client.staging_owner()?;
        let ordered_parameter = [if indexed_join_fanout_benchmark_only {
            if indexed_join_skewed_high_fanout_benchmark_only {
                b'h'
            } else if indexed_join_skewed_fanout_benchmark_only {
                b's'
            } else if indexed_join_fanout_seventeen_ratio7_benchmark_only {
                b'z'
            } else if indexed_join_fanout_seventeen_threshold_benchmark_only {
                b'a'
            } else if indexed_join_fanout_seventeen_over_threshold_benchmark_only {
                b'b'
            } else if indexed_join_fanout_seventeen_benchmark_only {
                b'Y'
            } else if indexed_join_fanout_eighteen_ratio7_benchmark_only {
                b'n'
            } else if indexed_join_fanout_nineteen_ratio7_benchmark_only {
                b'p'
            } else if indexed_join_fanout_nineteen_over_cutoff_benchmark_only {
                b'r'
            } else if indexed_join_fanout_eighteen_threshold_benchmark_only {
                b'j'
            } else if indexed_join_fanout_eighteen_over_threshold_benchmark_only {
                b'k'
            } else if indexed_join_fanout_seventeen_large_inner_benchmark_only {
                b'W'
            } else if indexed_join_fanout_seventeen_cap_inner_benchmark_only {
                b'E'
            } else if indexed_join_fanout_seventeen_4097_benchmark_only {
                b'q'
            } else if indexed_join_fanout_sixteen_benchmark_only {
                b'X'
            } else if indexed_join_fanout == 2 {
                b'2'
            } else if indexed_join_fanout == 3 {
                b'3'
            } else if indexed_join_fanout_nine_ratio8_benchmark_only {
                b'c'
            } else if indexed_join_fanout == 4 {
                b'4'
            } else if indexed_join_fanout == 5 {
                b'5'
            } else {
                b'9'
            }
        } else if indexed_join_skewed_fanout_benchmark_only {
            b's'
        } else if indexed_join_skew_benchmark_only {
            b'8'
        } else if indexed_join_asymmetric_benchmark_only {
            b'7'
        } else if indexed_join_benchmark_only {
            b'6'
        } else if spill_limit_only {
            b'L'
        } else if spill_disk_full_only {
            b'F'
        } else if spill_only || spill_quota_only || spill_disabled_only {
            b'S'
        } else if spill_sort_only {
            b'O'
        } else if spill_decimal_only {
            b'D'
        } else if spill_redacted_only {
            b'E'
        } else if spill_join_decimal_only {
            b'T'
        } else if spill_join_filtered_only {
            b'U'
        } else if spill_join_parameter_only {
            b'V'
        } else if spill_join_text_only {
            b'H'
        } else if spill_join_bigint_only {
            b'M'
        } else if spill_join_oversize_only {
            b'Y'
        } else if spill_join_only {
            b'J'
        } else if spill_crash_only {
            b'X'
        } else if spill_pressure_only {
            b'P'
        } else if spill_pressure_cancel_only {
            b'K'
        } else if count_column_only {
            b'V'
        } else if count_bigint_only {
            b'I'
        } else if sql_in_any_only {
            if sql_in_fallback_only {
                b'$'
            } else if sql_in_null_left_only {
                b'&'
            } else if sql_in_cancel_only {
                b'^'
            } else if sql_in_denied_key_only {
                b'?'
            } else if sql_in_over_budget_only {
                b'@'
            } else if sql_in_elevated_budget_only {
                b'~'
            } else {
                b'#'
            }
        } else if elevated_result_only {
            b'!'
        } else if filtered_composite_only {
            b'Q'
        } else {
            b'W'
        }];
        let ordered_reservation = staging.prepare(
            ordered_owner,
            PrepareSubmission {
                manifest_version: 1,
                manifest: manifest.clone(),
                artifact_bytes: u64::try_from(artifact.len())?,
                artifact_sha256: Sha256::digest(&artifact).into(),
                scope_version: 1,
                requested_scope: requested_scope.clone(),
                parameter_bytes: 1,
                parameter_sha256: Sha256::digest(ordered_parameter).into(),
                deadline_ms: 30_000,
                workload_class: WorkloadClass::Analytical,
            },
        )?;
        let mut ordered_offset = 0;
        for chunk in artifact.chunks(prdb_engine::supervisor::MAX_CHUNK_BYTES) {
            ordered_offset = staging.upload_chunk(
                ordered_owner,
                ordered_reservation.invocation_id,
                ChunkKind::Artifact,
                ordered_offset,
                chunk,
            )?;
        }
        staging.upload_chunk(
            ordered_owner,
            ordered_reservation.invocation_id,
            ChunkKind::Parameters,
            0,
            &ordered_parameter,
        )?;
        staging.seal(ordered_owner, ordered_reservation.invocation_id)?;
        let ordered_invocation_hex = ordered_reservation
            .invocation_id
            .iter()
            .map(|byte| format!("{byte:02x}"))
            .collect::<String>();
        let ordered_query_started_at = std::time::Instant::now();
        let (ordered_accepted, mut ordered_invocation, mut ordered_executor) = ordered_client
            .handle_start_request_with_executor(
                &storage,
                &staging,
                ClientFrame {
                    kind: 7,
                    request_id: 90,
                    response_sequence: 0,
                    payload: format!("{{\"invocation_id\":\"{ordered_invocation_hex}\"}}")
                        .into_bytes(),
                },
                GatewayAdmissionConfig {
                    retention_period_us: 3_600_000_000,
                    max_rows: if spill_pressure_only || spill_pressure_cancel_only {
                        2048
                    } else {
                        128
                    },
                    max_bytes: 1_048_576,
                },
                &runtime,
            )?;
        assert_eq!(ordered_accepted.payload, b"{\"state\":\"accepted\"}");
        let (mut ordered_service, mut ordered_gateway) = UnixStream::pair()?;
        ordered_gateway.set_read_timeout(Some(Duration::from_secs(10)))?;
        let (pressure_trigger_tx, pressure_trigger_rx) = std::sync::mpsc::channel();
        let (cancel_trigger_tx, cancel_trigger_rx) = std::sync::mpsc::channel();
        let join_timing_observed_at = Arc::new(std::sync::Mutex::new(Vec::new()));
        let join_timing_observed_at_reader = Arc::clone(&join_timing_observed_at);
        let elevated_result_reader = elevated_result_only;
        let expected_large_result_row = if elevated_result_only {
            let mut row = b"SQLR".to_vec();
            row.extend_from_slice(&1u16.to_be_bytes());
            row.extend_from_slice(&11u16.to_be_bytes());
            for _ in 0..11 {
                row.extend_from_slice(&13u16.to_be_bytes());
                row.extend_from_slice(b"large_payload");
                row.extend_from_slice(&6u32.to_be_bytes());
                row.extend_from_slice(&[0, 0, 0]);
                row.extend_from_slice(&16_350u32.to_be_bytes());
                row.extend(std::iter::repeat_n(b'x', 16_350));
            }
            Some(row)
        } else {
            None
        };
        let mut pressure_trigger_tx =
            (spill_pressure_only || spill_pressure_cancel_only || indexed_join_benchmark_only)
                .then_some(pressure_trigger_tx);
        let mut cancel_trigger_tx = spill_pressure_cancel_only.then_some(cancel_trigger_tx);
        let ordered_reader = std::thread::spawn(move || {
            let mut frames = Vec::new();
            let mut discarded_result_bytes = 0u64;
            let mut discarded_result_frames = 0usize;
            let mut discarded_result_rows = 0usize;
            let mut expected_row_offset = 0usize;
            let mut first_discarded_result_prefix = Vec::new();
            let expected_large_result_row = expected_large_result_row;
            while let Some(frame) = prdb_engine::ipc::read_frame(&mut ordered_gateway)? {
                if frame.operation == Operation::GatewayResultData as u16
                    && frame.payload.len() == 16
                    && &frame.payload[..7] == b"OP10J01"
                {
                    join_timing_observed_at_reader
                        .lock()
                        .map_err(|_| {
                            prdb_engine::ipc::FrameError::Io(std::io::Error::other(
                                "join timing observer lock poisoned",
                            ))
                        })?
                        .push((frame.payload[7], std::time::Instant::now()));
                }
                if frame.operation == Operation::GatewayResultData as u16
                    && (frame.payload == b"spill-pressure-started"
                        || frame.payload == b"JOIN_BENCHMARK_STARTED")
                {
                    if let Some(trigger) = pressure_trigger_tx.take() {
                        let _ = trigger.send(());
                    }
                    if let Some(trigger) = cancel_trigger_tx.take() {
                        let _ = trigger.send(());
                    }
                }
                if elevated_result_reader && frame.operation == Operation::GatewayResultData as u16
                {
                    if first_discarded_result_prefix.is_empty() {
                        first_discarded_result_prefix
                            .extend_from_slice(&frame.payload[..frame.payload.len().min(64)]);
                    }
                    let expected_row = expected_large_result_row.as_ref().ok_or_else(|| {
                        prdb_engine::ipc::FrameError::Io(std::io::Error::other(
                            "large SQL result verifier is unavailable",
                        ))
                    })?;
                    let mut frame_offset = 0;
                    while frame_offset < frame.payload.len() {
                        let compared = (expected_row.len() - expected_row_offset)
                            .min(frame.payload.len() - frame_offset);
                        if frame.payload[frame_offset..frame_offset + compared]
                            != expected_row[expected_row_offset..expected_row_offset + compared]
                        {
                            return Err(prdb_engine::ipc::FrameError::Io(std::io::Error::new(
                                std::io::ErrorKind::InvalidData,
                                "large SQL result row differs from expected SQLR bytes",
                            )));
                        }
                        frame_offset += compared;
                        expected_row_offset += compared;
                        if expected_row_offset == expected_row.len() {
                            expected_row_offset = 0;
                            discarded_result_rows += 1;
                        }
                    }
                    discarded_result_bytes = discarded_result_bytes
                        .checked_add(u64::try_from(frame.payload.len()).map_err(|_| {
                            prdb_engine::ipc::FrameError::Io(std::io::Error::other(
                                "result frame length exceeds u64",
                            ))
                        })?)
                        .ok_or_else(|| {
                            prdb_engine::ipc::FrameError::Io(std::io::Error::other(
                                "result byte counter overflow",
                            ))
                        })?;
                    discarded_result_frames += 1;
                    continue;
                }
                let terminal = frame.operation == Operation::GatewayTerminal as u16;
                frames.push(frame);
                if terminal {
                    break;
                }
            }
            Ok::<_, prdb_engine::ipc::FrameError>((
                frames,
                discarded_result_bytes,
                discarded_result_frames,
                discarded_result_rows,
                first_discarded_result_prefix,
            ))
        });
        let elevated_result_started_at = elevated_result_only.then(std::time::Instant::now);
        let mut operational_load_interval = None;
        let ordered_decision = if spill_pressure_only
            || spill_pressure_cancel_only
            || indexed_join_benchmark_only
        {
            let query_started_at = std::time::Instant::now();
            let (
                decision,
                (operational_started_at, operational_finished_at, read_count, commit_count),
                query_finished_at,
                cancellation,
            ) = std::thread::scope(|scope| {
                let storage_ref = &storage;
                let operational_table_id = composite_table.table_id;
                let expected_operational_scan_rows = if indexed_join_fanout_benchmark_only
                    || indexed_join_skewed_fanout_benchmark_only
                {
                    usize::from(indexed_join_fanout_outer_rows)
                } else if indexed_join_skew_benchmark_only {
                    8
                } else if indexed_join_asymmetric_benchmark_only {
                    4
                } else if indexed_join_benchmark_only {
                    64
                } else if sql_in_elevated_budget_only {
                    1_028
                } else if sql_in_over_budget_only {
                    129
                } else {
                    5
                };
                let operational_worker = scope.spawn(move || -> Result<_, String> {
                    pressure_trigger_rx
                        .recv_timeout(Duration::from_secs(5))
                        .map_err(|error| {
                            format!("spill pressure marker was not relayed: {error}")
                        })?;
                    let started_at = std::time::Instant::now();
                    let (mut reads, mut commits) = run_operational_read_commit_samples(
                        storage_ref,
                        operational_table_id,
                        expected_operational_scan_rows,
                        token_id,
                        secret,
                        now + 3_600_000_000,
                        0x91,
                    )
                    .map_err(|error| format!("operational load during spill: {error}"))?;
                    if indexed_join_benchmark_only {
                        for sample in 0..7u8 {
                            let namespace = 0x92 + sample;
                            let (additional_reads, additional_commits) =
                                run_operational_read_commit_samples(
                                    storage_ref,
                                    operational_table_id,
                                    expected_operational_scan_rows,
                                    token_id,
                                    secret,
                                    now + 3_600_000_000,
                                    namespace,
                                )
                                .map_err(|error| {
                                    format!(
                                        "additional operational load during join benchmark: {error}"
                                    )
                                })?;
                            reads.extend(additional_reads);
                            commits.extend(additional_commits);
                        }
                    }
                    Ok((
                        started_at,
                        std::time::Instant::now(),
                        reads.len(),
                        commits.len(),
                    ))
                });
                let cancellation_worker = if spill_pressure_cancel_only {
                    let storage_ref = &storage;
                    let invocation_id = ordered_reservation.invocation_id;
                    Some(scope.spawn(move || -> Result<_, String> {
                        cancel_trigger_rx
                            .recv_timeout(Duration::from_secs(5))
                            .map_err(|error| {
                                format!("spill cancellation marker was not relayed: {error}")
                            })?;
                        std::thread::sleep(Duration::from_millis(75));
                        let requested_at = std::time::Instant::now();
                        let result = storage_ref
                            .cancel_owned_invocation(user_id, invocation_id, now + 3_600_000_000)
                            .map_err(|error| {
                                format!("cancel spill pressure invocation: {error:?}")
                            })?;
                        Ok((requested_at, std::time::Instant::now(), result))
                    }))
                } else {
                    None
                };
                let decision = observe_executor_relay_and_finalize_gateway(
                    &storage,
                    &mut ordered_invocation,
                    &mut ordered_executor,
                    &mut ordered_service,
                    ordered_accepted.request_id,
                )?;
                let query_finished_at = std::time::Instant::now();
                let operational = operational_worker
                    .join()
                    .map_err(|_| std::io::Error::other("spill pressure worker panicked"))?
                    .map_err(std::io::Error::other)?;
                let cancellation = cancellation_worker
                    .map(|worker| {
                        worker
                            .join()
                            .map_err(|_| {
                                std::io::Error::other("spill cancellation worker panicked")
                            })?
                            .map_err(std::io::Error::other)
                    })
                    .transpose()?;
                Ok::<_, Box<dyn std::error::Error>>((
                    decision,
                    operational,
                    query_finished_at,
                    cancellation,
                ))
            })?;
            assert!(
                operational_started_at < query_finished_at
                    && operational_finished_at > query_started_at,
                "operational reads/commits did not overlap the spill query"
            );
            let expected_operational_count = if indexed_join_benchmark_only { 512 } else { 64 };
            assert_eq!(
                read_count, expected_operational_count,
                "unexpected operational point read count"
            );
            assert_eq!(
                commit_count, expected_operational_count,
                "unexpected operational commit count"
            );
            operational_load_interval = Some((operational_started_at, operational_finished_at));
            if spill_pressure_cancel_only {
                let (cancel_started_at, cancel_finished_at, result) =
                    cancellation.ok_or("spill pressure cancellation was not observed")?;
                assert_eq!(
                    result,
                    prdb_engine::storage_manager::OwnerCancellationResult::Accepted
                );
                assert!(
                    operational_started_at < cancel_started_at
                        && operational_finished_at > cancel_started_at,
                    "operational reads/commits did not overlap cancellation under spill pressure"
                );
                assert!(
                    cancel_finished_at.duration_since(cancel_started_at) < Duration::from_secs(2)
                );
                assert!(matches!(&decision, GatewayTerminalDecision::Abort(_)));
            }
            if indexed_join_benchmark_only {
                let benchmark_label = if indexed_join_fanout_seventeen_large_inner_benchmark_only
                    || indexed_join_fanout_seventeen_cap_inner_benchmark_only
                    || indexed_join_fanout_seventeen_4097_benchmark_only
                    || indexed_join_fanout_nineteen_ratio7_benchmark_only
                    || indexed_join_fanout_nineteen_over_cutoff_benchmark_only
                {
                    "indexed join"
                } else {
                    "indexed/hash join"
                };
                eprintln!(
                    "{benchmark_label} benchmark invocation overlapped {read_count} operational reads and {commit_count} commits for {} ms",
                    query_finished_at
                        .duration_since(query_started_at)
                        .as_millis(),
                );
            } else {
                eprintln!(
                    "broker-backed SQL sort overlapped {read_count} operational reads and {commit_count} commits for {} ms",
                    query_finished_at
                        .duration_since(query_started_at)
                        .as_millis(),
                );
            }
            decision
        } else {
            observe_executor_relay_and_finalize_gateway(
                &storage,
                &mut ordered_invocation,
                &mut ordered_executor,
                &mut ordered_service,
                ordered_accepted.request_id,
            )?
        };
        let (
            ordered_frames,
            discarded_result_bytes,
            discarded_result_frames,
            discarded_result_rows,
            first_discarded_result_prefix,
        ) = ordered_reader
            .join()
            .map_err(|_| "ordered analytical gateway reader panicked")??;
        if sql_in_elevated_budget_only {
            eprintln!(
                "confined indexed IN over-16-MiB range invocation latency: {:.3} ms",
                ordered_query_started_at.elapsed().as_secs_f64() * 1000.0,
            );
        }
        if let Some(started_at) = elevated_result_started_at {
            assert_eq!(
                discarded_result_bytes,
                800 * (8 + 11 * (26 + 16_350)),
                "unexpected first streamed SQL result payload: {:?}",
                String::from_utf8_lossy(&first_discarded_result_prefix)
            );
            assert_eq!(discarded_result_frames, 800 * 11);
            assert_eq!(discarded_result_rows, 800);
            eprintln!(
                "confined 137 MiB SQL result relay latency: {:.3} ms ({} bytes, {} bounded frames)",
                started_at.elapsed().as_secs_f64() * 1000.0,
                discarded_result_bytes,
                discarded_result_frames,
            );
        }
        if spill_quota_only
            || spill_crash_only
            || spill_redacted_only
            || spill_join_oversize_only
            || spill_disk_full_only
            || spill_pressure_cancel_only
            || sql_in_cancel_only
            || sql_in_denied_key_only
        {
            assert!(
                matches!(ordered_decision, GatewayTerminalDecision::Abort(_)),
                "spill quota, filesystem exhaustion, cancellation, denied SQL IN key, redacted key, oversized joined projection, or worker crash must abort; frames: {:?}",
                ordered_frames
                    .iter()
                    .map(|frame| (
                        frame.operation,
                        String::from_utf8_lossy(&frame.payload).into_owned()
                    ))
                    .collect::<Vec<_>>()
            );
            if spill_quota_only
                || spill_redacted_only
                || spill_join_oversize_only
                || spill_disk_full_only
                || sql_in_cancel_only
                || sql_in_denied_key_only
            {
                assert!(
                    ordered_frames
                        .iter()
                        .all(|frame| { frame.operation != Operation::GatewayResultData as u16 }),
                    "expected no provisional data frames: {:?}",
                    ordered_frames
                        .iter()
                        .map(|frame| (
                            frame.operation,
                            String::from_utf8_lossy(&frame.payload).into_owned()
                        ))
                        .collect::<Vec<_>>()
                );
            }
        } else {
            assert_eq!(
                ordered_decision,
                GatewayTerminalDecision::Commit,
                "confined analytical fixture failed; frames: {:?}",
                ordered_frames
                    .iter()
                    .map(|frame| (
                        frame.operation,
                        String::from_utf8_lossy(&frame.payload).into_owned()
                    ))
                    .collect::<Vec<_>>()
            );
        }
        staging.finish_started(ordered_owner, ordered_reservation.invocation_id)?;
        let mut ordered_frames = ordered_frames.into_iter();
        if elevated_result_only {
            assert_eq!(
                ordered_frames.len(),
                1,
                "result relay retained payload frames"
            );
        } else if spill_only || spill_disabled_only || spill_limit_only {
            fn expected_spilled_group_count(left_key: Option<i32>, count: i64) -> Vec<u8> {
                let mut output = b"SQLR".to_vec();
                output.extend_from_slice(&1u16.to_be_bytes());
                output.extend_from_slice(&2u16.to_be_bytes());
                output.extend_from_slice(&(b"left_key".len() as u16).to_be_bytes());
                output.extend_from_slice(b"left_key");
                output.extend_from_slice(&3u32.to_be_bytes());
                if let Some(left_key) = left_key {
                    output.extend_from_slice(&[0, 0, 0]);
                    output.extend_from_slice(&4u32.to_be_bytes());
                    output.extend_from_slice(&left_key.to_be_bytes());
                } else {
                    output.extend_from_slice(&[0, 0, 1]);
                    output.extend_from_slice(&0u32.to_be_bytes());
                }
                output.extend_from_slice(&(b"count".len() as u16).to_be_bytes());
                output.extend_from_slice(b"count");
                output.extend_from_slice(&4u32.to_be_bytes());
                output.extend_from_slice(&[0, 0, 0]);
                output.extend_from_slice(&8u32.to_be_bytes());
                output.extend_from_slice(&count.to_be_bytes());
                output
            }
            let expected_groups = if spill_only || spill_limit_only {
                [(None, 2), (Some(1), 2), (Some(2), 3), (Some(3), 1)]
            } else {
                [(Some(1), 2), (Some(2), 3), (Some(3), 1), (None, 2)]
            };
            for (left_key, count) in
                expected_groups
                    .into_iter()
                    .take(if spill_limit_only { 2 } else { 4 })
            {
                let frame = ordered_frames
                    .next()
                    .ok_or("missing confined spilled GROUP BY row")?;
                assert_eq!(frame.operation, Operation::GatewayResultData as u16);
                assert_eq!(frame.request_id, ordered_accepted.request_id);
                assert_eq!(frame.payload, expected_spilled_group_count(left_key, count));
            }
        } else if spill_sort_only {
            fn expected_spilled_order_key(left_key: Option<i32>) -> Vec<u8> {
                let mut output = b"SQLR".to_vec();
                output.extend_from_slice(&1u16.to_be_bytes());
                output.extend_from_slice(&1u16.to_be_bytes());
                output.extend_from_slice(&(b"left_key".len() as u16).to_be_bytes());
                output.extend_from_slice(b"left_key");
                output.extend_from_slice(&3u32.to_be_bytes());
                if let Some(left_key) = left_key {
                    output.extend_from_slice(&[0, 0, 0]);
                    output.extend_from_slice(&4u32.to_be_bytes());
                    output.extend_from_slice(&left_key.to_be_bytes());
                } else {
                    output.extend_from_slice(&[0, 0, 1]);
                    output.extend_from_slice(&0u32.to_be_bytes());
                }
                output
            }
            for left_key in [
                None,
                None,
                Some(3),
                Some(2),
                Some(2),
                Some(2),
                Some(1),
                Some(1),
            ] {
                let frame = ordered_frames
                    .next()
                    .ok_or("missing confined spilled ORDER BY row")?;
                assert_eq!(frame.operation, Operation::GatewayResultData as u16);
                assert_eq!(frame.request_id, ordered_accepted.request_id);
                assert_eq!(frame.payload, expected_spilled_order_key(left_key));
            }
        } else if spill_decimal_only {
            fn expected_spilled_decimal(value: Option<i128>) -> Vec<u8> {
                let mut output = b"SQLR".to_vec();
                output.extend_from_slice(&1u16.to_be_bytes());
                output.extend_from_slice(&1u16.to_be_bytes());
                output.extend_from_slice(&(b"decimal_value".len() as u16).to_be_bytes());
                output.extend_from_slice(b"decimal_value");
                output.extend_from_slice(&5u32.to_be_bytes());
                output.extend_from_slice(&[4, 2]);
                if let Some(value) = value {
                    output.push(0);
                    output.extend_from_slice(&16u32.to_be_bytes());
                    output.extend_from_slice(&value.to_be_bytes());
                } else {
                    output.push(1);
                    output.extend_from_slice(&0u32.to_be_bytes());
                }
                output
            }
            for value in [
                None,
                None,
                None,
                None,
                Some(225),
                Some(125),
                Some(100),
                Some(100),
            ] {
                let frame = ordered_frames
                    .next()
                    .ok_or("missing confined spilled Decimal ORDER BY row")?;
                assert_eq!(frame.operation, Operation::GatewayResultData as u16);
                assert_eq!(frame.request_id, ordered_accepted.request_id);
                assert_eq!(frame.payload, expected_spilled_decimal(value));
            }
        } else if spill_pressure_cancel_only {
            let marker = ordered_frames
                .next()
                .ok_or("missing spill pressure start marker before cancellation")?;
            assert_eq!(marker.operation, Operation::GatewayResultData as u16);
            assert_eq!(marker.payload, b"spill-pressure-started");
            assert!(
                ordered_frames
                    .as_slice()
                    .iter()
                    .all(|frame| frame.operation != Operation::GatewayResultData as u16)
            );
        } else if indexed_join_benchmark_only {
            let mut indexed_samples = Vec::new();
            let mut hash_samples = Vec::new();
            for frame in ordered_frames.as_slice() {
                if frame.operation == Operation::GatewayResultData as u16
                    && frame.payload.len() == 16
                    && &frame.payload[..7] == b"OP10J01"
                {
                    let elapsed_ns = u64::from_be_bytes(frame.payload[8..16].try_into()?);
                    assert!(elapsed_ns > 0);
                    if frame.payload[7] == b'I' {
                        indexed_samples.push(elapsed_ns);
                    } else if frame.payload[7] == b'H' {
                        hash_samples.push(elapsed_ns);
                    } else {
                        return Err("join benchmark reported an unknown strategy".into());
                    }
                }
            }
            let expected_indexed_samples = if indexed_join_skewed_high_fanout_benchmark_only
                || indexed_join_fanout_seventeen_over_threshold_benchmark_only
                || indexed_join_fanout_eighteen_over_threshold_benchmark_only
                || (indexed_join_fanout_benchmark_only
                    && indexed_join_fanout == 5
                    && !indexed_join_fanout_nineteen_over_cutoff_benchmark_only)
            {
                0
            } else {
                2
            };
            let expected_hash_samples = if indexed_join_fanout_seventeen_large_inner_benchmark_only
                || indexed_join_fanout_seventeen_cap_inner_benchmark_only
                || indexed_join_fanout_seventeen_4097_benchmark_only
                || indexed_join_fanout_nineteen_ratio7_benchmark_only
                || indexed_join_fanout_nineteen_over_cutoff_benchmark_only
            {
                0
            } else if expected_indexed_samples == 0 {
                4
            } else {
                2
            };
            assert_eq!(indexed_samples.len(), expected_indexed_samples);
            assert_eq!(hash_samples.len(), expected_hash_samples);
            let profile = if indexed_join_skewed_high_fanout_benchmark_only {
                "high skewed fanout"
            } else if indexed_join_skewed_fanout_benchmark_only {
                "low skewed fanout"
            } else {
                "uniform fanout"
            };
            eprintln!(
                "SQL-10 mixed-load join latency (ns), same invocation snapshot and {}-row indexed input, outer_rows={}, {profile}: indexed={indexed_samples:?} hash={hash_samples:?}",
                indexed_join_fanout_inner_rows, indexed_join_fanout_outer_rows
            );
            let (load_started, load_finished) = operational_load_interval
                .ok_or("indexed/hash join benchmark did not overlap operational load")?;
            let observed_samples = join_timing_observed_at
                .lock()
                .map_err(|_| std::io::Error::other("join timing observer lock poisoned"))?;
            assert_eq!(
                observed_samples.len(),
                expected_hash_samples + expected_indexed_samples
            );
            if expected_indexed_samples > 0 {
                assert!(
                    observed_samples.iter().any(|(kind, observed_at)| {
                        *kind == b'I'
                            && load_started <= *observed_at
                            && *observed_at <= load_finished
                    }),
                    "no indexed sample completed during operational load; load_duration={:?}, samples relative_to_load_start={:?}",
                    load_finished.duration_since(load_started),
                    observed_samples
                        .iter()
                        .map(|(kind, observed_at)| (
                            *kind as char,
                            observed_at.checked_duration_since(load_started),
                            load_finished.checked_duration_since(*observed_at),
                        ))
                        .collect::<Vec<_>>()
                );
            }
            if expected_hash_samples > 0 {
                assert!(
                    observed_samples.iter().any(|(kind, observed_at)| {
                        *kind == b'H'
                            && load_started <= *observed_at
                            && *observed_at <= load_finished
                    }),
                    "no hash sample completed during operational load"
                );
            }
            assert!(ordered_frames.as_slice().iter().any(|frame| {
                frame.operation == Operation::GatewayResultData as u16
                    && frame.payload == b"JOIN_BENCHMARK_STARTED"
            }));
            assert!(ordered_frames.as_slice().iter().any(|frame| {
                frame.operation == Operation::GatewayResultData as u16
                    && frame.payload == b"JOIN_BENCHMARK_SAMPLES_FINISHED"
            }));
            while let Some(frame) = ordered_frames.as_slice().first() {
                if frame.operation == Operation::GatewayTerminal as u16 {
                    break;
                }
                assert_eq!(frame.operation, Operation::GatewayResultData as u16);
                ordered_frames.next();
            }
        } else if spill_pressure_only {
            fn expected_pressure_order_key(left_key: Option<i32>) -> Vec<u8> {
                let mut output = b"SQLR".to_vec();
                output.extend_from_slice(&1u16.to_be_bytes());
                output.extend_from_slice(&1u16.to_be_bytes());
                output.extend_from_slice(&(b"left_key".len() as u16).to_be_bytes());
                output.extend_from_slice(b"left_key");
                output.extend_from_slice(&3u32.to_be_bytes());
                if let Some(left_key) = left_key {
                    output.extend_from_slice(&[0, 0, 0]);
                    output.extend_from_slice(&4u32.to_be_bytes());
                    output.extend_from_slice(&left_key.to_be_bytes());
                } else {
                    output.extend_from_slice(&[0, 0, 1]);
                    output.extend_from_slice(&0u32.to_be_bytes());
                }
                output
            }
            let marker = ordered_frames
                .next()
                .ok_or("missing spill pressure start marker")?;
            assert_eq!(marker.operation, Operation::GatewayResultData as u16);
            assert_eq!(marker.payload, b"spill-pressure-started");
            let mut expected_keys = vec![None, None];
            expected_keys.extend(std::iter::repeat_n(Some(3), 43));
            expected_keys.extend(std::iter::repeat_n(Some(2), 46));
            expected_keys.extend(std::iter::repeat_n(Some(1), 45));
            assert_eq!(expected_keys.len(), 136);
            for left_key in expected_keys {
                let frame = ordered_frames
                    .next()
                    .ok_or("missing concurrently spilled ORDER BY row")?;
                assert_eq!(frame.operation, Operation::GatewayResultData as u16);
                assert_eq!(frame.request_id, ordered_accepted.request_id);
                assert_eq!(frame.payload, expected_pressure_order_key(left_key));
            }
        } else if spill_join_text_only {
            fn decode_text_join_sqlr_row(
                bytes: &[u8],
            ) -> Result<(i32, Option<String>), Box<dyn std::error::Error>> {
                let mut offset = 8;
                assert_eq!(&bytes[..4], b"SQLR");
                assert_eq!(u16::from_be_bytes(bytes[4..6].try_into()?), 1);
                assert_eq!(u16::from_be_bytes(bytes[6..8].try_into()?), 2);
                let name_len = u16::from_be_bytes(bytes[offset..offset + 2].try_into()?) as usize;
                offset += 2;
                assert_eq!(&bytes[offset..offset + name_len], b"left_key");
                offset += name_len;
                assert_eq!(u32::from_be_bytes(bytes[offset..offset + 4].try_into()?), 3);
                offset += 4;
                assert_eq!(&bytes[offset..offset + 3], &[0, 0, 0]);
                offset += 3;
                assert_eq!(u32::from_be_bytes(bytes[offset..offset + 4].try_into()?), 4);
                offset += 4;
                let left = i32::from_be_bytes(bytes[offset..offset + 4].try_into()?);
                offset += 4;
                let name_len = u16::from_be_bytes(bytes[offset..offset + 2].try_into()?) as usize;
                offset += 2;
                assert_eq!(&bytes[offset..offset + name_len], b"large_payload");
                offset += name_len;
                assert_eq!(u32::from_be_bytes(bytes[offset..offset + 4].try_into()?), 6);
                offset += 4;
                assert_eq!(&bytes[offset..offset + 2], &[0, 0]);
                let state = bytes[offset + 2];
                offset += 3;
                let length = u32::from_be_bytes(bytes[offset..offset + 4].try_into()?) as usize;
                offset += 4;
                let value = match (state, length) {
                    (0, length) => {
                        let value = String::from_utf8(bytes[offset..offset + length].to_vec())?;
                        offset += length;
                        Some(value)
                    }
                    (1, 0) => None,
                    other => {
                        return Err(format!("invalid TEXT SQLR state/length: {other:?}").into());
                    }
                };
                assert_eq!(offset, bytes.len());
                Ok((left, value))
            }
            let mut values = Vec::new();
            for _ in 0..6 {
                let frame = ordered_frames
                    .next()
                    .ok_or("missing ordered TEXT JOIN SQLR row")?;
                assert_eq!(frame.operation, Operation::GatewayResultData as u16);
                assert_eq!(frame.request_id, ordered_accepted.request_id);
                values.push(decode_text_join_sqlr_row(&frame.payload)?);
            }
            assert_eq!(
                values,
                [
                    (3, None),
                    (1, None),
                    (1, None),
                    (2, Some("echo".to_owned())),
                    (2, Some("echo".to_owned())),
                    (2, Some("echo".to_owned())),
                ]
            );
        } else if spill_join_bigint_only {
            fn decode_bigint_join_sqlr_row(
                bytes: &[u8],
            ) -> Result<(i32, Option<i64>), Box<dyn std::error::Error>> {
                let mut offset = 8;
                assert_eq!(&bytes[..4], b"SQLR");
                assert_eq!(u16::from_be_bytes(bytes[4..6].try_into()?), 1);
                assert_eq!(u16::from_be_bytes(bytes[6..8].try_into()?), 2);
                let name_len = u16::from_be_bytes(bytes[offset..offset + 2].try_into()?) as usize;
                offset += 2;
                assert_eq!(&bytes[offset..offset + name_len], b"left_key");
                offset += name_len;
                assert_eq!(u32::from_be_bytes(bytes[offset..offset + 4].try_into()?), 3);
                offset += 4;
                assert_eq!(&bytes[offset..offset + 3], &[0, 0, 0]);
                offset += 3;
                assert_eq!(u32::from_be_bytes(bytes[offset..offset + 4].try_into()?), 4);
                offset += 4;
                let left = i32::from_be_bytes(bytes[offset..offset + 4].try_into()?);
                offset += 4;
                let name_len = u16::from_be_bytes(bytes[offset..offset + 2].try_into()?) as usize;
                offset += 2;
                assert_eq!(&bytes[offset..offset + name_len], b"bigint_value");
                offset += name_len;
                assert_eq!(u32::from_be_bytes(bytes[offset..offset + 4].try_into()?), 4);
                offset += 4;
                assert_eq!(&bytes[offset..offset + 2], &[0, 0]);
                let state = bytes[offset + 2];
                offset += 3;
                let length = u32::from_be_bytes(bytes[offset..offset + 4].try_into()?) as usize;
                offset += 4;
                let value = match (state, length) {
                    (0, 8) => {
                        let value = i64::from_be_bytes(bytes[offset..offset + 8].try_into()?);
                        offset += 8;
                        Some(value)
                    }
                    (1, 0) => None,
                    other => {
                        return Err(format!("invalid BIGINT SQLR state/length: {other:?}").into());
                    }
                };
                assert_eq!(offset, bytes.len());
                Ok((left, value))
            }
            let mut values = Vec::new();
            for _ in 0..5 {
                let frame = ordered_frames
                    .next()
                    .ok_or("missing ordered BIGINT JOIN SQLR row")?;
                assert_eq!(frame.operation, Operation::GatewayResultData as u16);
                assert_eq!(frame.request_id, ordered_accepted.request_id);
                values.push(decode_bigint_join_sqlr_row(&frame.payload)?);
            }
            assert_eq!(
                values.iter().map(|(_, value)| *value).collect::<Vec<_>>(),
                [None, None, None, Some(12), Some(12)]
            );
        } else if spill_join_only || spill_join_filtered_only || spill_join_parameter_only {
            fn decode_two_integer_sqlr_fields(
                bytes: &[u8],
            ) -> Result<(i32, i32), Box<dyn std::error::Error>> {
                fn read_integer(
                    bytes: &[u8],
                    offset: &mut usize,
                ) -> Result<i32, Box<dyn std::error::Error>> {
                    let name_len = u16::from_be_bytes(bytes[*offset..*offset + 2].try_into()?);
                    *offset += 2 + usize::from(name_len);
                    assert_eq!(
                        u32::from_be_bytes(bytes[*offset..*offset + 4].try_into()?),
                        3
                    );
                    *offset += 4 + 3;
                    let value_len = u32::from_be_bytes(bytes[*offset..*offset + 4].try_into()?);
                    *offset += 4;
                    assert_eq!(value_len, 4);
                    let value = i32::from_be_bytes(bytes[*offset..*offset + 4].try_into()?);
                    *offset += 4;
                    Ok(value)
                }
                assert_eq!(&bytes[..4], b"SQLR");
                assert_eq!(u16::from_be_bytes(bytes[4..6].try_into()?), 1);
                assert_eq!(u16::from_be_bytes(bytes[6..8].try_into()?), 2);
                let mut offset = 8;
                let left = read_integer(bytes, &mut offset)?;
                let right = read_integer(bytes, &mut offset)?;
                assert_eq!(offset, bytes.len());
                Ok((left, right))
            }
            let mut observed = std::collections::BTreeMap::new();
            let row_count = ordered_frames
                .as_slice()
                .iter()
                .take_while(|frame| frame.operation == Operation::GatewayResultData as u16)
                .count();
            let mut ordered = Vec::new();
            for frame in ordered_frames.as_slice().iter().take(row_count) {
                assert_eq!(frame.request_id, ordered_accepted.request_id);
                let (left, right) = decode_two_integer_sqlr_fields(&frame.payload)?;
                *observed.entry((left, right)).or_insert(0usize) += 1;
                ordered.push((left, right));
            }
            for _ in 0..row_count {
                ordered_frames.next();
            }
            if spill_join_filtered_only {
                assert_eq!(ordered, [(3, 11), (1, 7)]);
                assert_eq!(
                    observed.into_iter().collect::<Vec<_>>(),
                    [((1, 7), 1), ((3, 11), 1)]
                );
            } else if spill_join_parameter_only {
                assert_eq!(ordered, [(3, 11), (2, 9)]);
                assert_eq!(
                    observed.into_iter().collect::<Vec<_>>(),
                    [((2, 9), 1), ((3, 11), 1)]
                );
            } else {
                assert_eq!(ordered, [(3, 11), (2, 9), (2, 9), (2, 9), (2, 9)]);
                assert_eq!(
                    observed.into_iter().collect::<Vec<_>>(),
                    [((2, 9), 4), ((3, 11), 1)]
                );
            }
        } else if spill_join_decimal_only {
            fn decode_decimal_join_sqlr_row(
                bytes: &[u8],
            ) -> Result<(i32, Option<i128>), Box<dyn std::error::Error>> {
                fn read_name(
                    bytes: &[u8],
                    offset: &mut usize,
                ) -> Result<String, Box<dyn std::error::Error>> {
                    let length =
                        u16::from_be_bytes(bytes[*offset..*offset + 2].try_into()?) as usize;
                    *offset += 2;
                    let name = std::str::from_utf8(&bytes[*offset..*offset + length])?.to_owned();
                    *offset += length;
                    Ok(name)
                }
                assert_eq!(&bytes[..4], b"SQLR");
                assert_eq!(u16::from_be_bytes(bytes[4..6].try_into()?), 1);
                assert_eq!(u16::from_be_bytes(bytes[6..8].try_into()?), 2);
                let mut offset = 8;
                assert_eq!(read_name(bytes, &mut offset)?, "left_key");
                assert_eq!(u32::from_be_bytes(bytes[offset..offset + 4].try_into()?), 3);
                offset += 4;
                assert_eq!(&bytes[offset..offset + 3], &[0, 0, 0]);
                offset += 3;
                assert_eq!(u32::from_be_bytes(bytes[offset..offset + 4].try_into()?), 4);
                offset += 4;
                let left = i32::from_be_bytes(bytes[offset..offset + 4].try_into()?);
                offset += 4;
                assert_eq!(read_name(bytes, &mut offset)?, "decimal_value");
                assert_eq!(u32::from_be_bytes(bytes[offset..offset + 4].try_into()?), 5);
                offset += 4;
                assert_eq!(&bytes[offset..offset + 2], &[4, 2]);
                let state = bytes[offset + 2];
                offset += 2;
                offset += 1;
                let length = u32::from_be_bytes(bytes[offset..offset + 4].try_into()?) as usize;
                offset += 4;
                let value = match (state, length) {
                    (0, 16) => {
                        let value = i128::from_be_bytes(bytes[offset..offset + 16].try_into()?);
                        offset += 16;
                        Some(value)
                    }
                    (1, 0) => None,
                    other => {
                        return Err(format!("invalid Decimal SQLR state/length: {other:?}").into());
                    }
                };
                assert_eq!(offset, bytes.len());
                Ok((left, value))
            }
            let mut observed = Vec::new();
            for _ in 0..6 {
                let frame = ordered_frames
                    .next()
                    .ok_or("missing ordered Decimal JOIN SQLR row")?;
                assert_eq!(frame.operation, Operation::GatewayResultData as u16);
                assert_eq!(frame.request_id, ordered_accepted.request_id);
                observed.push(decode_decimal_join_sqlr_row(&frame.payload)?);
            }
            assert_eq!(
                observed.iter().map(|(_, value)| *value).collect::<Vec<_>>(),
                [None, None, None, Some(225), Some(225), Some(225)]
            );
        } else if sql_in_cancel_only || sql_in_denied_key_only {
            assert!(
                ordered_frames
                    .as_slice()
                    .iter()
                    .all(|frame| frame.operation != Operation::GatewayResultData as u16),
                "cancelled or denied-key SQL IN must not expose partial rows"
            );
        } else if sql_in_only {
            fn decode_pair(bytes: &[u8]) -> Result<(i32, i32), Box<dyn std::error::Error>> {
                assert_eq!(&bytes[..4], b"SQLR");
                assert_eq!(u16::from_be_bytes(bytes[4..6].try_into()?), 1);
                assert_eq!(u16::from_be_bytes(bytes[6..8].try_into()?), 2);
                let mut offset = 8;
                let mut values = [0i32; 2];
                for (index, expected_name) in
                    [b"left_key".as_slice(), b"right_key"].iter().enumerate()
                {
                    let name_len =
                        u16::from_be_bytes(bytes[offset..offset + 2].try_into()?) as usize;
                    offset += 2;
                    assert_eq!(&bytes[offset..offset + name_len], *expected_name);
                    offset += name_len;
                    assert_eq!(u32::from_be_bytes(bytes[offset..offset + 4].try_into()?), 3);
                    offset += 4;
                    assert_eq!(&bytes[offset..offset + 3], &[0, 0, 0]);
                    offset += 3;
                    assert_eq!(u32::from_be_bytes(bytes[offset..offset + 4].try_into()?), 4);
                    offset += 4;
                    values[index] = i32::from_be_bytes(bytes[offset..offset + 4].try_into()?);
                    offset += 4;
                }
                assert_eq!(offset, bytes.len());
                Ok((values[0], values[1]))
            }
            let mut observed = std::collections::BTreeMap::new();
            let expected_rows = if sql_in_elevated_budget_only {
                1_028
            } else if sql_in_over_budget_only {
                129
            } else {
                5
            };
            for _ in 0..expected_rows {
                let frame = ordered_frames.next().ok_or("missing batched IN SQLR row")?;
                assert_eq!(frame.operation, Operation::GatewayResultData as u16);
                assert_eq!(frame.request_id, ordered_accepted.request_id);
                *observed
                    .entry(decode_pair(&frame.payload)?)
                    .or_insert(0usize) += 1;
            }
            assert_eq!(
                observed.into_iter().collect::<Vec<_>>(),
                if sql_in_elevated_budget_only {
                    vec![((1, 7), 1_025), ((2, 9), 3)]
                } else if sql_in_over_budget_only {
                    vec![((1, 7), 126), ((2, 9), 3)]
                } else {
                    vec![((1, 7), 2), ((2, 9), 3)]
                },
                "IN must return the authorized reference rows with duplicate list values and a NULL member"
            );
        } else if !spill_quota_only
            && !spill_crash_only
            && !spill_redacted_only
            && !spill_join_oversize_only
            && (count_column_only || count_bigint_only)
        {
            fn expected_count_column_row(left_key: i32, right_key: i32, count: i64) -> Vec<u8> {
                fn append_field(output: &mut Vec<u8>, name: &[u8], type_code: u32, value: &[u8]) {
                    output.extend_from_slice(&(name.len() as u16).to_be_bytes());
                    output.extend_from_slice(name);
                    output.extend_from_slice(&type_code.to_be_bytes());
                    output.extend_from_slice(&[0, 0, 0]);
                    output.extend_from_slice(&(value.len() as u32).to_be_bytes());
                    output.extend_from_slice(value);
                }
                let mut output = b"SQLR".to_vec();
                output.extend_from_slice(&1u16.to_be_bytes());
                output.extend_from_slice(&3u16.to_be_bytes());
                append_field(&mut output, b"left_key", 3, &left_key.to_be_bytes());
                append_field(&mut output, b"right_key", 3, &right_key.to_be_bytes());
                append_field(&mut output, b"count", 4, &count.to_be_bytes());
                output
            }
            for (left_key, right_key, count) in [(2, 9, 3), (1, 7, 1), (3, 11, 0)] {
                let frame = ordered_frames
                    .next()
                    .ok_or("missing confined COUNT(column) SQLR row")?;
                assert_eq!(frame.operation, Operation::GatewayResultData as u16);
                assert_eq!(frame.request_id, ordered_accepted.request_id);
                assert_eq!(
                    frame.payload,
                    expected_count_column_row(left_key, right_key, count)
                );
            }
        } else if spill_quota_only
            || spill_redacted_only
            || spill_join_oversize_only
            || spill_disk_full_only
            || spill_pressure_cancel_only
        {
            assert!(
                ordered_frames
                    .as_slice()
                    .iter()
                    .all(|frame| frame.operation != Operation::GatewayResultData as u16)
            );
        } else if spill_crash_only {
            let provisional = ordered_frames
                .next()
                .ok_or("missing result emitted after spill write and before worker crash")?;
            assert_eq!(provisional.operation, Operation::GatewayResultData as u16);
            assert_eq!(provisional.payload, b"spill-crash-provisional");
            assert!(
                ordered_frames
                    .as_slice()
                    .iter()
                    .all(|frame| frame.operation != Operation::GatewayResultData as u16)
            );
        } else if filtered_composite_only {
            fn expected_filtered_sum_row(
                left_key: i32,
                right_key: i32,
                coefficient: u128,
            ) -> Vec<u8> {
                fn append_field(
                    output: &mut Vec<u8>,
                    name: &[u8],
                    type_code: u32,
                    precision: u8,
                    scale: u8,
                    value: &[u8],
                ) {
                    output.extend_from_slice(&(name.len() as u16).to_be_bytes());
                    output.extend_from_slice(name);
                    output.extend_from_slice(&type_code.to_be_bytes());
                    output.extend_from_slice(&[precision, scale, 0]);
                    output.extend_from_slice(&(value.len() as u32).to_be_bytes());
                    output.extend_from_slice(value);
                }
                let mut output = b"SQLR".to_vec();
                output.extend_from_slice(&1u16.to_be_bytes());
                output.extend_from_slice(&3u16.to_be_bytes());
                append_field(&mut output, b"left_key", 3, 0, 0, &left_key.to_be_bytes());
                append_field(&mut output, b"right_key", 3, 0, 0, &right_key.to_be_bytes());
                append_field(&mut output, b"sum", 5, 38, 2, &coefficient.to_be_bytes());
                output
            }
            {
                let (left_key, right_key, coefficient) = (1i32, 7i32, 100u128);
                let frame = ordered_frames
                    .next()
                    .ok_or("missing filtered composite Decimal SUM SQLR row")?;
                assert_eq!(frame.operation, Operation::GatewayResultData as u16);
                assert_eq!(frame.request_id, ordered_accepted.request_id);
                assert_eq!(
                    frame.payload,
                    expected_filtered_sum_row(left_key, right_key, coefficient)
                );
            }
        } else {
            for group in (0i32..64).rev() {
                let mut group_key = format!("{group:04}").into_bytes();
                group_key.extend(std::iter::repeat_n(b'a', 8_168));
                let mut expected = b"SQLR".to_vec();
                expected.extend_from_slice(&1u16.to_be_bytes());
                expected.extend_from_slice(&2u16.to_be_bytes());
                expected.extend_from_slice(&(b"group_key".len() as u16).to_be_bytes());
                expected.extend_from_slice(b"group_key");
                expected.extend_from_slice(&6u32.to_be_bytes());
                expected.extend_from_slice(&[0, 0, 0]);
                expected.extend_from_slice(&(group_key.len() as u32).to_be_bytes());
                expected.extend_from_slice(&group_key);
                expected.extend_from_slice(&(b"sum".len() as u16).to_be_bytes());
                expected.extend_from_slice(b"sum");
                expected.extend_from_slice(&4u32.to_be_bytes());
                expected.extend_from_slice(&[0, 0, 0]);
                expected.extend_from_slice(&8u32.to_be_bytes());
                expected.extend_from_slice(&(300i64 + 2 * i64::from(group)).to_be_bytes());
                let frame = ordered_frames
                    .next()
                    .ok_or_else(|| format!("missing ordered analytical SUM group {group}"))?;
                assert_eq!(frame.operation, Operation::GatewayResultData as u16);
                assert_eq!(frame.request_id, ordered_accepted.request_id);
                assert_eq!(
                    frame.payload, expected,
                    "ordered analytical SUM group {group}"
                );
            }
        }
        let terminal = ordered_frames
            .next()
            .ok_or("missing ordered SUM terminal")?;
        assert_eq!(terminal.operation, Operation::GatewayTerminal as u16);
        assert_eq!(terminal.request_id, ordered_accepted.request_id);
        assert!(String::from_utf8(terminal.payload)?.contains(
            if spill_quota_only
                || spill_crash_only
                || spill_redacted_only
                || spill_join_oversize_only
                || spill_disk_full_only
                || spill_pressure_cancel_only
                || sql_in_cancel_only
                || sql_in_denied_key_only
            {
                "\"outcome\":\"aborted\""
            } else {
                "\"outcome\":\"committed\""
            }
        ));
        assert_eq!(
            storage
                .lookup_invocation_outcome(ordered_reservation.invocation_id, user_id, now + 1)?
                .ok_or("missing ordered SUM outcome")?
                .status,
            if spill_quota_only
                || spill_crash_only
                || spill_redacted_only
                || spill_join_oversize_only
                || spill_disk_full_only
                || spill_pressure_cancel_only
                || sql_in_cancel_only
                || sql_in_denied_key_only
            {
                OutcomeStatus::Aborted
            } else {
                OutcomeStatus::Committed
            }
        );
        if let Some(parent) = spill_parent.as_ref() {
            assert!(
                std::fs::read_dir(parent)?.all(|entry| {
                    entry
                        .and_then(|entry| entry.file_type())
                        .is_ok_and(|kind| !kind.is_dir())
                }),
                "spill invocation directory must be removed when the package ends"
            );
        }
        if spill_disk_full_only {
            let parent = spill_parent
                .as_ref()
                .ok_or("disk-full acceptance must have a spill parent")?
                .clone();
            drop(runtime);
            drop(spill_disk_full);
            assert!(!parent.exists(), "disk-full test scratch should be removed");
        }
        return Ok(());
    }
    let (accepted, mut invocation, mut executor) = client.handle_start_request_with_executor(
        &storage,
        &staging,
        ClientFrame {
            kind: 7,
            request_id: 1,
            response_sequence: 0,
            payload: format!("{{\"invocation_id\":\"{invocation_hex}\"}}").into_bytes(),
        },
        GatewayAdmissionConfig {
            retention_period_us: 3_600_000_000,
            max_rows: 8,
            max_bytes: 4096,
        },
        &runtime,
    )?;
    assert_eq!(accepted.payload, b"{\"state\":\"accepted\"}");

    let mut cancellation_client = ClientConnectionSession::default();
    cancellation_client.accept_hello(ClientMode::Query, "sandbox-sql-cancellation")?;
    cancellation_client.authenticate(&storage, token_id, &secret)?;
    let cancellation_owner = cancellation_client.staging_owner()?;
    let cancellation_parameter = [b'C'];
    let cancellation_reservation = staging.prepare(
        cancellation_owner,
        PrepareSubmission {
            manifest_version: 1,
            manifest: manifest.clone(),
            artifact_bytes: u64::try_from(artifact.len())?,
            artifact_sha256: Sha256::digest(&artifact).into(),
            scope_version: 1,
            requested_scope: requested_scope.clone(),
            parameter_bytes: 1,
            parameter_sha256: Sha256::digest(cancellation_parameter).into(),
            deadline_ms: 120_000,
            workload_class: WorkloadClass::Analytical,
        },
    )?;
    let mut cancellation_artifact_offset = 0;
    for chunk in artifact.chunks(prdb_engine::supervisor::MAX_CHUNK_BYTES) {
        cancellation_artifact_offset = staging
            .upload_chunk(
                cancellation_owner,
                cancellation_reservation.invocation_id,
                ChunkKind::Artifact,
                cancellation_artifact_offset,
                chunk,
            )
            .map_err(|error| format!("cancellation SQL artifact chunk upload: {error:?}"))?;
    }
    staging
        .upload_chunk(
            cancellation_owner,
            cancellation_reservation.invocation_id,
            ChunkKind::Parameters,
            0,
            &cancellation_parameter,
        )
        .map_err(|error| format!("cancellation SQL parameter upload: {error:?}"))?;
    staging.seal(cancellation_owner, cancellation_reservation.invocation_id)?;
    let cancellation_invocation_hex = cancellation_reservation
        .invocation_id
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect::<String>();
    let (cancellation_accepted, cancellation_invocation, mut cancellation_executor) =
        cancellation_client.handle_start_request_with_executor(
            &storage,
            &staging,
            ClientFrame {
                kind: 7,
                request_id: 2,
                response_sequence: 0,
                payload: format!("{{\"invocation_id\":\"{cancellation_invocation_hex}\"}}")
                    .into_bytes(),
            },
            GatewayAdmissionConfig {
                retention_period_us: 3_600_000_000,
                max_rows: 8,
                max_bytes: 4096,
            },
            &runtime,
        )?;
    assert_eq!(cancellation_accepted.payload, b"{\"state\":\"accepted\"}");
    let mut additional_cancellation_workers = Vec::new();
    for worker_index in 0..6u8 {
        let mut worker_client = ClientConnectionSession::default();
        worker_client.accept_hello(
            ClientMode::Query,
            &format!("sandbox-sql-cancellation-{worker_index}"),
        )?;
        worker_client.authenticate(&storage, token_id, &secret)?;
        let worker_owner = worker_client.staging_owner()?;
        let worker_reservation = staging.prepare(
            worker_owner,
            PrepareSubmission {
                manifest_version: 1,
                manifest: manifest.clone(),
                artifact_bytes: u64::try_from(artifact.len())?,
                artifact_sha256: Sha256::digest(&artifact).into(),
                scope_version: 1,
                requested_scope: requested_scope.clone(),
                parameter_bytes: 1,
                parameter_sha256: Sha256::digest(cancellation_parameter).into(),
                deadline_ms: 120_000,
                workload_class: WorkloadClass::Analytical,
            },
        )?;
        let mut worker_artifact_offset = 0;
        for chunk in artifact.chunks(prdb_engine::supervisor::MAX_CHUNK_BYTES) {
            worker_artifact_offset = staging
                .upload_chunk(
                    worker_owner,
                    worker_reservation.invocation_id,
                    ChunkKind::Artifact,
                    worker_artifact_offset,
                    chunk,
                )
                .map_err(|error| format!("additional cancellation artifact chunk: {error:?}"))?;
        }
        staging
            .upload_chunk(
                worker_owner,
                worker_reservation.invocation_id,
                ChunkKind::Parameters,
                0,
                &cancellation_parameter,
            )
            .map_err(|error| format!("additional cancellation parameter: {error:?}"))?;
        staging.seal(worker_owner, worker_reservation.invocation_id)?;
        let worker_invocation_hex = worker_reservation
            .invocation_id
            .iter()
            .map(|byte| format!("{byte:02x}"))
            .collect::<String>();
        let (worker_accepted, worker_invocation, worker_executor) = worker_client
            .handle_start_request_with_executor(
                &storage,
                &staging,
                ClientFrame {
                    kind: 7,
                    request_id: 20 + u64::from(worker_index),
                    response_sequence: 0,
                    payload: format!("{{\"invocation_id\":\"{worker_invocation_hex}\"}}")
                        .into_bytes(),
                },
                GatewayAdmissionConfig {
                    retention_period_us: 3_600_000_000,
                    max_rows: 8,
                    max_bytes: 4096,
                },
                &runtime,
            )?;
        assert_eq!(worker_accepted.payload, b"{\"state\":\"accepted\"}");
        let (worker_service, worker_gateway) = UnixStream::pair()?;
        worker_gateway.set_read_timeout(Some(Duration::from_secs(10)))?;
        additional_cancellation_workers.push((
            worker_reservation,
            worker_accepted,
            worker_invocation,
            worker_executor,
            worker_service,
            worker_gateway,
        ));
    }
    let mut saturated_client = ClientConnectionSession::default();
    saturated_client.accept_hello(ClientMode::Query, "sandbox-sql-saturation")?;
    saturated_client.authenticate(&storage, restricted_token_id, &restricted_secret)?;
    let saturated_owner = saturated_client.staging_owner()?;
    let saturated_reservation = staging.prepare(
        saturated_owner,
        PrepareSubmission {
            manifest_version: 1,
            manifest: manifest.clone(),
            artifact_bytes: u64::try_from(artifact.len())?,
            artifact_sha256: Sha256::digest(&artifact).into(),
            scope_version: 1,
            requested_scope: requested_scope.clone(),
            parameter_bytes: 0,
            parameter_sha256: Sha256::digest([]).into(),
            deadline_ms: 30_000,
            workload_class: WorkloadClass::Analytical,
        },
    )?;
    let mut saturated_artifact_offset = 0;
    for chunk in artifact.chunks(prdb_engine::supervisor::MAX_CHUNK_BYTES) {
        saturated_artifact_offset = staging
            .upload_chunk(
                saturated_owner,
                saturated_reservation.invocation_id,
                ChunkKind::Artifact,
                saturated_artifact_offset,
                chunk,
            )
            .map_err(|error| format!("saturated SQL artifact chunk upload: {error:?}"))?;
    }
    staging.seal(saturated_owner, saturated_reservation.invocation_id)?;
    let saturated_invocation_hex = saturated_reservation
        .invocation_id
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect::<String>();
    let (mut cancellation_service, mut cancellation_gateway) = UnixStream::pair()?;
    cancellation_gateway.set_read_timeout(Some(Duration::from_secs(10)))?;

    let schema_rows = invocation.scan_information_schema_schemata(128)?.rows;
    let mut table_rows = Vec::new();
    let mut table_cursor = None;
    loop {
        let page = invocation.scan_information_schema_tables(128, table_cursor)?;
        table_rows.extend(page.rows);
        table_cursor = page.continuation;
        if table_cursor.is_none() {
            break;
        }
    }
    let mut column_rows = Vec::new();
    let mut column_cursor = None;
    loop {
        let page = invocation.scan_information_schema_columns(128, column_cursor)?;
        column_rows.extend(page.rows);
        column_cursor = page.continuation;
        if column_cursor.is_none() {
            break;
        }
    }
    assert!(
        !table_rows
            .iter()
            .any(|row| row.table_name == "private_records")
    );
    assert!(
        !column_rows
            .iter()
            .any(|row| row.table_name == "private_records")
    );
    assert!(
        !schema_rows
            .iter()
            .any(|row| row.schema_name == "hidden_roundtrip")
    );
    assert!(
        !schema_rows
            .iter()
            .any(|row| row.schema_name == "missing_roundtrip")
    );
    assert!(
        !table_rows
            .iter()
            .any(|row| row.table_schema == "hidden_roundtrip")
    );
    assert!(
        !table_rows
            .iter()
            .any(|row| row.table_schema == "missing_roundtrip")
    );
    assert!(
        !column_rows
            .iter()
            .any(|row| row.table_schema == "hidden_roundtrip")
    );
    assert!(
        !column_rows
            .iter()
            .any(|row| row.table_schema == "missing_roundtrip")
    );
    assert!(
        !table_rows
            .iter()
            .any(|row| row.table_schema == "empty_roundtrip")
    );
    assert!(
        !column_rows
            .iter()
            .any(|row| row.table_schema == "empty_roundtrip")
    );
    let expected_schema = schema_rows
        .iter()
        .find(|row| row.schema_name == "information_schema")
        .ok_or("metadata API omitted the public information_schema schema")?
        .schema_name
        .as_bytes()
        .to_vec();
    let expected_table = table_rows
        .iter()
        .find(|row| row.table_schema == "information_schema")
        .ok_or("metadata API omitted the public information_schema tables")?
        .table_name
        .as_bytes()
        .to_vec();
    let expected_column = column_rows
        .iter()
        .find(|row| row.table_schema == "information_schema" && row.table_name == "columns")
        .ok_or("metadata API omitted information_schema.columns metadata")?
        .column_name
        .as_bytes()
        .to_vec();
    let expected_user_schema = schema_rows
        .iter()
        .find(|row| row.schema_name == "sql_roundtrip")
        .ok_or("metadata API omitted the admitted user schema")?
        .schema_name
        .as_bytes()
        .to_vec();
    let expected_empty_schema = schema_rows
        .iter()
        .find(|row| row.schema_name == "empty_roundtrip")
        .ok_or("metadata API omitted the authorized empty schema")?
        .schema_name
        .as_bytes()
        .to_vec();
    let expected_user_column = column_rows
        .iter()
        .find(|row| row.table_schema == "sql_roundtrip" && row.table_name == "records")
        .ok_or("metadata API omitted the admitted user column")?
        .column_name
        .as_bytes()
        .to_vec();

    let (mut service, mut gateway) = UnixStream::pair()?;
    gateway.set_read_timeout(Some(Duration::from_secs(10)))?;
    let worker_pid = executor.id();
    let operational_storage = &storage;
    let cancellation_storage = &storage;
    let saturated_storage = &storage;
    let saturated_status_storage = &storage;
    let saturated_staging = &staging;
    let saturated_runtime = &runtime;
    let operational_table_id = composite_table.table_id;
    let operational_pressure_relation_name = "operational_pressure_records";
    let index_build_invocation = id(0xe6);
    let mut index_build_audit = identity_audit_event(0xe7, user_id, index_pressure_table.table_id);
    index_build_audit.correlation_id = index_build_invocation;
    let additional_cancellation_ids = additional_cancellation_workers
        .iter()
        .map(|(reservation, _, _, _, _, _)| reservation.invocation_id)
        .collect::<Vec<_>>();
    let (
        worker_status_result,
        concurrent_operational_invocations,
        active_worker_point_read_latencies,
        active_worker_commit_latencies,
        cancellation_request_latency,
        cancellation_observed_latency,
        analytics_memory_current_bytes,
        analytics_memory_peak_bytes,
        analytical_cap_rejected,
        optimizer_range_latency_samples,
        optimizer_planner_dispatch_samples,
    ) = std::thread::scope(|scope| {
        let observer_done = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let analytics_result_relayed = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let active_operational_transactions = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let completed_operational_transactions = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let peak_active_operational_transactions = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let active_cancellation_workers = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let additional_worker_memory_samples =
            Arc::new(std::sync::Mutex::new(Vec::<(u64, u64)>::with_capacity(7)));
        let optimizer_planner_dispatch_samples = Arc::new(std::sync::Mutex::new(Vec::new()));
        let start = Arc::new(std::sync::Barrier::new(72));
        let first_operational_batch = Arc::new(std::sync::Barrier::new(64));
        let (saturation_started_tx, saturation_started_rx) = std::sync::mpsc::channel();
        let (index_build_start_tx, index_build_start_rx) = std::sync::mpsc::channel();
        let (index_build_started_tx, index_build_started_rx) = std::sync::mpsc::channel();
        let index_build_storage = &storage;
        let index_build_worker = scope.spawn(move || -> Result<_, String> {
            index_build_start_rx
                .recv_timeout(Duration::from_secs(10))
                .map_err(|error| {
                    format!("analytical pressure index build was not started: {error}")
                })?;
            let started_at = std::time::Instant::now();
            index_build_started_tx
                .send(started_at)
                .map_err(|error| format!("index build start notification failed: {error}"))?;
            let created = index_build_storage
                .create_index(prdb_engine::storage_manager::CreateIndexRequest {
                    invocation_id: index_build_invocation,
                    authenticated_actor_id: user_id,
                    table_id: index_pressure_table.table_id,
                    name: "analytics_pressure_group_key".to_owned(),
                    column_ids: vec![index_pressure_table.column_ids[0]],
                    unique: false,
                    retention_deadline_us: now + 3_600_000_000,
                    audit_event: index_build_audit,
                })
                .map_err(|error| format!("analytical pressure index build failed: {error:?}"))?;
            Ok((started_at, std::time::Instant::now(), created.index_id))
        });
        let mut operational_workers = Vec::with_capacity(64);
        for worker_index in 0..64u8 {
            let task_done = Arc::clone(&observer_done);
            let task_active_transactions = Arc::clone(&active_operational_transactions);
            let task_completed_transactions = Arc::clone(&completed_operational_transactions);
            let task_peak_transactions = Arc::clone(&peak_active_operational_transactions);
            let task_first_batch = Arc::clone(&first_operational_batch);
            let task_start = Arc::clone(&start);
            operational_workers.push(scope.spawn(move || -> Result<_, String> {
                task_start.wait();
                let mut active_worker_point_read_latencies = Vec::new();
                let mut active_worker_commit_latencies = Vec::new();
                let mut completed = 0;
                // The four long-running writers continue until the observers
                // finish, so saturation and cancellation checks see live
                // operational transactions even when the measured window is
                // extended.
                let max_operations = if worker_index < 60 { 1 } else { 60_000 };
                let min_operations_before_exit = if worker_index < 60 { 1 } else { 16 };
                for index in 0..max_operations {
                    let mut invocation_id = [0xe0; 16];
                    invocation_id[13] = worker_index;
                    invocation_id[14..].copy_from_slice(&(index as u16).to_be_bytes());
                    let transaction_result = operational_storage
                        .begin_token_authenticated_invocation(
                            invocation_id,
                            token_id,
                            &secret,
                            now + 3_600_000_000,
                            16,
                            4096,
                        );
                    if transaction_result.is_ok() {
                        let active = task_active_transactions
                            .fetch_add(1, std::sync::atomic::Ordering::AcqRel)
                            + 1;
                        task_peak_transactions
                            .fetch_max(active, std::sync::atomic::Ordering::AcqRel);
                    }
                    if index == 0 {
                        // Keep the first 64 admitted invocation contexts live together before
                        // any of them reaches its first read or durable commit.
                        task_first_batch.wait();
                    }
                    let (mut transaction, _) = transaction_result
                        .map_err(|error| format!("operational admission: {error:?}"))?;
                    let operation_result = (|| {
                        let point_read_started = std::time::Instant::now();
                        let point_read = transaction
                            .read_catalog_authorized_row(operational_table_id, id(0xd3))
                            .map_err(|error| {
                                format!("operational authorized point read: {error:?}")
                            })?;
                        if point_read.as_ref().is_none_or(|cells| {
                            cells.len() < 2 || cells.len() > 2 + usize::from(sql_in_elevated_budget_only)
                        }) {
                            return Err(format!(
                                "operational point read returned an unexpected row shape: {:?}",
                                point_read.as_ref().map(Vec::len)
                            ));
                        }
                        let point_read_latency = point_read_started.elapsed();
                        let expected_scan_rows = if sql_in_elevated_budget_only {
                            1_028
                        } else if sql_in_over_budget_only {
                            129
                        } else if sql_in_null_left_only {
                            6
                        } else {
                            5
                        };
                        let mut continuation = None;
                        let mut scanned_rows = 0usize;
                        loop {
                            let page = transaction
                                .scan_catalog_authorized_rows(
                                    operational_table_id,
                                    continuation.as_ref(),
                                    128,
                                )
                                .map_err(|error| {
                                    format!("operational authorized scan: {error:?}")
                                })?;
                            scanned_rows += page.rows.len();
                            continuation = page.continuation;
                            if continuation.is_none() {
                                break;
                            }
                        }
                        if scanned_rows != expected_scan_rows {
                            return Err(format!(
                                "operational authorized scan returned {scanned_rows} rows, expected {expected_scan_rows}"
                            ));
                        }
                        if worker_index >= 60 {
                            let worker_key = i32::from(worker_index - 60);
                            let mut executor = transaction.open_executor_metadata_connection();
                            let (relation, description) = executor
                                .resolve_relation(
                                    "sql_roundtrip",
                                    operational_pressure_relation_name,
                                )
                                .map_err(|error| {
                                    format!("operational pressure relation resolve: {error:?}")
                                })?
                                .ok_or_else(|| {
                                    "operational pressure relation was not visible".to_owned()
                                })?;
                            if description.columns.len() != 2 {
                                return Err("operational pressure table schema changed".into());
                            }
                            let cursor = executor
                                .open_relation_scan(
                                    &relation,
                                    &description
                                        .columns
                                        .iter()
                                        .map(|column| column.column_id)
                                        .collect::<Vec<_>>(),
                                )
                                .map_err(|error| {
                                    format!("operational pressure scan open: {error:?}")
                                })?;
                            let page =
                                executor
                                    .next_relation_scan_page(&cursor, 8)
                                    .map_err(|error| {
                                        format!("operational pressure scan page: {error:?}")
                                    })?;
                            if page.continuation.is_some() {
                                return Err(
                                    "operational pressure scan unexpectedly continued".into()
                                );
                            }
                            let (row_handle, _) = page
                                .rows
                                .into_iter()
                                .find(|(_, cells)| {
                                    matches!(
                                        cells.first(),
                                        Some(prdb_engine::authorization::SanitizedCell::Value(
                                            ScalarValue::Integer(value)
                                        )) if *value == worker_key
                                    )
                                })
                                .ok_or_else(|| {
                                    format!("operational pressure row {worker_key} was not visible")
                                })?;
                            executor
                                .stage_update(
                                    &row_handle,
                                    &[FieldPatch {
                                        ordinal: 1,
                                        value: Some(ScalarValue::Integer(index + 1)),
                                    }],
                                )
                                .map_err(|error| {
                                    format!("operational pressure update: {error:?}")
                                })?;
                            drop(executor);
                        }
                        let commit_started = std::time::Instant::now();
                        transaction
                            .commit()
                            .map_err(|error| format!("operational durable commit: {error:?}"))?;
                        Ok::<_, String>((point_read_latency, commit_started.elapsed()))
                    })();
                    task_active_transactions.fetch_sub(1, std::sync::atomic::Ordering::AcqRel);
                    let (point_read_latency, commit_latency) =
                        operation_result.map_err(|error| {
                            format!("operational worker {worker_index} iteration {index}: {error}")
                        })?;
                    let worker_state = std::fs::read_to_string(format!("/proc/{worker_pid}/stat"))
                        .ok()
                        .and_then(|stat| {
                            stat.rsplit_once(") ")
                                .and_then(|(_, fields)| fields.split_whitespace().next())
                                .map(str::to_owned)
                        });
                    if worker_state
                        .as_deref()
                        .is_some_and(|state| state != "Z" && state != "X")
                    {
                        active_worker_point_read_latencies.push(point_read_latency);
                        active_worker_commit_latencies.push(commit_latency);
                    }
                    completed += 1;
                    task_completed_transactions.fetch_add(1, std::sync::atomic::Ordering::AcqRel);
                    if completed >= min_operations_before_exit
                        && task_done.load(std::sync::atomic::Ordering::Acquire)
                    {
                        break;
                    }
                    std::thread::sleep(Duration::from_millis(5));
                }
                Ok((
                    completed,
                    active_worker_point_read_latencies,
                    active_worker_commit_latencies,
                ))
            }));
        }
        let mut additional_cancellation_observers = Vec::new();
        for (
            worker_reservation,
            worker_accepted,
            worker_invocation,
            mut worker_executor,
            mut worker_service,
            mut worker_gateway,
        ) in additional_cancellation_workers
        {
            let worker_start = Arc::clone(&start);
            let active_workers = Arc::clone(&active_cancellation_workers);
            let memory_samples = Arc::clone(&additional_worker_memory_samples);
            let worker_pid = worker_executor.id();
            let worker_storage = &storage;
            let worker_id = worker_reservation.invocation_id;
            additional_cancellation_observers.push(scope.spawn(move || -> Result<(), String> {
                worker_start.wait();
                let observed =
                    worker_executor.observe_exit_with_results(&worker_invocation, |payload| {
                        if payload != b"CANCEL_READY"
                            && payload != b"CANCEL_OBSERVED"
                            && payload != b"CANCEL_UPDATE_CONFIRMED"
                        {
                            return Err(std::io::Error::new(
                                std::io::ErrorKind::InvalidData,
                                "unexpected additional cancellation result",
                            ));
                        }
                        if payload == b"CANCEL_READY" {
                            active_workers.fetch_add(1, std::sync::atomic::Ordering::AcqRel);
                            let sample = worker_cgroup_memory(worker_pid)?;
                            if sample.1 > 256 * 1024 * 1024 {
                                return Err(std::io::Error::other(format!(
                                    "additional analytical worker cgroup peak {} exceeded its 256 MiB limit",
                                    sample.1
                                )));
                            }
                            memory_samples
                                .lock()
                                .map_err(|_| {
                                    std::io::Error::other(
                                        "additional worker memory sample lock poisoned",
                                    )
                                })?
                                .push(sample);
                        }
                        send_gateway_result_data_event(
                            &mut worker_service,
                            worker_accepted.request_id,
                            payload.to_vec(),
                        )
                        .map_err(|error| {
                            std::io::Error::other(format!(
                                "additional cancellation relay: {error:?}"
                            ))
                        })
                    });
                match observed {
                    Ok(status) if !status.success() => {}
                    Err(prdb_engine::storage_manager::SupervisorInvocationError::WorkerExited(
                        status,
                    )) if !status.success() => {}
                    Err(prdb_engine::storage_manager::SupervisorInvocationError::Deadline(
                        prdb_engine::transaction::TransactionError::CancellationRequested
                        | prdb_engine::transaction::TransactionError::Aborted,
                    )) => {}
                    Ok(status) => {
                        return Err(format!(
                            "additional cancellation worker exited successfully: {status}"
                        ));
                    }
                    Err(error) => {
                        return Err(format!("additional cancellation worker failed: {error:?}"));
                    }
                }
                if worker_storage
                    .lookup_invocation_outcome(worker_id, user_id, now + 3)
                    .map_err(|error| format!("additional cancellation outcome: {error:?}"))?
                    .is_none_or(|outcome| outcome.status != OutcomeStatus::Aborted)
                {
                    return Err("additional cancellation did not durably abort".to_owned());
                }
                match send_gateway_terminal_for_confirmed_outcome(
                    worker_storage,
                    &worker_invocation,
                    &mut worker_service,
                    worker_accepted.request_id,
                    GatewayAbortReason::Cancelled,
                )
                .map_err(|error| format!("additional cancellation terminal: {error:?}"))?
                {
                    GatewayTerminalDecision::Abort(GatewayAbortReason::Cancelled) => {}
                    decision => {
                        return Err(format!(
                            "unexpected additional cancellation terminal: {decision:?}"
                        ));
                    }
                }
                for expected_payload in [
                    b"CANCEL_READY".as_slice(),
                    b"CANCEL_OBSERVED".as_slice(),
                    b"CANCEL_UPDATE_CONFIRMED".as_slice(),
                ] {
                    let result = read_frame(&mut worker_gateway)
                        .map_err(|error| format!("additional cancellation result read: {error}"))?
                        .ok_or("missing additional cancellation result event")?;
                    if result.operation != Operation::GatewayResultData as u16
                        || result.request_id != worker_accepted.request_id
                        || result.payload != expected_payload
                    {
                        return Err(format!(
                            "unexpected additional cancellation result frame: operation={} request_id={} payload={:?}",
                            result.operation, result.request_id, result.payload
                        ));
                    }
                }
                let terminal = read_frame(&mut worker_gateway)
                    .map_err(|error| format!("additional cancellation terminal read: {error}"))?
                    .ok_or("missing additional cancellation terminal event")?;
                let terminal_payload =
                    String::from_utf8(terminal.payload).map_err(|error| error.to_string())?;
                if terminal.operation != Operation::GatewayTerminal as u16
                    || !terminal_payload.contains("\"outcome\":\"aborted\"")
                {
                    return Err(format!(
                        "unexpected additional cancellation terminal frame: operation={} payload={terminal_payload}",
                        terminal.operation
                    ));
                }
                Ok(())
            }));
        }
        let cancellation_start = Arc::clone(&start);
        let cancellation_analytics_result_relayed = Arc::clone(&analytics_result_relayed);
        let cancellation_active_transactions = Arc::clone(&active_operational_transactions);
        let cancellation_completed_transactions = Arc::clone(&completed_operational_transactions);
        let cancellation_active_workers = Arc::clone(&active_cancellation_workers);
        let cancellation_worker_pid = worker_pid;
        let cancellation_executor_pid = cancellation_executor.id();
        let cancellation_memory_samples = Arc::clone(&additional_worker_memory_samples);
        let cancellation_saturation_started = saturation_started_rx;
        let cancellation_observer = scope.spawn(move || {
            let result = (|| -> Result<_, Box<dyn std::error::Error>> {
                cancellation_start.wait();
                let mut cancellation_requested_at: Option<std::time::Instant> = None;
                let mut cancellation_request_latency: Option<Duration> = None;
                let mut cancellation_observed_latency: Option<Duration> = None;
                let mut analytics_memory_sample = None;
                let cancellation_observation = cancellation_executor
                    .observe_exit_with_results(&cancellation_invocation, |payload| {
                        if payload != b"CANCEL_READY"
                            && payload != b"CANCEL_OBSERVED"
                            && payload != b"CANCEL_UPDATE_CONFIRMED"
                        {
                            return Err(std::io::Error::new(
                                std::io::ErrorKind::InvalidData,
                                "unexpected cancellation fixture result",
                            ));
                        }
                        send_gateway_result_data_event(
                            &mut cancellation_service,
                            cancellation_accepted.request_id,
                            payload.to_vec(),
                        )
                        .map_err(|error| {
                            std::io::Error::other(format!("result relay: {error:?}"))
                        })?;
                        if payload == b"CANCEL_READY" {
                            cancellation_active_workers
                                .fetch_add(1, std::sync::atomic::Ordering::AcqRel);
                            let sample = worker_cgroup_memory(cancellation_executor_pid)?;
                            if sample.1 > 256 * 1024 * 1024 {
                                return Err(std::io::Error::other(format!(
                                    "cancellation worker cgroup peak {} exceeded its 256 MiB limit",
                                    sample.1
                                )));
                            }
                            cancellation_memory_samples
                                .lock()
                                .map_err(|_| {
                                    std::io::Error::other(
                                        "cancellation worker memory sample lock poisoned",
                                    )
                                })?
                                .push(sample);
                        }
                        if payload == b"CANCEL_OBSERVED" {
                            let requested_at = cancellation_requested_at.ok_or_else(|| {
                                std::io::Error::other(
                                    "query reported cancellation before it was requested",
                                )
                            })?;
                            cancellation_observed_latency = Some(requested_at.elapsed());
                            return Ok(());
                        }
                        if payload == b"CANCEL_UPDATE_CONFIRMED" {
                            if cancellation_observed_latency.is_none() {
                                return Err(std::io::Error::other(
                                    "UPDATE returned before cancellation was observed",
                                ));
                            }
                            return Ok(());
                        }
                        let live_deadline = std::time::Instant::now() + Duration::from_secs(2);
                        let mut analytics_worker_live = false;
                        while std::time::Instant::now() < live_deadline {
                            let state = std::fs::read_to_string(format!(
                                "/proc/{cancellation_worker_pid}/stat"
                            ))
                            .ok()
                            .and_then(|stat| {
                                stat.rsplit_once(") ")
                                    .and_then(|(_, fields)| fields.split_whitespace().next())
                                    .map(str::to_owned)
                            });
                            analytics_worker_live = state
                                .as_deref()
                                .is_some_and(|state| state != "Z" && state != "X");
                            if cancellation_analytics_result_relayed
                                .load(std::sync::atomic::Ordering::Acquire)
                                && analytics_worker_live
                                && cancellation_active_transactions
                                    .load(std::sync::atomic::Ordering::Acquire)
                                    > 0
                                && cancellation_active_workers
                                    .load(std::sync::atomic::Ordering::Acquire)
                                    == 7
                            {
                                break;
                            }
                            std::thread::sleep(Duration::from_millis(1));
                        }
                        if !cancellation_analytics_result_relayed
                            .load(std::sync::atomic::Ordering::Acquire)
                            || !analytics_worker_live
                            || cancellation_active_transactions
                                .load(std::sync::atomic::Ordering::Acquire)
                                == 0
                            || cancellation_active_workers
                                .load(std::sync::atomic::Ordering::Acquire)
                                != 7
                        {
                            return Err(std::io::Error::other(
                                format!(
                                    "three cancellation SQL workers did not overlap relayed analytics and an operational transaction (relayed={}, analytics_live={}, active_transactions={}, cancellation_workers={})",
                                    cancellation_analytics_result_relayed.load(std::sync::atomic::Ordering::Acquire),
                                    analytics_worker_live,
                                    cancellation_active_transactions.load(std::sync::atomic::Ordering::Acquire),
                                    cancellation_active_workers.load(std::sync::atomic::Ordering::Acquire),
                                ),
                            ));
                        }
                        cancellation_saturation_started
                            .recv_timeout(Duration::from_secs(10))
                            .map_err(|error| {
                                std::io::Error::other(format!(
                                    "primary SQL worker did not reach its sustained interval: {error}"
                                ))
                            })?;
                        let transactions_before_saturation =
                            cancellation_completed_transactions
                                .load(std::sync::atomic::Ordering::Acquire);
                        let saturation_started_at = std::time::Instant::now();
                        std::thread::sleep(Duration::from_secs(30));
                        let saturation_elapsed = saturation_started_at.elapsed();
                        let operational_progress = cancellation_completed_transactions
                            .load(std::sync::atomic::Ordering::Acquire)
                            .saturating_sub(transactions_before_saturation);
                        let primary_state = std::fs::read_to_string(format!(
                            "/proc/{cancellation_worker_pid}/stat"
                        ))
                        .ok()
                        .and_then(|stat| {
                            stat.rsplit_once(") ")
                                .and_then(|(_, fields)| fields.split_whitespace().next())
                                .map(str::to_owned)
                        });
                        if saturation_elapsed < Duration::from_secs(30)
                            || operational_progress < 4
                            || primary_state
                                .as_deref()
                                .is_none_or(|state| state == "Z" || state == "X")
                            || cancellation_active_workers
                                .load(std::sync::atomic::Ordering::Acquire)
                                != 7
                        {
                            return Err(std::io::Error::other(format!(
                                "saturated analytics did not sustain operational progress: elapsed={saturation_elapsed:?}, commits={operational_progress}"
                            )));
                        }
                        eprintln!(
                            "eight-worker saturation interval: elapsed={} ms operational_commits={operational_progress}",
                            saturation_elapsed.as_millis()
                        );
                        let memory_sample = worker_cgroup_memory(cancellation_worker_pid)?;
                        let (_, analytics_memory_peak_bytes) = memory_sample;
                        analytics_memory_sample = Some(memory_sample);
                        if analytics_memory_peak_bytes > 256 * 1024 * 1024 {
                            return Err(std::io::Error::other(format!(
                                "analytical worker cgroup peak {analytics_memory_peak_bytes} exceeded its 256 MiB limit"
                            )));
                        }
                        let mut analytical_statuses = vec![
                            saturated_status_storage.get_invocation_status(
                                reservation.invocation_id,
                                user_id,
                                now + 4,
                            )
                            .map_err(std::io::Error::other)?,
                            saturated_status_storage.get_invocation_status(
                                cancellation_reservation.invocation_id,
                                user_id,
                                now + 4,
                            )
                            .map_err(std::io::Error::other)?,
                        ];
                        analytical_statuses.extend(
                            additional_cancellation_ids.iter().map(|invocation_id| {
                                saturated_status_storage.get_invocation_status(
                                    *invocation_id,
                                    user_id,
                                    now + 4,
                                )
                            }).collect::<Result<Vec<_>, _>>()
                                .map_err(std::io::Error::other)?,
                        );
                        if analytical_statuses.len() != 8
                            || analytical_statuses.iter().any(|status| {
                                status.as_ref().is_none_or(|snapshot| {
                                    !matches!(
                                        &snapshot.status,
                                        prdb_engine::storage_manager::InvocationStatus::Active { .. }
                                    )
                                })
                            })
                        {
                            return Err(std::io::Error::other(format!(
                                "not all eight analytical invocations remained active during sustained load: {analytical_statuses:?}"
                            )));
                        }
                        let saturated_start = saturated_client.handle_start_request_with_executor(
                            saturated_storage,
                            saturated_staging,
                            ClientFrame {
                                kind: 7,
                                request_id: 3,
                                response_sequence: 0,
                                payload: format!(
                                    "{{\"invocation_id\":\"{saturated_invocation_hex}\"}}"
                                )
                                .into_bytes(),
                            },
                            GatewayAdmissionConfig {
                                retention_period_us: 3_600_000_000,
                                max_rows: 8,
                                max_bytes: 4096,
                            },
                            saturated_runtime,
                        );
                        match saturated_start {
                            Err(prdb_engine::client_protocol::ClientStartError::ExecutorStart(
                                prdb_engine::supervisor::StartError::Admission(
                                    prdb_engine::executor_launcher::ExecutorStartError::Admission {
                                        error: prdb_engine::transaction::TransactionError::AdmissionLimitExceeded,
                                        ..
                                    },
                                ),
                            )) => {}
                            Err(error) => {
                                return Err(std::io::Error::other(format!(
                                    "ninth analytical START failed for the wrong reason: {error:?}"
                                )));
                            }
                            Ok(_) => {
                                return Err(std::io::Error::other(
                                    "ninth analytical START was accepted above the configured cap",
                                ));
                            }
                        }
                        let request_started = std::time::Instant::now();
                        match cancellation_storage
                            .cancel_owned_invocation(
                                user_id,
                                cancellation_reservation.invocation_id,
                                now + 2,
                            )
                            .map_err(|error| {
                                std::io::Error::other(format!(
                                    "cancellation request: {error:?}"
                                ))
                            })? {
                            prdb_engine::storage_manager::OwnerCancellationResult::Accepted => {
                                cancellation_request_latency = Some(request_started.elapsed());
                                cancellation_requested_at = Some(std::time::Instant::now());
                                for invocation_id in &additional_cancellation_ids {
                                    match cancellation_storage
                                        .cancel_owned_invocation(
                                            user_id,
                                            *invocation_id,
                                            now + 2,
                                        )
                                        .map_err(|error| {
                                            std::io::Error::other(format!(
                                                "additional cancellation request: {error:?}"
                                            ))
                                        })? {
                                        prdb_engine::storage_manager::OwnerCancellationResult::Accepted => {}
                                        result => {
                                            return Err(std::io::Error::other(format!(
                                                "unexpected additional cancellation result: {result:?}"
                                            )));
                                        }
                                    }
                                }
                                Ok(())
                            }
                            result => Err(std::io::Error::other(format!(
                                "unexpected cancellation result: {result:?}"
                            ))),
                        }
                    });
                match cancellation_observation {
                    Ok(status) if !status.success() => {}
                    Err(
                        prdb_engine::storage_manager::SupervisorInvocationError::WorkerExited(
                            status,
                        ),
                    ) if !status.success() => {}
                    Err(prdb_engine::storage_manager::SupervisorInvocationError::Deadline(
                        prdb_engine::transaction::TransactionError::CancellationRequested
                        | prdb_engine::transaction::TransactionError::Aborted,
                    )) => {}
                    Ok(status) => {
                        return Err(format!(
                            "cancellation worker unexpectedly exited successfully: {status}"
                        )
                        .into());
                    }
                    Err(error) => return Err(error.into()),
                }
                let cancellation_observed_latency = cancellation_observed_latency
                    .ok_or("query never observed cancellation through SDK poll")?;
                let (analytics_memory_current_bytes, analytics_memory_peak_bytes) =
                    analytics_memory_sample.ok_or("live SQL cgroup memory was not sampled")?;
                let cancellation_request_latency =
                    cancellation_request_latency.ok_or("cancellation request was not timed")?;
                if cancellation_observed_latency >= Duration::from_secs(2) {
                    return Err(format!(
                        "query cancellation poll was relayed after {cancellation_observed_latency:?}"
                    )
                    .into());
                }
                let ready_event = read_frame(&mut cancellation_gateway)?
                    .ok_or("missing cancel ready event")?;
                assert_eq!(ready_event.operation, Operation::GatewayResultData as u16);
                assert_eq!(ready_event.request_id, cancellation_accepted.request_id);
                assert_eq!(ready_event.payload, b"CANCEL_READY");
                let observed_event = read_frame(&mut cancellation_gateway)?
                    .ok_or("missing cancellation-observed result")?;
                assert_eq!(observed_event.operation, Operation::GatewayResultData as u16);
                assert_eq!(observed_event.request_id, cancellation_accepted.request_id);
                assert_eq!(observed_event.payload, b"CANCEL_OBSERVED");
                let update_event = read_frame(&mut cancellation_gateway)?
                    .ok_or("missing cancelled UPDATE confirmation")?;
                assert_eq!(update_event.operation, Operation::GatewayResultData as u16);
                assert_eq!(update_event.request_id, cancellation_accepted.request_id);
                assert_eq!(update_event.payload, b"CANCEL_UPDATE_CONFIRMED");
                assert_eq!(
                    cancellation_storage
                        .lookup_invocation_outcome(
                            cancellation_reservation.invocation_id,
                            user_id,
                            now + 3,
                        )?
                        .ok_or("missing durable cancellation outcome")?
                        .status,
                    OutcomeStatus::Aborted
                );
                assert_eq!(
                    send_gateway_terminal_for_confirmed_outcome(
                        cancellation_storage,
                        &cancellation_invocation,
                        &mut cancellation_service,
                        cancellation_accepted.request_id,
                        GatewayAbortReason::Cancelled,
                    )?,
                    GatewayTerminalDecision::Abort(GatewayAbortReason::Cancelled)
                );
                let cancellation_terminal = read_frame(&mut cancellation_gateway)?
                    .ok_or("missing cancellation terminal event")?;
                assert_eq!(
                    cancellation_terminal.operation,
                    Operation::GatewayTerminal as u16
                );
                assert!(String::from_utf8(cancellation_terminal.payload)?
                    .contains("\"outcome\":\"aborted\""));
                Ok((
                    cancellation_request_latency,
                    cancellation_observed_latency,
                    analytics_memory_current_bytes,
                    analytics_memory_peak_bytes,
                    true,
                ))
            })();
            result.map_err(|error| error.to_string())
        });
        start.wait();
        let mut index_build_start_tx = Some(index_build_start_tx);
        let mut index_build_started_rx = Some(index_build_started_rx);
        let saturation_started_tx = saturation_started_tx;
        let mut analytical_group_resumed_at = None;
        let mut optimizer_range_latency_samples = Vec::with_capacity(2);
        let optimizer_relation_descriptions = std::sync::atomic::AtomicUsize::new(0);
        let optimizer_planner_dispatch_samples_for_worker =
            Arc::clone(&optimizer_planner_dispatch_samples);
        let worker_status_result = executor
            .observe_exit_with_results_and_optimizer_planner_timing_observer(
                &invocation,
                |payload| {
                    if payload.len() == 16 && &payload[..7] == b"OP5RNG1" {
                        let elapsed_ns = u64::from_be_bytes(
                            payload[8..16]
                                .try_into()
                                .expect("range latency payload has a fixed-width duration"),
                        );
                        optimizer_range_latency_samples.push((payload[7], elapsed_ns));
                        return Ok(());
                    }
                    send_gateway_result_data_event(
                        &mut service,
                        accepted.request_id,
                        payload.to_vec(),
                    )
                    .map_err(|error| std::io::Error::other(format!("result relay: {error:?}")))?;
                    if payload == b"ANALYTICAL_GROUP_STARTED" {
                        index_build_start_tx
                            .take()
                            .ok_or_else(|| std::io::Error::other("index build already started"))?
                            .send(())
                            .map_err(|error| {
                                std::io::Error::other(format!("start index build: {error}"))
                            })?;
                        index_build_started_rx
                            .take()
                            .ok_or_else(|| {
                                std::io::Error::other("index build start already observed")
                            })?
                            .recv()
                            .map_err(|error| {
                                std::io::Error::other(format!("observe index build start: {error}"))
                            })?;
                    } else if payload == b"ANALYTICAL_GROUP_RESUMED" {
                        analytical_group_resumed_at = Some(std::time::Instant::now());
                    }
                    analytics_result_relayed.store(true, std::sync::atomic::Ordering::Release);
                    if payload == b"ANALYTICAL_GROUP_RESUMED" {
                        saturation_started_tx.send(()).map_err(|error| {
                            std::io::Error::other(format!(
                                "notify sustained analytics window: {error}"
                            ))
                        })?;
                    }
                    Ok(())
                },
                |operation, _request_id| {
                    if operation == Operation::DescribePlanRelation as u16 {
                        optimizer_relation_descriptions
                            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    }
                },
                move |operation, request_id, elapsed| {
                    optimizer_planner_dispatch_samples_for_worker
                        .lock()
                        .expect("optimizer planner timing samples")
                        .push((operation, request_id, elapsed));
                },
                true,
            );
        let (index_build_started_at, index_build_finished_at, index_build_id) = index_build_worker
            .join()
            .map_err(|_| std::io::Error::other("analytical pressure index builder panicked"))?
            .map_err(std::io::Error::other)?;
        let analytical_group_resumed_at =
            analytical_group_resumed_at.ok_or("missing resumed analytical group marker")?;
        assert_ne!(index_build_id, [0; 16]);
        assert!(index_build_started_at < analytical_group_resumed_at);
        assert!(analytical_group_resumed_at < index_build_finished_at);
        let (
            cancellation_request_latency,
            cancellation_observed_latency,
            analytics_memory_current_bytes,
            analytics_memory_peak_bytes,
            analytical_cap_rejected,
        ) = cancellation_observer
            .join()
            .map_err(|_| std::io::Error::other("cancellation observer thread panicked"))?
            .map_err(std::io::Error::other)?;
        for observer in additional_cancellation_observers {
            observer
                .join()
                .map_err(|_| std::io::Error::other("additional cancellation observer panicked"))?
                .map_err(std::io::Error::other)?;
        }
        let (main_worker_current, main_worker_peak) =
            (analytics_memory_current_bytes, analytics_memory_peak_bytes);
        let mut additional_memory_samples = additional_worker_memory_samples
            .lock()
            .map_err(|_| std::io::Error::other("worker memory sample lock poisoned"))?;
        if additional_memory_samples.len() != 7 {
            return Err(format!(
                "sampled {} of 7 additional analytical worker cgroups",
                additional_memory_samples.len()
            )
            .into());
        }
        additional_memory_samples.push((main_worker_current, main_worker_peak));
        let analytics_memory_current_bytes = additional_memory_samples
            .iter()
            .map(|(current, _)| *current)
            .sum::<u64>();
        let analytics_memory_peak_bytes = additional_memory_samples
            .iter()
            .map(|(_, peak)| *peak)
            .sum::<u64>();
        drop(additional_memory_samples);
        observer_done.store(true, std::sync::atomic::Ordering::Release);
        let mut operational_count = 0;
        let mut active_worker_point_read_latencies = Vec::new();
        let mut active_worker_commit_latencies = Vec::new();
        for operational in operational_workers {
            let (completed, point_read_latencies, commit_latencies) = operational
                .join()
                .map_err(|_| std::io::Error::other("operational workload thread panicked"))?
                .map_err(std::io::Error::other)?;
            operational_count += completed;
            active_worker_point_read_latencies.extend(point_read_latencies);
            active_worker_commit_latencies.extend(commit_latencies);
        }
        for index in 0..4u8 {
            let stored = row(
                &database,
                &table_row_key(operational_pressure_table.table_id, id(0xed + index)),
            )?
            .ok_or("operational pressure update removed its row")?;
            let tuple = decode_tuple(&operational_pressure_schema, 1, &stored.payload)?;
            if !matches!(
                tuple.values.as_slice(),
                [
                    Some(ScalarValue::Integer(key)),
                    Some(ScalarValue::Integer(value)),
                ] if *key == i32::from(index) && *value > 0
            ) {
                return Err(format!(
                    "operational pressure row {index} has unexpected committed values: {:?}",
                    tuple.values
                )
                .into());
            }
        }
        let peak_active_operational_transactions =
            peak_active_operational_transactions.load(std::sync::atomic::Ordering::Acquire);
        if peak_active_operational_transactions < 64 {
            return Err(format!(
                "only {peak_active_operational_transactions} operational transactions were simultaneously admitted; expected 64"
            )
            .into());
        }
        Ok::<_, Box<dyn std::error::Error>>((
            worker_status_result,
            operational_count,
            active_worker_point_read_latencies,
            active_worker_commit_latencies,
            cancellation_request_latency,
            cancellation_observed_latency,
            analytics_memory_current_bytes,
            analytics_memory_peak_bytes,
            analytical_cap_rejected,
            optimizer_range_latency_samples,
            optimizer_planner_dispatch_samples
                .lock()
                .expect("optimizer planner timing samples")
                .clone(),
        ))
    })?;
    assert!(analytical_cap_rejected);
    eprintln!(
        "analytical worker cgroup memory during live SQL: current={} KiB peak={} KiB",
        analytics_memory_current_bytes / 1024,
        analytics_memory_peak_bytes / 1024,
    );
    assert!(concurrent_operational_invocations >= 64);
    assert!(
        !active_worker_point_read_latencies.is_empty()
            && !active_worker_commit_latencies.is_empty(),
        "operational scans and durable commits did not overlap the live SQL worker"
    );
    let mut active_worker_point_read_latencies = active_worker_point_read_latencies;
    let mut active_worker_commit_latencies = active_worker_commit_latencies;
    active_worker_point_read_latencies.sort_unstable();
    active_worker_commit_latencies.sort_unstable();
    let percentile_us = |samples: &[Duration], percentile: usize| {
        let rank = samples.len().saturating_mul(percentile).div_ceil(100);
        samples[rank.saturating_sub(1)].as_micros()
    };
    eprintln!(
        "operational latency (us): point read baseline n={} p50={} p95={} p99={} / confined SQL n={} p50={} p95={} p99={}; durable commit baseline n={} p50={} p95={} p99={} / confined SQL n={} p50={} p95={} p99={}",
        operational_only_point_reads.len(),
        percentile_us(&operational_only_point_reads, 50),
        percentile_us(&operational_only_point_reads, 95),
        percentile_us(&operational_only_point_reads, 99),
        active_worker_point_read_latencies.len(),
        percentile_us(&active_worker_point_read_latencies, 50),
        percentile_us(&active_worker_point_read_latencies, 95),
        percentile_us(&active_worker_point_read_latencies, 99),
        operational_only_commits.len(),
        percentile_us(&operational_only_commits, 50),
        percentile_us(&operational_only_commits, 95),
        percentile_us(&operational_only_commits, 99),
        active_worker_commit_latencies.len(),
        percentile_us(&active_worker_commit_latencies, 50),
        percentile_us(&active_worker_commit_latencies, 95),
        percentile_us(&active_worker_commit_latencies, 99),
    );
    let worker_status = match worker_status_result {
        Ok(status) => status,
        Err(error) => {
            gateway.set_read_timeout(Some(Duration::from_millis(100)))?;
            let mut relayed_payloads = Vec::new();
            while let Ok(Some(frame)) = read_frame(&mut gateway) {
                relayed_payloads.push(frame.payload);
            }
            return Err(format!("{error:?}; relayed payloads: {relayed_payloads:?}").into());
        }
    };
    assert!(
        worker_status.success(),
        "sandbox query worker failed: {worker_status}"
    );
    assert_eq!(
        optimizer_range_latency_samples.len(),
        2,
        "confined range fixture must report optimizer and reference timings"
    );
    assert_eq!(optimizer_range_latency_samples[0].0, b'L');
    assert_eq!(optimizer_range_latency_samples[1].0, b'F');
    assert!(
        optimizer_range_latency_samples
            .iter()
            .all(|(_, ns)| *ns > 0)
    );
    eprintln!(
        "OP-5 confined SQL range latency (ns): 1,024-row duplicate range optimizer={} reference={}",
        optimizer_range_latency_samples[0].1, optimizer_range_latency_samples[1].1,
    );
    assert!(
        !optimizer_planner_dispatch_samples.is_empty(),
        "confined SQL worker must report optimizer planner dispatch samples"
    );
    let mut optimizer_planner_dispatch_samples = optimizer_planner_dispatch_samples;
    optimizer_planner_dispatch_samples.sort_by_key(|(_, _, elapsed)| *elapsed);
    let dispatch_p50 = optimizer_planner_dispatch_samples
        [(optimizer_planner_dispatch_samples.len() - 1) * 50 / 100]
        .2;
    let dispatch_p95 = optimizer_planner_dispatch_samples
        [(optimizer_planner_dispatch_samples.len() - 1) * 95 / 100]
        .2;
    eprintln!(
        "confined optimizer planner storage dispatch (requests={} p50={:.3} ms p95={:.3} ms); excludes optimizer scheduling and socket delivery",
        optimizer_planner_dispatch_samples.len(),
        dispatch_p50.as_secs_f64() * 1_000.0,
        dispatch_p95.as_secs_f64() * 1_000.0,
    );
    staging.finish_started(owner, reservation.invocation_id)?;
    let analytical_group_started =
        read_frame(&mut gateway)?.ok_or("missing bounded analytical group marker")?;
    assert_eq!(
        analytical_group_started.operation,
        Operation::GatewayResultData as u16
    );
    assert_eq!(analytical_group_started.request_id, accepted.request_id);
    assert_eq!(
        analytical_group_started.payload,
        b"ANALYTICAL_GROUP_STARTED"
    );
    let analytical_group_resumed =
        read_frame(&mut gateway)?.ok_or("missing resumed analytical group marker")?;
    assert_eq!(
        analytical_group_resumed.operation,
        Operation::GatewayResultData as u16
    );
    assert_eq!(analytical_group_resumed.request_id, accepted.request_id);
    assert_eq!(
        analytical_group_resumed.payload,
        b"ANALYTICAL_GROUP_RESUMED"
    );
    let inserted = read_frame(&mut gateway)?.ok_or("missing INSERT visibility result")?;
    assert_eq!(inserted.operation, Operation::GatewayResultData as u16);
    assert_eq!(inserted.request_id, accepted.request_id);
    assert_eq!(inserted.payload, b"insert-visible");
    let metadata = read_frame(&mut gateway)?.ok_or("missing SQL metadata result")?;
    assert_eq!(metadata.operation, Operation::GatewayResultData as u16);
    assert_eq!(metadata.request_id, accepted.request_id);
    assert_eq!(metadata.payload, expected_schema);
    let tables = read_frame(&mut gateway)?.ok_or("missing SQL tables metadata result")?;
    assert_eq!(tables.operation, Operation::GatewayResultData as u16);
    assert_eq!(tables.request_id, accepted.request_id);
    assert_eq!(tables.payload, expected_table);
    let columns = read_frame(&mut gateway)?.ok_or("missing SQL columns metadata result")?;
    assert_eq!(columns.operation, Operation::GatewayResultData as u16);
    assert_eq!(columns.request_id, accepted.request_id);
    assert_eq!(columns.payload, expected_column);
    let user_schema = read_frame(&mut gateway)?.ok_or("missing SQL user schema result")?;
    assert_eq!(user_schema.operation, Operation::GatewayResultData as u16);
    assert_eq!(user_schema.request_id, accepted.request_id);
    assert_eq!(user_schema.payload, expected_user_schema);
    let user_table = read_frame(&mut gateway)?.ok_or("missing SQL user table result")?;
    assert_eq!(user_table.operation, Operation::GatewayResultData as u16);
    assert_eq!(user_table.request_id, accepted.request_id);
    assert!(
        user_table.payload == b"records"
            || user_table.payload == b"linked_records"
            || user_table.payload == b"grouped_records"
            || user_table.payload == b"analytics_pressure_records"
            || user_table.payload == b"analytics_range_benchmark_records"
            || user_table.payload == b"decimal_grouped_records"
            || user_table.payload == b"bigint_grouped_records"
            || user_table.payload == b"identity_records",
        "unexpected SQL user table result: {:?}",
        user_table.payload
    );
    let user_column = read_frame(&mut gateway)?.ok_or("missing SQL user column result")?;
    assert_eq!(user_column.operation, Operation::GatewayResultData as u16);
    assert_eq!(user_column.request_id, accepted.request_id);
    assert_eq!(user_column.payload, expected_user_column);
    let empty_schema_result = read_frame(&mut gateway)?.ok_or("missing empty schema SQL result")?;
    assert_eq!(
        empty_schema_result.operation,
        Operation::GatewayResultData as u16
    );
    assert_eq!(empty_schema_result.request_id, accepted.request_id);
    assert_eq!(empty_schema_result.payload, expected_empty_schema);
    let updated = read_frame(&mut gateway)?.ok_or("missing UPDATE visibility result")?;
    assert_eq!(updated.operation, Operation::GatewayResultData as u16);
    assert_eq!(updated.request_id, accepted.request_id);
    assert_eq!(updated.payload, b"update-visible");
    let grouped_composite_first =
        read_frame(&mut gateway)?.ok_or("missing first composite GROUP BY SQLR result")?;
    assert_eq!(
        grouped_composite_first.payload,
        [
            b"SQLR".as_slice(),
            &[
                0, 1, 0, 3, 0, 8, b'l', b'e', b'f', b't', b'_', b'k', b'e', b'y', 0, 0, 0, 3, 0, 0,
                0, 0, 0, 0, 4, 0, 0, 0, 1, 0, 9, b'r', b'i', b'g', b'h', b't', b'_', b'k', b'e',
                b'y', 0, 0, 0, 3, 0, 0, 0, 0, 0, 0, 4, 0, 0, 0, 7, 0, 5, b'c', b'o', b'u', b'n',
                b't', 0, 0, 0, 4, 0, 0, 0, 0, 0, 0, 8, 0, 0, 0, 0, 0, 0, 0, 2,
            ][..],
        ]
        .concat()
    );
    let grouped_composite_second =
        read_frame(&mut gateway)?.ok_or("missing second composite GROUP BY SQLR result")?;
    assert_eq!(
        grouped_composite_second.payload,
        [
            b"SQLR".as_slice(),
            &[
                0, 1, 0, 3, 0, 8, b'l', b'e', b'f', b't', b'_', b'k', b'e', b'y', 0, 0, 0, 3, 0, 0,
                0, 0, 0, 0, 4, 0, 0, 0, 2, 0, 9, b'r', b'i', b'g', b'h', b't', b'_', b'k', b'e',
                b'y', 0, 0, 0, 3, 0, 0, 0, 0, 0, 0, 4, 0, 0, 0, 9, 0, 5, b'c', b'o', b'u', b'n',
                b't', 0, 0, 0, 4, 0, 0, 0, 0, 0, 0, 8, 0, 0, 0, 0, 0, 0, 0, 3,
            ][..],
        ]
        .concat()
    );
    let grouped_composite_order_first =
        read_frame(&mut gateway)?.ok_or("missing first composite ORDER BY SQLR result")?;
    assert_eq!(
        grouped_composite_order_first.payload,
        [
            b"SQLR".as_slice(),
            &[
                0, 1, 0, 3, 0, 8, b'l', b'e', b'f', b't', b'_', b'k', b'e', b'y', 0, 0, 0, 3, 0, 0,
                0, 0, 0, 0, 4, 0, 0, 0, 2, 0, 9, b'r', b'i', b'g', b'h', b't', b'_', b'k', b'e',
                b'y', 0, 0, 0, 3, 0, 0, 0, 0, 0, 0, 4, 0, 0, 0, 9, 0, 5, b'c', b'o', b'u', b'n',
                b't', 0, 0, 0, 4, 0, 0, 0, 0, 0, 0, 8, 0, 0, 0, 0, 0, 0, 0, 3,
            ][..],
        ]
        .concat()
    );
    let grouped_composite_order_second =
        read_frame(&mut gateway)?.ok_or("missing second composite ORDER BY SQLR result")?;
    assert_eq!(
        grouped_composite_order_second.payload,
        [
            b"SQLR".as_slice(),
            &[
                0, 1, 0, 3, 0, 8, b'l', b'e', b'f', b't', b'_', b'k', b'e', b'y', 0, 0, 0, 3, 0, 0,
                0, 0, 0, 0, 4, 0, 0, 0, 1, 0, 9, b'r', b'i', b'g', b'h', b't', b'_', b'k', b'e',
                b'y', 0, 0, 0, 3, 0, 0, 0, 0, 0, 0, 4, 0, 0, 0, 7, 0, 5, b'c', b'o', b'u', b'n',
                b't', 0, 0, 0, 4, 0, 0, 0, 0, 0, 0, 8, 0, 0, 0, 0, 0, 0, 0, 2,
            ][..],
        ]
        .concat()
    );
    let grouped_composite_literal_first =
        read_frame(&mut gateway)?.ok_or("missing first literal HAVING SQLR result")?;
    assert_eq!(
        grouped_composite_literal_first.payload,
        [
            b"SQLR".as_slice(),
            &[
                0, 1, 0, 3, 0, 8, b'l', b'e', b'f', b't', b'_', b'k', b'e', b'y', 0, 0, 0, 3, 0, 0,
                0, 0, 0, 0, 4, 0, 0, 0, 2, 0, 9, b'r', b'i', b'g', b'h', b't', b'_', b'k', b'e',
                b'y', 0, 0, 0, 3, 0, 0, 0, 0, 0, 0, 4, 0, 0, 0, 9, 0, 5, b'c', b'o', b'u', b'n',
                b't', 0, 0, 0, 4, 0, 0, 0, 0, 0, 0, 8, 0, 0, 0, 0, 0, 0, 0, 3,
            ][..],
        ]
        .concat()
    );
    let grouped_composite_literal_second =
        read_frame(&mut gateway)?.ok_or("missing second literal HAVING SQLR result")?;
    assert_eq!(
        grouped_composite_literal_second.payload,
        [
            b"SQLR".as_slice(),
            &[
                0, 1, 0, 3, 0, 8, b'l', b'e', b'f', b't', b'_', b'k', b'e', b'y', 0, 0, 0, 3, 0, 0,
                0, 0, 0, 0, 4, 0, 0, 0, 1, 0, 9, b'r', b'i', b'g', b'h', b't', b'_', b'k', b'e',
                b'y', 0, 0, 0, 3, 0, 0, 0, 0, 0, 0, 4, 0, 0, 0, 7, 0, 5, b'c', b'o', b'u', b'n',
                b't', 0, 0, 0, 4, 0, 0, 0, 0, 0, 0, 8, 0, 0, 0, 0, 0, 0, 0, 2,
            ][..],
        ]
        .concat()
    );
    let grouped_composite_boolean =
        read_frame(&mut gateway)?.ok_or("missing compound HAVING SQLR result")?;
    assert_eq!(
        grouped_composite_boolean.operation,
        Operation::GatewayResultData as u16
    );
    assert_eq!(grouped_composite_boolean.request_id, accepted.request_id);
    assert_eq!(
        grouped_composite_boolean.payload,
        [
            b"SQLR".as_slice(),
            &[
                0, 1, 0, 3, 0, 8, b'l', b'e', b'f', b't', b'_', b'k', b'e', b'y', 0, 0, 0, 3, 0, 0,
                0, 0, 0, 0, 4, 0, 0, 0, 2, 0, 9, b'r', b'i', b'g', b'h', b't', b'_', b'k', b'e',
                b'y', 0, 0, 0, 3, 0, 0, 0, 0, 0, 0, 4, 0, 0, 0, 9, 0, 5, b'c', b'o', b'u', b'n',
                b't', 0, 0, 0, 4, 0, 0, 0, 0, 0, 0, 8, 0, 0, 0, 0, 0, 0, 0, 3,
            ][..],
        ]
        .concat()
    );
    let grouped_composite_boolean_second =
        read_frame(&mut gateway)?.ok_or("missing second compound HAVING SQLR result")?;
    assert_eq!(
        grouped_composite_boolean_second.operation,
        Operation::GatewayResultData as u16
    );
    assert_eq!(
        grouped_composite_boolean_second.request_id,
        accepted.request_id
    );
    assert_eq!(
        grouped_composite_boolean_second.payload,
        [
            b"SQLR".as_slice(),
            &[
                0, 1, 0, 3, 0, 8, b'l', b'e', b'f', b't', b'_', b'k', b'e', b'y', 0, 0, 0, 3, 0, 0,
                0, 0, 0, 0, 4, 0, 0, 0, 1, 0, 9, b'r', b'i', b'g', b'h', b't', b'_', b'k', b'e',
                b'y', 0, 0, 0, 3, 0, 0, 0, 0, 0, 0, 4, 0, 0, 0, 7, 0, 5, b'c', b'o', b'u', b'n',
                b't', 0, 0, 0, 4, 0, 0, 0, 0, 0, 0, 8, 0, 0, 0, 0, 0, 0, 0, 2,
            ][..],
        ]
        .concat()
    );
    let grouped_composite_avg_numeric_v21 =
        read_frame(&mut gateway)?.ok_or("missing Decimal AVG v21 numeric HAVING SQLR result")?;
    assert_eq!(
        grouped_composite_avg_numeric_v21.operation,
        Operation::GatewayResultData as u16
    );
    assert_eq!(
        grouped_composite_avg_numeric_v21.request_id,
        accepted.request_id
    );
    assert_eq!(
        grouped_composite_avg_numeric_v21.payload,
        [
            b"SQLR".as_slice(),
            &[
                0, 1, 0, 3, 0, 8, b'l', b'e', b'f', b't', b'_', b'k', b'e', b'y', 0, 0, 0, 3, 0, 0,
                0, 0, 0, 0, 4, 0, 0, 0, 2, 0, 9, b'r', b'i', b'g', b'h', b't', b'_', b'k', b'e',
                b'y', 0, 0, 0, 3, 0, 0, 0, 0, 0, 0, 4, 0, 0, 0, 9, 0, 3, b'a', b'v', b'g', 0, 0, 0,
                5, 38, 8, 0, 0, 0, 0, 16, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 8, 240, 209, 128,
            ][..],
        ]
        .concat()
    );
    let grouped_composite_min_numeric_v21 =
        read_frame(&mut gateway)?.ok_or("missing Decimal MIN v21 numeric HAVING SQLR result")?;
    assert_eq!(
        grouped_composite_min_numeric_v21.operation,
        Operation::GatewayResultData as u16
    );
    assert_eq!(
        grouped_composite_min_numeric_v21.request_id,
        accepted.request_id
    );
    assert_eq!(
        grouped_composite_min_numeric_v21.payload,
        [
            b"SQLR".as_slice(),
            &[
                0, 1, 0, 3, 0, 8, b'l', b'e', b'f', b't', b'_', b'k', b'e', b'y', 0, 0, 0, 3, 0, 0,
                0, 0, 0, 0, 4, 0, 0, 0, 1, 0, 9, b'r', b'i', b'g', b'h', b't', b'_', b'k', b'e',
                b'y', 0, 0, 0, 3, 0, 0, 0, 0, 0, 0, 4, 0, 0, 0, 7, 0, 3, b'm', b'i', b'n', 0, 0, 0,
                5, 4, 2, 0, 0, 0, 0, 16, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 100,
            ][..],
        ]
        .concat()
    );
    let dispatched_composite_hidden_count = read_frame(&mut gateway)?
        .ok_or("missing dispatched composite COUNT/SUM HAVING SQLR result")?;
    assert_eq!(
        dispatched_composite_hidden_count.operation,
        Operation::GatewayResultData as u16
    );
    assert_eq!(
        dispatched_composite_hidden_count.request_id,
        accepted.request_id
    );
    assert_eq!(
        dispatched_composite_hidden_count.payload,
        [
            b"SQLR".as_slice(),
            &[
                0, 1, 0, 3, 0, 8, b'l', b'e', b'f', b't', b'_', b'k', b'e', b'y', 0, 0, 0, 3, 0, 0,
                0, 0, 0, 0, 4, 0, 0, 0, 2, 0, 9, b'r', b'i', b'g', b'h', b't', b'_', b'k', b'e',
                b'y', 0, 0, 0, 3, 0, 0, 0, 0, 0, 0, 4, 0, 0, 0, 9, 0, 3, b's', b'u', b'm', 0, 0, 0,
                5, 38, 2, 0, 0, 0, 0, 16, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 194,
            ][..],
        ]
        .concat()
    );
    let grouped_sum_first = read_frame(&mut gateway)?
        .ok_or("missing first optimizer-backed composite SUM SQLR result")?;
    let grouped_sum_second = read_frame(&mut gateway)?
        .ok_or("missing second optimizer-backed composite SUM SQLR result")?;
    let expected_grouped_sum_rows = [
        [
            b"SQLR".as_slice(),
            &[
                0, 1, 0, 3, 0, 8, b'l', b'e', b'f', b't', b'_', b'k', b'e', b'y', 0, 0, 0, 3, 0, 0,
                0, 0, 0, 0, 4, 0, 0, 0, 1, 0, 9, b'r', b'i', b'g', b'h', b't', b'_', b'k', b'e',
                b'y', 0, 0, 0, 3, 0, 0, 0, 0, 0, 0, 4, 0, 0, 0, 7, 0, 3, b's', b'u', b'm', 0, 0, 0,
                4, 0, 0, 0, 0, 0, 0, 8, 0, 0, 0, 0, 0, 0, 0, 2,
            ][..],
        ]
        .concat(),
        [
            b"SQLR".as_slice(),
            &[
                0, 1, 0, 3, 0, 8, b'l', b'e', b'f', b't', b'_', b'k', b'e', b'y', 0, 0, 0, 3, 0, 0,
                0, 0, 0, 0, 4, 0, 0, 0, 2, 0, 9, b'r', b'i', b'g', b'h', b't', b'_', b'k', b'e',
                b'y', 0, 0, 0, 3, 0, 0, 0, 0, 0, 0, 4, 0, 0, 0, 9, 0, 3, b's', b'u', b'm', 0, 0, 0,
                4, 0, 0, 0, 0, 0, 0, 8, 0, 0, 0, 0, 0, 0, 0, 6,
            ][..],
        ]
        .concat(),
    ];
    assert_eq!(
        grouped_sum_first.operation,
        Operation::GatewayResultData as u16
    );
    assert_eq!(grouped_sum_first.request_id, accepted.request_id);
    assert_eq!(
        grouped_sum_second.operation,
        Operation::GatewayResultData as u16
    );
    assert_eq!(grouped_sum_second.request_id, accepted.request_id);
    let actual_grouped_sum_rows = [grouped_sum_first.payload, grouped_sum_second.payload];
    assert!(
        expected_grouped_sum_rows
            .iter()
            .all(|expected| actual_grouped_sum_rows
                .iter()
                .any(|actual| actual.as_slice() == expected.as_slice())),
        "optimizer-backed composite SUM SQLR rows differ: {actual_grouped_sum_rows:?}"
    );
    let ordered_grouped_avg =
        read_frame(&mut gateway)?.ok_or("missing ordered grouped AVG SQLR result")?;
    assert_eq!(
        ordered_grouped_avg.operation,
        Operation::GatewayResultData as u16
    );
    assert_eq!(ordered_grouped_avg.request_id, accepted.request_id);
    assert_eq!(
        ordered_grouped_avg.payload,
        [
            b"SQLR".as_slice(),
            &[
                0, 1, 0, 2, 0, 9, b'r', b'i', b'g', b'h', b't', b'_', b'k', b'e', b'y', 0, 0, 0, 3,
                0, 0, 0, 0, 0, 0, 4, 0, 0, 0, 9, 0, 3, b'a', b'v', b'g', 0, 0, 0, 5, 38, 6, 0, 0,
                0, 0, 16, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 30, 132, 128,
            ][..],
        ]
        .concat()
    );
    let ordered_grouped_avg_second =
        read_frame(&mut gateway)?.ok_or("missing second ordered grouped AVG SQLR result")?;
    assert_eq!(
        ordered_grouped_avg_second.operation,
        Operation::GatewayResultData as u16
    );
    assert_eq!(ordered_grouped_avg_second.request_id, accepted.request_id);
    assert_eq!(
        ordered_grouped_avg_second.payload,
        [
            b"SQLR".as_slice(),
            &[
                0, 1, 0, 2, 0, 9, b'r', b'i', b'g', b'h', b't', b'_', b'k', b'e', b'y', 0, 0, 0, 3,
                0, 0, 0, 0, 0, 0, 4, 0, 0, 0, 7, 0, 3, b'a', b'v', b'g', 0, 0, 0, 5, 38, 6, 0, 0,
                0, 0, 16, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 15, 66, 64,
            ][..],
        ]
        .concat()
    );
    let grouped_order = read_frame(&mut gateway)?.ok_or("missing grouped ORDER BY SQLR result")?;
    assert_eq!(grouped_order.operation, Operation::GatewayResultData as u16);
    assert_eq!(grouped_order.request_id, accepted.request_id);
    assert_eq!(
        grouped_order.payload,
        [
            b"SQLR".as_slice(),
            &[
                0, 1, 0, 2, 0, 2, b'i', b'd', 0, 0, 0, 3, 0, 0, 0, 0, 0, 0, 4, 0, 0, 0, 42, 0, 5,
                b'c', b'o', b'u', b'n', b't', 0, 0, 0, 4, 0, 0, 0, 0, 0, 0, 8, 0, 0, 0, 0, 0, 0, 0,
                1,
            ][..]
        ]
        .concat()
    );
    let joined = read_frame(&mut gateway)?.ok_or("missing joined SQLR result")?;
    assert_eq!(joined.operation, Operation::GatewayResultData as u16);
    assert_eq!(joined.request_id, accepted.request_id);
    assert_eq!(
        joined.payload,
        [
            b"SQLR".as_slice(),
            &[
                0, 1, 0, 2, 0, 2, b'i', b'd', 0, 0, 0, 3, 0, 0, 0, 0, 0, 0, 4, 0, 0, 0, 42, 0, 2,
                b'i', b'd', 0, 0, 0, 3, 0, 0, 0, 0, 0, 0, 4, 0, 0, 0, 42,
            ][..]
        ]
        .concat()
    );
    for (expected_left, expected_right) in [(1u32, 7u32), (1u32, 7u32)] {
        let composite_joined =
            read_frame(&mut gateway)?.ok_or("missing composite joined SQLR result")?;
        assert_eq!(
            composite_joined.operation,
            Operation::GatewayResultData as u16
        );
        assert_eq!(composite_joined.request_id, accepted.request_id);
        let integer_field = |name: &[u8], value: u32| {
            [
                &(name.len() as u16).to_be_bytes()[..],
                name,
                &[0, 0, 0, 3],
                &[0, 0, 0],
                &[0, 0, 0, 4],
                &value.to_be_bytes(),
            ]
            .concat()
        };
        let expected_payload = [
            b"SQLR".as_slice(),
            &[0, 1, 0, 4],
            &integer_field(b"left_key", expected_left),
            &integer_field(b"right_key", expected_right),
            &integer_field(b"id", expected_left),
            &integer_field(b"match_key", expected_right),
        ]
        .concat();
        assert_eq!(composite_joined.payload, expected_payload);
    }
    for expected_id in [42u32, 1u32] {
        let integer_id_field = |value: u32| {
            [
                &[0, 2][..],
                b"id",
                &[0, 0, 0, 3],
                &[0, 0, 0],
                &[0, 0, 0, 4],
                &value.to_be_bytes(),
            ]
            .concat()
        };
        let expected_payload = [
            b"SQLR".as_slice(),
            &[0, 1, 0, 2],
            &integer_id_field(expected_id),
            &integer_id_field(expected_id),
        ]
        .concat();
        let self_joined = read_frame(&mut gateway)?.ok_or("missing self-join SQLR result")?;
        assert_eq!(self_joined.operation, Operation::GatewayResultData as u16);
        assert_eq!(self_joined.request_id, accepted.request_id);
        assert_eq!(self_joined.payload, expected_payload);
    }
    let filtered_join_integer_field = |name: &[u8], value: u32| {
        [
            &(name.len() as u16).to_be_bytes()[..],
            name,
            &[0, 0, 0, 3],
            &[0, 0, 0],
            &[0, 0, 0, 4],
            &value.to_be_bytes(),
        ]
        .concat()
    };
    let filtered_joined = read_frame(&mut gateway)?.ok_or("missing filtered join SQLR result")?;
    assert_eq!(
        filtered_joined.operation,
        Operation::GatewayResultData as u16
    );
    assert_eq!(filtered_joined.request_id, accepted.request_id);
    assert_eq!(
        filtered_joined.payload,
        [
            b"SQLR".as_slice(),
            &[0, 1, 0, 2],
            &filtered_join_integer_field(b"left_key", 1),
            &filtered_join_integer_field(b"id", 42),
        ]
        .concat()
    );
    let optimizer_filtered_joined =
        read_frame(&mut gateway)?.ok_or("missing optimizer filtered join SQLR result")?;
    assert_eq!(
        optimizer_filtered_joined.operation,
        Operation::GatewayResultData as u16
    );
    assert_eq!(optimizer_filtered_joined.request_id, accepted.request_id);
    assert_eq!(
        optimizer_filtered_joined.payload,
        [
            b"SQLR".as_slice(),
            &[0, 1, 0, 2],
            &filtered_join_integer_field(b"left_key", 1),
            &filtered_join_integer_field(b"id", 42),
        ]
        .concat()
    );
    for expected_right_key in [9u32, 9u32] {
        let ordered_select =
            read_frame(&mut gateway)?.ok_or("missing ordered SELECT SQLR result")?;
        assert_eq!(
            ordered_select.operation,
            Operation::GatewayResultData as u16
        );
        assert_eq!(ordered_select.request_id, accepted.request_id);
        assert_eq!(
            ordered_select.payload,
            [
                b"SQLR".as_slice(),
                &[
                    0, 1, 0, 1, 0, 9, b'r', b'i', b'g', b'h', b't', b'_', b'k', b'e', b'y', 0, 0,
                    0, 3, 0, 0, 0, 0, 0, 0, 4,
                ][..],
                &expected_right_key.to_be_bytes(),
            ]
            .concat()
        );
    }
    drop(gateway);
    let terminal_result = finalize_gateway_invocation_and_send_terminal(
        &mut invocation,
        &mut service,
        accepted.request_id,
        GatewayTerminalDecision::Commit,
    );
    assert!(
        matches!(
            terminal_result,
            Err(prdb_engine::supervisor_ipc::GatewayTerminalRelayError::Frame(_))
        ),
        "terminal relay did not preserve the committed outcome after gateway loss: {terminal_result:?}"
    );
    assert_eq!(
        storage
            .lookup_invocation_outcome(reservation.invocation_id, user_id, now + 1)?
            .ok_or("missing durable invocation outcome")?
            .status,
        OutcomeStatus::Committed
    );
    {
        fn expected_aggregate_row(
            aggregate_name: &[u8],
            aggregate_type: u32,
            precision: u8,
            scale: u8,
            left_key: i32,
            right_key: i32,
            aggregate_bytes: &[u8],
        ) -> Vec<u8> {
            fn append_field(
                output: &mut Vec<u8>,
                name: &[u8],
                type_code: u32,
                precision: u8,
                scale: u8,
                value: &[u8],
            ) {
                output.extend_from_slice(&(name.len() as u16).to_be_bytes());
                output.extend_from_slice(name);
                output.extend_from_slice(&type_code.to_be_bytes());
                output.extend_from_slice(&[precision, scale, 0]);
                output.extend_from_slice(&(value.len() as u32).to_be_bytes());
                output.extend_from_slice(value);
            }
            let mut output = b"SQLR".to_vec();
            output.extend_from_slice(&1u16.to_be_bytes());
            output.extend_from_slice(&3u16.to_be_bytes());
            append_field(&mut output, b"left_key", 3, 0, 0, &left_key.to_be_bytes());
            append_field(&mut output, b"right_key", 3, 0, 0, &right_key.to_be_bytes());
            append_field(
                &mut output,
                aggregate_name,
                aggregate_type,
                precision,
                scale,
                aggregate_bytes,
            );
            output
        }

        fn expected_null_aggregate_row(
            aggregate_name: &[u8],
            aggregate_type: u32,
            precision: u8,
            scale: u8,
            left_key: i32,
            right_key: i32,
        ) -> Vec<u8> {
            fn append_value_field(output: &mut Vec<u8>, name: &[u8], type_code: u32, value: &[u8]) {
                output.extend_from_slice(&(name.len() as u16).to_be_bytes());
                output.extend_from_slice(name);
                output.extend_from_slice(&type_code.to_be_bytes());
                output.extend_from_slice(&[0, 0, 0]);
                output.extend_from_slice(&(value.len() as u32).to_be_bytes());
                output.extend_from_slice(value);
            }
            let mut output = b"SQLR".to_vec();
            output.extend_from_slice(&1u16.to_be_bytes());
            output.extend_from_slice(&3u16.to_be_bytes());
            append_value_field(&mut output, b"left_key", 3, &left_key.to_be_bytes());
            append_value_field(&mut output, b"right_key", 3, &right_key.to_be_bytes());
            output.extend_from_slice(&(aggregate_name.len() as u16).to_be_bytes());
            output.extend_from_slice(aggregate_name);
            output.extend_from_slice(&aggregate_type.to_be_bytes());
            output.extend_from_slice(&[precision, scale, 1]);
            output.extend_from_slice(&0u32.to_be_bytes());
            output
        }

        let bigint_table_invocation = id(0x9a);
        let mut bigint_table_audit = identity_audit_event(0x9b, user_id, schema.schema_id);
        bigint_table_audit.correlation_id = bigint_table_invocation;
        let bigint_table = storage
            .create_table(CreateTableRequest {
                invocation_id: bigint_table_invocation,
                authenticated_actor_id: user_id,
                schema_id: schema.schema_id,
                name: "bigint_grouped_records".to_owned(),
                columns: vec![
                    TableColumnDefinition {
                        name: "left_key".to_owned(),
                        scalar_type: ScalarType::Integer,
                        nullable: false,
                        generated_uuid_identity: false,
                        default: None,
                    },
                    TableColumnDefinition {
                        name: "right_key".to_owned(),
                        scalar_type: ScalarType::Integer,
                        nullable: false,
                        generated_uuid_identity: false,
                        default: None,
                    },
                    TableColumnDefinition {
                        name: "measure".to_owned(),
                        scalar_type: ScalarType::BigInt,
                        nullable: true,
                        generated_uuid_identity: false,
                        default: None,
                    },
                    TableColumnDefinition {
                        name: "small_measure".to_owned(),
                        scalar_type: ScalarType::SmallInt,
                        nullable: true,
                        generated_uuid_identity: false,
                        default: None,
                    },
                    TableColumnDefinition {
                        name: "integer_measure".to_owned(),
                        scalar_type: ScalarType::Integer,
                        nullable: true,
                        generated_uuid_identity: false,
                        default: None,
                    },
                ],
                retention_deadline_us: now + 3_600_000_000,
                audit_event: bigint_table_audit,
            })
            .map_err(|error| format!("create confined BIGINT aggregate table: {error:?}"))?;
        let bigint_authority_invocation = id(0x9c);
        let mut bigint_authority_audit = identity_audit_event(0x9d, user_id, user_id);
        bigint_authority_audit.action = AuditAction::AuthorityChange;
        bigint_authority_audit.correlation_id = bigint_authority_invocation;
        let mut bigint_authority_writes = vec![
            dependency_write(user_id, bigint_table.table_id, 5),
            grant_write(
                user_id,
                bigint_table.table_id,
                2,
                1,
                bigint_table.table_id,
                user_id,
            ),
        ];
        bigint_authority_writes.extend(
            bigint_table
                .column_ids
                .iter()
                .map(|column_id| dependency_write(user_id, *column_id, 5)),
        );
        bigint_authority_writes.extend(bigint_table.column_ids.iter().map(|column_id| {
            grant_write(user_id, bigint_table.table_id, 2, 2, *column_id, user_id)
        }));
        let (old_user_record, old_token_record) = {
            let read = database.begin_read()?;
            let catalog = read.open_table(CATALOG)?;
            (
                catalog
                    .get(id_catalog_key(10, user_id).as_slice())?
                    .ok_or("sandbox SQL user record missing before BIGINT grants")?
                    .value()
                    .to_vec(),
                catalog
                    .get(id_catalog_key(16, token_id).as_slice())?
                    .ok_or("sandbox SQL token record missing before BIGINT grants")?
                    .value()
                    .to_vec(),
            )
        };
        let mut bumped_user = principal_write_with_name(user_id, 1, 3, "sandbox_sql_roundtrip");
        bumped_user.expected_value = Some(old_user_record);
        bigint_authority_writes.push(bumped_user);
        let mut refreshed_token = credential_verifier_write(
            token_id,
            user_id,
            &secret,
            now - 1_000_000,
            now + 60_000_000,
            None,
            3,
        );
        refreshed_token.expected_value = Some(old_token_record);
        bigint_authority_writes.push(refreshed_token);
        storage
            .commit_catalog_and_audit(
                bigint_authority_invocation,
                user_id,
                now + 3_600_000_000,
                &bigint_authority_writes,
                &[bigint_authority_audit],
            )
            .map_err(|error| format!("grant confined BIGINT aggregate table: {error:?}"))?;
        let bigint_policy_invocation = id(0x9e);
        let mut bigint_policy_audit = identity_audit_event(0x9f, user_id, bigint_table.table_id);
        bigint_policy_audit.correlation_id = bigint_policy_invocation;
        storage
            .create_policy(CreatePolicyRequest {
                invocation_id: bigint_policy_invocation,
                authenticated_actor_id: user_id,
                table_id: bigint_table.table_id,
                name: "sandbox_bigint_grouped_select_policy".to_owned(),
                kind: PolicyKind::Row,
                operation: prdb_engine::authorization::Operation::Select,
                effect: prdb_engine::authorization::Effect::Allow,
                recipients: vec![prdb_engine::authorization::Subject::Principal(user_id)],
                target_column_ids: Vec::new(),
                using_expression: Some(prdb_engine::authorization::Expr::Literal(Some(
                    ScalarValue::Boolean(true),
                ))),
                with_check_expression: None,
                retention_deadline_us: now + 3_600_000_000,
                audit_event: bigint_policy_audit,
            })
            .map_err(|error| format!("create confined BIGINT aggregate policy: {error:?}"))?;
        let hidden_row_policy_invocation = id(0xcc);
        let mut hidden_row_policy_audit =
            identity_audit_event(0xcd, user_id, bigint_table.table_id);
        hidden_row_policy_audit.correlation_id = hidden_row_policy_invocation;
        storage
            .create_policy(CreatePolicyRequest {
                invocation_id: hidden_row_policy_invocation,
                authenticated_actor_id: user_id,
                table_id: bigint_table.table_id,
                name: "sandbox_bigint_grouped_restrict_high_key_rows".to_owned(),
                kind: PolicyKind::Row,
                operation: prdb_engine::authorization::Operation::Select,
                effect: prdb_engine::authorization::Effect::Restrict,
                recipients: vec![prdb_engine::authorization::Subject::Principal(user_id)],
                target_column_ids: Vec::new(),
                using_expression: Some(prdb_engine::authorization::Expr::LessEqual(
                    Box::new(prdb_engine::authorization::Expr::Field(0)),
                    Box::new(prdb_engine::authorization::Expr::Literal(Some(
                        ScalarValue::Integer(3),
                    ))),
                )),
                with_check_expression: None,
                retention_deadline_us: now + 3_600_000_000,
                audit_event: hidden_row_policy_audit,
            })
            .map_err(|error| format!("create confined BIGINT row restriction: {error:?}"))?;
        for (column_index, column_id) in bigint_table.column_ids.iter().enumerate() {
            let invocation_byte = [0xf3, 0xf5, 0xf7, 0xaa, 0xb9][column_index];
            let audit_byte = [0xf2, 0xf3, 0xf5, 0xab, 0xba][column_index];
            let invocation_id = id(invocation_byte);
            let mut audit_event = identity_audit_event(audit_byte, user_id, *column_id);
            audit_event.correlation_id = invocation_id;
            storage
                .create_policy(CreatePolicyRequest {
                    invocation_id,
                    authenticated_actor_id: user_id,
                    table_id: bigint_table.table_id,
                    name: format!("sandbox_bigint_grouped_column_{column_index}_select_policy"),
                    kind: PolicyKind::Cell,
                    operation: prdb_engine::authorization::Operation::Select,
                    effect: prdb_engine::authorization::Effect::Allow,
                    recipients: vec![prdb_engine::authorization::Subject::Principal(user_id)],
                    target_column_ids: vec![*column_id],
                    using_expression: Some(prdb_engine::authorization::Expr::Literal(Some(
                        ScalarValue::Boolean(true),
                    ))),
                    with_check_expression: None,
                    retention_deadline_us: now + 3_600_000_000,
                    audit_event,
                })
                .map_err(|error| {
                    format!(
                        "create confined BIGINT cell policy for column {column_index}: {error:?}"
                    )
                })?;
        }
        let bigint_schema = [
            ColumnSchema {
                scalar_type: ScalarType::Integer,
                nullable: false,
            },
            ColumnSchema {
                scalar_type: ScalarType::Integer,
                nullable: false,
            },
            ColumnSchema {
                scalar_type: ScalarType::BigInt,
                nullable: true,
            },
            ColumnSchema {
                scalar_type: ScalarType::SmallInt,
                nullable: true,
            },
            ColumnSchema {
                scalar_type: ScalarType::Integer,
                nullable: true,
            },
        ];
        let bigint_rows = [
            (0x92, 1, 7, Some(2i64), Some(2i16), Some(2i32)),
            (0x93, 1, 7, None, Some(4), Some(3)),
            (0x94, 2, 9, Some(6i64), Some(6), Some(6)),
            (0x95, 2, 9, Some(9i64), Some(9), Some(9)),
            (0x96, 2, 9, Some(12i64), Some(12), Some(12)),
            (0x97, 3, 11, None, None, None),
            (0x98, 4, 13, Some(44i64), Some(44), Some(44)),
        ]
        .into_iter()
        .map(
            |(row_id, left, right, measure, small_measure, integer_measure)| {
                Ok(RowWrite {
                    key: table_row_key(bigint_table.table_id, id(row_id)),
                    expected_version: None,
                    payload: Some(encode_tuple(
                        &bigint_schema,
                        1,
                        &[
                            Some(ScalarValue::Integer(left)),
                            Some(ScalarValue::Integer(right)),
                            measure.map(ScalarValue::BigInt),
                            small_measure.map(ScalarValue::SmallInt),
                            integer_measure.map(ScalarValue::Integer),
                        ],
                    )?),
                })
            },
        )
        .collect::<Result<Vec<_>, Box<dyn std::error::Error>>>()?;
        commit_rows(
            &database,
            id(0x97),
            builtin_owner_id(),
            now + 3_600_000_000,
            &bigint_rows,
        )
        .map_err(|error| format!("seed confined BIGINT aggregate rows: {error:?}"))?;
        let mut aggregate_client = ClientConnectionSession::default();
        aggregate_client.accept_hello(ClientMode::Query, "sandbox-sql-aggregates")?;
        aggregate_client.authenticate(&storage, token_id, &secret)?;
        let aggregate_owner = aggregate_client.staging_owner()?;
        let aggregate_parameter = [b'A'];
        let aggregate_reservation = staging.prepare(
            aggregate_owner,
            PrepareSubmission {
                manifest_version: 1,
                manifest: manifest.clone(),
                artifact_bytes: u64::try_from(artifact.len())?,
                artifact_sha256: Sha256::digest(&artifact).into(),
                scope_version: 1,
                requested_scope: requested_scope.clone(),
                parameter_bytes: 1,
                parameter_sha256: Sha256::digest(aggregate_parameter).into(),
                deadline_ms: 30_000,
                workload_class: WorkloadClass::Analytical,
            },
        )?;
        let mut aggregate_offset = 0;
        for chunk in artifact.chunks(prdb_engine::supervisor::MAX_CHUNK_BYTES) {
            aggregate_offset = staging.upload_chunk(
                aggregate_owner,
                aggregate_reservation.invocation_id,
                ChunkKind::Artifact,
                aggregate_offset,
                chunk,
            )?;
        }
        staging.upload_chunk(
            aggregate_owner,
            aggregate_reservation.invocation_id,
            ChunkKind::Parameters,
            0,
            &aggregate_parameter,
        )?;
        staging.seal(aggregate_owner, aggregate_reservation.invocation_id)?;
        let aggregate_invocation_hex = aggregate_reservation
            .invocation_id
            .iter()
            .map(|byte| format!("{byte:02x}"))
            .collect::<String>();
        let (aggregate_accepted, mut aggregate_invocation, mut aggregate_executor) =
            aggregate_client.handle_start_request_with_executor(
                &storage,
                &staging,
                ClientFrame {
                    kind: 7,
                    request_id: 1,
                    response_sequence: 0,
                    payload: format!("{{\"invocation_id\":\"{aggregate_invocation_hex}\"}}")
                        .into_bytes(),
                },
                GatewayAdmissionConfig {
                    retention_period_us: 3_600_000_000,
                    max_rows: 128,
                    max_bytes: 1_048_576,
                },
                &runtime,
            )?;
        assert_eq!(aggregate_accepted.payload, b"{\"state\":\"accepted\"}");
        let (mut aggregate_service, mut aggregate_gateway) = UnixStream::pair()?;
        aggregate_gateway.set_read_timeout(Some(Duration::from_secs(10)))?;
        let aggregate_reader = std::thread::spawn(move || {
            let mut frames = Vec::new();
            while let Some(frame) = prdb_engine::ipc::read_frame(&mut aggregate_gateway)? {
                let terminal = frame.operation == Operation::GatewayTerminal as u16;
                frames.push(frame);
                if terminal {
                    break;
                }
            }
            Ok::<_, prdb_engine::ipc::FrameError>(frames)
        });
        let aggregate_decision = observe_executor_relay_and_finalize_gateway(
            &storage,
            &mut aggregate_invocation,
            &mut aggregate_executor,
            &mut aggregate_service,
            aggregate_accepted.request_id,
        )?;
        let aggregate_frames = aggregate_reader
            .join()
            .map_err(|_| "aggregate gateway reader panicked")??;
        if aggregate_decision != GatewayTerminalDecision::Commit {
            let relayed = aggregate_frames
                .iter()
                .map(|frame| {
                    (
                        frame.operation,
                        frame.payload.len(),
                        String::from_utf8_lossy(&frame.payload[..frame.payload.len().min(80)])
                            .into_owned(),
                    )
                })
                .collect::<Vec<_>>();
            return Err(format!(
                "confined aggregate matrix aborted: {aggregate_decision:?}; relayed {relayed:?}"
            )
            .into());
        }
        staging.finish_started(aggregate_owner, aggregate_reservation.invocation_id)?;
        let mut aggregate_frames = aggregate_frames.into_iter();
        for group in (0i32..64).rev() {
            let mut group_key = format!("{group:04}").into_bytes();
            group_key.extend(std::iter::repeat_n(b'a', 8_168));
            let mut expected = b"SQLR".to_vec();
            expected.extend_from_slice(&1u16.to_be_bytes());
            expected.extend_from_slice(&2u16.to_be_bytes());
            expected.extend_from_slice(&(b"group_key".len() as u16).to_be_bytes());
            expected.extend_from_slice(b"group_key");
            expected.extend_from_slice(&6u32.to_be_bytes());
            expected.extend_from_slice(&[0, 0, 0]);
            expected.extend_from_slice(&(group_key.len() as u32).to_be_bytes());
            expected.extend_from_slice(&group_key);
            expected.extend_from_slice(&(b"sum".len() as u16).to_be_bytes());
            expected.extend_from_slice(b"sum");
            expected.extend_from_slice(&4u32.to_be_bytes());
            expected.extend_from_slice(&[0, 0, 0]);
            expected.extend_from_slice(&8u32.to_be_bytes());
            expected.extend_from_slice(&(300i64 + 2 * i64::from(group)).to_be_bytes());
            let frame = aggregate_frames
                .next()
                .ok_or_else(|| format!("missing ordered analytical SUM group {group}"))?;
            assert_eq!(frame.operation, Operation::GatewayResultData as u16);
            assert_eq!(frame.request_id, aggregate_accepted.request_id);
            assert_eq!(
                frame.payload, expected,
                "ordered analytical SUM group {group}"
            );
        }
        for (aggregate_name, aggregate_type, precision, scale) in [
            (b"min".as_slice(), 3u32, 0u8, 0u8),
            (b"max".as_slice(), 3u32, 0u8, 0u8),
            (b"avg".as_slice(), 5u32, 38u8, 6u8),
        ] {
            for (left_key, right_key) in [(4i32, 9i32), (2i32, 9i32)] {
                let aggregate_bytes = if aggregate_type == 5 {
                    ((left_key as u128) * 1_000_000).to_be_bytes().to_vec()
                } else {
                    left_key.to_be_bytes().to_vec()
                };
                let frame = aggregate_frames.next().ok_or_else(|| {
                    format!(
                        "missing optimizer-backed composite {} SQLR row",
                        String::from_utf8_lossy(aggregate_name)
                    )
                })?;
                assert_eq!(frame.operation, Operation::GatewayResultData as u16);
                assert_eq!(frame.request_id, aggregate_accepted.request_id);
                assert_eq!(
                    frame.payload,
                    expected_aggregate_row(
                        aggregate_name,
                        aggregate_type,
                        precision,
                        scale,
                        left_key,
                        right_key,
                        &aggregate_bytes,
                    )
                );
            }
        }
        for (aggregate_name, precision, scale, values) in [
            (
                b"min".as_slice(),
                4,
                2,
                [
                    (1i32, 7i32, Some(100u128)),
                    (2, 9, Some(100)),
                    (3, 11, None),
                ],
            ),
            (
                b"max".as_slice(),
                4,
                2,
                [
                    (2i32, 9i32, Some(225u128)),
                    (1, 7, Some(100)),
                    (3, 11, None),
                ],
            ),
            (
                b"sum".as_slice(),
                38,
                2,
                [
                    (2i32, 9i32, Some(450u128)),
                    (1, 7, Some(100)),
                    (3, 11, None),
                ],
            ),
            (
                b"avg".as_slice(),
                38,
                8,
                [
                    (2i32, 9i32, Some(150_000_000u128)),
                    (1, 7, Some(100_000_000)),
                    (3, 11, None),
                ],
            ),
        ] {
            for (left_key, right_key, coefficient) in values {
                let frame = aggregate_frames.next().ok_or_else(|| {
                    format!(
                        "missing optimizer-backed composite Decimal {} SQLR row",
                        String::from_utf8_lossy(aggregate_name)
                    )
                })?;
                assert_eq!(frame.operation, Operation::GatewayResultData as u16);
                assert_eq!(frame.request_id, aggregate_accepted.request_id);
                let expected = coefficient.map_or_else(
                    || {
                        expected_null_aggregate_row(
                            aggregate_name,
                            5,
                            precision,
                            scale,
                            left_key,
                            right_key,
                        )
                    },
                    |coefficient| {
                        expected_aggregate_row(
                            aggregate_name,
                            5,
                            precision,
                            scale,
                            left_key,
                            right_key,
                            &coefficient.to_be_bytes(),
                        )
                    },
                );
                assert_eq!(frame.payload, expected);
            }
        }
        for (left_key, right_key, count) in [(2i32, 9i32, 3u64), (1, 7, 2), (3, 11, 1)] {
            let frame = aggregate_frames
                .next()
                .ok_or("missing optimizer-backed composite Decimal COUNT SQLR row")?;
            assert_eq!(frame.operation, Operation::GatewayResultData as u16);
            assert_eq!(frame.request_id, aggregate_accepted.request_id);
            assert_eq!(
                frame.payload,
                expected_aggregate_row(
                    b"count",
                    4,
                    0,
                    0,
                    left_key,
                    right_key,
                    &count.to_be_bytes(),
                )
            );
        }
        {
            let (left_key, right_key, coefficient) = (1i32, 7i32, 100u128);
            let frame = aggregate_frames
                .next()
                .ok_or("missing filtered composite Decimal SUM SQLR row")?;
            assert_eq!(frame.operation, Operation::GatewayResultData as u16);
            assert_eq!(frame.request_id, aggregate_accepted.request_id);
            assert_eq!(
                frame.payload,
                expected_aggregate_row(
                    b"sum",
                    5,
                    38,
                    2,
                    left_key,
                    right_key,
                    &coefficient.to_be_bytes(),
                )
            );
        }
        {
            let frame = aggregate_frames
                .next()
                .ok_or("missing all-NULL SUM HAVING SQLR row")?;
            assert_eq!(frame.operation, Operation::GatewayResultData as u16);
            assert_eq!(frame.request_id, aggregate_accepted.request_id);
            assert_eq!(
                frame.payload,
                expected_null_aggregate_row(b"sum", 5, 38, 2, 3, 11)
            );
        }
        {
            let frame = aggregate_frames
                .next()
                .ok_or("missing single-key SUM HAVING SQLR row")?;
            assert_eq!(frame.operation, Operation::GatewayResultData as u16);
            assert_eq!(frame.request_id, aggregate_accepted.request_id);
            let expected = [
                b"SQLR\x00\x01\x00\x02".as_slice(),
                b"\x00\x09right_key\x00\x00\x00\x03\x00\x00\x00\x00\x00\x00\x04",
                &9i32.to_be_bytes(),
                b"\x00\x03sum\x00\x00\x00\x05\x26\x02\x00\x00\x00\x00\x10",
                &350i128.to_be_bytes(),
            ]
            .concat();
            assert_eq!(frame.payload, expected);
        }
        {
            let mut group_key = b"0000".to_vec();
            group_key.extend(std::iter::repeat_n(b'a', 8_168));
            let mut expected = b"SQLR".to_vec();
            expected.extend_from_slice(&1u16.to_be_bytes());
            expected.extend_from_slice(&3u16.to_be_bytes());
            expected.extend_from_slice(&(b"group_key".len() as u16).to_be_bytes());
            expected.extend_from_slice(b"group_key");
            expected.extend_from_slice(&6u32.to_be_bytes());
            expected.extend_from_slice(&[0, 0, 0]);
            expected.extend_from_slice(&(group_key.len() as u32).to_be_bytes());
            expected.extend_from_slice(&group_key);
            expected.extend_from_slice(&(b"measure".len() as u16).to_be_bytes());
            expected.extend_from_slice(b"measure");
            expected.extend_from_slice(&3u32.to_be_bytes());
            expected.extend_from_slice(&[0, 0, 0]);
            expected.extend_from_slice(&4u32.to_be_bytes());
            expected.extend_from_slice(&100i32.to_be_bytes());
            expected.extend_from_slice(&(b"min".len() as u16).to_be_bytes());
            expected.extend_from_slice(b"min");
            expected.extend_from_slice(&6u32.to_be_bytes());
            expected.extend_from_slice(&[0, 0, 0]);
            expected.extend_from_slice(&(group_key.len() as u32).to_be_bytes());
            expected.extend_from_slice(&group_key);
            let frame = aggregate_frames
                .next()
                .ok_or("missing filtered optimizer-backed TEXT MIN SQLR row")?;
            assert_eq!(frame.operation, Operation::GatewayResultData as u16);
            assert_eq!(frame.request_id, aggregate_accepted.request_id);
            let mut actual = frame.payload;
            let aggregate_terminal = loop {
                let frame = aggregate_frames
                    .next()
                    .ok_or("missing confined aggregate matrix terminal")?;
                if frame.operation == Operation::GatewayResultData as u16 {
                    assert_eq!(frame.request_id, aggregate_accepted.request_id);
                    actual.extend_from_slice(&frame.payload);
                } else {
                    break frame;
                }
            };
            assert_eq!(
                actual.len(),
                expected.len(),
                "filtered TEXT MIN SQLR length"
            );
            assert_eq!(
                actual
                    .iter()
                    .zip(&expected)
                    .position(|(left, right)| left != right),
                None,
                "filtered TEXT MIN SQLR bytes"
            );
            assert_eq!(
                aggregate_terminal.operation,
                Operation::GatewayTerminal as u16
            );
            assert_eq!(aggregate_terminal.request_id, aggregate_accepted.request_id);
            assert!(
                String::from_utf8(aggregate_terminal.payload.clone())?
                    .contains("\"outcome\":\"committed\"")
            );
            assert_eq!(actual, expected, "filtered TEXT MIN SQLR bytes");
        }
        assert_eq!(
            storage
                .lookup_invocation_outcome(aggregate_reservation.invocation_id, user_id, now + 1,)?
                .ok_or("missing confined aggregate matrix outcome")?
                .status,
            OutcomeStatus::Committed
        );

        for (mode, client_name) in [
            (b'P', "sandbox-sql-grouped-parameter-scale"),
            (b'J', "sandbox-sql-join-parameter-scale"),
            (b'R', "sandbox-sql-redacted-aggregate"),
            (b'C', "sandbox-sql-redacted-count-column"),
            (b'N', "sandbox-sql-redacted-null-filter"),
            (b'H', "sandbox-sql-redacted-null-having"),
        ] {
            // Run the parameter mismatch against visible values first.
            if mode == b'R' {
                let redacted_policy_invocation = id(0xac);
                let mut redacted_policy_audit =
                    identity_audit_event(0xad, user_id, decimal_table.column_ids[2]);
                redacted_policy_audit.correlation_id = redacted_policy_invocation;
                storage
                    .create_policy(CreatePolicyRequest {
                        invocation_id: redacted_policy_invocation,
                        authenticated_actor_id: user_id,
                        table_id: decimal_table.table_id,
                        name: "sandbox_decimal_group_redacted_value_policy".to_owned(),
                        kind: PolicyKind::Cell,
                        operation: prdb_engine::authorization::Operation::Select,
                        effect: prdb_engine::authorization::Effect::Restrict,
                        recipients: vec![prdb_engine::authorization::Subject::Principal(user_id)],
                        target_column_ids: vec![decimal_table.column_ids[2]],
                        using_expression: Some(prdb_engine::authorization::Expr::Literal(Some(
                            ScalarValue::Boolean(false),
                        ))),
                        with_check_expression: None,
                        retention_deadline_us: now + 3_600_000_000,
                        audit_event: redacted_policy_audit,
                    })
                    .map_err(|error| format!("create confined redaction policy: {error:?}"))?;
            }
            let mut redacted_client = ClientConnectionSession::default();
            redacted_client.accept_hello(ClientMode::Query, client_name)?;
            redacted_client.authenticate(&storage, token_id, &secret)?;
            let redacted_owner = redacted_client.staging_owner()?;
            let redacted_parameter = [if mode == b'C' { b'Z' } else { mode }];
            let redacted_reservation = staging.prepare(
                redacted_owner,
                PrepareSubmission {
                    manifest_version: 1,
                    manifest: manifest.clone(),
                    artifact_bytes: u64::try_from(artifact.len())?,
                    artifact_sha256: Sha256::digest(&artifact).into(),
                    scope_version: 1,
                    requested_scope: requested_scope.clone(),
                    parameter_bytes: 1,
                    parameter_sha256: Sha256::digest(redacted_parameter).into(),
                    deadline_ms: 30_000,
                    workload_class: WorkloadClass::Analytical,
                },
            )?;
            let mut redacted_offset = 0;
            for chunk in artifact.chunks(prdb_engine::supervisor::MAX_CHUNK_BYTES) {
                redacted_offset = staging.upload_chunk(
                    redacted_owner,
                    redacted_reservation.invocation_id,
                    ChunkKind::Artifact,
                    redacted_offset,
                    chunk,
                )?;
            }
            staging.upload_chunk(
                redacted_owner,
                redacted_reservation.invocation_id,
                ChunkKind::Parameters,
                0,
                &redacted_parameter,
            )?;
            staging.seal(redacted_owner, redacted_reservation.invocation_id)?;
            let redacted_invocation_hex = redacted_reservation
                .invocation_id
                .iter()
                .map(|byte| format!("{byte:02x}"))
                .collect::<String>();
            let (redacted_accepted, mut redacted_invocation, mut redacted_executor) =
                redacted_client.handle_start_request_with_executor(
                    &storage,
                    &staging,
                    ClientFrame {
                        kind: 7,
                        request_id: 1,
                        response_sequence: 0,
                        payload: format!("{{\"invocation_id\":\"{redacted_invocation_hex}\"}}")
                            .into_bytes(),
                    },
                    GatewayAdmissionConfig {
                        retention_period_us: 3_600_000_000,
                        max_rows: 128,
                        max_bytes: 1_048_576,
                    },
                    &runtime,
                )?;
            assert_eq!(redacted_accepted.payload, b"{\"state\":\"accepted\"}");
            let (mut redacted_service, mut redacted_gateway) = UnixStream::pair()?;
            redacted_gateway.set_read_timeout(Some(Duration::from_secs(10)))?;
            let redacted_reader = std::thread::spawn(move || {
                let mut frames = Vec::new();
                while let Some(frame) = prdb_engine::ipc::read_frame(&mut redacted_gateway)? {
                    let terminal = frame.operation == Operation::GatewayTerminal as u16;
                    frames.push(frame);
                    if terminal {
                        break;
                    }
                }
                Ok::<_, prdb_engine::ipc::FrameError>(frames)
            });
            let redacted_decision = observe_executor_relay_and_finalize_gateway(
                &storage,
                &mut redacted_invocation,
                &mut redacted_executor,
                &mut redacted_service,
                redacted_accepted.request_id,
            )?;
            let redacted_frames = redacted_reader
                .join()
                .map_err(|_| "redacted aggregate gateway reader panicked")??;
            assert_eq!(
                redacted_decision,
                GatewayTerminalDecision::Abort(GatewayAbortReason::WorkerFailure),
                "{client_name}"
            );
            assert_eq!(
                redacted_frames.len(),
                1,
                "{client_name} relayed unexpected frames: {redacted_frames:?}"
            );
            let terminal = &redacted_frames[0];
            assert_eq!(terminal.operation, Operation::GatewayTerminal as u16);
            assert_eq!(terminal.request_id, redacted_accepted.request_id);
            assert!(String::from_utf8_lossy(&terminal.payload).contains("\"outcome\":\"aborted\""));
            assert_eq!(
                storage
                    .lookup_invocation_outcome(
                        redacted_reservation.invocation_id,
                        user_id,
                        now + 1
                    )?
                    .ok_or("missing confined redacted aggregate outcome")?
                    .status,
                OutcomeStatus::Aborted,
                "{client_name}"
            );
            staging.finish_started(redacted_owner, redacted_reservation.invocation_id)?;
        }

        let mut bigint_scope_records = vec![
            [vec![1, 1], schema.schema_id.to_vec()].concat(),
            [vec![2, 2], bigint_table.table_id.to_vec()].concat(),
            [vec![3, 2], identity_table.table_id.to_vec()].concat(),
        ];
        bigint_scope_records.extend(bigint_table.column_ids.iter().map(|column_id| {
            [
                vec![2, 3],
                bigint_table.table_id.to_vec(),
                column_id.to_vec(),
            ]
            .concat()
        }));
        bigint_scope_records.extend(identity_table.column_ids.iter().map(|column_id| {
            [
                vec![3, 3],
                identity_table.table_id.to_vec(),
                column_id.to_vec(),
            ]
            .concat()
        }));
        bigint_scope_records.sort();
        let bigint_scope_count = u16::try_from(bigint_scope_records.len())?;
        let mut bigint_requested_scope = b"PSC1".to_vec();
        bigint_requested_scope.extend_from_slice(&bigint_scope_count.to_be_bytes());
        for record in bigint_scope_records {
            bigint_requested_scope.extend_from_slice(&record);
        }
        let bigint_staging = StagingStore::new(
            StagingLimits::default(),
            validator_for_scope(bigint_requested_scope.clone()),
        )?;
        let mut bigint_client = ClientConnectionSession::default();
        bigint_client.accept_hello(ClientMode::Query, "sandbox-sql-bigint-aggregates")?;
        bigint_client.authenticate(&storage, token_id, &secret)?;
        let bigint_owner = bigint_client.staging_owner()?;
        let bigint_parameter = [b'B'];
        let bigint_reservation = bigint_staging.prepare(
            bigint_owner,
            PrepareSubmission {
                manifest_version: 1,
                manifest: manifest.clone(),
                artifact_bytes: u64::try_from(artifact.len())?,
                artifact_sha256: Sha256::digest(&artifact).into(),
                scope_version: 1,
                requested_scope: bigint_requested_scope,
                parameter_bytes: 1,
                parameter_sha256: Sha256::digest(bigint_parameter).into(),
                deadline_ms: 30_000,
                workload_class: WorkloadClass::Analytical,
            },
        )?;
        let mut bigint_offset = 0;
        for chunk in artifact.chunks(prdb_engine::supervisor::MAX_CHUNK_BYTES) {
            bigint_offset = bigint_staging.upload_chunk(
                bigint_owner,
                bigint_reservation.invocation_id,
                ChunkKind::Artifact,
                bigint_offset,
                chunk,
            )?;
        }
        bigint_staging.upload_chunk(
            bigint_owner,
            bigint_reservation.invocation_id,
            ChunkKind::Parameters,
            0,
            &bigint_parameter,
        )?;
        bigint_staging.seal(bigint_owner, bigint_reservation.invocation_id)?;
        let bigint_invocation_hex = bigint_reservation
            .invocation_id
            .iter()
            .map(|byte| format!("{byte:02x}"))
            .collect::<String>();
        let (bigint_accepted, mut bigint_invocation, mut bigint_executor) = bigint_client
            .handle_start_request_with_executor(
                &storage,
                &bigint_staging,
                ClientFrame {
                    kind: 7,
                    request_id: 1,
                    response_sequence: 0,
                    payload: format!("{{\"invocation_id\":\"{bigint_invocation_hex}\"}}")
                        .into_bytes(),
                },
                GatewayAdmissionConfig {
                    retention_period_us: 3_600_000_000,
                    max_rows: 16,
                    max_bytes: 65_536,
                },
                &runtime,
            )?;
        assert_eq!(bigint_accepted.payload, b"{\"state\":\"accepted\"}");
        let (mut bigint_service, mut bigint_gateway) = UnixStream::pair()?;
        bigint_gateway.set_read_timeout(Some(Duration::from_secs(10)))?;
        let bigint_reader = std::thread::spawn(move || {
            let mut frames = Vec::new();
            while let Some(frame) = prdb_engine::ipc::read_frame(&mut bigint_gateway)? {
                let terminal = frame.operation == Operation::GatewayTerminal as u16;
                frames.push(frame);
                if terminal {
                    break;
                }
            }
            Ok::<_, prdb_engine::ipc::FrameError>(frames)
        });
        let bigint_decision = observe_executor_relay_and_finalize_gateway(
            &storage,
            &mut bigint_invocation,
            &mut bigint_executor,
            &mut bigint_service,
            bigint_accepted.request_id,
        )?;
        let bigint_frames = bigint_reader
            .join()
            .map_err(|_| "BIGINT aggregate gateway reader panicked")??;
        if bigint_decision != GatewayTerminalDecision::Commit {
            let relayed = bigint_frames
                .iter()
                .map(|frame| String::from_utf8_lossy(&frame.payload).into_owned())
                .collect::<Vec<_>>();
            return Err(format!(
                "confined BIGINT aggregate invocation aborted: {bigint_decision:?}; relayed {relayed:?}"
            )
            .into());
        }
        bigint_staging.finish_started(bigint_owner, bigint_reservation.invocation_id)?;
        let mut bigint_frames = bigint_frames.into_iter();
        for (left_key, right_key, sum) in
            [(2i32, 9i32, Some(27i64)), (1, 7, Some(2)), (3, 11, None)]
        {
            let frame = bigint_frames
                .next()
                .ok_or("missing optimizer-backed composite BIGINT SUM row")?;
            assert_eq!(frame.operation, Operation::GatewayResultData as u16);
            assert_eq!(frame.request_id, bigint_accepted.request_id);
            assert_eq!(
                frame.payload,
                sum.map_or_else(
                    || expected_null_aggregate_row(b"sum", 4, 0, 0, left_key, right_key),
                    |sum| {
                        expected_aggregate_row(
                            b"sum",
                            4,
                            0,
                            0,
                            left_key,
                            right_key,
                            &sum.to_be_bytes(),
                        )
                    },
                )
            );
        }
        for (aggregate, values) in [
            (
                b"min".as_slice(),
                [(2i32, 9i32, Some(6i64)), (1, 7, Some(2)), (3, 11, None)],
            ),
            (
                b"max".as_slice(),
                [(2i32, 9i32, Some(12i64)), (1, 7, Some(2)), (3, 11, None)],
            ),
        ] {
            for (left_key, right_key, value) in values {
                let frame = bigint_frames
                    .next()
                    .ok_or("missing optimizer-backed composite BIGINT MIN/MAX row")?;
                assert_eq!(frame.operation, Operation::GatewayResultData as u16);
                assert_eq!(frame.request_id, bigint_accepted.request_id);
                assert_eq!(
                    frame.payload,
                    value.map_or_else(
                        || { expected_null_aggregate_row(aggregate, 4, 0, 0, left_key, right_key) },
                        |value| {
                            expected_aggregate_row(
                                aggregate,
                                4,
                                0,
                                0,
                                left_key,
                                right_key,
                                &value.to_be_bytes(),
                            )
                        },
                    )
                );
            }
        }
        for (left_key, right_key, average) in [
            (2i32, 9i32, Some(9_000_000u128)),
            (1, 7, Some(2_000_000)),
            (3, 11, None),
        ] {
            let frame = bigint_frames
                .next()
                .ok_or("missing optimizer-backed composite BIGINT AVG row")?;
            assert_eq!(frame.operation, Operation::GatewayResultData as u16);
            assert_eq!(frame.request_id, bigint_accepted.request_id);
            assert_eq!(
                frame.payload,
                average.map_or_else(
                    || expected_null_aggregate_row(b"avg", 5, 38, 6, left_key, right_key),
                    |average| {
                        expected_aggregate_row(
                            b"avg",
                            5,
                            38,
                            6,
                            left_key,
                            right_key,
                            &average.to_be_bytes(),
                        )
                    },
                )
            );
        }
        for (left_key, right_key, sum) in
            [(2i32, 9i32, Some(27i64)), (1, 7, Some(6)), (3, 11, None)]
        {
            let frame = bigint_frames
                .next()
                .ok_or("missing optimizer-backed composite SMALLINT SUM row")?;
            assert_eq!(frame.operation, Operation::GatewayResultData as u16);
            assert_eq!(frame.request_id, bigint_accepted.request_id);
            assert_eq!(
                frame.payload,
                sum.map_or_else(
                    || expected_null_aggregate_row(b"sum", 4, 0, 0, left_key, right_key),
                    |sum| {
                        expected_aggregate_row(
                            b"sum",
                            4,
                            0,
                            0,
                            left_key,
                            right_key,
                            &sum.to_be_bytes(),
                        )
                    },
                )
            );
        }
        for (aggregate, values) in [
            (
                b"min".as_slice(),
                [(2i32, 9i32, Some(6i16)), (1, 7, Some(2)), (3, 11, None)],
            ),
            (
                b"max".as_slice(),
                [(2i32, 9i32, Some(12i16)), (1, 7, Some(4)), (3, 11, None)],
            ),
        ] {
            for (left_key, right_key, value) in values {
                let frame = bigint_frames
                    .next()
                    .ok_or("missing optimizer-backed composite SMALLINT MIN/MAX row")?;
                assert_eq!(frame.operation, Operation::GatewayResultData as u16);
                assert_eq!(frame.request_id, bigint_accepted.request_id);
                assert_eq!(
                    frame.payload,
                    value.map_or_else(
                        || { expected_null_aggregate_row(aggregate, 2, 0, 0, left_key, right_key) },
                        |value| {
                            expected_aggregate_row(
                                aggregate,
                                2,
                                0,
                                0,
                                left_key,
                                right_key,
                                &value.to_be_bytes(),
                            )
                        },
                    )
                );
            }
        }
        for (left_key, right_key, average) in [
            (2i32, 9i32, Some(9_000_000u128)),
            (1, 7, Some(3_000_000)),
            (3, 11, None),
        ] {
            let frame = bigint_frames
                .next()
                .ok_or("missing optimizer-backed composite SMALLINT AVG row")?;
            assert_eq!(frame.operation, Operation::GatewayResultData as u16);
            assert_eq!(frame.request_id, bigint_accepted.request_id);
            assert_eq!(
                frame.payload,
                average.map_or_else(
                    || expected_null_aggregate_row(b"avg", 5, 38, 6, left_key, right_key),
                    |average| {
                        expected_aggregate_row(
                            b"avg",
                            5,
                            38,
                            6,
                            left_key,
                            right_key,
                            &average.to_be_bytes(),
                        )
                    },
                )
            );
        }
        for (aggregate_name, values) in [
            (
                b"min".as_slice(),
                [(2i32, 9i32, Some(6i32)), (1, 7, Some(2)), (3, 11, None)],
            ),
            (
                b"max".as_slice(),
                [(2i32, 9i32, Some(12i32)), (1, 7, Some(3)), (3, 11, None)],
            ),
        ] {
            for (left_key, right_key, value) in values {
                let frame = bigint_frames
                    .next()
                    .ok_or("missing optimizer-backed composite INTEGER MIN/MAX row")?;
                assert_eq!(frame.operation, Operation::GatewayResultData as u16);
                assert_eq!(frame.request_id, bigint_accepted.request_id);
                assert_eq!(
                    frame.payload,
                    value.map_or_else(
                        || expected_null_aggregate_row(
                            aggregate_name,
                            3,
                            0,
                            0,
                            left_key,
                            right_key
                        ),
                        |value| expected_aggregate_row(
                            aggregate_name,
                            3,
                            0,
                            0,
                            left_key,
                            right_key,
                            &value.to_be_bytes(),
                        ),
                    )
                );
            }
        }
        for (left_key, right_key, average) in [
            (2i32, 9i32, Some(9_000_000u128)),
            (1, 7, Some(2_500_000)),
            (3, 11, None),
        ] {
            let frame = bigint_frames
                .next()
                .ok_or("missing optimizer-backed composite INTEGER AVG row")?;
            assert_eq!(frame.operation, Operation::GatewayResultData as u16);
            assert_eq!(frame.request_id, bigint_accepted.request_id);
            assert_eq!(
                frame.payload,
                average.map_or_else(
                    || expected_null_aggregate_row(b"avg", 5, 38, 6, left_key, right_key),
                    |average| {
                        expected_aggregate_row(
                            b"avg",
                            5,
                            38,
                            6,
                            left_key,
                            right_key,
                            &average.to_be_bytes(),
                        )
                    },
                )
            );
        }
        for (left_key, right_key, count) in [(2i32, 9i32, 3u64), (1, 7, 2), (3, 11, 1)] {
            let frame = bigint_frames
                .next()
                .ok_or("missing policy-filtered BIGINT COUNT(*) row")?;
            assert_eq!(frame.operation, Operation::GatewayResultData as u16);
            assert_eq!(frame.request_id, bigint_accepted.request_id);
            assert_eq!(
                frame.payload,
                expected_aggregate_row(
                    b"count",
                    4,
                    0,
                    0,
                    left_key,
                    right_key,
                    &count.to_be_bytes(),
                )
            );
        }
        let bigint_terminal = bigint_frames
            .next()
            .ok_or("missing confined BIGINT aggregate terminal")?;
        assert_eq!(bigint_terminal.operation, Operation::GatewayTerminal as u16);
        assert_eq!(bigint_terminal.request_id, bigint_accepted.request_id);
        assert!(String::from_utf8(bigint_terminal.payload)?.contains("\"outcome\":\"committed\""));
        assert_eq!(
            storage
                .lookup_invocation_outcome(bigint_reservation.invocation_id, user_id, now + 1,)?
                .ok_or("missing confined BIGINT aggregate outcome")?
                .status,
            OutcomeStatus::Committed
        );
    }
    {
        let read = database.begin_read()?;
        let rows = read.open_table(ROWS)?;
        let lower = table_row_key(identity_table.table_id, [0; 16]);
        let upper = table_row_key(identity_table.table_id, [u8::MAX; 16]);
        let identity_columns = [
            ColumnSchema {
                scalar_type: ScalarType::Uuid,
                nullable: false,
            },
            ColumnSchema {
                scalar_type: ScalarType::Text,
                nullable: false,
            },
        ];
        let mut identity_rows = 0;
        for entry in rows.range(lower.as_slice()..=upper.as_slice())? {
            let (_, record) = entry?;
            let record = RowRecord::decode(record.value())?;
            let tuple = decode_tuple(&identity_columns, 1, &record.payload)?;
            assert!(matches!(
                tuple.values.as_slice(),
                [Some(ScalarValue::Uuid(value)), Some(ScalarValue::Text(label))]
                    if *value != [0; 16] && label == "generated-label"
            ));
            identity_rows += 1;
        }
        assert_eq!(identity_rows, 1, "SQL DEFAULT should commit one UUID row");
    }

    // Keep these expired fixtures early in key order so bounded pruning can
    // reach them ahead of the sustained operational outcome history.
    for seed in 0x10u8..0x18u8 {
        let mut expired_outcome = prdb_engine::transaction::InvocationTransaction::begin(
            &database,
            id(seed),
            user_id,
            now + 1,
            1,
            1,
        )
        .map_err(|error| format!("expired outcome fixture seed {seed}: {error:?}"))?;
        expired_outcome.abort()?;
    }

    let output_pressure_parameter = [b'O'];
    let output_pressure_reservation = staging.prepare(
        owner,
        PrepareSubmission {
            manifest_version: 1,
            manifest: manifest.clone(),
            artifact_bytes: u64::try_from(artifact.len())?,
            artifact_sha256: Sha256::digest(&artifact).into(),
            scope_version: 1,
            requested_scope: requested_scope.clone(),
            parameter_bytes: 1,
            parameter_sha256: Sha256::digest(output_pressure_parameter).into(),
            deadline_ms: 30_000,
            workload_class: WorkloadClass::Analytical,
        },
    )?;
    let mut output_artifact_offset = 0;
    for chunk in artifact.chunks(prdb_engine::supervisor::MAX_CHUNK_BYTES) {
        output_artifact_offset = staging
            .upload_chunk(
                owner,
                output_pressure_reservation.invocation_id,
                ChunkKind::Artifact,
                output_artifact_offset,
                chunk,
            )
            .map_err(|error| format!("output-pressure artifact upload: {error:?}"))?;
    }
    staging
        .upload_chunk(
            owner,
            output_pressure_reservation.invocation_id,
            ChunkKind::Parameters,
            0,
            &output_pressure_parameter,
        )
        .map_err(|error| format!("output-pressure parameter upload: {error:?}"))?;
    staging.seal(owner, output_pressure_reservation.invocation_id)?;
    let output_invocation_hex = output_pressure_reservation
        .invocation_id
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect::<String>();
    let (output_accepted, mut output_invocation, mut output_executor) = client
        .handle_start_request_with_executor(
            &storage,
            &staging,
            ClientFrame {
                kind: 7,
                request_id: 4,
                response_sequence: 0,
                payload: format!("{{\"invocation_id\":\"{output_invocation_hex}\"}}").into_bytes(),
            },
            GatewayAdmissionConfig {
                retention_period_us: 3_600_000_000,
                max_rows: 8,
                max_bytes: 4096,
            },
            &runtime,
        )?;
    let (mut output_service, mut output_gateway) = UnixStream::pair()?;
    output_gateway.set_read_timeout(Some(Duration::from_secs(10)))?;
    const OUTPUT_PRESSURE_CHUNKS: usize = 256;
    let output_pressure_started = std::time::Instant::now();
    let (
        output_decision,
        (output_chunks, output_bytes),
        output_operations,
        maintenance_result,
        output_relay_elapsed,
    ) = std::thread::scope(|scope| {
        let output_reader = scope.spawn(move || -> Result<(usize, usize), String> {
            let mut chunk_count = 0;
            let mut output_bytes = 0;
            loop {
                let frame = read_frame(&mut output_gateway)
                    .map_err(|error| format!("output-pressure gateway read: {error:?}"))?
                    .ok_or("output-pressure gateway closed before terminal")?;
                if frame.request_id != output_accepted.request_id {
                    return Err("output-pressure frame request ID changed".into());
                }
                if frame.operation == Operation::GatewayTerminal as u16 {
                    if !String::from_utf8(frame.payload)
                        .map_err(|error| format!("terminal payload was not UTF-8: {error}"))?
                        .contains("\"outcome\":\"committed\"")
                    {
                        return Err("output-pressure worker did not commit".into());
                    }
                    break;
                }
                if frame.operation != Operation::GatewayResultData as u16
                    || frame.payload.len() != 16 * 1024
                {
                    return Err(format!(
                        "unexpected output-pressure frame: operation={} payload={}",
                        frame.operation,
                        frame.payload.len()
                    ));
                }
                chunk_count += 1;
                output_bytes += frame.payload.len();
                std::thread::sleep(Duration::from_millis(2));
            }
            Ok((chunk_count, output_bytes))
        });
        let operations = scope.spawn(|| {
            run_operational_read_commit_samples(
                &storage,
                composite_table.table_id,
                5,
                token_id,
                secret,
                now + 3_600_000_000,
                0xe9,
            )
            .map_err(|error| error.to_string())
        });
        let maintenance = scope.spawn(|| {
            prdb_engine::outcome::prune_expired_outcomes(
                &database,
                now + 2,
                prdb_engine::outcome::MAX_OUTCOME_PRUNE_BATCH,
            )
            .map_err(|error| error.to_string())
        });
        let decision = observe_executor_relay_and_finalize_gateway(
            &storage,
            &mut output_invocation,
            &mut output_executor,
            &mut output_service,
            output_accepted.request_id,
        )?;
        let relay_elapsed = output_pressure_started.elapsed();
        let output_read = output_reader
            .join()
            .map_err(|_| std::io::Error::other("output reader thread panicked"))?
            .map_err(std::io::Error::other)?;
        let operation_samples = operations
            .join()
            .map_err(|_| std::io::Error::other("output-pressure operational thread panicked"))?
            .map_err(std::io::Error::other)?;
        let maintenance_result = maintenance
            .join()
            .map_err(|_| std::io::Error::other("outcome-maintenance thread panicked"))?
            .map_err(std::io::Error::other)?;
        Ok::<_, Box<dyn std::error::Error>>((
            decision,
            output_read,
            operation_samples,
            maintenance_result,
            relay_elapsed,
        ))
    })?;
    assert_eq!(output_decision, GatewayTerminalDecision::Commit);
    staging.finish_started(owner, output_pressure_reservation.invocation_id)?;
    assert_eq!(output_chunks, OUTPUT_PRESSURE_CHUNKS);
    assert_eq!(output_bytes, OUTPUT_PRESSURE_CHUNKS * 16 * 1024);
    assert_eq!(output_operations.0.len(), 64);
    assert_eq!(output_operations.1.len(), 64);
    assert_eq!(maintenance_result.removed, 8);
    assert!(
        output_relay_elapsed >= Duration::from_millis(200),
        "4 MiB result relay completed in {output_relay_elapsed:?} despite the slow gateway reader"
    );
    eprintln!(
        "slow-gateway output pressure: chunks={OUTPUT_PRESSURE_CHUNKS} bytes={output_bytes} operational reads/commits={} pruned_outcomes={} relay={} ms",
        output_operations.0.len(),
        maintenance_result.removed,
        output_relay_elapsed.as_millis(),
    );

    let cancelled_output_parameter = [b'O'];
    let cancelled_output_reservation = staging.prepare(
        owner,
        PrepareSubmission {
            manifest_version: 1,
            manifest: manifest.clone(),
            artifact_bytes: u64::try_from(artifact.len())?,
            artifact_sha256: Sha256::digest(&artifact).into(),
            parameter_bytes: 1,
            parameter_sha256: Sha256::digest(cancelled_output_parameter).into(),
            scope_version: 1,
            requested_scope: requested_scope.clone(),
            deadline_ms: 30_000,
            workload_class: WorkloadClass::Analytical,
        },
    )?;
    let mut cancelled_artifact_offset = 0;
    for chunk in artifact.chunks(prdb_engine::supervisor::MAX_CHUNK_BYTES) {
        cancelled_artifact_offset = staging
            .upload_chunk(
                owner,
                cancelled_output_reservation.invocation_id,
                ChunkKind::Artifact,
                cancelled_artifact_offset,
                chunk,
            )
            .map_err(|error| format!("cancelled-output artifact upload: {error:?}"))?;
    }
    staging.upload_chunk(
        owner,
        cancelled_output_reservation.invocation_id,
        ChunkKind::Parameters,
        0,
        &cancelled_output_parameter,
    )?;
    staging.seal(owner, cancelled_output_reservation.invocation_id)?;
    let cancelled_output_invocation_hex = cancelled_output_reservation
        .invocation_id
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect::<String>();
    let (cancelled_output_accepted, mut cancelled_output_invocation, mut cancelled_output_executor) =
        client.handle_start_request_with_executor(
            &storage,
            &staging,
            ClientFrame {
                kind: 7,
                request_id: 5,
                response_sequence: 0,
                payload: format!("{{\"invocation_id\":\"{cancelled_output_invocation_hex}\"}}")
                    .into_bytes(),
            },
            GatewayAdmissionConfig {
                retention_period_us: 3_600_000_000,
                max_rows: 8,
                max_bytes: 4096,
            },
            &runtime,
        )?;
    let (mut cancelled_output_service, mut cancelled_output_gateway) = UnixStream::pair()?;
    cancelled_output_gateway.set_read_timeout(Some(Duration::from_secs(2)))?;
    let (cancelled_output_decision, (cancelled_output_chunks, cancellation_to_terminal)) =
        std::thread::scope(|scope| {
            let output_reader = scope.spawn(move || -> Result<usize, String> {
                let mut chunk_count = 0;
                loop {
                    let frame = read_frame(&mut cancelled_output_gateway)
                        .map_err(|error| format!("cancelled-output gateway read: {error:?}"))?
                        .ok_or("cancelled-output gateway closed before terminal")?;
                    if frame.request_id != cancelled_output_accepted.request_id {
                        return Err("cancelled-output frame request ID changed".into());
                    }
                    if frame.operation == Operation::GatewayTerminal as u16 {
                        if !String::from_utf8(frame.payload)
                            .map_err(|error| format!("terminal payload was not UTF-8: {error}"))?
                            .contains("\"outcome\":\"aborted\"")
                        {
                            return Err(
                                "cancelled output did not end in an aborted terminal".into()
                            );
                        }
                        break;
                    }
                    if frame.operation != Operation::GatewayResultData as u16
                        || frame.payload.len() != 16 * 1024
                    {
                        return Err(format!(
                            "unexpected cancelled-output frame: operation={} payload={}",
                            frame.operation,
                            frame.payload.len()
                        ));
                    }
                    chunk_count += 1;
                    std::thread::sleep(Duration::from_millis(2));
                }
                Ok(chunk_count)
            });
            let cancellation = scope.spawn(|| -> Result<std::time::Instant, String> {
                std::thread::sleep(Duration::from_millis(30));
                match storage
                    .cancel_owned_invocation(
                        user_id,
                        cancelled_output_reservation.invocation_id,
                        now + 4,
                    )
                    .map_err(|error| format!("slow-relay cancellation: {error:?}"))?
                {
                    prdb_engine::storage_manager::OwnerCancellationResult::Accepted => {
                        Ok(std::time::Instant::now())
                    }
                    result => Err(format!("unexpected slow-relay cancellation: {result:?}")),
                }
            });
            let decision = observe_executor_relay_and_finalize_gateway(
                &storage,
                &mut cancelled_output_invocation,
                &mut cancelled_output_executor,
                &mut cancelled_output_service,
                cancelled_output_accepted.request_id,
            );
            if decision.is_err() {
                let _ = cancelled_output_service.shutdown(std::net::Shutdown::Both);
            }
            let output_chunks = output_reader
                .join()
                .map_err(|_| std::io::Error::other("cancelled-output reader thread panicked"))?
                .map_err(std::io::Error::other)?;
            let cancellation_requested_at = cancellation
                .join()
                .map_err(|_| std::io::Error::other("slow-relay cancellation thread panicked"))?
                .map_err(std::io::Error::other)?;
            let decision = decision?;
            Ok::<_, Box<dyn std::error::Error>>((
                decision,
                (output_chunks, cancellation_requested_at.elapsed()),
            ))
        })?;
    assert!(
        matches!(cancelled_output_decision, GatewayTerminalDecision::Abort(_)),
        "slow-relay cancellation produced {cancelled_output_decision:?}"
    );
    staging.finish_started(owner, cancelled_output_reservation.invocation_id)?;
    assert!(
        (1..=OUTPUT_PRESSURE_CHUNKS).contains(&cancelled_output_chunks),
        "unexpected number of chunks before cancellation terminal: {cancelled_output_chunks}"
    );
    assert!(
        cancellation_to_terminal < Duration::from_secs(2),
        "slow-relay cancellation took {cancellation_to_terminal:?} to reach a terminal outcome"
    );
    eprintln!(
        "slow-gateway cancellation: relayed_chunks={cancelled_output_chunks} cancellation_to_terminal={} ms",
        cancellation_to_terminal.as_millis(),
    );

    let timed_out_output_parameter = [b'O'];
    let timed_out_output_reservation = staging.prepare(
        owner,
        PrepareSubmission {
            manifest_version: 1,
            manifest: manifest.clone(),
            artifact_bytes: u64::try_from(artifact.len())?,
            artifact_sha256: Sha256::digest(&artifact).into(),
            parameter_bytes: 1,
            parameter_sha256: Sha256::digest(timed_out_output_parameter).into(),
            scope_version: 1,
            requested_scope: requested_scope.clone(),
            deadline_ms: 30_000,
            workload_class: WorkloadClass::Analytical,
        },
    )?;
    let mut timed_out_artifact_offset = 0;
    for chunk in artifact.chunks(prdb_engine::supervisor::MAX_CHUNK_BYTES) {
        timed_out_artifact_offset = staging.upload_chunk(
            owner,
            timed_out_output_reservation.invocation_id,
            ChunkKind::Artifact,
            timed_out_artifact_offset,
            chunk,
        )?;
    }
    staging.upload_chunk(
        owner,
        timed_out_output_reservation.invocation_id,
        ChunkKind::Parameters,
        0,
        &timed_out_output_parameter,
    )?;
    staging.seal(owner, timed_out_output_reservation.invocation_id)?;
    let timed_out_output_invocation_hex = timed_out_output_reservation
        .invocation_id
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect::<String>();
    let (timed_out_output_accepted, mut timed_out_output_invocation, mut timed_out_output_executor) =
        client.handle_start_request_with_executor(
            &storage,
            &staging,
            ClientFrame {
                kind: 7,
                request_id: 6,
                response_sequence: 0,
                payload: format!("{{\"invocation_id\":\"{timed_out_output_invocation_hex}\"}}")
                    .into_bytes(),
            },
            GatewayAdmissionConfig {
                retention_period_us: 3_600_000_000,
                max_rows: 8,
                max_bytes: 4096,
            },
            &runtime,
        )?;
    let listener = std::net::TcpListener::bind(("127.0.0.1", 0))?;
    let address = listener.local_addr()?;
    let (mut gateway_service, mut supervisor_service) = UnixStream::pair()?;
    let gateway = std::thread::spawn(move || {
        let (mut tcp_client, _) = listener.accept().unwrap();
        // SAFETY: the accepted stream owns a live socket descriptor and the option is an integer.
        let send_buffer_size: libc::c_int = 16 * 1024;
        let result = unsafe {
            libc::setsockopt(
                std::os::fd::AsRawFd::as_raw_fd(&tcp_client),
                libc::SOL_SOCKET,
                libc::SO_SNDBUF,
                (&send_buffer_size as *const libc::c_int).cast(),
                std::mem::size_of_val(&send_buffer_size) as libc::socklen_t,
            )
        };
        assert_eq!(result, 0);
        prdb_engine::gateway::serve_tcp_client_connection_with_timeouts(
            &mut tcp_client,
            &mut gateway_service,
            Duration::from_millis(150),
            Duration::from_secs(1),
        )
    });
    let mut stalled_client = std::net::TcpStream::connect(address)?;
    let start_request = ClientFrame {
        kind: 7,
        request_id: timed_out_output_accepted.request_id,
        response_sequence: 0,
        payload: format!("{{\"invocation_id\":\"{timed_out_output_invocation_hex}\"}}")
            .into_bytes(),
    }
    .encode()?;
    std::io::Write::write_all(&mut stalled_client, &start_request)?;
    let start = read_frame(&mut supervisor_service)?.ok_or("missing timed-output START")?;
    assert_eq!(start.operation, Operation::StartSubmission as u16);
    assert_eq!(start.request_id, timed_out_output_accepted.request_id);
    prdb_engine::ipc::write_frame(
        &mut supervisor_service,
        &prdb_engine::ipc::IpcFrame {
            operation: start.operation,
            request_id: start.request_id,
            payload: b"{\"state\":\"accepted\"}".to_vec(),
        },
    )?;
    let relay_result = observe_executor_relay_and_finalize_gateway(
        &storage,
        &mut timed_out_output_invocation,
        &mut timed_out_output_executor,
        &mut supervisor_service,
        timed_out_output_accepted.request_id,
    );
    assert!(
        relay_result.is_err(),
        "timed-out client result relay succeeded"
    );
    assert!(matches!(
        gateway.join().map_err(|_| "timed-out gateway panicked")?,
        Err(prdb_engine::gateway::GatewayConnectionError::Io(ref error))
            if matches!(error.kind(), std::io::ErrorKind::TimedOut | std::io::ErrorKind::WouldBlock)
    ));
    staging.finish_started(owner, timed_out_output_reservation.invocation_id)?;
    assert_eq!(
        storage
            .lookup_invocation_outcome(
                timed_out_output_reservation.invocation_id,
                user_id,
                now + 5,
            )?
            .ok_or("missing timed-output durable outcome")?
            .status,
        OutcomeStatus::Aborted,
        "a stalled TCP result writer must abort the real confined invocation"
    );
    drop(stalled_client);

    eprintln!(
        "cancellation alongside live analytics: request={} us; SDK poll relay={} us",
        cancellation_request_latency.as_micros(),
        cancellation_observed_latency.as_micros(),
    );

    let denied_update_row_key = table_row_key(joined_table.table_id, id(0xc5));
    let denied_update_row_before = row(&database, &denied_update_row_key)?
        .ok_or("linked record missing before denied UPDATE")?
        .payload;
    let failure_parameter = [b'F'];
    let mut failure_client = ClientConnectionSession::default();
    failure_client.accept_hello(ClientMode::Query, "sandbox-sql-update-failure")?;
    failure_client.authenticate(&storage, token_id, &secret)?;
    let failure_owner = failure_client.staging_owner()?;
    let failure_staging = StagingStore::new(
        StagingLimits::default(),
        validator_for_scope(requested_scope.clone()),
    )?;
    let failure_reservation = failure_staging
        .prepare(
            failure_owner,
            PrepareSubmission {
                manifest_version: 1,
                manifest: manifest.clone(),
                artifact_bytes: u64::try_from(artifact.len())?,
                artifact_sha256: Sha256::digest(&artifact).into(),
                scope_version: 1,
                requested_scope: requested_scope.clone(),
                parameter_bytes: 1,
                parameter_sha256: Sha256::digest(failure_parameter).into(),
                deadline_ms: 30_000,
                workload_class: WorkloadClass::Operational,
            },
        )
        .map_err(|error| format!("prepare denied UPDATE fixture: {error:?}"))?;
    let mut failure_artifact_offset = 0;
    for chunk in artifact.chunks(prdb_engine::supervisor::MAX_CHUNK_BYTES) {
        failure_artifact_offset = failure_staging
            .upload_chunk(
                failure_owner,
                failure_reservation.invocation_id,
                ChunkKind::Artifact,
                failure_artifact_offset,
                chunk,
            )
            .map_err(|error| format!("UPDATE failure fixture upload: {error:?}"))?;
    }
    failure_staging
        .upload_chunk(
            failure_owner,
            failure_reservation.invocation_id,
            ChunkKind::Parameters,
            0,
            &failure_parameter,
        )
        .map_err(|error| format!("UPDATE failure parameter upload: {error:?}"))?;
    failure_staging
        .seal(failure_owner, failure_reservation.invocation_id)
        .map_err(|error| format!("seal denied UPDATE fixture: {error:?}"))?;
    let failure_invocation_hex = failure_reservation
        .invocation_id
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect::<String>();
    let (failure_accepted, mut failure_invocation, mut failure_executor) = failure_client
        .handle_start_request_with_executor(
            &storage,
            &failure_staging,
            ClientFrame {
                kind: 7,
                request_id: 3,
                response_sequence: 0,
                payload: format!("{{\"invocation_id\":\"{failure_invocation_hex}\"}}").into_bytes(),
            },
            GatewayAdmissionConfig {
                retention_period_us: 3_600_000_000,
                max_rows: 8,
                max_bytes: 4096,
            },
            &runtime,
        )
        .map_err(|error| format!("start denied UPDATE fixture: {error:?}"))?;
    assert_eq!(failure_accepted.payload, b"{\"state\":\"accepted\"}");
    let (mut failure_service, mut failure_gateway) = UnixStream::pair()?;
    let failure_decision = observe_executor_relay_and_finalize_gateway(
        &storage,
        &mut failure_invocation,
        &mut failure_executor,
        &mut failure_service,
        failure_accepted.request_id,
    )
    .map_err(|error| format!("finalize denied UPDATE fixture: {error:?}"))?;
    let failure_ready =
        read_frame(&mut failure_gateway)?.ok_or("missing denied UPDATE ready event")?;
    assert_eq!(failure_ready.operation, Operation::GatewayResultData as u16);
    assert_eq!(failure_ready.payload, b"UPDATE_REJECTION_READY");
    let failure_stage =
        read_frame(&mut failure_gateway)?.ok_or("missing UPDATE stage-reached event")?;
    assert_eq!(failure_stage.operation, Operation::GatewayResultData as u16);
    assert_eq!(failure_stage.payload, b"UPDATE_STAGE_REACHED");
    let failure_returned =
        read_frame(&mut failure_gateway)?.ok_or("missing UPDATE stage-returned event")?;
    assert_eq!(
        failure_returned.operation,
        Operation::GatewayResultData as u16
    );
    assert_eq!(failure_returned.payload, b"UPDATE_STAGE_RETURNED");
    assert_eq!(
        failure_decision,
        GatewayTerminalDecision::Abort(GatewayAbortReason::QueryError)
    );
    let failure_terminal =
        read_frame(&mut failure_gateway)?.ok_or("missing UPDATE failure terminal event")?;
    assert_eq!(
        failure_terminal.operation,
        Operation::GatewayTerminal as u16
    );
    assert!(String::from_utf8(failure_terminal.payload)?.contains("\"outcome\":\"aborted\""));
    assert_eq!(
        storage
            .lookup_invocation_outcome(failure_reservation.invocation_id, user_id, now + 4)?
            .ok_or("missing denied UPDATE outcome")?
            .status,
        OutcomeStatus::Aborted
    );
    assert_eq!(
        row(&database, &denied_update_row_key)?
            .ok_or("linked record missing after denied UPDATE")?
            .payload,
        denied_update_row_before,
        "denied optimizer UPDATE changed a committed row"
    );
    drop(failure_gateway);

    let target_limit_row_keys =
        [id(0xd3), id(0xd4)].map(|row_id| table_row_key(composite_table.table_id, row_id));
    let target_limit_rows_before = target_limit_row_keys
        .iter()
        .map(|key| {
            row(&database, key)?
                .map(|record| record.payload)
                .ok_or_else(|| "grouped row missing before UPDATE target-limit test".into())
        })
        .collect::<Result<Vec<_>, Box<dyn std::error::Error>>>()?;
    let target_limit_parameter = [b'T'];
    let mut target_limit_client = ClientConnectionSession::default();
    target_limit_client.accept_hello(ClientMode::Query, "sandbox-sql-update-target-limit")?;
    target_limit_client.authenticate(&storage, token_id, &secret)?;
    let target_limit_owner = target_limit_client.staging_owner()?;
    let target_limit_staging = StagingStore::new(
        StagingLimits::default(),
        validator_for_scope(requested_scope.clone()),
    )?;
    let target_limit_reservation = target_limit_staging
        .prepare(
            target_limit_owner,
            PrepareSubmission {
                manifest_version: 1,
                manifest: manifest.clone(),
                artifact_bytes: u64::try_from(artifact.len())?,
                artifact_sha256: Sha256::digest(&artifact).into(),
                scope_version: 1,
                requested_scope: requested_scope.clone(),
                parameter_bytes: 1,
                parameter_sha256: Sha256::digest(target_limit_parameter).into(),
                deadline_ms: 30_000,
                workload_class: WorkloadClass::Operational,
            },
        )
        .map_err(|error| format!("prepare UPDATE target-limit fixture: {error:?}"))?;
    let mut target_limit_artifact_offset = 0;
    for chunk in artifact.chunks(prdb_engine::supervisor::MAX_CHUNK_BYTES) {
        target_limit_artifact_offset = target_limit_staging
            .upload_chunk(
                target_limit_owner,
                target_limit_reservation.invocation_id,
                ChunkKind::Artifact,
                target_limit_artifact_offset,
                chunk,
            )
            .map_err(|error| format!("UPDATE target-limit fixture upload: {error:?}"))?;
    }
    target_limit_staging
        .upload_chunk(
            target_limit_owner,
            target_limit_reservation.invocation_id,
            ChunkKind::Parameters,
            0,
            &target_limit_parameter,
        )
        .map_err(|error| format!("UPDATE target-limit parameter upload: {error:?}"))?;
    target_limit_staging
        .seal(target_limit_owner, target_limit_reservation.invocation_id)
        .map_err(|error| format!("seal UPDATE target-limit fixture: {error:?}"))?;
    let target_limit_invocation_hex = target_limit_reservation
        .invocation_id
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect::<String>();
    let (target_limit_accepted, mut target_limit_invocation, mut target_limit_executor) =
        target_limit_client
            .handle_start_request_with_executor(
                &storage,
                &target_limit_staging,
                ClientFrame {
                    kind: 7,
                    request_id: 4,
                    response_sequence: 0,
                    payload: format!("{{\"invocation_id\":\"{target_limit_invocation_hex}\"}}")
                        .into_bytes(),
                },
                GatewayAdmissionConfig {
                    retention_period_us: 3_600_000_000,
                    max_rows: 8,
                    max_bytes: 4096,
                },
                &runtime,
            )
            .map_err(|error| format!("start UPDATE target-limit fixture: {error:?}"))?;
    assert_eq!(target_limit_accepted.payload, b"{\"state\":\"accepted\"}");
    let (mut target_limit_service, mut target_limit_gateway) = UnixStream::pair()?;
    assert_eq!(
        observe_executor_relay_and_finalize_gateway(
            &storage,
            &mut target_limit_invocation,
            &mut target_limit_executor,
            &mut target_limit_service,
            target_limit_accepted.request_id,
        )
        .map_err(|error| format!("finalize UPDATE target-limit fixture: {error:?}"))?,
        GatewayTerminalDecision::Abort(GatewayAbortReason::WorkerFailure)
    );
    let target_limit_ready =
        read_frame(&mut target_limit_gateway)?.ok_or("missing UPDATE target-limit ready event")?;
    assert_eq!(
        target_limit_ready.operation,
        Operation::GatewayResultData as u16
    );
    assert_eq!(
        target_limit_ready.request_id,
        target_limit_accepted.request_id
    );
    assert_eq!(target_limit_ready.payload, b"UPDATE_TARGET_LIMIT_READY");
    let target_limit_terminal = read_frame(&mut target_limit_gateway)?
        .ok_or("missing UPDATE target-limit terminal event")?;
    assert_eq!(
        target_limit_terminal.operation,
        Operation::GatewayTerminal as u16
    );
    assert_eq!(
        target_limit_terminal.request_id,
        target_limit_accepted.request_id
    );
    assert!(String::from_utf8(target_limit_terminal.payload)?.contains("\"outcome\":\"aborted\""));
    assert_eq!(
        storage
            .lookup_invocation_outcome(target_limit_reservation.invocation_id, user_id, now + 5,)?
            .ok_or("missing UPDATE target-limit outcome")?
            .status,
        OutcomeStatus::Aborted
    );
    for (key, expected) in target_limit_row_keys.iter().zip(&target_limit_rows_before) {
        assert_eq!(
            row(&database, key)?
                .ok_or("grouped row missing after UPDATE target-limit test")?
                .payload,
            *expected,
            "capacity-exhausted optimizer UPDATE partially changed a committed row"
        );
    }
    drop(target_limit_gateway);

    let stage_failure_parameter = [b'G'];
    let mut stage_failure_client = ClientConnectionSession::default();
    stage_failure_client.accept_hello(ClientMode::Query, "sandbox-sql-update-stage-failure")?;
    stage_failure_client.authenticate(&storage, token_id, &secret)?;
    let stage_failure_owner = stage_failure_client.staging_owner()?;
    let stage_failure_staging = StagingStore::new(
        StagingLimits::default(),
        validator_for_scope(requested_scope.clone()),
    )?;
    let stage_failure_reservation = stage_failure_staging.prepare(
        stage_failure_owner,
        PrepareSubmission {
            manifest_version: 1,
            manifest: manifest.clone(),
            artifact_bytes: u64::try_from(artifact.len())?,
            artifact_sha256: Sha256::digest(&artifact).into(),
            scope_version: 1,
            requested_scope: requested_scope.clone(),
            parameter_bytes: 1,
            parameter_sha256: Sha256::digest(stage_failure_parameter).into(),
            deadline_ms: 30_000,
            workload_class: WorkloadClass::Operational,
        },
    )?;
    let mut stage_failure_artifact_offset = 0;
    for chunk in artifact.chunks(prdb_engine::supervisor::MAX_CHUNK_BYTES) {
        stage_failure_artifact_offset = stage_failure_staging.upload_chunk(
            stage_failure_owner,
            stage_failure_reservation.invocation_id,
            ChunkKind::Artifact,
            stage_failure_artifact_offset,
            chunk,
        )?;
    }
    stage_failure_staging.upload_chunk(
        stage_failure_owner,
        stage_failure_reservation.invocation_id,
        ChunkKind::Parameters,
        0,
        &stage_failure_parameter,
    )?;
    stage_failure_staging.seal(stage_failure_owner, stage_failure_reservation.invocation_id)?;
    let stage_failure_invocation_hex = stage_failure_reservation
        .invocation_id
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect::<String>();
    let (stage_failure_accepted, mut stage_failure_invocation, mut stage_failure_executor) =
        stage_failure_client.handle_start_request_with_executor(
            &storage,
            &stage_failure_staging,
            ClientFrame {
                kind: 7,
                request_id: 5,
                response_sequence: 0,
                payload: format!("{{\"invocation_id\":\"{stage_failure_invocation_hex}\"}}")
                    .into_bytes(),
            },
            GatewayAdmissionConfig {
                retention_period_us: 3_600_000_000,
                max_rows: 8,
                max_bytes: 4096,
            },
            &runtime,
        )?;
    assert_eq!(stage_failure_accepted.payload, b"{\"state\":\"accepted\"}");
    let (mut stage_failure_service, mut stage_failure_gateway) = UnixStream::pair()?;
    assert_eq!(
        observe_executor_relay_and_finalize_gateway(
            &storage,
            &mut stage_failure_invocation,
            &mut stage_failure_executor,
            &mut stage_failure_service,
            stage_failure_accepted.request_id,
        )?,
        GatewayTerminalDecision::Abort(GatewayAbortReason::WorkerFailure)
    );
    for (expected, label) in [
        (
            b"UPDATE_STAGE_FAILURE_READY".as_slice(),
            "stage-failure readiness",
        ),
        (
            b"UPDATE_STAGE_BATCH_ACCEPTED".as_slice(),
            "first accepted UPDATE batch",
        ),
        (
            b"UPDATE_STAGE_FAILURE_INJECTED".as_slice(),
            "injected later StageUpdates failure",
        ),
    ] {
        let event = read_frame(&mut stage_failure_gateway)?
            .ok_or_else(|| format!("missing UPDATE {label} event"))?;
        assert_eq!(event.operation, Operation::GatewayResultData as u16);
        assert_eq!(event.request_id, stage_failure_accepted.request_id);
        assert_eq!(event.payload, expected);
    }
    let stage_failure_terminal = read_frame(&mut stage_failure_gateway)?
        .ok_or("missing UPDATE stage-failure terminal event")?;
    assert_eq!(
        stage_failure_terminal.operation,
        Operation::GatewayTerminal as u16
    );
    assert_eq!(
        stage_failure_terminal.request_id,
        stage_failure_accepted.request_id
    );
    assert!(String::from_utf8(stage_failure_terminal.payload)?.contains("\"outcome\":\"aborted\""));
    assert_eq!(
        storage
            .lookup_invocation_outcome(stage_failure_reservation.invocation_id, user_id, now + 6)?
            .ok_or("missing UPDATE stage-failure outcome")?
            .status,
        OutcomeStatus::Aborted
    );
    for (key, expected) in target_limit_row_keys.iter().zip(&target_limit_rows_before) {
        assert_eq!(
            row(&database, key)?
                .ok_or("grouped row missing after UPDATE stage-failure test")?
                .payload,
            *expected,
            "StageUpdate failure partially changed a committed row"
        );
    }
    drop(stage_failure_gateway);

    let restricted_scope = [
        b"PSC1\0\x01".as_slice(),
        &[1, 1],
        empty_schema.schema_id.as_slice(),
    ]
    .concat();
    let mut restricted_client = ClientConnectionSession::default();
    restricted_client.accept_hello(ClientMode::Query, "sandbox-sql-restricted-metadata")?;
    restricted_client.authenticate(&storage, restricted_token_id, &restricted_secret)?;
    let restricted_owner = restricted_client.staging_owner()?;
    let restricted_staging = StagingStore::new(
        StagingLimits::default(),
        validator_for_scope(restricted_scope.clone()),
    )?;
    let restricted_reservation = restricted_staging.prepare(
        restricted_owner,
        PrepareSubmission {
            manifest_version: 1,
            manifest: manifest.clone(),
            artifact_bytes: u64::try_from(restricted_artifact.len())?,
            artifact_sha256: Sha256::digest(&restricted_artifact).into(),
            scope_version: 1,
            requested_scope: restricted_scope,
            parameter_bytes: 0,
            parameter_sha256: Sha256::digest([]).into(),
            deadline_ms: 30_000,
            workload_class: WorkloadClass::Operational,
        },
    )?;
    let mut restricted_artifact_offset = 0;
    for chunk in restricted_artifact.chunks(prdb_engine::supervisor::MAX_CHUNK_BYTES) {
        restricted_artifact_offset = restricted_staging
            .upload_chunk(
                restricted_owner,
                restricted_reservation.invocation_id,
                ChunkKind::Artifact,
                restricted_artifact_offset,
                chunk,
            )
            .map_err(|error| format!("restricted SQL artifact chunk upload: {error:?}"))?;
    }
    restricted_staging.seal(restricted_owner, restricted_reservation.invocation_id)?;
    let restricted_invocation_hex = restricted_reservation
        .invocation_id
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect::<String>();
    let (restricted_accepted, mut restricted_invocation, mut restricted_executor) =
        restricted_client.handle_start_request_with_executor(
            &storage,
            &restricted_staging,
            ClientFrame {
                kind: 7,
                request_id: 2,
                response_sequence: 0,
                payload: format!("{{\"invocation_id\":\"{restricted_invocation_hex}\"}}")
                    .into_bytes(),
            },
            GatewayAdmissionConfig {
                retention_period_us: 3_600_000_000,
                max_rows: 8,
                max_bytes: 4096,
            },
            &runtime,
        )?;
    assert_eq!(restricted_accepted.payload, b"{\"state\":\"accepted\"}");

    let restricted_schemata = restricted_invocation
        .scan_information_schema_schemata(128)?
        .rows;
    assert!(
        restricted_schemata
            .iter()
            .any(|row| row.schema_name == "information_schema")
    );
    assert!(
        restricted_schemata
            .iter()
            .any(|row| row.schema_name == "empty_roundtrip")
    );
    assert!(
        !restricted_schemata
            .iter()
            .any(|row| row.schema_name == "sql_roundtrip")
    );
    let mut restricted_tables = Vec::new();
    let mut restricted_table_cursor = None;
    loop {
        let page =
            restricted_invocation.scan_information_schema_tables(128, restricted_table_cursor)?;
        restricted_tables.extend(page.rows);
        restricted_table_cursor = page.continuation;
        if restricted_table_cursor.is_none() {
            break;
        }
    }
    let mut restricted_columns = Vec::new();
    let mut restricted_column_cursor = None;
    loop {
        let page =
            restricted_invocation.scan_information_schema_columns(128, restricted_column_cursor)?;
        restricted_columns.extend(page.rows);
        restricted_column_cursor = page.continuation;
        if restricted_column_cursor.is_none() {
            break;
        }
    }
    assert!(
        !restricted_tables
            .iter()
            .any(|row| row.table_schema == "sql_roundtrip")
    );
    assert!(
        !restricted_columns
            .iter()
            .any(|row| row.table_schema == "sql_roundtrip")
    );

    let (mut restricted_service, mut restricted_gateway) = UnixStream::pair()?;
    restricted_gateway.set_read_timeout(Some(Duration::from_secs(10)))?;
    let restricted_decision = observe_executor_relay_and_finalize_gateway(
        &storage,
        &mut restricted_invocation,
        &mut restricted_executor,
        &mut restricted_service,
        restricted_accepted.request_id,
    )?;
    if restricted_decision != GatewayTerminalDecision::Commit {
        let mut relayed = Vec::new();
        while let Some(frame) = read_frame(&mut restricted_gateway)? {
            let terminal = frame.operation == Operation::GatewayTerminal as u16;
            relayed.push((
                frame.operation,
                String::from_utf8_lossy(&frame.payload).into_owned(),
            ));
            if terminal {
                break;
            }
        }
        panic!(
            "restricted sandbox query aborted: {restricted_decision:?}; relayed frames: {relayed:?}"
        );
    }
    restricted_staging.finish_started(restricted_owner, restricted_reservation.invocation_id)?;
    let restricted_schema_result =
        read_frame(&mut restricted_gateway)?.ok_or("missing restricted SQL metadata result")?;
    assert_eq!(
        restricted_schema_result.operation,
        Operation::GatewayResultData as u16
    );
    assert_eq!(
        restricted_schema_result.request_id,
        restricted_accepted.request_id
    );
    assert_eq!(restricted_schema_result.payload, b"empty_roundtrip");
    let restricted_terminal = read_frame(&mut restricted_gateway)?
        .ok_or("missing restricted committed terminal event")?;
    assert_eq!(
        restricted_terminal.operation,
        Operation::GatewayTerminal as u16
    );
    assert_eq!(
        restricted_terminal.request_id,
        restricted_accepted.request_id
    );
    assert!(String::from_utf8(restricted_terminal.payload)?.contains("\"outcome\":\"committed\""));
    assert_eq!(
        storage
            .lookup_invocation_outcome(
                restricted_reservation.invocation_id,
                restricted_user_id,
                now + 1,
            )?
            .ok_or("missing restricted durable invocation outcome")?
            .status,
        OutcomeStatus::Committed
    );
    Ok(())
}
