#![cfg(all(unix, target_os = "linux", target_arch = "x86_64"))]

use prdb_engine::audit::{AuditAction, AuditEvent, AuditResult};
use prdb_engine::bootstrap::initialize;
use prdb_engine::client_credentials::{
    read_client_credentials, remove_client_credentials, write_client_credentials,
};
use prdb_engine::client_protocol::ClientCredentials;
use prdb_engine::client_submit::{
    AdminClient, AdminColumnDefinition, AdminCreatePolicyRequest, AdminObjectGrantTarget,
    AdminObjectPrivilege, AdminPolicyEffect, AdminPolicyKind, AdminPolicyOperation,
    AdminPolicyRecipient, AdminSystemPrivilege, CancellationResult, InvocationStatus,
    MetadataClient, SubmissionClient, TerminalOutcome,
};
use prdb_engine::storage_manager::{
    CreateTableRequest, ObjectGrantPrivilege, ObjectGrantTarget, SetObjectGrantRequest,
    TableColumnDefinition, TokenIssueRequest,
};
use prdb_engine::submission_manifest::{encode_manifest_v1, manifest_v1_from_elf};
use prdb_engine::supervisor::WorkloadClass;
use prdb_engine::tuple::ScalarType;
use sha2::{Digest, Sha256};
use std::io::{BufRead, BufReader};
use std::os::unix::fs::PermissionsExt;
use std::process::{Command, Stdio};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use super::helpers::*;

pub(super) fn authenticated_metadata_and_admin_lifecycle_cross_gateway_and_supervisor_processes()
-> Result<(), Box<dyn std::error::Error>> {
    let nonce = SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos();
    let root = std::env::temp_dir().join(format!(
        "prdb-metadata-process-{}-{nonce}",
        std::process::id()
    ));
    std::fs::create_dir(&root)?;
    std::fs::set_permissions(&root, std::fs::Permissions::from_mode(0o700))?;
    let _cleanup = DirectoryGuard(root.clone());

    let instance_dir = root.join("instance");
    let mut setup = initialize(&instance_dir, "metadata_process_admin", i64::MAX)?;
    let token_id = setup.token_id();
    let admin_user_id = setup.admin_user_id();
    let mut token_secret = setup.take_secret().ok_or("bootstrap secret missing")?;
    let committed_invocation_id = [0xa3; 16];
    let restricted_token;
    let mut restricted_secret;
    let owner_token;
    let mut owner_secret;
    let restricted_schema_id;
    let restricted_user_id;
    let restricted_table_id;
    let restricted_index_id;
    let sql_visible_schema_id;
    {
        let instance = setup.acknowledge()?;
        let storage = instance.storage_manager(Default::default())?;
        let now_us = i64::try_from(SystemTime::now().duration_since(UNIX_EPOCH)?.as_micros())?;
        let retention_deadline_us = now_us.saturating_add(3_600_000_000);

        let restricted_user = storage.create_user(
            [0xa6; 16],
            admin_user_id,
            "metadata_reader",
            retention_deadline_us,
            AuditEvent {
                event_id: [0xa7; 16],
                actor_id: Some(admin_user_id),
                correlation_id: [0xa6; 16],
                occurred_at_us: now_us,
                action: AuditAction::Ddl,
                result: AuditResult::Succeeded,
                target_ids: Vec::new(),
            },
        )?;
        restricted_user_id = restricted_user.user_id;
        let owner_user = storage.create_user(
            [0xac; 16],
            admin_user_id,
            "schema_owner",
            retention_deadline_us,
            AuditEvent {
                event_id: [0xad; 16],
                actor_id: Some(admin_user_id),
                correlation_id: [0xac; 16],
                occurred_at_us: now_us,
                action: AuditAction::Ddl,
                result: AuditResult::Succeeded,
                target_ids: Vec::new(),
            },
        )?;
        let restricted_schema = storage.create_schema(
            [0xa4; 16],
            admin_user_id,
            "admin_private",
            owner_user.user_id,
            retention_deadline_us,
            AuditEvent {
                event_id: [0xa5; 16],
                actor_id: Some(admin_user_id),
                correlation_id: [0xa4; 16],
                occurred_at_us: now_us,
                action: AuditAction::Ddl,
                result: AuditResult::Succeeded,
                target_ids: Vec::new(),
            },
        )?;
        restricted_schema_id = restricted_schema.schema_id;
        let sql_visible_schema = storage.create_schema(
            [0xb4; 16],
            admin_user_id,
            "empty_roundtrip",
            restricted_user.user_id,
            retention_deadline_us,
            AuditEvent {
                event_id: [0xb5; 16],
                actor_id: Some(admin_user_id),
                correlation_id: [0xb4; 16],
                occurred_at_us: now_us,
                action: AuditAction::Ddl,
                result: AuditResult::Succeeded,
                target_ids: Vec::new(),
            },
        )?;
        sql_visible_schema_id = sql_visible_schema.schema_id;
        storage.set_object_grant(SetObjectGrantRequest {
            invocation_id: [0xba; 16],
            authenticated_actor_id: admin_user_id,
            grantee_id: restricted_user.user_id,
            object_id: sql_visible_schema_id,
            privilege: ObjectGrantPrivilege::Usage,
            target: ObjectGrantTarget::Object,
            grant: true,
            retention_deadline_us,
            audit_event: AuditEvent {
                event_id: [0xbb; 16],
                actor_id: Some(admin_user_id),
                correlation_id: [0xba; 16],
                occurred_at_us: now_us,
                action: AuditAction::AuthorityChange,
                result: AuditResult::Succeeded,
                target_ids: vec![restricted_user.user_id, sql_visible_schema_id],
            },
        })?;
        let sql_hidden_schema = storage.create_schema(
            [0xb6; 16],
            admin_user_id,
            "sql_roundtrip",
            admin_user_id,
            retention_deadline_us,
            AuditEvent {
                event_id: [0xb7; 16],
                actor_id: Some(admin_user_id),
                correlation_id: [0xb6; 16],
                occurred_at_us: now_us,
                action: AuditAction::Ddl,
                result: AuditResult::Succeeded,
                target_ids: Vec::new(),
            },
        )?;
        storage.create_table(CreateTableRequest {
            invocation_id: [0xb8; 16],
            authenticated_actor_id: admin_user_id,
            schema_id: sql_hidden_schema.schema_id,
            name: "records".to_owned(),
            columns: vec![TableColumnDefinition {
                name: "id".to_owned(),
                scalar_type: ScalarType::Integer,
                nullable: true,
                generated_uuid_identity: false,
                default: None,
            }],
            retention_deadline_us,
            audit_event: AuditEvent {
                event_id: [0xb9; 16],
                actor_id: Some(admin_user_id),
                correlation_id: [0xb8; 16],
                occurred_at_us: now_us,
                action: AuditAction::Ddl,
                result: AuditResult::Succeeded,
                target_ids: Vec::new(),
            },
        })?;
        let private_table = storage.create_table(CreateTableRequest {
            invocation_id: [0xaa; 16],
            authenticated_actor_id: admin_user_id,
            schema_id: restricted_schema_id,
            name: "private_records".to_owned(),
            columns: vec![TableColumnDefinition {
                name: "payload".to_owned(),
                scalar_type: ScalarType::Text,
                nullable: false,
                generated_uuid_identity: false,
                default: None,
            }],
            retention_deadline_us,
            audit_event: AuditEvent {
                event_id: [0xab; 16],
                actor_id: Some(admin_user_id),
                correlation_id: [0xaa; 16],
                occurred_at_us: now_us,
                action: AuditAction::Ddl,
                result: AuditResult::Succeeded,
                target_ids: Vec::new(),
            },
        })?;
        restricted_table_id = private_table.table_id;
        let index_invocation = [0x8a; 16];
        let mut index_audit = AuditEvent {
            event_id: [0x8b; 16],
            actor_id: Some(admin_user_id),
            correlation_id: index_invocation,
            occurred_at_us: now_us,
            action: AuditAction::Ddl,
            result: AuditResult::Succeeded,
            target_ids: Vec::new(),
        };
        index_audit.target_ids = vec![private_table.table_id];
        restricted_index_id = storage
            .create_index(prdb_engine::storage_manager::CreateIndexRequest {
                invocation_id: index_invocation,
                authenticated_actor_id: admin_user_id,
                table_id: private_table.table_id,
                name: "private_payload_key".to_owned(),
                column_ids: vec![private_table.column_ids[0]],
                unique: false,
                retention_deadline_us,
                audit_event: index_audit,
            })?
            .index_id;
        let mut issued = storage.issue_token(TokenIssueRequest {
            invocation_id: [0xa8; 16],
            authenticated_actor_id: admin_user_id,
            user_id: restricted_user.user_id,
            label: "metadata reader",
            expires_at_us: now_us.saturating_add(1_800_000_000),
            retention_deadline_us,
            audit_event: AuditEvent {
                event_id: [0xa9; 16],
                actor_id: Some(admin_user_id),
                correlation_id: [0xa8; 16],
                occurred_at_us: now_us,
                action: AuditAction::TokenLifecycle,
                result: AuditResult::Succeeded,
                target_ids: vec![restricted_user.user_id],
            },
        })?;
        restricted_token = issued.token_id;
        restricted_secret = issued
            .take_secret()
            .ok_or("restricted metadata token secret missing")?;
        let mut owner_issued = storage.issue_token(TokenIssueRequest {
            invocation_id: [0xb0; 16],
            authenticated_actor_id: admin_user_id,
            user_id: owner_user.user_id,
            label: "schema owner",
            expires_at_us: now_us.saturating_add(1_800_000_000),
            retention_deadline_us,
            audit_event: AuditEvent {
                event_id: [0xb1; 16],
                actor_id: Some(admin_user_id),
                correlation_id: [0xb0; 16],
                occurred_at_us: now_us,
                action: AuditAction::TokenLifecycle,
                result: AuditResult::Succeeded,
                target_ids: vec![owner_user.user_id],
            },
        })?;
        owner_token = owner_issued.token_id;
        owner_secret = owner_issued
            .take_secret()
            .ok_or("schema owner token secret missing")?;
        let (mut admin_query, _) = storage.begin_token_authenticated_invocation(
            [0xb2; 16],
            token_id,
            &token_secret,
            retention_deadline_us,
            8,
            4096,
        )?;
        assert!(
            admin_query
                .list_catalog_authorized_schemas(128, None)?
                .schemas
                .iter()
                .all(|schema| schema.schema_id != restricted_schema_id)
        );
        admin_query.abort()?;

        let (mut owner_query, _) = storage.begin_token_authenticated_invocation(
            [0xb3; 16],
            owner_token,
            &owner_secret,
            retention_deadline_us,
            8,
            4096,
        )?;
        assert!(
            owner_query
                .list_catalog_authorized_schemas(128, None)?
                .schemas
                .iter()
                .all(|schema| schema.schema_id != restricted_schema_id)
        );
        owner_query.abort()?;

        let mut invocation = storage.admit_supervisor_invocation(
            committed_invocation_id,
            token_id,
            &token_secret,
            retention_deadline_us,
            8,
            4096,
        )?;
        let mut successful_worker = Command::new("true").spawn()?;
        invocation.observe_worker_exit(&mut successful_worker)?;

        let (mut terminal_writer, terminal_reader) = std::os::unix::net::UnixStream::pair()?;
        drop(terminal_reader);
        assert!(
            prdb_engine::supervisor_ipc::finalize_gateway_invocation_and_send_terminal(
                &mut invocation,
                &mut terminal_writer,
                1,
                prdb_engine::supervisor_ipc::GatewayTerminalDecision::Commit,
            )
            .is_err(),
            "closed terminal receiver must lose delivery after storage commits"
        );
        assert!(matches!(
            storage
                .get_invocation_status(
                    committed_invocation_id,
                    invocation.principal_id(),
                    now_us,
                )?
                .map(|snapshot| snapshot.status),
            Some(prdb_engine::storage_manager::InvocationStatus::Terminal(outcome))
                if outcome.status == prdb_engine::outcome::OutcomeStatus::Committed
        ));
    }
    install_test_sdk_repository(&instance_dir)?;
    let mut outcome_status_secret = token_secret;

    let network_sql_package = if let Some(artifact_path) =
        std::env::var_os("PRDB_TEST_RESTRICTED_METADATA_QUERY_ARTIFACT")
    {
        let artifact = std::fs::read(artifact_path)?;
        let manifest = manifest_v1_from_elf(
            &artifact,
            1,
            1,
            "x86_64-v1".to_owned(),
            "nixos-x86_64-glibc-2.42".to_owned(),
            "TinyCC restricted metadata process fixture".to_owned(),
        )
        .map_err(|error| format!("derive network SQL fixture manifest: {error:?}"))?;
        let dependencies = manifest.dependencies.clone();
        let manifest_bytes = encode_manifest_v1(&manifest)
            .map_err(|error| format!("encode network SQL fixture manifest: {error:?}"))?;
        let mut requested_scope = b"PSC1\0\x01\x01\x01".to_vec();
        requested_scope.extend_from_slice(&sql_visible_schema_id);
        let package = root.join("restricted-metadata-package");
        std::fs::create_dir(&package)?;
        std::fs::set_permissions(&package, std::fs::Permissions::from_mode(0o700))?;
        for (name, bytes) in [
            ("query.so", artifact.as_slice()),
            ("manifest.pmf1", manifest_bytes.as_slice()),
            ("requested_scope.psc1", requested_scope.as_slice()),
        ] {
            let path = package.join(name);
            std::fs::write(&path, bytes)?;
            std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o600))?;
        }
        let artifact_hash = Sha256::digest(&artifact)
            .iter()
            .map(|byte| format!("{byte:02x}"))
            .collect::<String>();
        let hash_path = package.join("artifact.sha256");
        std::fs::write(&hash_path, format!("{artifact_hash}\n"))?;
        std::fs::set_permissions(&hash_path, std::fs::Permissions::from_mode(0o600))?;
        Some((package, dependencies))
    } else {
        None
    };

    let cgroup_parent = if network_sql_package.is_some() {
        std::env::var_os("PRDB_TEST_CGROUP_PARENT")
            .map(std::path::PathBuf::from)
            .ok_or("PRDB_TEST_CGROUP_PARENT is required for network SQL sandbox testing")?
    } else {
        root.clone()
    };
    let runtime_paths =
        |name: &str| -> Result<Vec<std::path::PathBuf>, Box<dyn std::error::Error>> {
            let value = std::env::var_os(name).ok_or(format!("{name} is required"))?;
            if name == "PRDB_TEST_RUNTIME_READ_ONLY" && value.is_empty() {
                return Ok(Vec::new());
            }
            let paths = std::env::split_paths(&value).collect::<Vec<_>>();
            if paths.is_empty() || paths.iter().any(|path| !path.is_absolute()) {
                return Err(format!("{name} must contain absolute paths").into());
            }
            Ok(paths)
        };
    let runtime_read_execute = if network_sql_package.is_some() {
        runtime_paths("PRDB_TEST_RUNTIME_READ_EXECUTE")?
    } else {
        vec![std::path::PathBuf::from("/usr")]
    };
    let runtime_read_only = if network_sql_package.is_some() {
        runtime_paths("PRDB_TEST_RUNTIME_READ_ONLY")?
    } else {
        vec![std::path::PathBuf::from("/etc/ld.so.cache")]
    };

    let socket_path = root.join("supervisor.sock");
    let uid = unsafe { libc::geteuid() };
    let mut supervisor_command = Command::new(env!("CARGO_BIN_EXE_prdb-supervisor"));
    supervisor_command.args([
        "--instance-dir",
        instance_dir.to_str().ok_or("non-UTF-8 instance path")?,
        "--service-socket",
        socket_path.to_str().ok_or("non-UTF-8 socket path")?,
        "--gateway-uid",
        &uid.to_string(),
        "--cgroup-parent",
        cgroup_parent.to_str().ok_or("non-UTF-8 cgroup path")?,
        "--executor",
        env!("CARGO_BIN_EXE_prdb-executor"),
        "--optimizer",
        env!("CARGO_BIN_EXE_prdb-optimizer"),
        "--scratch-parent",
        root.to_str().ok_or("non-UTF-8 scratch path")?,
        "--cpu-baseline",
        "x86_64-v1",
        "--runtime-profile",
        "nixos-x86_64-glibc-2.42",
        "--retention-seconds",
        "3600",
        "--max-rows",
        "8",
        "--max-bytes",
        "4096",
        "--cpu-quota-us",
        "100000",
        "--cpu-period-us",
        "100000",
        "--memory-max-bytes",
        "67108864",
        "--max-processes",
        "16",
    ]);
    for path in &runtime_read_execute {
        supervisor_command.arg("--runtime-read-execute").arg(path);
    }
    for path in &runtime_read_only {
        supervisor_command.arg("--runtime-read-only").arg(path);
    }
    if let Some((_, dependencies)) = &network_sql_package {
        for dependency in dependencies {
            supervisor_command
                .arg("--allowed-dependency")
                .arg(dependency);
        }
    }
    let mut supervisor = ChildGuard(Some(
        supervisor_command
            .stdout(Stdio::null())
            .stderr(Stdio::inherit())
            .spawn()?,
    ));

    let socket_deadline = Instant::now() + Duration::from_secs(5);
    while !socket_path.exists() {
        if let Some(status) = supervisor.child().try_wait()? {
            return Err(format!("supervisor exited before binding: {status}").into());
        }
        if Instant::now() >= socket_deadline {
            return Err("supervisor did not bind its service socket".into());
        }
        std::thread::sleep(Duration::from_millis(10));
    }

    let mut gateway = ChildGuard(Some(
        Command::new(env!("CARGO_BIN_EXE_prdb-gateway"))
            .args([
                "--bind",
                "127.0.0.1:0",
                "--service-socket",
                socket_path.to_str().ok_or("non-UTF-8 socket path")?,
                "--service-uid",
                &uid.to_string(),
                "--connections-per-ip",
                "256",
            ])
            .stdout(Stdio::null())
            .stderr(Stdio::piped())
            .spawn()?,
    ));
    let stderr = gateway
        .child()
        .stderr
        .take()
        .ok_or("gateway stderr missing")?;
    let (address_sender, address_receiver) = std::sync::mpsc::channel();
    std::thread::spawn(move || {
        let mut line = String::new();
        let result = BufReader::new(stderr).read_line(&mut line).map(|_| line);
        let _ = address_sender.send(result);
    });
    let address_line = address_receiver.recv_timeout(Duration::from_secs(5))??;
    let address: std::net::SocketAddr = address_line
        .trim()
        .strip_prefix("prdb-gateway listening on ")
        .ok_or("gateway listen line had an unexpected prefix")?
        .parse()?;

    if let Some((package, _)) = network_sql_package.as_ref() {
        let mut query_secret = restricted_secret;
        let mut query =
            SubmissionClient::connect_authenticated(address, restricted_token, &mut query_secret)?;
        assert_eq!(query_secret, [0; 32]);
        let invocation_id =
            query.upload_and_seal(package, &[], 30_000, WorkloadClass::Operational)?;
        let receipt_path = root.join("restricted-metadata.receipt");
        let (results, terminal_outcome) = {
            let mut started = query.start(invocation_id, &receipt_path)?;
            let mut results = Vec::new();
            let mut terminal_outcome = None;
            while let Some(event) = started.next_event()? {
                match event {
                    prdb_engine::client_submit::ClientResultEvent::Data(bytes) => {
                        results.push(bytes);
                    }
                    prdb_engine::client_submit::ClientResultEvent::Terminal(terminal) => {
                        terminal_outcome = Some(terminal.outcome);
                        break;
                    }
                    prdb_engine::client_submit::ClientResultEvent::PackageTerminal { .. } => {
                        panic!("expected invocation terminal")
                    }
                }
            }
            (results, terminal_outcome)
        };
        assert_eq!(results, vec![b"empty_roundtrip".to_vec()]);
        assert_eq!(
            terminal_outcome,
            Some(prdb_engine::client_submit::TerminalOutcome::Committed)
        );
        assert_eq!(query.status(invocation_id)?, InvocationStatus::Committed);
        assert_eq!(
            query.cancel(invocation_id)?,
            CancellationResult::Terminal(TerminalOutcome::Committed)
        );

        let credentials_path = root.join("interactive-reader.cred");
        write_client_credentials(
            &credentials_path,
            &ClientCredentials {
                token_id: restricted_token,
                token_secret: restricted_secret,
            },
        )?;
        let parameters_path = root.join("interactive.parameters.json");
        std::fs::write(
            &parameters_path,
            r#"{"format":"PDB-SQL-PARAMETERS-1","values":[{"type":"text","value":"empty_roundtrip"}]}"#,
        )?;
        std::fs::set_permissions(&parameters_path, std::fs::Permissions::from_mode(0o600))?;
        let interactive_query_path = root.join("interactive-query.sql");
        std::fs::write(
            &interactive_query_path,
            "SELECT schema_name FROM schemata WHERE schema_name = $1 LIMIT 1",
        )?;
        std::fs::set_permissions(
            &interactive_query_path,
            std::fs::Permissions::from_mode(0o600),
        )?;
        let requested_scope_path = root.join("interactive.scope.psc1");
        let mut requested_scope = b"PSC1\0\x01\x01\x01".to_vec();
        requested_scope.extend_from_slice(&sql_visible_schema_id);
        std::fs::write(&requested_scope_path, requested_scope)?;
        std::fs::set_permissions(
            &requested_scope_path,
            std::fs::Permissions::from_mode(0o600),
        )?;
        let sql_address = address.to_string();
        let sdk_release_dir = std::env::var_os("PRDB_TEST_INSTALLED_SDK_RELEASE_DIR")
            .map(std::path::PathBuf::from)
            .ok_or("PRDB_TEST_INSTALLED_SDK_RELEASE_DIR is required for interactive SQL")?;
        let sql_sdk = sdk_release_dir.join("libprdb_sql.so.1");
        let include_dir = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("include");

        let interactive_admin_credentials_path = root.join("interactive-admin.cred");
        write_client_credentials(
            &interactive_admin_credentials_path,
            &ClientCredentials {
                token_id,
                token_secret,
            },
        )?;
        let interactive_sql_path = root.join("interactive-mixed.sql");
        std::fs::write(
            &interactive_sql_path,
            "CREATE SCHEMA interactive_client_probe;\n\
             RUNAS metadata_reader\n\
             SELECT schema_name FROM schemata WHERE schema_name = $1 LIMIT 1;\n\
             ENDRUNAS;\n\
             BEGIN;\n\
             RUNAS metadata_reader\n\
             SELECT schema_name FROM schemata WHERE schema_name = $1 LIMIT 1;\n\
             ENDRUNAS;\n\
             ROLLBACK;\n\
             DROP SCHEMA interactive_client_probe RESTRICT;\n",
        )?;
        std::fs::set_permissions(
            &interactive_sql_path,
            std::fs::Permissions::from_mode(0o600),
        )?;
        let interactive_receipt_dir = root.join("interactive-receipts");
        std::fs::create_dir(&interactive_receipt_dir)?;
        std::fs::set_permissions(
            &interactive_receipt_dir,
            std::fs::Permissions::from_mode(0o700),
        )?;
        let interactive = Command::new(env!("CARGO_BIN_EXE_prdb-client"))
            .args([
                "interactive-sql",
                "--address",
                &sql_address,
                "--credentials",
                interactive_admin_credentials_path
                    .to_str()
                    .ok_or("interactive admin credentials path is not UTF-8")?,
                "--default-user",
                "metadata_process_admin",
                "--credential-profile",
                &format!(
                    "metadata_reader={}",
                    credentials_path
                        .to_str()
                        .ok_or("interactive reader credentials path is not UTF-8")?
                ),
                "--sql-file",
                interactive_sql_path
                    .to_str()
                    .ok_or("interactive SQL path is not UTF-8")?,
                "--schema",
                "information_schema",
                "--scope",
                requested_scope_path
                    .to_str()
                    .ok_or("interactive scope path is not UTF-8")?,
                "--sdk-library",
                sql_sdk.to_str().ok_or("SQL SDK path is not UTF-8")?,
                "--include-dir",
                include_dir
                    .to_str()
                    .ok_or("SQL SDK include path is not UTF-8")?,
                "--sdk-release",
                "1",
                "--cpu-baseline",
                "x86_64-v1",
                "--runtime-profile",
                "nixos-x86_64-glibc-2.42",
                "--compiler-identity",
                "TinyCC interactive SQL process integration",
                "--parameters",
                parameters_path
                    .to_str()
                    .ok_or("interactive parameters path is not UTF-8")?,
            ])
            .current_dir(&interactive_receipt_dir)
            .output()?;
        assert!(
            interactive.status.success(),
            "interactive-sql process failed: {}",
            String::from_utf8_lossy(&interactive.stderr)
        );
        assert_eq!(
            String::from_utf8_lossy(&interactive.stdout)
                .matches("empty_roundtrip")
                .count(),
            2,
            "interactive-sql did not execute both RUNAS packages: stdout={:?}",
            String::from_utf8_lossy(&interactive.stdout)
        );
        let interactive_stderr = String::from_utf8_lossy(&interactive.stderr);
        assert!(interactive_stderr.contains("began transaction"));
        assert!(interactive_stderr.contains("rolled back transaction"));
        assert!(interactive_stderr.contains("results are provisional until this package commits"));
        assert!(
            interactive_receipt_dir.read_dir()?.next().is_none(),
            "interactive transaction receipts should not be persisted as ordinary invocations"
        );
        let mut schema_verify_credentials =
            read_client_credentials(&interactive_admin_credentials_path)?;
        let mut schema_verify = AdminClient::connect_authenticated(
            address,
            schema_verify_credentials.token_id,
            &mut schema_verify_credentials.token_secret,
        )?;
        assert!(
            !schema_verify
                .list_schemas_page(128, None)?
                .records
                .iter()
                .any(|schema| schema.name == "interactive_client_probe"),
            "interactive DDL did not drop its temporary schema"
        );

        let uncertain_commit_sql_path = root.join("interactive-uncertain-commit.sql");
        std::fs::write(&uncertain_commit_sql_path, "BEGIN;\nCOMMIT;\n")?;
        std::fs::set_permissions(
            &uncertain_commit_sql_path,
            std::fs::Permissions::from_mode(0o600),
        )?;
        let (commit_proxy_address, commit_proxy) = start_interactive_response_dropping_proxy(
            address,
            prdb_engine::client_protocol::CLIENT_V2_TX_COMMIT_KIND,
            1,
            false,
        )?;
        let uncertain_commit = Command::new(env!("CARGO_BIN_EXE_prdb-client"))
            .args([
                "interactive-sql",
                "--address",
                &commit_proxy_address.to_string(),
                "--credentials",
                interactive_admin_credentials_path
                    .to_str()
                    .ok_or("interactive admin credentials path is not UTF-8")?,
                "--default-user",
                "metadata_process_admin",
                "--credential-profile",
                &format!(
                    "metadata_reader={}",
                    credentials_path
                        .to_str()
                        .ok_or("interactive reader credentials path is not UTF-8")?
                ),
                "--sql-file",
                uncertain_commit_sql_path
                    .to_str()
                    .ok_or("uncertain COMMIT SQL path is not UTF-8")?,
                "--schema",
                "information_schema",
                "--scope",
                requested_scope_path
                    .to_str()
                    .ok_or("interactive scope path is not UTF-8")?,
                "--sdk-library",
                sql_sdk.to_str().ok_or("SQL SDK path is not UTF-8")?,
                "--include-dir",
                include_dir
                    .to_str()
                    .ok_or("SQL SDK include path is not UTF-8")?,
                "--sdk-release",
                "1",
                "--cpu-baseline",
                "x86_64-v1",
                "--runtime-profile",
                "nixos-x86_64-glibc-2.42",
                "--compiler-identity",
                "TinyCC interactive uncertain COMMIT fixture",
                "--parameters",
                parameters_path
                    .to_str()
                    .ok_or("interactive parameters path is not UTF-8")?,
            ])
            .output()?;
        let commit_proxy_result = commit_proxy.join().unwrap();
        assert!(
            uncertain_commit.status.success(),
            "interactive-sql did not reconcile a lost COMMIT reply: {}; proxy={commit_proxy_result:?}",
            String::from_utf8_lossy(&uncertain_commit.stderr),
        );
        assert_interactive_response_drop_stats(commit_proxy_result?, 1, 1);
        assert!(uncertain_commit.stdout.is_empty());

        let (start_proxy_address, start_proxy) =
            start_interactive_response_dropping_proxy(address, 7, 1, false)?;
        let uncertain_start = Command::new(env!("CARGO_BIN_EXE_prdb-client"))
            .args([
                "interactive-sql",
                "--address",
                &start_proxy_address.to_string(),
                "--credentials",
                credentials_path
                    .to_str()
                    .ok_or("interactive reader credentials path is not UTF-8")?,
                "--default-user",
                "metadata_reader",
                "--sql-file",
                interactive_query_path
                    .to_str()
                    .ok_or("interactive query path is not UTF-8")?,
                "--schema",
                "information_schema",
                "--scope",
                requested_scope_path
                    .to_str()
                    .ok_or("interactive scope path is not UTF-8")?,
                "--sdk-library",
                sql_sdk.to_str().ok_or("SQL SDK path is not UTF-8")?,
                "--include-dir",
                include_dir
                    .to_str()
                    .ok_or("SQL SDK include path is not UTF-8")?,
                "--sdk-release",
                "1",
                "--cpu-baseline",
                "x86_64-v1",
                "--runtime-profile",
                "nixos-x86_64-glibc-2.42",
                "--compiler-identity",
                "TinyCC interactive uncertain START fixture",
                "--parameters",
                parameters_path
                    .to_str()
                    .ok_or("interactive parameters path is not UTF-8")?,
            ])
            .output()?;
        let start_proxy_result = start_proxy.join().unwrap();
        assert!(
            !uncertain_start.status.success(),
            "interactive-sql should report the lost package result instead of replaying it"
        );
        let uncertain_start_stderr = String::from_utf8_lossy(&uncertain_start.stderr);
        assert!(
            uncertain_start_stderr.contains("uncertain START reply"),
            "interactive-sql failed without reconciling the lost START reply: {uncertain_start_stderr}"
        );
        assert_interactive_response_drop_stats(start_proxy_result?, 0, 1);

        let uncertain_package_sql_path = root.join("interactive-uncertain-package.sql");
        std::fs::write(
            &uncertain_package_sql_path,
            "BEGIN;\n\
             RUNAS metadata_reader\n\
             SELECT schema_name FROM schemata WHERE schema_name = $1 LIMIT 1;\n\
             ENDRUNAS;\n\
             COMMIT;\n",
        )?;
        std::fs::set_permissions(
            &uncertain_package_sql_path,
            std::fs::Permissions::from_mode(0o600),
        )?;
        let (package_proxy_address, package_proxy) =
            start_interactive_response_dropping_proxy(address, 7, 1, true)?;
        let uncertain_package = Command::new(env!("CARGO_BIN_EXE_prdb-client"))
            .args([
                "interactive-sql",
                "--address",
                &package_proxy_address.to_string(),
                "--credentials",
                interactive_admin_credentials_path
                    .to_str()
                    .ok_or("interactive admin credentials path is not UTF-8")?,
                "--default-user",
                "metadata_process_admin",
                "--credential-profile",
                &format!(
                    "metadata_reader={}",
                    credentials_path
                        .to_str()
                        .ok_or("interactive reader credentials path is not UTF-8")?
                ),
                "--sql-file",
                uncertain_package_sql_path
                    .to_str()
                    .ok_or("uncertain package SQL path is not UTF-8")?,
                "--schema",
                "information_schema",
                "--scope",
                requested_scope_path
                    .to_str()
                    .ok_or("interactive scope path is not UTF-8")?,
                "--sdk-library",
                sql_sdk.to_str().ok_or("SQL SDK path is not UTF-8")?,
                "--include-dir",
                include_dir
                    .to_str()
                    .ok_or("SQL SDK include path is not UTF-8")?,
                "--sdk-release",
                "1",
                "--cpu-baseline",
                "x86_64-v1",
                "--runtime-profile",
                "nixos-x86_64-glibc-2.42",
                "--compiler-identity",
                "TinyCC interactive uncertain transaction package fixture",
                "--parameters",
                parameters_path
                    .to_str()
                    .ok_or("interactive parameters path is not UTF-8")?,
            ])
            .output()?;
        let package_proxy_result = package_proxy.join().unwrap();
        assert!(
            !uncertain_package.status.success(),
            "interactive-sql should report a completed package with a lost result"
        );
        assert!(
            String::from_utf8_lossy(&uncertain_package.stderr)
                .contains("completed but its result was lost"),
            "interactive-sql did not reconcile the lost package reply: {}",
            String::from_utf8_lossy(&uncertain_package.stderr)
        );
        assert_interactive_response_drop_stats(package_proxy_result?, 1, 1);
        assert!(uncertain_package.stdout.is_empty());

        let begin_uncertain_sql_path = root.join("interactive-uncertain-begin.sql");
        std::fs::write(&begin_uncertain_sql_path, "BEGIN;\nROLLBACK;\n")?;
        std::fs::set_permissions(
            &begin_uncertain_sql_path,
            std::fs::Permissions::from_mode(0o600),
        )?;
        let (begin_proxy_address, begin_proxy) = start_interactive_response_dropping_proxy(
            address,
            prdb_engine::client_protocol::CLIENT_V2_TX_BEGIN_KIND,
            1,
            false,
        )?;
        let uncertain_begin = Command::new(env!("CARGO_BIN_EXE_prdb-client"))
            .args([
                "interactive-sql",
                "--address",
                &begin_proxy_address.to_string(),
                "--credentials",
                interactive_admin_credentials_path
                    .to_str()
                    .ok_or("interactive admin credentials path is not UTF-8")?,
                "--default-user",
                "metadata_process_admin",
                "--sql-file",
                begin_uncertain_sql_path
                    .to_str()
                    .ok_or("uncertain BEGIN SQL path is not UTF-8")?,
                "--schema",
                "information_schema",
                "--scope",
                requested_scope_path
                    .to_str()
                    .ok_or("interactive scope path is not UTF-8")?,
                "--sdk-library",
                sql_sdk.to_str().ok_or("SQL SDK path is not UTF-8")?,
                "--include-dir",
                include_dir
                    .to_str()
                    .ok_or("SQL SDK include path is not UTF-8")?,
                "--sdk-release",
                "1",
                "--cpu-baseline",
                "x86_64-v1",
                "--runtime-profile",
                "nixos-x86_64-glibc-2.42",
                "--compiler-identity",
                "TinyCC interactive uncertain BEGIN fixture",
            ])
            .output()?;
        let begin_proxy_result = begin_proxy.join().unwrap();
        assert!(
            uncertain_begin.status.success(),
            "interactive-sql did not retry an idempotent BEGIN after a lost reply: {}; proxy={begin_proxy_result:?}",
            String::from_utf8_lossy(&uncertain_begin.stderr)
        );
        assert!(
            String::from_utf8_lossy(&uncertain_begin.stderr).contains("rolled back transaction")
        );
        assert_eq!(begin_proxy_result?, (1, 0, 2, 2));

        let rollback_uncertain_sql_path = root.join("interactive-uncertain-rollback.sql");
        std::fs::write(&rollback_uncertain_sql_path, "BEGIN;\nROLLBACK;\n")?;
        std::fs::set_permissions(
            &rollback_uncertain_sql_path,
            std::fs::Permissions::from_mode(0o600),
        )?;
        let (rollback_proxy_address, rollback_proxy) = start_interactive_response_dropping_proxy(
            address,
            prdb_engine::client_protocol::CLIENT_V2_TX_ROLLBACK_KIND,
            1,
            false,
        )?;
        let uncertain_rollback = Command::new(env!("CARGO_BIN_EXE_prdb-client"))
            .args([
                "interactive-sql",
                "--address",
                &rollback_proxy_address.to_string(),
                "--credentials",
                interactive_admin_credentials_path
                    .to_str()
                    .ok_or("interactive admin credentials path is not UTF-8")?,
                "--default-user",
                "metadata_process_admin",
                "--sql-file",
                rollback_uncertain_sql_path
                    .to_str()
                    .ok_or("uncertain ROLLBACK SQL path is not UTF-8")?,
                "--schema",
                "information_schema",
                "--scope",
                requested_scope_path
                    .to_str()
                    .ok_or("interactive scope path is not UTF-8")?,
                "--sdk-library",
                sql_sdk.to_str().ok_or("SQL SDK path is not UTF-8")?,
                "--include-dir",
                include_dir
                    .to_str()
                    .ok_or("SQL SDK include path is not UTF-8")?,
                "--sdk-release",
                "1",
                "--cpu-baseline",
                "x86_64-v1",
                "--runtime-profile",
                "nixos-x86_64-glibc-2.42",
                "--compiler-identity",
                "TinyCC interactive uncertain ROLLBACK fixture",
            ])
            .output()?;
        let rollback_proxy_result = rollback_proxy.join().unwrap();
        assert!(
            uncertain_rollback.status.success(),
            "interactive-sql did not reconcile a lost ROLLBACK reply: {}; proxy={rollback_proxy_result:?}",
            String::from_utf8_lossy(&uncertain_rollback.stderr)
        );
        assert!(
            String::from_utf8_lossy(&uncertain_rollback.stderr).contains("rolled back transaction")
        );
        assert_interactive_response_drop_stats(rollback_proxy_result?, 1, 1);

        let uncertain_ddl_sql_path = root.join("interactive-uncertain-ddl.sql");
        std::fs::write(
            &uncertain_ddl_sql_path,
            "CREATE SCHEMA interactive_ddl_uncertain_probe;\n",
        )?;
        std::fs::set_permissions(
            &uncertain_ddl_sql_path,
            std::fs::Permissions::from_mode(0o600),
        )?;
        let (ddl_proxy_address, ddl_proxy) =
            start_interactive_response_dropping_proxy(address, 10, 2, false)?;
        let uncertain_ddl = Command::new(env!("CARGO_BIN_EXE_prdb-client"))
            .args([
                "interactive-sql",
                "--address",
                &ddl_proxy_address.to_string(),
                "--credentials",
                interactive_admin_credentials_path
                    .to_str()
                    .ok_or("interactive admin credentials path is not UTF-8")?,
                "--default-user",
                "metadata_process_admin",
                "--sql-file",
                uncertain_ddl_sql_path
                    .to_str()
                    .ok_or("uncertain DDL SQL path is not UTF-8")?,
                "--schema",
                "information_schema",
                "--scope",
                requested_scope_path
                    .to_str()
                    .ok_or("interactive scope path is not UTF-8")?,
                "--sdk-library",
                sql_sdk.to_str().ok_or("SQL SDK path is not UTF-8")?,
                "--include-dir",
                include_dir
                    .to_str()
                    .ok_or("SQL SDK include path is not UTF-8")?,
                "--sdk-release",
                "1",
                "--cpu-baseline",
                "x86_64-v1",
                "--runtime-profile",
                "nixos-x86_64-glibc-2.42",
                "--compiler-identity",
                "TinyCC interactive uncertain DDL fixture",
            ])
            .output()?;
        let ddl_proxy_result = ddl_proxy.join().unwrap();
        assert!(
            uncertain_ddl.status.success(),
            "interactive-sql did not reconcile a lost DDL reply: {}; proxy={ddl_proxy_result:?}",
            String::from_utf8_lossy(&uncertain_ddl.stderr)
        );
        assert!(
            String::from_utf8_lossy(&uncertain_ddl.stderr).contains("committed after reconnect")
        );
        assert_interactive_response_drop_stats(ddl_proxy_result?, 0, 2);
        let created_uncertain_schema = schema_verify
            .list_schemas_page(128, None)?
            .records
            .into_iter()
            .find(|schema| schema.name == "interactive_ddl_uncertain_probe")
            .ok_or("uncertain DDL operation was not committed")?;
        schema_verify.drop_schema(created_uncertain_schema.schema_id)?;

        let cancel_credentials_path = root.join("cancel-owner.cred");
        let cancel_credentials = ClientCredentials {
            token_id: restricted_token,
            token_secret: restricted_secret,
        };
        write_client_credentials(&cancel_credentials_path, &cancel_credentials)?;
        drop(cancel_credentials);
        let invocation_id_text = encode_id_hex(&invocation_id);
        let cancel_address_text = address.to_string();
        let cancel_command = Command::new(env!("CARGO_BIN_EXE_prdb-client"))
            .args([
                "cancel",
                "--address",
                &cancel_address_text,
                "--credentials",
                cancel_credentials_path
                    .to_str()
                    .ok_or("credential path is not UTF-8")?,
                "--invocation-id",
                &invocation_id_text,
            ])
            .output()?;
        assert!(
            cancel_command.status.success(),
            "cancel CLI failed: {}",
            String::from_utf8_lossy(&cancel_command.stderr)
        );
        assert_eq!(
            String::from_utf8_lossy(&cancel_command.stdout).trim(),
            "committed"
        );
        remove_client_credentials(&cancel_credentials_path)?;
    }

    let mut admin_control_secret = token_secret;
    let mut metadata = MetadataClient::connect_authenticated(address, token_id, &mut token_secret)?;
    assert_eq!(token_secret, [0; 32]);
    let schemas = metadata.list_schemas()?;
    assert!(schemas.iter().any(|schema| {
        schema.schema_id == prdb_engine::metadata_discovery::INFORMATION_SCHEMA_SCHEMA_ID
            && schema.name == "information_schema"
    }));
    assert!(schemas.iter().any(|schema| schema.name == "admin_private"));
    let relations =
        metadata.list_relations(prdb_engine::metadata_discovery::INFORMATION_SCHEMA_SCHEMA_ID)?;
    assert_eq!(relations.len(), 3);
    let columns = metadata.list_columns(
        prdb_engine::metadata_discovery::INFORMATION_SCHEMA_SCHEMA_ID,
        relations
            .iter()
            .find(|relation| relation.name == "schemata")
            .ok_or("information_schema.schemata was missing")?
            .relation_id,
    )?;
    assert!(!columns.is_empty());
    let schemata_relation = relations
        .iter()
        .find(|relation| relation.name == "schemata")
        .ok_or("information_schema.schemata was missing")?;
    let discover_credentials_path = root.join("discover.credentials");
    let discover_credentials = ClientCredentials {
        token_id,
        token_secret: admin_control_secret,
    };
    write_client_credentials(&discover_credentials_path, &discover_credentials)?;
    drop(discover_credentials);
    let address_text = address.to_string();
    let schema_id_text =
        encode_id_hex(&prdb_engine::metadata_discovery::INFORMATION_SCHEMA_SCHEMA_ID);
    let relation_id_text = encode_id_hex(&schemata_relation.relation_id);
    let discover = Command::new(env!("CARGO_BIN_EXE_prdb-client"))
        .args([
            "discover",
            "--address",
            &address_text,
            "--credentials",
            discover_credentials_path
                .to_str()
                .ok_or("non-UTF-8 credential path")?,
            "--schema",
            "information_schema",
            "--relation",
            "schemata",
        ])
        .output()?;
    assert!(
        discover.status.success(),
        "discover CLI failed: {}",
        String::from_utf8_lossy(&discover.stderr)
    );
    let discover_output = String::from_utf8(discover.stdout)?;
    assert!(discover_output.contains(&format!("schema\t{schema_id_text}\tinformation_schema")));
    assert!(discover_output.contains(&format!(
        "relation\t{schema_id_text}\t{relation_id_text}\tschemata"
    )));
    assert!(discover_output.contains(&format!("column\t{relation_id_text}\t0\t")));
    remove_client_credentials(&discover_credentials_path)?;
    assert!(
        metadata
            .list_relations(restricted_schema_id)?
            .iter()
            .any(|relation| relation.relation_id == restricted_table_id)
    );
    assert!(
        !metadata
            .list_columns(restricted_schema_id, restricted_table_id)?
            .is_empty()
    );
    drop(metadata);

    let mut owner_metadata =
        AdminClient::connect_authenticated(address, owner_token, &mut owner_secret)?;
    assert!(
        owner_metadata
            .list_schemas_page(128, None)?
            .records
            .iter()
            .any(|schema| schema.name == "admin_private")
    );
    assert!(
        owner_metadata
            .list_relations_page(restricted_schema_id, 128, None)?
            .records
            .iter()
            .any(|relation| relation.relation_id == restricted_table_id)
    );
    assert!(
        !owner_metadata
            .list_columns_page(restricted_schema_id, restricted_table_id, 128, None)?
            .records
            .is_empty()
    );
    let owner_indexes =
        owner_metadata.list_indexes_page(restricted_schema_id, restricted_table_id, 128, None)?;
    let owner_column_id = owner_metadata
        .list_columns_page(restricted_schema_id, restricted_table_id, 128, None)?
        .records
        .first()
        .ok_or("schema owner returned no declared columns")?
        .column_id;
    assert!(owner_indexes.records.iter().any(|index| {
        index.index_id == restricted_index_id
            && index.column_ids == vec![owner_column_id]
            && !index.unique
    }));
    let owner_schema_cursor = owner_metadata
        .list_schemas_page(1, None)?
        .next_cursor
        .ok_or("expected a second schema visible through ownership")?;
    let mut revocation_admin_secret = admin_control_secret;
    let mut reconnect_admin_secret = admin_control_secret;
    let mut administrator =
        AdminClient::connect_authenticated(address, token_id, &mut revocation_admin_secret)?;
    administrator
        .set_schema_owner(restricted_schema_id, admin_user_id)
        .map_err(|error| format!("schema-owner transfer failed: {error:?}"))?;
    assert!(matches!(
        owner_metadata.list_schemas_page(1, Some(&owner_schema_cursor)),
        Err(prdb_engine::client_submit::ClientSubmitError::ServerRejected(_))
    ));
    let administrator_cursor = administrator
        .list_schemas_page(1, None)
        .map_err(|error| format!("administrator schema cursor start failed: {error:?}"))?
        .next_cursor
        .ok_or("expected a second administrator schema page")?;
    drop(owner_metadata);
    drop(administrator);
    let mut reconnected_admin =
        AdminClient::connect_authenticated(address, token_id, &mut reconnect_admin_secret)
            .map_err(|error| format!("administrator reconnect failed: {error:?}"))?;
    let replayed_cursor = reconnected_admin.list_schemas_page(1, Some(&administrator_cursor));
    assert!(
        matches!(
            &replayed_cursor,
            Err(prdb_engine::client_submit::ClientSubmitError::ServerRejected(_))
        ),
        "replayed administrator cursor was not rejected: {replayed_cursor:?}"
    );
    drop(reconnected_admin);

    let mut restricted_admin_secret = restricted_secret;
    let mut restricted_metadata =
        MetadataClient::connect_authenticated(address, restricted_token, &mut restricted_secret)?;
    let restricted_schemas = restricted_metadata.list_schemas()?;
    assert!(restricted_schemas.iter().any(|schema| {
        schema.schema_id == prdb_engine::metadata_discovery::INFORMATION_SCHEMA_SCHEMA_ID
    }));
    assert!(
        !restricted_schemas
            .iter()
            .any(|schema| schema.name == "admin_private")
    );
    assert!(
        restricted_metadata
            .list_relations(prdb_engine::metadata_discovery::INFORMATION_SCHEMA_SCHEMA_ID)?
            .iter()
            .any(|relation| relation.name == "schemata")
    );
    assert!(
        restricted_metadata
            .list_relations(restricted_schema_id)?
            .is_empty()
    );
    assert!(
        restricted_metadata
            .list_columns(restricted_schema_id, restricted_table_id)?
            .is_empty()
    );
    drop(restricted_metadata);
    let mut restricted_admin_retry_secret = restricted_admin_secret;
    let mut restricted_admin_issue_retry_secret = restricted_admin_secret;
    let mut restricted_admin_issue_missing_secret = restricted_admin_secret;
    let mut restricted_admin_followup_secret = restricted_admin_secret;
    let mut restricted_admin = AdminClient::connect_authenticated(
        address,
        restricted_token,
        &mut restricted_admin_secret,
    )?;
    let unauthorized_existing_rotation = restricted_admin.rotate_token(
        restricted_token,
        "unauthorized existing-token probe",
        i64::MAX,
    );
    drop(restricted_admin);
    let mut restricted_admin = AdminClient::connect_authenticated(
        address,
        restricted_token,
        &mut restricted_admin_retry_secret,
    )?;
    let unauthorized_unknown_rotation =
        restricted_admin.rotate_token([0xd1; 16], "unauthorized unknown-token probe", i64::MAX);
    let rejection_message = |result: Result<_, _>| match result {
        Err(prdb_engine::client_submit::ClientSubmitError::AdministrativeOperationRejected {
            message,
            ..
        }) => message,
        Err(prdb_engine::client_submit::ClientSubmitError::ServerRejected(message)) => message,
        Err(other) => panic!("unauthorized token rotation had an unexpected error: {other}"),
        Ok(_) => panic!("unauthorized token rotation unexpectedly succeeded"),
    };
    assert_eq!(
        rejection_message(unauthorized_existing_rotation),
        rejection_message(unauthorized_unknown_rotation),
        "ADMIN exposed whether a token ID exists"
    );
    drop(restricted_admin);
    let mut restricted_admin = AdminClient::connect_authenticated(
        address,
        restricted_token,
        &mut restricted_admin_issue_retry_secret,
    )?;
    let unauthorized_existing_issue = restricted_admin.issue_token(
        restricted_user_id,
        "unauthorized existing-user probe",
        i64::MAX,
    );
    drop(restricted_admin);
    let mut restricted_admin = AdminClient::connect_authenticated(
        address,
        restricted_token,
        &mut restricted_admin_issue_missing_secret,
    )?;
    let unauthorized_missing_issue =
        restricted_admin.issue_token([0xd2; 16], "unauthorized missing-user probe", i64::MAX);
    assert_eq!(
        rejection_message(unauthorized_existing_issue),
        rejection_message(unauthorized_missing_issue),
        "ADMIN exposed whether an identity exists during token issuance"
    );
    drop(restricted_admin);
    let mut restricted_admin = AdminClient::connect_authenticated(
        address,
        restricted_token,
        &mut restricted_admin_followup_secret,
    )?;
    assert!(
        restricted_admin
            .list_indexes_page(restricted_schema_id, restricted_table_id, 128, None)?
            .records
            .is_empty()
    );
    assert!(
        restricted_admin
            .list_grants_page(restricted_schema_id, restricted_table_id, 128, None)?
            .records
            .is_empty()
    );
    assert!(
        restricted_admin
            .list_policies_page(restricted_schema_id, restricted_table_id, 128, None)?
            .records
            .is_empty()
    );
    drop(restricted_admin);

    let admin_cancel_credentials_path = root.join("admin-cancel.cred");
    let admin_cancel_credentials = ClientCredentials {
        token_id,
        token_secret: admin_control_secret,
    };
    write_client_credentials(&admin_cancel_credentials_path, &admin_cancel_credentials)?;
    drop(admin_cancel_credentials);
    let mut admin =
        AdminClient::connect_authenticated(address, token_id, &mut admin_control_secret)?;
    assert_eq!(admin_control_secret, [0; 32]);
    let ddl_schema = admin.create_schema("process_ddl", admin_user_id)?;
    let ddl_table = admin.create_table(
        ddl_schema.identity_id,
        "records",
        &[
            AdminColumnDefinition {
                name: "key".to_owned(),
                scalar_type: ScalarType::Integer,
                nullable: true,
                generated_uuid_identity: false,
                default: None,
            },
            AdminColumnDefinition {
                name: "label".to_owned(),
                scalar_type: ScalarType::Text,
                nullable: true,
                generated_uuid_identity: false,
                default: None,
            },
        ],
    )?;
    let ddl_constraint =
        admin.create_unique_constraint(ddl_table.table_id, &[ddl_table.column_ids[0]])?;
    assert!(ddl_schema.commit_sequence < ddl_table.commit_sequence);
    assert!(ddl_table.commit_sequence < ddl_constraint.commit_sequence);
    let mut verify_credentials = read_client_credentials(&admin_cancel_credentials_path)?;
    let mut ddl_metadata = MetadataClient::connect_authenticated(
        address,
        verify_credentials.token_id,
        &mut verify_credentials.token_secret,
    )?;
    assert!(
        ddl_metadata
            .list_schemas()?
            .iter()
            .any(
                |schema| schema.schema_id == ddl_schema.identity_id && schema.name == "process_ddl"
            )
    );
    assert!(
        ddl_metadata
            .list_relations(ddl_schema.identity_id)?
            .iter()
            .any(
                |relation| relation.relation_id == ddl_table.table_id && relation.name == "records"
            )
    );
    assert_eq!(
        ddl_metadata
            .list_columns(ddl_schema.identity_id, ddl_table.table_id)?
            .first()
            .map(|column| column.column_id),
        Some(ddl_table.column_ids[0])
    );
    drop(ddl_metadata);
    assert_eq!(
        admin.cancel_invocation(committed_invocation_id)?,
        CancellationResult::Terminal(TerminalOutcome::Committed)
    );
    let committed_invocation_id_text = encode_id_hex(&committed_invocation_id);
    let admin_cancel_address_text = address.to_string();
    let admin_cancel_command = Command::new(env!("CARGO_BIN_EXE_prdb-client"))
        .args([
            "admin-cancel",
            "--address",
            &admin_cancel_address_text,
            "--credentials",
            admin_cancel_credentials_path
                .to_str()
                .ok_or("credential path is not UTF-8")?,
            "--invocation-id",
            &committed_invocation_id_text,
        ])
        .output()?;
    assert!(
        admin_cancel_command.status.success(),
        "admin-cancel CLI failed: {}",
        String::from_utf8_lossy(&admin_cancel_command.stderr)
    );
    assert_eq!(
        String::from_utf8_lossy(&admin_cancel_command.stdout).trim(),
        "committed"
    );
    let admin_status_address_text = address.to_string();
    let admin_status_command = Command::new(env!("CARGO_BIN_EXE_prdb-client"))
        .args([
            "admin-status",
            "--address",
            &admin_status_address_text,
            "--credentials",
            admin_cancel_credentials_path
                .to_str()
                .ok_or("credential path is not UTF-8")?,
            "--invocation-id",
            &committed_invocation_id_text,
        ])
        .output()?;
    assert!(
        admin_status_command.status.success(),
        "admin-status CLI failed: {}",
        String::from_utf8_lossy(&admin_status_command.stderr)
    );
    assert_eq!(
        String::from_utf8_lossy(&admin_status_command.stdout).trim(),
        "committed"
    );
    let created_role = admin.create_role("process_admin_role")?;
    assert_ne!(created_role.identity_id, [0; 16]);
    let renamed = admin.rename_role(created_role.identity_id, "process_admin_role_v2")?;
    assert!(renamed.commit_sequence > created_role.commit_sequence);
    let granted = admin
        .grant_system_privilege(created_role.identity_id, AdminSystemPrivilege::CreateSchema)?;
    let revoked = admin
        .revoke_system_privilege(created_role.identity_id, AdminSystemPrivilege::CreateSchema)?;
    assert!(revoked.commit_sequence > granted.commit_sequence);
    let privilege_cli_address = address.to_string();
    let privilege_cli_credentials = admin_cancel_credentials_path
        .to_str()
        .ok_or("credential path is not UTF-8")?;
    let system_privilege_sql_path = root.join("system-privilege.sql");
    std::fs::write(
        &system_privilege_sql_path,
        "GRANT MANAGE_ROLES ON DATABASE TO ROLE process_admin_role_v2;",
    )?;
    let system_privilege_sql_path_text = system_privilege_sql_path
        .to_str()
        .ok_or("SQL path is not UTF-8")?;
    let grant_privilege_cli = Command::new(env!("CARGO_BIN_EXE_prdb-client"))
        .args([
            "admin-sql",
            "--address",
            &privilege_cli_address,
            "--credentials",
            privilege_cli_credentials,
            "--sql-file",
            system_privilege_sql_path_text,
        ])
        .output()?;
    assert!(
        grant_privilege_cli.status.success(),
        "system-privilege-grant CLI failed: {}",
        String::from_utf8_lossy(&grant_privilege_cli.stderr)
    );
    assert!(
        String::from_utf8_lossy(&grant_privilege_cli.stdout).contains("granted system privilege")
    );
    let member_user = admin
        .create_user("process_member_user")
        .map_err(|error| format!("create delegated member user: {error:?}"))?;
    let member_user_expiration =
        i64::try_from(SystemTime::now().duration_since(UNIX_EPOCH)?.as_micros())?
            .saturating_add(900_000_000);
    let member_token = admin.issue_token(
        member_user.identity_id,
        "role membership test",
        member_user_expiration,
    )?;
    let member_credentials_path = root.join("role-member.cred");
    let member_credentials = ClientCredentials {
        token_id: member_token.token_id,
        token_secret: *member_token.token_secret(),
    };
    write_client_credentials(&member_credentials_path, &member_credentials)?;
    drop(member_credentials);
    drop(member_token);
    let usage_grant = admin.grant_object_privilege(
        member_user.identity_id,
        ddl_schema.identity_id,
        AdminObjectPrivilege::Usage,
        AdminObjectGrantTarget::Object,
    )?;
    let table_grant = admin.grant_object_privilege(
        member_user.identity_id,
        ddl_table.table_id,
        AdminObjectPrivilege::Select,
        AdminObjectGrantTarget::Object,
    )?;
    let column_grant = admin.grant_object_privilege(
        member_user.identity_id,
        ddl_table.table_id,
        AdminObjectPrivilege::Select,
        AdminObjectGrantTarget::Column(ddl_table.column_ids[0]),
    )?;
    assert!(usage_grant.commit_sequence < table_grant.commit_sequence);
    assert!(table_grant.commit_sequence < column_grant.commit_sequence);
    let mut member_metadata_credentials = read_client_credentials(&member_credentials_path)?;
    let mut member_metadata = MetadataClient::connect_authenticated(
        address,
        member_metadata_credentials.token_id,
        &mut member_metadata_credentials.token_secret,
    )?;
    assert!(
        member_metadata
            .list_schemas()?
            .iter()
            .any(|schema| schema.schema_id == ddl_schema.identity_id)
    );
    assert_eq!(
        member_metadata
            .list_relations(ddl_schema.identity_id)?
            .iter()
            .map(|relation| relation.relation_id)
            .collect::<Vec<_>>(),
        [ddl_table.table_id]
    );
    assert_eq!(
        member_metadata
            .list_columns(ddl_schema.identity_id, ddl_table.table_id)?
            .iter()
            .map(|column| column.column_id)
            .collect::<Vec<_>>(),
        ddl_table.column_ids
    );
    let revoked_column = admin.revoke_object_privilege(
        member_user.identity_id,
        ddl_table.table_id,
        AdminObjectPrivilege::Select,
        AdminObjectGrantTarget::Column(ddl_table.column_ids[0]),
    )?;
    let revoked_table = admin.revoke_object_privilege(
        member_user.identity_id,
        ddl_table.table_id,
        AdminObjectPrivilege::Select,
        AdminObjectGrantTarget::Object,
    )?;
    let revoked_usage = admin.revoke_object_privilege(
        member_user.identity_id,
        ddl_schema.identity_id,
        AdminObjectPrivilege::Usage,
        AdminObjectGrantTarget::Object,
    )?;
    assert!(revoked_column.commit_sequence < revoked_table.commit_sequence);
    assert!(revoked_table.commit_sequence < revoked_usage.commit_sequence);
    assert!(
        !member_metadata
            .list_schemas()?
            .iter()
            .any(|schema| schema.schema_id == ddl_schema.identity_id)
    );
    drop(member_metadata);
    let object_privilege_sql_path = root.join("object-privilege.sql");
    std::fs::write(
        &object_privilege_sql_path,
        "GRANT USAGE ON SCHEMA process_ddl TO USER process_member_user;",
    )?;
    let object_privilege_sql_path_text = object_privilege_sql_path
        .to_str()
        .ok_or("SQL path is not UTF-8")?;
    let grant_object_privilege_sql = Command::new(env!("CARGO_BIN_EXE_prdb-client"))
        .args([
            "admin-sql",
            "--address",
            &address.to_string(),
            "--credentials",
            admin_cancel_credentials_path
                .to_str()
                .ok_or("credential path is not UTF-8")?,
            "--sql-file",
            object_privilege_sql_path_text,
        ])
        .output()?;
    assert!(
        grant_object_privilege_sql.status.success(),
        "object privilege GRANT SQL failed: {}",
        String::from_utf8_lossy(&grant_object_privilege_sql.stderr)
    );
    assert!(
        String::from_utf8_lossy(&grant_object_privilege_sql.stdout)
            .contains("granted object privilege")
    );
    std::fs::write(
        &object_privilege_sql_path,
        "REVOKE USAGE ON SCHEMA process_ddl FROM USER process_member_user;",
    )?;
    let revoke_object_privilege_sql = Command::new(env!("CARGO_BIN_EXE_prdb-client"))
        .args([
            "admin-sql",
            "--address",
            &address.to_string(),
            "--credentials",
            admin_cancel_credentials_path
                .to_str()
                .ok_or("credential path is not UTF-8")?,
            "--sql-file",
            object_privilege_sql_path_text,
        ])
        .output()?;
    assert!(
        revoke_object_privilege_sql.status.success(),
        "object privilege REVOKE SQL failed: {}",
        String::from_utf8_lossy(&revoke_object_privilege_sql.stderr)
    );
    assert!(
        String::from_utf8_lossy(&revoke_object_privilege_sql.stdout)
            .contains("revoked object privilege")
    );
    std::fs::write(
        &object_privilege_sql_path,
        "GRANT SELECT (key) ON TABLE process_ddl.records TO USER process_member_user;",
    )?;
    let grant_existing_column = Command::new(env!("CARGO_BIN_EXE_prdb-client"))
        .args([
            "admin-sql",
            "--address",
            &address.to_string(),
            "--credentials",
            admin_cancel_credentials_path
                .to_str()
                .ok_or("credential path is not UTF-8")?,
            "--sql-file",
            object_privilege_sql_path_text,
        ])
        .output()?;
    assert!(
        grant_existing_column.status.success(),
        "single-column privilege GRANT SQL failed: {}",
        String::from_utf8_lossy(&grant_existing_column.stderr)
    );
    admin.execute_object_privilege_sql(
        "GRANT ALTER, REFERENCES (key) ON TABLE process_ddl.records TO USER process_member_user",
    )?;
    let mixed_scope_grants =
        admin.list_grants_page(ddl_schema.identity_id, ddl_table.table_id, 128, None)?;
    assert!(mixed_scope_grants.records.iter().any(|grant| {
        grant.grantee_id == member_user.identity_id
            && grant.privilege
                == prdb_engine::metadata_discovery::AdministrativeGrantPrivilege::Alter
            && grant.target_scope
                == prdb_engine::metadata_discovery::AdministrativeGrantTargetScope::Object
            && grant.target_id == ddl_table.table_id
    }));
    assert!(mixed_scope_grants.records.iter().any(|grant| {
        grant.grantee_id == member_user.identity_id
            && grant.privilege
                == prdb_engine::metadata_discovery::AdministrativeGrantPrivilege::References
            && grant.target_scope
                == prdb_engine::metadata_discovery::AdministrativeGrantTargetScope::Column
            && grant.target_id == ddl_table.column_ids[0]
    }));
    admin.execute_object_privilege_sql(
        "REVOKE ALTER, REFERENCES (key) ON TABLE process_ddl.records FROM USER process_member_user",
    )?;
    let after_mixed_scope_revoke =
        admin.list_grants_page(ddl_schema.identity_id, ddl_table.table_id, 128, None)?;
    assert!(!after_mixed_scope_revoke.records.iter().any(|grant| {
        grant.grantee_id == member_user.identity_id
            && matches!(
                grant.privilege,
                prdb_engine::metadata_discovery::AdministrativeGrantPrivilege::Alter
                    | prdb_engine::metadata_discovery::AdministrativeGrantPrivilege::References
            )
    }));
    admin.execute_object_privilege_sql(
        "GRANT ALTER, REFERENCES ON TABLE process_ddl.records TO USER process_member_user",
    )?;
    let expanded_mixed_scope_grants =
        admin.list_grants_page(ddl_schema.identity_id, ddl_table.table_id, 128, None)?;
    assert!(expanded_mixed_scope_grants.records.iter().any(|grant| {
        grant.grantee_id == member_user.identity_id
            && grant.privilege
                == prdb_engine::metadata_discovery::AdministrativeGrantPrivilege::Alter
            && grant.target_scope
                == prdb_engine::metadata_discovery::AdministrativeGrantTargetScope::Object
            && grant.target_id == ddl_table.table_id
    }));
    for column_id in &ddl_table.column_ids {
        assert!(expanded_mixed_scope_grants.records.iter().any(|grant| {
            grant.grantee_id == member_user.identity_id
                && grant.privilege
                    == prdb_engine::metadata_discovery::AdministrativeGrantPrivilege::References
                && grant.target_scope
                    == prdb_engine::metadata_discovery::AdministrativeGrantTargetScope::Column
                && grant.target_id == *column_id
        }));
    }
    assert_eq!(
        expanded_mixed_scope_grants
            .records
            .iter()
            .filter(|grant| {
                grant.grantee_id == member_user.identity_id
                    && grant.privilege
                        == prdb_engine::metadata_discovery::AdministrativeGrantPrivilege::References
            })
            .count(),
        ddl_table.column_ids.len(),
        "an unqualified data privilege expands to exactly the existing columns"
    );
    admin.execute_object_privilege_sql(
        "REVOKE ALTER, REFERENCES ON TABLE process_ddl.records FROM USER process_member_user",
    )?;
    let after_expanded_mixed_scope_revoke =
        admin.list_grants_page(ddl_schema.identity_id, ddl_table.table_id, 128, None)?;
    assert!(
        !after_expanded_mixed_scope_revoke
            .records
            .iter()
            .any(|grant| {
                grant.grantee_id == member_user.identity_id
                    && matches!(
                grant.privilege,
                prdb_engine::metadata_discovery::AdministrativeGrantPrivilege::Alter
                    | prdb_engine::metadata_discovery::AdministrativeGrantPrivilege::References
            )
            })
    );
    admin.execute_object_privilege_sql(
        "GRANT ALTER, UPDATE (label) ON TABLE process_ddl.records TO USER process_member_user",
    )?;
    let mixed_update_grants =
        admin.list_grants_page(ddl_schema.identity_id, ddl_table.table_id, 128, None)?;
    assert!(mixed_update_grants.records.iter().any(|grant| {
        grant.grantee_id == member_user.identity_id
            && grant.privilege
                == prdb_engine::metadata_discovery::AdministrativeGrantPrivilege::Alter
            && grant.target_scope
                == prdb_engine::metadata_discovery::AdministrativeGrantTargetScope::Object
            && grant.target_id == ddl_table.table_id
    }));
    assert!(mixed_update_grants.records.iter().any(|grant| {
        grant.grantee_id == member_user.identity_id
            && grant.privilege
                == prdb_engine::metadata_discovery::AdministrativeGrantPrivilege::Update
            && grant.target_scope
                == prdb_engine::metadata_discovery::AdministrativeGrantTargetScope::Column
            && grant.target_id == ddl_table.column_ids[1]
    }));
    admin.execute_object_privilege_sql(
        "REVOKE ALTER, UPDATE (label) ON TABLE process_ddl.records FROM USER process_member_user",
    )?;
    let after_mixed_update_revoke =
        admin.list_grants_page(ddl_schema.identity_id, ddl_table.table_id, 128, None)?;
    assert!(!after_mixed_update_revoke.records.iter().any(|grant| {
        grant.grantee_id == member_user.identity_id
            && matches!(
                grant.privilege,
                prdb_engine::metadata_discovery::AdministrativeGrantPrivilege::Alter
                    | prdb_engine::metadata_discovery::AdministrativeGrantPrivilege::Update
            )
    }));
    std::fs::write(
        &object_privilege_sql_path,
        "GRANT SELECT (key, label) ON TABLE process_ddl.records TO USER process_member_user;",
    )?;
    let grant_column_privilege_sql = Command::new(env!("CARGO_BIN_EXE_prdb-client"))
        .args([
            "admin-sql",
            "--address",
            &address.to_string(),
            "--credentials",
            admin_cancel_credentials_path
                .to_str()
                .ok_or("credential path is not UTF-8")?,
            "--sql-file",
            object_privilege_sql_path_text,
        ])
        .output()?;
    assert!(
        !grant_column_privilege_sql.status.success(),
        "overlapping multi-column privilege GRANT unexpectedly succeeded"
    );
    let after_overlapping_grant =
        admin.list_grants_page(ddl_schema.identity_id, ddl_table.table_id, 128, None)?;
    assert!(
        ddl_table
            .column_ids
            .iter()
            .enumerate()
            .all(|(index, column_id)| {
                let has_grant = after_overlapping_grant.records.iter().any(|grant| {
                    grant.grantee_id == member_user.identity_id
                && grant.privilege
                    == prdb_engine::metadata_discovery::AdministrativeGrantPrivilege::Select
                && grant.target_scope
                    == prdb_engine::metadata_discovery::AdministrativeGrantTargetScope::Column
                && grant.target_id == *column_id
                });
                has_grant == (index == 0)
            })
    );
    std::fs::write(
        &object_privilege_sql_path,
        "GRANT SELECT (label) ON TABLE process_ddl.records TO USER process_member_user;",
    )?;
    let grant_sibling_column = Command::new(env!("CARGO_BIN_EXE_prdb-client"))
        .args([
            "admin-sql",
            "--address",
            &address.to_string(),
            "--credentials",
            admin_cancel_credentials_path
                .to_str()
                .ok_or("credential path is not UTF-8")?,
            "--sql-file",
            object_privilege_sql_path_text,
        ])
        .output()?;
    assert!(
        grant_sibling_column.status.success(),
        "sibling-column privilege GRANT SQL failed: {}",
        String::from_utf8_lossy(&grant_sibling_column.stderr)
    );
    std::fs::write(
        &object_privilege_sql_path,
        "REVOKE SELECT (key) ON TABLE process_ddl.records FROM USER process_member_user;",
    )?;
    let revoke_existing_column = Command::new(env!("CARGO_BIN_EXE_prdb-client"))
        .args([
            "admin-sql",
            "--address",
            &address.to_string(),
            "--credentials",
            admin_cancel_credentials_path
                .to_str()
                .ok_or("credential path is not UTF-8")?,
            "--sql-file",
            object_privilege_sql_path_text,
        ])
        .output()?;
    assert!(
        revoke_existing_column.status.success(),
        "single-column privilege REVOKE SQL failed: {}",
        String::from_utf8_lossy(&revoke_existing_column.stderr)
    );
    let after_single_column_revoke =
        admin.list_grants_page(ddl_schema.identity_id, ddl_table.table_id, 128, None)?;
    assert!(
        ddl_table
            .column_ids
            .iter()
            .enumerate()
            .all(|(index, column_id)| {
                let has_select = after_single_column_revoke.records.iter().any(|grant| {
                    grant.grantee_id == member_user.identity_id
                && grant.privilege
                    == prdb_engine::metadata_discovery::AdministrativeGrantPrivilege::Select
                && grant.target_scope
                    == prdb_engine::metadata_discovery::AdministrativeGrantTargetScope::Column
                && grant.target_id == *column_id
                });
                has_select == (index == 1)
            }),
        "partial column REVOKE must remove key while preserving label"
    );
    std::fs::write(
        &object_privilege_sql_path,
        "REVOKE SELECT (label) ON TABLE process_ddl.records FROM USER process_member_user;",
    )?;
    let revoke_sibling_column = Command::new(env!("CARGO_BIN_EXE_prdb-client"))
        .args([
            "admin-sql",
            "--address",
            &address.to_string(),
            "--credentials",
            admin_cancel_credentials_path
                .to_str()
                .ok_or("credential path is not UTF-8")?,
            "--sql-file",
            object_privilege_sql_path_text,
        ])
        .output()?;
    assert!(
        revoke_sibling_column.status.success(),
        "sibling-column privilege REVOKE SQL failed: {}",
        String::from_utf8_lossy(&revoke_sibling_column.stderr)
    );
    std::fs::write(
        &object_privilege_sql_path,
        "GRANT SELECT (key, label) ON TABLE process_ddl.records TO USER process_member_user;",
    )?;
    let grant_column_privilege_sql = Command::new(env!("CARGO_BIN_EXE_prdb-client"))
        .args([
            "admin-sql",
            "--address",
            &address.to_string(),
            "--credentials",
            admin_cancel_credentials_path
                .to_str()
                .ok_or("credential path is not UTF-8")?,
            "--sql-file",
            object_privilege_sql_path_text,
        ])
        .output()?;
    assert!(
        grant_column_privilege_sql.status.success(),
        "multi-column privilege GRANT SQL failed: {}",
        String::from_utf8_lossy(&grant_column_privilege_sql.stderr)
    );
    let multi_column_grants =
        admin.list_grants_page(ddl_schema.identity_id, ddl_table.table_id, 128, None)?;
    assert!(ddl_table.column_ids.iter().all(|column_id| {
        multi_column_grants.records.iter().any(|grant| {
            grant.grantee_id == member_user.identity_id
                && grant.privilege
                    == prdb_engine::metadata_discovery::AdministrativeGrantPrivilege::Select
                && grant.target_scope
                    == prdb_engine::metadata_discovery::AdministrativeGrantTargetScope::Column
                && grant.target_id == *column_id
        })
    }));
    std::fs::write(
        &object_privilege_sql_path,
        "REVOKE SELECT (key, label) ON TABLE process_ddl.records FROM USER process_member_user;",
    )?;
    let revoke_column_privilege_sql = Command::new(env!("CARGO_BIN_EXE_prdb-client"))
        .args([
            "admin-sql",
            "--address",
            &address.to_string(),
            "--credentials",
            admin_cancel_credentials_path
                .to_str()
                .ok_or("credential path is not UTF-8")?,
            "--sql-file",
            object_privilege_sql_path_text,
        ])
        .output()?;
    assert!(
        revoke_column_privilege_sql.status.success(),
        "column privilege REVOKE SQL failed: {}",
        String::from_utf8_lossy(&revoke_column_privilege_sql.stderr)
    );
    let multi_column_revokes =
        admin.list_grants_page(ddl_schema.identity_id, ddl_table.table_id, 128, None)?;
    assert!(!multi_column_revokes.records.iter().any(|grant| {
        grant.grantee_id == member_user.identity_id
            && grant.privilege
                == prdb_engine::metadata_discovery::AdministrativeGrantPrivilege::Select
            && grant.target_scope
                == prdb_engine::metadata_discovery::AdministrativeGrantTargetScope::Column
    }));
    admin.execute_object_privilege_sql(
        "GRANT ALTER ON TABLE process_ddl.records TO USER process_member_user",
    )?;
    std::fs::write(
        &object_privilege_sql_path,
        "GRANT ALTER, INDEX ON TABLE process_ddl.records TO USER process_member_user;",
    )?;
    let overlapping_privilege_batch = Command::new(env!("CARGO_BIN_EXE_prdb-client"))
        .args([
            "admin-sql",
            "--address",
            &address.to_string(),
            "--credentials",
            admin_cancel_credentials_path
                .to_str()
                .ok_or("credential path is not UTF-8")?,
            "--sql-file",
            object_privilege_sql_path_text,
        ])
        .output()?;
    assert!(
        !overlapping_privilege_batch.status.success(),
        "multi-privilege GRANT unexpectedly accepted a pre-existing member"
    );
    let after_overlapping_privilege_batch =
        admin.list_grants_page(ddl_schema.identity_id, ddl_table.table_id, 128, None)?;
    assert!(
        after_overlapping_privilege_batch
            .records
            .iter()
            .any(|grant| {
                grant.grantee_id == member_user.identity_id
                    && grant.privilege
                        == prdb_engine::metadata_discovery::AdministrativeGrantPrivilege::Alter
            })
    );
    assert!(
        !after_overlapping_privilege_batch
            .records
            .iter()
            .any(|grant| {
                grant.grantee_id == member_user.identity_id
                    && grant.privilege
                        == prdb_engine::metadata_discovery::AdministrativeGrantPrivilege::Index
            })
    );
    admin.execute_object_privilege_sql(
        "REVOKE ALTER ON TABLE process_ddl.records FROM USER process_member_user",
    )?;
    std::fs::write(
        &object_privilege_sql_path,
        "GRANT ALTER, INDEX, MANAGE_POLICIES, MANAGE_GRANTS ON TABLE process_ddl.records TO USER process_member_user;",
    )?;
    let grant_multiple_privileges = Command::new(env!("CARGO_BIN_EXE_prdb-client"))
        .args([
            "admin-sql",
            "--address",
            &address.to_string(),
            "--credentials",
            admin_cancel_credentials_path
                .to_str()
                .ok_or("credential path is not UTF-8")?,
            "--sql-file",
            object_privilege_sql_path_text,
        ])
        .output()?;
    assert!(
        grant_multiple_privileges.status.success(),
        "multi-privilege GRANT SQL failed: {}",
        String::from_utf8_lossy(&grant_multiple_privileges.stderr)
    );
    let multiple_privilege_grants =
        admin.list_grants_page(ddl_schema.identity_id, ddl_table.table_id, 128, None)?;
    for privilege in [
        prdb_engine::metadata_discovery::AdministrativeGrantPrivilege::Alter,
        prdb_engine::metadata_discovery::AdministrativeGrantPrivilege::Index,
        prdb_engine::metadata_discovery::AdministrativeGrantPrivilege::ManagePolicies,
        prdb_engine::metadata_discovery::AdministrativeGrantPrivilege::ManageGrants,
    ] {
        assert!(multiple_privilege_grants.records.iter().any(|grant| {
            grant.grantee_id == member_user.identity_id
                && grant.privilege == privilege
                && grant.target_scope
                    == prdb_engine::metadata_discovery::AdministrativeGrantTargetScope::Object
        }));
    }
    assert!(!multiple_privilege_grants.records.iter().any(|grant| {
        grant.grantee_id == member_user.identity_id
            && grant.privilege
                == prdb_engine::metadata_discovery::AdministrativeGrantPrivilege::Delete
    }));
    std::fs::write(
        &object_privilege_sql_path,
        "REVOKE ALTER, INDEX, MANAGE_POLICIES, MANAGE_GRANTS, DELETE ON TABLE process_ddl.records FROM USER process_member_user;",
    )?;
    let partial_revoke_batch = Command::new(env!("CARGO_BIN_EXE_prdb-client"))
        .args([
            "admin-sql",
            "--address",
            &address.to_string(),
            "--credentials",
            admin_cancel_credentials_path
                .to_str()
                .ok_or("credential path is not UTF-8")?,
            "--sql-file",
            object_privilege_sql_path_text,
        ])
        .output()?;
    assert!(
        !partial_revoke_batch.status.success(),
        "multi-privilege REVOKE unexpectedly accepted an absent DELETE grant"
    );
    let after_partial_revoke_batch =
        admin.list_grants_page(ddl_schema.identity_id, ddl_table.table_id, 128, None)?;
    for privilege in [
        prdb_engine::metadata_discovery::AdministrativeGrantPrivilege::Alter,
        prdb_engine::metadata_discovery::AdministrativeGrantPrivilege::Index,
        prdb_engine::metadata_discovery::AdministrativeGrantPrivilege::ManagePolicies,
        prdb_engine::metadata_discovery::AdministrativeGrantPrivilege::ManageGrants,
    ] {
        assert!(after_partial_revoke_batch.records.iter().any(|grant| {
            grant.grantee_id == member_user.identity_id
                && grant.privilege == privilege
                && grant.target_scope
                    == prdb_engine::metadata_discovery::AdministrativeGrantTargetScope::Object
        }));
    }
    std::fs::write(
        &object_privilege_sql_path,
        "REVOKE ALTER, INDEX, MANAGE_POLICIES, MANAGE_GRANTS ON TABLE process_ddl.records FROM USER process_member_user;",
    )?;
    let revoke_multiple_privileges = Command::new(env!("CARGO_BIN_EXE_prdb-client"))
        .args([
            "admin-sql",
            "--address",
            &address.to_string(),
            "--credentials",
            admin_cancel_credentials_path
                .to_str()
                .ok_or("credential path is not UTF-8")?,
            "--sql-file",
            object_privilege_sql_path_text,
        ])
        .output()?;
    assert!(
        revoke_multiple_privileges.status.success(),
        "multi-privilege REVOKE SQL failed: {}",
        String::from_utf8_lossy(&revoke_multiple_privileges.stderr)
    );
    let revoked_multiple_privileges =
        admin.list_grants_page(ddl_schema.identity_id, ddl_table.table_id, 128, None)?;
    assert!(!revoked_multiple_privileges.records.iter().any(|grant| {
        grant.grantee_id == member_user.identity_id
            && matches!(
                grant.privilege,
                prdb_engine::metadata_discovery::AdministrativeGrantPrivilege::Alter
                    | prdb_engine::metadata_discovery::AdministrativeGrantPrivilege::Index
                    | prdb_engine::metadata_discovery::AdministrativeGrantPrivilege::ManagePolicies
                    | prdb_engine::metadata_discovery::AdministrativeGrantPrivilege::ManageGrants
            )
    }));
    std::fs::write(
        &object_privilege_sql_path,
        "GRANT SELECT ON TABLE process_ddl.records TO USER process_member_user;",
    )?;
    let grant_all_existing_columns = Command::new(env!("CARGO_BIN_EXE_prdb-client"))
        .args([
            "admin-sql",
            "--address",
            &address.to_string(),
            "--credentials",
            admin_cancel_credentials_path
                .to_str()
                .ok_or("credential path is not UTF-8")?,
            "--sql-file",
            object_privilege_sql_path_text,
        ])
        .output()?;
    assert!(
        grant_all_existing_columns.status.success(),
        "table-wide SELECT GRANT SQL failed: {}",
        String::from_utf8_lossy(&grant_all_existing_columns.stderr)
    );
    let table_wide_grants =
        admin.list_grants_page(ddl_schema.identity_id, ddl_table.table_id, 128, None)?;
    assert!(ddl_table.column_ids.iter().all(|column_id| {
        table_wide_grants.records.iter().any(|grant| {
            grant.grantee_id == member_user.identity_id
                && grant.privilege
                    == prdb_engine::metadata_discovery::AdministrativeGrantPrivilege::Select
                && grant.target_scope
                    == prdb_engine::metadata_discovery::AdministrativeGrantTargetScope::Column
                && grant.target_id == *column_id
        })
    }));
    assert!(!table_wide_grants.records.iter().any(|grant| {
        grant.grantee_id == member_user.identity_id
            && grant.privilege
                == prdb_engine::metadata_discovery::AdministrativeGrantPrivilege::Select
            && grant.target_scope
                == prdb_engine::metadata_discovery::AdministrativeGrantTargetScope::Object
    }));
    let added_column = admin.add_column(
        ddl_table.table_id,
        &AdminColumnDefinition {
            name: "future_field".to_owned(),
            scalar_type: ScalarType::Integer,
            nullable: true,
            generated_uuid_identity: false,
            default: None,
        },
    )?;
    let after_column_add =
        admin.list_grants_page(ddl_schema.identity_id, ddl_table.table_id, 128, None)?;
    assert!(!after_column_add.records.iter().any(|grant| {
        grant.grantee_id == member_user.identity_id
            && grant.privilege
                == prdb_engine::metadata_discovery::AdministrativeGrantPrivilege::Select
            && grant.target_id == added_column.column_id
    }));
    let dropped_table = admin.drop_table(ddl_table.table_id)?;
    assert!(dropped_table.commit_sequence > ddl_constraint.commit_sequence);
    assert!(
        admin.drop_schema(ddl_schema.identity_id)?.commit_sequence > dropped_table.commit_sequence
    );
    let role_membership_address = address.to_string();
    let role_membership_credentials = admin_cancel_credentials_path
        .to_str()
        .ok_or("credential path is not UTF-8")?;
    let role_membership_sql_path = root.join("role-membership.sql");
    std::fs::write(
        &role_membership_sql_path,
        "GRANT process_admin_role_v2 TO USER process_member_user;",
    )?;
    let role_membership_sql_path_text = role_membership_sql_path
        .to_str()
        .ok_or("SQL path is not UTF-8")?;
    let grant_role_cli = Command::new(env!("CARGO_BIN_EXE_prdb-client"))
        .args([
            "admin-sql",
            "--address",
            &role_membership_address,
            "--credentials",
            role_membership_credentials,
            "--sql-file",
            role_membership_sql_path_text,
        ])
        .output()?;
    assert!(
        grant_role_cli.status.success(),
        "role-grant CLI failed: {}",
        String::from_utf8_lossy(&grant_role_cli.stderr)
    );
    assert!(String::from_utf8_lossy(&grant_role_cli.stdout).contains("granted role membership"));
    let member_cli_credentials = member_credentials_path
        .to_str()
        .ok_or("member credential path is not UTF-8")?;
    let delegated_role_create_cli = Command::new(env!("CARGO_BIN_EXE_prdb-client"))
        .args([
            "role-create",
            "--address",
            &role_membership_address,
            "--credentials",
            member_cli_credentials,
            "--name",
            "process_delegated_role",
        ])
        .output()?;
    assert!(
        delegated_role_create_cli.status.success(),
        "role-create with inherited MANAGE_ROLES failed: {}",
        String::from_utf8_lossy(&delegated_role_create_cli.stderr)
    );
    let delegated_role_output = String::from_utf8_lossy(&delegated_role_create_cli.stdout);
    assert!(delegated_role_output.contains("created role "));
    let delegated_role_id = delegated_role_output
        .split_whitespace()
        .nth(2)
        .ok_or("delegated role-create output omitted its ID")?;
    let delegated_role_id = decode_id_hex(delegated_role_id)
        .ok_or("delegated role-create output contained an invalid ID")?;
    assert!(
        matches!(
            admin.drop_role(created_role.identity_id),
            Err(
                prdb_engine::client_submit::ClientSubmitError::AdministrativeOperationRejected { .. }
            )
        ),
        "role drop did not return a rejected outcome while membership still referenced the role"
    );
    std::fs::write(
        &role_membership_sql_path,
        "REVOKE process_admin_role_v2 FROM USER process_member_user;",
    )?;
    let revoke_role_cli = Command::new(env!("CARGO_BIN_EXE_prdb-client"))
        .args([
            "admin-sql",
            "--address",
            &role_membership_address,
            "--credentials",
            role_membership_credentials,
            "--sql-file",
            role_membership_sql_path_text,
        ])
        .output()?;
    assert!(
        revoke_role_cli.status.success(),
        "role-revoke CLI failed: {}",
        String::from_utf8_lossy(&revoke_role_cli.stderr)
    );
    assert!(String::from_utf8_lossy(&revoke_role_cli.stdout).contains("revoked role membership"));
    let mut member_authentication_probe = read_client_credentials(&member_credentials_path)?;
    let authenticated_member = SubmissionClient::connect_authenticated(
        address,
        member_authentication_probe.token_id,
        &mut member_authentication_probe.token_secret,
    )?;
    drop(authenticated_member);
    let denied_role_create_cli = Command::new(env!("CARGO_BIN_EXE_prdb-client"))
        .args([
            "role-create",
            "--address",
            &role_membership_address,
            "--credentials",
            member_cli_credentials,
            "--name",
            "process_denied_role_after_membership_revoke",
        ])
        .output()?;
    assert!(
        !denied_role_create_cli.status.success(),
        "role-create remained authorized after the member's role was revoked"
    );
    std::fs::write(
        &system_privilege_sql_path,
        "REVOKE MANAGE_ROLES ON DATABASE FROM ROLE process_admin_role_v2;",
    )?;
    let revoke_privilege_cli = Command::new(env!("CARGO_BIN_EXE_prdb-client"))
        .args([
            "admin-sql",
            "--address",
            &privilege_cli_address,
            "--credentials",
            privilege_cli_credentials,
            "--sql-file",
            system_privilege_sql_path_text,
        ])
        .output()?;
    assert!(
        revoke_privilege_cli.status.success(),
        "system-privilege-revoke CLI failed: {}",
        String::from_utf8_lossy(&revoke_privilege_cli.stderr)
    );
    assert!(
        String::from_utf8_lossy(&revoke_privilege_cli.stdout).contains("revoked system privilege")
    );
    let admin_credentials = read_client_credentials(&admin_cancel_credentials_path)?;
    let mut reauthenticated_secret = admin_credentials.token_secret;
    admin = AdminClient::connect_authenticated(
        address,
        admin_credentials.token_id,
        &mut reauthenticated_secret,
    )?;
    assert_eq!(reauthenticated_secret, [0; 32]);
    let dropped = admin
        .drop_role(created_role.identity_id)
        .map_err(|error| format!("drop membership target role after revoke: {error:?}"))?;
    assert!(dropped.commit_sequence > revoked.commit_sequence);
    admin.drop_role(delegated_role_id)?;
    admin.drop_user(member_user.identity_id)?;
    let mut dropped_member_credentials = read_client_credentials(&member_credentials_path)?;
    assert!(
        SubmissionClient::connect_authenticated(
            address,
            dropped_member_credentials.token_id,
            &mut dropped_member_credentials.token_secret,
        )
        .is_err()
    );
    remove_client_credentials(&member_credentials_path)?;

    let created_schema = admin.create_schema("process_admin_schema", admin_user_id)?;
    let created_table = admin.create_table(
        created_schema.identity_id,
        "process_admin_table",
        &[AdminColumnDefinition {
            name: "payload".to_owned(),
            scalar_type: ScalarType::Text,
            nullable: true,
            generated_uuid_identity: false,
            default: None,
        }],
    )?;
    assert_ne!(created_table.table_id, [0; 16]);
    assert_eq!(created_table.column_ids.len(), 1);
    let add_column_sql_path = root.join("add-column.sql");
    std::fs::write(
        &add_column_sql_path,
        "ALTER TABLE process_admin_schema.process_admin_table ADD COLUMN IF NOT EXISTS label VARCHAR(30) NOT NULL DEFAULT 'unknown';",
    )?;
    let add_column_cli = Command::new(env!("CARGO_BIN_EXE_prdb-client"))
        .args([
            "admin-sql",
            "--address",
            &address.to_string(),
            "--credentials",
            admin_cancel_credentials_path
                .to_str()
                .ok_or("credential path is not UTF-8")?,
            "--sql-file",
            add_column_sql_path
                .to_str()
                .ok_or("SQL path is not UTF-8")?,
        ])
        .output()?;
    assert!(
        add_column_cli.status.success(),
        "admin-sql ADD COLUMN failed: {}",
        String::from_utf8_lossy(&add_column_cli.stderr)
    );
    assert!(String::from_utf8_lossy(&add_column_cli.stdout).contains("added column"));
    let added_columns =
        admin.list_columns_page(created_schema.identity_id, created_table.table_id, 16, None)?;
    let added_label = added_columns
        .records
        .iter()
        .find(|column| column.name == "label")
        .ok_or("committed ADD COLUMN SQL was absent from authenticated metadata")?;
    assert_eq!(added_label.ordinal, 1);
    assert_eq!(
        added_label.scalar_type,
        ScalarType::Varchar { max_chars: 30 }
    );
    assert!(!added_label.nullable);
    assert!(added_columns.next_cursor.is_none());
    for (suffix, sql, expected_nullable) in [
        (
            "set-not-null",
            "ALTER TABLE process_admin_schema.process_admin_table ALTER COLUMN payload SET NOT NULL;",
            false,
        ),
        (
            "drop-not-null",
            "ALTER TABLE process_admin_schema.process_admin_table ALTER COLUMN payload DROP NOT NULL;",
            true,
        ),
    ] {
        let sql_path = root.join(format!("{suffix}.sql"));
        std::fs::write(&sql_path, sql)?;
        let result = Command::new(env!("CARGO_BIN_EXE_prdb-client"))
            .args([
                "admin-sql",
                "--address",
                &address.to_string(),
                "--credentials",
                admin_cancel_credentials_path
                    .to_str()
                    .ok_or("credential path is not UTF-8")?,
                "--sql-file",
                sql_path.to_str().ok_or("SQL path is not UTF-8")?,
            ])
            .output()?;
        assert!(
            result.status.success(),
            "admin-sql {suffix} failed: {}",
            String::from_utf8_lossy(&result.stderr)
        );
        let columns = admin.list_columns_page(
            created_schema.identity_id,
            created_table.table_id,
            16,
            None,
        )?;
        let label = columns
            .records
            .iter()
            .find(|column| column.name == "payload")
            .ok_or("column nullability SQL removed its target column")?;
        assert_eq!(label.nullable, expected_nullable, "{suffix} metadata");
    }
    let add_column_if_missing_path = root.join("add-column-if-not-exists.sql");
    std::fs::write(
        &add_column_if_missing_path,
        "ALTER TABLE process_admin_schema.process_admin_table ADD COLUMN IF NOT EXISTS label TEXT;",
    )?;
    let add_column_if_missing = Command::new(env!("CARGO_BIN_EXE_prdb-client"))
        .args([
            "admin-sql",
            "--address",
            &address.to_string(),
            "--credentials",
            admin_cancel_credentials_path
                .to_str()
                .ok_or("credential path is not UTF-8")?,
            "--sql-file",
            add_column_if_missing_path
                .to_str()
                .ok_or("SQL path is not UTF-8")?,
        ])
        .output()?;
    assert!(
        add_column_if_missing.status.success(),
        "admin-sql conditional ADD COLUMN failed: {}",
        String::from_utf8_lossy(&add_column_if_missing.stderr)
    );
    assert!(
        String::from_utf8_lossy(&add_column_if_missing.stdout)
            .contains("column already exists; no operation committed")
    );
    let dependency_sql_path = root.join("column-dependency.sql");
    std::fs::write(
        &dependency_sql_path,
        "CREATE INDEX IF NOT EXISTS process_label_index ON process_admin_schema.process_admin_table (label);",
    )?;
    let create_dependency_index = Command::new(env!("CARGO_BIN_EXE_prdb-client"))
        .args([
            "admin-sql",
            "--address",
            &address.to_string(),
            "--credentials",
            admin_cancel_credentials_path
                .to_str()
                .ok_or("credential path is not UTF-8")?,
            "--sql-file",
            dependency_sql_path
                .to_str()
                .ok_or("SQL path is not UTF-8")?,
        ])
        .output()?;
    assert!(
        create_dependency_index.status.success(),
        "admin-sql CREATE INDEX failed: {}",
        String::from_utf8_lossy(&create_dependency_index.stderr)
    );
    let repeated_create_dependency_index = Command::new(env!("CARGO_BIN_EXE_prdb-client"))
        .args([
            "admin-sql",
            "--address",
            &address.to_string(),
            "--credentials",
            admin_cancel_credentials_path
                .to_str()
                .ok_or("credential path is not UTF-8")?,
            "--sql-file",
            dependency_sql_path
                .to_str()
                .ok_or("SQL path is not UTF-8")?,
        ])
        .output()?;
    assert!(
        repeated_create_dependency_index.status.success(),
        "admin-sql repeated CREATE INDEX IF NOT EXISTS failed: {}",
        String::from_utf8_lossy(&repeated_create_dependency_index.stderr)
    );
    assert!(
        String::from_utf8_lossy(&repeated_create_dependency_index.stdout)
            .contains("index already exists; no operation committed")
    );
    let widen_column_sql_path = root.join("widen-varchar-column.sql");
    std::fs::write(
        &widen_column_sql_path,
        "ALTER TABLE process_admin_schema.process_admin_table ALTER COLUMN label TYPE VARCHAR(60);",
    )?;
    let widen_column_cli = Command::new(env!("CARGO_BIN_EXE_prdb-client"))
        .args([
            "admin-sql",
            "--address",
            &address.to_string(),
            "--credentials",
            admin_cancel_credentials_path
                .to_str()
                .ok_or("credential path is not UTF-8")?,
            "--sql-file",
            widen_column_sql_path
                .to_str()
                .ok_or("SQL path is not UTF-8")?,
        ])
        .output()?;
    assert!(
        widen_column_cli.status.success(),
        "admin-sql VARCHAR widening failed: {}",
        String::from_utf8_lossy(&widen_column_cli.stderr)
    );
    assert!(
        String::from_utf8_lossy(&widen_column_cli.stdout)
            .contains("widened VARCHAR column label to 60 characters")
    );
    let widened_columns =
        admin.list_columns_page(created_schema.identity_id, created_table.table_id, 16, None)?;
    let widened_label = widened_columns
        .records
        .iter()
        .find(|column| column.name == "label")
        .ok_or("widened column missing from authenticated metadata")?;
    assert_eq!(widened_label.column_id, added_label.column_id);
    assert_eq!(
        widened_label.scalar_type,
        ScalarType::Varchar { max_chars: 60 }
    );
    let table_index_list = Command::new(env!("CARGO_BIN_EXE_prdb-client"))
        .args([
            "index-list",
            "--address",
            &address.to_string(),
            "--credentials",
            admin_cancel_credentials_path
                .to_str()
                .ok_or("credential path is not UTF-8")?,
            "--schema-id",
            &encode_id_hex(&created_schema.identity_id),
            "--table-id",
            &encode_id_hex(&created_table.table_id),
        ])
        .output()?;
    assert!(
        table_index_list.status.success(),
        "index-list failed after CREATE INDEX: {}",
        String::from_utf8_lossy(&table_index_list.stderr)
    );
    let table_index_json = String::from_utf8_lossy(&table_index_list.stdout);
    assert!(table_index_json.contains("\"name\":\"process_label_index\""));
    assert!(table_index_json.contains(&encode_id_hex(&added_label.column_id)));
    assert!(table_index_json.contains("\"unique\":false"));
    let truncate_sql_path = root.join("truncate-table.sql");
    std::fs::write(
        &truncate_sql_path,
        "TRUNCATE TABLE process_admin_schema.process_admin_table RESTRICT;",
    )?;
    let truncate_cli = Command::new(env!("CARGO_BIN_EXE_prdb-client"))
        .args([
            "admin-sql",
            "--address",
            &address.to_string(),
            "--credentials",
            admin_cancel_credentials_path
                .to_str()
                .ok_or("credential path is not UTF-8")?,
            "--sql-file",
            truncate_sql_path.to_str().ok_or("SQL path is not UTF-8")?,
        ])
        .output()?;
    assert!(
        truncate_cli.status.success(),
        "admin-sql TRUNCATE TABLE failed: {}",
        String::from_utf8_lossy(&truncate_cli.stderr)
    );
    assert!(String::from_utf8_lossy(&truncate_cli.stdout).contains("truncated table"));
    let retained_index_list = Command::new(env!("CARGO_BIN_EXE_prdb-client"))
        .args([
            "index-list",
            "--address",
            &address.to_string(),
            "--credentials",
            admin_cancel_credentials_path
                .to_str()
                .ok_or("credential path is not UTF-8")?,
            "--schema-id",
            &encode_id_hex(&created_schema.identity_id),
            "--table-id",
            &encode_id_hex(&created_table.table_id),
        ])
        .output()?;
    assert!(
        retained_index_list.status.success(),
        "index-list failed after TRUNCATE TABLE: {}",
        String::from_utf8_lossy(&retained_index_list.stderr)
    );
    assert!(
        String::from_utf8_lossy(&retained_index_list.stdout)
            .contains("\"name\":\"process_label_index\"")
    );
    let named_check_sql_path = root.join("add-named-check-constraint.sql");
    std::fs::write(
        &named_check_sql_path,
        "ALTER TABLE process_admin_schema.process_admin_table ADD CONSTRAINT process_label_present CHECK (label IS NOT NULL);",
    )?;
    let named_check_cli = Command::new(env!("CARGO_BIN_EXE_prdb-client"))
        .args([
            "admin-sql",
            "--address",
            &address.to_string(),
            "--credentials",
            admin_cancel_credentials_path
                .to_str()
                .ok_or("credential path is not UTF-8")?,
            "--sql-file",
            named_check_sql_path
                .to_str()
                .ok_or("SQL path is not UTF-8")?,
        ])
        .output()?;
    assert!(
        named_check_cli.status.success(),
        "admin-sql named CHECK creation failed: {}",
        String::from_utf8_lossy(&named_check_cli.stderr)
    );
    let listed_named_constraint = admin
        .list_constraints_page(created_schema.identity_id, created_table.table_id, 16, None)?
        .records
        .into_iter()
        .find(|constraint| constraint.name == "process_label_present")
        .ok_or("authenticated constraint metadata omitted the named CHECK")?;
    assert_eq!(
        listed_named_constraint.kind,
        prdb_engine::metadata_discovery::AdministrativeConstraintKind::Check
    );
    let drop_named_check_sql_path = root.join("drop-named-check-constraint.sql");
    std::fs::write(
        &drop_named_check_sql_path,
        "ALTER TABLE process_admin_schema.process_admin_table DROP CONSTRAINT process_label_present;",
    )?;
    let drop_named_check_cli = Command::new(env!("CARGO_BIN_EXE_prdb-client"))
        .args([
            "admin-sql",
            "--address",
            &address.to_string(),
            "--credentials",
            admin_cancel_credentials_path
                .to_str()
                .ok_or("credential path is not UTF-8")?,
            "--sql-file",
            drop_named_check_sql_path
                .to_str()
                .ok_or("SQL path is not UTF-8")?,
        ])
        .output()?;
    assert!(
        drop_named_check_cli.status.success(),
        "admin-sql named CHECK drop failed: {}",
        String::from_utf8_lossy(&drop_named_check_cli.stderr)
    );
    let drop_column_sql_path = root.join("drop-column.sql");
    std::fs::write(
        &drop_column_sql_path,
        "ALTER TABLE process_admin_schema.process_admin_table DROP COLUMN label RESTRICT;",
    )?;
    let blocked_column_drop = Command::new(env!("CARGO_BIN_EXE_prdb-client"))
        .args([
            "admin-sql",
            "--address",
            &address.to_string(),
            "--credentials",
            admin_cancel_credentials_path
                .to_str()
                .ok_or("credential path is not UTF-8")?,
            "--sql-file",
            drop_column_sql_path
                .to_str()
                .ok_or("SQL path is not UTF-8")?,
        ])
        .output()?;
    assert!(
        !blocked_column_drop.status.success(),
        "admin-sql DROP COLUMN ignored its index dependency"
    );
    let columns_after_blocked_drop =
        admin.list_columns_page(created_schema.identity_id, created_table.table_id, 16, None)?;
    assert_eq!(columns_after_blocked_drop.records.len(), 2);
    assert!(
        columns_after_blocked_drop
            .records
            .iter()
            .any(|column| column.name == "label")
    );
    std::fs::write(
        &dependency_sql_path,
        "DROP INDEX IF EXISTS process_admin_schema.process_label_index RESTRICT;",
    )?;
    let drop_dependency_index = Command::new(env!("CARGO_BIN_EXE_prdb-client"))
        .args([
            "admin-sql",
            "--address",
            &address.to_string(),
            "--credentials",
            admin_cancel_credentials_path
                .to_str()
                .ok_or("credential path is not UTF-8")?,
            "--sql-file",
            dependency_sql_path
                .to_str()
                .ok_or("SQL path is not UTF-8")?,
        ])
        .output()?;
    assert!(
        drop_dependency_index.status.success(),
        "admin-sql DROP INDEX failed: {}",
        String::from_utf8_lossy(&drop_dependency_index.stderr)
    );
    std::fs::write(
        &dependency_sql_path,
        "DROP INDEX IF EXISTS process_admin_schema.process_label_index RESTRICT;",
    )?;
    let repeated_drop_dependency_index = Command::new(env!("CARGO_BIN_EXE_prdb-client"))
        .args([
            "admin-sql",
            "--address",
            &address.to_string(),
            "--credentials",
            admin_cancel_credentials_path
                .to_str()
                .ok_or("credential path is not UTF-8")?,
            "--sql-file",
            dependency_sql_path
                .to_str()
                .ok_or("SQL path is not UTF-8")?,
        ])
        .output()?;
    assert!(
        repeated_drop_dependency_index.status.success(),
        "admin-sql repeated DROP INDEX IF EXISTS failed: {}",
        String::from_utf8_lossy(&repeated_drop_dependency_index.stderr)
    );
    assert!(
        String::from_utf8_lossy(&repeated_drop_dependency_index.stdout)
            .contains("index was absent; no operation committed")
    );
    let drop_column_cli = Command::new(env!("CARGO_BIN_EXE_prdb-client"))
        .args([
            "admin-sql",
            "--address",
            &address.to_string(),
            "--credentials",
            admin_cancel_credentials_path
                .to_str()
                .ok_or("credential path is not UTF-8")?,
            "--sql-file",
            drop_column_sql_path
                .to_str()
                .ok_or("SQL path is not UTF-8")?,
        ])
        .output()?;
    assert!(
        drop_column_cli.status.success(),
        "admin-sql DROP COLUMN failed: {}",
        String::from_utf8_lossy(&drop_column_cli.stderr)
    );
    assert!(String::from_utf8_lossy(&drop_column_cli.stdout).contains("dropped column"));
    let columns_after_drop =
        admin.list_columns_page(created_schema.identity_id, created_table.table_id, 16, None)?;
    assert_eq!(columns_after_drop.records.len(), 1);
    assert_eq!(columns_after_drop.records[0].name, "payload");
    assert!(columns_after_drop.next_cursor.is_none());
    let unique_constraint = admin
        .create_unique_constraint(created_table.table_id, &created_table.column_ids)
        .map_err(|error| format!("process ADMIN create_unique_constraint failed: {error}"))?;
    let transfer_owner = admin.create_user("process_table_owner")?;
    let token_expiration =
        i64::try_from(SystemTime::now().duration_since(UNIX_EPOCH)?.as_micros())?
            .saturating_add(900_000_000);
    let transfer_credentials = admin.issue_token(
        transfer_owner.identity_id,
        "table owner transfer",
        token_expiration,
    )?;
    let mut transfer_token_id = transfer_credentials.token_id;
    let transfer_credential_path = root.join("table-owner.cred");
    let transfer_client_credentials = ClientCredentials {
        token_id: transfer_token_id,
        token_secret: *transfer_credentials.token_secret(),
    };
    write_client_credentials(&transfer_credential_path, &transfer_client_credentials)?;
    drop(transfer_client_credentials);
    drop(transfer_credentials);
    let address_text = address.to_string();
    let admin_cli_credentials = admin_cancel_credentials_path
        .to_str()
        .ok_or("credential path is not UTF-8")?;
    let rename_sql_path = root.join("rename-user.sql");
    std::fs::write(
        &rename_sql_path,
        "ALTER USER process_table_owner RENAME TO process_table_owner_renamed;",
    )?;
    let rename_sql_path_text = rename_sql_path.to_str().ok_or("SQL path is not UTF-8")?;
    let rename_user_cli = Command::new(env!("CARGO_BIN_EXE_prdb-client"))
        .args([
            "admin-sql",
            "--address",
            &address_text,
            "--credentials",
            admin_cli_credentials,
            "--sql-file",
            rename_sql_path_text,
        ])
        .output()?;
    assert!(
        rename_user_cli.status.success(),
        "user-rename CLI failed: {}",
        String::from_utf8_lossy(&rename_user_cli.stderr)
    );
    assert!(String::from_utf8_lossy(&rename_user_cli.stdout).contains("renamed user"));
    let mut renamed_transfer_credentials = read_client_credentials(&transfer_credential_path)?;
    assert_eq!(renamed_transfer_credentials.token_id, transfer_token_id);
    let renamed_user_session = SubmissionClient::connect_authenticated(
        address,
        renamed_transfer_credentials.token_id,
        &mut renamed_transfer_credentials.token_secret,
    )?;
    assert_eq!(
        renamed_transfer_credentials.token_secret, [0; 32],
        "successful authentication must consume the local secret copy"
    );
    drop(renamed_user_session);
    let login_sql_path = root.join("login-state.sql");
    std::fs::write(
        &login_sql_path,
        "ALTER USER process_table_owner_renamed NOLOGIN;",
    )?;
    let login_sql_path_text = login_sql_path.to_str().ok_or("SQL path is not UTF-8")?;
    let disable_login_cli = Command::new(env!("CARGO_BIN_EXE_prdb-client"))
        .args([
            "admin-sql",
            "--address",
            &address_text,
            "--credentials",
            admin_cli_credentials,
            "--sql-file",
            login_sql_path_text,
        ])
        .output()?;
    assert!(
        disable_login_cli.status.success(),
        "user-login disable CLI failed: {}",
        String::from_utf8_lossy(&disable_login_cli.stderr)
    );
    assert!(String::from_utf8_lossy(&disable_login_cli.stdout).contains("set user NOLOGIN"));
    let mut revoked_transfer_credentials = read_client_credentials(&transfer_credential_path)?;
    let revoked_transfer_token_id = revoked_transfer_credentials.token_id;
    assert!(
        SubmissionClient::connect_authenticated(
            address,
            revoked_transfer_token_id,
            &mut revoked_transfer_credentials.token_secret,
        )
        .is_err()
    );
    std::fs::write(
        &login_sql_path,
        "ALTER USER process_table_owner_renamed LOGIN;",
    )?;
    let enable_login_cli = Command::new(env!("CARGO_BIN_EXE_prdb-client"))
        .args([
            "admin-sql",
            "--address",
            &address_text,
            "--credentials",
            admin_cli_credentials,
            "--sql-file",
            login_sql_path_text,
        ])
        .output()?;
    assert!(
        enable_login_cli.status.success(),
        "user-login enable CLI failed: {}",
        String::from_utf8_lossy(&enable_login_cli.stderr)
    );
    assert!(String::from_utf8_lossy(&enable_login_cli.stdout).contains("set user LOGIN"));
    let mut still_revoked_credentials = read_client_credentials(&transfer_credential_path)?;
    assert!(
        SubmissionClient::connect_authenticated(
            address,
            still_revoked_credentials.token_id,
            &mut still_revoked_credentials.token_secret,
        )
        .is_err()
    );
    let renewed_credentials = admin.issue_token(
        transfer_owner.identity_id,
        "table owner transfer renewed",
        token_expiration,
    )?;
    transfer_token_id = renewed_credentials.token_id;
    remove_client_credentials(&transfer_credential_path)?;
    let transfer_client_credentials = ClientCredentials {
        token_id: transfer_token_id,
        token_secret: *renewed_credentials.token_secret(),
    };
    write_client_credentials(&transfer_credential_path, &transfer_client_credentials)?;
    drop(transfer_client_credentials);
    drop(renewed_credentials);
    let admin_owner_id_text = encode_id_hex(&admin_user_id);
    admin.execute_object_privilege_sql(
        "GRANT ALTER ON TABLE process_admin_schema.process_admin_table TO USER process_table_owner_renamed",
    )?;
    let table_alter_sql_path = root.join("table-alter-grantee.sql");
    for (statement, expected) in [
        (
            "ALTER TABLE process_admin_schema.process_admin_table RENAME TO process_admin_table_altered;",
            "renamed table",
        ),
        (
            "ALTER TABLE process_admin_schema.process_admin_table_altered RENAME TO process_admin_table;",
            "renamed table",
        ),
    ] {
        std::fs::write(&table_alter_sql_path, statement)?;
        let command = Command::new(env!("CARGO_BIN_EXE_prdb-client"))
            .args([
                "admin-sql",
                "--address",
                &address_text,
                "--credentials",
                transfer_credential_path
                    .to_str()
                    .ok_or("credential path is not UTF-8")?,
                "--sql-file",
                table_alter_sql_path
                    .to_str()
                    .ok_or("SQL path is not UTF-8")?,
            ])
            .output()?;
        assert!(
            command.status.success(),
            "table ALTER grantee SQL {statement:?} failed: {}",
            String::from_utf8_lossy(&command.stderr)
        );
        assert!(String::from_utf8_lossy(&command.stdout).contains(expected));
    }
    let table_alter_credentials = read_client_credentials(&transfer_credential_path)?;
    let mut table_alter_secret = table_alter_credentials.token_secret;
    let mut table_alter_client = AdminClient::connect_authenticated(
        address,
        table_alter_credentials.token_id,
        &mut table_alter_secret,
    )?;
    table_alter_client.set_table_owner(created_table.table_id, transfer_owner.identity_id)?;
    drop(table_alter_client);
    admin.set_table_owner(created_table.table_id, admin_user_id)?;
    admin.execute_object_privilege_sql(
        "REVOKE ALTER ON TABLE process_admin_schema.process_admin_table FROM USER process_table_owner_renamed",
    )?;
    let create_alter_drop_sql_path = root.join("create-alter-drop-target.sql");
    std::fs::write(
        &create_alter_drop_sql_path,
        "CREATE TABLE IF NOT EXISTS process_admin_schema.process_admin_drop_target (value INTEGER);",
    )?;
    let create_alter_drop_target = Command::new(env!("CARGO_BIN_EXE_prdb-client"))
        .args([
            "admin-sql",
            "--address",
            &address_text,
            "--credentials",
            admin_cli_credentials,
            "--sql-file",
            create_alter_drop_sql_path
                .to_str()
                .ok_or("SQL path is not UTF-8")?,
        ])
        .output()?;
    assert!(
        create_alter_drop_target.status.success(),
        "ALTER drop target CREATE TABLE failed: {}",
        String::from_utf8_lossy(&create_alter_drop_target.stderr)
    );
    let repeated_create_alter_drop_target = Command::new(env!("CARGO_BIN_EXE_prdb-client"))
        .args([
            "admin-sql",
            "--address",
            &address_text,
            "--credentials",
            admin_cli_credentials,
            "--sql-file",
            create_alter_drop_sql_path
                .to_str()
                .ok_or("SQL path is not UTF-8")?,
        ])
        .output()?;
    assert!(
        repeated_create_alter_drop_target.status.success(),
        "administrator CREATE TABLE IF NOT EXISTS repeat failed: {}",
        String::from_utf8_lossy(&repeated_create_alter_drop_target.stderr)
    );
    assert!(
        String::from_utf8_lossy(&repeated_create_alter_drop_target.stdout)
            .contains("table already exists; no operation committed")
    );
    admin.execute_object_privilege_sql(
        "GRANT ALTER ON TABLE process_admin_schema.process_admin_drop_target TO USER process_table_owner_renamed",
    )?;
    let non_admin_create_existing_table = Command::new(env!("CARGO_BIN_EXE_prdb-client"))
        .args([
            "admin-sql",
            "--address",
            &address_text,
            "--credentials",
            transfer_credential_path
                .to_str()
                .ok_or("credential path is not UTF-8")?,
            "--sql-file",
            create_alter_drop_sql_path
                .to_str()
                .ok_or("SQL path is not UTF-8")?,
        ])
        .output()?;
    assert!(
        !non_admin_create_existing_table.status.success(),
        "non-administrator CREATE TABLE IF NOT EXISTS suppressed an existing target"
    );
    std::fs::write(
        &table_alter_sql_path,
        "DROP TABLE process_admin_schema.process_admin_drop_target RESTRICT;",
    )?;
    let drop_table_alter_grantee = Command::new(env!("CARGO_BIN_EXE_prdb-client"))
        .args([
            "admin-sql",
            "--address",
            &address_text,
            "--credentials",
            transfer_credential_path
                .to_str()
                .ok_or("credential path is not UTF-8")?,
            "--sql-file",
            table_alter_sql_path
                .to_str()
                .ok_or("SQL path is not UTF-8")?,
        ])
        .output()?;
    assert!(
        drop_table_alter_grantee.status.success(),
        "table ALTER grantee DROP TABLE failed: {}",
        String::from_utf8_lossy(&drop_table_alter_grantee.stderr)
    );
    assert!(String::from_utf8_lossy(&drop_table_alter_grantee.stdout).contains("dropped table"));
    admin.execute_object_privilege_sql(
        "GRANT ALTER ON SCHEMA process_admin_schema TO USER process_table_owner_renamed",
    )?;
    let schema_alter_sql_path = root.join("schema-alter-grantee.sql");
    for (old_name, new_name) in [
        ("process_admin_schema", "process_admin_schema_renamed"),
        ("process_admin_schema_renamed", "process_admin_schema"),
    ] {
        std::fs::write(
            &schema_alter_sql_path,
            format!("ALTER SCHEMA {old_name} RENAME TO {new_name};"),
        )?;
        let command = Command::new(env!("CARGO_BIN_EXE_prdb-client"))
            .args([
                "admin-sql",
                "--address",
                &address_text,
                "--credentials",
                transfer_credential_path
                    .to_str()
                    .ok_or("credential path is not UTF-8")?,
                "--sql-file",
                schema_alter_sql_path
                    .to_str()
                    .ok_or("SQL path is not UTF-8")?,
            ])
            .output()?;
        assert!(
            command.status.success(),
            "schema ALTER grantee SQL failed: {}",
            String::from_utf8_lossy(&command.stderr)
        );
        assert!(String::from_utf8_lossy(&command.stdout).contains("renamed schema"));
    }
    admin.execute_object_privilege_sql(
        "REVOKE ALTER ON SCHEMA process_admin_schema FROM USER process_table_owner_renamed",
    )?;
    admin.set_schema_owner(created_schema.identity_id, transfer_owner.identity_id)?;
    let schema_id_text = encode_id_hex(&created_schema.identity_id);
    let schema_transfer_command = Command::new(env!("CARGO_BIN_EXE_prdb-client"))
        .args([
            "schema-owner-set",
            "--address",
            &address_text,
            "--credentials",
            transfer_credential_path
                .to_str()
                .ok_or("credential path is not UTF-8")?,
            "--schema-id",
            &schema_id_text,
            "--owner-id",
            &admin_owner_id_text,
        ])
        .output()?;
    assert!(
        schema_transfer_command.status.success(),
        "schema-owner CLI failed: {}",
        String::from_utf8_lossy(&schema_transfer_command.stderr)
    );
    assert!(
        String::from_utf8_lossy(&schema_transfer_command.stdout).contains("changed schema owner")
    );
    let schema_owner_sql_path = root.join("schema-owner.sql");
    admin.execute_object_privilege_sql(
        "GRANT ALTER ON SCHEMA process_admin_schema TO USER process_table_owner_renamed",
    )?;
    std::fs::write(
        &schema_owner_sql_path,
        "ALTER SCHEMA process_admin_schema OWNER TO process_table_owner_renamed;",
    )?;
    let schema_owner_sql_command = Command::new(env!("CARGO_BIN_EXE_prdb-client"))
        .args([
            "admin-sql",
            "--address",
            &address_text,
            "--credentials",
            transfer_credential_path
                .to_str()
                .ok_or("credential path is not UTF-8")?,
            "--sql-file",
            schema_owner_sql_path
                .to_str()
                .ok_or("SQL path is not UTF-8")?,
        ])
        .output()?;
    assert!(
        schema_owner_sql_command.status.success(),
        "schema owner SQL failed: {}",
        String::from_utf8_lossy(&schema_owner_sql_command.stderr)
    );
    assert!(
        String::from_utf8_lossy(&schema_owner_sql_command.stdout).contains("changed schema owner")
    );
    admin.execute_object_privilege_sql(
        "REVOKE ALTER ON SCHEMA process_admin_schema FROM USER process_table_owner_renamed",
    )?;
    std::fs::write(
        &schema_owner_sql_path,
        "CREATE SCHEMA IF NOT EXISTS process_sql_owned AUTHORIZATION process_table_owner_renamed;",
    )?;
    let create_owned_schema_sql = Command::new(env!("CARGO_BIN_EXE_prdb-client"))
        .args([
            "admin-sql",
            "--address",
            &address_text,
            "--credentials",
            admin_cli_credentials,
            "--sql-file",
            schema_owner_sql_path
                .to_str()
                .ok_or("SQL path is not UTF-8")?,
        ])
        .output()?;
    assert!(
        create_owned_schema_sql.status.success(),
        "CREATE SCHEMA owner-name SQL failed: {}",
        String::from_utf8_lossy(&create_owned_schema_sql.stderr)
    );
    assert!(String::from_utf8_lossy(&create_owned_schema_sql.stdout).contains("created schema"));
    let repeated_create_owned_schema_sql = Command::new(env!("CARGO_BIN_EXE_prdb-client"))
        .args([
            "admin-sql",
            "--address",
            &address_text,
            "--credentials",
            admin_cli_credentials,
            "--sql-file",
            schema_owner_sql_path
                .to_str()
                .ok_or("SQL path is not UTF-8")?,
        ])
        .output()?;
    assert!(
        repeated_create_owned_schema_sql.status.success(),
        "CREATE SCHEMA IF NOT EXISTS repeat failed: {}",
        String::from_utf8_lossy(&repeated_create_owned_schema_sql.stderr)
    );
    assert!(
        String::from_utf8_lossy(&repeated_create_owned_schema_sql.stdout)
            .contains("schema already exists; no operation committed")
    );
    let non_admin_create_existing_schema = Command::new(env!("CARGO_BIN_EXE_prdb-client"))
        .args([
            "admin-sql",
            "--address",
            &address_text,
            "--credentials",
            transfer_credential_path
                .to_str()
                .ok_or("credential path is not UTF-8")?,
            "--sql-file",
            schema_owner_sql_path
                .to_str()
                .ok_or("SQL path is not UTF-8")?,
        ])
        .output()?;
    assert!(
        !non_admin_create_existing_schema.status.success(),
        "non-administrator CREATE SCHEMA IF NOT EXISTS suppressed an existing target"
    );
    std::fs::write(
        &schema_owner_sql_path,
        "DROP SCHEMA process_sql_owned RESTRICT;",
    )?;
    let drop_owned_schema_sql = Command::new(env!("CARGO_BIN_EXE_prdb-client"))
        .args([
            "admin-sql",
            "--address",
            &address_text,
            "--credentials",
            admin_cli_credentials,
            "--sql-file",
            schema_owner_sql_path
                .to_str()
                .ok_or("SQL path is not UTF-8")?,
        ])
        .output()?;
    assert!(
        drop_owned_schema_sql.status.success(),
        "DROP SCHEMA SQL cleanup failed: {}",
        String::from_utf8_lossy(&drop_owned_schema_sql.stderr)
    );
    assert!(String::from_utf8_lossy(&drop_owned_schema_sql.stdout).contains("dropped schema"));
    std::fs::write(&schema_owner_sql_path, "CREATE SCHEMA process_sql_default;")?;
    let create_default_owned_schema_sql = Command::new(env!("CARGO_BIN_EXE_prdb-client"))
        .args([
            "admin-sql",
            "--address",
            &address_text,
            "--credentials",
            admin_cli_credentials,
            "--sql-file",
            schema_owner_sql_path
                .to_str()
                .ok_or("SQL path is not UTF-8")?,
        ])
        .output()?;
    assert!(
        create_default_owned_schema_sql.status.success(),
        "CREATE SCHEMA current-user SQL failed: {}",
        String::from_utf8_lossy(&create_default_owned_schema_sql.stderr)
    );
    std::fs::write(
        &schema_owner_sql_path,
        "DROP SCHEMA process_sql_default RESTRICT;",
    )?;
    let drop_default_owned_schema_sql = Command::new(env!("CARGO_BIN_EXE_prdb-client"))
        .args([
            "admin-sql",
            "--address",
            &address_text,
            "--credentials",
            admin_cli_credentials,
            "--sql-file",
            schema_owner_sql_path
                .to_str()
                .ok_or("SQL path is not UTF-8")?,
        ])
        .output()?;
    assert!(
        drop_default_owned_schema_sql.status.success(),
        "DROP SCHEMA current-user cleanup failed: {}",
        String::from_utf8_lossy(&drop_default_owned_schema_sql.stderr)
    );
    admin.set_schema_owner(created_schema.identity_id, admin_user_id)?;
    let transferred = admin.set_table_owner(created_table.table_id, transfer_owner.identity_id)?;
    assert!(transferred.commit_sequence > 0);
    let table_id_text = encode_id_hex(&created_table.table_id);
    let credential_path_text = transfer_credential_path
        .to_str()
        .ok_or("credential path is not UTF-8")?;
    let transfer_command = Command::new(env!("CARGO_BIN_EXE_prdb-client"))
        .args([
            "table-owner-set",
            "--address",
            &address_text,
            "--credentials",
            credential_path_text,
            "--table-id",
            &table_id_text,
            "--owner-id",
            &admin_owner_id_text,
        ])
        .output()?;
    assert!(
        transfer_command.status.success(),
        "table-owner CLI failed: {}",
        String::from_utf8_lossy(&transfer_command.stderr)
    );
    assert!(String::from_utf8_lossy(&transfer_command.stdout).contains("changed table owner"));
    let table_owner_sql_path = root.join("table-owner.sql");
    admin.execute_object_privilege_sql(
        "GRANT ALTER ON TABLE process_admin_schema.process_admin_table TO USER process_table_owner_renamed",
    )?;
    std::fs::write(
        &table_owner_sql_path,
        "ALTER TABLE process_admin_schema.process_admin_table OWNER TO USER process_table_owner_renamed;",
    )?;
    let table_owner_sql_command = Command::new(env!("CARGO_BIN_EXE_prdb-client"))
        .args([
            "admin-sql",
            "--address",
            &address_text,
            "--credentials",
            credential_path_text,
            "--sql-file",
            table_owner_sql_path
                .to_str()
                .ok_or("SQL path is not UTF-8")?,
        ])
        .output()?;
    assert!(
        table_owner_sql_command.status.success(),
        "table owner SQL failed: {}",
        String::from_utf8_lossy(&table_owner_sql_command.stderr)
    );
    assert!(
        String::from_utf8_lossy(&table_owner_sql_command.stdout).contains("changed table owner")
    );
    admin.execute_object_privilege_sql(
        "REVOKE ALTER ON TABLE process_admin_schema.process_admin_table FROM USER process_table_owner_renamed",
    )?;
    let mut table_owner_credentials = read_client_credentials(&transfer_credential_path)?;
    let mut table_owner_client = AdminClient::connect_authenticated(
        address,
        table_owner_credentials.token_id,
        &mut table_owner_credentials.token_secret,
    )?;
    let table_owner_constraints = table_owner_client.list_constraints_page(
        created_schema.identity_id,
        created_table.table_id,
        128,
        None,
    )?;
    assert!(table_owner_constraints.records.iter().any(|constraint| {
        constraint.kind == prdb_engine::metadata_discovery::AdministrativeConstraintKind::Unique
    }));
    let owner_alter_sql_path = root.join("table-owner-add-column.sql");
    std::fs::write(
        &owner_alter_sql_path,
        "ALTER TABLE process_admin_schema.process_admin_table ADD COLUMN owner_added INTEGER;",
    )?;
    let owner_alter_sql = Command::new(env!("CARGO_BIN_EXE_prdb-client"))
        .args([
            "admin-sql",
            "--address",
            &address_text,
            "--credentials",
            credential_path_text,
            "--sql-file",
            owner_alter_sql_path
                .to_str()
                .ok_or("SQL path is not UTF-8")?,
        ])
        .output()?;
    assert!(
        owner_alter_sql.status.success(),
        "table-owner ALTER SQL failed: {}",
        String::from_utf8_lossy(&owner_alter_sql.stderr)
    );
    assert!(String::from_utf8_lossy(&owner_alter_sql.stdout).contains("added column"));
    drop(table_owner_client);
    admin.set_table_owner(created_table.table_id, admin_user_id)?;
    admin.grant_object_privilege(
        transfer_owner.identity_id,
        created_table.table_id,
        AdminObjectPrivilege::Alter,
        AdminObjectGrantTarget::Object,
    )?;
    let grantee_alter_sql_path = root.join("alter-grantee-add-column.sql");
    std::fs::write(
        &grantee_alter_sql_path,
        "ALTER TABLE process_admin_schema.process_admin_table ADD COLUMN grantee_added INTEGER;",
    )?;
    let grantee_alter_sql = Command::new(env!("CARGO_BIN_EXE_prdb-client"))
        .args([
            "admin-sql",
            "--address",
            &address_text,
            "--credentials",
            credential_path_text,
            "--sql-file",
            grantee_alter_sql_path
                .to_str()
                .ok_or("SQL path is not UTF-8")?,
        ])
        .output()?;
    assert!(
        grantee_alter_sql.status.success(),
        "ALTER-grantee SQL failed: {}",
        String::from_utf8_lossy(&grantee_alter_sql.stderr)
    );
    assert!(String::from_utf8_lossy(&grantee_alter_sql.stdout).contains("added column"));
    admin
        .grant_object_privilege(
            transfer_owner.identity_id,
            created_schema.identity_id,
            AdminObjectPrivilege::Create,
            AdminObjectGrantTarget::Object,
        )
        .map_err(|error| format!("schema CREATE grant failed: {error:?}"))?;
    let schema_create_sql_path = root.join("create-table-with-schema-grant.sql");
    std::fs::write(
        &schema_create_sql_path,
        "CREATE TABLE process_admin_schema.process_created_by_grantee (payload TEXT);",
    )?;
    let schema_create_sql = Command::new(env!("CARGO_BIN_EXE_prdb-client"))
        .args([
            "admin-sql",
            "--address",
            &address_text,
            "--credentials",
            credential_path_text,
            "--sql-file",
            schema_create_sql_path
                .to_str()
                .ok_or("SQL path is not UTF-8")?,
        ])
        .output()?;
    assert!(
        schema_create_sql.status.success(),
        "CREATE-grantee SQL failed without schema USAGE: {}",
        String::from_utf8_lossy(&schema_create_sql.stderr)
    );
    assert!(String::from_utf8_lossy(&schema_create_sql.stdout).contains("created table"));
    let denied_constraint_noop_sql_path = root.join("alter-grantee-missing-constraint.sql");
    std::fs::write(
        &denied_constraint_noop_sql_path,
        "ALTER TABLE process_admin_schema.process_admin_table DROP CONSTRAINT IF EXISTS '00000000-0000-0000-0000-000000000001';",
    )?;
    let denied_constraint_noop = Command::new(env!("CARGO_BIN_EXE_prdb-client"))
        .args([
            "admin-sql",
            "--address",
            &address_text,
            "--credentials",
            credential_path_text,
            "--sql-file",
            denied_constraint_noop_sql_path
                .to_str()
                .ok_or("SQL path is not UTF-8")?,
        ])
        .output()?;
    assert!(
        !denied_constraint_noop.status.success(),
        "ALTER grantee received an absent-constraint no-op without database-admin authority"
    );
    let added_column = admin.add_column(
        created_table.table_id,
        &AdminColumnDefinition {
            name: "extra".to_owned(),
            scalar_type: ScalarType::Integer,
            nullable: true,
            generated_uuid_identity: false,
            default: None,
        },
    )?;
    let source_table = admin.create_table(
        created_schema.identity_id,
        "process_admin_source",
        &[AdminColumnDefinition {
            name: "payload".to_owned(),
            scalar_type: ScalarType::Text,
            nullable: true,
            generated_uuid_identity: false,
            default: None,
        }],
    )?;
    let select_grant = admin.grant_object_privilege(
        admin_user_id,
        created_table.table_id,
        AdminObjectPrivilege::Select,
        AdminObjectGrantTarget::Column(created_table.column_ids[0]),
    )?;
    let references_grant = admin.grant_object_privilege(
        admin_user_id,
        created_table.table_id,
        AdminObjectPrivilege::References,
        AdminObjectGrantTarget::Column(created_table.column_ids[0]),
    )?;
    let inline_reference_table = admin.execute_create_table_sql(
        "CREATE TABLE process_admin_schema.process_admin_inline_source (payload TEXT REFERENCES process_admin_schema.process_admin_table (payload))",
    )?;
    assert_ne!(inline_reference_table.table_id, [0; 16]);
    assert_eq!(inline_reference_table.column_ids.len(), 1);
    let inline_reference_constraints = admin.list_constraints_page(
        created_schema.identity_id,
        inline_reference_table.table_id,
        128,
        None,
    )?;
    let inline_reference_fk = inline_reference_constraints
        .records
        .iter()
        .find(|constraint| {
            constraint.kind
                == prdb_engine::metadata_discovery::AdministrativeConstraintKind::ForeignKey
        })
        .ok_or("inline REFERENCES did not publish a foreign key")?;
    assert_eq!(
        inline_reference_fk.column_ids,
        inline_reference_table.column_ids
    );
    assert_eq!(
        inline_reference_fk.referenced_table_id,
        Some(created_table.table_id)
    );
    assert_eq!(
        inline_reference_fk.referenced_column_ids,
        created_table.column_ids
    );
    let administrative_grants = admin.list_grants_page(
        created_schema.identity_id,
        created_table.table_id,
        128,
        None,
    )?;
    assert!(administrative_grants.records.iter().any(|grant| {
        grant.grantee_id == admin_user_id
            && grant.grantor_id == admin_user_id
            && grant.privilege
                == prdb_engine::metadata_discovery::AdministrativeGrantPrivilege::Select
            && grant.target_scope
                == prdb_engine::metadata_discovery::AdministrativeGrantTargetScope::Column
            && grant.target_id == created_table.column_ids[0]
    }));
    assert!(administrative_grants.records.iter().any(|grant| {
        grant.grantee_id == admin_user_id
            && grant.privilege
                == prdb_engine::metadata_discovery::AdministrativeGrantPrivilege::References
    }));
    let foreign_key_cli_address = address.to_string();
    let foreign_key_cli_credentials = admin_cancel_credentials_path
        .to_str()
        .ok_or("credential path is not UTF-8")?;
    let add_foreign_key_sql_path = root.join("add-foreign-key.sql");
    std::fs::write(
        &add_foreign_key_sql_path,
        "ALTER TABLE process_admin_schema.process_admin_source ADD FOREIGN KEY (payload) REFERENCES process_admin_schema.process_admin_table (payload);",
    )?;
    let create_foreign_key_cli = Command::new(env!("CARGO_BIN_EXE_prdb-client"))
        .args([
            "admin-sql",
            "--address",
            &foreign_key_cli_address,
            "--credentials",
            foreign_key_cli_credentials,
            "--sql-file",
            add_foreign_key_sql_path
                .to_str()
                .ok_or("SQL path is not UTF-8")?,
        ])
        .output()?;
    assert!(
        create_foreign_key_cli.status.success(),
        "admin-sql ADD FOREIGN KEY failed: {}",
        String::from_utf8_lossy(&create_foreign_key_cli.stderr)
    );
    let foreign_key_cli_output = String::from_utf8_lossy(&create_foreign_key_cli.stdout);
    assert!(foreign_key_cli_output.contains("created FOREIGN KEY "));
    let foreign_key_cli_id = foreign_key_cli_output
        .split_whitespace()
        .nth(3)
        .ok_or("admin-sql ADD FOREIGN KEY output omitted the generated ID")?;
    let foreign_key_id = decode_id_hex(foreign_key_cli_id)
        .ok_or("admin-sql ADD FOREIGN KEY output contained an invalid ID")?;
    let listed_source_constraints = admin.list_constraints_page(
        created_schema.identity_id,
        source_table.table_id,
        128,
        None,
    )?;
    let listed_foreign_key = listed_source_constraints
        .records
        .iter()
        .find(|constraint| constraint.constraint_id == foreign_key_id)
        .ok_or("administrative constraint page omitted the foreign key")?;
    assert_eq!(
        listed_foreign_key.kind,
        prdb_engine::metadata_discovery::AdministrativeConstraintKind::ForeignKey
    );
    assert_eq!(
        listed_foreign_key.column_ids,
        vec![source_table.column_ids[0]]
    );
    assert_eq!(
        listed_foreign_key.referenced_table_id,
        Some(created_table.table_id)
    );
    assert_eq!(
        listed_foreign_key.referenced_column_ids,
        created_table.column_ids
    );
    let hidden_target_schema = admin.create_schema("process_hidden_fk_target", admin_user_id)?;
    let hidden_target_table = admin.create_table(
        hidden_target_schema.identity_id,
        "target",
        &[AdminColumnDefinition {
            name: "payload".to_owned(),
            scalar_type: ScalarType::Text,
            nullable: true,
            generated_uuid_identity: false,
            default: None,
        }],
    )?;
    let hidden_target_unique = admin.create_unique_constraint(
        hidden_target_table.table_id,
        &hidden_target_table.column_ids,
    )?;
    admin.grant_object_privilege(
        admin_user_id,
        hidden_target_table.table_id,
        AdminObjectPrivilege::References,
        AdminObjectGrantTarget::Column(hidden_target_table.column_ids[0]),
    )?;
    let redacted_source_schema =
        admin.create_schema("process_redacted_fk_source", transfer_owner.identity_id)?;
    let redacted_source_table = admin.create_table(
        redacted_source_schema.identity_id,
        "process_admin_redacted_source",
        &[AdminColumnDefinition {
            name: "payload".to_owned(),
            scalar_type: ScalarType::Text,
            nullable: true,
            generated_uuid_identity: false,
            default: None,
        }],
    )?;
    let redacted_foreign_key = admin.create_foreign_key(
        redacted_source_table.table_id,
        &redacted_source_table.column_ids,
        hidden_target_unique.constraint_id,
    )?;
    let second_redacted_foreign_key = admin.create_foreign_key(
        redacted_source_table.table_id,
        &redacted_source_table.column_ids,
        hidden_target_unique.constraint_id,
    )?;
    let dependency_list = Command::new(env!("CARGO_BIN_EXE_prdb-client"))
        .args([
            "dependency-list",
            "--address",
            &address.to_string(),
            "--credentials",
            admin_cancel_credentials_path
                .to_str()
                .ok_or("credential path is not UTF-8")?,
            "--object-id",
            &encode_id_hex(&redacted_foreign_key.foreign_key_id),
        ])
        .output()?;
    assert!(
        dependency_list.status.success(),
        "dependency-list CLI failed: {}",
        String::from_utf8_lossy(&dependency_list.stderr)
    );
    assert!(String::from_utf8(dependency_list.stdout)?.contains("foreign_key_target"));
    let administrator_dependencies =
        admin.list_dependencies_page(redacted_foreign_key.foreign_key_id, 128, None)?;
    assert!(
        administrator_dependencies.records.iter().any(|edge| {
            edge.dependent_id == redacted_foreign_key.foreign_key_id
            && edge.referenced_id == hidden_target_unique.constraint_id
            && edge.kind
                == prdb_engine::metadata_discovery::AdministrativeDependencyKind::ForeignKeyTarget
        }),
        "administrator dependency page: {:?}",
        administrator_dependencies.records
    );
    let redacted_owner_credentials = read_client_credentials(&transfer_credential_path)?;
    let mut redacted_owner_secret = redacted_owner_credentials.token_secret;
    let mut redacted_owner = AdminClient::connect_authenticated(
        address,
        redacted_owner_credentials.token_id,
        &mut redacted_owner_secret,
    )?;
    drop(redacted_owner_credentials);
    let redacted_constraints = redacted_owner.list_constraints_page(
        redacted_source_schema.identity_id,
        redacted_source_table.table_id,
        1,
        None,
    )?;
    assert_eq!(redacted_constraints.records.len(), 1);
    let redacted_constraint_cursor = redacted_constraints
        .next_cursor
        .ok_or("constraint metadata omitted a continuation for a second constraint")?;
    let redacted_foreign_key_page = redacted_constraints
        .records
        .iter()
        .find(|constraint| {
            constraint.constraint_id == redacted_foreign_key.foreign_key_id
                || constraint.constraint_id == second_redacted_foreign_key.foreign_key_id
        })
        .ok_or("owner constraint page omitted both foreign keys")?;
    let redacted_foreign_key = redacted_foreign_key_page.clone();
    assert_eq!(redacted_foreign_key_page.referenced_table_id, None);
    assert!(redacted_foreign_key_page.referenced_column_ids.is_empty());
    let redacted_dependencies = redacted_owner.list_dependencies_page(
        redacted_foreign_key_page.constraint_id,
        128,
        None,
    )?;
    assert!(
        redacted_dependencies.records.is_empty(),
        "dependency metadata must omit edges whose target is hidden"
    );
    assert!(
        redacted_dependencies.next_cursor.is_none(),
        "an omitted hidden edge must not affect continuation or counts"
    );
    admin.set_schema_owner(redacted_source_schema.identity_id, admin_user_id)?;
    assert!(
        redacted_owner
            .list_constraints_page(
                redacted_source_schema.identity_id,
                redacted_source_table.table_id,
                1,
                Some(&redacted_constraint_cursor),
            )
            .is_err(),
        "a constraint cursor must be reauthorized after schema ownership transfer"
    );
    drop(redacted_owner);
    let constraint_uuid = format!(
        "{}-{}-{}-{}-{}",
        &foreign_key_cli_id[0..8],
        &foreign_key_cli_id[8..12],
        &foreign_key_cli_id[12..16],
        &foreign_key_cli_id[16..20],
        &foreign_key_cli_id[20..32]
    );
    let drop_constraint_sql_path = root.join("drop-foreign-key-constraint.sql");
    std::fs::write(
        &drop_constraint_sql_path,
        format!(
            "ALTER TABLE process_admin_schema.process_admin_source DROP CONSTRAINT IF EXISTS '{constraint_uuid}';"
        ),
    )?;
    let drop_foreign_key_cli = Command::new(env!("CARGO_BIN_EXE_prdb-client"))
        .args([
            "admin-sql",
            "--address",
            &foreign_key_cli_address,
            "--credentials",
            foreign_key_cli_credentials,
            "--sql-file",
            drop_constraint_sql_path
                .to_str()
                .ok_or("SQL path is not UTF-8")?,
        ])
        .output()?;
    assert!(
        drop_foreign_key_cli.status.success(),
        "admin-sql DROP CONSTRAINT failed: {}",
        String::from_utf8_lossy(&drop_foreign_key_cli.stderr)
    );
    assert!(String::from_utf8_lossy(&drop_foreign_key_cli.stdout).contains("dropped constraint"));
    let repeated_drop_foreign_key_cli = Command::new(env!("CARGO_BIN_EXE_prdb-client"))
        .args([
            "admin-sql",
            "--address",
            &foreign_key_cli_address,
            "--credentials",
            foreign_key_cli_credentials,
            "--sql-file",
            drop_constraint_sql_path
                .to_str()
                .ok_or("SQL path is not UTF-8")?,
        ])
        .output()?;
    assert!(
        repeated_drop_foreign_key_cli.status.success(),
        "admin-sql conditional DROP CONSTRAINT no-op failed: {}",
        String::from_utf8_lossy(&repeated_drop_foreign_key_cli.stderr)
    );
    assert!(
        String::from_utf8_lossy(&repeated_drop_foreign_key_cli.stdout)
            .contains("constraint did not exist")
    );
    let revoked_references = admin.revoke_object_privilege(
        admin_user_id,
        created_table.table_id,
        AdminObjectPrivilege::References,
        AdminObjectGrantTarget::Column(created_table.column_ids[0]),
    )?;
    assert!(revoked_references.commit_sequence > references_grant.commit_sequence);
    let revoked_select = admin.revoke_object_privilege(
        admin_user_id,
        created_table.table_id,
        AdminObjectPrivilege::Select,
        AdminObjectGrantTarget::Column(created_table.column_ids[0]),
    )?;
    assert!(revoked_select.commit_sequence > select_grant.commit_sequence);
    let object_grant_address = address.to_string();
    let object_grant_credentials = admin_cancel_credentials_path
        .to_str()
        .ok_or("credential path is not UTF-8")?;
    let object_grant_grantee = encode_id_hex(&transfer_owner.identity_id);
    let object_grant_table = encode_id_hex(&created_table.table_id);
    let object_grant_column = encode_id_hex(&created_table.column_ids[0]);
    let grant_object_cli = Command::new(env!("CARGO_BIN_EXE_prdb-client"))
        .args([
            "object-grant",
            "--address",
            &object_grant_address,
            "--credentials",
            object_grant_credentials,
            "--grantee-id",
            &object_grant_grantee,
            "--object-id",
            &object_grant_table,
            "--privilege",
            "select",
            "--target-scope",
            "column",
            "--target-id",
            &object_grant_column,
        ])
        .output()?;
    assert!(
        grant_object_cli.status.success(),
        "object-grant CLI failed: {}",
        String::from_utf8_lossy(&grant_object_cli.stderr)
    );
    assert!(String::from_utf8_lossy(&grant_object_cli.stdout).contains("granted object privilege"));
    let grant_list_schema = encode_id_hex(&created_schema.identity_id);
    let grant_list_cli = Command::new(env!("CARGO_BIN_EXE_prdb-client"))
        .args([
            "grant-list",
            "--address",
            &object_grant_address,
            "--credentials",
            object_grant_credentials,
            "--schema-id",
            &grant_list_schema,
            "--table-id",
            &object_grant_table,
        ])
        .output()?;
    assert!(
        grant_list_cli.status.success(),
        "grant-list CLI failed: {}",
        String::from_utf8_lossy(&grant_list_cli.stderr)
    );
    let grant_list_json = String::from_utf8_lossy(&grant_list_cli.stdout);
    assert!(grant_list_json.contains(&format!("\"grantee_id\":\"{object_grant_grantee}\"")));
    assert!(grant_list_json.contains(&format!("\"grantor_id\":\"{admin_owner_id_text}\"")));
    assert!(grant_list_json.contains("\"privilege\":\"select\""));
    assert!(grant_list_json.contains(&format!("\"target_id\":\"{object_grant_column}\"")));
    let revoke_object_cli = Command::new(env!("CARGO_BIN_EXE_prdb-client"))
        .args([
            "object-revoke",
            "--address",
            &object_grant_address,
            "--credentials",
            object_grant_credentials,
            "--grantee-id",
            &object_grant_grantee,
            "--object-id",
            &object_grant_table,
            "--privilege",
            "select",
            "--target-scope",
            "column",
            "--target-id",
            &object_grant_column,
        ])
        .output()?;
    assert!(
        revoke_object_cli.status.success(),
        "object-revoke CLI failed: {}",
        String::from_utf8_lossy(&revoke_object_cli.stderr)
    );
    assert!(
        String::from_utf8_lossy(&revoke_object_cli.stdout).contains("revoked object privilege")
    );
    let dropped_inline_reference_table = admin.drop_table(inline_reference_table.table_id)?;
    let dropped_unique = admin
        .drop_unique_constraint(unique_constraint.constraint_id)
        .map_err(|error| format!("process ADMIN drop_unique_constraint failed: {error}"))?;
    assert!(dropped_unique.commit_sequence > dropped_inline_reference_table.commit_sequence);
    assert!(dropped_unique.commit_sequence > unique_constraint.commit_sequence);
    let constraint_cli_address = address.to_string();
    let constraint_cli_credentials = admin_cancel_credentials_path
        .to_str()
        .ok_or("credential path is not UTF-8")?;
    let constraint_cli_table_id = encode_id_hex(&created_table.table_id);
    let constraint_cli_column_ids = encode_id_hex(&created_table.column_ids[0]);
    let create_unique_cli = Command::new(env!("CARGO_BIN_EXE_prdb-client"))
        .args([
            "unique-create",
            "--address",
            &constraint_cli_address,
            "--credentials",
            constraint_cli_credentials,
            "--table-id",
            &constraint_cli_table_id,
            "--columns",
            &constraint_cli_column_ids,
        ])
        .output()?;
    assert!(
        create_unique_cli.status.success(),
        "unique-create CLI failed: {}",
        String::from_utf8_lossy(&create_unique_cli.stderr)
    );
    let unique_cli_output = String::from_utf8_lossy(&create_unique_cli.stdout);
    assert!(unique_cli_output.contains("created unique constraint "));
    let unique_cli_id = unique_cli_output
        .split_whitespace()
        .nth(3)
        .ok_or("unique-create output omitted the generated ID")?;
    let drop_unique_cli = Command::new(env!("CARGO_BIN_EXE_prdb-client"))
        .args([
            "unique-drop",
            "--address",
            &constraint_cli_address,
            "--credentials",
            constraint_cli_credentials,
            "--constraint-id",
            unique_cli_id,
        ])
        .output()?;
    assert!(
        drop_unique_cli.status.success(),
        "unique-drop CLI failed: {}",
        String::from_utf8_lossy(&drop_unique_cli.stderr)
    );
    assert!(String::from_utf8_lossy(&drop_unique_cli.stdout).contains("dropped key constraint"));

    admin.alter_column_nullability(created_table.table_id, created_table.column_ids[0], false)?;
    let create_primary_key_cli = Command::new(env!("CARGO_BIN_EXE_prdb-client"))
        .args([
            "primary-key-create",
            "--address",
            &constraint_cli_address,
            "--credentials",
            constraint_cli_credentials,
            "--table-id",
            &constraint_cli_table_id,
            "--columns",
            &constraint_cli_column_ids,
        ])
        .output()?;
    assert!(
        create_primary_key_cli.status.success(),
        "primary-key-create CLI failed: {}",
        String::from_utf8_lossy(&create_primary_key_cli.stderr)
    );
    let primary_key_cli_output = String::from_utf8_lossy(&create_primary_key_cli.stdout);
    assert!(primary_key_cli_output.contains("created primary-key constraint "));
    let primary_key_cli_id = primary_key_cli_output
        .split_whitespace()
        .nth(3)
        .ok_or("primary-key-create output omitted the generated ID")?;
    let drop_primary_key_cli = Command::new(env!("CARGO_BIN_EXE_prdb-client"))
        .args([
            "primary-key-drop",
            "--address",
            &constraint_cli_address,
            "--credentials",
            constraint_cli_credentials,
            "--constraint-id",
            primary_key_cli_id,
        ])
        .output()?;
    assert!(
        drop_primary_key_cli.status.success(),
        "primary-key-drop CLI failed: {}",
        String::from_utf8_lossy(&drop_primary_key_cli.stderr)
    );
    assert!(
        String::from_utf8_lossy(&drop_primary_key_cli.stdout).contains("dropped key constraint")
    );
    admin.alter_column_nullability(created_table.table_id, created_table.column_ids[0], true)?;
    let check_expression = prdb_engine::authorization::Expr::Literal(Some(
        prdb_engine::tuple::ScalarValue::Boolean(true),
    ));
    let check_expression_hex = prdb_engine::expression::encode_expression(&check_expression)?
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect::<String>();
    let create_check_cli = Command::new(env!("CARGO_BIN_EXE_prdb-client"))
        .args([
            "check-create",
            "--address",
            &constraint_cli_address,
            "--credentials",
            constraint_cli_credentials,
            "--table-id",
            &constraint_cli_table_id,
            "--expression-eir1",
            &check_expression_hex,
            "--columns",
            "",
        ])
        .output()?;
    assert!(
        create_check_cli.status.success(),
        "check-create CLI failed: {}",
        String::from_utf8_lossy(&create_check_cli.stderr)
    );
    let check_cli_output = String::from_utf8_lossy(&create_check_cli.stdout);
    assert!(check_cli_output.contains("created CHECK constraint "));
    let check_cli_id = check_cli_output
        .split_whitespace()
        .nth(3)
        .ok_or("check-create output omitted the generated ID")?;
    let drop_check_cli = Command::new(env!("CARGO_BIN_EXE_prdb-client"))
        .args([
            "check-drop",
            "--address",
            &constraint_cli_address,
            "--credentials",
            constraint_cli_credentials,
            "--constraint-id",
            check_cli_id,
        ])
        .output()?;
    assert!(
        drop_check_cli.status.success(),
        "check-drop CLI failed: {}",
        String::from_utf8_lossy(&drop_check_cli.stderr)
    );
    assert!(String::from_utf8_lossy(&drop_check_cli.stdout).contains("dropped CHECK constraint"));
    let policy = admin.create_policy(AdminCreatePolicyRequest {
        table_id: created_table.table_id,
        name: "process_visible_rows",
        kind: AdminPolicyKind::Row,
        operation: AdminPolicyOperation::Select,
        effect: AdminPolicyEffect::Allow,
        recipients: &[AdminPolicyRecipient::User(admin_user_id)],
        target_column_ids: &[],
        using_expression: Some(&prdb_engine::authorization::Expr::Literal(Some(
            prdb_engine::tuple::ScalarValue::Boolean(true),
        ))),
        with_check_expression: None,
    })?;
    let listed_policies = admin.list_policies_page(
        created_schema.identity_id,
        created_table.table_id,
        128,
        None,
    )?;
    let listed_policy = listed_policies
        .records
        .iter()
        .find(|listed| listed.policy_id == policy.policy_id)
        .ok_or("administrative policy page omitted the created policy")?;
    assert_eq!(
        listed_policy.kind,
        prdb_engine::metadata_discovery::AdministrativePolicyKind::Row
    );
    assert_eq!(
        listed_policy.operation,
        prdb_engine::metadata_discovery::AdministrativePolicyOperation::Select
    );
    assert_eq!(
        listed_policy.effect,
        prdb_engine::metadata_discovery::AdministrativePolicyEffect::Allow
    );
    assert!(listed_policy.column_ids.is_empty());
    assert_eq!(
        listed_policy
            .recipients
            .as_ref()
            .and_then(|recipients| recipients.first())
            .map(|recipient| recipient.principal_id),
        Some(admin_user_id)
    );

    let schema_owner_policy = admin.create_policy(AdminCreatePolicyRequest {
        table_id: redacted_source_table.table_id,
        name: "schema_owner_policy_summary",
        kind: AdminPolicyKind::Row,
        operation: AdminPolicyOperation::Select,
        effect: AdminPolicyEffect::Allow,
        recipients: &[AdminPolicyRecipient::User(admin_user_id)],
        target_column_ids: &[],
        using_expression: Some(&prdb_engine::authorization::Expr::Literal(Some(
            prdb_engine::tuple::ScalarValue::Boolean(true),
        ))),
        with_check_expression: None,
    })?;
    admin.set_schema_owner(
        redacted_source_schema.identity_id,
        transfer_owner.identity_id,
    )?;
    let owner_policy_credentials = read_client_credentials(&transfer_credential_path)?;
    let mut owner_policy_secret = owner_policy_credentials.token_secret;
    let mut owner_policy_client = AdminClient::connect_authenticated(
        address,
        owner_policy_credentials.token_id,
        &mut owner_policy_secret,
    )?;
    drop(owner_policy_credentials);
    let owner_policies = owner_policy_client.list_policies_page(
        redacted_source_schema.identity_id,
        redacted_source_table.table_id,
        128,
        None,
    )?;
    let owner_policy = owner_policies
        .records
        .iter()
        .find(|listed| listed.policy_id == schema_owner_policy.policy_id)
        .ok_or("schema-owner policy page omitted the created policy")?;
    assert!(owner_policy.recipients.is_none());
    let owner_dependencies = owner_policy_client.list_dependencies_page(
        redacted_foreign_key.constraint_id,
        128,
        None,
    )?;
    assert!(owner_dependencies.records.is_empty());
    drop(owner_policy_client);
    admin.set_schema_owner(redacted_source_schema.identity_id, admin_user_id)?;
    admin.drop_policy(schema_owner_policy.policy_id)?;
    let dropped_policy = admin.drop_policy(policy.policy_id)?;
    assert!(dropped_policy.commit_sequence > policy.commit_sequence);
    let cli_policy_true = prdb_engine::authorization::Expr::Literal(Some(
        prdb_engine::tuple::ScalarValue::Boolean(true),
    ));
    let cli_policy_expression = prdb_engine::expression::encode_expression(&cli_policy_true)?
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect::<String>();
    let policy_cli_address = address.to_string();
    let policy_cli_credentials = admin_cancel_credentials_path
        .to_str()
        .ok_or("credential path is not UTF-8")?;
    let policy_cli_table_id = encode_id_hex(&created_table.table_id);
    let policy_cli_recipient = format!("user:{}", encode_id_hex(&admin_user_id));
    let create_policy_cli = Command::new(env!("CARGO_BIN_EXE_prdb-client"))
        .args([
            "policy-create",
            "--address",
            &policy_cli_address,
            "--credentials",
            policy_cli_credentials,
            "--table-id",
            &policy_cli_table_id,
            "--name",
            "process_cli_policy",
            "--kind",
            "row",
            "--operation",
            "select",
            "--effect",
            "allow",
            "--recipients",
            &policy_cli_recipient,
            "--using-expression-eir1",
            &cli_policy_expression,
        ])
        .output()?;
    assert!(
        create_policy_cli.status.success(),
        "policy-create CLI failed: {}",
        String::from_utf8_lossy(&create_policy_cli.stderr)
    );
    let create_policy_output = String::from_utf8_lossy(&create_policy_cli.stdout);
    assert!(create_policy_output.contains("created policy "));
    let cli_policy_id = create_policy_output
        .split_whitespace()
        .nth(2)
        .ok_or("policy-create output omitted the generated ID")?;
    let drop_policy_cli = Command::new(env!("CARGO_BIN_EXE_prdb-client"))
        .args([
            "policy-drop",
            "--address",
            &policy_cli_address,
            "--credentials",
            policy_cli_credentials,
            "--policy-id",
            cli_policy_id,
        ])
        .output()?;
    assert!(
        drop_policy_cli.status.success(),
        "policy-drop CLI failed: {}",
        String::from_utf8_lossy(&drop_policy_cli.stderr)
    );
    assert!(String::from_utf8_lossy(&drop_policy_cli.stdout).contains("dropped policy"));
    let cell_policy = admin.create_policy(AdminCreatePolicyRequest {
        table_id: created_table.table_id,
        name: "process_visible_cells",
        kind: AdminPolicyKind::Cell,
        operation: AdminPolicyOperation::Select,
        effect: AdminPolicyEffect::Allow,
        recipients: &[AdminPolicyRecipient::User(admin_user_id)],
        target_column_ids: &[added_column.column_id],
        using_expression: Some(&prdb_engine::authorization::Expr::Literal(Some(
            prdb_engine::tuple::ScalarValue::Boolean(true),
        ))),
        with_check_expression: None,
    })?;
    let dropped_cell_policy = admin.drop_policy(cell_policy.policy_id)?;
    assert!(dropped_cell_policy.commit_sequence > cell_policy.commit_sequence);
    let policy_true = prdb_engine::authorization::Expr::Literal(Some(
        prdb_engine::tuple::ScalarValue::Boolean(true),
    ));
    let cell_insert_policy = admin.create_policy(AdminCreatePolicyRequest {
        table_id: created_table.table_id,
        name: "process_insertable_cells",
        kind: AdminPolicyKind::Cell,
        operation: AdminPolicyOperation::Insert,
        effect: AdminPolicyEffect::Allow,
        recipients: &[AdminPolicyRecipient::User(admin_user_id)],
        target_column_ids: &[added_column.column_id],
        using_expression: None,
        with_check_expression: Some(&policy_true),
    })?;
    let dropped_cell_insert_policy = admin.drop_policy(cell_insert_policy.policy_id)?;
    assert!(dropped_cell_insert_policy.commit_sequence > cell_insert_policy.commit_sequence);
    let cell_update_policy = admin.create_policy(AdminCreatePolicyRequest {
        table_id: created_table.table_id,
        name: "process_editable_cells",
        kind: AdminPolicyKind::Cell,
        operation: AdminPolicyOperation::Update,
        effect: AdminPolicyEffect::Allow,
        recipients: &[AdminPolicyRecipient::User(admin_user_id)],
        target_column_ids: &[added_column.column_id],
        using_expression: Some(&policy_true),
        with_check_expression: Some(&policy_true),
    })?;
    let dropped_cell_update_policy = admin.drop_policy(cell_update_policy.policy_id)?;
    assert!(dropped_cell_update_policy.commit_sequence > cell_update_policy.commit_sequence);
    for (name, operation, using_expression, with_check_expression) in [
        (
            "process_insertable_rows",
            AdminPolicyOperation::Insert,
            None,
            Some(&policy_true),
        ),
        (
            "process_updatable_rows",
            AdminPolicyOperation::Update,
            Some(&policy_true),
            Some(&policy_true),
        ),
        (
            "process_deletable_rows",
            AdminPolicyOperation::Delete,
            Some(&policy_true),
            None,
        ),
    ] {
        let row_policy = admin.create_policy(AdminCreatePolicyRequest {
            table_id: created_table.table_id,
            name,
            kind: AdminPolicyKind::Row,
            operation,
            effect: AdminPolicyEffect::Allow,
            recipients: &[AdminPolicyRecipient::User(admin_user_id)],
            target_column_ids: &[],
            using_expression,
            with_check_expression,
        })?;
        let dropped_row_policy = admin.drop_policy(row_policy.policy_id)?;
        assert!(dropped_row_policy.commit_sequence > row_policy.commit_sequence);
    }
    let renamed_column =
        admin.rename_column(created_table.table_id, added_column.column_id, "extra_v2")?;
    let dropped_column = admin.drop_column(created_table.table_id, added_column.column_id)?;
    assert!(dropped_column.commit_sequence > renamed_column.commit_sequence);
    let column_cli_address = address.to_string();
    let column_cli_credentials = admin_cancel_credentials_path
        .to_str()
        .ok_or("credential path is not UTF-8")?;
    let column_cli_table_id = encode_id_hex(&created_table.table_id);
    let column_cli_column_id = encode_id_hex(&created_table.column_ids[0]);
    for nullable in ["false", "true"] {
        let nullability_cli = Command::new(env!("CARGO_BIN_EXE_prdb-client"))
            .args([
                "column-nullability",
                "--address",
                &column_cli_address,
                "--credentials",
                column_cli_credentials,
                "--table-id",
                &column_cli_table_id,
                "--column-id",
                &column_cli_column_id,
                "--nullable",
                nullable,
            ])
            .output()?;
        assert!(
            nullability_cli.status.success(),
            "column-nullability CLI ({nullable}) failed: {}",
            String::from_utf8_lossy(&nullability_cli.stderr)
        );
        assert!(
            String::from_utf8_lossy(&nullability_cli.stdout).contains("changed column nullability")
        );
    }
    let set_default_cli = Command::new(env!("CARGO_BIN_EXE_prdb-client"))
        .args([
            "column-default-set",
            "--address",
            &column_cli_address,
            "--credentials",
            column_cli_credentials,
            "--table-id",
            &column_cli_table_id,
            "--column-id",
            &column_cli_column_id,
            "--type",
            "text",
            "--value",
            "seed",
        ])
        .output()?;
    assert!(
        set_default_cli.status.success(),
        "column-default-set CLI failed: {}",
        String::from_utf8_lossy(&set_default_cli.stderr)
    );
    assert!(String::from_utf8_lossy(&set_default_cli.stdout).contains("set column default"));
    let drop_default_cli = Command::new(env!("CARGO_BIN_EXE_prdb-client"))
        .args([
            "column-default-drop",
            "--address",
            &column_cli_address,
            "--credentials",
            column_cli_credentials,
            "--table-id",
            &column_cli_table_id,
            "--column-id",
            &column_cli_column_id,
        ])
        .output()?;
    assert!(
        drop_default_cli.status.success(),
        "column-default-drop CLI failed: {}",
        String::from_utf8_lossy(&drop_default_cli.stderr)
    );
    assert!(String::from_utf8_lossy(&drop_default_cli.stdout).contains("dropped column default"));
    let created_index = admin
        .create_index(
            created_table.table_id,
            "process_admin_index",
            &created_table.column_ids,
            false,
        )
        .map_err(|error| format!("process ADMIN create_index failed: {error}"))?;
    let create_index_sql_path = root.join("create-index.sql");
    std::fs::write(
        &create_index_sql_path,
        "CREATE INDEX process_admin_sql_index ON process_admin_schema.process_admin_table (payload);",
    )?;
    let create_index_sql = Command::new(env!("CARGO_BIN_EXE_prdb-client"))
        .args([
            "admin-sql",
            "--address",
            &address_text,
            "--credentials",
            admin_cancel_credentials_path
                .to_str()
                .ok_or("credential path is not UTF-8")?,
            "--sql-file",
            create_index_sql_path
                .to_str()
                .ok_or("SQL path is not UTF-8")?,
        ])
        .output()?;
    assert!(
        create_index_sql.status.success(),
        "CREATE INDEX SQL failed: {}",
        String::from_utf8_lossy(&create_index_sql.stderr)
    );
    assert!(String::from_utf8_lossy(&create_index_sql.stdout).contains("created index "));
    let original_sql_index = admin
        .list_indexes_page(created_schema.identity_id, created_table.table_id, 64, None)?
        .records
        .into_iter()
        .find(|index| index.name == "process_admin_sql_index")
        .ok_or("created index is missing from metadata")?;
    let rename_index_sql_path = root.join("rename-index.sql");
    std::fs::write(
        &rename_index_sql_path,
        "ALTER INDEX process_admin_schema.process_admin_sql_index RENAME TO process_admin_renamed_sql_index;",
    )?;
    let rename_index_sql = Command::new(env!("CARGO_BIN_EXE_prdb-client"))
        .args([
            "admin-sql",
            "--address",
            &address_text,
            "--credentials",
            admin_cancel_credentials_path
                .to_str()
                .ok_or("credential path is not UTF-8")?,
            "--sql-file",
            rename_index_sql_path
                .to_str()
                .ok_or("SQL path is not UTF-8")?,
        ])
        .output()?;
    assert!(
        rename_index_sql.status.success(),
        "ALTER INDEX RENAME SQL failed: {}",
        String::from_utf8_lossy(&rename_index_sql.stderr)
    );
    assert!(String::from_utf8_lossy(&rename_index_sql.stdout).contains("renamed index"));
    let renamed_index = admin
        .list_indexes_page(created_schema.identity_id, created_table.table_id, 64, None)?
        .records
        .into_iter()
        .find(|index| index.name == "process_admin_renamed_sql_index")
        .ok_or("renamed index is missing from metadata")?;
    assert_eq!(renamed_index.name, "process_admin_renamed_sql_index");
    assert_eq!(renamed_index.index_id, original_sql_index.index_id);
    let duplicate_rename_index_sql_path = root.join("rename-index-collision.sql");
    std::fs::write(
        &duplicate_rename_index_sql_path,
        "ALTER INDEX process_admin_schema.process_admin_renamed_sql_index RENAME TO process_admin_index;",
    )?;
    let duplicate_rename_index_sql = Command::new(env!("CARGO_BIN_EXE_prdb-client"))
        .args([
            "admin-sql",
            "--address",
            &address_text,
            "--credentials",
            admin_cancel_credentials_path
                .to_str()
                .ok_or("credential path is not UTF-8")?,
            "--sql-file",
            duplicate_rename_index_sql_path
                .to_str()
                .ok_or("SQL path is not UTF-8")?,
        ])
        .output()?;
    assert!(!duplicate_rename_index_sql.status.success());
    assert!(
        admin
            .list_indexes_page(created_schema.identity_id, created_table.table_id, 64, None)?
            .records
            .iter()
            .any(|index| index.index_id == original_sql_index.index_id
                && index.name == "process_admin_renamed_sql_index")
    );
    let alter_index_sql_path = root.join("alter-index.sql");
    std::fs::write(
        &alter_index_sql_path,
        "ALTER INDEX process_admin_schema.process_admin_index REBUILD USING BTREE NONUNIQUE (payload);",
    )?;
    let alter_index_sql = Command::new(env!("CARGO_BIN_EXE_prdb-client"))
        .args([
            "admin-sql",
            "--address",
            &address_text,
            "--credentials",
            admin_cancel_credentials_path
                .to_str()
                .ok_or("credential path is not UTF-8")?,
            "--sql-file",
            alter_index_sql_path
                .to_str()
                .ok_or("SQL path is not UTF-8")?,
        ])
        .output()?;
    assert!(
        alter_index_sql.status.success(),
        "ALTER INDEX REBUILD SQL failed: {}",
        String::from_utf8_lossy(&alter_index_sql.stderr)
    );
    assert!(String::from_utf8_lossy(&alter_index_sql.stdout).contains("rebuilt index definition"));
    let payload_column_id = admin
        .list_columns_page(created_schema.identity_id, created_table.table_id, 64, None)?
        .records
        .into_iter()
        .find(|column| column.name == "payload")
        .ok_or("ALTER INDEX SQL fixture column is missing")?
        .column_id;
    let altered_index = admin
        .list_indexes_page(created_schema.identity_id, created_table.table_id, 64, None)?
        .records
        .into_iter()
        .find(|index| index.index_id == created_index.index_id)
        .ok_or("ALTER INDEX SQL fixture index is missing")?;
    assert_eq!(altered_index.column_ids, [payload_column_id]);
    assert!(!altered_index.unique);
    let rebuild_index_sql_path = root.join("rebuild-index.sql");
    std::fs::write(
        &rebuild_index_sql_path,
        "REBUILD INDEX process_admin_schema.process_admin_index;",
    )?;
    let rebuild_index_sql = Command::new(env!("CARGO_BIN_EXE_prdb-client"))
        .args([
            "admin-sql",
            "--address",
            &address_text,
            "--credentials",
            admin_cancel_credentials_path
                .to_str()
                .ok_or("credential path is not UTF-8")?,
            "--sql-file",
            rebuild_index_sql_path
                .to_str()
                .ok_or("SQL path is not UTF-8")?,
        ])
        .output()?;
    assert!(
        rebuild_index_sql.status.success(),
        "REBUILD INDEX SQL failed: {}",
        String::from_utf8_lossy(&rebuild_index_sql.stderr)
    );
    assert!(String::from_utf8_lossy(&rebuild_index_sql.stdout).contains("rebuilt index"));
    let drop_index_sql_path = root.join("drop-index.sql");
    std::fs::write(
        &drop_index_sql_path,
        "DROP INDEX process_admin_schema.process_admin_renamed_sql_index RESTRICT;",
    )?;
    let drop_index_sql = Command::new(env!("CARGO_BIN_EXE_prdb-client"))
        .args([
            "admin-sql",
            "--address",
            &address_text,
            "--credentials",
            admin_cancel_credentials_path
                .to_str()
                .ok_or("credential path is not UTF-8")?,
            "--sql-file",
            drop_index_sql_path
                .to_str()
                .ok_or("SQL path is not UTF-8")?,
        ])
        .output()?;
    assert!(
        drop_index_sql.status.success(),
        "DROP INDEX SQL failed: {}",
        String::from_utf8_lossy(&drop_index_sql.stderr)
    );
    assert!(String::from_utf8_lossy(&drop_index_sql.stdout).contains("dropped index"));
    let rebuilt_index = admin
        .rebuild_index(created_index.index_id, &created_table.column_ids, true)
        .map_err(|error| format!("process ADMIN rebuild_index failed: {error}"))?;
    let second_index = admin
        .create_index(
            created_table.table_id,
            "process_admin_index_second",
            &created_table.column_ids,
            false,
        )
        .map_err(|error| format!("create second metadata index: {error:?}"))?;
    let cursor_credentials = read_client_credentials(&admin_cancel_credentials_path)?;
    let mut cursor_secret = cursor_credentials.token_secret;
    let mut cursor_admin = AdminClient::connect_authenticated(
        address,
        cursor_credentials.token_id,
        &mut cursor_secret,
    )?;
    drop(cursor_credentials);
    let first_index_page = cursor_admin
        .list_indexes_page(created_schema.identity_id, created_table.table_id, 1, None)
        .map_err(|error| format!("first index page: {error:?}"))?;
    let stale_index_cursor = first_index_page
        .next_cursor
        .ok_or("expected another index page before dropping an index")?;
    admin
        .drop_index(second_index.index_id)
        .map_err(|error| format!("drop second metadata index: {error:?}"))?;
    let stale_page = cursor_admin.list_indexes_page(
        created_schema.identity_id,
        created_table.table_id,
        1,
        Some(&stale_index_cursor),
    );
    assert!(matches!(
        stale_page,
        Err(prdb_engine::client_submit::ClientSubmitError::ServerRejected(_))
    ));
    drop(cursor_admin);
    let listed_indexes = admin
        .list_indexes_page(
            created_schema.identity_id,
            created_table.table_id,
            128,
            None,
        )
        .map_err(|error| format!("complete index page after stale cursor: {error:?}"))?;
    let listed_index = listed_indexes
        .records
        .iter()
        .find(|index| index.index_id == created_index.index_id)
        .ok_or_else(|| {
            format!(
                "administrative index page omitted {} from {:?}",
                encode_id_hex(&created_index.index_id),
                listed_indexes.records
            )
        })?;
    assert_eq!(listed_index.column_ids, created_table.column_ids);
    assert!(listed_index.unique);
    let repeated_rebuild = admin
        .rebuild_index(created_index.index_id, &created_table.column_ids, true)
        .map_err(|error| format!("process ADMIN repeated rebuild failed: {error}"))?;
    let dropped_index = admin
        .drop_index(created_index.index_id)
        .map_err(|error| format!("process ADMIN drop_index failed: {error}"))?;
    assert!(repeated_rebuild.commit_sequence > rebuilt_index.commit_sequence);
    assert!(dropped_index.commit_sequence > repeated_rebuild.commit_sequence);
    let index_cli_address = address.to_string();
    let index_cli_credentials = admin_cancel_credentials_path
        .to_str()
        .ok_or("credential path is not UTF-8")?;
    let index_cli_table_id = encode_id_hex(&created_table.table_id);
    let index_cli_column_ids = encode_id_hex(&created_table.column_ids[0]);
    let create_index_cli = Command::new(env!("CARGO_BIN_EXE_prdb-client"))
        .args([
            "index-create",
            "--address",
            &index_cli_address,
            "--credentials",
            index_cli_credentials,
            "--table-id",
            &index_cli_table_id,
            "--name",
            "process_cli_index",
            "--columns",
            &index_cli_column_ids,
            "--unique",
            "true",
        ])
        .output()?;
    assert!(
        create_index_cli.status.success(),
        "index-create CLI failed: {}",
        String::from_utf8_lossy(&create_index_cli.stderr)
    );
    let create_index_output = String::from_utf8_lossy(&create_index_cli.stdout);
    assert!(create_index_output.contains("created index "));
    let cli_index_id = create_index_output
        .split_whitespace()
        .nth(2)
        .ok_or("index-create output omitted the generated ID")?;
    assert_eq!(cli_index_id.len(), 32);
    let rebuild_index_cli = Command::new(env!("CARGO_BIN_EXE_prdb-client"))
        .args([
            "index-rebuild",
            "--address",
            &index_cli_address,
            "--credentials",
            index_cli_credentials,
            "--index-id",
            cli_index_id,
            "--columns",
            &index_cli_column_ids,
            "--unique",
            "false",
        ])
        .output()?;
    assert!(
        rebuild_index_cli.status.success(),
        "index-rebuild CLI failed: {}",
        String::from_utf8_lossy(&rebuild_index_cli.stderr)
    );
    assert!(String::from_utf8_lossy(&rebuild_index_cli.stdout).contains("rebuilt index"));
    let drop_index_cli = Command::new(env!("CARGO_BIN_EXE_prdb-client"))
        .args([
            "index-drop",
            "--address",
            &index_cli_address,
            "--credentials",
            index_cli_credentials,
            "--index-id",
            cli_index_id,
        ])
        .output()?;
    assert!(
        drop_index_cli.status.success(),
        "index-drop CLI failed: {}",
        String::from_utf8_lossy(&drop_index_cli.stderr)
    );
    assert!(String::from_utf8_lossy(&drop_index_cli.stdout).contains("dropped index"));
    let policy_role = admin.create_role("process_policy_role")?;
    admin.grant_object_privilege(
        transfer_owner.identity_id,
        created_schema.identity_id,
        AdminObjectPrivilege::Usage,
        AdminObjectGrantTarget::Object,
    )?;
    admin.set_table_owner(created_table.table_id, transfer_owner.identity_id)?;
    let pre_policy_ids = admin
        .list_policies_page(created_schema.identity_id, created_table.table_id, 64, None)?
        .records
        .into_iter()
        .map(|policy| policy.policy_id)
        .collect::<std::collections::HashSet<_>>();
    let create_policy_sql_path = root.join("create-row-policy.sql");
    std::fs::write(
        &create_policy_sql_path,
        "CREATE ROW POLICY process_visible ON process_admin_schema.process_admin_table FOR SELECT TO USER metadata_process_admin AS ALLOW USING (payload = 'visible' AND HAS_ROLE('process_policy_role'));",
    )?;
    let create_policy_sql = Command::new(env!("CARGO_BIN_EXE_prdb-client"))
        .args([
            "admin-sql",
            "--address",
            &address_text,
            "--credentials",
            transfer_credential_path
                .to_str()
                .ok_or("credential path is not UTF-8")?,
            "--sql-file",
            create_policy_sql_path
                .to_str()
                .ok_or("SQL path is not UTF-8")?,
        ])
        .output()?;
    assert!(
        create_policy_sql.status.success(),
        "CREATE POLICY SQL failed: {}",
        String::from_utf8_lossy(&create_policy_sql.stderr)
    );
    assert!(String::from_utf8_lossy(&create_policy_sql.stdout).contains("created policy"));
    let create_cell_policy_sql_path = root.join("create-cell-policy.sql");
    std::fs::write(
        &create_cell_policy_sql_path,
        "CREATE CELL POLICY process_payload ON process_admin_schema.process_admin_table (payload) FOR SELECT TO ROLE process_policy_role AS RESTRICT USING (HAS_ROLE('process_policy_role'));",
    )?;
    let create_cell_policy_sql = Command::new(env!("CARGO_BIN_EXE_prdb-client"))
        .args([
            "admin-sql",
            "--address",
            &address_text,
            "--credentials",
            transfer_credential_path
                .to_str()
                .ok_or("credential path is not UTF-8")?,
            "--sql-file",
            create_cell_policy_sql_path
                .to_str()
                .ok_or("SQL path is not UTF-8")?,
        ])
        .output()?;
    assert!(
        create_cell_policy_sql.status.success(),
        "CREATE CELL POLICY SQL failed: {}",
        String::from_utf8_lossy(&create_cell_policy_sql.stderr)
    );
    assert!(String::from_utf8_lossy(&create_cell_policy_sql.stdout).contains("created policy"));
    let policy_list_schema = encode_id_hex(&created_schema.identity_id);
    let policy_list_table = encode_id_hex(&created_table.table_id);
    let policy_list_cli = Command::new(env!("CARGO_BIN_EXE_prdb-client"))
        .args([
            "policy-list",
            "--address",
            &address_text,
            "--credentials",
            admin_cancel_credentials_path
                .to_str()
                .ok_or("policy-list credentials path is not UTF-8")?,
            "--schema-id",
            &policy_list_schema,
            "--table-id",
            &policy_list_table,
        ])
        .output()?;
    assert!(
        policy_list_cli.status.success(),
        "policy-list CLI failed: {}",
        String::from_utf8_lossy(&policy_list_cli.stderr)
    );
    let policy_list_json = String::from_utf8_lossy(&policy_list_cli.stdout);
    assert!(
        policy_list_json.contains("\"kind\":\"row\""),
        "policy-list omitted row policy summary: {policy_list_json}"
    );
    assert!(policy_list_json.contains("\"kind\":\"cell\""));
    assert!(policy_list_json.contains(&encode_id_hex(&admin_user_id)));
    assert!(policy_list_json.contains(&encode_id_hex(&policy_role.identity_id)));
    assert!(!policy_list_json.contains("HAS_ROLE"));
    let original_policy_ids = admin
        .list_policies_page(created_schema.identity_id, created_table.table_id, 64, None)?
        .records
        .into_iter()
        .map(|policy| policy.policy_id)
        .collect::<std::collections::HashSet<_>>();
    let replace_policy_sql_path = root.join("replace-row-policy.sql");
    std::fs::write(
        &replace_policy_sql_path,
        "CREATE OR REPLACE CELL POLICY process_visible ON process_admin_schema.process_admin_table (payload) FOR SELECT TO USER metadata_process_admin AS ALLOW USING (HAS_ROLE('process_policy_role'));",
    )?;
    let replace_policy_sql = Command::new(env!("CARGO_BIN_EXE_prdb-client"))
        .args([
            "admin-sql",
            "--address",
            &address_text,
            "--credentials",
            transfer_credential_path
                .to_str()
                .ok_or("credential path is not UTF-8")?,
            "--sql-file",
            replace_policy_sql_path
                .to_str()
                .ok_or("SQL path is not UTF-8")?,
        ])
        .output()?;
    assert!(
        replace_policy_sql.status.success(),
        "CREATE OR REPLACE POLICY SQL failed: {}",
        String::from_utf8_lossy(&replace_policy_sql.stderr)
    );
    assert!(String::from_utf8_lossy(&replace_policy_sql.stdout).contains("created policy"));
    let replaced_policy_ids = admin
        .list_policies_page(created_schema.identity_id, created_table.table_id, 64, None)?
        .records
        .into_iter()
        .map(|policy| policy.policy_id)
        .collect::<std::collections::HashSet<_>>();
    assert_eq!(replaced_policy_ids.len(), original_policy_ids.len());
    assert_eq!(
        replaced_policy_ids
            .intersection(&original_policy_ids)
            .count(),
        original_policy_ids.len() - 1,
        "replacement must remove exactly the old definition and publish its successor"
    );
    let created_policy_ids = admin
        .list_policies_page(created_schema.identity_id, created_table.table_id, 64, None)?
        .records
        .into_iter()
        .map(|policy| policy.policy_id)
        .filter(|policy_id| !pre_policy_ids.contains(policy_id))
        .collect::<Vec<_>>();
    assert_eq!(created_policy_ids.len(), 2);
    for (filename, sql) in [
        (
            "drop-row-policy.sql",
            "DROP CELL POLICY process_visible ON process_admin_schema.process_admin_table RESTRICT;",
        ),
        (
            "drop-cell-policy.sql",
            "DROP CELL POLICY process_payload ON process_admin_schema.process_admin_table RESTRICT;",
        ),
    ] {
        let drop_policy_sql_path = root.join(filename);
        std::fs::write(&drop_policy_sql_path, sql)?;
        let drop_policy_sql = Command::new(env!("CARGO_BIN_EXE_prdb-client"))
            .args([
                "admin-sql",
                "--address",
                &address_text,
                "--credentials",
                transfer_credential_path
                    .to_str()
                    .ok_or("credential path is not UTF-8")?,
                "--sql-file",
                drop_policy_sql_path
                    .to_str()
                    .ok_or("SQL path is not UTF-8")?,
            ])
            .output()?;
        assert!(
            drop_policy_sql.status.success(),
            "DROP POLICY SQL failed: {}",
            String::from_utf8_lossy(&drop_policy_sql.stderr)
        );
        assert!(String::from_utf8_lossy(&drop_policy_sql.stdout).contains("dropped "));
    }
    assert!(admin
        .execute_drop_policy_if_exists_sql(
            "DROP ROW POLICY IF EXISTS process_missing ON process_admin_schema.process_admin_table RESTRICT"
        )
        .map_err(|error| format!("conditional missing policy drop: {error:?}"))?
        .is_none());
    assert!(admin
        .execute_drop_policy_if_exists_sql(
            "DROP ROW POLICY IF EXISTS process_missing ON process_admin_schema.process_admin_table RESTRICT"
        )
        .map_err(|error| format!("repeated conditional missing policy drop: {error:?}"))?
        .is_none());
    admin.set_table_owner(created_table.table_id, admin_user_id)?;
    admin.grant_object_privilege(
        transfer_owner.identity_id,
        created_table.table_id,
        AdminObjectPrivilege::ManagePolicies,
        AdminObjectGrantTarget::Object,
    )?;
    let mut manager_metadata_credentials = read_client_credentials(&transfer_credential_path)?;
    let mut manager_metadata = MetadataClient::connect_authenticated(
        address,
        manager_metadata_credentials.token_id,
        &mut manager_metadata_credentials.token_secret,
    )?;
    assert!(
        manager_metadata
            .list_relations(created_schema.identity_id)?
            .iter()
            .any(|relation| relation.relation_id == created_table.table_id)
    );
    drop(manager_metadata);
    drop(manager_metadata_credentials);
    let managed_policy_sql_path = root.join("create-managed-policy.sql");
    std::fs::write(
        &managed_policy_sql_path,
        "CREATE ROW POLICY process_managed ON process_admin_schema.process_admin_table FOR SELECT TO ROLE process_policy_role AS ALLOW USING (HAS_ROLE('process_policy_role'));",
    )?;
    let create_managed_policy_sql = Command::new(env!("CARGO_BIN_EXE_prdb-client"))
        .args([
            "admin-sql",
            "--address",
            &address_text,
            "--credentials",
            transfer_credential_path
                .to_str()
                .ok_or("credential path is not UTF-8")?,
            "--sql-file",
            managed_policy_sql_path
                .to_str()
                .ok_or("SQL path is not UTF-8")?,
        ])
        .output()?;
    assert!(
        create_managed_policy_sql.status.success(),
        "CREATE POLICY as MANAGE_POLICIES holder failed: {}",
        String::from_utf8_lossy(&create_managed_policy_sql.stderr)
    );
    let drop_managed_policy_sql_path = root.join("drop-managed-policy.sql");
    std::fs::write(
        &drop_managed_policy_sql_path,
        "DROP ROW POLICY process_managed ON process_admin_schema.process_admin_table RESTRICT;",
    )?;
    let drop_managed_policy_sql = Command::new(env!("CARGO_BIN_EXE_prdb-client"))
        .args([
            "admin-sql",
            "--address",
            &address_text,
            "--credentials",
            transfer_credential_path
                .to_str()
                .ok_or("credential path is not UTF-8")?,
            "--sql-file",
            drop_managed_policy_sql_path
                .to_str()
                .ok_or("SQL path is not UTF-8")?,
        ])
        .output()?;
    assert!(
        drop_managed_policy_sql.status.success(),
        "DROP POLICY as MANAGE_POLICIES holder failed: {}",
        String::from_utf8_lossy(&drop_managed_policy_sql.stderr)
    );
    let cursor_policy_one = admin.execute_create_policy_sql(
        "CREATE ROW POLICY process_cursor_policy_one ON process_admin_schema.process_admin_table FOR SELECT TO ROLE process_policy_role AS ALLOW USING (HAS_ROLE('process_policy_role'))",
    )?;
    let cursor_policy_two = admin.execute_create_policy_sql(
        "CREATE ROW POLICY process_cursor_policy_two ON process_admin_schema.process_admin_table FOR SELECT TO ROLE process_policy_role AS ALLOW USING (HAS_ROLE('process_policy_role'))",
    )?;
    for privilege in [
        prdb_engine::client_submit::AdminObjectPrivilege::Select,
        prdb_engine::client_submit::AdminObjectPrivilege::Update,
    ] {
        admin.grant_object_privilege(
            policy_role.identity_id,
            created_table.table_id,
            privilege,
            prdb_engine::client_submit::AdminObjectGrantTarget::Object,
        )?;
    }
    let revoked_index_one = admin.create_index(
        created_table.table_id,
        "process_revoked_cursor_index_one",
        &created_table.column_ids,
        false,
    )?;
    let revoked_index_two = admin.create_index(
        created_table.table_id,
        "process_revoked_cursor_index_two",
        &created_table.column_ids,
        false,
    )?;
    admin.set_schema_owner(created_schema.identity_id, transfer_owner.identity_id)?;
    let mut cursor_owner_credentials = read_client_credentials(&transfer_credential_path)?;
    let mut cursor_owner = AdminClient::connect_authenticated(
        address,
        cursor_owner_credentials.token_id,
        &mut cursor_owner_credentials.token_secret,
    )?;
    let first_policy_page = cursor_owner.list_policies_page(
        created_schema.identity_id,
        created_table.table_id,
        1,
        None,
    )?;
    assert_eq!(first_policy_page.records.len(), 1);
    let revoked_policy_cursor = first_policy_page
        .next_cursor
        .ok_or("expected a policy cursor before schema ownership is transferred")?;
    let mut index_cursor_owner_credentials = read_client_credentials(&transfer_credential_path)?;
    let mut index_cursor_owner = AdminClient::connect_authenticated(
        address,
        index_cursor_owner_credentials.token_id,
        &mut index_cursor_owner_credentials.token_secret,
    )?;
    let first_index_page = index_cursor_owner.list_indexes_page(
        created_schema.identity_id,
        created_table.table_id,
        1,
        None,
    )?;
    assert_eq!(first_index_page.records.len(), 1);
    let revoked_index_cursor = first_index_page
        .next_cursor
        .ok_or("expected an index cursor before schema ownership is transferred")?;
    let mut dependency_cursor_owner_credentials =
        read_client_credentials(&transfer_credential_path)?;
    let mut dependency_cursor_owner = AdminClient::connect_authenticated(
        address,
        dependency_cursor_owner_credentials.token_id,
        &mut dependency_cursor_owner_credentials.token_secret,
    )?;
    let first_dependency_page =
        dependency_cursor_owner.list_dependencies_page(created_table.table_id, 1, None)?;
    assert_eq!(first_dependency_page.records.len(), 1);
    let revoked_dependency_cursor = first_dependency_page
        .next_cursor
        .ok_or("expected a dependency cursor before schema ownership is transferred")?;
    let mut grant_cursor_owner_credentials = read_client_credentials(&transfer_credential_path)?;
    let mut grant_cursor_owner = AdminClient::connect_authenticated(
        address,
        grant_cursor_owner_credentials.token_id,
        &mut grant_cursor_owner_credentials.token_secret,
    )?;
    let first_grant_page = grant_cursor_owner.list_grants_page(
        created_schema.identity_id,
        created_table.table_id,
        1,
        None,
    )?;
    assert_eq!(first_grant_page.records.len(), 1);
    let revoked_grant_cursor = first_grant_page
        .next_cursor
        .ok_or("expected a grant cursor before schema ownership is transferred")?;
    admin.set_schema_owner(created_schema.identity_id, admin_user_id)?;
    let invalidated_grant_page = grant_cursor_owner.list_grants_page(
        created_schema.identity_id,
        created_table.table_id,
        1,
        Some(&revoked_grant_cursor),
    );
    assert!(
        matches!(
            &invalidated_grant_page,
            Err(prdb_engine::client_submit::ClientSubmitError::ServerRejected(_))
        ),
        "revoked grant cursor result: {invalidated_grant_page:?}"
    );
    let invalidated_index_page = index_cursor_owner.list_indexes_page(
        created_schema.identity_id,
        created_table.table_id,
        1,
        Some(&revoked_index_cursor),
    );
    assert!(
        matches!(
            &invalidated_index_page,
            Err(prdb_engine::client_submit::ClientSubmitError::ServerRejected(_))
        ),
        "revoked index cursor result: {invalidated_index_page:?}"
    );
    assert!(matches!(
        cursor_owner.list_policies_page(
            created_schema.identity_id,
            created_table.table_id,
            1,
            Some(&revoked_policy_cursor),
        ),
        Err(prdb_engine::client_submit::ClientSubmitError::ServerRejected(_))
    ));
    let invalidated_dependency_page = dependency_cursor_owner.list_dependencies_page(
        created_table.table_id,
        1,
        Some(&revoked_dependency_cursor),
    );
    assert!(matches!(
        invalidated_dependency_page,
        Err(prdb_engine::client_submit::ClientSubmitError::ServerRejected(_))
    ));
    drop(grant_cursor_owner);
    drop(grant_cursor_owner_credentials);
    drop(index_cursor_owner);
    drop(index_cursor_owner_credentials);
    drop(dependency_cursor_owner);
    drop(dependency_cursor_owner_credentials);
    drop(cursor_owner);
    drop(cursor_owner_credentials);

    admin.drop_index(revoked_index_one.index_id)?;
    admin.drop_index(revoked_index_two.index_id)?;

    // MANAGE_POLICIES authorizes DDL but does not disclose administrative
    // policy metadata after schema ownership is returned.
    let mut manager_cursor_credentials = read_client_credentials(&transfer_credential_path)?;
    let mut manager_cursor = AdminClient::connect_authenticated(
        address,
        manager_cursor_credentials.token_id,
        &mut manager_cursor_credentials.token_secret,
    )?;
    let manager_policy_page = manager_cursor.list_policies_page(
        created_schema.identity_id,
        created_table.table_id,
        1,
        None,
    )?;
    assert!(manager_policy_page.records.is_empty());
    assert!(manager_policy_page.next_cursor.is_none());
    drop(manager_cursor);
    drop(manager_cursor_credentials);
    for privilege in [
        prdb_engine::client_submit::AdminObjectPrivilege::Select,
        prdb_engine::client_submit::AdminObjectPrivilege::Update,
    ] {
        admin.revoke_object_privilege(
            policy_role.identity_id,
            created_table.table_id,
            privilege,
            prdb_engine::client_submit::AdminObjectGrantTarget::Object,
        )?;
    }
    admin.drop_policy(cursor_policy_one.policy_id)?;
    admin.drop_policy(cursor_policy_two.policy_id)?;
    assert!(
        admin
            .list_policies_page(created_schema.identity_id, created_table.table_id, 64, None)?
            .records
            .into_iter()
            .all(|policy| pre_policy_ids.contains(&policy.policy_id))
    );
    admin.drop_role(policy_role.identity_id)?;
    admin.revoke_object_privilege(
        transfer_owner.identity_id,
        created_schema.identity_id,
        AdminObjectPrivilege::Usage,
        AdminObjectGrantTarget::Object,
    )?;
    let columns_before_rename =
        admin.list_columns_page(created_schema.identity_id, created_table.table_id, 16, None)?;
    let payload_before_rename = columns_before_rename
        .records
        .iter()
        .find(|column| column.name == "payload")
        .ok_or("column rename process fixture lost its original payload column")?;
    let payload_column_id = payload_before_rename.column_id;
    let payload_ordinal = payload_before_rename.ordinal;
    let rename_column_sql_path = root.join("rename-column.sql");
    std::fs::write(
        &rename_column_sql_path,
        "ALTER TABLE process_admin_schema.process_admin_table RENAME COLUMN payload TO payload_renamed;",
    )?;
    let rename_column_cli = Command::new(env!("CARGO_BIN_EXE_prdb-client"))
        .args([
            "admin-sql",
            "--address",
            &address_text,
            "--credentials",
            admin_cli_credentials,
            "--sql-file",
            rename_column_sql_path
                .to_str()
                .ok_or("SQL path is not UTF-8")?,
        ])
        .output()?;
    assert!(
        rename_column_cli.status.success(),
        "admin-sql RENAME COLUMN failed: {}",
        String::from_utf8_lossy(&rename_column_cli.stderr)
    );
    assert!(String::from_utf8_lossy(&rename_column_cli.stdout).contains("renamed column"));
    let columns_after_rename =
        admin.list_columns_page(created_schema.identity_id, created_table.table_id, 16, None)?;
    let renamed_payload = columns_after_rename
        .records
        .iter()
        .find(|column| column.name == "payload_renamed")
        .ok_or("gateway RENAME COLUMN did not publish the new column name")?;
    assert_eq!(renamed_payload.column_id, payload_column_id);
    assert_eq!(renamed_payload.ordinal, payload_ordinal);
    assert!(
        columns_after_rename
            .records
            .iter()
            .all(|column| column.name != "payload")
    );
    for (suffix, sql, expected_output) in [
        (
            "set-default",
            "ALTER TABLE process_admin_schema.process_admin_table ALTER COLUMN payload_renamed SET DEFAULT 'process-default';",
            "set default on column payload_renamed",
        ),
        (
            "drop-default",
            "ALTER TABLE process_admin_schema.process_admin_table ALTER COLUMN payload_renamed DROP DEFAULT;",
            "dropped default on column payload_renamed",
        ),
    ] {
        let sql_path = root.join(format!("{suffix}-column-default.sql"));
        std::fs::write(&sql_path, sql)?;
        let result = Command::new(env!("CARGO_BIN_EXE_prdb-client"))
            .args([
                "admin-sql",
                "--address",
                &address_text,
                "--credentials",
                admin_cli_credentials,
                "--sql-file",
                sql_path.to_str().ok_or("SQL path is not UTF-8")?,
            ])
            .output()?;
        assert!(
            result.status.success(),
            "admin-sql {suffix} failed: {}",
            String::from_utf8_lossy(&result.stderr)
        );
        assert!(
            String::from_utf8_lossy(&result.stdout).contains(expected_output),
            "admin-sql {suffix} returned unexpected output: {}",
            String::from_utf8_lossy(&result.stdout)
        );
    }
    let renamed_table = admin.rename_table(created_table.table_id, "process_admin_table_v2")?;
    let dropped_source_table = admin.drop_table(source_table.table_id)?;
    let dropped_table = admin.drop_table(created_table.table_id)?;
    let created_by_grantee_id = admin
        .list_relations_page(created_schema.identity_id, 128, None)?
        .records
        .into_iter()
        .find(|relation| relation.name == "process_created_by_grantee")
        .map(|relation| relation.relation_id)
        .ok_or("CREATE grantee table was not visible during cleanup")?;
    let dropped_by_grantee_table = admin.drop_table(created_by_grantee_id)?;
    admin.revoke_object_privilege(
        transfer_owner.identity_id,
        created_schema.identity_id,
        AdminObjectPrivilege::Create,
        AdminObjectGrantTarget::Object,
    )?;
    assert!(dropped_source_table.commit_sequence > renamed_table.commit_sequence);
    assert!(dropped_table.commit_sequence > dropped_source_table.commit_sequence);
    assert!(dropped_by_grantee_table.commit_sequence > dropped_table.commit_sequence);
    let dropped_schema = admin.drop_schema(created_schema.identity_id)?;
    assert!(dropped_schema.commit_sequence > dropped_by_grantee_table.commit_sequence);
    let drop_sql_path = root.join("drop-user.sql");
    std::fs::write(
        &drop_sql_path,
        "DROP USER process_table_owner_renamed RESTRICT;",
    )?;
    let drop_sql_path_text = drop_sql_path.to_str().ok_or("SQL path is not UTF-8")?;
    let drop_user_cli = Command::new(env!("CARGO_BIN_EXE_prdb-client"))
        .args([
            "admin-sql",
            "--address",
            &address_text,
            "--credentials",
            admin_cli_credentials,
            "--sql-file",
            drop_sql_path_text,
        ])
        .output()?;
    assert!(
        drop_user_cli.status.success(),
        "user-drop CLI failed: {}",
        String::from_utf8_lossy(&drop_user_cli.stderr)
    );
    assert!(String::from_utf8_lossy(&drop_user_cli.stdout).contains("dropped user"));
    let mut dropped_user_credentials = read_client_credentials(&transfer_credential_path)?;
    assert!(
        SubmissionClient::connect_authenticated(
            address,
            dropped_user_credentials.token_id,
            &mut dropped_user_credentials.token_secret,
        )
        .is_err()
    );
    remove_client_credentials(&transfer_credential_path)?;
    let created_user = admin.create_user("process_token_user")?;
    let credential_path = root.join("issued-token.cred");
    let issued_user_id_text = encode_id_hex(&created_user.identity_id);
    let credential_path_text = credential_path
        .to_str()
        .ok_or("credential path is not UTF-8")?;
    let issue_token_cli = Command::new(env!("CARGO_BIN_EXE_prdb-client"))
        .args([
            "token-issue",
            "--address",
            &address_text,
            "--credentials",
            admin_cli_credentials,
            "--user-id",
            &issued_user_id_text,
            "--label",
            "multi-process token",
            "--ttl-seconds",
            "60",
            "--output-credentials",
            credential_path_text,
        ])
        .output()?;
    assert!(
        issue_token_cli.status.success(),
        "token-issue CLI failed: {}",
        String::from_utf8_lossy(&issue_token_cli.stderr)
    );
    let issued_cli_output = String::from_utf8_lossy(&issue_token_cli.stdout);
    assert!(issued_cli_output.contains("saved token"));
    assert!(!issued_cli_output.contains("token_secret"));
    let issued_token_id = read_client_credentials(&credential_path)?.token_id;

    let list_tokens_cli = Command::new(env!("CARGO_BIN_EXE_prdb-client"))
        .args([
            "token-list",
            "--address",
            &address_text,
            "--credentials",
            admin_cli_credentials,
            "--user-id",
            &issued_user_id_text,
        ])
        .output()?;
    assert!(
        list_tokens_cli.status.success(),
        "token-list CLI failed: {}",
        String::from_utf8_lossy(&list_tokens_cli.stderr)
    );
    let token_inventory = String::from_utf8_lossy(&list_tokens_cli.stdout);
    assert!(token_inventory.contains(&encode_id_hex(&issued_token_id)));
    assert!(token_inventory.contains("multi-process token"));
    assert!(!token_inventory.contains("token_secret"));

    let saved_credentials = read_client_credentials(&credential_path)?;
    let encode_secret_material = |bytes: &[u8]| {
        bytes
            .iter()
            .map(|byte| format!("{byte:02x}"))
            .collect::<String>()
    };
    let raw_secret_hex = encode_secret_material(&saved_credentials.token_secret);
    let verifier_hex = encode_secret_material(&prdb_engine::credentials::hash_token_secret(
        &saved_credentials.token_secret,
    ));
    assert!(!token_inventory.contains(&raw_secret_hex));
    assert!(!token_inventory.contains(&verifier_hex));
    let mut issued_secret = saved_credentials.token_secret;
    let mut issued_metadata = MetadataClient::connect_authenticated(
        address,
        saved_credentials.token_id,
        &mut issued_secret,
    )?;
    assert_eq!(issued_secret, [0; 32]);
    assert!(
        issued_metadata
            .list_schemas()?
            .iter()
            .any(|schema| schema.name == "information_schema")
    );
    drop(issued_metadata);
    drop(saved_credentials);

    let old_credentials = read_client_credentials(&credential_path)?;
    let mut preflight_old_secret = old_credentials.token_secret;
    let occupied_rotation_output = Command::new(env!("CARGO_BIN_EXE_prdb-client"))
        .args([
            "token-rotate",
            "--address",
            &address_text,
            "--credentials",
            admin_cli_credentials,
            "--token-id",
            &encode_id_hex(&issued_token_id),
            "--label",
            "must not rotate",
            "--ttl-seconds",
            "60",
            "--output-credentials",
            credential_path
                .to_str()
                .ok_or("credential path is not UTF-8")?,
        ])
        .output()?;
    assert!(!occupied_rotation_output.status.success());
    assert!(
        String::from_utf8_lossy(&occupied_rotation_output.stderr)
            .contains("token rotation was not attempted")
    );
    let mut preflight_metadata = MetadataClient::connect_authenticated(
        address,
        old_credentials.token_id,
        &mut preflight_old_secret,
    )?;
    assert_eq!(preflight_old_secret, [0; 32]);
    assert!(
        preflight_metadata
            .list_schemas()?
            .iter()
            .any(|schema| schema.name == "information_schema")
    );
    drop(preflight_metadata);

    let mut old_secret_after_rotation = old_credentials.token_secret;
    remove_client_credentials(&credential_path)?;
    let rotate_token_cli = Command::new(env!("CARGO_BIN_EXE_prdb-client"))
        .args([
            "token-rotate",
            "--address",
            &address_text,
            "--credentials",
            admin_cli_credentials,
            "--token-id",
            &encode_id_hex(&issued_token_id),
            "--label",
            "rotated process token",
            "--ttl-seconds",
            "60",
            "--output-credentials",
            credential_path
                .to_str()
                .ok_or("credential path is not UTF-8")?,
        ])
        .output()?;
    assert!(
        rotate_token_cli.status.success(),
        "token-rotate CLI failed: {}",
        String::from_utf8_lossy(&rotate_token_cli.stderr)
    );
    let rotate_output = String::from_utf8_lossy(&rotate_token_cli.stdout);
    assert!(rotate_output.contains("saved replacement token"));
    assert!(!rotate_output.contains("token_secret"));
    let rotated_credentials = read_client_credentials(&credential_path)?;
    let rotated_token_id = rotated_credentials.token_id;
    assert_ne!(rotated_token_id, issued_token_id);
    assert!(
        MetadataClient::connect_authenticated(
            address,
            issued_token_id,
            &mut old_secret_after_rotation
        )
        .is_err()
    );
    assert_eq!(old_secret_after_rotation, [0; 32]);
    let mut rotated_secret = rotated_credentials.token_secret;
    let mut rotated_metadata =
        MetadataClient::connect_authenticated(address, rotated_token_id, &mut rotated_secret)?;
    assert_eq!(rotated_secret, [0; 32]);
    assert!(
        rotated_metadata
            .list_schemas()?
            .iter()
            .any(|schema| schema.name == "information_schema")
    );
    drop(rotated_metadata);

    let revoke_token_cli = Command::new(env!("CARGO_BIN_EXE_prdb-client"))
        .args([
            "token-revoke",
            "--address",
            &address_text,
            "--credentials",
            admin_cli_credentials,
            "--token-id",
            &encode_id_hex(&rotated_token_id),
        ])
        .output()?;
    assert!(
        revoke_token_cli.status.success(),
        "token-revoke CLI failed: {}",
        String::from_utf8_lossy(&revoke_token_cli.stderr)
    );
    let revoked_inventory_cli = Command::new(env!("CARGO_BIN_EXE_prdb-client"))
        .args([
            "token-list",
            "--address",
            &address_text,
            "--credentials",
            admin_cli_credentials,
            "--user-id",
            &issued_user_id_text,
        ])
        .output()?;
    assert!(
        revoked_inventory_cli.status.success(),
        "token-list after revoke failed: {}",
        String::from_utf8_lossy(&revoked_inventory_cli.stderr)
    );
    let revoked_inventory = String::from_utf8_lossy(&revoked_inventory_cli.stdout);
    assert!(revoked_inventory.contains("\"revoked\":true"));
    let revoked_credentials = read_client_credentials(&credential_path)?;
    let mut revoked_secret = revoked_credentials.token_secret;
    assert!(
        MetadataClient::connect_authenticated(
            address,
            revoked_credentials.token_id,
            &mut revoked_secret,
        )
        .is_err()
    );
    assert_eq!(revoked_secret, [0; 32]);
    let credential_path_text = credential_path
        .to_str()
        .ok_or("credential path is not UTF-8")?;
    let remove_credential_command = Command::new(env!("CARGO_BIN_EXE_prdb-client"))
        .args(["credential-remove", "--credentials", credential_path_text])
        .output()?;
    assert!(
        remove_credential_command.status.success(),
        "credential-remove CLI failed: {}",
        String::from_utf8_lossy(&remove_credential_command.stderr)
    );
    assert!(
        String::from_utf8_lossy(&remove_credential_command.stdout)
            .contains("removed credential file")
    );
    assert!(!credential_path.exists());
    let mut no_login_admin_credentials = read_client_credentials(&admin_cancel_credentials_path)?;
    let mut no_login_admin = AdminClient::connect_authenticated(
        address,
        no_login_admin_credentials.token_id,
        &mut no_login_admin_credentials.token_secret,
    )?;
    let no_login_user = no_login_admin
        .execute_create_identity_sql("CREATE USER sql_service NOLOGIN")
        .map_err(|error| format!("CREATE USER NOLOGIN failed: {error:?}"))?;
    assert!(
        no_login_admin
            .execute_create_identity_if_not_exists_sql(
                "CREATE USER IF NOT EXISTS sql_service LOGIN"
            )?
            .is_none()
    );
    let conditional_role_sql_path = root.join("create-role-if-not-exists.sql");
    std::fs::write(
        &conditional_role_sql_path,
        "CREATE ROLE IF NOT EXISTS sql_conditional_role;",
    )?;
    let create_conditional_role = Command::new(env!("CARGO_BIN_EXE_prdb-client"))
        .args([
            "admin-sql",
            "--address",
            &address.to_string(),
            "--credentials",
            admin_cancel_credentials_path
                .to_str()
                .ok_or("credential path is not UTF-8")?,
            "--sql-file",
            conditional_role_sql_path
                .to_str()
                .ok_or("SQL path is not UTF-8")?,
        ])
        .output()?;
    assert!(
        create_conditional_role.status.success(),
        "conditional CREATE ROLE failed: {}",
        String::from_utf8_lossy(&create_conditional_role.stderr)
    );
    assert!(String::from_utf8_lossy(&create_conditional_role.stdout).contains("created role"));
    let repeat_conditional_role = Command::new(env!("CARGO_BIN_EXE_prdb-client"))
        .args([
            "admin-sql",
            "--address",
            &address.to_string(),
            "--credentials",
            admin_cancel_credentials_path
                .to_str()
                .ok_or("credential path is not UTF-8")?,
            "--sql-file",
            conditional_role_sql_path
                .to_str()
                .ok_or("SQL path is not UTF-8")?,
        ])
        .output()?;
    assert!(
        repeat_conditional_role.status.success(),
        "repeated conditional CREATE ROLE failed: {}",
        String::from_utf8_lossy(&repeat_conditional_role.stderr)
    );
    assert!(
        String::from_utf8_lossy(&repeat_conditional_role.stdout)
            .contains("role already exists; no operation committed")
    );
    let token_expiry = i64::try_from(SystemTime::now().duration_since(UNIX_EPOCH)?.as_micros())?
        .saturating_add(60_000_000);
    assert!(
        no_login_admin
            .issue_token(
                no_login_user.identity_id,
                "must stay disabled",
                token_expiry
            )
            .is_err()
    );
    let mut conditional_drop_credentials = read_client_credentials(&admin_cancel_credentials_path)?;
    let mut conditional_drop_admin = AdminClient::connect_authenticated(
        address,
        conditional_drop_credentials.token_id,
        &mut conditional_drop_credentials.token_secret,
    )?;
    conditional_drop_admin
        .execute_role_membership_sql("GRANT sql_conditional_role TO USER sql_service")?;
    assert!(
        conditional_drop_admin
            .execute_drop_identity_if_exists_sql(
                "DROP ROLE IF EXISTS sql_conditional_role RESTRICT"
            )
            .is_err(),
        "RESTRICT must reject dropping a role that still has a member"
    );
    drop(conditional_drop_admin);
    drop(conditional_drop_credentials);
    let mut membership_cleanup_credentials =
        read_client_credentials(&admin_cancel_credentials_path)?;
    let mut membership_cleanup_admin = AdminClient::connect_authenticated(
        address,
        membership_cleanup_credentials.token_id,
        &mut membership_cleanup_credentials.token_secret,
    )?;
    membership_cleanup_admin
        .execute_role_membership_sql("REVOKE sql_conditional_role FROM USER sql_service")?;
    assert!(
        membership_cleanup_admin
            .execute_drop_identity_if_exists_sql(
                "DROP ROLE IF EXISTS missing_conditional_role RESTRICT"
            )?
            .is_none()
    );
    assert!(
        membership_cleanup_admin
            .execute_drop_identity_if_exists_sql("DROP USER IF EXISTS sql_service RESTRICT")?
            .is_some()
    );
    assert!(
        membership_cleanup_admin
            .execute_drop_identity_if_exists_sql("DROP USER IF EXISTS sql_service RESTRICT")?
            .is_none()
    );
    assert!(
        membership_cleanup_admin
            .execute_drop_identity_if_exists_sql(
                "DROP ROLE IF EXISTS sql_conditional_role RESTRICT"
            )?
            .is_some()
    );
    assert!(
        membership_cleanup_admin
            .execute_drop_identity_if_exists_sql(
                "DROP ROLE IF EXISTS sql_conditional_role RESTRICT"
            )?
            .is_none()
    );
    drop(membership_cleanup_admin);
    drop(membership_cleanup_credentials);
    drop(no_login_admin);
    drop(no_login_admin_credentials);
    remove_client_credentials(&admin_cancel_credentials_path)?;
    drop(admin);

    let mut outcome_status_client =
        SubmissionClient::connect_authenticated(address, token_id, &mut outcome_status_secret)?;
    assert_eq!(
        outcome_status_client.status(committed_invocation_id)?,
        InvocationStatus::Committed,
        "the supervisor must recover a committed outcome after storage and terminal delivery end"
    );
    drop(outcome_status_client);

    assert!(gateway.stop()?.success());
    assert!(supervisor.stop()?.success());
    Ok(())
}
