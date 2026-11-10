#![cfg(all(unix, target_os = "linux", target_arch = "x86_64"))]

use prdb_engine::audit::{AuditAction, AuditEvent, AuditResult};
use prdb_engine::bootstrap::initialize;
use prdb_engine::client_credentials::{
    read_client_credentials, remove_client_credentials, write_client_credentials,
};
use prdb_engine::client_protocol::ClientCredentials;
use prdb_engine::client_submit::{
    AdminClient, AdminColumnDefault, AdminColumnDefinition, AdminCreatePolicyRequest,
    AdminObjectGrantTarget, AdminObjectPrivilege, AdminPolicyEffect, AdminPolicyKind,
    AdminPolicyOperation, AdminPolicyRecipient, AdminSystemPrivilege, CancellationResult,
    InvocationStatus, MetadataClient, SubmissionClient, TerminalOutcome,
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
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

struct ChildGuard(Option<Child>);

impl ChildGuard {
    fn child(&mut self) -> &mut Child {
        self.0.as_mut().expect("child process still running")
    }

    fn stop(&mut self) -> std::io::Result<std::process::ExitStatus> {
        let mut child = self.0.take().expect("child process still running");
        // SAFETY: the PID belongs to the child process held by this guard.
        unsafe {
            libc::kill(child.id() as libc::pid_t, libc::SIGTERM);
        }
        let deadline = Instant::now() + Duration::from_secs(5);
        loop {
            if let Some(status) = child.try_wait()? {
                return Ok(status);
            }
            if Instant::now() >= deadline {
                let _ = child.kill();
                return child.wait();
            }
            std::thread::sleep(Duration::from_millis(10));
        }
    }

    fn kill(&mut self) -> std::io::Result<std::process::ExitStatus> {
        let mut child = self.0.take().expect("child process still running");
        let _ = child.kill();
        child.wait()
    }
}

impl Drop for ChildGuard {
    fn drop(&mut self) {
        if let Some(mut child) = self.0.take() {
            let _ = child.kill();
            let _ = child.wait();
        }
    }
}

#[test]
fn authenticated_metadata_and_admin_lifecycle_cross_gateway_and_supervisor_processes()
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
    let restricted_table_id;
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
    let mut outcome_status_secret = token_secret;

    let network_sql_package = if let Some(artifact_path) =
        std::env::var_os("PRDB_TEST_RESTRICTED_METADATA_QUERY_ARTIFACT")
    {
        let artifact = std::fs::read(artifact_path)?;
        let manifest = manifest_v1_from_elf(
            &artifact,
            3,
            1,
            "x86_64-v1".to_owned(),
            "test-runtime".to_owned(),
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
        "test-runtime",
        "--sdk-release",
        "1",
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
        MetadataClient::connect_authenticated(address, owner_token, &mut owner_secret)?;
    assert!(
        owner_metadata
            .list_schemas()?
            .iter()
            .any(|schema| schema.name == "admin_private")
    );
    assert!(
        owner_metadata
            .list_relations(restricted_schema_id)?
            .iter()
            .any(|relation| relation.relation_id == restricted_table_id)
    );
    assert!(
        !owner_metadata
            .list_columns(restricted_schema_id, restricted_table_id)?
            .is_empty()
    );
    drop(owner_metadata);

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
    let created_role = admin.create_role("process_admin_role")?;
    assert_ne!(created_role.identity_id, [0; 16]);
    let renamed = admin.rename_role(created_role.identity_id, "process_admin_role_v2")?;
    assert!(renamed.commit_sequence > created_role.commit_sequence);
    let granted = admin
        .grant_system_privilege(created_role.identity_id, AdminSystemPrivilege::CreateSchema)?;
    let revoked = admin
        .revoke_system_privilege(created_role.identity_id, AdminSystemPrivilege::CreateSchema)?;
    assert!(revoked.commit_sequence > granted.commit_sequence);
    let dropped = admin.drop_role(created_role.identity_id)?;
    assert!(dropped.commit_sequence > revoked.commit_sequence);

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
    let transfer_owner = admin.create_user("process_table_owner")?;
    let token_expiration =
        i64::try_from(SystemTime::now().duration_since(UNIX_EPOCH)?.as_micros())?
            .saturating_add(900_000_000);
    let transfer_credentials = admin.issue_token(
        transfer_owner.identity_id,
        "table owner transfer",
        token_expiration,
    )?;
    let transfer_token_id = transfer_credentials.token_id;
    let transfer_credential_path = root.join("table-owner.cred");
    let transfer_client_credentials = ClientCredentials {
        token_id: transfer_token_id,
        token_secret: *transfer_credentials.token_secret(),
    };
    write_client_credentials(&transfer_credential_path, &transfer_client_credentials)?;
    drop(transfer_client_credentials);
    drop(transfer_credentials);
    let address_text = address.to_string();
    let admin_owner_id_text = encode_id_hex(&admin_user_id);
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
    admin.revoke_token(transfer_token_id)?;
    remove_client_credentials(&transfer_credential_path)?;
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
    let unique_constraint = admin
        .create_unique_constraint(created_table.table_id, &created_table.column_ids)
        .map_err(|error| format!("process ADMIN create_unique_constraint failed: {error}"))?;
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
    let foreign_key = admin
        .create_foreign_key(
            source_table.table_id,
            &source_table.column_ids,
            unique_constraint.constraint_id,
        )
        .map_err(|error| format!("process ADMIN create_foreign_key failed: {error}"))?;
    let dropped_foreign_key = admin
        .drop_foreign_key(foreign_key.foreign_key_id)
        .map_err(|error| format!("process ADMIN drop_foreign_key failed: {error}"))?;
    assert!(dropped_foreign_key.commit_sequence > foreign_key.commit_sequence);
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
    let dropped_unique = admin
        .drop_unique_constraint(unique_constraint.constraint_id)
        .map_err(|error| format!("process ADMIN drop_unique_constraint failed: {error}"))?;
    assert!(dropped_unique.commit_sequence > unique_constraint.commit_sequence);
    let check_constraint = admin.create_check_constraint(
        created_table.table_id,
        &prdb_engine::authorization::Expr::Literal(Some(prdb_engine::tuple::ScalarValue::Boolean(
            true,
        ))),
        &[],
    )?;
    let dropped_check = admin
        .drop_check_constraint(check_constraint.constraint_id)
        .map_err(|error| format!("process ADMIN drop_check_constraint failed: {error}"))?;
    assert!(dropped_check.commit_sequence > check_constraint.commit_sequence);
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
    let dropped_policy = admin.drop_policy(policy.policy_id)?;
    assert!(dropped_policy.commit_sequence > policy.commit_sequence);
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
    admin.alter_column_nullability(created_table.table_id, created_table.column_ids[0], false)?;
    admin.alter_column_nullability(created_table.table_id, created_table.column_ids[0], true)?;
    admin.set_column_default(
        created_table.table_id,
        created_table.column_ids[0],
        ScalarType::Text,
        AdminColumnDefault::Literal(prdb_engine::tuple::ScalarValue::Text("seed".to_owned())),
    )?;
    admin.drop_column_default(created_table.table_id, created_table.column_ids[0])?;
    let created_index = admin
        .create_index(
            created_table.table_id,
            "process_admin_index",
            &created_table.column_ids,
            false,
        )
        .map_err(|error| format!("process ADMIN create_index failed: {error}"))?;
    let rebuilt_index = admin
        .rebuild_index(created_index.index_id, &created_table.column_ids, true)
        .map_err(|error| format!("process ADMIN rebuild_index failed: {error}"))?;
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
    remove_client_credentials(&admin_cancel_credentials_path)?;
    let renamed_table = admin.rename_table(created_table.table_id, "process_admin_table_v2")?;
    let dropped_source_table = admin.drop_table(source_table.table_id)?;
    let dropped_table = admin.drop_table(created_table.table_id)?;
    assert!(dropped_source_table.commit_sequence > renamed_table.commit_sequence);
    assert!(dropped_table.commit_sequence > dropped_source_table.commit_sequence);
    let dropped_schema = admin.drop_schema(created_schema.identity_id)?;
    assert!(dropped_schema.commit_sequence > dropped_table.commit_sequence);

    let created_user = admin.create_user("process_token_user")?;
    let now_us = i64::try_from(SystemTime::now().duration_since(UNIX_EPOCH)?.as_micros())?;
    let issued = admin.issue_token(
        created_user.identity_id,
        "multi-process token",
        now_us.saturating_add(60_000_000),
    )?;
    let issued_token_id = issued.token_id;
    let credential_path = root.join("issued-token.cred");
    let issued_credentials = ClientCredentials {
        token_id: issued.token_id,
        token_secret: *issued.token_secret(),
    };
    write_client_credentials(&credential_path, &issued_credentials)?;
    drop(issued_credentials);
    drop(issued);

    let saved_credentials = read_client_credentials(&credential_path)?;
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

    admin.revoke_token(issued_token_id)?;
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

#[test]
#[ignore = "requires a delegated cgroup and configured sandbox runtime paths"]
fn disconnected_submission_is_reconciled_after_gateway_restart()
-> Result<(), Box<dyn std::error::Error>> {
    fn required_paths(name: &str) -> Result<Vec<std::path::PathBuf>, Box<dyn std::error::Error>> {
        let value = std::env::var_os(name).ok_or(format!("{name} is required"))?;
        if name == "PRDB_TEST_RUNTIME_READ_ONLY" && value.is_empty() {
            return Ok(Vec::new());
        }
        let paths = std::env::split_paths(&value).collect::<Vec<_>>();
        if paths.is_empty() || paths.iter().any(|path| !path.is_absolute()) {
            return Err(format!("{name} must contain absolute paths").into());
        }
        Ok(paths)
    }

    fn start_gateway(
        service_socket: &std::path::Path,
        uid: u32,
    ) -> Result<(ChildGuard, std::net::SocketAddr), Box<dyn std::error::Error>> {
        let mut gateway = ChildGuard(Some(
            Command::new(env!("CARGO_BIN_EXE_prdb-gateway"))
                .args([
                    "--bind",
                    "127.0.0.1:0",
                    "--service-socket",
                    service_socket.to_str().ok_or("non-UTF-8 socket path")?,
                    "--service-uid",
                    &uid.to_string(),
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
        let (sender, receiver) = std::sync::mpsc::channel();
        std::thread::spawn(move || {
            let mut line = String::new();
            let result = BufReader::new(stderr).read_line(&mut line).map(|_| line);
            let _ = sender.send(result);
        });
        let line = receiver.recv_timeout(Duration::from_secs(5))??;
        let address = line
            .trim()
            .strip_prefix("prdb-gateway listening on ")
            .ok_or("gateway listen line had an unexpected prefix")?
            .parse()?;
        Ok((gateway, address))
    }

    let nonce = SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos();
    let root = std::env::temp_dir().join(format!(
        "prdb-outcome-process-{}-{nonce}",
        std::process::id()
    ));
    std::fs::create_dir(&root)?;
    std::fs::set_permissions(&root, std::fs::Permissions::from_mode(0o700))?;
    let _cleanup = DirectoryGuard(root.clone());

    let cgroup_parent = std::env::var_os("PRDB_TEST_CGROUP_PARENT")
        .map(std::path::PathBuf::from)
        .ok_or("PRDB_TEST_CGROUP_PARENT must name a delegated cgroup v2 parent")?;
    if !cgroup_parent.is_absolute() {
        return Err("PRDB_TEST_CGROUP_PARENT must be absolute".into());
    }
    let runtime_read_execute = required_paths("PRDB_TEST_RUNTIME_READ_EXECUTE")?;
    let runtime_read_only = required_paths("PRDB_TEST_RUNTIME_READ_ONLY")?;

    let source = root.join("query.c");
    let object_path = root.join("query.o");
    let artifact_path = root.join("query.so");
    std::fs::write(
        &source,
        "#include <stddef.h>\n\
         #include \"prdb_query_abi_v3.h\"\n\
         const prdb_query_descriptor_v3_record prdb_query_descriptor_v3 = {\n\
             sizeof(prdb_query_descriptor_v3_record), 3,\n\
             sizeof(prdb_host_api_v3), sizeof(prdb_invocation_v3)\n\
         };\n\
         int32_t prdb_query_main_v3(const prdb_host_api_v3 *host,\n\
             const prdb_invocation_v3 *invocation) {\n\
             static const uint8_t ready[] = \"QUERY_READY\";\n\
             (void)invocation;\n\
             if (host == NULL || host->write_result == NULL ||\n\
                 host->write_result(host->context, ready, sizeof(ready) - 1) != 0) return 1;\n\
             for (;;) {}\n\
         }\n",
    )?;
    let include_dir = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("include");
    let compile = Command::new("tcc")
        .arg("-fPIC")
        .arg("-c")
        .arg("-I")
        .arg(include_dir)
        .arg(&source)
        .arg("-o")
        .arg(&object_path)
        .output()?;
    if !compile.status.success() {
        return Err(format!(
            "TinyCC failed: {}",
            String::from_utf8_lossy(&compile.stderr)
        )
        .into());
    }
    let link = Command::new("cc")
        .arg("-shared")
        .arg("-Wl,-z,noexecstack")
        .arg(&object_path)
        .arg("-o")
        .arg(&artifact_path)
        .env_remove("NIX_LDFLAGS")
        .env("NIX_DONT_SET_RPATH", "1")
        .output()?;
    if !link.status.success() {
        return Err(format!(
            "shared linker failed: {}",
            String::from_utf8_lossy(&link.stderr)
        )
        .into());
    }
    let artifact = std::fs::read(&artifact_path)?;
    let manifest_value = manifest_v1_from_elf(
        &artifact,
        3,
        1,
        "x86_64-v1".to_owned(),
        "test-runtime".to_owned(),
        "TinyCC process integration fixture".to_owned(),
    )
    .map_err(|error| format!("derive TinyCC fixture manifest: {error:?}"))?;
    let manifest = encode_manifest_v1(&manifest_value)
        .map_err(|error| format!("encode TinyCC fixture manifest: {error:?}"))?;
    let dependencies = manifest_value.dependencies.clone();
    let package = root.join("package");
    std::fs::create_dir(&package)?;
    std::fs::set_permissions(&package, std::fs::Permissions::from_mode(0o700))?;
    let mut requested_scope = b"PSC1\0\x01\x01\x01".to_vec();
    requested_scope.extend_from_slice(&[1; 16]);
    for (name, bytes) in [
        ("query.so", artifact.as_slice()),
        ("manifest.pmf1", manifest.as_slice()),
        // One canonical schema-USAGE capability; the query performs no data access.
        ("requested_scope.psc1", requested_scope.as_slice()),
    ] {
        std::fs::write(package.join(name), bytes)?;
        std::fs::set_permissions(package.join(name), std::fs::Permissions::from_mode(0o600))?;
    }
    let artifact_hash = Sha256::digest(&artifact);
    let hash_sidecar = format!(
        "{}\n",
        artifact_hash
            .iter()
            .map(|byte| format!("{byte:02x}"))
            .collect::<String>()
    );
    std::fs::write(package.join("artifact.sha256"), hash_sidecar)?;
    std::fs::set_permissions(
        package.join("artifact.sha256"),
        std::fs::Permissions::from_mode(0o600),
    )?;

    let instance_dir = root.join("instance");
    let mut setup = initialize(&instance_dir, "outcome_process_admin", i64::MAX)?;
    let token_id = setup.token_id();
    let token_secret = setup.take_secret().ok_or("bootstrap secret missing")?;
    drop(setup.acknowledge()?);

    let service_socket = root.join("supervisor.sock");
    let uid = unsafe { libc::geteuid() };
    let uid_string = uid.to_string();
    let cgroup_string = cgroup_parent.to_str().ok_or("non-UTF-8 cgroup path")?;
    let mut supervisor_command = Command::new(env!("CARGO_BIN_EXE_prdb-supervisor"));
    supervisor_command.args([
        "--instance-dir",
        instance_dir.to_str().ok_or("non-UTF-8 instance path")?,
        "--service-socket",
        service_socket.to_str().ok_or("non-UTF-8 socket path")?,
        "--gateway-uid",
        &uid_string,
        "--cgroup-parent",
        cgroup_string,
        "--executor",
        env!("CARGO_BIN_EXE_prdb-executor"),
        "--optimizer",
        env!("CARGO_BIN_EXE_prdb-optimizer"),
        "--scratch-parent",
        root.to_str().ok_or("non-UTF-8 scratch path")?,
        "--cpu-baseline",
        "x86_64-v1",
        "--runtime-profile",
        "test-runtime",
        "--sdk-release",
        "1",
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
    for path in runtime_read_execute {
        supervisor_command.arg("--runtime-read-execute").arg(path);
    }
    for path in runtime_read_only {
        supervisor_command.arg("--runtime-read-only").arg(path);
    }
    for dependency in dependencies {
        supervisor_command
            .arg("--allowed-dependency")
            .arg(dependency);
    }
    let mut supervisor = ChildGuard(Some(
        supervisor_command
            .stdout(Stdio::null())
            .stderr(Stdio::inherit())
            .spawn()?,
    ));
    let deadline = Instant::now() + Duration::from_secs(5);
    while !service_socket.exists() {
        if let Some(status) = supervisor.child().try_wait()? {
            return Err(format!("supervisor exited before binding: {status}").into());
        }
        if Instant::now() >= deadline {
            return Err("supervisor did not bind its service socket".into());
        }
        std::thread::sleep(Duration::from_millis(10));
    }

    let (mut gateway, address) = start_gateway(&service_socket, uid)?;
    let mut admin_secret = token_secret;
    let mut admin = AdminClient::connect_authenticated(address, token_id, &mut admin_secret)?;
    let query_user = admin
        .create_user("process_query_owner")
        .map_err(|error| format!("create query user failed: {error:?}"))?;
    let query_schema = admin
        .create_schema("process_query_schema", query_user.identity_id)
        .map_err(|error| format!("create query schema failed: {error:?}"))?;
    admin
        .grant_object_privilege(
            query_user.identity_id,
            query_schema.identity_id,
            AdminObjectPrivilege::Usage,
            AdminObjectGrantTarget::Object,
        )
        .map_err(|error| format!("grant query schema usage failed: {error:?}"))?;
    let mut requested_scope = b"PSC1\0\x01\x01\x01".to_vec();
    requested_scope.extend_from_slice(&query_schema.identity_id);
    std::fs::write(package.join("requested_scope.psc1"), requested_scope)?;
    std::fs::set_permissions(
        package.join("requested_scope.psc1"),
        std::fs::Permissions::from_mode(0o600),
    )?;
    let query_token_expiry =
        i64::try_from(SystemTime::now().duration_since(UNIX_EPOCH)?.as_micros())? + 3_600_000_000;
    let query_token = admin
        .issue_token(query_user.identity_id, "process query", query_token_expiry)
        .map_err(|error| format!("issue query token failed: {error:?}"))?;
    let query_token_id = query_token.token_id;
    let mut query_secret = *query_token.token_secret();
    let mut restart_query_secret = query_secret;
    let mut recovery_query_secret = query_secret;
    let mut client =
        SubmissionClient::connect_authenticated(address, query_token_id, &mut query_secret)?;
    let invocation_id = client
        .upload_and_seal(&package, &[], 30_000, WorkloadClass::Operational)
        .map_err(|error| format!("upload/seal failed: {error:?}"))?;
    let receipt = root.join("start.receipt");
    let cancellation_requested_at;
    {
        let mut started = client
            .start(invocation_id, &receipt)
            .map_err(|error| format!("query START failed: {error:?}"))?;
        let ready_event = started.next_event()?;
        assert!(
            matches!(ready_event, Some(prdb_engine::client_submit::ClientResultEvent::Data(ref bytes)) if bytes == b"QUERY_READY"),
            "unexpected query startup event: {ready_event:?}"
        );
        let active_status = admin
            .status(invocation_id)
            .map_err(|error| format!("active STATUS failed: {error:?}"))?;
        assert_eq!(active_status, InvocationStatus::Active);
        let cancellation = admin
            .cancel_invocation(invocation_id)
            .map_err(|error| format!("active CANCEL failed: {error:?}"))?;
        assert_eq!(cancellation, CancellationResult::Accepted);
        cancellation_requested_at = Instant::now();
        let _gateway_crash_status = gateway.kill()?;
    }
    drop(client);

    let (mut replacement_gateway, replacement_address) = start_gateway(&service_socket, uid)?;
    let mut status_client = SubmissionClient::connect_authenticated(
        replacement_address,
        query_token_id,
        &mut restart_query_secret,
    )?;
    let status_deadline = Instant::now() + Duration::from_secs(10);
    let status = loop {
        let status = status_client.status(invocation_id)?;
        if matches!(
            status,
            InvocationStatus::Committed | InvocationStatus::Aborted
        ) {
            break status;
        }
        if Instant::now() >= status_deadline {
            return Err(
                format!("invocation did not reach a durable terminal status: {status:?}").into(),
            );
        }
        std::thread::sleep(Duration::from_millis(20));
    };
    assert_eq!(status, InvocationStatus::Aborted);
    assert!(
        cancellation_requested_at.elapsed() < Duration::from_secs(2),
        "non-cooperative worker did not reach durable ABORTED within two seconds"
    );
    drop(status_client);
    drop(admin);
    assert!(replacement_gateway.stop()?.success());
    assert!(supervisor.stop()?.success());
    assert!(!service_socket.exists(), "supervisor left a stale socket");

    let mut restarted_supervisor_command = Command::new(env!("CARGO_BIN_EXE_prdb-supervisor"));
    restarted_supervisor_command.args([
        "--instance-dir",
        instance_dir.to_str().ok_or("non-UTF-8 instance path")?,
        "--service-socket",
        service_socket.to_str().ok_or("non-UTF-8 socket path")?,
        "--gateway-uid",
        &uid_string,
        "--cgroup-parent",
        cgroup_string,
        "--executor",
        env!("CARGO_BIN_EXE_prdb-executor"),
        "--optimizer",
        env!("CARGO_BIN_EXE_prdb-optimizer"),
        "--scratch-parent",
        root.to_str().ok_or("non-UTF-8 scratch path")?,
        "--cpu-baseline",
        "x86_64-v1",
        "--runtime-profile",
        "test-runtime",
        "--sdk-release",
        "1",
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
    for path in required_paths("PRDB_TEST_RUNTIME_READ_EXECUTE")? {
        restarted_supervisor_command
            .arg("--runtime-read-execute")
            .arg(path);
    }
    for path in required_paths("PRDB_TEST_RUNTIME_READ_ONLY")? {
        restarted_supervisor_command
            .arg("--runtime-read-only")
            .arg(path);
    }
    for dependency in &manifest_value.dependencies {
        restarted_supervisor_command
            .arg("--allowed-dependency")
            .arg(dependency);
    }
    let mut restarted_supervisor = ChildGuard(Some(
        restarted_supervisor_command
            .stdout(Stdio::null())
            .stderr(Stdio::inherit())
            .spawn()?,
    ));
    let restart_deadline = Instant::now() + Duration::from_secs(5);
    while !service_socket.exists() {
        if let Some(status) = restarted_supervisor.child().try_wait()? {
            return Err(format!("restarted supervisor exited before binding: {status}").into());
        }
        if Instant::now() >= restart_deadline {
            return Err("restarted supervisor did not bind its service socket".into());
        }
        std::thread::sleep(Duration::from_millis(10));
    }
    let (mut recovered_gateway, recovered_address) = start_gateway(&service_socket, uid)?;
    let mut recovered_status_client = SubmissionClient::connect_authenticated(
        recovered_address,
        query_token_id,
        &mut recovery_query_secret,
    )?;
    assert_eq!(
        recovered_status_client.status(invocation_id)?,
        InvocationStatus::Aborted,
        "ABORTED must survive closing and reopening the storage process"
    );
    drop(recovered_status_client);
    assert!(recovered_gateway.stop()?.success());
    assert!(restarted_supervisor.stop()?.success());
    Ok(())
}

struct DirectoryGuard(std::path::PathBuf);

fn encode_id_hex(id: &[u8; 16]) -> String {
    let mut encoded = String::with_capacity(32);
    for byte in id {
        use std::fmt::Write as _;
        write!(&mut encoded, "{byte:02x}").expect("writing to String cannot fail");
    }
    encoded
}

impl Drop for DirectoryGuard {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}
