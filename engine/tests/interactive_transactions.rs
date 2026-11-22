use prdb_engine::audit::{AuditAction, AuditEvent, AuditResult};
use prdb_engine::bootstrap::initialize;
use prdb_engine::client_protocol::{
    CLIENT_V2_TX_ATTACH_KIND, CLIENT_V2_TX_BEGIN_KIND, CLIENT_V2_TX_COMMIT_KIND,
    ClientConnectionSession, ClientFrame, ClientMode, ClientV2Request, ClientV2Response,
    ClientV2TransactionState,
};
use prdb_engine::storage_manager::{
    InteractivePackageExecutionError, InteractivePackageRequest, InteractivePackageState,
    InteractiveTransactionError, InteractiveTransactionState, StorageManager, StorageManagerConfig,
    TokenIssueRequest,
};
use prdb_engine::supervisor::{
    StagingLimits, StagingStore, SubmissionMetadata, SubmissionMetadataValidator,
};
use std::sync::Arc;
use std::sync::Barrier;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use zeroize::Zeroize;

static NEXT: AtomicU64 = AtomicU64::new(0);

struct Fixture(std::path::PathBuf);

impl Fixture {
    fn new() -> Self {
        let id = NEXT.fetch_add(1, Ordering::Relaxed);
        let directory =
            std::env::temp_dir().join(format!("prdb-interactive-tx-{}-{id}", std::process::id()));
        Self(directory)
    }
}

#[test]
fn supervisor_package_handle_shares_context_and_leaves_commit_to_controller() {
    let fixture = Fixture::new();
    let mut bootstrap = initialize(&fixture.0, "interactive_admin", now_us() + 86_400_000_000)
        .expect("initialize database");
    let token_id = bootstrap.token_id();
    let mut token_secret = bootstrap.take_secret().expect("bootstrap token secret");
    let instance = bootstrap.acknowledge().expect("publish READY");
    let storage = StorageManager::new(instance.database(), Duration::from_secs(2)).unwrap();
    let begun = storage
        .begin_interactive_transaction(token_id, &token_secret, now_us() + 86_400_000_000)
        .unwrap();

    storage
        .execute_interactive_package_with_supervisor(
            package_request(
                token_id,
                &token_secret,
                begun.transaction_id,
                [0x31; 16],
                [0x41; 32],
            ),
            |supervisor| {
                assert_eq!(supervisor.invocation_id(), begun.transaction_id);
                assert!(supervisor.principal_id() != [0; 16]);
                assert!(!supervisor.role_ids().is_empty());
                let page = supervisor.scan_information_schema_schemata(32).unwrap();
                assert!(!page.rows.is_empty());
                Ok::<_, &'static str>(())
            },
        )
        .unwrap();
    assert_eq!(
        storage
            .interactive_transaction_status(token_id, &token_secret, begun.transaction_id)
            .unwrap(),
        InteractiveTransactionState::Active
    );
    assert_eq!(
        storage
            .interactive_package_status(token_id, &token_secret, begun.transaction_id, [0x31; 16],)
            .unwrap()
            .state,
        InteractivePackageState::Completed
    );
    assert!(
        storage
            .commit_interactive_transaction(token_id, &token_secret, begun.transaction_id)
            .unwrap()
            > 0
    );

    let failed = storage
        .begin_interactive_transaction(token_id, &token_secret, now_us() + 86_400_000_000)
        .unwrap();
    assert!(matches!(
        storage.execute_interactive_package_with_supervisor(
            package_request(
                token_id,
                &token_secret,
                failed.transaction_id,
                [0x32; 16],
                [0x42; 32],
            ),
            |_| Err::<(), _>("worker failed"),
        ),
        Err(InteractivePackageExecutionError::Execution("worker failed"))
    ));
    assert_eq!(
        storage
            .interactive_transaction_status(token_id, &token_secret, failed.transaction_id)
            .unwrap(),
        InteractiveTransactionState::AbortOnly
    );
    token_secret.zeroize();
}

#[test]
fn runas_actor_proof_checks_normalized_name_and_sets_package_principal() {
    let fixture = Fixture::new();
    let mut bootstrap = initialize(&fixture.0, "interactive_admin", now_us() + 86_400_000_000)
        .expect("initialize database");
    let token_id = bootstrap.token_id();
    let mut token_secret = bootstrap.take_secret().expect("bootstrap token secret");
    let instance = bootstrap.acknowledge().expect("publish READY");
    let storage = StorageManager::new(instance.database(), Duration::from_secs(2)).unwrap();
    let controller = storage
        .authenticate_query_session(token_id, &token_secret)
        .unwrap();
    let expires_at_us = now_us() + 3_600_000_000;
    let actor = storage
        .create_user(
            [0x61; 16],
            controller.principal_id(),
            "runas_actor",
            expires_at_us,
            test_audit_event(
                [0x61; 16],
                controller.principal_id(),
                AuditAction::Ddl,
                vec![],
            ),
        )
        .expect("create distinct RUNAS user");
    let mut actor_token = storage
        .issue_token(TokenIssueRequest {
            invocation_id: [0x62; 16],
            authenticated_actor_id: controller.principal_id(),
            user_id: actor.user_id,
            label: "runas-test",
            expires_at_us,
            retention_deadline_us: expires_at_us,
            audit_event: test_audit_event(
                [0x62; 16],
                controller.principal_id(),
                AuditAction::TokenLifecycle,
                vec![actor.user_id],
            ),
        })
        .expect("issue RUNAS user token");
    let actor_token_id = actor_token.token_id;
    let mut actor_secret = actor_token.take_secret().expect("RUNAS token secret");
    let actor_session = storage
        .authenticate_query_session(actor_token_id, &actor_secret)
        .expect("authenticate RUNAS user");
    assert_ne!(actor_session.principal_id(), controller.principal_id());
    let begun = storage
        .begin_interactive_transaction(token_id, &token_secret, now_us() + 86_400_000_000)
        .unwrap();

    let mismatch = storage.execute_interactive_package_with_supervisor(
        package_request_with_actor(
            token_id,
            &token_secret,
            actor_token_id,
            &actor_secret,
            begun.transaction_id,
            [0x71; 16],
            [0x81; 32],
            "different_user",
        ),
        |_| Ok::<_, &'static str>(()),
    );
    assert!(matches!(
        mismatch,
        Err(InteractivePackageExecutionError::Transaction(
            InteractiveTransactionError::InvalidActor
        ))
    ));
    assert_eq!(
        storage
            .interactive_transaction_status(token_id, &token_secret, begun.transaction_id)
            .unwrap(),
        InteractiveTransactionState::Active
    );

    storage
        .execute_interactive_package_with_supervisor(
            package_request_with_actor(
                token_id,
                &token_secret,
                actor_token_id,
                &actor_secret,
                begun.transaction_id,
                [0x72; 16],
                [0x82; 32],
                actor_session.principal_name(),
            ),
            |supervisor| {
                assert_eq!(supervisor.principal_id(), actor_session.principal_id());
                assert_eq!(supervisor.role_ids(), actor_session.role_ids());
                Ok::<_, &'static str>(())
            },
        )
        .unwrap();
    storage
        .commit_interactive_transaction(token_id, &token_secret, begun.transaction_id)
        .unwrap();
    actor_secret.zeroize();
    token_secret.zeroize();
}

#[test]
fn v2_transaction_attaches_and_commits_through_fresh_tcp_connections() {
    use prdb_engine::client_protocol::{ClientV2Response, ClientV2TransactionState};
    use prdb_engine::client_submit::SubmissionClient;
    use prdb_engine::gateway::serve_tcp_client_connection;
    use prdb_engine::supervisor_ipc::{
        GatewayAdmissionConfig, GatewayClientServiceEvent, serve_gateway_client_session,
    };
    use std::net::TcpListener;
    use std::os::unix::net::UnixStream;

    struct AcceptMetadata;
    impl SubmissionMetadataValidator for AcceptMetadata {
        fn supports_versions(&self, manifest_version: u32, scope_version: u32) -> bool {
            manifest_version == 1 && scope_version == 1
        }

        fn validate(&self, _metadata: &SubmissionMetadata) -> bool {
            true
        }
    }

    let fixture = Fixture::new();
    let mut bootstrap = initialize(&fixture.0, "interactive_admin", now_us() + 86_400_000_000)
        .expect("initialize database");
    let token_id = bootstrap.token_id();
    let mut token_secret = bootstrap.take_secret().expect("bootstrap token secret");
    let instance = bootstrap.acknowledge().expect("publish READY");
    let storage = StorageManager::new(instance.database(), Duration::from_secs(2)).unwrap();
    let staging = StagingStore::new(StagingLimits::default(), Arc::new(AcceptMetadata)).unwrap();
    let admission = GatewayAdmissionConfig {
        retention_period_us: 86_400_000_000,
        max_rows: 8,
        max_bytes: 4096,
    };

    std::thread::scope(|scope| {
        let storage_ref = &storage;
        let staging_ref = &staging;
        let open_service = || {
            let listener = TcpListener::bind("127.0.0.1:0").unwrap();
            let address = listener.local_addr().unwrap();
            let (mut gateway_stream, mut supervisor_stream) = UnixStream::pair().unwrap();
            let gateway = scope.spawn(move || {
                let (mut client, _) = listener.accept().unwrap();
                serve_tcp_client_connection(&mut client, &mut gateway_stream).unwrap();
            });
            let supervisor = scope.spawn(move || {
                let mut session = ClientConnectionSession::default();
                match serve_gateway_client_session(
                    &mut supervisor_stream,
                    storage_ref,
                    staging_ref,
                    &mut session,
                    admission,
                )
                .unwrap()
                {
                    GatewayClientServiceEvent::Disconnected => (),
                    GatewayClientServiceEvent::Started { .. } => {
                        panic!("transaction-control session unexpectedly started a package")
                    }
                }
            });
            (address, gateway, supervisor)
        };

        let (address, gateway, supervisor) = open_service();
        let mut begin_secret = token_secret;
        let mut client = SubmissionClient::connect_interactive_authenticated(
            address,
            token_id,
            &mut begin_secret,
        )
        .unwrap();
        let begun = client.interactive_transaction_begin([0xa9; 16]).unwrap();
        let ClientV2Response::TransactionBegun {
            transaction_id,
            state: ClientV2TransactionState::Active,
            ..
        } = begun
        else {
            panic!("unexpected BEGIN response: {begun:?}")
        };
        drop(client);
        gateway.join().unwrap();
        supervisor.join().unwrap();

        let (address, gateway, supervisor) = open_service();
        let mut attach_secret = token_secret;
        let mut client = SubmissionClient::connect_interactive_authenticated(
            address,
            token_id,
            &mut attach_secret,
        )
        .unwrap();
        assert_eq!(
            client
                .interactive_transaction_attach(transaction_id)
                .unwrap(),
            ClientV2Response::Transaction {
                transaction_id,
                state: ClientV2TransactionState::Active,
            }
        );
        assert_eq!(
            client
                .interactive_transaction_commit(transaction_id)
                .unwrap(),
            ClientV2Response::Transaction {
                transaction_id,
                state: ClientV2TransactionState::Committed,
            }
        );
        drop(client);
        gateway.join().unwrap();
        supervisor.join().unwrap();
    });
    token_secret.zeroize();
}

#[cfg(all(target_os = "linux", target_arch = "x86_64"))]
#[test]
fn runas_package_crosses_v2_tcp_gateway_and_confined_executor()
-> Result<(), Box<dyn std::error::Error>> {
    use prdb_engine::client_package::{PackageOptions, package_query_repository};
    use prdb_engine::client_protocol::{
        ClientStartActorCredentials, ClientV2PackageState, ClientV2Response,
        ClientV2TransactionState,
    };
    use prdb_engine::client_sql::{encode_parameter_file, generate_query_batch_source};
    use prdb_engine::client_submit::{ClientResultEvent, SubmissionClient};
    use prdb_engine::gateway::serve_tcp_client_connection;
    use prdb_engine::metadata_discovery::VisibleRelation;
    use prdb_engine::storage_manager::{
        CreatePolicyRequest, CreateTableRequest, ObjectGrantPrivilege, ObjectGrantTarget,
        PolicyKind, SetObjectGrantRequest, TableColumnDefinition,
    };
    use prdb_engine::submission_manifest::{
        ArtifactCompatibilityProfile, VersionedSubmissionMetadataValidator,
    };
    use prdb_engine::submission_scope::RequestedScopeBuilder;
    use prdb_engine::supervisor::{StagingLimits, StagingStore, WorkloadClass};
    use prdb_engine::supervisor_ipc::{
        GatewayAdmissionConfig, GatewayClientServiceEvent, GatewayExecutorConfig,
        serve_gateway_client_session_with_executor,
    };
    use std::collections::HashSet;
    use std::fs;
    use std::net::TcpListener;
    use std::os::unix::fs::PermissionsExt;
    use std::os::unix::net::UnixStream;
    use std::path::PathBuf;

    fn required_paths(name: &str) -> Result<Vec<PathBuf>, Box<dyn std::error::Error>> {
        let value = std::env::var_os(name).ok_or_else(|| format!("{name} is required"))?;
        if name == "PRDB_TEST_RUNTIME_READ_ONLY" && value.is_empty() {
            return Ok(Vec::new());
        }
        let paths = std::env::split_paths(&value).collect::<Vec<_>>();
        if paths.is_empty() || paths.iter().any(|path| !path.is_absolute()) {
            return Err(format!("{name} must contain absolute paths").into());
        }
        Ok(paths)
    }

    let cgroup_parent = PathBuf::from(
        std::env::var_os("PRDB_TEST_CGROUP_PARENT")
            .ok_or("PRDB_TEST_CGROUP_PARENT must name a delegated cgroup v2 parent")?,
    );
    let sdk_release_dir = PathBuf::from(
        std::env::var_os("PRDB_TEST_INSTALLED_SDK_RELEASE_DIR")
            .ok_or("PRDB_TEST_INSTALLED_SDK_RELEASE_DIR must name the installed SDK")?,
    );
    if !cgroup_parent.is_absolute() || !sdk_release_dir.is_absolute() {
        return Err("interactive sandbox paths must be absolute".into());
    }

    let fixture = Fixture::new();
    fs::create_dir_all(&fixture.0)?;
    let database_dir = fixture.0.join("database");
    let package = fixture.0.join("runas-package");
    let source = fixture.0.join("runas-batch.c");
    let hidden_package = fixture.0.join("hidden-runas-package");
    let hidden_source = fixture.0.join("hidden-runas.c");
    let hostile_package = fixture.0.join("hostile-runas-package");
    let hostile_source = fixture.0.join("hostile-runas.c");
    let scope_path = fixture.0.join("runas-scope.psc1");
    fs::write(
        &source,
        generate_query_batch_source(
            "public",
            b"INSERT INTO records VALUES ($1); SELECT id FROM records; SELECT id FROM records",
        )?,
    )?;
    fs::set_permissions(&source, fs::Permissions::from_mode(0o600))?;
    fs::write(
        &hidden_source,
        generate_query_batch_source("public", b"SELECT id FROM records")?,
    )?;
    fs::set_permissions(&hidden_source, fs::Permissions::from_mode(0o600))?;
    fs::write(
        &hostile_source,
        r##"
#include <stdint.h>
#include <stddef.h>
#include "prdb_query_abi_v3.h"
#include "prdb_query_abi_constants.h"

const prdb_query_descriptor_v3_record prdb_query_descriptor_v3 = {
    sizeof(prdb_query_descriptor_v3_record), 3,
    sizeof(prdb_host_api_v3), sizeof(prdb_invocation_v3)
};

int32_t prdb_query_main_v3(const prdb_host_api_v3 *host,
                           const prdb_invocation_v3 *invocation) {
    static const uint8_t request[] = {0};
    static const uint8_t denied[] = "CONTROL_OPERATIONS_DENIED";
    uint8_t response[64];
    uint64_t response_len = 0;
    int32_t transaction_status;
    int32_t admin_status;
    (void)invocation;
    if (host == NULL || host->executor_request == NULL || host->write_result == NULL) {
        return 1;
    }
    transaction_status = host->executor_request(
        host->context, PRDB_IPC_OP_INTERACTIVE_TRANSACTION,
        request, sizeof(request), response, sizeof(response), &response_len);
    response_len = 0;
    admin_status = host->executor_request(
        host->context, PRDB_IPC_OP_SUBMIT_ADMINISTRATIVE_OPERATION,
        request, sizeof(request), response, sizeof(response), &response_len);
    if (transaction_status == PRDB_QUERY_CALLBACK_OK ||
        admin_status == PRDB_QUERY_CALLBACK_OK) {
        return 2;
    }
    return host->write_result(host->context, denied, sizeof(denied) - 1) == 0
        ? 0 : 3;
}
"##,
    )?;
    fs::set_permissions(&hostile_source, fs::Permissions::from_mode(0o600))?;
    let mut bootstrap = initialize(
        &database_dir,
        "interactive_admin",
        now_us() + 86_400_000_000,
    )?;
    let controller_token_id = bootstrap.token_id();
    let mut controller_secret = bootstrap.take_secret().ok_or("bootstrap secret missing")?;
    let instance = bootstrap.acknowledge()?;
    let storage = StorageManager::new(instance.database(), Duration::from_secs(10))?;
    let controller = storage.authenticate_query_session(controller_token_id, &controller_secret)?;
    let expires_at_us = now_us() + 3_600_000_000;
    let actor = storage.create_user(
        [0xb1; 16],
        controller.principal_id(),
        "tcp_runas_actor",
        expires_at_us,
        test_audit_event(
            [0xb1; 16],
            controller.principal_id(),
            AuditAction::Ddl,
            Vec::new(),
        ),
    )?;
    let mut actor_token = storage.issue_token(TokenIssueRequest {
        invocation_id: [0xb2; 16],
        authenticated_actor_id: controller.principal_id(),
        user_id: actor.user_id,
        label: "tcp-runas-test",
        expires_at_us,
        retention_deadline_us: expires_at_us,
        audit_event: test_audit_event(
            [0xb2; 16],
            controller.principal_id(),
            AuditAction::TokenLifecycle,
            vec![actor.user_id],
        ),
    })?;
    let actor_token_id = actor_token.token_id;
    let mut actor_secret = actor_token.take_secret().ok_or("RUNAS secret missing")?;
    let hidden_actor = storage.create_user(
        [0xd1; 16],
        controller.principal_id(),
        "tcp_runas_hidden",
        expires_at_us,
        test_audit_event(
            [0xd1; 16],
            controller.principal_id(),
            AuditAction::Ddl,
            Vec::new(),
        ),
    )?;
    let mut hidden_actor_token = storage.issue_token(TokenIssueRequest {
        invocation_id: [0xd2; 16],
        authenticated_actor_id: controller.principal_id(),
        user_id: hidden_actor.user_id,
        label: "tcp-hidden-test",
        expires_at_us,
        retention_deadline_us: expires_at_us,
        audit_event: test_audit_event(
            [0xd2; 16],
            controller.principal_id(),
            AuditAction::TokenLifecycle,
            vec![hidden_actor.user_id],
        ),
    })?;
    let hidden_actor_token_id = hidden_actor_token.token_id;
    let mut hidden_actor_secret = hidden_actor_token
        .take_secret()
        .ok_or("hidden RUNAS secret missing")?;

    let public_schema = storage.create_schema(
        [0xb8; 16],
        controller.principal_id(),
        "public",
        controller.principal_id(),
        expires_at_us,
        test_audit_event(
            [0xb8; 16],
            controller.principal_id(),
            AuditAction::Ddl,
            Vec::new(),
        ),
    )?;
    let table = storage.create_table(CreateTableRequest {
        invocation_id: [0xb5; 16],
        authenticated_actor_id: controller.principal_id(),
        schema_id: public_schema.schema_id,
        name: "records".to_owned(),
        columns: vec![TableColumnDefinition {
            name: "id".to_owned(),
            scalar_type: prdb_engine::tuple::ScalarType::Integer,
            nullable: true,
            generated_uuid_identity: false,
            default: None,
        }],
        retention_deadline_us: expires_at_us,
        audit_event: test_audit_event(
            [0xb5; 16],
            controller.principal_id(),
            AuditAction::Ddl,
            vec![public_schema.schema_id],
        ),
    })?;
    storage.set_object_grant(SetObjectGrantRequest {
        invocation_id: [0xb6; 16],
        authenticated_actor_id: controller.principal_id(),
        grantee_id: actor.user_id,
        object_id: public_schema.schema_id,
        privilege: ObjectGrantPrivilege::Usage,
        target: ObjectGrantTarget::Object,
        grant: true,
        retention_deadline_us: expires_at_us,
        audit_event: test_audit_event(
            [0xb6; 16],
            controller.principal_id(),
            AuditAction::AuthorityChange,
            vec![actor.user_id, public_schema.schema_id],
        ),
    })?;
    storage.set_object_grant(SetObjectGrantRequest {
        invocation_id: [0xb7; 16],
        authenticated_actor_id: controller.principal_id(),
        grantee_id: actor.user_id,
        object_id: table.table_id,
        privilege: ObjectGrantPrivilege::Select,
        target: ObjectGrantTarget::Object,
        grant: true,
        retention_deadline_us: expires_at_us,
        audit_event: test_audit_event(
            [0xb7; 16],
            controller.principal_id(),
            AuditAction::AuthorityChange,
            vec![actor.user_id, table.table_id],
        ),
    })?;
    storage.set_object_grant(SetObjectGrantRequest {
        invocation_id: [0xd3; 16],
        authenticated_actor_id: controller.principal_id(),
        grantee_id: hidden_actor.user_id,
        object_id: public_schema.schema_id,
        privilege: ObjectGrantPrivilege::Usage,
        target: ObjectGrantTarget::Object,
        grant: true,
        retention_deadline_us: expires_at_us,
        audit_event: test_audit_event(
            [0xd3; 16],
            controller.principal_id(),
            AuditAction::AuthorityChange,
            vec![hidden_actor.user_id, public_schema.schema_id],
        ),
    })?;
    storage.set_object_grant(SetObjectGrantRequest {
        invocation_id: [0xd4; 16],
        authenticated_actor_id: controller.principal_id(),
        grantee_id: hidden_actor.user_id,
        object_id: table.table_id,
        privilege: ObjectGrantPrivilege::Select,
        target: ObjectGrantTarget::Object,
        grant: true,
        retention_deadline_us: expires_at_us,
        audit_event: test_audit_event(
            [0xd4; 16],
            controller.principal_id(),
            AuditAction::AuthorityChange,
            vec![hidden_actor.user_id, table.table_id],
        ),
    })?;
    storage.set_object_grant(SetObjectGrantRequest {
        invocation_id: [0xb9; 16],
        authenticated_actor_id: controller.principal_id(),
        grantee_id: actor.user_id,
        object_id: table.table_id,
        privilege: ObjectGrantPrivilege::Insert,
        target: ObjectGrantTarget::Object,
        grant: true,
        retention_deadline_us: expires_at_us,
        audit_event: test_audit_event(
            [0xb9; 16],
            controller.principal_id(),
            AuditAction::AuthorityChange,
            vec![actor.user_id, table.table_id],
        ),
    })?;
    storage.set_object_grant(SetObjectGrantRequest {
        invocation_id: [0xba; 16],
        authenticated_actor_id: controller.principal_id(),
        grantee_id: actor.user_id,
        object_id: table.table_id,
        privilege: ObjectGrantPrivilege::Insert,
        target: ObjectGrantTarget::Column(table.column_ids[0]),
        grant: true,
        retention_deadline_us: expires_at_us,
        audit_event: test_audit_event(
            [0xba; 16],
            controller.principal_id(),
            AuditAction::AuthorityChange,
            vec![actor.user_id, table.table_id, table.column_ids[0]],
        ),
    })?;
    storage.set_object_grant(SetObjectGrantRequest {
        invocation_id: [0xbd; 16],
        authenticated_actor_id: controller.principal_id(),
        grantee_id: actor.user_id,
        object_id: table.table_id,
        privilege: ObjectGrantPrivilege::Select,
        target: ObjectGrantTarget::Column(table.column_ids[0]),
        grant: true,
        retention_deadline_us: expires_at_us,
        audit_event: test_audit_event(
            [0xbd; 16],
            controller.principal_id(),
            AuditAction::AuthorityChange,
            vec![actor.user_id, table.table_id, table.column_ids[0]],
        ),
    })?;
    storage.set_object_grant(SetObjectGrantRequest {
        invocation_id: [0xd5; 16],
        authenticated_actor_id: controller.principal_id(),
        grantee_id: hidden_actor.user_id,
        object_id: table.table_id,
        privilege: ObjectGrantPrivilege::Select,
        target: ObjectGrantTarget::Column(table.column_ids[0]),
        grant: true,
        retention_deadline_us: expires_at_us,
        audit_event: test_audit_event(
            [0xd5; 16],
            controller.principal_id(),
            AuditAction::AuthorityChange,
            vec![hidden_actor.user_id, table.table_id, table.column_ids[0]],
        ),
    })?;
    storage.create_policy(CreatePolicyRequest {
        invocation_id: [0xbb; 16],
        authenticated_actor_id: controller.principal_id(),
        table_id: table.table_id,
        name: "tcp_runas_insert".to_owned(),
        kind: PolicyKind::Row,
        operation: prdb_engine::authorization::Operation::Insert,
        effect: prdb_engine::authorization::Effect::Allow,
        recipients: vec![prdb_engine::authorization::Subject::Principal(
            actor.user_id,
        )],
        target_column_ids: Vec::new(),
        using_expression: None,
        with_check_expression: Some(prdb_engine::authorization::Expr::Literal(Some(
            prdb_engine::tuple::ScalarValue::Boolean(true),
        ))),
        retention_deadline_us: expires_at_us,
        audit_event: test_audit_event(
            [0xbb; 16],
            controller.principal_id(),
            AuditAction::Ddl,
            vec![actor.user_id, table.table_id],
        ),
    })?;
    storage.create_policy(CreatePolicyRequest {
        invocation_id: [0xbc; 16],
        authenticated_actor_id: controller.principal_id(),
        table_id: table.table_id,
        name: "tcp_runas_insert_id".to_owned(),
        kind: PolicyKind::Cell,
        operation: prdb_engine::authorization::Operation::Insert,
        effect: prdb_engine::authorization::Effect::Allow,
        recipients: vec![prdb_engine::authorization::Subject::Principal(
            actor.user_id,
        )],
        target_column_ids: vec![table.column_ids[0]],
        using_expression: None,
        with_check_expression: Some(prdb_engine::authorization::Expr::Literal(Some(
            prdb_engine::tuple::ScalarValue::Boolean(true),
        ))),
        retention_deadline_us: expires_at_us,
        audit_event: test_audit_event(
            [0xbc; 16],
            controller.principal_id(),
            AuditAction::Ddl,
            vec![actor.user_id, table.table_id, table.column_ids[0]],
        ),
    })?;
    storage.create_policy(CreatePolicyRequest {
        invocation_id: [0xbe; 16],
        authenticated_actor_id: controller.principal_id(),
        table_id: table.table_id,
        name: "tcp_runas_select".to_owned(),
        kind: PolicyKind::Row,
        operation: prdb_engine::authorization::Operation::Select,
        effect: prdb_engine::authorization::Effect::Allow,
        recipients: vec![
            prdb_engine::authorization::Subject::Principal(actor.user_id),
            prdb_engine::authorization::Subject::Principal(hidden_actor.user_id),
        ],
        target_column_ids: Vec::new(),
        using_expression: Some(prdb_engine::authorization::Expr::Literal(Some(
            prdb_engine::tuple::ScalarValue::Boolean(true),
        ))),
        with_check_expression: None,
        retention_deadline_us: expires_at_us,
        audit_event: test_audit_event(
            [0xbe; 16],
            controller.principal_id(),
            AuditAction::Ddl,
            vec![actor.user_id, hidden_actor.user_id, table.table_id],
        ),
    })?;
    storage.create_policy(CreatePolicyRequest {
        invocation_id: [0xbf; 16],
        authenticated_actor_id: controller.principal_id(),
        table_id: table.table_id,
        name: "tcp_runas_select_id".to_owned(),
        kind: PolicyKind::Cell,
        operation: prdb_engine::authorization::Operation::Select,
        effect: prdb_engine::authorization::Effect::Allow,
        recipients: vec![
            prdb_engine::authorization::Subject::Principal(actor.user_id),
            prdb_engine::authorization::Subject::Principal(hidden_actor.user_id),
        ],
        target_column_ids: vec![table.column_ids[0]],
        using_expression: Some(prdb_engine::authorization::Expr::Literal(Some(
            prdb_engine::tuple::ScalarValue::Boolean(true),
        ))),
        with_check_expression: None,
        retention_deadline_us: expires_at_us,
        audit_event: test_audit_event(
            [0xbf; 16],
            controller.principal_id(),
            AuditAction::Ddl,
            vec![
                actor.user_id,
                hidden_actor.user_id,
                table.table_id,
                table.column_ids[0],
            ],
        ),
    })?;

    let visible_relation = VisibleRelation {
        relation_id: table.table_id,
        name: "records".to_owned(),
    };
    let mut requested_scope = RequestedScopeBuilder::new();
    requested_scope
        .include_relation(
            &prdb_engine::metadata_discovery::VisibleSchema {
                schema_id: public_schema.schema_id,
                name: "public".to_owned(),
            },
            &visible_relation,
            &[
                prdb_engine::authorization::Operation::Insert,
                prdb_engine::authorization::Operation::Select,
            ],
        )
        .map_err(|error| format!("failed to build package scope: {error:?}"))?;
    requested_scope
        .include_column(
            &prdb_engine::metadata_discovery::VisibleSchema {
                schema_id: public_schema.schema_id,
                name: "public".to_owned(),
            },
            &visible_relation,
            &prdb_engine::metadata_discovery::VisibleColumn {
                column_id: table.column_ids[0],
                name: "id".to_owned(),
                ordinal: 0,
                scalar_type: prdb_engine::tuple::ScalarType::Integer,
                nullable: true,
            },
            &[
                prdb_engine::authorization::Operation::Insert,
                prdb_engine::authorization::Operation::Select,
            ],
        )
        .map_err(|error| format!("failed to add column scope: {error:?}"))?;
    fs::write(
        &scope_path,
        requested_scope
            .encode()
            .map_err(|error| format!("failed to encode package scope: {error:?}"))?,
    )?;
    fs::set_permissions(&scope_path, fs::Permissions::from_mode(0o600))?;
    let find_tool = |tool: &str| -> Result<PathBuf, Box<dyn std::error::Error>> {
        std::env::split_paths(&std::env::var_os("PATH").ok_or("PATH is required")?)
            .map(|directory| directory.join(tool))
            .find(|path| path.is_file())
            .ok_or_else(|| format!("{tool} was not found in PATH").into())
    };
    package_query_repository(&PackageOptions {
        tcc: find_tool("tcc")?,
        shared_linker: find_tool("cc")?,
        patchelf: find_tool("patchelf")?,
        source,
        sdk_library: sdk_release_dir.join("libprdb_sql.so.1"),
        include_dir: PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("include"),
        requested_scope: scope_path.clone(),
        output_dir: package.clone(),
        native_abi_version: 3,
        sdk_release: 1,
        cpu_baseline: "x86_64-v1".to_owned(),
        runtime_profile: "nixos-x86_64-glibc-2.42".to_owned(),
        compiler_identity: "interactive RUNAS integration".to_owned(),
    })?;
    package_query_repository(&PackageOptions {
        tcc: find_tool("tcc")?,
        shared_linker: find_tool("cc")?,
        patchelf: find_tool("patchelf")?,
        source: hidden_source,
        sdk_library: sdk_release_dir.join("libprdb_sql.so.1"),
        include_dir: PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("include"),
        requested_scope: scope_path.clone(),
        output_dir: hidden_package.clone(),
        native_abi_version: 3,
        sdk_release: 1,
        cpu_baseline: "x86_64-v1".to_owned(),
        runtime_profile: "nixos-x86_64-glibc-2.42".to_owned(),
        compiler_identity: "interactive RUNAS visibility integration".to_owned(),
    })?;
    package_query_repository(&PackageOptions {
        tcc: find_tool("tcc")?,
        shared_linker: find_tool("cc")?,
        patchelf: find_tool("patchelf")?,
        source: hostile_source,
        sdk_library: sdk_release_dir.join("libprdb_sql.so.1"),
        include_dir: PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("include"),
        requested_scope: scope_path.clone(),
        output_dir: hostile_package.clone(),
        native_abi_version: 3,
        sdk_release: 1,
        cpu_baseline: "x86_64-v1".to_owned(),
        runtime_profile: "nixos-x86_64-glibc-2.42".to_owned(),
        compiler_identity: "interactive transaction boundary attack".to_owned(),
    })?;
    let manifest = prdb_engine::submission_manifest::decode_manifest_v1(&fs::read(
        package.join("manifest.pmf1"),
    )?)
    .map_err(|error| format!("generated package manifest is invalid: {error:?}"))?;
    let profile = ArtifactCompatibilityProfile::for_repository(
        manifest.cpu_baseline.clone(),
        manifest.runtime_profile.clone(),
        manifest
            .dependencies
            .iter()
            .cloned()
            .collect::<HashSet<_>>(),
    )
    .map_err(|error| format!("package compatibility profile failed: {error:?}"))?;
    let sdk_repository = Arc::new(
        prdb_engine::sdk_registry::SdkLibraryRepository::open(
            &fixture.0.join("sdk-libraries"),
            "linux-x86_64".to_owned(),
            manifest.cpu_baseline.clone(),
            manifest.runtime_profile.clone(),
            ["libc.so.6".to_owned()].into_iter().collect(),
            128 * 1024 * 1024,
        )
        .map_err(|error| format!("open SDK repository: {error:?}"))?,
    );
    sdk_repository
        .install(
            &fs::read(sdk_release_dir.join("sdk-release.json"))?,
            &fs::read(sdk_release_dir.join("libprdb_sql.so.1"))?,
            &["libc.so.6".to_owned()].into_iter().collect(),
        )
        .map_err(|error| format!("install SDK release: {error:?}"))?;
    let validator = VersionedSubmissionMetadataValidator::new(profile)
        .with_sdk_registry(sdk_repository)
        .map_err(|error| format!("configure SDK repository: {error:?}"))?;

    let staging = StagingStore::new(StagingLimits::default(), Arc::new(validator))?;
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
            cleanup_timeout: Duration::from_secs(2),
        },
    };
    let admission = GatewayAdmissionConfig {
        retention_period_us: 3_600_000_000,
        max_rows: 8,
        max_bytes: 4096,
    };
    let storage_ref = &storage;
    let staging_ref = &staging;
    let runtime_ref = &runtime;
    let transaction_id = std::thread::scope(
        |scope| -> Result<[u8; 16], Box<dyn std::error::Error>> {
            let open_service = || {
                let listener = TcpListener::bind("127.0.0.1:0").unwrap();
                let address = listener.local_addr().unwrap();
                let (mut gateway_stream, mut supervisor_stream) = UnixStream::pair().unwrap();
                let gateway = scope.spawn(move || -> Result<(), String> {
                    let (mut client, _) = listener.accept().map_err(|error| error.to_string())?;
                    serve_tcp_client_connection(&mut client, &mut gateway_stream)
                        .map_err(|error| format!("TCP gateway failed: {error:?}"))
                });
                let supervisor = scope.spawn(move || -> Result<(), String> {
                    let mut session = ClientConnectionSession::default();
                    match serve_gateway_client_session_with_executor(
                        &mut supervisor_stream,
                        storage_ref,
                        staging_ref,
                        &mut session,
                        admission,
                        runtime_ref,
                    )
                    .map_err(|error| format!("supervisor session failed: {error:?}"))?
                    {
                        GatewayClientServiceEvent::Disconnected => Ok(()),
                        GatewayClientServiceEvent::Started {
                            request_id,
                            mut invocation,
                            submission,
                            executor: Some(mut executor),
                        } => {
                            prdb_engine::supervisor_ipc::observe_executor_relay_and_finalize_gateway(
                                storage_ref,
                                &mut invocation,
                                &mut executor,
                                &mut supervisor_stream,
                                request_id,
                            )
                            .map_err(|error| format!("one-shot execution failed: {error:?}"))?;
                            let owner = session
                                .staging_owner()
                                .map_err(|error| format!("staging owner lookup failed: {error:?}"))?;
                            staging_ref
                                .finish_started(owner, submission.invocation_id())
                                .map_err(|error| format!("staging cleanup failed: {error:?}"))
                        }
                        GatewayClientServiceEvent::Started { .. } => {
                            Err("one-shot START did not include its confined executor".to_owned())
                        }
                    }
                });
                (address, gateway, supervisor)
            };

            let (address, gateway, supervisor) = open_service();
            let mut begin_secret = controller_secret;
            let mut client = SubmissionClient::connect_interactive_authenticated(
                address,
                controller_token_id,
                &mut begin_secret,
            )?;
            let begun = client.interactive_transaction_begin([0xb3; 16])?;
            let ClientV2Response::TransactionBegun { transaction_id, .. } = begun else {
                return Err("unexpected interactive BEGIN response".into());
            };
            drop(client);
            gateway.join().unwrap().map_err(|error| error.to_owned())?;
            supervisor
                .join()
                .unwrap()
                .map_err(|error| error.to_owned())?;

            let (address, gateway, supervisor) = open_service();
            let mut package_controller_secret = controller_secret;
            let mut client = SubmissionClient::connect_interactive_authenticated(
                address,
                controller_token_id,
                &mut package_controller_secret,
            )?;
            assert_eq!(
                client.interactive_transaction_attach(transaction_id)?,
                ClientV2Response::Transaction {
                    transaction_id,
                    state: ClientV2TransactionState::Active,
                }
            );
            let package_id = [0xb4; 16];
            let batch_parameters = encode_parameter_file(
                br#"{"format":"PDB-SQL-PARAMETERS-1","values":[{"type":"integer","value":"7"}]}"#,
            )?;
            let invocation_id = client.upload_interactive_package_and_seal(
                &package,
                batch_parameters.as_slice(),
                30_000,
                WorkloadClass::Operational,
                transaction_id,
                package_id,
            )?;
            let actor_secret_for_start = actor_secret;
            let mut started = client.start_interactive_package(
                invocation_id,
                transaction_id,
                package_id,
                Some(ClientStartActorCredentials {
                    expected_name: "tcp_runas_actor".to_owned(),
                    token_id: actor_token_id,
                    token_secret: actor_secret_for_start.into(),
                }),
                &fixture.0.join("runas.receipt"),
            )?;
            let mut batch_output = Vec::new();
            let mut package_terminal_state = None;
            let mut stream_error: Option<String> = None;
            loop {
                let event = match started.next_event() {
                    Ok(Some(event)) => event,
                    Ok(None) => break,
                    Err(error) => {
                        stream_error = Some(error.to_string());
                        break;
                    }
                };
                match event {
                    ClientResultEvent::Data(data) => {
                        batch_output.extend_from_slice(&data);
                    }
                    ClientResultEvent::PackageTerminal {
                        transaction_id: result_transaction_id,
                        package_id: result_package_id,
                        state,
                        result_lost: false,
                    } => {
                        assert_eq!(result_transaction_id, transaction_id);
                        assert_eq!(result_package_id, package_id);
                        package_terminal_state = Some(state);
                    }
                    other => {
                        stream_error = Some(format!("unexpected RUNAS package event: {other:?}"));
                        break;
                    }
                }
            }
            if let Some(error) = stream_error {
                drop(client);
                let gateway_result = gateway.join().unwrap();
                let supervisor_result = supervisor.join().unwrap();
                return Err(format!(
                    "RUNAS package stream failed: {error}; gateway={gateway_result:?}; supervisor={supervisor_result:?}"
                )
                .into());
            }
            assert_eq!(
                package_terminal_state,
                Some(ClientV2PackageState::Completed),
                "RUNAS batch package did not complete; SQLB bytes: {batch_output:?}"
            );
            let mut batch_events = Vec::new();
            let mut sql_results = Vec::new();
            let mut cursor = 0usize;
            while cursor < batch_output.len() {
                let header = batch_output
                    .get(cursor..cursor + 12)
                    .ok_or("truncated SQLB event header")?;
                if &header[..4] != b"SQLB" || header[4] != 1 {
                    return Err("invalid SQLB event header".into());
                }
                let payload_bytes = u32::from_be_bytes(header[8..12].try_into()?) as usize;
                let record_bytes = 12usize
                    .checked_add(payload_bytes)
                    .ok_or("SQLB event length overflow")?;
                if cursor
                    .checked_add(record_bytes)
                    .is_none_or(|end| end > batch_output.len())
                {
                    return Err("truncated SQLB event payload".into());
                }
                batch_events.push((header[5], u16::from_be_bytes([header[6], header[7]])));
                if header[5] == 2 {
                    sql_results.push((
                        u16::from_be_bytes([header[6], header[7]]),
                        &batch_output[cursor + 12..cursor + record_bytes],
                    ));
                }
                cursor += record_bytes;
            }
            for expected in [(1, 0), (3, 0), (1, 1), (3, 1), (1, 2), (3, 2)] {
                assert!(
                    batch_events.contains(&expected),
                    "missing SQLB event {expected:?} in {batch_events:?}"
                );
            }
            assert!(
                sql_results.len() == 2
                    && sql_results.iter().all(|(ordinal, result)| {
                        matches!(*ordinal, 1 | 2)
                            && result.starts_with(b"SQLR")
                            && result.ends_with(&7i32.to_be_bytes())
                    })
                    && sql_results[0].1 == sql_results[1].1,
                "RUNAS saw a foreign staged row or missed its own staged INSERT: {sql_results:?}"
            );
            let hidden_package_id = [0xd8; 16];
            let hidden_invocation_id = client.upload_interactive_package_and_seal(
                &hidden_package,
                &[],
                30_000,
                WorkloadClass::Operational,
                transaction_id,
                hidden_package_id,
            )?;
            let mut hidden_started = client.start_interactive_package(
                hidden_invocation_id,
                transaction_id,
                hidden_package_id,
                Some(ClientStartActorCredentials {
                    expected_name: "tcp_runas_hidden".to_owned(),
                    token_id: hidden_actor_token_id,
                    token_secret: hidden_actor_secret.into(),
                }),
                &fixture.0.join("runas-hidden.receipt"),
            )?;
            let mut hidden_output = Vec::new();
            let mut hidden_terminal_state = None;
            while let Some(event) = hidden_started.next_event()? {
                match event {
                    ClientResultEvent::Data(data) => hidden_output.extend_from_slice(&data),
                    ClientResultEvent::PackageTerminal {
                        transaction_id: result_transaction_id,
                        package_id: result_package_id,
                        state,
                        result_lost: false,
                    } => {
                        assert_eq!(result_transaction_id, transaction_id);
                        assert_eq!(result_package_id, hidden_package_id);
                        hidden_terminal_state = Some(state);
                    }
                    other => {
                        return Err(
                            format!("unexpected hidden actor package event: {other:?}").into()
                        );
                    }
                }
            }
            assert_eq!(hidden_terminal_state, Some(ClientV2PackageState::Completed));
            let mut hidden_events = Vec::new();
            let mut cursor = 0usize;
            while cursor < hidden_output.len() {
                let header = hidden_output
                    .get(cursor..cursor + 12)
                    .ok_or("truncated hidden SQLB event header")?;
                if &header[..4] != b"SQLB" || header[4] != 1 {
                    return Err("invalid hidden SQLB event header".into());
                }
                let payload_bytes = u32::from_be_bytes(header[8..12].try_into()?) as usize;
                let record_bytes = 12usize
                    .checked_add(payload_bytes)
                    .ok_or("hidden SQLB event length overflow")?;
                if cursor
                    .checked_add(record_bytes)
                    .is_none_or(|end| end > hidden_output.len())
                {
                    return Err("truncated hidden SQLB event payload".into());
                }
                hidden_events.push((header[5], u16::from_be_bytes([header[6], header[7]])));
                cursor += record_bytes;
            }
            assert!(
                hidden_events.contains(&(1, 0))
                    && hidden_events.contains(&(3, 0))
                    && !hidden_events.iter().any(|(kind, _)| *kind == 2),
                "a second RUNAS actor observed another actor's private staged INSERT: {hidden_events:?}"
            );
            let second_package_id = [0xc8; 16];
            let second_invocation_id = client.upload_interactive_package_and_seal(
                &package,
                batch_parameters.as_slice(),
                30_000,
                WorkloadClass::Operational,
                transaction_id,
                second_package_id,
            )?;
            let mut second_started = client.start_interactive_package(
                second_invocation_id,
                transaction_id,
                second_package_id,
                Some(ClientStartActorCredentials {
                    expected_name: "tcp_runas_actor".to_owned(),
                    token_id: actor_token_id,
                    token_secret: actor_secret.into(),
                }),
                &fixture.0.join("runas-second.receipt"),
            )?;
            let mut second_output = Vec::new();
            let mut second_terminal_state = None;
            while let Some(event) = second_started.next_event()? {
                match event {
                    ClientResultEvent::Data(data) => second_output.extend_from_slice(&data),
                    ClientResultEvent::PackageTerminal {
                        transaction_id: result_transaction_id,
                        package_id: result_package_id,
                        state,
                        result_lost: false,
                    } => {
                        assert_eq!(result_transaction_id, transaction_id);
                        assert_eq!(result_package_id, second_package_id);
                        second_terminal_state = Some(state);
                    }
                    other => {
                        return Err(format!("unexpected second package event: {other:?}").into());
                    }
                }
            }
            assert_eq!(
                second_terminal_state,
                Some(ClientV2PackageState::Completed),
                "second RUNAS package did not complete"
            );
            let mut second_sql_results = Vec::new();
            let mut cursor = 0usize;
            while cursor < second_output.len() {
                let header = second_output
                    .get(cursor..cursor + 12)
                    .ok_or("truncated second SQLB event header")?;
                if &header[..4] != b"SQLB" || header[4] != 1 {
                    return Err("invalid second SQLB event header".into());
                }
                let payload_bytes = u32::from_be_bytes(header[8..12].try_into()?) as usize;
                let record_bytes = 12usize
                    .checked_add(payload_bytes)
                    .ok_or("second SQLB event length overflow")?;
                if cursor
                    .checked_add(record_bytes)
                    .is_none_or(|end| end > second_output.len())
                {
                    return Err("truncated second SQLB event payload".into());
                }
                if header[5] == 2 {
                    second_sql_results.push((
                        u16::from_be_bytes([header[6], header[7]]),
                        &second_output[cursor + 12..cursor + record_bytes],
                    ));
                }
                cursor += record_bytes;
            }
            assert!(
                second_sql_results.len() == 4
                    && [1u16, 2].iter().all(|ordinal| {
                        second_sql_results
                            .iter()
                            .filter(|(actual, _)| actual == ordinal)
                            .count()
                            == 2
                    })
                    && second_sql_results.iter().all(|(_, result)| {
                        result.starts_with(b"SQLR") && result.ends_with(&7i32.to_be_bytes())
                    }),
                "second RUNAS package did not read both transactions' own staged rows: {second_sql_results:?}"
            );
            let hostile_package_id = [0xb5; 16];
            let hostile_invocation_id = client.upload_interactive_package_and_seal(
                &hostile_package,
                &[],
                30_000,
                WorkloadClass::Operational,
                transaction_id,
                hostile_package_id,
            )?;
            let mut hostile_started = client.start_interactive_package(
                hostile_invocation_id,
                transaction_id,
                hostile_package_id,
                None,
                &fixture.0.join("hostile.receipt"),
            )?;
            let mut hostile_output = Vec::new();
            let mut hostile_terminal = None;
            while let Some(event) = hostile_started.next_event()? {
                match event {
                    ClientResultEvent::Data(data) => hostile_output.extend_from_slice(&data),
                    ClientResultEvent::PackageTerminal {
                        transaction_id: response_transaction_id,
                        package_id: response_package_id,
                        state,
                        result_lost: false,
                    } => {
                        assert_eq!(response_transaction_id, transaction_id);
                        assert_eq!(response_package_id, hostile_package_id);
                        hostile_terminal = Some(state);
                    }
                    other => {
                        return Err(format!(
                            "unexpected hostile interactive package event: {other:?}"
                        )
                        .into());
                    }
                }
            }
            assert_eq!(
                hostile_terminal,
                Some(ClientV2PackageState::Completed),
                "hostile control operation attempt must not take over or abort the transaction"
            );
            assert_eq!(
                hostile_output, b"CONTROL_OPERATIONS_DENIED",
                "confined query code must not invoke transaction control or ADMIN"
            );
            assert_eq!(
                client.interactive_transaction_status(transaction_id)?,
                ClientV2Response::Transaction {
                    transaction_id,
                    state: ClientV2TransactionState::Active,
                }
            );
            assert_eq!(
                client.interactive_transaction_commit(transaction_id)?,
                ClientV2Response::Transaction {
                    transaction_id,
                    state: ClientV2TransactionState::Committed,
                }
            );
            drop(client);
            gateway.join().unwrap().map_err(|error| error.to_owned())?;
            supervisor
                .join()
                .unwrap()
                .map_err(|error| error.to_owned())?;

            let (address, gateway, supervisor) = open_service();
            let mut autocommit_secret = actor_secret;
            let mut client = SubmissionClient::connect_interactive_authenticated(
                address,
                actor_token_id,
                &mut autocommit_secret,
            )?;
            let autocommit_parameters = encode_parameter_file(
                br#"{"format":"PDB-SQL-PARAMETERS-1","values":[{"type":"integer","value":"9"}]}"#,
            )?;
            let invocation_id = client.upload_and_seal(
                &package,
                autocommit_parameters.as_slice(),
                30_000,
                WorkloadClass::Operational,
            )?;
            let mut started = client
                .start(invocation_id, &fixture.0.join("autocommit.receipt"))
                .map_err(|error| format!("autocommit START failed: {error:?}"))?;
            let mut autocommit_outcome = None;
            while let Some(event) = started
                .next_event()
                .map_err(|error| format!("autocommit result stream failed: {error:?}"))?
            {
                if let ClientResultEvent::Terminal(terminal) = event {
                    assert_eq!(terminal.invocation_id, invocation_id);
                    autocommit_outcome = Some(terminal.outcome);
                }
            }
            assert_eq!(
                autocommit_outcome,
                Some(prdb_engine::client_submit::TerminalOutcome::Committed),
                "autocommit RUNAS package did not publish durably"
            );
            drop(client);
            gateway.join().unwrap().map_err(|error| error.to_owned())?;
            supervisor
                .join()
                .unwrap()
                .map_err(|error| error.to_owned())?;

            let (address, gateway, supervisor) = open_service();
            let mut rollback_controller_secret = controller_secret;
            let mut client = SubmissionClient::connect_interactive_authenticated(
                address,
                controller_token_id,
                &mut rollback_controller_secret,
            )?;
            let rollback_transaction_id = match client.interactive_transaction_begin([0xe0; 16])? {
                ClientV2Response::TransactionBegun { transaction_id, .. } => transaction_id,
                response => {
                    return Err(format!("unexpected rollback BEGIN response: {response:?}").into());
                }
            };
            let rollback_package_id = [0xe1; 16];
            let rollback_parameters = encode_parameter_file(
                br#"{"format":"PDB-SQL-PARAMETERS-1","values":[{"type":"integer","value":"11"}]}"#,
            )?;
            let rollback_invocation_id = client.upload_interactive_package_and_seal(
                &package,
                rollback_parameters.as_slice(),
                30_000,
                WorkloadClass::Operational,
                rollback_transaction_id,
                rollback_package_id,
            )?;
            let mut rollback_started = client.start_interactive_package(
                rollback_invocation_id,
                rollback_transaction_id,
                rollback_package_id,
                Some(ClientStartActorCredentials {
                    expected_name: "tcp_runas_actor".to_owned(),
                    token_id: actor_token_id,
                    token_secret: actor_secret.into(),
                }),
                &fixture.0.join("runas-rollback.receipt"),
            )?;
            let mut rollback_package_state = None;
            while let Some(event) = rollback_started.next_event()? {
                match event {
                    ClientResultEvent::PackageTerminal {
                        transaction_id: result_transaction_id,
                        package_id: result_package_id,
                        state,
                        result_lost: false,
                    } => {
                        assert_eq!(result_transaction_id, rollback_transaction_id);
                        assert_eq!(result_package_id, rollback_package_id);
                        rollback_package_state = Some(state);
                    }
                    ClientResultEvent::Data(_) => {}
                    other => {
                        return Err(format!("unexpected rollback package event: {other:?}").into());
                    }
                }
            }
            assert_eq!(
                rollback_package_state,
                Some(ClientV2PackageState::Completed)
            );
            assert_eq!(
                client.interactive_transaction_rollback(rollback_transaction_id)?,
                ClientV2Response::Transaction {
                    transaction_id: rollback_transaction_id,
                    state: ClientV2TransactionState::Aborted,
                }
            );
            drop(client);
            gateway.join().unwrap().map_err(|error| error.to_owned())?;
            supervisor
                .join()
                .unwrap()
                .map_err(|error| error.to_owned())?;

            let (address, gateway, supervisor) = open_service();
            let mut actor_verification_secret = actor_secret;
            let mut client = SubmissionClient::connect_interactive_authenticated(
                address,
                actor_token_id,
                &mut actor_verification_secret,
            )?;
            let verification_invocation_id =
                client.upload_and_seal(&hidden_package, &[], 30_000, WorkloadClass::Operational)?;
            let mut verification_started = client.start(
                verification_invocation_id,
                &fixture.0.join("runas-rollback-verify.receipt"),
            )?;
            let mut verification_output = Vec::new();
            let mut verification_outcome = None;
            while let Some(event) = verification_started.next_event()? {
                match event {
                    ClientResultEvent::Data(data) => verification_output.extend_from_slice(&data),
                    ClientResultEvent::Terminal(terminal) => {
                        assert_eq!(terminal.invocation_id, verification_invocation_id);
                        verification_outcome = Some(terminal.outcome);
                    }
                    other => {
                        return Err(
                            format!("unexpected rollback verification event: {other:?}").into()
                        );
                    }
                }
            }
            assert_eq!(
                verification_outcome,
                Some(prdb_engine::client_submit::TerminalOutcome::Committed)
            );
            let mut durable_rows = Vec::new();
            let mut cursor = 0usize;
            while cursor < verification_output.len() {
                let header = verification_output
                    .get(cursor..cursor + 12)
                    .ok_or("truncated rollback verification SQLB header")?;
                if &header[..4] != b"SQLB" || header[4] != 1 {
                    return Err("invalid rollback verification SQLB header".into());
                }
                let payload_bytes = u32::from_be_bytes(header[8..12].try_into()?) as usize;
                let record_bytes = 12usize
                    .checked_add(payload_bytes)
                    .ok_or("rollback verification SQLB length overflow")?;
                if cursor
                    .checked_add(record_bytes)
                    .is_none_or(|end| end > verification_output.len())
                {
                    return Err("truncated rollback verification SQLB payload".into());
                }
                if header[5] == 2 {
                    let result = &verification_output[cursor + 12..cursor + record_bytes];
                    if !result.starts_with(b"SQLR") || result.len() < 4 {
                        return Err("invalid rollback verification SQLR result".into());
                    }
                    durable_rows.push(result[result.len() - 4..].to_vec());
                }
                cursor += record_bytes;
            }
            durable_rows.sort_unstable();
            assert_eq!(
                durable_rows,
                [
                    7i32.to_be_bytes().to_vec(),
                    7i32.to_be_bytes().to_vec(),
                    9i32.to_be_bytes().to_vec(),
                ]
            );
            drop(client);
            gateway.join().unwrap().map_err(|error| error.to_owned())?;
            supervisor
                .join()
                .unwrap()
                .map_err(|error| error.to_owned())?;

            let (address, gateway, supervisor) = open_service();
            let mut revocation_controller_secret = controller_secret;
            let mut client = SubmissionClient::connect_interactive_authenticated(
                address,
                controller_token_id,
                &mut revocation_controller_secret,
            )?;
            let revocation_transaction_id =
                match client.interactive_transaction_begin([0xf0; 16])? {
                    ClientV2Response::TransactionBegun { transaction_id, .. } => transaction_id,
                    response => {
                        return Err(
                            format!("unexpected revocation BEGIN response: {response:?}").into(),
                        );
                    }
                };
            let revocation_package_id = [0xf1; 16];
            let revocation_parameters = encode_parameter_file(
                br#"{"format":"PDB-SQL-PARAMETERS-1","values":[{"type":"integer","value":"13"}]}"#,
            )?;
            let revocation_invocation_id = client.upload_interactive_package_and_seal(
                &package,
                revocation_parameters.as_slice(),
                30_000,
                WorkloadClass::Operational,
                revocation_transaction_id,
                revocation_package_id,
            )?;
            let mut revocation_started = client.start_interactive_package(
                revocation_invocation_id,
                revocation_transaction_id,
                revocation_package_id,
                Some(ClientStartActorCredentials {
                    expected_name: "tcp_runas_actor".to_owned(),
                    token_id: actor_token_id,
                    token_secret: actor_secret.into(),
                }),
                &fixture.0.join("runas-revocation.receipt"),
            )?;
            let mut revocation_package_state = None;
            while let Some(event) = revocation_started.next_event()? {
                match event {
                    ClientResultEvent::Data(_) => {}
                    ClientResultEvent::PackageTerminal {
                        transaction_id: result_transaction_id,
                        package_id: result_package_id,
                        state,
                        result_lost: false,
                    } => {
                        assert_eq!(result_transaction_id, revocation_transaction_id);
                        assert_eq!(result_package_id, revocation_package_id);
                        revocation_package_state = Some(state);
                    }
                    other => {
                        return Err(
                            format!("unexpected revocation package event: {other:?}").into()
                        );
                    }
                }
            }
            assert_eq!(
                revocation_package_state,
                Some(ClientV2PackageState::Completed),
                "the actor package must stage successfully before revocation"
            );
            storage.revoke_token(
                [0xf2; 16],
                controller.principal_id(),
                actor_token_id,
                expires_at_us,
                test_audit_event(
                    [0xf2; 16],
                    controller.principal_id(),
                    AuditAction::TokenLifecycle,
                    vec![actor_token_id],
                ),
            )?;
            assert!(
                client
                    .interactive_transaction_commit(revocation_transaction_id)
                    .is_err(),
                "COMMIT must reject a staged writer whose credential was revoked"
            );
            assert_eq!(
                storage.interactive_transaction_status(
                    controller_token_id,
                    &controller_secret,
                    revocation_transaction_id,
                )?,
                InteractiveTransactionState::AbortOnly
            );
            drop(client);
            gateway.join().unwrap().map_err(|error| error.to_owned())?;
            let supervisor_result = supervisor.join().unwrap();
            assert!(
                matches!(
                    supervisor_result,
                    Err(ref error) if error.contains("InteractiveTransaction(InvalidActor)")
                ),
                "revocation rejection should be returned by the gateway session: {supervisor_result:?}"
            );

            let (address, gateway, supervisor) = open_service();
            let mut verification_hidden_actor_secret = hidden_actor_secret;
            let mut client = SubmissionClient::connect_authenticated(
                address,
                hidden_actor_token_id,
                &mut verification_hidden_actor_secret,
            )?;
            let verification_invocation_id =
                client.upload_and_seal(&hidden_package, &[], 30_000, WorkloadClass::Operational)?;
            let mut verification_started = client.start(
                verification_invocation_id,
                &fixture.0.join("runas-revocation-verify.receipt"),
            )?;
            let mut verification_output = Vec::new();
            let mut verification_outcome = None;
            while let Some(event) = verification_started.next_event()? {
                match event {
                    ClientResultEvent::Data(data) => verification_output.extend_from_slice(&data),
                    ClientResultEvent::Terminal(terminal) => {
                        assert_eq!(terminal.invocation_id, verification_invocation_id);
                        verification_outcome = Some(terminal.outcome);
                    }
                    other => {
                        return Err(
                            format!("unexpected revocation verification event: {other:?}").into(),
                        );
                    }
                }
            }
            assert_eq!(
                verification_outcome,
                Some(prdb_engine::client_submit::TerminalOutcome::Committed)
            );
            let mut rows_after_revocation = Vec::new();
            let mut cursor = 0usize;
            while cursor < verification_output.len() {
                let header = verification_output
                    .get(cursor..cursor + 12)
                    .ok_or("truncated revocation verification SQLB header")?;
                if &header[..4] != b"SQLB" || header[4] != 1 {
                    return Err("invalid revocation verification SQLB header".into());
                }
                let payload_bytes = u32::from_be_bytes(header[8..12].try_into()?) as usize;
                let record_bytes = 12usize
                    .checked_add(payload_bytes)
                    .ok_or("revocation verification SQLB length overflow")?;
                if cursor
                    .checked_add(record_bytes)
                    .is_none_or(|end| end > verification_output.len())
                {
                    return Err("truncated revocation verification SQLB payload".into());
                }
                if header[5] == 2 {
                    let result = &verification_output[cursor + 12..cursor + record_bytes];
                    if !result.starts_with(b"SQLR") || result.len() < 4 {
                        return Err("invalid revocation verification SQLR result".into());
                    }
                    rows_after_revocation.push(result[result.len() - 4..].to_vec());
                }
                cursor += record_bytes;
            }
            rows_after_revocation.sort_unstable();
            assert_eq!(
                rows_after_revocation,
                [
                    7i32.to_be_bytes().to_vec(),
                    7i32.to_be_bytes().to_vec(),
                    9i32.to_be_bytes().to_vec(),
                ],
                "the revoked actor's staged row became visible after rejected COMMIT"
            );
            drop(client);
            gateway.join().unwrap().map_err(|error| error.to_owned())?;
            supervisor
                .join()
                .unwrap()
                .map_err(|error| error.to_owned())?;
            Ok(transaction_id)
        },
    )?;
    assert_eq!(
        storage.interactive_transaction_status(
            controller_token_id,
            &controller_secret,
            transaction_id,
        )?,
        InteractiveTransactionState::Committed
    );
    controller_secret.zeroize();
    actor_secret.zeroize();
    hidden_actor_secret.zeroize();
    Ok(())
}

impl Drop for Fixture {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

#[test]
fn repeated_begin_request_id_returns_the_same_transaction() {
    let fixture = Fixture::new();
    let mut bootstrap = initialize(&fixture.0, "interactive_admin", now_us() + 86_400_000_000)
        .expect("initialize database");
    let token_id = bootstrap.token_id();
    let mut token_secret = bootstrap.take_secret().expect("bootstrap token secret");
    let instance = bootstrap.acknowledge().expect("publish READY");
    let storage = StorageManager::new(instance.database(), Duration::from_secs(2)).unwrap();
    let deadline = now_us() + 86_400_000_000;
    let begin_request_id = [0xa1; 16];

    let first = storage
        .begin_interactive_transaction_with_request_id(
            token_id,
            &token_secret,
            deadline,
            begin_request_id,
        )
        .unwrap();
    let replay = storage
        .begin_interactive_transaction_with_request_id(
            token_id,
            &token_secret,
            deadline,
            begin_request_id,
        )
        .unwrap();
    assert_eq!(first.transaction_id, replay.transaction_id);
    assert_eq!(replay.state, InteractiveTransactionState::Active);

    let separate_request = storage
        .begin_interactive_transaction_with_request_id(
            token_id,
            &token_secret,
            deadline,
            [0xa2; 16],
        )
        .unwrap();
    assert_ne!(first.transaction_id, separate_request.transaction_id);
    storage
        .rollback_interactive_transaction(token_id, &token_secret, first.transaction_id)
        .unwrap();
    storage
        .rollback_interactive_transaction(token_id, &token_secret, separate_request.transaction_id)
        .unwrap();
    token_secret.zeroize();
}

fn now_us() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_micros() as i64
}

fn package_request<'a>(
    token_id: [u8; 16],
    token_secret: &'a [u8; 32],
    transaction_id: [u8; 16],
    package_id: [u8; 16],
    package_digest: [u8; 32],
) -> InteractivePackageRequest<'a> {
    InteractivePackageRequest {
        token_id,
        token_secret,
        transaction_id,
        package_id,
        package_digest,
        requested_scope: std::collections::HashSet::new(),
        actor: None,
    }
}

#[allow(clippy::too_many_arguments)]
fn package_request_with_actor<'a>(
    token_id: [u8; 16],
    token_secret: &'a [u8; 32],
    actor_token_id: [u8; 16],
    actor_token_secret: &'a [u8; 32],
    transaction_id: [u8; 16],
    package_id: [u8; 16],
    package_digest: [u8; 32],
    expected_name: &'a str,
) -> InteractivePackageRequest<'a> {
    InteractivePackageRequest {
        token_id,
        token_secret,
        transaction_id,
        package_id,
        package_digest,
        requested_scope: std::collections::HashSet::new(),
        actor: Some(prdb_engine::storage_manager::InteractivePackageActorProof {
            expected_name,
            token_id: actor_token_id,
            token_secret: actor_token_secret,
        }),
    }
}

fn test_audit_event(
    event_id: [u8; 16],
    actor_id: [u8; 16],
    action: AuditAction,
    target_ids: Vec<[u8; 16]>,
) -> AuditEvent {
    AuditEvent {
        event_id,
        actor_id: Some(actor_id),
        correlation_id: event_id,
        occurred_at_us: now_us(),
        action,
        result: AuditResult::Succeeded,
        target_ids,
    }
}

#[test]
fn authenticated_transaction_survives_packages_and_commits_once() {
    let fixture = Fixture::new();
    let mut bootstrap = initialize(&fixture.0, "interactive_admin", now_us() + 86_400_000_000)
        .expect("initialize database");
    let token_id = bootstrap.token_id();
    let mut token_secret = bootstrap.take_secret().expect("bootstrap token secret");
    let instance = bootstrap.acknowledge().expect("publish READY");
    let storage = StorageManager::new(instance.database(), Duration::from_secs(2)).unwrap();

    let transaction = storage
        .begin_interactive_transaction(token_id, &token_secret, now_us() + 86_400_000_000)
        .unwrap();
    assert_eq!(transaction.state, InteractiveTransactionState::Active);
    assert_eq!(
        storage
            .attach_interactive_transaction(token_id, &token_secret, transaction.transaction_id)
            .unwrap(),
        InteractiveTransactionState::Active
    );
    let reconnected_storage = storage.clone();
    assert_eq!(
        reconnected_storage
            .attach_interactive_transaction(token_id, &token_secret, transaction.transaction_id)
            .unwrap(),
        InteractiveTransactionState::Active
    );

    let first_context = storage
        .execute_interactive_package(
            package_request(
                token_id,
                &token_secret,
                transaction.transaction_id,
                [1; 16],
                [7; 32],
            ),
            |context| Ok(context as *mut _ as usize),
        )
        .unwrap();
    let second_context = storage
        .execute_interactive_package(
            package_request(
                token_id,
                &token_secret,
                transaction.transaction_id,
                [2; 16],
                [8; 32],
            ),
            |context| Ok(context as *mut _ as usize),
        )
        .unwrap();
    assert_eq!(first_context, second_context);
    assert_eq!(
        storage
            .interactive_package_status(
                token_id,
                &token_secret,
                transaction.transaction_id,
                [1; 16],
            )
            .unwrap()
            .state,
        InteractivePackageState::Completed
    );
    storage
        .mark_interactive_package_result_lost(
            token_id,
            &token_secret,
            transaction.transaction_id,
            [1; 16],
        )
        .unwrap();
    assert!(
        storage
            .interactive_package_status(
                token_id,
                &token_secret,
                transaction.transaction_id,
                [1; 16],
            )
            .unwrap()
            .result_lost
    );
    assert!(matches!(
        storage.execute_interactive_package(
            package_request(
                token_id,
                &token_secret,
                transaction.transaction_id,
                [1; 16],
                [7; 32],
            ),
            |_| Ok(())
        ),
        Err(InteractiveTransactionError::DuplicatePackage)
    ));
    assert!(matches!(
        storage.execute_interactive_package(
            package_request(
                token_id,
                &token_secret,
                transaction.transaction_id,
                [1; 16],
                [9; 32],
            ),
            |_| Ok(())
        ),
        Err(InteractiveTransactionError::PackageConflict)
    ));

    let mut wrong_secret = [0; 32];
    assert!(
        storage
            .attach_interactive_transaction(token_id, &wrong_secret, transaction.transaction_id)
            .is_err()
    );
    wrong_secret.zeroize();

    assert!(
        storage
            .commit_interactive_transaction(token_id, &token_secret, transaction.transaction_id)
            .unwrap()
            > 0
    );
    assert_eq!(
        storage
            .interactive_transaction_status(token_id, &token_secret, transaction.transaction_id)
            .unwrap(),
        InteractiveTransactionState::Committed
    );
    token_secret.zeroize();
}

#[test]
fn rollback_and_package_failure_are_transaction_wide() {
    let fixture = Fixture::new();
    let mut bootstrap = initialize(&fixture.0, "interactive_admin", now_us() + 86_400_000_000)
        .expect("initialize database");
    let token_id = bootstrap.token_id();
    let mut token_secret = bootstrap.take_secret().expect("bootstrap token secret");
    let instance = bootstrap.acknowledge().expect("publish READY");
    let storage = StorageManager::new(instance.database(), Duration::from_secs(2)).unwrap();

    let failed = storage
        .begin_interactive_transaction(token_id, &token_secret, now_us() + 86_400_000_000)
        .unwrap();
    let result: Result<(), _> = storage.execute_interactive_package(
        package_request(
            token_id,
            &token_secret,
            failed.transaction_id,
            [3; 16],
            [9; 32],
        ),
        |_| Err(prdb_engine::transaction::TransactionError::InvalidContext),
    );
    assert!(result.is_err());
    assert_eq!(
        storage
            .interactive_transaction_status(token_id, &token_secret, failed.transaction_id)
            .unwrap(),
        InteractiveTransactionState::AbortOnly
    );
    assert_eq!(
        storage
            .interactive_package_status(token_id, &token_secret, failed.transaction_id, [3; 16])
            .unwrap()
            .state,
        InteractivePackageState::Failed
    );
    storage
        .rollback_interactive_transaction(token_id, &token_secret, failed.transaction_id)
        .unwrap();
    assert_eq!(
        storage
            .interactive_transaction_status(token_id, &token_secret, failed.transaction_id)
            .unwrap(),
        InteractiveTransactionState::Aborted
    );

    let rolled_back = storage
        .begin_interactive_transaction(token_id, &token_secret, now_us() + 86_400_000_000)
        .unwrap();
    storage
        .rollback_interactive_transaction(token_id, &token_secret, rolled_back.transaction_id)
        .unwrap();
    assert_eq!(
        storage
            .interactive_transaction_status(token_id, &token_secret, rolled_back.transaction_id)
            .unwrap(),
        InteractiveTransactionState::Aborted
    );
    token_secret.zeroize();
}

#[test]
fn transaction_lifetime_expiry_aborts_and_releases_the_snapshot() {
    let fixture = Fixture::new();
    let mut bootstrap = initialize(&fixture.0, "interactive_admin", now_us() + 86_400_000_000)
        .expect("initialize database");
    let token_id = bootstrap.token_id();
    let mut token_secret = bootstrap.take_secret().expect("bootstrap token secret");
    let instance = bootstrap.acknowledge().expect("publish READY");
    let storage = StorageManager::with_config(
        instance.database(),
        StorageManagerConfig {
            max_invocation_age: Duration::from_millis(1),
            ..StorageManagerConfig::default()
        },
    )
    .unwrap();
    let transaction = storage
        .begin_interactive_transaction(token_id, &token_secret, now_us() + 86_400_000_000)
        .unwrap();
    std::thread::sleep(Duration::from_millis(5));
    assert_eq!(
        storage.reap_interactive_transactions(now_us(), 1).unwrap(),
        1
    );
    assert_eq!(
        storage
            .interactive_transaction_status(token_id, &token_secret, transaction.transaction_id)
            .unwrap(),
        InteractiveTransactionState::Expired
    );
    token_secret.zeroize();
}

#[test]
fn concurrent_package_start_is_rejected_while_one_package_is_running() {
    let fixture = Fixture::new();
    let mut bootstrap = initialize(&fixture.0, "interactive_admin", now_us() + 86_400_000_000)
        .expect("initialize database");
    let token_id = bootstrap.token_id();
    let mut token_secret = bootstrap.take_secret().expect("bootstrap token secret");
    let instance = bootstrap.acknowledge().expect("publish READY");
    let storage = StorageManager::new(instance.database(), Duration::from_secs(2)).unwrap();
    let transaction = storage
        .begin_interactive_transaction(token_id, &token_secret, now_us() + 86_400_000_000)
        .unwrap();
    let started = Barrier::new(2);
    let finish = Barrier::new(2);

    std::thread::scope(|scope| {
        let worker_storage = storage.clone();
        let worker_started = &started;
        let worker_finish = &finish;
        let worker = scope.spawn(move || {
            worker_storage.execute_interactive_package(
                package_request(
                    token_id,
                    &token_secret,
                    transaction.transaction_id,
                    [4; 16],
                    [4; 32],
                ),
                |_| {
                    worker_started.wait();
                    worker_finish.wait();
                    Ok(())
                },
            )
        });
        started.wait();
        let concurrent = storage.execute_interactive_package(
            package_request(
                token_id,
                &token_secret,
                transaction.transaction_id,
                [5; 16],
                [5; 32],
            ),
            |_| Ok(()),
        );
        assert!(matches!(
            concurrent,
            Err(InteractiveTransactionError::ConcurrentOperation)
        ));
        finish.wait();
        worker.join().unwrap().unwrap();
    });
    storage
        .rollback_interactive_transaction(token_id, &token_secret, transaction.transaction_id)
        .unwrap();
    token_secret.zeroize();
}

#[test]
fn v2_session_routes_authenticated_transaction_control_across_connections() {
    let fixture = Fixture::new();
    let mut bootstrap = initialize(&fixture.0, "interactive_admin", now_us() + 86_400_000_000)
        .expect("initialize database");
    let token_id = bootstrap.token_id();
    let mut token_secret = bootstrap.take_secret().expect("bootstrap token secret");
    let instance = bootstrap.acknowledge().expect("publish READY");
    let storage = StorageManager::new(instance.database(), Duration::from_secs(2)).unwrap();

    let mut first_connection = ClientConnectionSession::default();
    first_connection
        .accept_hello(ClientMode::Query, "interactive-test")
        .unwrap();
    first_connection
        .authenticate(&storage, token_id, &token_secret)
        .unwrap();
    let begin_request = ClientV2Request::TxBegin {
        begin_request_id: [0x89; 16],
    };
    let (kind, payload) = begin_request.encode().unwrap();
    let begun = first_connection
        .handle_v2_transaction_request(
            &storage,
            ClientFrame {
                kind,
                request_id: 1,
                response_sequence: 0,
                payload,
            },
            now_us() + 86_400_000_000,
        )
        .unwrap();
    assert_eq!(begun.kind, 100);
    let ClientV2Response::TransactionBegun { transaction_id, .. } =
        ClientV2Response::decode(CLIENT_V2_TX_BEGIN_KIND, &begun.payload).unwrap()
    else {
        panic!("TX_BEGIN response has the wrong shape");
    };
    let (replay_kind, replay_payload) = begin_request.encode().unwrap();
    let replay = first_connection
        .handle_v2_transaction_request(
            &storage,
            ClientFrame {
                kind: replay_kind,
                request_id: 2,
                response_sequence: 0,
                payload: replay_payload,
            },
            now_us() + 86_400_000_000,
        )
        .unwrap();
    assert_eq!(
        ClientV2Response::decode(replay_kind, &replay.payload).unwrap(),
        ClientV2Response::decode(kind, &begun.payload).unwrap()
    );

    let mut reconnected = ClientConnectionSession::default();
    reconnected
        .accept_hello(ClientMode::Query, "interactive-test")
        .unwrap();
    reconnected
        .authenticate(&storage, token_id, &token_secret)
        .unwrap();
    let (kind, payload) = ClientV2Request::TxAttach { transaction_id }
        .encode()
        .unwrap();
    let attached = reconnected
        .handle_v2_transaction_request(
            &storage,
            ClientFrame {
                kind,
                request_id: 1,
                response_sequence: 0,
                payload,
            },
            now_us() + 86_400_000_000,
        )
        .unwrap();
    assert_eq!(
        ClientV2Response::decode(CLIENT_V2_TX_ATTACH_KIND, &attached.payload).unwrap(),
        ClientV2Response::Transaction {
            transaction_id,
            state: ClientV2TransactionState::Active,
        }
    );

    let (kind, payload) = ClientV2Request::TxCommit { transaction_id }
        .encode()
        .unwrap();
    let committed = reconnected
        .handle_v2_transaction_request(
            &storage,
            ClientFrame {
                kind,
                request_id: 2,
                response_sequence: 0,
                payload,
            },
            now_us() + 86_400_000_000,
        )
        .unwrap();
    assert_eq!(
        ClientV2Response::decode(CLIENT_V2_TX_COMMIT_KIND, &committed.payload).unwrap(),
        ClientV2Response::Transaction {
            transaction_id,
            state: ClientV2TransactionState::Committed,
        }
    );
    token_secret.zeroize();
}
