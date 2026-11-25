#![cfg(all(unix, target_os = "linux", target_arch = "x86_64"))]

use prdb_engine::bootstrap::initialize;
use prdb_engine::client_credentials::write_client_credentials;
use prdb_engine::client_protocol::ClientCredentials;
use prdb_engine::client_submit::{
    AdminClient, AdminObjectGrantTarget, AdminObjectPrivilege, CancellationResult,
    InvocationStatus, SubmissionClient,
};
use prdb_engine::submission_manifest::{encode_manifest_v1, manifest_v1_from_elf};
use prdb_engine::supervisor::WorkloadClass;
use sha2::{Digest, Sha256};
use std::io::{BufRead, BufReader};
use std::os::unix::fs::PermissionsExt;
use std::process::{Command, Stdio};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use super::helpers::*;

pub(super) fn self_referencing_create_table_sql_crosses_gateway_and_supervisor_processes()
-> Result<(), Box<dyn std::error::Error>> {
    fn runtime_paths(name: &str) -> Result<Vec<std::path::PathBuf>, Box<dyn std::error::Error>> {
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

    let nonce = SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos();
    let root = std::env::temp_dir().join(format!(
        "prdb-self-reference-process-{}-{nonce}",
        std::process::id()
    ));
    std::fs::create_dir(&root)?;
    std::fs::set_permissions(&root, std::fs::Permissions::from_mode(0o700))?;
    let _cleanup = DirectoryGuard(root.clone());

    let instance_dir = root.join("instance");
    let mut setup = initialize(&instance_dir, "selfref_process_admin", i64::MAX)?;
    let token_id = setup.token_id();
    let admin_user_id = setup.admin_user_id();
    let token_secret = setup.take_secret().ok_or("bootstrap secret missing")?;
    drop(setup.acknowledge()?);
    install_test_sdk_repository(&instance_dir)?;

    let cgroup_parent = std::env::var_os("PRDB_TEST_CGROUP_PARENT")
        .map(std::path::PathBuf::from)
        .ok_or("PRDB_TEST_CGROUP_PARENT must name a delegated cgroup v2 parent")?;
    if !cgroup_parent.is_absolute() {
        return Err("PRDB_TEST_CGROUP_PARENT must be absolute".into());
    }
    let runtime_read_execute = runtime_paths("PRDB_TEST_RUNTIME_READ_EXECUTE")?;
    let runtime_read_only = runtime_paths("PRDB_TEST_RUNTIME_READ_ONLY")?;
    let service_socket = root.join("supervisor.sock");
    let uid = unsafe { libc::geteuid() };
    let uid_string = uid.to_string();
    let mut supervisor_command = Command::new(env!("CARGO_BIN_EXE_prdb-supervisor"));
    supervisor_command.args([
        "--instance-dir",
        instance_dir.to_str().ok_or("non-UTF-8 instance path")?,
        "--service-socket",
        service_socket.to_str().ok_or("non-UTF-8 socket path")?,
        "--gateway-uid",
        &uid_string,
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
        "--allowed-dependency",
        "libc.so.6",
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
    let mut supervisor = ChildGuard(Some(
        supervisor_command
            .stdout(Stdio::null())
            .stderr(Stdio::inherit())
            .spawn()?,
    ));
    let socket_deadline = Instant::now() + Duration::from_secs(5);
    while !service_socket.exists() {
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
                service_socket.to_str().ok_or("non-UTF-8 socket path")?,
                "--service-uid",
                &uid_string,
            ])
            .stdout(Stdio::null())
            .stderr(Stdio::piped())
            .spawn()?,
    ));
    let gateway_stderr = gateway
        .child()
        .stderr
        .take()
        .ok_or("gateway stderr missing")?;
    let (address_sender, address_receiver) = std::sync::mpsc::channel();
    std::thread::spawn(move || {
        let mut line = String::new();
        let result = BufReader::new(gateway_stderr)
            .read_line(&mut line)
            .map(|_| line);
        let _ = address_sender.send(result);
    });
    let address_line = address_receiver.recv_timeout(Duration::from_secs(5))??;
    let address: std::net::SocketAddr = address_line
        .trim()
        .strip_prefix("prdb-gateway listening on ")
        .ok_or("gateway listen line had an unexpected prefix")?
        .parse()?;

    let mut authenticated_secret = token_secret;
    let mut admin =
        AdminClient::connect_authenticated(address, token_id, &mut authenticated_secret)?;
    assert_eq!(authenticated_secret, [0; 32]);
    let created_schema = admin.execute_create_schema_sql("CREATE SCHEMA selfref_app")?;
    let credentials_path = root.join("selfref-admin.cred");
    write_client_credentials(
        &credentials_path,
        &ClientCredentials {
            token_id,
            token_secret,
        },
    )?;
    let sql_path = root.join("selfref-create-table.sql");
    let address_text = address.to_string();
    let execute_admin_sql = |sql: &str| -> std::io::Result<std::process::Output> {
        std::fs::write(&sql_path, sql)?;
        Command::new(env!("CARGO_BIN_EXE_prdb-client"))
            .args([
                "admin-sql",
                "--address",
                &address_text,
                "--credentials",
                credentials_path
                    .to_str()
                    .ok_or_else(|| std::io::Error::other("non-UTF-8 credentials path"))?,
                "--sql-file",
                sql_path
                    .to_str()
                    .ok_or_else(|| std::io::Error::other("non-UTF-8 SQL path"))?,
            ])
            .output()
    };
    let absent_drop = execute_admin_sql("DROP TABLE IF EXISTS selfref_app.not_present RESTRICT;")?;
    assert!(
        absent_drop.status.success(),
        "administrator DROP TABLE IF EXISTS failed: {}",
        String::from_utf8_lossy(&absent_drop.stderr)
    );
    assert!(String::from_utf8_lossy(&absent_drop.stdout).contains("table did not exist"));
    let absent_schema_drop = execute_admin_sql("DROP SCHEMA IF EXISTS absent_schema RESTRICT;")?;
    assert!(
        absent_schema_drop.status.success(),
        "administrator DROP SCHEMA IF EXISTS failed: {}",
        String::from_utf8_lossy(&absent_schema_drop.stderr)
    );
    assert!(String::from_utf8_lossy(&absent_schema_drop.stdout).contains("schema did not exist"));
    let drop_probe_user = admin.create_user("drop_probe_user")?;
    let token_expiry_us = i64::try_from(SystemTime::now().duration_since(UNIX_EPOCH)?.as_micros())?
        .saturating_add(3_600_000_000);
    let drop_probe_token = admin.issue_token(
        drop_probe_user.identity_id,
        "drop-if-exists authorization test",
        token_expiry_us,
    )?;
    let mut drop_probe_secret = *drop_probe_token.token_secret();
    let mut drop_probe_admin = AdminClient::connect_authenticated(
        address,
        drop_probe_token.token_id,
        &mut drop_probe_secret,
    )?;
    assert!(
        drop_probe_admin
            .execute_drop_table_if_exists_sql(
                "DROP TABLE IF EXISTS selfref_app.not_present RESTRICT",
            )
            .is_err(),
        "non-administrator must not suppress an absent-table result"
    );
    assert!(
        drop_probe_admin
            .execute_drop_schema_if_exists_sql("DROP SCHEMA IF EXISTS absent_schema RESTRICT")
            .is_err(),
        "non-administrator must not suppress an absent-schema result"
    );
    let cli_output = execute_admin_sql(
        "CREATE TABLE selfref_app.nodes (tenant_id INTEGER, id INTEGER, parent_tenant_id INTEGER, parent_id INTEGER, PRIMARY KEY (tenant_id, id), CONSTRAINT node_tenant_nonnegative CHECK (tenant_id >= 0), FOREIGN KEY (parent_tenant_id, parent_id) REFERENCES selfref_app.nodes (tenant_id, id));",
    )?;
    assert!(
        cli_output.status.success(),
        "admin-sql self-referencing CREATE TABLE failed: {}",
        String::from_utf8_lossy(&cli_output.stderr)
    );
    let restricted_schema_drop = execute_admin_sql("DROP SCHEMA IF EXISTS selfref_app RESTRICT;")?;
    assert!(
        !restricted_schema_drop.status.success(),
        "DROP SCHEMA IF EXISTS must preserve RESTRICT failures for a present schema"
    );
    let cli_stdout = String::from_utf8_lossy(&cli_output.stdout);
    let table_id_text = cli_stdout
        .split_whitespace()
        .nth(2)
        .ok_or("admin-sql output omitted the created table ID")?;
    let table_id = decode_id_hex(table_id_text).ok_or("admin-sql returned an invalid table ID")?;
    let columns = admin.list_columns_page(created_schema.identity_id, table_id, 16, None)?;
    assert_eq!(columns.records.len(), 4);
    let constraints =
        admin.list_constraints_page(created_schema.identity_id, table_id, 16, None)?;
    let foreign_key = constraints
        .records
        .iter()
        .find(|constraint| {
            constraint.kind
                == prdb_engine::metadata_discovery::AdministrativeConstraintKind::ForeignKey
        })
        .ok_or("self-referencing FK was absent from process metadata")?;
    let named_check = constraints
        .records
        .iter()
        .find(|constraint| {
            constraint.kind == prdb_engine::metadata_discovery::AdministrativeConstraintKind::Check
                && constraint.name == "node_tenant_nonnegative"
        })
        .ok_or("named CREATE TABLE CHECK was absent from process metadata")?;
    assert_ne!(named_check.constraint_id, [0; 16]);
    assert_eq!(foreign_key.referenced_table_id, Some(table_id));
    assert_eq!(
        foreign_key.column_ids,
        vec![columns.records[2].column_id, columns.records[3].column_id]
    );
    assert_eq!(
        foreign_key.referenced_column_ids,
        vec![columns.records[0].column_id, columns.records[1].column_id]
    );
    assert!(constraints.next_cursor.is_none());

    let customers = admin.execute_create_table_sql(
        "CREATE TABLE selfref_app.customers (id INTEGER NOT NULL, email TEXT)",
    )?;
    for sql in [
        "ALTER TABLE selfref_app.customers ADD CONSTRAINT customers_pk PRIMARY KEY (id)",
        "ALTER TABLE selfref_app.customers ADD CONSTRAINT customers_email_key UNIQUE (email)",
    ] {
        let output = execute_admin_sql(sql)?;
        assert!(
            output.status.success(),
            "named key ALTER failed for {sql}: {}",
            String::from_utf8_lossy(&output.stderr)
        );
    }
    let orders = admin.execute_create_table_sql(
        "CREATE TABLE selfref_app.orders (id INTEGER NOT NULL, customer_id INTEGER)",
    )?;
    admin.grant_object_privilege(
        admin_user_id,
        customers.table_id,
        prdb_engine::client_submit::AdminObjectPrivilege::References,
        prdb_engine::client_submit::AdminObjectGrantTarget::Object,
    )?;
    for sql in [
        "ALTER TABLE selfref_app.orders ADD CONSTRAINT orders_pk PRIMARY KEY (id)",
        "ALTER TABLE selfref_app.orders ADD CONSTRAINT orders_customer_fk FOREIGN KEY (customer_id) REFERENCES selfref_app.customers (id)",
    ] {
        let output = execute_admin_sql(sql)?;
        assert!(
            output.status.success(),
            "named key/FK ALTER failed for {sql}: {}",
            String::from_utf8_lossy(&output.stderr)
        );
    }
    let customer_constraints =
        admin.list_constraints_page(created_schema.identity_id, customers.table_id, 16, None)?;
    for name in ["customers_pk", "customers_email_key"] {
        assert!(
            customer_constraints
                .records
                .iter()
                .any(|constraint| constraint.name == name)
        );
    }
    let order_constraints =
        admin.list_constraints_page(created_schema.identity_id, orders.table_id, 16, None)?;
    for name in ["orders_pk", "orders_customer_fk"] {
        assert!(
            order_constraints
                .records
                .iter()
                .any(|constraint| constraint.name == name)
        );
    }
    for sql in [
        "ALTER TABLE selfref_app.orders DROP CONSTRAINT orders_customer_fk",
        "ALTER TABLE selfref_app.orders DROP CONSTRAINT orders_pk",
        "ALTER TABLE selfref_app.customers DROP CONSTRAINT customers_email_key",
        "ALTER TABLE selfref_app.customers DROP CONSTRAINT customers_pk",
    ] {
        let output = execute_admin_sql(sql)?;
        assert!(
            output.status.success(),
            "name-based constraint drop failed for {sql}: {}",
            String::from_utf8_lossy(&output.stderr)
        );
    }
    let schema_id_text = encode_id_hex(&created_schema.identity_id);
    let table_id_text = encode_id_hex(&table_id);
    let expected_foreign_key = format!(
        "\"constraint_id\":\"{}\",\"kind\":\"foreign_key\",\"column_ids\":[\"{}\",\"{}\"],\"referenced_table_id\":\"{}\",\"referenced_column_ids\":[\"{}\",\"{}\"]",
        encode_id_hex(&foreign_key.constraint_id),
        encode_id_hex(&columns.records[2].column_id),
        encode_id_hex(&columns.records[3].column_id),
        encode_id_hex(&table_id),
        encode_id_hex(&columns.records[0].column_id),
        encode_id_hex(&columns.records[1].column_id)
    );
    let constraint_list = Command::new(env!("CARGO_BIN_EXE_prdb-client"))
        .args([
            "constraint-list",
            "--address",
            &address_text,
            "--credentials",
            credentials_path
                .to_str()
                .ok_or("non-UTF-8 credentials path")?,
            "--schema-id",
            &schema_id_text,
            "--table-id",
            &table_id_text,
        ])
        .output()?;
    assert!(
        constraint_list.status.success(),
        "constraint-list failed: {}",
        String::from_utf8_lossy(&constraint_list.stderr)
    );
    let constraint_json = String::from_utf8_lossy(&constraint_list.stdout);
    assert!(
        constraint_json.contains(&expected_foreign_key),
        "constraint-list omitted ordered self-referencing FK metadata: {constraint_json}"
    );
    assert!(constraint_json.ends_with("]}\n"));

    let many_constraint_columns = (0..130)
        .map(|ordinal| format!("column_{ordinal} INTEGER"))
        .collect::<Vec<_>>()
        .join(", ");
    let many_unique_constraints = (0..130)
        .map(|ordinal| format!("UNIQUE (column_{ordinal})"))
        .collect::<Vec<_>>()
        .join(", ");
    let many_constraint_table = admin.execute_create_table_sql(&format!(
        "CREATE TABLE selfref_app.many_constraints ({many_constraint_columns}, {many_unique_constraints});"
    ))?;
    let many_table_id_text = encode_id_hex(&many_constraint_table.table_id);
    let many_constraint_list = Command::new(env!("CARGO_BIN_EXE_prdb-client"))
        .args([
            "constraint-list",
            "--address",
            &address_text,
            "--credentials",
            credentials_path
                .to_str()
                .ok_or("non-UTF-8 credentials path")?,
            "--schema-id",
            &schema_id_text,
            "--table-id",
            &many_table_id_text,
        ])
        .output()?;
    assert!(
        many_constraint_list.status.success(),
        "constraint-list failed to continue across pages: {}",
        String::from_utf8_lossy(&many_constraint_list.stderr)
    );
    let many_constraint_json = String::from_utf8_lossy(&many_constraint_list.stdout);
    assert_eq!(
        many_constraint_json.matches("\"kind\":\"unique\"").count(),
        130,
        "constraint-list omitted UNIQUE constraints across pages: {many_constraint_json}"
    );
    let many_index_list = Command::new(env!("CARGO_BIN_EXE_prdb-client"))
        .args([
            "index-list",
            "--address",
            &address_text,
            "--credentials",
            credentials_path
                .to_str()
                .ok_or("non-UTF-8 credentials path")?,
            "--schema-id",
            &schema_id_text,
            "--table-id",
            &many_table_id_text,
        ])
        .output()?;
    assert!(
        many_index_list.status.success(),
        "index-list failed to continue across pages: {}",
        String::from_utf8_lossy(&many_index_list.stderr)
    );
    let many_index_json = String::from_utf8_lossy(&many_index_list.stdout);
    assert_eq!(
        many_index_json.matches("\"unique\":true").count(),
        130,
        "index-list omitted constraint-owned indexes across pages: {many_index_json}"
    );
    let drop_output = execute_admin_sql("DROP TABLE IF EXISTS selfref_app.nodes RESTRICT;")?;
    assert!(
        drop_output.status.success(),
        "admin-sql DROP TABLE failed: {}",
        String::from_utf8_lossy(&drop_output.stderr)
    );
    assert!(String::from_utf8_lossy(&drop_output.stdout).contains("dropped table"));
    let relations = admin.list_relations_page(created_schema.identity_id, 16, None)?;
    assert!(
        relations
            .records
            .iter()
            .all(|relation| relation.name != "nodes")
    );
    let parent_create =
        execute_admin_sql("CREATE TABLE selfref_app.parents (id INTEGER PRIMARY KEY);")?;
    assert!(
        parent_create.status.success(),
        "admin-sql CREATE TABLE parent failed: {}",
        String::from_utf8_lossy(&parent_create.stderr)
    );
    let parent_table_id = decode_id_hex(
        String::from_utf8_lossy(&parent_create.stdout)
            .split_whitespace()
            .nth(2)
            .ok_or("admin-sql parent output omitted the table ID")?,
    )
    .ok_or("admin-sql returned an invalid parent table ID")?;
    admin.grant_object_privilege(
        admin_user_id,
        parent_table_id,
        AdminObjectPrivilege::References,
        AdminObjectGrantTarget::AllColumns,
    )?;
    let child_create = execute_admin_sql(
        "CREATE TABLE selfref_app.children (parent_id INTEGER REFERENCES selfref_app.parents (id));",
    )?;
    assert!(
        child_create.status.success(),
        "admin-sql CREATE TABLE child failed: {}",
        String::from_utf8_lossy(&child_create.stderr)
    );
    let child_table_id = decode_id_hex(
        String::from_utf8_lossy(&child_create.stdout)
            .split_whitespace()
            .nth(2)
            .ok_or("admin-sql child output omitted the table ID")?,
    )
    .ok_or("admin-sql returned an invalid child table ID")?;
    let restricted_drop = execute_admin_sql("DROP TABLE IF EXISTS selfref_app.parents RESTRICT;")?;
    assert!(!restricted_drop.status.success());
    let relations = admin.list_relations_page(created_schema.identity_id, 16, None)?;
    assert!(
        relations
            .records
            .iter()
            .any(|relation| relation.name == "parents")
    );
    assert!(
        relations
            .records
            .iter()
            .any(|relation| relation.name == "children")
    );
    let child_constraints =
        admin.list_constraints_page(created_schema.identity_id, child_table_id, 128, None)?;
    let child_foreign_key = child_constraints
        .records
        .iter()
        .find(|constraint| {
            constraint.kind
                == prdb_engine::metadata_discovery::AdministrativeConstraintKind::ForeignKey
        })
        .ok_or("external FK was absent from child metadata")?;
    let child_table_id_text = encode_id_hex(&child_table_id);
    let child_constraint_list = Command::new(env!("CARGO_BIN_EXE_prdb-client"))
        .args([
            "constraint-list",
            "--address",
            &address_text,
            "--credentials",
            credentials_path
                .to_str()
                .ok_or("non-UTF-8 credentials path")?,
            "--schema-id",
            &schema_id_text,
            "--table-id",
            &child_table_id_text,
        ])
        .output()?;
    assert!(
        child_constraint_list.status.success(),
        "constraint-list failed for external FK: {}",
        String::from_utf8_lossy(&child_constraint_list.stderr)
    );
    let child_constraint_json = String::from_utf8_lossy(&child_constraint_list.stdout);
    let foreign_key_kind_offset = child_constraint_json
        .find("\"kind\":\"foreign_key\"")
        .ok_or("constraint-list omitted external FK kind")?;
    let constraint_id_start = child_constraint_json[..foreign_key_kind_offset]
        .rfind("\"constraint_id\":\"")
        .ok_or("constraint-list omitted external FK ID")?
        + "\"constraint_id\":\"".len();
    let constraint_id_end = constraint_id_start + 32;
    let child_constraint_id = &child_constraint_json[constraint_id_start..constraint_id_end];
    assert_eq!(
        child_constraint_id,
        encode_id_hex(&child_foreign_key.constraint_id)
    );
    let drop_constraint = execute_admin_sql(&format!(
        "ALTER TABLE selfref_app.children DROP CONSTRAINT '{child_constraint_id}';"
    ))?;
    assert!(
        drop_constraint.status.success(),
        "admin-sql DROP CONSTRAINT failed: {}",
        String::from_utf8_lossy(&drop_constraint.stderr)
    );
    let parent_drop = execute_admin_sql("DROP TABLE selfref_app.parents RESTRICT;")?;
    assert!(
        parent_drop.status.success(),
        "admin-sql parent DROP TABLE remained restricted after FK drop: {}",
        String::from_utf8_lossy(&parent_drop.stderr)
    );
    let child_drop = execute_admin_sql("DROP TABLE selfref_app.children RESTRICT;")?;
    assert!(
        child_drop.status.success(),
        "admin-sql child DROP TABLE failed: {}",
        String::from_utf8_lossy(&child_drop.stderr)
    );
    drop(admin);

    assert!(gateway.stop()?.success());
    assert!(supervisor.stop()?.success());
    Ok(())
}

pub(super) fn disconnected_submission_is_reconciled_after_gateway_restart()
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
    let sdk_release_dir = std::env::var_os("PRDB_TEST_INSTALLED_SDK_RELEASE_DIR")
        .map(std::path::PathBuf::from)
        .ok_or("PRDB_TEST_INSTALLED_SDK_RELEASE_DIR is required for SQL package tests")?;
    let sdk_library_path = sdk_release_dir.join("libprdb_sql.so.1");
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
        .arg("-Wl,--no-as-needed")
        .arg(&sdk_library_path)
        .arg("-Wl,--no-as-needed")
        .arg("-lc")
        .arg("-Wl,--as-needed")
        .arg("-Wl,-rpath,$ORIGIN")
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
        "nixos-x86_64-glibc-2.42".to_owned(),
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
    install_test_sdk_repository(&instance_dir)?;

    let service_socket = root.join("supervisor.sock");
    let uid = unsafe { libc::geteuid() };
    let uid_string = uid.to_string();
    let cgroup_string = cgroup_parent.to_str().ok_or("non-UTF-8 cgroup path")?;
    let audit_sink_dir = root.join("supervisor-audit");
    let audit_jsonl_path = audit_sink_dir.join("events.jsonl");
    let mut supervisor_command = Command::new(env!("CARGO_BIN_EXE_prdb-supervisor"));
    supervisor_command.env("PRDB_AUDIT_JSONL_PATH", &audit_jsonl_path);
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
    let mut invalid_admin_secret = [0; 32];
    assert!(
        SubmissionClient::connect_authenticated(address, token_id, &mut invalid_admin_secret)
            .is_err()
    );
    let authentication_audit = admin.list_audit_events_page(128, None)?;
    assert!(authentication_audit.events.iter().any(|event| {
        event.action == 4
            && event.result == 2
            && event.actor_id.is_none()
            && event.target_ids.as_slice() == [token_id]
    }));
    assert!(
        !audit_jsonl_path.exists(),
        "a failed sink must leave its audit event pending without changing authentication"
    );
    std::fs::create_dir(&audit_sink_dir)?;
    let sink_deadline = Instant::now() + Duration::from_secs(5);
    loop {
        if let Ok(lines) = std::fs::read_to_string(&audit_jsonl_path)
            && lines.contains("\"action\":4")
            && lines.contains("\"result\":2")
        {
            assert!(lines.contains("\"actor_id\":null"));
            assert!(lines.contains(&format!(
                "\"target_ids\":[\"{}\"]",
                encode_id_hex(&token_id)
            )));
            break;
        }
        if Instant::now() >= sink_deadline {
            return Err("supervisor did not deliver the authentication audit event".into());
        }
        std::thread::sleep(Duration::from_millis(20));
    }
    let runtime_list_credentials_path = root.join("runtime-list-admin.cred");
    write_client_credentials(
        &runtime_list_credentials_path,
        &ClientCredentials {
            token_id,
            token_secret,
        },
    )?;
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
    let recovery_token = admin
        .issue_token(
            query_user.identity_id,
            "process query recovery",
            query_token_expiry,
        )
        .map_err(|error| format!("issue recovery token failed: {error:?}"))?;
    let recovery_token_id = recovery_token.token_id;
    let mut restart_recovery_secret = *recovery_token.token_secret();
    let mut durable_recovery_secret = *recovery_token.token_secret();
    let mut interactive_restart_secret = *recovery_token.token_secret();
    let invocation_list_credentials_path = root.join("invocation-list-owner.cred");
    write_client_credentials(
        &invocation_list_credentials_path,
        &ClientCredentials {
            token_id: query_token_id,
            token_secret: query_secret,
        },
    )?;
    let mut restart_query_secret = query_secret;
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
        let invocation_list = Command::new(env!("CARGO_BIN_EXE_prdb-client"))
            .args([
                "invocation-list",
                "--address",
                &address.to_string(),
                "--credentials",
                invocation_list_credentials_path
                    .to_str()
                    .ok_or("credential path is not UTF-8")?,
            ])
            .output()?;
        assert!(
            invocation_list.status.success(),
            "invocation-list CLI failed: {}",
            String::from_utf8_lossy(&invocation_list.stderr)
        );
        let invocation_list_output = String::from_utf8(invocation_list.stdout)?;
        assert!(invocation_list_output.contains("\"consistency\":\"best_effort\""));
        assert!(invocation_list_output.contains(&format!(
            "\"invocation_id\":\"{}\"",
            encode_id_hex(&invocation_id)
        )));
        assert!(invocation_list_output.contains("\"state\":\"active\""));
        assert!(invocation_list_output.contains("\"staged_rows\":0"));
        assert!(invocation_list_output.contains("\"staged_value_bytes\":0"));
        assert!(!invocation_list_output.contains("\"principal_id\":"));
        let other_owner_list = Command::new(env!("CARGO_BIN_EXE_prdb-client"))
            .args([
                "invocation-list",
                "--address",
                &address.to_string(),
                "--credentials",
                runtime_list_credentials_path
                    .to_str()
                    .ok_or("credential path is not UTF-8")?,
            ])
            .output()?;
        assert!(
            other_owner_list.status.success(),
            "other-owner invocation-list CLI failed: {}",
            String::from_utf8_lossy(&other_owner_list.stderr)
        );
        let other_owner_output = String::from_utf8(other_owner_list.stdout)?;
        assert!(other_owner_output.contains("\"invocations\":[]"));
        assert!(!other_owner_output.contains(&encode_id_hex(&invocation_id)));
        let mut unrelated_owner_secret = token_secret;
        let mut unrelated_owner = SubmissionClient::connect_authenticated(
            address,
            token_id,
            &mut unrelated_owner_secret,
        )?;
        assert_eq!(
            unrelated_owner.status(invocation_id)?,
            InvocationStatus::Unknown,
            "cross-owner status must not reveal the invocation"
        );
        let runtime_list = Command::new(env!("CARGO_BIN_EXE_prdb-client"))
            .args([
                "runtime-list",
                "--address",
                &address.to_string(),
                "--credentials",
                runtime_list_credentials_path
                    .to_str()
                    .ok_or("credential path is not UTF-8")?,
            ])
            .output()?;
        assert!(
            runtime_list.status.success(),
            "runtime-list CLI failed: {}",
            String::from_utf8_lossy(&runtime_list.stderr)
        );
        let runtime_output = String::from_utf8(runtime_list.stdout)?;
        assert!(runtime_output.contains("\"observed_at_us\":"));
        assert!(runtime_output.contains("\"consistency\":\"best_effort\""));
        assert!(runtime_output.contains("\"invocations\":["));
        assert!(runtime_output.contains("\"workload_class\":\"operational\""));
        assert!(runtime_output.contains("\"staged_rows\":0"));
        assert!(runtime_output.contains("\"staged_value_bytes\":0"));
        assert!(runtime_output.contains("\"snapshot_pin_count\":1"));
        assert!(runtime_output.contains("\"transaction_id\":\""));
        assert!(runtime_output.contains("\"age_us\":"));
        assert!(runtime_output.contains("\"fence_waits\":[]"));
        assert!(runtime_output.contains("\"fence_waits_truncated\":false"));
        assert!(runtime_output.contains("\"gateway_result_bytes_relayed_total\":"));
        let gateway_bytes = runtime_output
            .split_once("\"gateway_result_bytes_sent_to_clients_total\":")
            .and_then(|(_, suffix)| {
                suffix
                    .split(|character: char| !character.is_ascii_digit())
                    .next()
            })
            .and_then(|value| value.parse::<u64>().ok())
            .ok_or("runtime-list omitted the gateway client-write byte counter")?;
        assert!(
            gateway_bytes >= b"QUERY_READY".len() as u64,
            "gateway byte counter did not include the successfully delivered ready event"
        );
        assert!(runtime_output.contains("\"pending_submissions\":0"));
        assert!(runtime_output.contains("\"starting_submissions\":0"));
        assert!(runtime_output.contains("\"started_submissions\":1"));
        assert!(runtime_output.contains("\"queued_operational_starts\":0"));
        assert!(runtime_output.contains("\"queued_analytical_starts\":0"));
        assert!(runtime_output.contains("\"oldest_queued_start_wait_us\":0"));
        assert!(runtime_output.contains("\"operational_start_wait_buckets\":[0, 0, 0, 0, 0, 0]"));
        assert!(runtime_output.contains("\"analytical_start_wait_buckets\":[0, 0, 0, 0, 0, 0]"));
        assert!(runtime_output.contains("\"reserved_submission_bytes\":"));
        assert!(runtime_output.contains("\"pinned_snapshot_count\":1"));
        assert!(runtime_output.contains("\"oldest_snapshot_pin_age_us\":"));
        assert!(runtime_output.contains("\"invocation_policy_denials_total\":0"));
        assert!(runtime_output.contains("\"invocation_staged_value_bytes_total\":0"));
        assert!(runtime_output.contains("\"invocation_commit_latency_buckets\":[0,0,0,0,0,0]"));
        assert!(runtime_output.contains("\"invocation_redb_write_acquisition_buckets\":["));
        let runtime_counter = |name: &str| {
            let field = format!("\"{name}\":");
            runtime_output
                .split_once(&field)
                .and_then(|(_, value)| {
                    value
                        .split(|character: char| !character.is_ascii_digit())
                        .next()
                })
                .and_then(|value| value.parse::<u64>().ok())
                .unwrap_or_else(|| panic!("missing numeric runtime field {name}: {runtime_output}"))
        };
        assert!(runtime_counter("outcome_maintenance_passes_total") > 0);
        assert_eq!(runtime_counter("outcome_maintenance_failures_total"), 0);
        let _ = runtime_counter("outcomes_pruned_total");
        let audit_list = Command::new(env!("CARGO_BIN_EXE_prdb-client"))
            .args([
                "audit-list",
                "--address",
                &address.to_string(),
                "--credentials",
                runtime_list_credentials_path
                    .to_str()
                    .ok_or("credential path is not UTF-8")?,
                "--limit",
                "1",
            ])
            .output()?;
        assert!(
            audit_list.status.success(),
            "audit-list CLI failed: {}",
            String::from_utf8_lossy(&audit_list.stderr)
        );
        let audit_output = String::from_utf8(audit_list.stdout)?;
        assert!(audit_output.contains("\"events\":[{"));
        assert!(audit_output.contains("\"event_id\":\""));
        assert!(audit_output.contains("\"occurred_at_us\":"));
        assert!(audit_output.contains("\"action\":"));
        assert!(audit_output.contains("\"result\":"));
        assert!(audit_output.contains("\"target_ids\":["));
        assert!(audit_output.contains("\"next_cursor\":\""));
        let active_status = admin
            .status(invocation_id)
            .map_err(|error| format!("active STATUS failed: {error:?}"))?;
        assert_eq!(active_status, InvocationStatus::Active);
        let cancellation = admin
            .cancel_invocation(invocation_id)
            .map_err(|error| format!("active CANCEL failed: {error:?}"))?;
        assert_eq!(cancellation, CancellationResult::Accepted);
        let revoked = admin
            .revoke_token(query_token_id)
            .map_err(|error| format!("revoke original query token failed: {error:?}"))?;
        assert!(revoked.commit_sequence > 0);
        cancellation_requested_at = Instant::now();
        let _gateway_crash_status = gateway.kill()?;
    }
    drop(client);

    let (mut replacement_gateway, replacement_address) = start_gateway(&service_socket, uid)?;
    assert!(
        SubmissionClient::connect_authenticated(
            replacement_address,
            query_token_id,
            &mut restart_query_secret,
        )
        .is_err(),
        "a revoked token must not reconnect for status reconciliation"
    );
    let mut status_client = SubmissionClient::connect_authenticated(
        replacement_address,
        recovery_token_id,
        &mut restart_recovery_secret,
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
    let mut interactive_begin_client = SubmissionClient::connect_interactive_authenticated(
        replacement_address,
        recovery_token_id,
        &mut interactive_restart_secret,
    )?;
    let interactive_transaction_id =
        match interactive_begin_client.interactive_transaction_begin([0xe1; 16])? {
            prdb_engine::client_protocol::ClientV2Response::TransactionBegun {
                transaction_id,
                state: prdb_engine::client_protocol::ClientV2TransactionState::Active,
                ..
            } => transaction_id,
            response => return Err(format!("interactive BEGIN failed: {response:?}").into()),
        };
    let interactive_committed_id =
        match interactive_begin_client.interactive_transaction_begin([0xe2; 16])? {
            prdb_engine::client_protocol::ClientV2Response::TransactionBegun {
                transaction_id,
                state: prdb_engine::client_protocol::ClientV2TransactionState::Active,
                ..
            } => transaction_id,
            response => return Err(format!("second interactive BEGIN failed: {response:?}").into()),
        };
    assert!(matches!(
        interactive_begin_client.interactive_transaction_commit(interactive_committed_id)?,
        prdb_engine::client_protocol::ClientV2Response::Transaction {
            transaction_id,
            state: prdb_engine::client_protocol::ClientV2TransactionState::Committed,
        } if transaction_id == interactive_committed_id
    ));
    drop(interactive_begin_client);
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
        recovery_token_id,
        &mut durable_recovery_secret,
    )?;
    assert_eq!(
        recovered_status_client.status(invocation_id)?,
        InvocationStatus::Aborted,
        "ABORTED must survive closing and reopening the storage process"
    );
    let mut interactive_status_secret = *recovery_token.token_secret();
    let mut interactive_status_client = SubmissionClient::connect_interactive_authenticated(
        recovered_address,
        recovery_token_id,
        &mut interactive_status_secret,
    )?;
    let interactive_status_response =
        interactive_status_client.interactive_transaction_status(interactive_transaction_id)?;
    assert!(
        matches!(
            &interactive_status_response,
            prdb_engine::client_protocol::ClientV2Response::Transaction {
                transaction_id,
                state: prdb_engine::client_protocol::ClientV2TransactionState::Aborted,
            } if *transaction_id == interactive_transaction_id
        ),
        "an undecided interactive transaction must be aborted by supervisor restart: {interactive_status_response:?}"
    );
    assert!(
        matches!(
            interactive_status_client.interactive_transaction_status(interactive_committed_id)?,
            prdb_engine::client_protocol::ClientV2Response::Transaction {
                transaction_id,
                state: prdb_engine::client_protocol::ClientV2TransactionState::Committed,
            } if transaction_id == interactive_committed_id
        ),
        "a committed interactive transaction outcome must survive supervisor restart"
    );
    drop(interactive_status_client);
    drop(recovered_status_client);
    assert!(recovered_gateway.stop()?.success());
    assert!(restarted_supervisor.stop()?.success());
    Ok(())
}
