#![cfg(all(unix, target_os = "linux", target_arch = "x86_64"))]

use prdb_engine::client_credentials::write_client_credentials;
use prdb_engine::client_protocol::ClientCredentials;
use prdb_engine::client_submit::AdminClient;
use prdb_engine::tuple::ScalarValue;
use std::os::unix::fs::PermissionsExt;

use super::helpers::*;

fn write_private_file(
    path: &std::path::Path,
    contents: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    std::fs::write(path, contents)?;
    std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o600))?;
    Ok(())
}

fn issue_user_credentials(
    instance: &InteractiveTestInstance,
    username: &str,
    cred_path: &std::path::Path,
) -> Result<(), Box<dyn std::error::Error>> {
    let mut creds =
        prdb_engine::client_credentials::read_client_credentials(&instance.admin_credentials_path)?;
    let mut admin = AdminClient::connect_authenticated(
        instance.address,
        creds.token_id,
        &mut creds.token_secret,
    )?;
    let created = admin.create_user(username)?;
    let issued = admin.issue_token(created.identity_id, "interactive-sql-e2e", i64::MAX)?;
    write_client_credentials(
        cred_path,
        &ClientCredentials {
            token_id: issued.token_id,
            token_secret: *issued.token_secret(),
        },
    )?;
    Ok(())
}

pub(super) fn interactive_sql_ddl_and_dml_lifecycle() -> Result<(), Box<dyn std::error::Error>> {
    let instance = InteractiveTestInstance::start("prdb-isql-ddl-dml", "isql_ddl_admin")?;
    let initial_scope = instance.write_scope_file("initial.scope", &["information_schema"])?;

    // 1. Create schema, roles, users, tables (all 11 scalar types, IDENTITY, DEFAULT, CHECK, PK, UNIQUE, FK),
    //    indexes, and alter statements via interactive-sql DDL routing.
    let ddl_statements = [
        "CREATE SCHEMA IF NOT EXISTS e2e_app;",
        "CREATE ROLE IF NOT EXISTS app_writers;",
        "CREATE USER IF NOT EXISTS writer_user LOGIN;",
        "GRANT app_writers TO USER writer_user;",
        "GRANT USAGE, CREATE ON SCHEMA e2e_app TO ROLE app_writers;",
        "GRANT USAGE ON SCHEMA e2e_app TO USER isql_ddl_admin;",
        "CREATE TABLE e2e_app.parents (\n\
            parent_id UUID GENERATED ALWAYS AS IDENTITY PRIMARY KEY,\n\
            code VARCHAR(20) NOT NULL UNIQUE,\n\
            label TEXT NOT NULL DEFAULT 'default_label',\n\
            CONSTRAINT chk_code_len CHECK (code <> '')\n\
        );",
        "GRANT REFERENCES, SELECT, INSERT, UPDATE, DELETE ON TABLE e2e_app.parents TO USER isql_ddl_admin;",
        "CREATE ROW POLICY p_parents_sel ON e2e_app.parents FOR SELECT TO USER isql_ddl_admin AS ALLOW USING (true);",
        "CREATE CELL POLICY c_parents_sel ON e2e_app.parents (parent_id, code, label) FOR SELECT TO USER isql_ddl_admin AS ALLOW USING (true);",
        "CREATE ROW POLICY p_parents_ins ON e2e_app.parents FOR INSERT TO USER isql_ddl_admin AS ALLOW WITH CHECK (true);",
        "CREATE CELL POLICY c_parents_ins ON e2e_app.parents (code, label) FOR INSERT TO USER isql_ddl_admin AS ALLOW WITH CHECK (true);",
        "CREATE ROW POLICY p_parents_upd ON e2e_app.parents FOR UPDATE TO USER isql_ddl_admin AS ALLOW USING (true) WITH CHECK (true);",
        "CREATE CELL POLICY c_parents_upd ON e2e_app.parents (code, label) FOR UPDATE TO USER isql_ddl_admin AS ALLOW USING (true) WITH CHECK (true);",
        "CREATE ROW POLICY p_parents_del ON e2e_app.parents FOR DELETE TO USER isql_ddl_admin AS ALLOW USING (true);",
        "CREATE TABLE e2e_app.all_types (\n\
            id INTEGER PRIMARY KEY,\n\
            flag BOOLEAN NOT NULL DEFAULT true,\n\
            tiny SMALLINT NOT NULL,\n\
            big BIGINT,\n\
            amount DECIMAL(10,2) NOT NULL,\n\
            note TEXT,\n\
            tag VARCHAR(40),\n\
            raw BYTEA,\n\
            parent_ref UUID REFERENCES e2e_app.parents (parent_id),\n\
            created_on DATE NOT NULL,\n\
            created_at TIMESTAMPTZ NOT NULL\n\
        );",
        "GRANT SELECT, INSERT, UPDATE, DELETE, ALTER, INDEX ON TABLE e2e_app.all_types TO USER isql_ddl_admin;",
        "CREATE INDEX idx_all_types_tiny ON e2e_app.all_types (tiny);",
        "ALTER INDEX e2e_app.idx_all_types_tiny RENAME TO idx_all_types_tiny_v2;",
        "ALTER TABLE e2e_app.all_types ADD COLUMN extra INTEGER;",
        "ALTER TABLE e2e_app.all_types ALTER COLUMN extra SET DEFAULT 9;",
        "ALTER TABLE e2e_app.all_types ALTER COLUMN extra DROP DEFAULT;",
        "ALTER TABLE e2e_app.all_types ALTER COLUMN extra SET NOT NULL;",
        "ALTER TABLE e2e_app.all_types ALTER COLUMN extra DROP NOT NULL;",
        "ALTER TABLE e2e_app.all_types RENAME COLUMN extra TO extra_val;",
        "ALTER TABLE e2e_app.all_types ADD CONSTRAINT chk_tiny_pos CHECK (tiny >= 0);",
        "ALTER TABLE e2e_app.all_types DROP CONSTRAINT chk_tiny_pos;",
        "ALTER TABLE e2e_app.all_types DROP COLUMN extra_val RESTRICT;",
    ];
    for stmt in ddl_statements {
        let out = instance.run_interactive_sql(
            "information_schema",
            &initial_scope,
            &instance.admin_user,
            &instance.admin_credentials_path,
            &[],
            None,
            InteractiveSqlInput::Stdin(stmt),
        )?;
        assert!(
            out.status.success(),
            "DDL setup failed on `{stmt}`: {}",
            String::from_utf8_lossy(&out.stderr)
        );
    }

    let app_scope = instance.write_scope_file("app.scope", &["e2e_app", "information_schema"])?;

    // 2. Autocommit INSERT (positional, explicit column list, multi-row, DEFAULT, NULL)
    //    followed by explicit BEGIN -> INSERT/UPDATE/SELECT -> ROLLBACK (private write discarded)
    //    and BEGIN -> INSERT/UPDATE/DELETE -> COMMIT (durable).
    let dml_script_path = instance.root.join("dml.sql");
    write_private_file(
        &dml_script_path,
        "\
        INSERT INTO parents (code, label) VALUES ('P1', DEFAULT), ('P2', 'custom');\n\
        SELECT code, label FROM parents ORDER BY code ASC;\n\
        BEGIN;\n\
        INSERT INTO parents (code, label) VALUES ('P_TMP', 'rolled_back');\n\
        UPDATE parents SET label = 'updated_tmp' WHERE code = 'P1';\n\
        SELECT code, label FROM parents ORDER BY code ASC;\n\
        ROLLBACK;\n\
        SELECT code, label FROM parents ORDER BY code ASC;\n\
        BEGIN;\n\
        INSERT INTO parents (code, label) VALUES ('P3', 'committed_p3');\n\
        UPDATE parents SET label = 'updated_p2' WHERE code = 'P2';\n\
        DELETE FROM parents WHERE code = 'P1';\n\
        COMMIT;\n\
        SELECT code, label FROM parents ORDER BY code ASC;\n\
        ",
    )?;
    let out = instance.run_interactive_sql(
        "e2e_app",
        &app_scope,
        &instance.admin_user,
        &instance.admin_credentials_path,
        &[],
        None,
        InteractiveSqlInput::File(&dml_script_path),
    )?;
    assert!(
        out.status.success(),
        "DML lifecycle failed: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    let rows = decode_sqlr_rows(&out.stdout)?;
    // Query 1: P1 (default_label), P2 (custom) -> 2 rows
    // Query 2 (in tx before rollback): P1 (updated_tmp), P2 (custom), P_TMP (rolled_back) -> 3 rows
    // Query 3 (after rollback): P1 (default_label), P2 (custom) -> 2 rows
    // Query 4 (after commit): P2 (updated_p2), P3 (committed_p3) -> 2 rows
    assert_eq!(rows.len(), 9, "unexpected total row count: {rows:?}");
    assert_eq!(
        rows[2],
        vec![
            (
                "code".to_owned(),
                Some(ScalarValue::Varchar("P1".to_owned()))
            ),
            (
                "label".to_owned(),
                Some(ScalarValue::Text("updated_tmp".to_owned()))
            ),
        ]
    );
    assert_eq!(
        rows[4],
        vec![
            (
                "code".to_owned(),
                Some(ScalarValue::Varchar("P_TMP".to_owned()))
            ),
            (
                "label".to_owned(),
                Some(ScalarValue::Text("rolled_back".to_owned()))
            ),
        ]
    );
    assert_eq!(
        rows[5],
        vec![
            (
                "code".to_owned(),
                Some(ScalarValue::Varchar("P1".to_owned()))
            ),
            (
                "label".to_owned(),
                Some(ScalarValue::Text("default_label".to_owned()))
            ),
        ]
    );
    assert_eq!(
        rows[7],
        vec![
            (
                "code".to_owned(),
                Some(ScalarValue::Varchar("P2".to_owned()))
            ),
            (
                "label".to_owned(),
                Some(ScalarValue::Text("updated_p2".to_owned()))
            ),
        ]
    );
    assert_eq!(
        rows[8],
        vec![
            (
                "code".to_owned(),
                Some(ScalarValue::Varchar("P3".to_owned()))
            ),
            (
                "label".to_owned(),
                Some(ScalarValue::Text("committed_p3".to_owned()))
            ),
        ]
    );

    // 3. TRUNCATE & DROP cleanup via DDL
    let cleanup_ddl = [
        "DROP ROW POLICY p_parents_sel ON e2e_app.parents RESTRICT;",
        "DROP CELL POLICY c_parents_sel ON e2e_app.parents RESTRICT;",
        "DROP ROW POLICY p_parents_ins ON e2e_app.parents RESTRICT;",
        "DROP CELL POLICY c_parents_ins ON e2e_app.parents RESTRICT;",
        "DROP ROW POLICY p_parents_upd ON e2e_app.parents RESTRICT;",
        "DROP CELL POLICY c_parents_upd ON e2e_app.parents RESTRICT;",
        "DROP ROW POLICY p_parents_del ON e2e_app.parents RESTRICT;",
        "DROP INDEX e2e_app.idx_all_types_tiny_v2 RESTRICT;",
        "DROP TABLE e2e_app.all_types RESTRICT;",
        "TRUNCATE TABLE e2e_app.parents RESTRICT;",
        "DROP TABLE e2e_app.parents RESTRICT;",
        "REVOKE USAGE, CREATE ON SCHEMA e2e_app FROM ROLE app_writers;",
        "REVOKE USAGE ON SCHEMA e2e_app FROM USER isql_ddl_admin;",
        "DROP SCHEMA e2e_app RESTRICT;",
        "REVOKE app_writers FROM USER writer_user;",
        "DROP ROLE app_writers RESTRICT;",
        "DROP USER writer_user RESTRICT;",
    ];
    for stmt in cleanup_ddl {
        let out = instance.run_interactive_sql(
            "information_schema",
            &initial_scope,
            &instance.admin_user,
            &instance.admin_credentials_path,
            &[],
            None,
            InteractiveSqlInput::Stdin(stmt),
        )?;
        assert!(
            out.status.success(),
            "DDL cleanup failed on `{stmt}`: {}",
            String::from_utf8_lossy(&out.stderr)
        );
    }
    Ok(())
}

pub(super) fn interactive_sql_queries_expressions_joins_aggregates_and_ctes()
-> Result<(), Box<dyn std::error::Error>> {
    let instance = InteractiveTestInstance::start("prdb-isql-queries", "isql_query_admin")?;
    let initial_scope = instance.write_scope_file("initial.scope", &["information_schema"])?;

    let ddl_statements = [
        "CREATE SCHEMA analytics;",
        "GRANT USAGE ON SCHEMA analytics TO USER isql_query_admin;",
        "CREATE TABLE analytics.depts (\n\
            dept_id INTEGER PRIMARY KEY,\n\
            dept_name VARCHAR(30) NOT NULL\n\
        );",
        "GRANT REFERENCES, SELECT, INSERT, UPDATE, DELETE ON TABLE analytics.depts TO USER isql_query_admin;",
        "CREATE ROW POLICY p_depts_sel ON analytics.depts FOR SELECT TO USER isql_query_admin AS ALLOW USING (true);",
        "CREATE CELL POLICY c_depts_sel ON analytics.depts (dept_id, dept_name) FOR SELECT TO USER isql_query_admin AS ALLOW USING (true);",
        "CREATE ROW POLICY p_depts_ins ON analytics.depts FOR INSERT TO USER isql_query_admin AS ALLOW WITH CHECK (true);",
        "CREATE CELL POLICY c_depts_ins ON analytics.depts (dept_id, dept_name) FOR INSERT TO USER isql_query_admin AS ALLOW WITH CHECK (true);",
        "CREATE TABLE analytics.items (\n\
            item_id INTEGER PRIMARY KEY,\n\
            dept_id INTEGER NOT NULL REFERENCES analytics.depts (dept_id),\n\
            title TEXT NOT NULL,\n\
            active BOOLEAN NOT NULL,\n\
            small_qty SMALLINT NOT NULL,\n\
            big_val BIGINT NOT NULL,\n\
            price DECIMAL(10,2),\n\
            code VARCHAR(20) NOT NULL,\n\
            payload BYTEA NOT NULL,\n\
            ref_uuid UUID NOT NULL,\n\
            due_date DATE NOT NULL,\n\
            updated_at TIMESTAMPTZ NOT NULL\n\
        );",
        "GRANT SELECT, INSERT, UPDATE, DELETE ON TABLE analytics.items TO USER isql_query_admin;",
        "CREATE ROW POLICY p_items_sel ON analytics.items FOR SELECT TO USER isql_query_admin AS ALLOW USING (true);",
        "CREATE CELL POLICY c_items_sel ON analytics.items (item_id, dept_id, title, active, small_qty, big_val, price, code, payload, ref_uuid, due_date, updated_at) FOR SELECT TO USER isql_query_admin AS ALLOW USING (true);",
        "CREATE ROW POLICY p_items_ins ON analytics.items FOR INSERT TO USER isql_query_admin AS ALLOW WITH CHECK (true);",
        "CREATE CELL POLICY c_items_ins ON analytics.items (item_id, dept_id, title, active, small_qty, big_val, price, code, payload, ref_uuid, due_date, updated_at) FOR INSERT TO USER isql_query_admin AS ALLOW WITH CHECK (true);",
    ];
    for stmt in ddl_statements {
        let out = instance.run_interactive_sql(
            "information_schema",
            &initial_scope,
            &instance.admin_user,
            &instance.admin_credentials_path,
            &[],
            None,
            InteractiveSqlInput::Stdin(stmt),
        )?;
        assert!(
            out.status.success(),
            "analytics DDL failed on `{stmt}`: {}",
            String::from_utf8_lossy(&out.stderr)
        );
    }

    let scope =
        instance.write_scope_file("analytics.scope", &["analytics", "information_schema"])?;

    // Parameters covering all 11 scalar types + NULL
    let params_path = instance.root.join("params.json");
    write_private_file(
        &params_path,
        r#"{
            "format": "PDB-SQL-PARAMETERS-1",
            "values": [
                {"type": "boolean", "value": "true"},
                {"type": "smallint", "value": "12"},
                {"type": "integer", "value": "101"},
                {"type": "bigint", "value": "9000000000"},
                {"type": "decimal(10,2)", "value": "25.50"},
                {"type": "text", "value": "Widget Alpha"},
                {"type": "varchar(20)", "value": "W-01"},
                {"type": "bytea", "value": "hex:deadbeef"},
                {"type": "uuid", "value": "00112233445566778899aabbccddeeff"},
                {"type": "date", "value": "20450"},
                {"type": "timestamptz", "value": "1767225600000000"},
                {"type": "decimal(10,2)", "value": null}
            ]
        }"#,
    )?;

    let seed_sql = "\
        BEGIN;\n\
        INSERT INTO depts (dept_id, dept_name) VALUES (1, 'Eng'), (2, 'Sales'), (3, 'Empty');\n\
        INSERT INTO items VALUES ($3, 1, $6, $1, $2, $4, $5, $7, $8, $9, $10, $11);\n\
        INSERT INTO items VALUES (102, 1, 'Widget Beta', false, 5, 200, 10.00, 'W-02', $8, $9, DATE '2026-01-15', TIMESTAMP '2026-01-15 00:00:00Z');\n\
        INSERT INTO items VALUES (103, 2, 'Gadget One', true, 5, 300, $12, 'G-01', $8, $9, DATE '2026-02-20', TIMESTAMP '2026-02-20 00:00:00Z');\n\
        INSERT INTO items VALUES (104, 2, 'Gadget Two', true, 8, 400, 40.00, 'G-02', $8, $9, DATE '2026-03-10', TIMESTAMP '2026-03-10 00:00:00Z');\n\
        COMMIT;\n\
    ";
    let out = instance.run_interactive_sql(
        "analytics",
        &scope,
        &instance.admin_user,
        &instance.admin_credentials_path,
        &[],
        Some(&params_path),
        InteractiveSqlInput::Stdin(seed_sql),
    )?;
    assert!(
        out.status.success(),
        "seed failed: {}",
        String::from_utf8_lossy(&out.stderr)
    );

    // Query 1: Parameterized all-11-types lookup + expressions + functions
    let expr_sql = "\
        SELECT DISTINCT\n\
            item_id,\n\
            small_qty + 3 AS qty_plus,\n\
            CASE WHEN price IS NULL THEN 0 ELSE 1 END AS has_price,\n\
            COALESCE(price, 0.00) AS eff_price,\n\
            SUBSTRING(title FROM 1 FOR 6) AS prefix,\n\
            EXTRACT(YEAR FROM due_date) AS yr\n\
        FROM items\n\
        WHERE active = $1\n\
          AND small_qty BETWEEN 5 AND 12\n\
          AND item_id IN (101, 103, 104)\n\
          AND title LIKE 'Widget%'\n\
          AND title NOT LIKE '%Zzz%'\n\
          AND payload = $8\n\
          AND ref_uuid = $9\n\
          AND due_date = $10\n\
          AND updated_at = $11\n\
        ORDER BY item_id ASC;\n\
    ";
    let out = instance.run_interactive_sql(
        "analytics",
        &scope,
        &instance.admin_user,
        &instance.admin_credentials_path,
        &[],
        Some(&params_path),
        InteractiveSqlInput::Stdin(expr_sql),
    )?;
    assert!(
        out.status.success(),
        "expr query failed: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    let expr_rows = decode_sqlr_rows(&out.stdout)?;
    assert_eq!(expr_rows.len(), 1);
    assert_eq!(expr_rows[0][0].1, Some(ScalarValue::Integer(101)));
    assert_eq!(expr_rows[0][1].1, Some(ScalarValue::BigInt(15)));
    assert_eq!(expr_rows[0][2].1, Some(ScalarValue::BigInt(1)));
    assert_eq!(expr_rows[0][3].1, Some(ScalarValue::Decimal(2550)));
    assert_eq!(
        expr_rows[0][4].1,
        Some(ScalarValue::Text("Widget".to_owned()))
    );
    assert_eq!(expr_rows[0][5].1, Some(ScalarValue::Integer(2025)));

    // Query 2: Joins (INNER, LEFT OUTER, CROSS, self-join), GROUP BY, aggregates, HAVING, ORDER BY NULLS LAST, LIMIT, OFFSET
    let join_agg_sql = "\
        SELECT d.dept_name, COUNT(*) AS cnt, COUNT(i.price) AS priced_cnt, COUNT(DISTINCT i.small_qty) AS distinct_qty, MIN(i.small_qty) AS min_q, MAX(i.small_qty) AS max_q, SUM(i.small_qty) AS sum_q, AVG(i.small_qty) AS avg_q\n\
        FROM depts AS d\n\
        LEFT OUTER JOIN items AS i ON d.dept_id = i.dept_id\n\
        GROUP BY d.dept_name\n\
        HAVING COUNT(*) >= 1\n\
        ORDER BY SUM(i.small_qty) DESC NULLS LAST, d.dept_name ASC\n\
        LIMIT 2 OFFSET 0;\n\
        SELECT a.item_id, b.item_id\n\
        FROM items AS a\n\
        INNER JOIN items AS b ON a.dept_id = b.dept_id AND a.item_id < b.item_id\n\
        ORDER BY a.item_id ASC;\n\
        SELECT d.dept_name, i.item_id\n\
        FROM depts AS d\n\
        CROSS JOIN items AS i\n\
        WHERE d.dept_id = 3 AND i.item_id = 101;\n\
    ";
    let out = instance.run_interactive_sql(
        "analytics",
        &scope,
        &instance.admin_user,
        &instance.admin_credentials_path,
        &[],
        None,
        InteractiveSqlInput::Stdin(join_agg_sql),
    )?;
    assert!(
        out.status.success(),
        "join/agg query failed: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    let join_rows = decode_sqlr_rows(&out.stdout)?;
    // 2 grouped rows (Eng: sum 17, Sales: sum 13) + 2 self-join pairs (101-102, 103-104) + 1 cross-join row
    assert_eq!(
        join_rows.len(),
        5,
        "unexpected join/agg rows: {join_rows:?}"
    );
    assert_eq!(
        join_rows[0][0].1,
        Some(ScalarValue::Varchar("Eng".to_owned()))
    );
    assert_eq!(join_rows[0][1].1, Some(ScalarValue::BigInt(2)));
    assert_eq!(join_rows[0][2].1, Some(ScalarValue::BigInt(2)));
    assert_eq!(join_rows[0][3].1, Some(ScalarValue::BigInt(2)));
    assert_eq!(join_rows[0][4].1, Some(ScalarValue::SmallInt(5)));
    assert_eq!(join_rows[0][5].1, Some(ScalarValue::SmallInt(12)));
    assert_eq!(join_rows[0][6].1, Some(ScalarValue::BigInt(17)));

    // Query 3: WITH CTE, derived table in FROM, EXISTS/NOT EXISTS, IN (SELECT ...), scalar subquery
    let cte_subq_sql = "\
        WITH active_items (id, did) AS (\n\
            SELECT item_id, dept_id FROM items WHERE active = true\n\
        )\n\
        SELECT sub.id, (SELECT d.dept_name FROM depts AS d WHERE d.dept_id = did) AS dname\n\
        FROM (SELECT id, did FROM active_items) AS sub\n\
        WHERE EXISTS (SELECT 1 FROM depts AS d WHERE d.dept_id = did AND d.dept_name = 'Eng')\n\
          AND sub.did IN (SELECT dept_id FROM depts WHERE dept_id < 3)\n\
          AND sub.id NOT IN (SELECT item_id FROM items WHERE active = false)\n\
        ORDER BY sub.id ASC;\n\
    ";
    let out = instance.run_interactive_sql(
        "analytics",
        &scope,
        &instance.admin_user,
        &instance.admin_credentials_path,
        &[],
        None,
        InteractiveSqlInput::Stdin(cte_subq_sql),
    )?;
    assert!(
        out.status.success(),
        "CTE/subquery failed: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    let cte_rows = decode_sqlr_rows(&out.stdout)?;
    assert_eq!(cte_rows.len(), 1);
    assert_eq!(cte_rows[0][0].1, Some(ScalarValue::Integer(101)));
    assert_eq!(
        cte_rows[0][1].1,
        Some(ScalarValue::Varchar("Eng".to_owned()))
    );

    // Query 4: Built-in information_schema
    let out = instance.run_interactive_sql(
        "information_schema",
        &scope,
        &instance.admin_user,
        &instance.admin_credentials_path,
        &[],
        None,
        InteractiveSqlInput::Stdin(
            "SELECT schema_name FROM schemata WHERE schema_name = 'analytics' LIMIT 1;",
        ),
    )?;
    assert!(
        out.status.success(),
        "information_schema query failed: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    let info_rows = decode_sqlr_rows(&out.stdout)?;
    assert_eq!(info_rows.len(), 1);
    assert_eq!(
        info_rows[0][0].1,
        Some(ScalarValue::Text("analytics".to_owned()))
    );

    Ok(())
}

pub(super) fn interactive_sql_runas_policies_and_error_boundaries()
-> Result<(), Box<dyn std::error::Error>> {
    let instance = InteractiveTestInstance::start("prdb-isql-runas", "isql_policy_admin")?;
    let initial_scope = instance.write_scope_file("initial.scope", &["information_schema"])?;

    let alice_cred = instance.root.join("alice.cred");
    issue_user_credentials(&instance, "reader_alice", &alice_cred)?;

    let setup_ddl = "\
        CREATE SCHEMA sec_app;\n\
        GRANT USAGE ON SCHEMA sec_app TO USER isql_policy_admin;\n\
        CREATE TABLE sec_app.secrets (\n\
            id INTEGER PRIMARY KEY,\n\
            owner_tag TEXT NOT NULL,\n\
            secret_note TEXT NOT NULL\n\
        );\n\
        GRANT USAGE ON SCHEMA sec_app TO USER reader_alice;\n\
        GRANT SELECT ON TABLE sec_app.secrets TO USER reader_alice;\n\
        GRANT SELECT, INSERT ON TABLE sec_app.secrets TO USER isql_policy_admin;\n\
        CREATE ROW POLICY admin_ins ON sec_app.secrets FOR INSERT TO USER isql_policy_admin AS ALLOW WITH CHECK (true);\n\
        CREATE CELL POLICY admin_ins_c ON sec_app.secrets (id, owner_tag, secret_note) FOR INSERT TO USER isql_policy_admin AS ALLOW WITH CHECK (true);\n\
        CREATE ROW POLICY admin_sel ON sec_app.secrets FOR SELECT TO USER isql_policy_admin AS ALLOW USING (true);\n\
        CREATE CELL POLICY admin_sel_c ON sec_app.secrets (id, owner_tag, secret_note) FOR SELECT TO USER isql_policy_admin AS ALLOW USING (true);\n\
        CREATE ROW POLICY alice_only_row ON sec_app.secrets FOR SELECT TO USER reader_alice AS ALLOW USING (owner_tag = 'alice');\n\
        CREATE CELL POLICY alice_sel_c ON sec_app.secrets (id, owner_tag) FOR SELECT TO USER reader_alice AS ALLOW USING (true);\n\
        CREATE CELL POLICY hide_note ON sec_app.secrets (secret_note) FOR SELECT TO USER reader_alice AS RESTRICT USING (true);\n\
    ";
    let out = instance.run_interactive_sql(
        "information_schema",
        &initial_scope,
        &instance.admin_user,
        &instance.admin_credentials_path,
        &[],
        None,
        InteractiveSqlInput::Stdin(setup_ddl),
    )?;
    assert!(
        out.status.success(),
        "policy DDL failed: {}",
        String::from_utf8_lossy(&out.stderr)
    );

    let scope = instance.write_scope_file("sec_app.scope", &["sec_app"])?;

    // 1. Seed rows as admin, then RUNAS reader_alice inside BEGIN ... COMMIT
    //    verifying row policy filters to owner_tag = 'alice' and non-redacted columns succeed.
    let info_probe = instance.run_interactive_sql(
        "information_schema",
        &initial_scope,
        &instance.admin_user,
        &instance.admin_credentials_path,
        &[],
        None,
        InteractiveSqlInput::Stdin("SELECT schema_name FROM schemata LIMIT 1;"),
    )?;
    assert!(
        info_probe.status.success(),
        "info_probe failed: {}",
        String::from_utf8_lossy(&info_probe.stderr)
    );
    assert!(
        !info_probe.stdout.is_empty(),
        "info_probe stdout was empty! stderr={}",
        String::from_utf8_lossy(&info_probe.stderr)
    );
    let probe_select = "SELECT id FROM secrets;";
    let probe_out = instance.run_interactive_sql(
        "sec_app",
        &scope,
        &instance.admin_user,
        &instance.admin_credentials_path,
        &[],
        None,
        InteractiveSqlInput::Stdin(probe_select),
    )?;
    assert!(
        probe_out.status.success(),
        "probe SELECT failed: {}",
        String::from_utf8_lossy(&probe_out.stderr)
    );
    let runas_sql = "\
        BEGIN;\n\
        INSERT INTO secrets VALUES (1, 'alice', 'top_secret_1'), (2, 'bob', 'top_secret_2');\n\
        COMMIT;\n\
        RUNAS reader_alice\n\
        SELECT id, owner_tag FROM secrets ORDER BY id ASC;\n\
        ENDRUNAS;\n\
    ";
    let out = instance.run_interactive_sql(
        "sec_app",
        &scope,
        &instance.admin_user,
        &instance.admin_credentials_path,
        &[("reader_alice", &alice_cred)],
        None,
        InteractiveSqlInput::Stdin(runas_sql),
    )?;
    assert!(
        out.status.success(),
        "RUNAS script failed: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    let rows = decode_sqlr_rows(&out.stdout)?;
    assert_eq!(
        rows.len(),
        1,
        "unexpected RUNAS row count: rows={rows:?}, stdout_len={}, stderr={}",
        out.stdout.len(),
        String::from_utf8_lossy(&out.stderr)
    );
    assert_eq!(rows[0][0].1, Some(ScalarValue::Integer(1)));
    assert_eq!(rows[0][1].1, Some(ScalarValue::Text("alice".to_owned())));

    // 2. Negative: DDL inside BEGIN is rejected before dispatch while transaction remains active
    let ddl_in_tx = "BEGIN;\nCREATE TABLE sec_app.nope (id INTEGER);\nROLLBACK;\n";
    let out = instance.run_interactive_sql(
        "sec_app",
        &scope,
        &instance.admin_user,
        &instance.admin_credentials_path,
        &[],
        None,
        InteractiveSqlInput::Stdin(ddl_in_tx),
    )?;
    assert!(!out.status.success());
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(
        stderr.contains("DDL") || stderr.contains("transaction"),
        "unexpected stderr for DDL in BEGIN: {stderr}"
    );

    // 3. Negative: DDL or BEGIN inside RUNAS is rejected before dispatch
    let bad_runas = "RUNAS reader_alice\nBEGIN\nENDRUNAS;\n";
    let out = instance.run_interactive_sql(
        "sec_app",
        &scope,
        &instance.admin_user,
        &instance.admin_credentials_path,
        &[("reader_alice", &alice_cred)],
        None,
        InteractiveSqlInput::Stdin(bad_runas),
    )?;
    assert!(!out.status.success());

    // 4. Negative: Statement failure inside BEGIN marks transaction abort-only
    let abort_only_sql = "BEGIN;\nSELECT nonexistent_col FROM secrets;\nCOMMIT;\n";
    let out = instance.run_interactive_sql(
        "sec_app",
        &scope,
        &instance.admin_user,
        &instance.admin_credentials_path,
        &[],
        None,
        InteractiveSqlInput::Stdin(abort_only_sql),
    )?;
    assert!(!out.status.success());

    // 5. Negative: Unknown RUNAS profile fails closed
    let missing_profile = "RUNAS unknown_user\nSELECT id FROM secrets;\nENDRUNAS;\n";
    let out = instance.run_interactive_sql(
        "sec_app",
        &scope,
        &instance.admin_user,
        &instance.admin_credentials_path,
        &[],
        None,
        InteractiveSqlInput::Stdin(missing_profile),
    )?;
    assert!(!out.status.success());

    Ok(())
}
