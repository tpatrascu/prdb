# Interactive Docker Compose Security Demo

This guide walks through a live, interactive demo of PRDB's **column**, **row**, and **cell** authorization policies using the `docker/compose.yaml` setup.

---

## 1. Prerequisites & Starting the Demo

The PRDB server requires a writable delegated **cgroup v2** parent with `cpu`, `memory`, and `pids` controllers enabled.

On a Linux host with `systemd`, start the compose stack inside a delegated user service:

```sh
systemd-run --user --wait --pipe --collect \
  --unit=prdb-compose-demo.service --slice=prdb-tests.slice \
  --property='Delegate=cpu memory pids' \
  --property="WorkingDirectory=$PWD" \
  "$PWD/engine/tools/with_delegated_cgroup.sh" \
  sh -c 'export PRDB_CGROUP_PARENT="$PRDB_TEST_CGROUP_PARENT"; \
    exec docker compose -f docker/compose.yaml up --build'
```

Once running, the `client` container automatically connects `prdb-client interactive-sql` to the server as `prdb_admin`. You can also open a second terminal to run `admin-sql` commands inside the `client` container:

```sh
docker compose -f docker/compose.yaml exec -T client /bin/bash
```

---

## 2. Setup: Schema, Users, and Sample Data

Run the following administrative DDL using `prdb-client admin-sql` (from your second terminal inside the `client` container):

```sh
cat <<'EOF' | prdb-client admin-sql \
  --address 127.0.0.1:8765 \
  --credentials /var/lib/prdb/admin.credentials \
  --sql-file -
CREATE SCHEMA IF NOT EXISTS app;

CREATE USER alice WITH LOGIN;
CREATE USER hr_mgr WITH LOGIN;
CREATE ROLE managers;
GRANT managers TO hr_mgr;

CREATE TABLE app.employees (
    id INT PRIMARY KEY,
    department_id INT NOT NULL,
    manager_id INT NOT NULL,
    name TEXT NOT NULL,
    salary INT
);
EOF
```

Next, insert sample employees in your `interactive-sql` REPL (or via `prdb-client interactive-sql`):

```sql
INSERT INTO app.employees (id, department_id, manager_id, name, salary) VALUES
  (1, 10, 100, 'Alice', 95000),
  (2, 10, 100, 'Bob', 88000),
  (3, 10, 200, 'Carol', 105000),
  (4, 20, 300, 'Dave', 92000);
```

---

## 3. Demo 1: Default-Deny & Column-Level Grants

Without explicit grants, `alice` cannot read any columns from `app.employees`.

Grant `alice` schema usage and **column-level** `SELECT` (omitting `salary`):

```sh
cat <<'EOF' | prdb-client admin-sql \
  --address 127.0.0.1:8765 \
  --credentials /var/lib/prdb/admin.credentials \
  --sql-file -
GRANT USAGE ON SCHEMA app TO alice;
GRANT SELECT (id, department_id, name) ON TABLE app.employees TO alice;
EOF
```

Now test in `interactive-sql`:
```sql
RUNAS alice;
SELECT id, department_id, name FROM app.employees;
-- Succeeds: returns id, department_id, name

SELECT salary FROM app.employees;
-- Fails: authorization error (no SELECT grant on salary)
```

---

## 4. Demo 2: Row-Level Policy (`CREATE ROW POLICY`)

Restrict `alice` so she can only see employees in `department_id = 10` (Dave in department `20` becomes invisible):

```sh
cat <<'EOF' | prdb-client admin-sql \
  --address 127.0.0.1:8765 \
  --credentials /var/lib/prdb/admin.credentials \
  --sql-file -
CREATE ROW POLICY dept10_only ON app.employees
  FOR SELECT TO alice
  USING (department_id = 10);
EOF
```

Query as `alice` in `interactive-sql`:
```sql
RUNAS alice;
SELECT id, department_id, name FROM app.employees;
-- Returns only Alice (1), Bob (2), and Carol (3).
-- Dave (4, department 20) is completely absent.
```

---

## 5. Demo 3: Cell-Level Policy (`CREATE CELL POLICY`)

Now configure `hr_mgr` (via role `managers`) so they can:
1. See all employees in `department_id = 10`.
2. Read `id`, `department_id`, `manager_id`, and `name` for all visible rows.
3. Read `salary` **only** for direct reports (`manager_id = 100`), while seeing **`REDACTED`** for Carol (`manager_id = 200`).

```sh
cat <<'EOF' | prdb-client admin-sql \
  --address 127.0.0.1:8765 \
  --credentials /var/lib/prdb/admin.credentials \
  --sql-file -
GRANT USAGE ON SCHEMA app TO ROLE managers;
GRANT SELECT (id, department_id, manager_id, name, salary)
  ON TABLE app.employees TO ROLE managers;

CREATE ROW POLICY mgr_dept10 ON app.employees
  FOR SELECT TO ROLE managers
  USING (department_id = 10);

CREATE CELL POLICY mgr_identity_read ON app.employees (id, department_id, manager_id, name)
  FOR SELECT TO ROLE managers
  USING (true);

CREATE CELL POLICY mgr_direct_salary_read ON app.employees (salary)
  FOR SELECT TO ROLE managers
  USING (manager_id = 100);
EOF
```

Query as `hr_mgr` in `interactive-sql`:
```sql
RUNAS hr_mgr;
SELECT id, name, manager_id, salary FROM app.employees;
-- Expected result:
--  1 | Alice | 100 | 95000
--  2 | Bob   | 100 | 88000
--  3 | Carol | 200 | REDACTED
```

---

## 6. Demo 4: Mutation Policy (`UPDATE` with Old & New Row Checks)

Grant `managers` permission to update `salary` only for direct reports (`manager_id = 100`):

```sh
cat <<'EOF' | prdb-client admin-sql \
  --address 127.0.0.1:8765 \
  --credentials /var/lib/prdb/admin.credentials \
  --sql-file -
GRANT UPDATE (salary) ON TABLE app.employees TO ROLE managers;

CREATE ROW POLICY mgr_update_row ON app.employees
  FOR UPDATE TO ROLE managers
  USING (manager_id = 100)
  WITH CHECK (manager_id = 100);

CREATE CELL POLICY mgr_update_salary ON app.employees (salary)
  FOR UPDATE TO ROLE managers
  USING (manager_id = 100)
  WITH CHECK (manager_id = 100);
EOF
```

Test in `interactive-sql`:
```sql
RUNAS hr_mgr;
UPDATE app.employees SET salary = 99000 WHERE id = 1;
-- Succeeds: Alice's manager_id is 100

UPDATE app.employees SET salary = 110000 WHERE id = 3;
-- Fails: Carol's manager_id is 200 (denied by row/cell UPDATE policy)
```
