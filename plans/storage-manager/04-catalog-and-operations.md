# Catalog and Operations over redb

Implement the logical catalog and secondary indexes as versioned redb tables. Build bootstrap and integrity validation, users/roles, DDL fences, constraints, grants, mandatory row/cell policies, metadata filtering, audit/outcomes and scoped statistics above the engine tables.

Use redb snapshots for old readers and atomic redb writes for DDL publication. Revalidate schema and authority epochs at final write commit. Do not build independent catalog roots, checkpoint manifests, allocation records, page reclamation or WAL. Evaluate the upstream-supported compaction/retention mechanisms and keep long readers bounded. The redb database file remains inaccessible to all other processes.
