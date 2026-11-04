# Engine Transactions over redb

Implement admission-pinned redb read transactions, sequential statement access, bounded per-invocation write overlays, own-write visibility, and authorized reads. Add row/version tokens and final validation in one redb write transaction. Merge field patches with the latest current row, then atomically update rows, logical indexes, catalog changes, required audit and durable outcome.

Test repeatable reads, overlays invisible to other invocations, same-row conflict, disjoint-row concurrent staging, write skew, constraints racing, authority changes, cancellation before and after commit begins, redb commit ambiguity, restart and status reconciliation. Verify all transaction data and the corresponding outcome are one atomic commit. Limit held snapshots and measure retained-page pressure. Preserve true cell-level permissions and sanitized output before IPC.
