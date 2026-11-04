# Validation and Integration with redb

Use independent codec fixtures and the actual pinned redb implementation. Test atomic rows/indexes/catalog/audit/outcomes, old read snapshots during commits, private overlays, conflicts, policy validation, aborted writes, process crash/reopen, corruption errors, long readers and compaction. Test the selected durable mode and target filesystem; process termination alone does not prove power-loss guarantees.

End-to-end tests use the TCP gateway, supervisor, sandboxed TinyCC library, storage IPC and outcome status. Verify only storage owns redb, arbitrary executor output cannot forge terminal outcomes, and gateway failure cannot cause automatic replay. Benchmark point writes/reads under concurrent analytics and report version/profile.

No test adapter, upstream benchmark, or documentation-only check counts as real persistence evidence.
