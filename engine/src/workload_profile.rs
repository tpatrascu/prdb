//! Workload profile definitions, validation, binary encoding, and built-in presets.

use crate::linux_sandbox::WorkerResourceLimits;
use crate::query_abi::{
    MAX_IPC_REQUEST_BYTES, MAX_IPC_RESPONSE_BYTES, MAX_PARAMETER_BYTES, MAX_RESULT_CHUNK_BYTES,
    MAX_SQL_DATA_BYTES, MAX_SQL_INDEX_PROBE_CANDIDATES,
};

pub const WORKLOAD_PROFILE_LIMITS_VERSION: u32 = 1;
pub const WORKLOAD_PROFILE_PAYLOAD_BYTES: usize = 332;

pub const BUILTIN_SMALL_OPERATIONAL_CODE: u16 = 4;
pub const BUILTIN_LARGE_OPERATIONAL_CODE: u16 = 5;
pub const BUILTIN_SMALL_ANALYTICAL_CODE: u16 = 6;
pub const BUILTIN_LARGE_ANALYTICAL_CODE: u16 = 7;

pub const fn builtin_workload_profile_id(code: u16) -> [u8; 16] {
    let mut id = [0x80; 16];
    id[15] = code as u8;
    id
}

pub const SMALL_OPERATIONAL_PROFILE_ID: [u8; 16] =
    builtin_workload_profile_id(BUILTIN_SMALL_OPERATIONAL_CODE);
pub const LARGE_OPERATIONAL_PROFILE_ID: [u8; 16] =
    builtin_workload_profile_id(BUILTIN_LARGE_OPERATIONAL_CODE);
pub const SMALL_ANALYTICAL_PROFILE_ID: [u8; 16] =
    builtin_workload_profile_id(BUILTIN_SMALL_ANALYTICAL_CODE);
pub const LARGE_ANALYTICAL_PROFILE_ID: [u8; 16] =
    builtin_workload_profile_id(BUILTIN_LARGE_ANALYTICAL_CODE);

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum WorkloadProfileError {
    InvalidName,
    InvalidLimits(&'static str),
    InvalidEncoding,
}

impl std::fmt::Display for WorkloadProfileError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::InvalidName => formatter.write_str("invalid workload profile name"),
            Self::InvalidLimits(reason) => write!(formatter, "invalid workload profile: {reason}"),
            Self::InvalidEncoding => formatter.write_str("invalid workload profile encoding"),
        }
    }
}

impl std::error::Error for WorkloadProfileError {}

/// Complete set of configurable query, sandbox, staging, spill, SDK, and
/// transaction limits bound to a database user via `WORKLOAD PROFILE`.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct WorkloadProfileLimits {
    // 1. Per-invocation cgroup v2 ceilings
    pub cpu_quota_micros: u64,
    pub cpu_period_micros: u64,
    pub memory_max_bytes: u64,
    pub max_processes: u64,

    // 2. Query ABI & IPC transport limits
    pub max_parameter_bytes: u64,
    pub max_sql_data_bytes: u64,
    pub max_ipc_request_bytes: u64,
    pub max_ipc_response_bytes: u64,
    pub max_result_chunk_bytes: u64,
    pub max_total_result_bytes: u64,
    pub max_executor_requests: u32,
    pub max_planner_requests: u32,
    pub max_optimizer_requests: u32,

    // 3. Invocation workload budgets
    pub max_sql_index_probe_candidates: u32,
    pub max_authorized_index_range_bytes: u64,
    pub max_invocation_scan_work: u64,
    pub max_planner_estimate_requests: u32,
    pub max_planner_estimate_candidates: u32,
    pub max_deadline_ms: u64,

    // 4. Spill broker limits (all 0 = spill disabled)
    pub spill_max_bytes: u64,
    pub spill_max_io_bytes: u64,
    pub spill_max_streams: u32,
    pub spill_max_operations: u64,
    pub spill_max_response_bytes: u64,

    // 5. Supervisor staging & per-principal/profile admission limits
    pub max_artifact_bytes: u64,
    pub max_manifest_bytes: u32,
    pub max_scope_bytes: u32,
    pub max_chunk_bytes: u32,
    pub max_principal_reserved_bytes: u64,
    pub max_live_reservations_per_principal: u32,
    pub max_pending_per_principal: u32,
    pub max_active_per_principal: u32,
    pub max_active_for_profile: u32,
    pub reservation_ttl_ms: u64,

    // 6. C SDK / SQL library & executor-IPC query limits
    pub sql_max_query_bytes: u32,
    pub sql_max_projection_columns: u32,
    pub sql_max_predicate_nodes: u32,
    pub sql_max_expression_depth: u32,
    pub sql_max_group_keys: u32,
    pub sql_max_join_keys: u32,
    pub sql_max_order_keys: u32,
    pub sql_max_in_values: u32,
    pub sql_max_limit: u32,
    pub sql_batch_max_statements: u32,
    pub sql_dispatch_max_heap_bytes: u64,
    pub sql_exec_max_scan_pages: u32,
    pub sql_exec_max_materialized_bytes: u64,
    pub sql_exec_max_mutation_targets: u32,
    pub sql_in_mem_max_rows: u32,
    pub sql_in_mem_join_max_pairs: u32,
    pub sql_spill_sort_max_row_bytes: u32,
    pub sql_spill_sort_max_workspace_bytes: u32,
    pub sql_spill_join_max_record_bytes: u32,
    pub sql_spill_join_max_workspace_bytes: u32,
    pub optimizer_max_plan_bytes: u32,

    // 7. Transaction & write-set limits
    pub max_index_changes_per_row: u32,
    pub interactive_txn_max_packages: u32,
    pub interactive_txn_max_rows: u64,
    pub interactive_txn_max_write_bytes: u64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct WorkloadProfile {
    pub profile_id: [u8; 16],
    pub name: String,
    pub definition_epoch: u64,
    pub is_builtin: bool,
    pub limits: WorkloadProfileLimits,
}

impl WorkloadProfileLimits {
    pub const fn small_operational() -> Self {
        Self {
            cpu_quota_micros: 100_000,
            cpu_period_micros: 100_000,
            memory_max_bytes: 64 * 1024 * 1024,
            max_processes: 16,

            max_parameter_bytes: 256 * 1024,
            max_sql_data_bytes: 256 * 1024,
            max_ipc_request_bytes: 256 * 1024,
            max_ipc_response_bytes: 256 * 1024,
            max_result_chunk_bytes: MAX_RESULT_CHUNK_BYTES,
            max_total_result_bytes: 4 * 1024 * 1024,
            max_executor_requests: 512,
            max_planner_requests: 64,
            max_optimizer_requests: 64,

            max_sql_index_probe_candidates: 128,
            max_authorized_index_range_bytes: 1024 * 1024,
            max_invocation_scan_work: 16_384,
            max_planner_estimate_requests: 16,
            max_planner_estimate_candidates: 16_384,
            max_deadline_ms: 30_000,

            spill_max_bytes: 0,
            spill_max_io_bytes: 0,
            spill_max_streams: 0,
            spill_max_operations: 0,
            spill_max_response_bytes: 0,

            max_artifact_bytes: 16 * 1024 * 1024,
            max_manifest_bytes: 16 * 1024,
            max_scope_bytes: 8 * 1024,
            max_chunk_bytes: 256 * 1024,
            max_principal_reserved_bytes: 64 * 1024 * 1024,
            max_live_reservations_per_principal: 32,
            max_pending_per_principal: 32,
            max_active_per_principal: 16,
            max_active_for_profile: 64,
            reservation_ttl_ms: 60_000,

            sql_max_query_bytes: 16 * 1024,
            sql_max_projection_columns: 32,
            sql_max_predicate_nodes: 64,
            sql_max_expression_depth: 32,
            sql_max_group_keys: 4,
            sql_max_join_keys: 4,
            sql_max_order_keys: 4,
            sql_max_in_values: 32,
            sql_max_limit: 10_000,
            sql_batch_max_statements: 16,
            sql_dispatch_max_heap_bytes: 8 * 1024 * 1024,
            sql_exec_max_scan_pages: 128,
            sql_exec_max_materialized_bytes: 1024 * 1024,
            sql_exec_max_mutation_targets: 128,
            sql_in_mem_max_rows: 128,
            sql_in_mem_join_max_pairs: 16_384,
            sql_spill_sort_max_row_bytes: 0,
            sql_spill_sort_max_workspace_bytes: 0,
            sql_spill_join_max_record_bytes: 0,
            sql_spill_join_max_workspace_bytes: 0,
            optimizer_max_plan_bytes: 32 * 1024,

            max_index_changes_per_row: 256,
            interactive_txn_max_packages: 32,
            interactive_txn_max_rows: 4_096,
            interactive_txn_max_write_bytes: 4 * 1024 * 1024,
        }
    }

    pub const fn large_operational() -> Self {
        Self {
            cpu_quota_micros: 200_000,
            cpu_period_micros: 100_000,
            memory_max_bytes: 256 * 1024 * 1024,
            max_processes: 32,

            max_parameter_bytes: MAX_PARAMETER_BYTES,
            max_sql_data_bytes: MAX_SQL_DATA_BYTES,
            max_ipc_request_bytes: MAX_IPC_REQUEST_BYTES,
            max_ipc_response_bytes: MAX_IPC_RESPONSE_BYTES,
            max_result_chunk_bytes: MAX_RESULT_CHUNK_BYTES,
            max_total_result_bytes: 64 * 1024 * 1024,
            max_executor_requests: 4_096,
            max_planner_requests: 256,
            max_optimizer_requests: 256,

            max_sql_index_probe_candidates: 1_024,
            max_authorized_index_range_bytes: 16 * 1024 * 1024,
            max_invocation_scan_work: 131_072,
            max_planner_estimate_requests: 64,
            max_planner_estimate_candidates: 65_536,
            max_deadline_ms: 120_000,

            spill_max_bytes: 0,
            spill_max_io_bytes: 0,
            spill_max_streams: 0,
            spill_max_operations: 0,
            spill_max_response_bytes: 0,

            max_artifact_bytes: 64 * 1024 * 1024,
            max_manifest_bytes: 16 * 1024,
            max_scope_bytes: 8 * 1024,
            max_chunk_bytes: 256 * 1024,
            max_principal_reserved_bytes: 256 * 1024 * 1024,
            max_live_reservations_per_principal: 128,
            max_pending_per_principal: 128,
            max_active_per_principal: 32,
            max_active_for_profile: 32,
            reservation_ttl_ms: 300_000,

            sql_max_query_bytes: 64 * 1024,
            sql_max_projection_columns: 128,
            sql_max_predicate_nodes: 128,
            sql_max_expression_depth: 32,
            sql_max_group_keys: 8,
            sql_max_join_keys: 8,
            sql_max_order_keys: 9,
            sql_max_in_values: 128,
            sql_max_limit: 100_000,
            sql_batch_max_statements: 64,
            sql_dispatch_max_heap_bytes: 32 * 1024 * 1024,
            sql_exec_max_scan_pages: 1_024,
            sql_exec_max_materialized_bytes: 16 * 1024 * 1024,
            sql_exec_max_mutation_targets: 4_096,
            sql_in_mem_max_rows: 1_024,
            sql_in_mem_join_max_pairs: 65_536,
            sql_spill_sort_max_row_bytes: 0,
            sql_spill_sort_max_workspace_bytes: 0,
            sql_spill_join_max_record_bytes: 0,
            sql_spill_join_max_workspace_bytes: 0,
            optimizer_max_plan_bytes: 64 * 1024,

            max_index_changes_per_row: 256,
            interactive_txn_max_packages: 128,
            interactive_txn_max_rows: 65_536,
            interactive_txn_max_write_bytes: 64 * 1024 * 1024,
        }
    }

    pub const fn small_analytical() -> Self {
        Self {
            cpu_quota_micros: 400_000,
            cpu_period_micros: 100_000,
            memory_max_bytes: 512 * 1024 * 1024,
            max_processes: 32,

            max_parameter_bytes: MAX_PARAMETER_BYTES,
            max_sql_data_bytes: MAX_SQL_DATA_BYTES,
            max_ipc_request_bytes: MAX_IPC_REQUEST_BYTES,
            max_ipc_response_bytes: MAX_IPC_RESPONSE_BYTES,
            max_result_chunk_bytes: MAX_RESULT_CHUNK_BYTES,
            max_total_result_bytes: 256 * 1024 * 1024,
            max_executor_requests: 32_768,
            max_planner_requests: 512,
            max_optimizer_requests: 512,

            max_sql_index_probe_candidates: 8_192,
            max_authorized_index_range_bytes: 64 * 1024 * 1024,
            max_invocation_scan_work: 2_097_152,
            max_planner_estimate_requests: 256,
            max_planner_estimate_candidates: 262_144,
            max_deadline_ms: 900_000,

            spill_max_bytes: 2 * 1024 * 1024 * 1024,
            spill_max_io_bytes: 8 * 1024 * 1024 * 1024,
            spill_max_streams: 32,
            spill_max_operations: 65_536,
            spill_max_response_bytes: 1024 * 1024,

            max_artifact_bytes: 64 * 1024 * 1024,
            max_manifest_bytes: 16 * 1024,
            max_scope_bytes: 8 * 1024,
            max_chunk_bytes: 256 * 1024,
            max_principal_reserved_bytes: 256 * 1024 * 1024,
            max_live_reservations_per_principal: 16,
            max_pending_per_principal: 16,
            max_active_per_principal: 4,
            max_active_for_profile: 16,
            reservation_ttl_ms: 300_000,

            sql_max_query_bytes: 64 * 1024,
            sql_max_projection_columns: 128,
            sql_max_predicate_nodes: 256,
            sql_max_expression_depth: 48,
            sql_max_group_keys: 8,
            sql_max_join_keys: 8,
            sql_max_order_keys: 9,
            sql_max_in_values: 256,
            sql_max_limit: 1_000_000,
            sql_batch_max_statements: 64,
            sql_dispatch_max_heap_bytes: 64 * 1024 * 1024,
            sql_exec_max_scan_pages: 16_384,
            sql_exec_max_materialized_bytes: 64 * 1024 * 1024,
            sql_exec_max_mutation_targets: 4_096,
            sql_in_mem_max_rows: 4_096,
            sql_in_mem_join_max_pairs: 262_144,
            sql_spill_sort_max_row_bytes: 2_048,
            sql_spill_sort_max_workspace_bytes: 4 * 1024 * 1024,
            sql_spill_join_max_record_bytes: 8_192,
            sql_spill_join_max_workspace_bytes: 8 * 1024 * 1024,
            optimizer_max_plan_bytes: 128 * 1024,

            max_index_changes_per_row: 256,
            interactive_txn_max_packages: 64,
            interactive_txn_max_rows: 65_536,
            interactive_txn_max_write_bytes: 64 * 1024 * 1024,
        }
    }

    pub const fn large_analytical() -> Self {
        Self {
            cpu_quota_micros: 800_000,
            cpu_period_micros: 100_000,
            memory_max_bytes: 2 * 1024 * 1024 * 1024,
            max_processes: 64,

            max_parameter_bytes: MAX_PARAMETER_BYTES,
            max_sql_data_bytes: MAX_SQL_DATA_BYTES,
            max_ipc_request_bytes: MAX_IPC_REQUEST_BYTES,
            max_ipc_response_bytes: MAX_IPC_RESPONSE_BYTES,
            max_result_chunk_bytes: MAX_RESULT_CHUNK_BYTES,
            max_total_result_bytes: 2 * 1024 * 1024 * 1024,
            max_executor_requests: 262_144,
            max_planner_requests: 2_048,
            max_optimizer_requests: 2_048,

            max_sql_index_probe_candidates: MAX_SQL_INDEX_PROBE_CANDIDATES,
            max_authorized_index_range_bytes: 256 * 1024 * 1024,
            max_invocation_scan_work: 50_000_000,
            max_planner_estimate_requests: 1_024,
            max_planner_estimate_candidates: 1_048_576,
            max_deadline_ms: 3_600_000,

            spill_max_bytes: 32 * 1024 * 1024 * 1024,
            spill_max_io_bytes: 128 * 1024 * 1024 * 1024,
            spill_max_streams: 128,
            spill_max_operations: 1_048_576,
            spill_max_response_bytes: 1024 * 1024,

            max_artifact_bytes: 64 * 1024 * 1024,
            max_manifest_bytes: 16 * 1024,
            max_scope_bytes: 8 * 1024,
            max_chunk_bytes: 256 * 1024,
            max_principal_reserved_bytes: 512 * 1024 * 1024,
            max_live_reservations_per_principal: 8,
            max_pending_per_principal: 8,
            max_active_per_principal: 2,
            max_active_for_profile: 4,
            reservation_ttl_ms: 600_000,

            sql_max_query_bytes: 256 * 1024,
            sql_max_projection_columns: 256,
            sql_max_predicate_nodes: 512,
            sql_max_expression_depth: 64,
            sql_max_group_keys: 16,
            sql_max_join_keys: 16,
            sql_max_order_keys: 16,
            sql_max_in_values: 1_024,
            sql_max_limit: 10_000_000,
            sql_batch_max_statements: 64,
            sql_dispatch_max_heap_bytes: 256 * 1024 * 1024,
            sql_exec_max_scan_pages: 400_000,
            sql_exec_max_materialized_bytes: 256 * 1024 * 1024,
            sql_exec_max_mutation_targets: 16_384,
            sql_in_mem_max_rows: 16_384,
            sql_in_mem_join_max_pairs: 1_048_576,
            sql_spill_sort_max_row_bytes: 8_192,
            sql_spill_sort_max_workspace_bytes: 64 * 1024 * 1024,
            sql_spill_join_max_record_bytes: 16_384,
            sql_spill_join_max_workspace_bytes: 128 * 1024 * 1024,
            optimizer_max_plan_bytes: 256 * 1024,

            max_index_changes_per_row: 256,
            interactive_txn_max_packages: 64,
            interactive_txn_max_rows: 262_144,
            interactive_txn_max_write_bytes: 256 * 1024 * 1024,
        }
    }

    pub const fn spill_enabled(&self) -> bool {
        self.spill_max_bytes > 0
    }

    pub const fn worker_resource_limits(&self) -> WorkerResourceLimits {
        WorkerResourceLimits {
            cpu_quota_micros: self.cpu_quota_micros,
            cpu_period_micros: self.cpu_period_micros,
            memory_max_bytes: self.memory_max_bytes,
            max_processes: self.max_processes,
        }
    }

    pub fn validate(&self) -> Result<(), WorkloadProfileError> {
        if self.cpu_quota_micros == 0
            || self.cpu_period_micros == 0
            || self.memory_max_bytes == 0
            || self.max_processes == 0
        {
            return Err(WorkloadProfileError::InvalidLimits(
                "cgroup limits must be positive",
            ));
        }
        if self.max_parameter_bytes == 0 || self.max_parameter_bytes > MAX_PARAMETER_BYTES {
            return Err(WorkloadProfileError::InvalidLimits(
                "max_parameter_bytes must be within 1..=1 MiB",
            ));
        }
        if self.max_sql_data_bytes == 0 || self.max_sql_data_bytes > MAX_SQL_DATA_BYTES {
            return Err(WorkloadProfileError::InvalidLimits(
                "max_sql_data_bytes must be within 1..=1 MiB",
            ));
        }
        if self.max_ipc_request_bytes == 0 || self.max_ipc_request_bytes > MAX_IPC_REQUEST_BYTES {
            return Err(WorkloadProfileError::InvalidLimits(
                "max_ipc_request_bytes must be within 1..=1 MiB",
            ));
        }
        if self.max_ipc_response_bytes == 0 || self.max_ipc_response_bytes > MAX_IPC_RESPONSE_BYTES
        {
            return Err(WorkloadProfileError::InvalidLimits(
                "max_ipc_response_bytes must be within 1..=1 MiB",
            ));
        }
        if self.max_result_chunk_bytes == 0
            || self.max_result_chunk_bytes > MAX_RESULT_CHUNK_BYTES
            || self.max_total_result_bytes < self.max_result_chunk_bytes
        {
            return Err(WorkloadProfileError::InvalidLimits(
                "invalid result chunk or total result bytes",
            ));
        }
        if self.max_executor_requests == 0
            || self.max_planner_requests == 0
            || self.max_optimizer_requests == 0
        {
            return Err(WorkloadProfileError::InvalidLimits(
                "IPC request limits must be positive",
            ));
        }
        if self.max_sql_index_probe_candidates == 0
            || self.max_sql_index_probe_candidates > MAX_SQL_INDEX_PROBE_CANDIDATES
        {
            return Err(WorkloadProfileError::InvalidLimits(
                "max_sql_index_probe_candidates outside ABI envelope",
            ));
        }
        if self.max_authorized_index_range_bytes == 0
            || self.max_invocation_scan_work == 0
            || self.max_planner_estimate_requests == 0
            || self.max_planner_estimate_candidates == 0
            || self.max_deadline_ms == 0
        {
            return Err(WorkloadProfileError::InvalidLimits(
                "invocation workload budgets must be positive",
            ));
        }
        let spill_all_zero = self.spill_max_bytes == 0
            && self.spill_max_io_bytes == 0
            && self.spill_max_streams == 0
            && self.spill_max_operations == 0
            && self.spill_max_response_bytes == 0;
        let spill_all_positive = self.spill_max_bytes > 0
            && self.spill_max_io_bytes > 0
            && self.spill_max_streams > 0
            && self.spill_max_operations > 0
            && self.spill_max_response_bytes > 0;
        if !spill_all_zero && !spill_all_positive {
            return Err(WorkloadProfileError::InvalidLimits(
                "spill limits must be either all zero or all positive",
            ));
        }
        if self.max_artifact_bytes == 0
            || self.max_manifest_bytes == 0
            || self.max_scope_bytes == 0
            || self.max_chunk_bytes == 0
            || self.max_principal_reserved_bytes == 0
            || self.max_live_reservations_per_principal == 0
            || self.max_pending_per_principal == 0
            || self.max_active_per_principal == 0
            || self.max_active_for_profile == 0
            || self.reservation_ttl_ms == 0
        {
            return Err(WorkloadProfileError::InvalidLimits(
                "staging and admission limits must be positive",
            ));
        }
        if self.sql_max_query_bytes == 0
            || self.sql_max_projection_columns == 0
            || self.sql_max_predicate_nodes == 0
            || self.sql_max_expression_depth == 0
            || self.sql_max_group_keys == 0
            || self.sql_max_join_keys == 0
            || self.sql_max_order_keys == 0
            || self.sql_max_in_values == 0
            || self.sql_max_limit == 0
            || self.sql_batch_max_statements == 0
            || self.sql_dispatch_max_heap_bytes == 0
            || self.sql_exec_max_scan_pages == 0
            || self.sql_exec_max_materialized_bytes == 0
            || self.sql_exec_max_mutation_targets == 0
            || self.sql_in_mem_max_rows == 0
            || self.sql_in_mem_join_max_pairs == 0
            || self.optimizer_max_plan_bytes == 0
        {
            return Err(WorkloadProfileError::InvalidLimits(
                "SDK SQL limits must be positive",
            ));
        }
        if self.sql_dispatch_max_heap_bytes >= self.memory_max_bytes {
            return Err(WorkloadProfileError::InvalidLimits(
                "sql_dispatch_max_heap_bytes must be smaller than cgroup memory_max_bytes",
            ));
        }
        if self.max_index_changes_per_row == 0
            || self.interactive_txn_max_packages == 0
            || self.interactive_txn_max_rows == 0
            || self.interactive_txn_max_write_bytes == 0
        {
            return Err(WorkloadProfileError::InvalidLimits(
                "transaction limits must be positive",
            ));
        }
        Ok(())
    }

    pub fn encode(&self) -> Vec<u8> {
        let mut out = Vec::with_capacity(WORKLOAD_PROFILE_PAYLOAD_BYTES);
        let push_u32 = |buf: &mut Vec<u8>, v: u32| buf.extend_from_slice(&v.to_le_bytes());
        let push_u64 = |buf: &mut Vec<u8>, v: u64| buf.extend_from_slice(&v.to_le_bytes());

        push_u64(&mut out, self.cpu_quota_micros);
        push_u64(&mut out, self.cpu_period_micros);
        push_u64(&mut out, self.memory_max_bytes);
        push_u64(&mut out, self.max_processes);

        push_u64(&mut out, self.max_parameter_bytes);
        push_u64(&mut out, self.max_sql_data_bytes);
        push_u64(&mut out, self.max_ipc_request_bytes);
        push_u64(&mut out, self.max_ipc_response_bytes);
        push_u64(&mut out, self.max_result_chunk_bytes);
        push_u64(&mut out, self.max_total_result_bytes);
        push_u32(&mut out, self.max_executor_requests);
        push_u32(&mut out, self.max_planner_requests);
        push_u32(&mut out, self.max_optimizer_requests);

        push_u32(&mut out, self.max_sql_index_probe_candidates);
        push_u64(&mut out, self.max_authorized_index_range_bytes);
        push_u64(&mut out, self.max_invocation_scan_work);
        push_u32(&mut out, self.max_planner_estimate_requests);
        push_u32(&mut out, self.max_planner_estimate_candidates);
        push_u64(&mut out, self.max_deadline_ms);

        push_u64(&mut out, self.spill_max_bytes);
        push_u64(&mut out, self.spill_max_io_bytes);
        push_u32(&mut out, self.spill_max_streams);
        push_u64(&mut out, self.spill_max_operations);
        push_u64(&mut out, self.spill_max_response_bytes);

        push_u64(&mut out, self.max_artifact_bytes);
        push_u32(&mut out, self.max_manifest_bytes);
        push_u32(&mut out, self.max_scope_bytes);
        push_u32(&mut out, self.max_chunk_bytes);
        push_u64(&mut out, self.max_principal_reserved_bytes);
        push_u32(&mut out, self.max_live_reservations_per_principal);
        push_u32(&mut out, self.max_pending_per_principal);
        push_u32(&mut out, self.max_active_per_principal);
        push_u32(&mut out, self.max_active_for_profile);
        push_u64(&mut out, self.reservation_ttl_ms);

        push_u32(&mut out, self.sql_max_query_bytes);
        push_u32(&mut out, self.sql_max_projection_columns);
        push_u32(&mut out, self.sql_max_predicate_nodes);
        push_u32(&mut out, self.sql_max_expression_depth);
        push_u32(&mut out, self.sql_max_group_keys);
        push_u32(&mut out, self.sql_max_join_keys);
        push_u32(&mut out, self.sql_max_order_keys);
        push_u32(&mut out, self.sql_max_in_values);
        push_u32(&mut out, self.sql_max_limit);
        push_u32(&mut out, self.sql_batch_max_statements);
        push_u64(&mut out, self.sql_dispatch_max_heap_bytes);
        push_u32(&mut out, self.sql_exec_max_scan_pages);
        push_u64(&mut out, self.sql_exec_max_materialized_bytes);
        push_u32(&mut out, self.sql_exec_max_mutation_targets);
        push_u32(&mut out, self.sql_in_mem_max_rows);
        push_u32(&mut out, self.sql_in_mem_join_max_pairs);
        push_u32(&mut out, self.sql_spill_sort_max_row_bytes);
        push_u32(&mut out, self.sql_spill_sort_max_workspace_bytes);
        push_u32(&mut out, self.sql_spill_join_max_record_bytes);
        push_u32(&mut out, self.sql_spill_join_max_workspace_bytes);
        push_u32(&mut out, self.optimizer_max_plan_bytes);

        push_u32(&mut out, self.max_index_changes_per_row);
        push_u32(&mut out, self.interactive_txn_max_packages);
        push_u64(&mut out, self.interactive_txn_max_rows);
        push_u64(&mut out, self.interactive_txn_max_write_bytes);
        debug_assert_eq!(out.len(), WORKLOAD_PROFILE_PAYLOAD_BYTES);
        out
    }

    pub fn decode(bytes: &[u8]) -> Result<Self, WorkloadProfileError> {
        if bytes.len() != WORKLOAD_PROFILE_PAYLOAD_BYTES {
            return Err(WorkloadProfileError::InvalidEncoding);
        }
        let mut offset = 0usize;
        let read_u32 = |off: &mut usize| -> u32 {
            let v = u32::from_le_bytes(bytes[*off..*off + 4].try_into().unwrap());
            *off += 4;
            v
        };
        let read_u64 = |off: &mut usize| -> u64 {
            let v = u64::from_le_bytes(bytes[*off..*off + 8].try_into().unwrap());
            *off += 8;
            v
        };
        let limits = Self {
            cpu_quota_micros: read_u64(&mut offset),
            cpu_period_micros: read_u64(&mut offset),
            memory_max_bytes: read_u64(&mut offset),
            max_processes: read_u64(&mut offset),

            max_parameter_bytes: read_u64(&mut offset),
            max_sql_data_bytes: read_u64(&mut offset),
            max_ipc_request_bytes: read_u64(&mut offset),
            max_ipc_response_bytes: read_u64(&mut offset),
            max_result_chunk_bytes: read_u64(&mut offset),
            max_total_result_bytes: read_u64(&mut offset),
            max_executor_requests: read_u32(&mut offset),
            max_planner_requests: read_u32(&mut offset),
            max_optimizer_requests: read_u32(&mut offset),

            max_sql_index_probe_candidates: read_u32(&mut offset),
            max_authorized_index_range_bytes: read_u64(&mut offset),
            max_invocation_scan_work: read_u64(&mut offset),
            max_planner_estimate_requests: read_u32(&mut offset),
            max_planner_estimate_candidates: read_u32(&mut offset),
            max_deadline_ms: read_u64(&mut offset),

            spill_max_bytes: read_u64(&mut offset),
            spill_max_io_bytes: read_u64(&mut offset),
            spill_max_streams: read_u32(&mut offset),
            spill_max_operations: read_u64(&mut offset),
            spill_max_response_bytes: read_u64(&mut offset),

            max_artifact_bytes: read_u64(&mut offset),
            max_manifest_bytes: read_u32(&mut offset),
            max_scope_bytes: read_u32(&mut offset),
            max_chunk_bytes: read_u32(&mut offset),
            max_principal_reserved_bytes: read_u64(&mut offset),
            max_live_reservations_per_principal: read_u32(&mut offset),
            max_pending_per_principal: read_u32(&mut offset),
            max_active_per_principal: read_u32(&mut offset),
            max_active_for_profile: read_u32(&mut offset),
            reservation_ttl_ms: read_u64(&mut offset),

            sql_max_query_bytes: read_u32(&mut offset),
            sql_max_projection_columns: read_u32(&mut offset),
            sql_max_predicate_nodes: read_u32(&mut offset),
            sql_max_expression_depth: read_u32(&mut offset),
            sql_max_group_keys: read_u32(&mut offset),
            sql_max_join_keys: read_u32(&mut offset),
            sql_max_order_keys: read_u32(&mut offset),
            sql_max_in_values: read_u32(&mut offset),
            sql_max_limit: read_u32(&mut offset),
            sql_batch_max_statements: read_u32(&mut offset),
            sql_dispatch_max_heap_bytes: read_u64(&mut offset),
            sql_exec_max_scan_pages: read_u32(&mut offset),
            sql_exec_max_materialized_bytes: read_u64(&mut offset),
            sql_exec_max_mutation_targets: read_u32(&mut offset),
            sql_in_mem_max_rows: read_u32(&mut offset),
            sql_in_mem_join_max_pairs: read_u32(&mut offset),
            sql_spill_sort_max_row_bytes: read_u32(&mut offset),
            sql_spill_sort_max_workspace_bytes: read_u32(&mut offset),
            sql_spill_join_max_record_bytes: read_u32(&mut offset),
            sql_spill_join_max_workspace_bytes: read_u32(&mut offset),
            optimizer_max_plan_bytes: read_u32(&mut offset),

            max_index_changes_per_row: read_u32(&mut offset),
            interactive_txn_max_packages: read_u32(&mut offset),
            interactive_txn_max_rows: read_u64(&mut offset),
            interactive_txn_max_write_bytes: read_u64(&mut offset),
        };
        limits.validate()?;
        Ok(limits)
    }

    pub fn set_field(&mut self, name: &str, value: u64) -> Result<(), WorkloadProfileError> {
        let as_u32 = |v: u64| -> Result<u32, WorkloadProfileError> {
            u32::try_from(v)
                .map_err(|_| WorkloadProfileError::InvalidLimits("value exceeds u32 range"))
        };
        match name {
            "cpu_quota_micros" => self.cpu_quota_micros = value,
            "cpu_period_micros" => self.cpu_period_micros = value,
            "memory_max_bytes" => self.memory_max_bytes = value,
            "max_processes" => self.max_processes = value,
            "max_parameter_bytes" => self.max_parameter_bytes = value,
            "max_sql_data_bytes" => self.max_sql_data_bytes = value,
            "max_ipc_request_bytes" => self.max_ipc_request_bytes = value,
            "max_ipc_response_bytes" => self.max_ipc_response_bytes = value,
            "max_result_chunk_bytes" => self.max_result_chunk_bytes = value,
            "max_total_result_bytes" => self.max_total_result_bytes = value,
            "max_executor_requests" => self.max_executor_requests = as_u32(value)?,
            "max_planner_requests" => self.max_planner_requests = as_u32(value)?,
            "max_optimizer_requests" => self.max_optimizer_requests = as_u32(value)?,
            "max_sql_index_probe_candidates" => {
                self.max_sql_index_probe_candidates = as_u32(value)?
            }
            "max_authorized_index_range_bytes" => self.max_authorized_index_range_bytes = value,
            "max_invocation_scan_work" => self.max_invocation_scan_work = value,
            "max_planner_estimate_requests" => self.max_planner_estimate_requests = as_u32(value)?,
            "max_planner_estimate_candidates" => {
                self.max_planner_estimate_candidates = as_u32(value)?
            }
            "max_deadline_ms" => self.max_deadline_ms = value,
            "spill_max_bytes" => self.spill_max_bytes = value,
            "spill_max_io_bytes" => self.spill_max_io_bytes = value,
            "spill_max_streams" => self.spill_max_streams = as_u32(value)?,
            "spill_max_operations" => self.spill_max_operations = value,
            "spill_max_response_bytes" => self.spill_max_response_bytes = value,
            "max_artifact_bytes" => self.max_artifact_bytes = value,
            "max_manifest_bytes" => self.max_manifest_bytes = as_u32(value)?,
            "max_scope_bytes" => self.max_scope_bytes = as_u32(value)?,
            "max_chunk_bytes" => self.max_chunk_bytes = as_u32(value)?,
            "max_principal_reserved_bytes" => self.max_principal_reserved_bytes = value,
            "max_live_reservations_per_principal" => {
                self.max_live_reservations_per_principal = as_u32(value)?
            }
            "max_pending_per_principal" => self.max_pending_per_principal = as_u32(value)?,
            "max_active_per_principal" => self.max_active_per_principal = as_u32(value)?,
            "max_active_for_profile" => self.max_active_for_profile = as_u32(value)?,
            "reservation_ttl_ms" => self.reservation_ttl_ms = value,
            "sql_max_query_bytes" => self.sql_max_query_bytes = as_u32(value)?,
            "sql_max_projection_columns" => self.sql_max_projection_columns = as_u32(value)?,
            "sql_max_predicate_nodes" => self.sql_max_predicate_nodes = as_u32(value)?,
            "sql_max_expression_depth" => self.sql_max_expression_depth = as_u32(value)?,
            "sql_max_group_keys" => self.sql_max_group_keys = as_u32(value)?,
            "sql_max_join_keys" => self.sql_max_join_keys = as_u32(value)?,
            "sql_max_order_keys" => self.sql_max_order_keys = as_u32(value)?,
            "sql_max_in_values" => self.sql_max_in_values = as_u32(value)?,
            "sql_max_limit" => self.sql_max_limit = as_u32(value)?,
            "sql_batch_max_statements" => self.sql_batch_max_statements = as_u32(value)?,
            "sql_dispatch_max_heap_bytes" => self.sql_dispatch_max_heap_bytes = value,
            "sql_exec_max_scan_pages" => self.sql_exec_max_scan_pages = as_u32(value)?,
            "sql_exec_max_materialized_bytes" => self.sql_exec_max_materialized_bytes = value,
            "sql_exec_max_mutation_targets" => self.sql_exec_max_mutation_targets = as_u32(value)?,
            "sql_in_mem_max_rows" => self.sql_in_mem_max_rows = as_u32(value)?,
            "sql_in_mem_join_max_pairs" => self.sql_in_mem_join_max_pairs = as_u32(value)?,
            "sql_spill_sort_max_row_bytes" => self.sql_spill_sort_max_row_bytes = as_u32(value)?,
            "sql_spill_sort_max_workspace_bytes" => {
                self.sql_spill_sort_max_workspace_bytes = as_u32(value)?
            }
            "sql_spill_join_max_record_bytes" => {
                self.sql_spill_join_max_record_bytes = as_u32(value)?
            }
            "sql_spill_join_max_workspace_bytes" => {
                self.sql_spill_join_max_workspace_bytes = as_u32(value)?
            }
            "optimizer_max_plan_bytes" => self.optimizer_max_plan_bytes = as_u32(value)?,
            "max_index_changes_per_row" => self.max_index_changes_per_row = as_u32(value)?,
            "interactive_txn_max_packages" => self.interactive_txn_max_packages = as_u32(value)?,
            "interactive_txn_max_rows" => self.interactive_txn_max_rows = value,
            "interactive_txn_max_write_bytes" => self.interactive_txn_max_write_bytes = value,
            _ => {
                return Err(WorkloadProfileError::InvalidLimits(
                    "unknown profile parameter",
                ));
            }
        }
        Ok(())
    }
}

pub fn builtin_presets() -> [WorkloadProfile; 4] {
    [
        WorkloadProfile {
            profile_id: SMALL_OPERATIONAL_PROFILE_ID,
            name: "small_operational".to_owned(),
            definition_epoch: 1,
            is_builtin: true,
            limits: WorkloadProfileLimits::small_operational(),
        },
        WorkloadProfile {
            profile_id: LARGE_OPERATIONAL_PROFILE_ID,
            name: "large_operational".to_owned(),
            definition_epoch: 1,
            is_builtin: true,
            limits: WorkloadProfileLimits::large_operational(),
        },
        WorkloadProfile {
            profile_id: SMALL_ANALYTICAL_PROFILE_ID,
            name: "small_analytical".to_owned(),
            definition_epoch: 1,
            is_builtin: true,
            limits: WorkloadProfileLimits::small_analytical(),
        },
        WorkloadProfile {
            profile_id: LARGE_ANALYTICAL_PROFILE_ID,
            name: "large_analytical".to_owned(),
            definition_epoch: 1,
            is_builtin: true,
            limits: WorkloadProfileLimits::large_analytical(),
        },
    ]
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn builtin_presets_validate_and_roundtrip_wire_encoding() {
        let presets = builtin_presets();
        assert_eq!(presets.len(), 4);
        for preset in presets {
            preset
                .limits
                .validate()
                .unwrap_or_else(|err| panic!("preset {} invalid: {err:?}", preset.name));
            let encoded = preset.limits.encode();
            assert_eq!(encoded.len(), WORKLOAD_PROFILE_PAYLOAD_BYTES);
            let decoded = WorkloadProfileLimits::decode(&encoded).expect("decode preset");
            assert_eq!(decoded, preset.limits);
            let worker = preset.limits.worker_resource_limits();
            assert_eq!(worker.cpu_quota_micros, preset.limits.cpu_quota_micros);
            assert_eq!(worker.memory_max_bytes, preset.limits.memory_max_bytes);
        }
        assert!(!WorkloadProfileLimits::small_operational().spill_enabled());
        assert!(!WorkloadProfileLimits::large_operational().spill_enabled());
        assert!(WorkloadProfileLimits::small_analytical().spill_enabled());
        assert!(WorkloadProfileLimits::large_analytical().spill_enabled());
    }

    #[test]
    fn set_field_updates_limits_and_validation_rejects_zero_or_invalid() {
        let mut limits = WorkloadProfileLimits::small_operational();
        limits
            .set_field("memory_max_bytes", 1024 * 1024 * 1024)
            .unwrap();
        limits
            .set_field("max_sql_index_probe_candidates", 2048)
            .unwrap();
        limits.validate().unwrap();
        assert_eq!(limits.memory_max_bytes, 1024 * 1024 * 1024);
        assert_eq!(limits.max_sql_index_probe_candidates, 2048);

        assert!(limits.set_field("nonexistent_field", 1).is_err());

        limits.max_processes = 0;
        assert!(limits.validate().is_err());

        assert!(WorkloadProfileLimits::decode(b"short").is_err());
    }
}
