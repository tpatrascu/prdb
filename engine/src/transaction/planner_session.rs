use super::estimates::*;
use super::*;
use crate::authorization::AuthorizedProjection;
use crate::metadata_discovery::{
    AuthorizedPlanRelation, AuthorizedRelationDescription, MAX_RELATION_HANDLES,
    PLANNING_TABLE_HANDLE_TOKEN_KIND, PlanningTableHandle,
};
use crate::statistics::{
    AuthorizedScanEstimate, MAX_CONFIGURABLE_ESTIMATE_CANDIDATES, MAX_JOIN_ESTIMATE_ROWS,
};
use std::collections::{HashMap, HashSet};

/// Planner metadata session with opaque, connection-local table handles.
/// Exact predicate, population, join-cardinality, and column estimates may be
/// reused while the invocation and its private-write generation remain
/// unchanged.
pub struct PlannerMetadataConnection<'connection, 'db> {
    pub(super) transaction: &'connection mut InvocationTransaction<'db>,
    pub(super) handles: HashMap<[u8; 16], PlannerTableHandleState>,
    pub(super) used_tokens: HashSet<[u8; 16]>,
    pub(super) estimate_requests: usize,
    pub(super) predicate_estimates: HashMap<PredicateEstimateCacheKey, (u64, (u64, u64))>,
    pub(super) population_estimates: HashMap<PopulationEstimateCacheKey, (u64, u64)>,
    pub(super) join_estimates: HashMap<JoinEstimateCacheKey, (u64, u64)>,
    pub(super) summary_estimates:
        HashMap<SummaryEstimateCacheKey, (u64, crate::statistics::AuthorizedScanSummary)>,
}

#[derive(Default)]
pub(crate) struct PlannerMetadataSessionState {
    pub(super) handles: HashMap<[u8; 16], PlannerTableHandleState>,
    pub(super) used_tokens: HashSet<[u8; 16]>,
    pub(super) estimate_requests: usize,
    pub(super) predicate_estimates: HashMap<PredicateEstimateCacheKey, (u64, (u64, u64))>,
    pub(super) population_estimates: HashMap<PopulationEstimateCacheKey, (u64, u64)>,
    pub(super) join_estimates: HashMap<JoinEstimateCacheKey, (u64, u64)>,
    pub(super) summary_estimates:
        HashMap<SummaryEstimateCacheKey, (u64, crate::statistics::AuthorizedScanSummary)>,
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub(super) struct PredicateEstimateCacheKey {
    relation_id: [u8; 16],
    schema_id: [u8; 16],
    schema_version: u64,
    schema_name: String,
    relation_name: String,
    predicate_bytes: Vec<u8>,
    maximum_rows: u64,
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub(super) struct PopulationEstimateCacheKey {
    pub(super) relation_id: [u8; 16],
    pub(super) schema_id: [u8; 16],
    pub(super) schema_version: u64,
    pub(super) maximum_rows: u64,
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub(super) struct JoinEstimateCacheKey {
    left_relation_id: [u8; 16],
    right_relation_id: [u8; 16],
    schema_id: [u8; 16],
    schema_version: u64,
    left_key_column_ids: Vec<[u8; 16]>,
    right_key_column_ids: Vec<[u8; 16]>,
    maximum_rows: u64,
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub(super) struct SummaryEstimateCacheKey {
    pub(super) relation_id: [u8; 16],
    pub(super) schema_id: [u8; 16],
    pub(super) schema_version: u64,
    pub(super) column_ids: Vec<[u8; 16]>,
}

#[derive(Clone)]
pub(super) struct PlannerTableHandleState {
    pub(super) schema_name: String,
    pub(super) relation_name: String,
    pub(super) relation_id: [u8; 16],
}

impl PlannerMetadataConnection<'_, '_> {
    pub(crate) fn detach(self) -> PlannerMetadataSessionState {
        PlannerMetadataSessionState {
            handles: self.handles,
            used_tokens: self.used_tokens,
            estimate_requests: self.estimate_requests,
            predicate_estimates: self.predicate_estimates,
            population_estimates: self.population_estimates,
            join_estimates: self.join_estimates,
            summary_estimates: self.summary_estimates,
        }
    }

    /// Resolve an authorized relation name and issue a planner-only handle.
    pub fn resolve_relation(
        &mut self,
        schema_name: &str,
        relation_name: &str,
    ) -> Result<Option<(PlanningTableHandle, AuthorizedRelationDescription)>, TransactionError>
    {
        let Some(description) = self
            .transaction
            .resolve_catalog_authorized_relation(schema_name, relation_name)?
        else {
            return Ok(None);
        };
        if self.used_tokens.len() >= MAX_RELATION_HANDLES {
            return Err(TransactionError::RelationHandleLimitExceeded);
        }
        let token =
            new_metadata_cursor_token(&mut self.used_tokens, PLANNING_TABLE_HANDLE_TOKEN_KIND)?;
        self.handles.insert(
            token,
            PlannerTableHandleState {
                schema_name: schema_name.to_owned(),
                relation_name: relation_name.to_owned(),
                relation_id: description.relation_id,
            },
        );
        Ok(Some((PlanningTableHandle { token }, description)))
    }

    /// Resolve an authorized planner relation together with pinned schema and
    /// table identity needed to bind a stable logical plan.
    pub fn resolve_relation_for_plan(
        &mut self,
        schema_name: &str,
        relation_name: &str,
    ) -> Result<Option<(PlanningTableHandle, AuthorizedPlanRelation)>, TransactionError> {
        let Some((handle, relation)) = self.resolve_relation(schema_name, relation_name)? else {
            return Ok(None);
        };
        let identity = attach_plan_identity(self.transaction, schema_name, relation)?;
        Ok(Some((handle, identity)))
    }

    /// Resolve an authorized relation and stable plan identity without
    /// creating a connection-local planning handle. This is the metadata
    /// surface used by a separately isolated optimizer process.
    pub fn describe_relation_for_plan(
        &mut self,
        schema_name: &str,
        relation_name: &str,
    ) -> Result<Option<AuthorizedPlanRelation>, TransactionError> {
        let Some(relation) = self
            .transaction
            .resolve_catalog_authorized_relation(schema_name, relation_name)?
        else {
            return Ok(None);
        };
        attach_plan_identity(self.transaction, schema_name, relation).map(Some)
    }

    /// Prove that a resolved relation has no rows in this invocation's
    /// authorized view using only constant SELECT policies. The proof does
    /// not inspect table rows and shares the planner's estimate allowance.
    pub fn prove_empty_population_for_plan(
        &mut self,
        table_id: [u8; 16],
        visible_column_ids: &[[u8; 16]],
    ) -> Result<bool, TransactionError> {
        self.transaction.check_deadline()?;
        if table_id == [0; 16]
            || visible_column_ids.is_empty()
            || visible_column_ids.len() > crate::tuple::MAX_COLUMNS
            || visible_column_ids.contains(&[0; 16])
            || visible_column_ids
                .iter()
                .copied()
                .collect::<HashSet<_>>()
                .len()
                != visible_column_ids.len()
        {
            return Ok(false);
        }
        if !consume_planner_estimate_budget(
            &mut self.estimate_requests,
            self.transaction.coordination.max_planner_estimate_requests,
        ) {
            return Ok(false);
        }
        let state = match self.transaction.load_catalog_estimate_projection(table_id) {
            Ok(state) => state,
            Err(error) if estimate_error_is_terminal(&error) => return Err(error),
            Err(_) => return Ok(false),
        };
        let ordinals = match visible_column_ids
            .iter()
            .map(|column_id| {
                state
                    .schema
                    .column_ids
                    .iter()
                    .position(|candidate| candidate == column_id)
                    .ok_or(TransactionError::InvalidMetadataRequest)
            })
            .collect::<Result<Vec<_>, _>>()
        {
            Ok(ordinals) => ordinals,
            Err(_) => return Ok(false),
        };
        let columns = ordinals
            .iter()
            .map(|ordinal| state.schema.columns[*ordinal].clone())
            .collect::<Vec<_>>();
        let selected_column_ids = ordinals
            .iter()
            .map(|ordinal| state.schema.column_ids[*ordinal])
            .collect::<Vec<_>>();
        let projection = AuthorizedProjection {
            columns: &columns,
            column_ids: &selected_column_ids,
            schema_version: state.schema.schema_version,
            schema_id: state.schema.schema_id,
            context: &state.context,
            grants: &state.grants,
            policies: &state.policies,
            checks: &state.checks,
            policy_driving_columns: &state.policy_driving_columns,
            dependencies: &[],
        };
        Ok(matches!(
            estimate_population(table_id, &projection),
            Some(EstimatePopulation::NoRows)
        ))
    }

    /// Revalidate the qualified name before returning its authorized schema.
    pub fn describe_relation(
        &mut self,
        handle: &PlanningTableHandle,
    ) -> Result<AuthorizedRelationDescription, TransactionError> {
        let state = self
            .handles
            .get(&handle.token)
            .cloned()
            .ok_or(TransactionError::InvalidMetadataRequest)?;
        let description = self
            .transaction
            .resolve_catalog_authorized_relation(&state.schema_name, &state.relation_name)?
            .ok_or(TransactionError::InvalidMetadataRequest)?;
        if description.relation_id != state.relation_id || description.name != state.relation_name {
            return Err(TransactionError::InvalidMetadataRequest);
        }
        Ok(description)
    }

    /// Produce an authorized-view summary for selected stable column IDs.
    /// Empty selection requests every declared column. The planner receives
    /// only counts and cannot use this handle for executor row access.
    pub fn estimate_scan(
        &mut self,
        handle: &PlanningTableHandle,
        column_ids: &[[u8; 16]],
    ) -> Result<AuthorizedScanEstimate, TransactionError> {
        let description = self.describe_relation(handle)?;
        if self
            .handles
            .get(&handle.token)
            .is_some_and(|state| state.schema_name == "information_schema")
        {
            return Ok(AuthorizedScanEstimate::Unknown);
        }
        if !consume_planner_estimate_budget(
            &mut self.estimate_requests,
            self.transaction.coordination.max_planner_estimate_requests,
        ) {
            return Ok(AuthorizedScanEstimate::Unknown);
        }
        let state = self
            .transaction
            .load_catalog_estimate_projection(description.relation_id)?;
        let ordinals = if column_ids.is_empty() {
            (0..state.schema.columns.len()).collect::<Vec<_>>()
        } else {
            if column_ids.iter().copied().collect::<HashSet<_>>().len() != column_ids.len() {
                return Err(TransactionError::InvalidMetadataRequest);
            }
            column_ids
                .iter()
                .map(|column_id| {
                    state
                        .schema
                        .column_ids
                        .iter()
                        .position(|candidate| candidate == column_id)
                        .ok_or(TransactionError::InvalidMetadataRequest)
                })
                .collect::<Result<Vec<_>, _>>()?
        };
        if ordinals.is_empty() {
            return Ok(AuthorizedScanEstimate::Unknown);
        }
        let columns = ordinals
            .iter()
            .map(|ordinal| state.schema.columns[*ordinal].clone())
            .collect::<Vec<_>>();
        let selected_column_ids = ordinals
            .iter()
            .map(|ordinal| state.schema.column_ids[*ordinal])
            .collect::<Vec<_>>();
        let cache_key = SummaryEstimateCacheKey {
            relation_id: description.relation_id,
            schema_id: state.schema.schema_id,
            schema_version: state.schema.schema_version,
            column_ids: selected_column_ids.clone(),
        };
        let write_generation = self.transaction.overlay.generation();
        self.summary_estimates
            .retain(|_, (generation, _)| *generation == write_generation);
        if let Some((_, summary)) = self.summary_estimates.get(&cache_key) {
            self.transaction.check_deadline()?;
            return Ok(AuthorizedScanEstimate::Exact(summary.clone()));
        }
        let projection = AuthorizedProjection {
            columns: &columns,
            column_ids: &selected_column_ids,
            schema_version: state.schema.schema_version,
            schema_id: state.schema.schema_id,
            context: &state.context,
            grants: &state.grants,
            policies: &state.policies,
            checks: &state.checks,
            policy_driving_columns: &state.policy_driving_columns,
            dependencies: &[],
        };
        self.transaction.check_deadline()?;
        let candidate_limit = self
            .transaction
            .coordination
            .max_planner_estimate_candidates;
        let transaction = &mut *self.transaction;
        let exact_summary = cached_exact_estimate(
            &mut self.summary_estimates,
            cache_key,
            write_generation,
            || {
                Ok::<Option<crate::statistics::AuthorizedScanSummary>, TransactionError>(
                    match transaction.estimate_authorized_scan_with_limit(
                        description.relation_id,
                        &projection,
                        candidate_limit,
                    )? {
                        AuthorizedScanEstimate::Exact(summary) => Some(summary),
                        AuthorizedScanEstimate::Unknown => None,
                    },
                )
            },
        )?;
        Ok(exact_summary.map_or(
            AuthorizedScanEstimate::Unknown,
            AuthorizedScanEstimate::Exact,
        ))
    }

    /// Probe whether a table's authorized view is at most a small row bound.
    /// Unlike a full column summary, this stops after the bound plus one row.
    pub fn estimate_population_at_most(
        &mut self,
        handle: &PlanningTableHandle,
        maximum_rows: u64,
    ) -> Result<Option<u64>, TransactionError> {
        let maximum_rows = maximum_rows.min(
            self.transaction
                .coordination
                .max_planner_estimate_candidates as u64,
        );
        let description = self.describe_relation(handle)?;
        if self
            .handles
            .get(&handle.token)
            .is_some_and(|state| state.schema_name == "information_schema")
        {
            return Ok(None);
        }
        if !consume_planner_estimate_budget(
            &mut self.estimate_requests,
            self.transaction.coordination.max_planner_estimate_requests,
        ) {
            return Ok(None);
        }
        let state = self
            .transaction
            .load_catalog_estimate_projection(description.relation_id)?;
        let cache_key = PopulationEstimateCacheKey {
            relation_id: description.relation_id,
            schema_id: state.schema.schema_id,
            schema_version: state.schema.schema_version,
            maximum_rows,
        };
        let write_generation = self.transaction.overlay.generation();
        let projection = AuthorizedProjection {
            columns: &state.schema.columns,
            column_ids: &state.schema.column_ids,
            schema_version: state.schema.schema_version,
            schema_id: state.schema.schema_id,
            context: &state.context,
            grants: &state.grants,
            policies: &state.policies,
            checks: &state.checks,
            policy_driving_columns: &state.policy_driving_columns,
            dependencies: &[],
        };
        self.transaction.check_deadline()?;
        let transaction = &mut *self.transaction;
        cached_exact_estimate(
            &mut self.population_estimates,
            cache_key,
            write_generation,
            || {
                transaction.estimate_authorized_population_at_most(
                    description.relation_id,
                    &projection,
                    maximum_rows,
                )
            },
        )
    }

    /// Estimate the exact bounded INNER JOIN fanout after re-resolving both
    /// named relations in this invocation's pinned authorization context.
    pub fn estimate_join_cardinality_for_plan(
        &mut self,
        schema_name: &str,
        left_relation_name: &str,
        right_relation_name: &str,
        left_key_column_ids: &[[u8; 16]],
        right_key_column_ids: &[[u8; 16]],
        maximum_rows: u64,
    ) -> Result<Option<u64>, TransactionError> {
        self.transaction.check_deadline()?;
        if schema_name == "information_schema"
            || maximum_rows > MAX_JOIN_ESTIMATE_ROWS
            || left_key_column_ids.is_empty()
            || left_key_column_ids.len() > 8
            || left_key_column_ids.len() != right_key_column_ids.len()
            || left_key_column_ids.contains(&[0; 16])
            || right_key_column_ids.contains(&[0; 16])
            || !consume_planner_estimate_budget(
                &mut self.estimate_requests,
                self.transaction.coordination.max_planner_estimate_requests,
            )
        {
            return Ok(None);
        }
        let Some(left_relation) =
            self.describe_relation_for_plan(schema_name, left_relation_name)?
        else {
            return Ok(None);
        };
        let Some(right_relation) =
            self.describe_relation_for_plan(schema_name, right_relation_name)?
        else {
            return Ok(None);
        };
        if left_relation.schema_id != right_relation.schema_id
            || left_relation.schema_version != right_relation.schema_version
            || left_key_column_ids.iter().any(|column_id| {
                !left_relation
                    .relation
                    .columns
                    .iter()
                    .any(|column| column.column_id == *column_id)
            })
            || right_key_column_ids.iter().any(|column_id| {
                !right_relation
                    .relation
                    .columns
                    .iter()
                    .any(|column| column.column_id == *column_id)
            })
        {
            return Ok(None);
        }
        let cache_key = JoinEstimateCacheKey {
            left_relation_id: left_relation.relation.relation_id,
            right_relation_id: right_relation.relation.relation_id,
            schema_id: left_relation.schema_id,
            schema_version: left_relation.schema_version,
            left_key_column_ids: left_key_column_ids.to_vec(),
            right_key_column_ids: right_key_column_ids.to_vec(),
            maximum_rows,
        };
        let left_state = match self
            .transaction
            .load_catalog_estimate_projection(left_relation.relation.relation_id)
        {
            Ok(state) => state,
            Err(error) if estimate_error_is_terminal(&error) => return Err(error),
            Err(_) => return Ok(None),
        };
        let right_state = match self
            .transaction
            .load_catalog_estimate_projection(right_relation.relation.relation_id)
        {
            Ok(state) => state,
            Err(error) if estimate_error_is_terminal(&error) => return Err(error),
            Err(_) => return Ok(None),
        };
        let left_projection = AuthorizedProjection {
            columns: &left_state.schema.columns,
            column_ids: &left_state.schema.column_ids,
            schema_version: left_state.schema.schema_version,
            schema_id: left_state.schema.schema_id,
            context: &left_state.context,
            grants: &left_state.grants,
            policies: &left_state.policies,
            checks: &left_state.checks,
            policy_driving_columns: &left_state.policy_driving_columns,
            dependencies: &[],
        };
        let right_projection = AuthorizedProjection {
            columns: &right_state.schema.columns,
            column_ids: &right_state.schema.column_ids,
            schema_version: right_state.schema.schema_version,
            schema_id: right_state.schema.schema_id,
            context: &right_state.context,
            grants: &right_state.grants,
            policies: &right_state.policies,
            checks: &right_state.checks,
            policy_driving_columns: &right_state.policy_driving_columns,
            dependencies: &[],
        };
        let write_generation = self.transaction.overlay.generation();
        let transaction = &mut *self.transaction;
        cached_exact_estimate(
            &mut self.join_estimates,
            cache_key,
            write_generation,
            || {
                transaction.estimate_authorized_join_cardinality_at_most(
                    left_relation.relation.relation_id,
                    &left_projection,
                    left_key_column_ids,
                    right_relation.relation.relation_id,
                    &right_projection,
                    right_key_column_ids,
                    maximum_rows,
                )
            },
        )
    }

    /// Estimate a bound planner predicate after re-resolving its relation in
    /// the invocation snapshot. Predicate column references are decoded
    /// against the pinned table shape and convey no storage capability.
    pub fn estimate_predicate_for_plan(
        &mut self,
        schema_name: &str,
        relation_name: &str,
        predicate_bytes: &[u8],
    ) -> Result<Option<(u64, u64)>, TransactionError> {
        self.transaction.check_deadline()?;
        if predicate_bytes.is_empty()
            || predicate_bytes.len() > crate::optimizer_request::MAX_PREDICATE_BYTES
        {
            return Ok(None);
        }
        let Some(relation) = self.describe_relation_for_plan(schema_name, relation_name)? else {
            return Ok(None);
        };
        if schema_name == "information_schema"
            || !consume_planner_estimate_budget(
                &mut self.estimate_requests,
                self.transaction.coordination.max_planner_estimate_requests,
            )
        {
            return Ok(None);
        }
        let cache_key = PredicateEstimateCacheKey {
            relation_id: relation.relation.relation_id,
            schema_id: relation.schema_id,
            schema_version: relation.schema_version,
            schema_name: schema_name.to_owned(),
            relation_name: relation_name.to_owned(),
            predicate_bytes: predicate_bytes.to_vec(),
            maximum_rows: self
                .transaction
                .coordination
                .max_planner_estimate_candidates
                .min(MAX_CONFIGURABLE_ESTIMATE_CANDIDATES) as u64,
        };
        let write_generation = self.transaction.overlay.generation();
        self.predicate_estimates
            .retain(|_, (generation, _)| *generation == write_generation);
        if let Some((_, estimate)) = self.predicate_estimates.get(&cache_key) {
            self.transaction.check_deadline()?;
            return Ok(Some(*estimate));
        }
        let state = match self
            .transaction
            .load_catalog_estimate_projection(relation.relation.relation_id)
        {
            Ok(state) => state,
            Err(error) if estimate_error_is_terminal(&error) => return Err(error),
            Err(_) => return Ok(None),
        };
        let scan_columns = state
            .schema
            .columns
            .iter()
            .zip(&state.schema.column_ids)
            .enumerate()
            .map(
                |(ordinal, (column, &column_id))| crate::optimizer_binding::BoundColumn {
                    column_id,
                    ordinal: ordinal as u32,
                    scalar_type: column.scalar_type,
                    nullable: column.nullable,
                },
            )
            .collect::<Vec<_>>();
        let predicate =
            crate::optimizer_plan::decode_bound_predicate_v1(predicate_bytes, &scan_columns)
                .map_err(|_| TransactionError::InvalidMetadataRequest)?;
        let projection = AuthorizedProjection {
            columns: &state.schema.columns,
            column_ids: &state.schema.column_ids,
            schema_version: state.schema.schema_version,
            schema_id: state.schema.schema_id,
            context: &state.context,
            grants: &state.grants,
            policies: &state.policies,
            checks: &state.checks,
            policy_driving_columns: &state.policy_driving_columns,
            dependencies: &[],
        };
        let candidate_limit = self
            .transaction
            .coordination
            .max_planner_estimate_candidates;
        let estimate = self
            .transaction
            .estimate_authorized_predicate_at_most_with_limit(
                relation.relation.relation_id,
                &projection,
                &predicate,
                candidate_limit as u64,
                candidate_limit,
            )?;
        if let Some(estimate) = estimate {
            self.predicate_estimates
                .insert(cache_key, (write_generation, estimate));
        }
        Ok(estimate)
    }

    pub fn close_relation(&mut self, handle: &PlanningTableHandle) -> Result<(), TransactionError> {
        self.handles
            .remove(&handle.token)
            .map(|_| ())
            .ok_or(TransactionError::InvalidMetadataRequest)
    }
}
