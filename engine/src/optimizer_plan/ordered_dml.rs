use super::types::*;
use super::validate::*;
use super::wire::*;
use crate::optimizer_binding::{
    BoundOrderedSelect, BoundPredicate, BoundUpdateAssignment, BoundUpdateValue,
};
use crate::tuple::ScalarType;
use std::collections::HashSet;

pub fn plan_ordered_select_v1(bound: BoundOrderedSelect) -> Result<OrderedSelectPlanV1, PlanError> {
    let relation = RelationReference {
        schema_name: bound.schema_name,
        schema_id: bound.schema_id,
        schema_version: bound.schema_version,
        relation_name: bound.relation_name,
        relation_id: bound.relation_id,
        table_epoch: bound.table_epoch,
    };
    let mut scan_columns = bound.projections.clone();
    if !scan_columns.contains(&bound.order_key) {
        scan_columns.push(bound.order_key.clone());
    }
    scan_columns.sort_by_key(|column| column.ordinal);
    let plan = OrderedSelectPlanV1 {
        relation,
        scan_columns,
        projections: bound.projections,
        order_key: bound.order_key,
        descending: bound.descending,
        limit: bound.limit,
    };
    validate_ordered_select_plan_v1(&plan)?;
    Ok(plan)
}

pub fn validate_ordered_select_plan_v1(plan: &OrderedSelectPlanV1) -> Result<(), PlanError> {
    validate_relation(&plan.relation)?;
    validate_columns(&plan.scan_columns)?;
    if plan.projections.is_empty()
        || plan.projections.len() > crate::optimizer_request::MAX_PROJECTIONS
        || plan.projections.iter().any(|projection| {
            projection.column_id == [0; 16]
                || !valid_scalar_type(projection.scalar_type)
                || !plan.scan_columns.contains(projection)
        })
        || plan.order_key.column_id == [0; 16]
        || !valid_scalar_type(plan.order_key.scalar_type)
        || !plan.scan_columns.contains(&plan.order_key)
        || plan.scan_columns.len()
            != plan
                .projections
                .iter()
                .chain(std::iter::once(&plan.order_key))
                .map(|column| column.ordinal)
                .collect::<HashSet<_>>()
                .len()
    {
        return Err(PlanError::InvalidColumnReference);
    }
    let mut projected = HashSet::with_capacity(plan.projections.len());
    if plan
        .projections
        .iter()
        .any(|column| !projected.insert(column.ordinal))
    {
        return Err(PlanError::InvalidColumnReference);
    }
    Ok(())
}

pub fn encode_ordered_select_plan_v1(plan: &OrderedSelectPlanV1) -> Result<Vec<u8>, PlanError> {
    validate_ordered_select_plan_v1(plan)?;
    let mut output = Vec::with_capacity(256);
    output.extend_from_slice(&crate::query_abi::encode_optimizer_plan_header_record(
        1,
        PLAN_KIND_ORDERED_SELECT,
        0,
    ));
    put_name(&mut output, &plan.relation.schema_name)?;
    output.extend_from_slice(
        &crate::query_abi::encode_optimizer_plan_schema_reference_record(
            plan.relation.schema_id,
            plan.relation.schema_version,
        ),
    );
    put_name(&mut output, &plan.relation.relation_name)?;
    output.extend_from_slice(
        &crate::query_abi::encode_optimizer_plan_relation_reference_record(
            plan.relation.relation_id,
            plan.relation.table_epoch,
        ),
    );
    output.extend_from_slice(&(plan.scan_columns.len() as u16).to_be_bytes());
    for column in &plan.scan_columns {
        put_column(&mut output, column);
    }
    output.extend_from_slice(&(plan.projections.len() as u16).to_be_bytes());
    for projection in &plan.projections {
        put_column(&mut output, projection);
    }
    output.push(ORDER_BY_NODE);
    put_column(&mut output, &plan.order_key);
    output.push(u8::from(plan.descending));
    match plan.limit {
        None => output.push(0),
        Some(limit) => {
            output.push(1);
            output.extend_from_slice(&limit.to_be_bytes());
        }
    }
    if output.len() > MAX_PLAN_BYTES {
        return Err(PlanError::TooLarge);
    }
    Ok(output)
}

pub fn decode_ordered_select_plan_v1(bytes: &[u8]) -> Result<OrderedSelectPlanV1, PlanError> {
    if bytes.len() > MAX_PLAN_BYTES {
        return Err(PlanError::TooLarge);
    }
    let (version, kind, reserved) =
        crate::query_abi::decode_optimizer_plan_header_record(bytes).ok_or(PlanError::Truncated)?;
    if version != 1 {
        return Err(PlanError::UnsupportedVersion(version));
    }
    if kind != PLAN_KIND_ORDERED_SELECT || reserved != 0 {
        return Err(PlanError::InvalidEncoding);
    }
    let mut reader = PlanReader {
        bytes,
        offset: crate::query_abi::OPTIMIZER_PLAN_HEADER_RECORD_HEADER_BYTES as usize,
    };
    let schema_name = reader.name()?;
    let (schema_id, schema_version) =
        crate::query_abi::decode_optimizer_plan_schema_reference_record(reader.take(
            crate::query_abi::OPTIMIZER_PLAN_SCHEMA_REFERENCE_RECORD_HEADER_BYTES as usize,
        )?)
        .ok_or(PlanError::Truncated)?;
    let relation_name = reader.name()?;
    let (relation_id, table_epoch) =
        crate::query_abi::decode_optimizer_plan_relation_reference_record(reader.take(
            crate::query_abi::OPTIMIZER_PLAN_RELATION_REFERENCE_RECORD_HEADER_BYTES as usize,
        )?)
        .ok_or(PlanError::Truncated)?;
    let scan_count = usize::from(u16::from_be_bytes(
        reader
            .take(2)?
            .try_into()
            .map_err(|_| PlanError::Truncated)?,
    ));
    if scan_count == 0 || scan_count > MAX_PLAN_SCAN_COLUMNS {
        return Err(PlanError::InvalidColumnReference);
    }
    let mut scan_columns = Vec::with_capacity(scan_count);
    for _ in 0..scan_count {
        scan_columns.push(reader.column()?);
    }
    let projection_count = usize::from(u16::from_be_bytes(
        reader
            .take(2)?
            .try_into()
            .map_err(|_| PlanError::Truncated)?,
    ));
    if projection_count == 0 || projection_count > crate::optimizer_request::MAX_PROJECTIONS {
        return Err(PlanError::EmptyProjection);
    }
    let mut projections = Vec::with_capacity(projection_count);
    for _ in 0..projection_count {
        projections.push(reader.column()?);
    }
    if reader.u8()? != ORDER_BY_NODE {
        return Err(PlanError::InvalidPipeline);
    }
    let order_key = reader.column()?;
    let descending = match reader.u8()? {
        0 => false,
        1 => true,
        _ => return Err(PlanError::InvalidEncoding),
    };
    let limit = match reader.u8()? {
        0 => None,
        1 => Some(u32::from_be_bytes(
            reader
                .take(4)?
                .try_into()
                .map_err(|_| PlanError::Truncated)?,
        )),
        _ => return Err(PlanError::InvalidEncoding),
    };
    if reader.offset != bytes.len() {
        return Err(PlanError::TrailingBytes);
    }
    let plan = OrderedSelectPlanV1 {
        relation: RelationReference {
            schema_name,
            schema_id,
            schema_version,
            relation_name,
            relation_id,
            table_epoch,
        },
        scan_columns,
        projections,
        order_key,
        descending,
        limit,
    };
    validate_ordered_select_plan_v1(&plan)?;
    Ok(plan)
}

/// Construct a deterministic DELETE plan from an already bound relation and
/// Boolean predicate. The plan conveys no storage capability or write grant.
pub fn plan_delete_v1(
    relation: RelationReference,
    predicate: BoundPredicate,
) -> Result<DeletePlanV1, PlanError> {
    let mut scan_columns = Vec::new();
    collect_predicate_columns(&predicate, &mut scan_columns)?;
    scan_columns.sort_by_key(|column| column.ordinal);
    let plan = DeletePlanV1 {
        relation,
        scan_columns,
        predicate,
    };
    validate_delete_plan_v1(&plan)?;
    Ok(plan)
}

/// Encode a canonical DELETE plan as a `Scan -> Filter -> Delete` pipeline.
pub fn encode_delete_plan_v1(plan: &DeletePlanV1) -> Result<Vec<u8>, PlanError> {
    validate_delete_plan_v1(plan)?;
    let mut output = Vec::with_capacity(256);
    output.extend_from_slice(&crate::query_abi::encode_optimizer_plan_header_record(
        1,
        PLAN_KIND_DELETE,
        0,
    ));
    put_name(&mut output, &plan.relation.schema_name)?;
    output.extend_from_slice(
        &crate::query_abi::encode_optimizer_plan_schema_reference_record(
            plan.relation.schema_id,
            plan.relation.schema_version,
        ),
    );
    put_name(&mut output, &plan.relation.relation_name)?;
    output.extend_from_slice(
        &crate::query_abi::encode_optimizer_plan_relation_reference_record(
            plan.relation.relation_id,
            plan.relation.table_epoch,
        ),
    );
    output.extend_from_slice(&crate::query_abi::encode_optimizer_plan_node_count_record(
        3,
    ));
    put_column_list_header(&mut output, 1, plan.scan_columns.len())?;
    for column in &plan.scan_columns {
        put_column(&mut output, column);
    }
    output.extend_from_slice(&crate::query_abi::encode_optimizer_plan_filter_node_record(
        2,
    ));
    put_predicate(&mut output, &plan.predicate)?;
    output.push(DELETE_SINK_NODE);
    if output.len() > MAX_PLAN_BYTES {
        return Err(PlanError::TooLarge);
    }
    Ok(output)
}

/// Decode and validate a canonical DELETE plan independently from the
/// optimizer. The executor must re-resolve the relation and still use its
/// ordinary scan and mutation IPC operations.
pub fn decode_delete_plan_v1(bytes: &[u8]) -> Result<DeletePlanV1, PlanError> {
    if bytes.len() > MAX_PLAN_BYTES {
        return Err(PlanError::TooLarge);
    }
    let (version, kind, reserved) =
        crate::query_abi::decode_optimizer_plan_header_record(bytes).ok_or(PlanError::Truncated)?;
    if version != 1 {
        return Err(PlanError::UnsupportedVersion(version));
    }
    if kind != PLAN_KIND_DELETE || reserved != 0 {
        return Err(PlanError::InvalidEncoding);
    }
    let mut reader = PlanReader {
        bytes,
        offset: crate::query_abi::OPTIMIZER_PLAN_HEADER_RECORD_HEADER_BYTES as usize,
    };
    let schema_name = reader.name()?;
    let schema_reference = reader
        .take(crate::query_abi::OPTIMIZER_PLAN_SCHEMA_REFERENCE_RECORD_HEADER_BYTES as usize)?;
    let (schema_id, schema_version) =
        crate::query_abi::decode_optimizer_plan_schema_reference_record(schema_reference)
            .ok_or(PlanError::Truncated)?;
    let relation_name = reader.name()?;
    let relation_reference = reader
        .take(crate::query_abi::OPTIMIZER_PLAN_RELATION_REFERENCE_RECORD_HEADER_BYTES as usize)?;
    let (relation_id, table_epoch) =
        crate::query_abi::decode_optimizer_plan_relation_reference_record(relation_reference)
            .ok_or(PlanError::Truncated)?;
    let relation = RelationReference {
        schema_name,
        schema_id,
        schema_version,
        relation_name,
        relation_id,
        table_epoch,
    };
    if reader.node_count()? != 3 {
        return Err(PlanError::InvalidPipeline);
    }
    let scan_tag_offset = reader.offset;
    if reader.u8()? != 1 {
        return Err(PlanError::InvalidPipeline);
    }
    let column_count = reader.column_list_count(scan_tag_offset, 1, crate::tuple::MAX_COLUMNS)?;
    let mut scan_columns = Vec::with_capacity(column_count);
    for _ in 0..column_count {
        scan_columns.push(reader.column()?);
    }
    if !scan_columns.is_empty() {
        validate_columns(&scan_columns)?;
    }
    let filter_tag_offset = reader.offset;
    reader.filter_node_tag(filter_tag_offset)?;
    let predicate = reader.predicate(&scan_columns)?;
    if reader.u8()? != DELETE_SINK_NODE || !reader.is_empty() {
        return Err(PlanError::InvalidPipeline);
    }
    let plan = DeletePlanV1 {
        relation,
        scan_columns,
        predicate,
    };
    validate_delete_plan_v1(&plan)?;
    Ok(plan)
}

/// Check a decoded DELETE plan's structural, type, and bounded-scan
/// invariants. Relation authorization and row-level permissions remain owned
/// by invocation-bound storage.
pub fn validate_delete_plan_v1(plan: &DeletePlanV1) -> Result<(), PlanError> {
    validate_relation(&plan.relation)?;
    if plan.scan_columns.len() > crate::tuple::MAX_COLUMNS {
        return Err(PlanError::TooLarge);
    }
    if !plan.scan_columns.is_empty() {
        validate_columns(&plan.scan_columns)?;
    }
    if predicate_type(&plan.predicate, &plan.scan_columns)? != Some(ScalarType::Boolean) {
        return Err(PlanError::InvalidPredicate);
    }
    let mut referenced_columns = Vec::new();
    collect_predicate_columns(&plan.predicate, &mut referenced_columns)?;
    referenced_columns.sort_by_key(|column| column.ordinal);
    if referenced_columns != plan.scan_columns {
        return Err(PlanError::InvalidColumnReference);
    }
    Ok(())
}

/// Construct a deterministic UPDATE plan. The scan carries the unique,
/// ordinal-sorted union of assignment targets and predicate references.
pub fn plan_update_v1(
    relation: RelationReference,
    predicate: BoundPredicate,
    assignments: Vec<BoundUpdateAssignment>,
) -> Result<UpdatePlanV1, PlanError> {
    let mut scan_columns = Vec::new();
    collect_predicate_columns(&predicate, &mut scan_columns)?;
    for assignment in &assignments {
        add_column(&mut scan_columns, &assignment.column)?;
    }
    scan_columns.sort_by_key(|column| column.ordinal);
    let plan = UpdatePlanV1 {
        relation,
        scan_columns,
        predicate,
        assignments,
    };
    validate_update_plan_v1(&plan)?;
    Ok(plan)
}

/// Encode the canonical `Scan -> Filter -> Update` pipeline.
pub fn encode_update_plan_v1(plan: &UpdatePlanV1) -> Result<Vec<u8>, PlanError> {
    validate_update_plan_v1(plan)?;
    let mut output = Vec::with_capacity(256);
    output.extend_from_slice(&crate::query_abi::encode_optimizer_plan_header_record(
        1,
        PLAN_KIND_UPDATE,
        0,
    ));
    put_name(&mut output, &plan.relation.schema_name)?;
    output.extend_from_slice(
        &crate::query_abi::encode_optimizer_plan_schema_reference_record(
            plan.relation.schema_id,
            plan.relation.schema_version,
        ),
    );
    put_name(&mut output, &plan.relation.relation_name)?;
    output.extend_from_slice(
        &crate::query_abi::encode_optimizer_plan_relation_reference_record(
            plan.relation.relation_id,
            plan.relation.table_epoch,
        ),
    );
    output.extend_from_slice(&crate::query_abi::encode_optimizer_plan_node_count_record(
        3,
    ));
    put_column_list_header(&mut output, 1, plan.scan_columns.len())?;
    for column in &plan.scan_columns {
        put_column(&mut output, column);
    }
    output.extend_from_slice(&crate::query_abi::encode_optimizer_plan_filter_node_record(
        2,
    ));
    put_predicate(&mut output, &plan.predicate)?;
    let assignment_count =
        u16::try_from(plan.assignments.len()).map_err(|_| PlanError::InvalidPipeline)?;
    output.extend_from_slice(&crate::query_abi::encode_optimizer_plan_update_node_record(
        crate::query_abi::OPTIMIZER_PLAN_UPDATE_NODE,
        assignment_count,
    ));
    for assignment in &plan.assignments {
        put_column(&mut output, &assignment.column);
        match assignment.value {
            BoundUpdateValue::Parameter(index) => {
                output.extend_from_slice(
                    &crate::query_abi::encode_optimizer_plan_update_assignment_value_record(
                        crate::query_abi::OPTIMIZER_UPDATE_PARAMETER_VALUE,
                        index,
                    ),
                );
            }
            BoundUpdateValue::Null => output.extend_from_slice(
                &crate::query_abi::encode_optimizer_plan_update_assignment_value_record(
                    crate::query_abi::OPTIMIZER_UPDATE_NULL_VALUE,
                    0,
                ),
            ),
        }
    }
    if output.len() > MAX_PLAN_BYTES {
        return Err(PlanError::TooLarge);
    }
    Ok(output)
}

/// Decode and validate a canonical UPDATE plan without granting storage
/// access. The executor must re-resolve the relation before execution.
pub fn decode_update_plan_v1(bytes: &[u8]) -> Result<UpdatePlanV1, PlanError> {
    if bytes.len() > MAX_PLAN_BYTES {
        return Err(PlanError::TooLarge);
    }
    let (version, kind, reserved) =
        crate::query_abi::decode_optimizer_plan_header_record(bytes).ok_or(PlanError::Truncated)?;
    if version != 1 {
        return Err(PlanError::UnsupportedVersion(version));
    }
    if kind != PLAN_KIND_UPDATE || reserved != 0 {
        return Err(PlanError::InvalidEncoding);
    }
    let mut reader = PlanReader {
        bytes,
        offset: crate::query_abi::OPTIMIZER_PLAN_HEADER_RECORD_HEADER_BYTES as usize,
    };
    let schema_name = reader.name()?;
    let schema_reference = reader
        .take(crate::query_abi::OPTIMIZER_PLAN_SCHEMA_REFERENCE_RECORD_HEADER_BYTES as usize)?;
    let (schema_id, schema_version) =
        crate::query_abi::decode_optimizer_plan_schema_reference_record(schema_reference)
            .ok_or(PlanError::Truncated)?;
    let relation_name = reader.name()?;
    let relation_reference = reader
        .take(crate::query_abi::OPTIMIZER_PLAN_RELATION_REFERENCE_RECORD_HEADER_BYTES as usize)?;
    let (relation_id, table_epoch) =
        crate::query_abi::decode_optimizer_plan_relation_reference_record(relation_reference)
            .ok_or(PlanError::Truncated)?;
    let relation = RelationReference {
        schema_name,
        schema_id,
        schema_version,
        relation_name,
        relation_id,
        table_epoch,
    };
    if reader.node_count()? != 3 {
        return Err(PlanError::InvalidPipeline);
    }
    let scan_tag_offset = reader.offset;
    if reader.u8()? != 1 {
        return Err(PlanError::InvalidPipeline);
    }
    let column_count = reader.column_list_count(scan_tag_offset, 1, crate::tuple::MAX_COLUMNS)?;
    let mut scan_columns = Vec::with_capacity(column_count);
    for _ in 0..column_count {
        scan_columns.push(reader.column()?);
    }
    if !scan_columns.is_empty() {
        validate_columns(&scan_columns)?;
    }
    let filter_tag_offset = reader.offset;
    reader.filter_node_tag(filter_tag_offset)?;
    let predicate = reader.predicate(&scan_columns)?;
    let update_node =
        reader.take(crate::query_abi::OPTIMIZER_PLAN_UPDATE_NODE_RECORD_HEADER_BYTES as usize)?;
    let (node_tag, assignment_count) =
        crate::query_abi::decode_optimizer_plan_update_node_record(update_node)
            .ok_or(PlanError::Truncated)?;
    if node_tag != crate::query_abi::OPTIMIZER_PLAN_UPDATE_NODE {
        return Err(PlanError::InvalidPipeline);
    }
    let assignment_count = usize::from(assignment_count);
    if assignment_count == 0 || assignment_count > crate::optimizer_request::MAX_UPDATE_ASSIGNMENTS
    {
        return Err(PlanError::InvalidPipeline);
    }
    let mut assignments = Vec::with_capacity(assignment_count);
    for _ in 0..assignment_count {
        let column = reader.column()?;
        let value_record = reader.take(
            crate::query_abi::OPTIMIZER_PLAN_UPDATE_ASSIGNMENT_VALUE_RECORD_HEADER_BYTES as usize,
        )?;
        let (kind, parameter_index) =
            crate::query_abi::decode_optimizer_plan_update_assignment_value_record(value_record)
                .ok_or(PlanError::Truncated)?;
        let value = match kind {
            crate::query_abi::OPTIMIZER_UPDATE_PARAMETER_VALUE => {
                let index = parameter_index;
                if index == 0 || index > crate::optimizer_request::MAX_PARAMETER_INDEX {
                    return Err(PlanError::InvalidEncoding);
                }
                BoundUpdateValue::Parameter(index)
            }
            crate::query_abi::OPTIMIZER_UPDATE_NULL_VALUE
                if parameter_index == 0 && column.nullable =>
            {
                BoundUpdateValue::Null
            }
            crate::query_abi::OPTIMIZER_UPDATE_NULL_VALUE => {
                return Err(PlanError::InvalidEncoding);
            }
            _ => return Err(PlanError::InvalidEncoding),
        };
        assignments.push(BoundUpdateAssignment { column, value });
    }
    if !reader.is_empty() {
        return Err(PlanError::TrailingBytes);
    }
    let plan = UpdatePlanV1 {
        relation,
        scan_columns,
        predicate,
        assignments,
    };
    validate_update_plan_v1(&plan)?;
    Ok(plan)
}

/// Validate relation references, scan shape, assignment uniqueness and
/// nullability, and the Boolean predicate without granting write authority.
pub fn validate_update_plan_v1(plan: &UpdatePlanV1) -> Result<(), PlanError> {
    validate_relation(&plan.relation)?;
    if plan.scan_columns.len() > crate::tuple::MAX_COLUMNS {
        return Err(PlanError::TooLarge);
    }
    if !plan.scan_columns.is_empty() {
        validate_columns(&plan.scan_columns)?;
    }
    if predicate_type(&plan.predicate, &plan.scan_columns)? != Some(ScalarType::Boolean) {
        return Err(PlanError::InvalidPredicate);
    }
    if plan.assignments.is_empty()
        || plan.assignments.len() > crate::optimizer_request::MAX_UPDATE_ASSIGNMENTS
    {
        return Err(PlanError::InvalidPipeline);
    }
    let mut ordinals = HashSet::with_capacity(plan.assignments.len());
    for assignment in &plan.assignments {
        if !plan
            .scan_columns
            .iter()
            .any(|column| column == &assignment.column)
            || !ordinals.insert(assignment.column.ordinal)
        {
            return Err(PlanError::InvalidColumnReference);
        }
        match assignment.value {
            BoundUpdateValue::Parameter(index)
                if index != 0 && index <= crate::optimizer_request::MAX_PARAMETER_INDEX => {}
            BoundUpdateValue::Parameter(_) => return Err(PlanError::InvalidEncoding),
            BoundUpdateValue::Null if assignment.column.nullable => {}
            BoundUpdateValue::Null => return Err(PlanError::InvalidEncoding),
        }
    }
    let mut required_columns = Vec::new();
    collect_predicate_columns(&plan.predicate, &mut required_columns)?;
    for assignment in &plan.assignments {
        add_column(&mut required_columns, &assignment.column)?;
    }
    required_columns.sort_by_key(|column| column.ordinal);
    if required_columns != plan.scan_columns {
        return Err(PlanError::InvalidColumnReference);
    }
    Ok(())
}
