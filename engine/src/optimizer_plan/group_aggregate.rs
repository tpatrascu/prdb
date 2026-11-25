use super::types::*;
use super::validate::*;
use super::wire::*;
use crate::optimizer_binding::{
    BoundCompositeGroupAggregate, BoundFilteredCompositeGroupAggregate, BoundGroupAggregate,
    BoundOrderedCompositeGroupAggregate, BoundOrderedGroupAggregate,
};
use crate::optimizer_request::{CompositeGroupCountOrderItem, GroupCountOrderItem};
use crate::tuple::ScalarType;
use std::collections::HashSet;

pub fn plan_group_aggregate_v1(
    bound: BoundGroupAggregate,
) -> Result<GroupAggregatePlanV1, PlanError> {
    let relation = RelationReference {
        schema_name: bound.schema_name,
        schema_id: bound.schema_id,
        schema_version: bound.schema_version,
        relation_name: bound.relation_name,
        relation_id: bound.relation_id,
        table_epoch: bound.table_epoch,
    };
    let mut scan_columns = vec![bound.group_key.clone()];
    if let Some(column) = &bound.aggregate_column
        && !scan_columns
            .iter()
            .any(|existing| existing.ordinal == column.ordinal)
    {
        scan_columns.push(column.clone());
    }
    scan_columns.sort_by_key(|column| column.ordinal);
    let plan = GroupAggregatePlanV1 {
        relation,
        scan_columns,
        group_key: bound.group_key,
        aggregate_kind: bound.aggregate_kind,
        aggregate_column: bound.aggregate_column,
        limit: bound.limit,
    };
    validate_group_aggregate_plan_v1(&plan)?;
    Ok(plan)
}

pub fn encode_group_aggregate_plan_v1(plan: &GroupAggregatePlanV1) -> Result<Vec<u8>, PlanError> {
    validate_group_aggregate_plan_v1(plan)?;
    let mut output = Vec::with_capacity(224);
    output.extend_from_slice(&crate::query_abi::encode_optimizer_plan_header_record(
        1,
        PLAN_KIND_GROUP_AGGREGATE,
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
    output.extend_from_slice(&[GROUP_AGGREGATE_NODE, plan.aggregate_kind]);
    put_column(&mut output, &plan.group_key);
    match &plan.aggregate_column {
        None => output.push(0),
        Some(column) => {
            output.push(1);
            put_column(&mut output, column);
        }
    }
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

pub fn decode_group_aggregate_plan_v1(bytes: &[u8]) -> Result<GroupAggregatePlanV1, PlanError> {
    if bytes.len() > MAX_PLAN_BYTES {
        return Err(PlanError::TooLarge);
    }
    let (version, kind, reserved) =
        crate::query_abi::decode_optimizer_plan_header_record(bytes).ok_or(PlanError::Truncated)?;
    if version != 1 {
        return Err(PlanError::UnsupportedVersion(version));
    }
    if kind != PLAN_KIND_GROUP_AGGREGATE || reserved != 0 {
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
    if !(1..=2).contains(&scan_count) {
        return Err(PlanError::InvalidPipeline);
    }
    let mut scan_columns = Vec::with_capacity(scan_count);
    for _ in 0..scan_count {
        scan_columns.push(reader.column()?);
    }
    if reader.u8()? != GROUP_AGGREGATE_NODE {
        return Err(PlanError::InvalidPipeline);
    }
    let aggregate_kind = reader.u8()?;
    let group_key = reader.column()?;
    let aggregate_column = match reader.u8()? {
        0 => None,
        1 => Some(reader.column()?),
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
    let plan = GroupAggregatePlanV1 {
        relation: RelationReference {
            schema_name,
            schema_id,
            schema_version,
            relation_name,
            relation_id,
            table_epoch,
        },
        scan_columns,
        group_key,
        aggregate_kind,
        aggregate_column,
        limit,
    };
    validate_group_aggregate_plan_v1(&plan)?;
    Ok(plan)
}

pub fn validate_group_aggregate_plan_v1(plan: &GroupAggregatePlanV1) -> Result<(), PlanError> {
    validate_relation(&plan.relation)?;
    validate_columns(&plan.scan_columns)?;
    if !(1..=6).contains(&plan.aggregate_kind)
        || plan.group_key.column_id == [0; 16]
        || !valid_scalar_type(plan.group_key.scalar_type)
    {
        return Err(PlanError::InvalidColumnReference);
    }
    let mut expected = vec![plan.group_key.clone()];
    match (plan.aggregate_kind, &plan.aggregate_column) {
        (1, None) => {}
        (2..=6, Some(column))
            if column.column_id != [0; 16] && valid_scalar_type(column.scalar_type) =>
        {
            if (plan.aggregate_kind == 4 || plan.aggregate_kind == 5)
                && !is_numeric_type(column.scalar_type)
            {
                return Err(PlanError::InvalidColumnReference);
            }
            if let Some(existing) = expected.iter().find(|item| item.ordinal == column.ordinal) {
                if existing != column {
                    return Err(PlanError::InvalidColumnReference);
                }
            } else {
                expected.push(column.clone());
            }
        }
        _ => return Err(PlanError::InvalidColumnReference),
    }
    expected.sort_by_key(|column| column.ordinal);
    if expected != plan.scan_columns {
        return Err(PlanError::InvalidColumnReference);
    }
    Ok(())
}

pub fn plan_ordered_group_aggregate_v1(
    bound: BoundOrderedGroupAggregate,
) -> Result<OrderedGroupAggregatePlanV1, PlanError> {
    let aggregate_plan = plan_group_aggregate_v1(BoundGroupAggregate {
        schema_name: bound.schema_name,
        schema_id: bound.schema_id,
        schema_version: bound.schema_version,
        table_epoch: bound.table_epoch,
        relation_id: bound.relation_id,
        relation_name: bound.relation_name,
        group_key: bound.group_key,
        aggregate_kind: bound.aggregate_kind,
        aggregate_column: bound.aggregate_column,
        limit: bound.limit,
    })?;
    let plan = OrderedGroupAggregatePlanV1 {
        aggregate_plan,
        order_items: bound.order_items,
    };
    validate_ordered_group_aggregate_plan_v1(&plan)?;
    Ok(plan)
}

pub fn validate_ordered_group_aggregate_plan_v1(
    plan: &OrderedGroupAggregatePlanV1,
) -> Result<(), PlanError> {
    validate_group_aggregate_plan_v1(&plan.aggregate_plan)?;
    if !(1..=2).contains(&plan.order_items.len()) {
        return Err(PlanError::InvalidColumnReference);
    }
    let mut seen = 0u8;
    for item in &plan.order_items {
        if item.source > 1 || seen & (1 << item.source) != 0 {
            return Err(PlanError::InvalidColumnReference);
        }
        seen |= 1 << item.source;
    }
    Ok(())
}

pub fn encode_ordered_group_aggregate_plan_v1(
    plan: &OrderedGroupAggregatePlanV1,
) -> Result<Vec<u8>, PlanError> {
    validate_ordered_group_aggregate_plan_v1(plan)?;
    let aggregate = &plan.aggregate_plan;
    let mut output = Vec::with_capacity(240);
    output.extend_from_slice(&crate::query_abi::encode_optimizer_plan_header_record(
        1,
        PLAN_KIND_ORDERED_GROUP_AGGREGATE,
        0,
    ));
    put_name(&mut output, &aggregate.relation.schema_name)?;
    output.extend_from_slice(
        &crate::query_abi::encode_optimizer_plan_schema_reference_record(
            aggregate.relation.schema_id,
            aggregate.relation.schema_version,
        ),
    );
    put_name(&mut output, &aggregate.relation.relation_name)?;
    output.extend_from_slice(
        &crate::query_abi::encode_optimizer_plan_relation_reference_record(
            aggregate.relation.relation_id,
            aggregate.relation.table_epoch,
        ),
    );
    output.extend_from_slice(&(aggregate.scan_columns.len() as u16).to_be_bytes());
    for column in &aggregate.scan_columns {
        put_column(&mut output, column);
    }
    output.extend_from_slice(&[ORDERED_GROUP_AGGREGATE_NODE, aggregate.aggregate_kind]);
    put_column(&mut output, &aggregate.group_key);
    match &aggregate.aggregate_column {
        None => output.push(0),
        Some(column) => {
            output.push(1);
            put_column(&mut output, column);
        }
    }
    output.push(plan.order_items.len() as u8);
    for item in &plan.order_items {
        output.extend_from_slice(&[
            item.source,
            u8::from(item.descending),
            u8::from(item.nulls_first),
        ]);
    }
    match aggregate.limit {
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

pub fn decode_ordered_group_aggregate_plan_v1(
    bytes: &[u8],
) -> Result<OrderedGroupAggregatePlanV1, PlanError> {
    if bytes.len() > MAX_PLAN_BYTES {
        return Err(PlanError::TooLarge);
    }
    let (version, kind, reserved) =
        crate::query_abi::decode_optimizer_plan_header_record(bytes).ok_or(PlanError::Truncated)?;
    if version != 1 {
        return Err(PlanError::UnsupportedVersion(version));
    }
    if kind != PLAN_KIND_ORDERED_GROUP_AGGREGATE || reserved != 0 {
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
    if !(1..=2).contains(&scan_count) {
        return Err(PlanError::InvalidPipeline);
    }
    let mut scan_columns = Vec::with_capacity(scan_count);
    for _ in 0..scan_count {
        scan_columns.push(reader.column()?);
    }
    if reader.u8()? != ORDERED_GROUP_AGGREGATE_NODE {
        return Err(PlanError::InvalidPipeline);
    }
    let aggregate_kind = reader.u8()?;
    let group_key = reader.column()?;
    let aggregate_column = match reader.u8()? {
        0 => None,
        1 => Some(reader.column()?),
        _ => return Err(PlanError::InvalidEncoding),
    };
    let order_count = usize::from(reader.u8()?);
    if !(1..=2).contains(&order_count) {
        return Err(PlanError::InvalidColumnReference);
    }
    let mut order_items = Vec::with_capacity(order_count);
    for _ in 0..order_count {
        let source = reader.u8()?;
        let descending = match reader.u8()? {
            0 => false,
            1 => true,
            _ => return Err(PlanError::InvalidEncoding),
        };
        let nulls_first = match reader.u8()? {
            0 => false,
            1 => true,
            _ => return Err(PlanError::InvalidEncoding),
        };
        order_items.push(GroupCountOrderItem {
            source,
            descending,
            nulls_first,
        });
    }
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
    let plan = OrderedGroupAggregatePlanV1 {
        aggregate_plan: GroupAggregatePlanV1 {
            relation: RelationReference {
                schema_name,
                schema_id,
                schema_version,
                relation_name,
                relation_id,
                table_epoch,
            },
            scan_columns,
            group_key,
            aggregate_kind,
            aggregate_column,
            limit,
        },
        order_items,
    };
    validate_ordered_group_aggregate_plan_v1(&plan)?;
    Ok(plan)
}

pub fn plan_composite_group_aggregate_v1(
    bound: BoundCompositeGroupAggregate,
) -> Result<CompositeGroupAggregatePlanV1, PlanError> {
    let relation = RelationReference {
        schema_name: bound.schema_name,
        schema_id: bound.schema_id,
        schema_version: bound.schema_version,
        table_epoch: bound.table_epoch,
        relation_id: bound.relation_id,
        relation_name: bound.relation_name,
    };
    let mut scan_columns = bound.group_keys.clone();
    if let Some(column) = &bound.aggregate_column
        && !scan_columns.iter().any(|key| key == column)
    {
        scan_columns.push(column.clone());
    }
    scan_columns.sort_by_key(|column| column.ordinal);
    let plan = CompositeGroupAggregatePlanV1 {
        relation,
        scan_columns,
        group_keys: bound.group_keys,
        aggregate_kind: bound.aggregate_kind,
        aggregate_column: bound.aggregate_column,
        limit: bound.limit,
    };
    validate_composite_group_aggregate_plan_v1(&plan)?;
    Ok(plan)
}

pub fn plan_filtered_composite_group_aggregate_v1(
    bound: BoundFilteredCompositeGroupAggregate,
) -> Result<FilteredCompositeGroupAggregatePlanV1, PlanError> {
    let relation = RelationReference {
        schema_name: bound.schema_name,
        schema_id: bound.schema_id,
        schema_version: bound.schema_version,
        table_epoch: bound.table_epoch,
        relation_id: bound.relation_id,
        relation_name: bound.relation_name,
    };
    let mut scan_columns = bound.group_keys.clone();
    if let Some(column) = &bound.aggregate_column {
        add_column(&mut scan_columns, column)?;
    }
    collect_predicate_columns(&bound.predicate, &mut scan_columns)?;
    scan_columns.sort_by_key(|column| column.ordinal);
    let plan = FilteredCompositeGroupAggregatePlanV1 {
        relation,
        scan_columns,
        group_keys: bound.group_keys,
        aggregate_kind: bound.aggregate_kind,
        aggregate_column: bound.aggregate_column,
        predicate: bound.predicate,
        order_items: bound.order_items,
        limit: bound.limit,
    };
    validate_filtered_composite_group_aggregate_plan_v1(&plan)?;
    Ok(plan)
}

pub fn validate_filtered_composite_group_aggregate_plan_v1(
    plan: &FilteredCompositeGroupAggregatePlanV1,
) -> Result<(), PlanError> {
    validate_relation(&plan.relation)?;
    validate_columns(&plan.scan_columns)?;
    if !(1..=crate::optimizer_request::MAX_GROUP_KEYS).contains(&plan.group_keys.len())
        || !(1..=6).contains(&plan.aggregate_kind)
        || plan
            .group_keys
            .iter()
            .any(|key| key.column_id == [0; 16] || !valid_scalar_type(key.scalar_type))
    {
        return Err(PlanError::InvalidColumnReference);
    }
    let mut expected = plan.group_keys.clone();
    let mut seen = HashSet::with_capacity(plan.group_keys.len());
    if plan.group_keys.iter().any(|key| !seen.insert(key.ordinal)) {
        return Err(PlanError::InvalidColumnReference);
    }
    match (plan.aggregate_kind, &plan.aggregate_column) {
        (1, None) => {}
        (2..=6, Some(column))
            if column.column_id != [0; 16] && valid_scalar_type(column.scalar_type) =>
        {
            if matches!(plan.aggregate_kind, 4 | 5) && !is_numeric_type(column.scalar_type) {
                return Err(PlanError::InvalidColumnReference);
            }
            add_column(&mut expected, column)?;
        }
        _ => return Err(PlanError::InvalidColumnReference),
    }
    collect_predicate_columns(&plan.predicate, &mut expected)?;
    if !plan.order_items.is_empty()
        && !(1..=plan.group_keys.len() + 1).contains(&plan.order_items.len())
    {
        return Err(PlanError::InvalidColumnReference);
    }
    let mut seen_order_keys = HashSet::new();
    let mut aggregate_seen = false;
    for item in &plan.order_items {
        match (item.source, item.group_key_index) {
            (0, Some(index))
                if usize::from(index) < plan.group_keys.len() && seen_order_keys.insert(index) => {}
            (1, None) if !aggregate_seen => aggregate_seen = true,
            _ => return Err(PlanError::InvalidColumnReference),
        }
    }
    expected.sort_by_key(|column| column.ordinal);
    if expected != plan.scan_columns
        || predicate_type(&plan.predicate, &plan.scan_columns)? != Some(ScalarType::Boolean)
    {
        return Err(PlanError::InvalidColumnReference);
    }
    Ok(())
}

pub fn validate_composite_group_aggregate_plan_v1(
    plan: &CompositeGroupAggregatePlanV1,
) -> Result<(), PlanError> {
    validate_relation(&plan.relation)?;
    validate_columns(&plan.scan_columns)?;
    if !(crate::optimizer_request::MIN_GROUP_KEYS..=crate::optimizer_request::MAX_GROUP_KEYS)
        .contains(&plan.group_keys.len())
        || !(1..=6).contains(&plan.aggregate_kind)
        || plan
            .group_keys
            .iter()
            .any(|key| key.column_id == [0; 16] || !valid_scalar_type(key.scalar_type))
    {
        return Err(PlanError::InvalidColumnReference);
    }
    let mut expected = plan.group_keys.clone();
    let mut seen = HashSet::with_capacity(plan.group_keys.len());
    if plan.group_keys.iter().any(|key| !seen.insert(key.ordinal)) {
        return Err(PlanError::InvalidColumnReference);
    }
    match (plan.aggregate_kind, &plan.aggregate_column) {
        (1, None) => {}
        (2..=6, Some(column))
            if column.column_id != [0; 16] && valid_scalar_type(column.scalar_type) =>
        {
            if matches!(plan.aggregate_kind, 4 | 5) && !is_numeric_type(column.scalar_type) {
                return Err(PlanError::InvalidColumnReference);
            }
            if let Some(existing) = expected.iter().find(|item| item.ordinal == column.ordinal) {
                if existing != column {
                    return Err(PlanError::InvalidColumnReference);
                }
            } else {
                expected.push(column.clone());
            }
        }
        _ => return Err(PlanError::InvalidColumnReference),
    }
    expected.sort_by_key(|column| column.ordinal);
    if expected != plan.scan_columns {
        return Err(PlanError::InvalidColumnReference);
    }
    Ok(())
}

pub fn encode_composite_group_aggregate_plan_v1(
    plan: &CompositeGroupAggregatePlanV1,
) -> Result<Vec<u8>, PlanError> {
    validate_composite_group_aggregate_plan_v1(plan)?;
    let mut output = Vec::with_capacity(512);
    output.extend_from_slice(&crate::query_abi::encode_optimizer_plan_header_record(
        1,
        PLAN_KIND_COMPOSITE_GROUP_AGGREGATE,
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
    output.extend_from_slice(&[
        COMPOSITE_GROUP_AGGREGATE_NODE,
        plan.aggregate_kind,
        plan.group_keys.len() as u8,
    ]);
    for key in &plan.group_keys {
        put_column(&mut output, key);
    }
    match &plan.aggregate_column {
        None => output.push(0),
        Some(column) => {
            output.push(1);
            put_column(&mut output, column);
        }
    }
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

pub fn decode_composite_group_aggregate_plan_v1(
    bytes: &[u8],
) -> Result<CompositeGroupAggregatePlanV1, PlanError> {
    if bytes.len() > MAX_PLAN_BYTES {
        return Err(PlanError::TooLarge);
    }
    let (version, kind, reserved) =
        crate::query_abi::decode_optimizer_plan_header_record(bytes).ok_or(PlanError::Truncated)?;
    if version != 1 {
        return Err(PlanError::UnsupportedVersion(version));
    }
    if kind != PLAN_KIND_COMPOSITE_GROUP_AGGREGATE || reserved != 0 {
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
    if !(crate::optimizer_request::MIN_GROUP_KEYS..=crate::optimizer_request::MAX_GROUP_KEYS + 1)
        .contains(&scan_count)
    {
        return Err(PlanError::InvalidColumnReference);
    }
    let mut scan_columns = Vec::with_capacity(scan_count);
    for _ in 0..scan_count {
        scan_columns.push(reader.column()?);
    }
    if reader.u8()? != COMPOSITE_GROUP_AGGREGATE_NODE {
        return Err(PlanError::InvalidPipeline);
    }
    let aggregate_kind = reader.u8()?;
    let key_count = usize::from(reader.u8()?);
    if !(crate::optimizer_request::MIN_GROUP_KEYS..=crate::optimizer_request::MAX_GROUP_KEYS)
        .contains(&key_count)
    {
        return Err(PlanError::InvalidColumnReference);
    }
    let mut group_keys = Vec::with_capacity(key_count);
    for _ in 0..key_count {
        group_keys.push(reader.column()?);
    }
    let aggregate_column = match reader.u8()? {
        0 => None,
        1 => Some(reader.column()?),
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
    let plan = CompositeGroupAggregatePlanV1 {
        relation: RelationReference {
            schema_name,
            schema_id,
            schema_version,
            relation_name,
            relation_id,
            table_epoch,
        },
        scan_columns,
        group_keys,
        aggregate_kind,
        aggregate_column,
        limit,
    };
    validate_composite_group_aggregate_plan_v1(&plan)?;
    Ok(plan)
}

pub fn encode_filtered_composite_group_aggregate_plan_v1(
    plan: &FilteredCompositeGroupAggregatePlanV1,
) -> Result<Vec<u8>, PlanError> {
    validate_filtered_composite_group_aggregate_plan_v1(plan)?;
    let mut output = Vec::with_capacity(1024);
    output.extend_from_slice(&crate::query_abi::encode_optimizer_plan_header_record(
        1,
        if plan.order_items.is_empty() {
            PLAN_KIND_FILTERED_COMPOSITE_GROUP_AGGREGATE
        } else {
            PLAN_KIND_ORDERED_FILTERED_COMPOSITE_GROUP_AGGREGATE
        },
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
    output.extend_from_slice(&crate::query_abi::encode_optimizer_plan_filter_node_record(
        2,
    ));
    put_predicate(&mut output, &plan.predicate)?;
    output.extend_from_slice(&[
        if plan.order_items.is_empty() {
            FILTERED_COMPOSITE_GROUP_AGGREGATE_NODE
        } else {
            ORDERED_FILTERED_COMPOSITE_GROUP_AGGREGATE_NODE
        },
        plan.aggregate_kind,
        plan.group_keys.len() as u8,
    ]);
    for key in &plan.group_keys {
        put_column(&mut output, key);
    }
    match &plan.aggregate_column {
        None => output.push(0),
        Some(column) => {
            output.push(1);
            put_column(&mut output, column);
        }
    }
    if !plan.order_items.is_empty() {
        output.push(plan.order_items.len() as u8);
        for item in &plan.order_items {
            output.push(item.source);
            if let Some(index) = item.group_key_index {
                output.push(index);
            }
            output.extend_from_slice(&[u8::from(item.descending), u8::from(item.nulls_first)]);
        }
    }
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

pub fn decode_filtered_composite_group_aggregate_plan_v1(
    bytes: &[u8],
) -> Result<FilteredCompositeGroupAggregatePlanV1, PlanError> {
    if bytes.len() > MAX_PLAN_BYTES {
        return Err(PlanError::TooLarge);
    }
    let (version, kind, reserved) =
        crate::query_abi::decode_optimizer_plan_header_record(bytes).ok_or(PlanError::Truncated)?;
    if version != 1 {
        return Err(PlanError::UnsupportedVersion(version));
    }
    if !matches!(
        kind,
        PLAN_KIND_FILTERED_COMPOSITE_GROUP_AGGREGATE
            | PLAN_KIND_ORDERED_FILTERED_COMPOSITE_GROUP_AGGREGATE
    ) || reserved != 0
    {
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
    let scan_count = usize::from(reader.u16()?);
    if !(1..=MAX_PLAN_SCAN_COLUMNS).contains(&scan_count) {
        return Err(PlanError::InvalidColumnReference);
    }
    let mut scan_columns = Vec::with_capacity(scan_count);
    for _ in 0..scan_count {
        scan_columns.push(reader.column()?);
    }
    let filter_tag_offset = reader.offset;
    reader.filter_node_tag(filter_tag_offset)?;
    let predicate = reader.predicate(&scan_columns)?;
    let expected_node = if kind == PLAN_KIND_FILTERED_COMPOSITE_GROUP_AGGREGATE {
        FILTERED_COMPOSITE_GROUP_AGGREGATE_NODE
    } else {
        ORDERED_FILTERED_COMPOSITE_GROUP_AGGREGATE_NODE
    };
    if reader.u8()? != expected_node {
        return Err(PlanError::InvalidPipeline);
    }
    let aggregate_kind = reader.u8()?;
    let key_count = usize::from(reader.u8()?);
    if !(1..=crate::optimizer_request::MAX_GROUP_KEYS).contains(&key_count) {
        return Err(PlanError::InvalidColumnReference);
    }
    let mut group_keys = Vec::with_capacity(key_count);
    for _ in 0..key_count {
        group_keys.push(reader.column()?);
    }
    let aggregate_column = match reader.u8()? {
        0 => None,
        1 => Some(reader.column()?),
        _ => return Err(PlanError::InvalidEncoding),
    };
    let mut order_items = Vec::new();
    if kind == PLAN_KIND_ORDERED_FILTERED_COMPOSITE_GROUP_AGGREGATE {
        let order_count = usize::from(reader.u8()?);
        if !(1..=key_count + 1).contains(&order_count) {
            return Err(PlanError::InvalidColumnReference);
        }
        let mut seen_keys = HashSet::new();
        let mut seen_aggregate = false;
        for _ in 0..order_count {
            let source = reader.u8()?;
            let group_key_index = match source {
                0 => {
                    let index = reader.u8()?;
                    if usize::from(index) >= key_count || !seen_keys.insert(index) {
                        return Err(PlanError::InvalidColumnReference);
                    }
                    Some(index)
                }
                1 if !seen_aggregate => {
                    seen_aggregate = true;
                    None
                }
                _ => return Err(PlanError::InvalidColumnReference),
            };
            let descending = match reader.u8()? {
                0 => false,
                1 => true,
                _ => return Err(PlanError::InvalidEncoding),
            };
            let nulls_first = match reader.u8()? {
                0 => false,
                1 => true,
                _ => return Err(PlanError::InvalidEncoding),
            };
            order_items.push(CompositeGroupCountOrderItem {
                source,
                group_key_index,
                descending,
                nulls_first,
            });
        }
    }
    let limit = match reader.u8()? {
        0 => None,
        1 => Some(reader.u32()?),
        _ => return Err(PlanError::InvalidEncoding),
    };
    if reader.offset != bytes.len() {
        return Err(PlanError::TrailingBytes);
    }
    let plan = FilteredCompositeGroupAggregatePlanV1 {
        relation: RelationReference {
            schema_name,
            schema_id,
            schema_version,
            relation_name,
            relation_id,
            table_epoch,
        },
        scan_columns,
        group_keys,
        aggregate_kind,
        aggregate_column,
        predicate,
        order_items,
        limit,
    };
    validate_filtered_composite_group_aggregate_plan_v1(&plan)?;
    Ok(plan)
}

pub fn plan_ordered_composite_group_aggregate_v1(
    bound: BoundOrderedCompositeGroupAggregate,
) -> Result<OrderedCompositeGroupAggregatePlanV1, PlanError> {
    let aggregate_plan = plan_composite_group_aggregate_v1(BoundCompositeGroupAggregate {
        schema_name: bound.schema_name,
        schema_id: bound.schema_id,
        schema_version: bound.schema_version,
        table_epoch: bound.table_epoch,
        relation_id: bound.relation_id,
        relation_name: bound.relation_name,
        group_keys: bound.group_keys,
        aggregate_kind: bound.aggregate_kind,
        aggregate_column: bound.aggregate_column,
        limit: bound.limit,
    })?;
    let plan = OrderedCompositeGroupAggregatePlanV1 {
        aggregate_plan,
        order_items: bound.order_items,
    };
    validate_ordered_composite_group_aggregate_plan_v1(&plan)?;
    Ok(plan)
}

pub fn validate_ordered_composite_group_aggregate_plan_v1(
    plan: &OrderedCompositeGroupAggregatePlanV1,
) -> Result<(), PlanError> {
    validate_composite_group_aggregate_plan_v1(&plan.aggregate_plan)?;
    if !(1..=9).contains(&plan.order_items.len()) {
        return Err(PlanError::InvalidColumnReference);
    }
    let mut seen_keys = HashSet::new();
    let mut aggregate_seen = false;
    for item in &plan.order_items {
        match (item.source, item.group_key_index) {
            (0, Some(index))
                if usize::from(index) < plan.aggregate_plan.group_keys.len()
                    && seen_keys.insert(index) => {}
            (1, None) if !aggregate_seen => aggregate_seen = true,
            _ => return Err(PlanError::InvalidColumnReference),
        }
    }
    Ok(())
}

pub fn encode_ordered_composite_group_aggregate_plan_v1(
    plan: &OrderedCompositeGroupAggregatePlanV1,
) -> Result<Vec<u8>, PlanError> {
    validate_ordered_composite_group_aggregate_plan_v1(plan)?;
    let mut output = encode_composite_group_aggregate_plan_v1(&plan.aggregate_plan)?;
    let mut reader = PlanReader {
        bytes: &output,
        offset: crate::query_abi::OPTIMIZER_PLAN_HEADER_RECORD_HEADER_BYTES as usize,
    };
    reader.name()?;
    reader.take(crate::query_abi::OPTIMIZER_PLAN_SCHEMA_REFERENCE_RECORD_HEADER_BYTES as usize)?;
    reader.name()?;
    reader
        .take(crate::query_abi::OPTIMIZER_PLAN_RELATION_REFERENCE_RECORD_HEADER_BYTES as usize)?;
    let scan_count = usize::from(reader.u16()?);
    for _ in 0..scan_count {
        reader.column()?;
    }
    let node_offset = reader.offset;
    reader.u8()?;
    reader.u8()?;
    let key_count = usize::from(reader.u8()?);
    for _ in 0..key_count {
        reader.column()?;
    }
    match reader.u8()? {
        0 => {}
        1 => {
            reader.column()?;
        }
        _ => return Err(PlanError::InvalidEncoding),
    }
    let limit_offset = reader.offset;
    output.truncate(limit_offset);
    output.extend_from_slice(&[
        ORDERED_COMPOSITE_GROUP_AGGREGATE_NODE,
        plan.order_items.len() as u8,
    ]);
    for item in &plan.order_items {
        output.push(item.source);
        if let Some(index) = item.group_key_index {
            output.push(index);
        }
        output.extend_from_slice(&[u8::from(item.descending), u8::from(item.nulls_first)]);
    }
    match plan.aggregate_plan.limit {
        None => output.push(0),
        Some(limit) => {
            output.push(1);
            output.extend_from_slice(&limit.to_be_bytes());
        }
    }
    output[2] = PLAN_KIND_ORDERED_COMPOSITE_GROUP_AGGREGATE;
    output[node_offset] = ORDERED_COMPOSITE_GROUP_AGGREGATE_NODE;
    if output.len() > MAX_PLAN_BYTES {
        return Err(PlanError::TooLarge);
    }
    Ok(output)
}

pub fn decode_ordered_composite_group_aggregate_plan_v1(
    bytes: &[u8],
) -> Result<OrderedCompositeGroupAggregatePlanV1, PlanError> {
    if bytes.len() > MAX_PLAN_BYTES {
        return Err(PlanError::TooLarge);
    }
    let (version, kind, reserved) =
        crate::query_abi::decode_optimizer_plan_header_record(bytes).ok_or(PlanError::Truncated)?;
    if version != 1 {
        return Err(PlanError::UnsupportedVersion(version));
    }
    if kind != PLAN_KIND_ORDERED_COMPOSITE_GROUP_AGGREGATE || reserved != 0 {
        return Err(PlanError::InvalidEncoding);
    }
    let mut reader = PlanReader {
        bytes,
        offset: crate::query_abi::OPTIMIZER_PLAN_HEADER_RECORD_HEADER_BYTES as usize,
    };
    reader.name()?;
    reader.take(crate::query_abi::OPTIMIZER_PLAN_SCHEMA_REFERENCE_RECORD_HEADER_BYTES as usize)?;
    reader.name()?;
    reader
        .take(crate::query_abi::OPTIMIZER_PLAN_RELATION_REFERENCE_RECORD_HEADER_BYTES as usize)?;
    let scan_count = usize::from(reader.u16()?);
    if !(crate::optimizer_request::MIN_GROUP_KEYS..=crate::optimizer_request::MAX_GROUP_KEYS + 1)
        .contains(&scan_count)
    {
        return Err(PlanError::InvalidColumnReference);
    }
    for _ in 0..scan_count {
        reader.column()?;
    }
    let node_offset = reader.offset;
    if reader.u8()? != ORDERED_COMPOSITE_GROUP_AGGREGATE_NODE {
        return Err(PlanError::InvalidPipeline);
    }
    let aggregate_kind = reader.u8()?;
    let key_count = usize::from(reader.u8()?);
    if !(crate::optimizer_request::MIN_GROUP_KEYS..=crate::optimizer_request::MAX_GROUP_KEYS)
        .contains(&key_count)
    {
        return Err(PlanError::InvalidColumnReference);
    }
    for _ in 0..key_count {
        reader.column()?;
    }
    match reader.u8()? {
        0 => {}
        1 => {
            reader.column()?;
        }
        _ => return Err(PlanError::InvalidEncoding),
    }
    let base_prefix_end = reader.offset;
    if reader.u8()? != ORDERED_COMPOSITE_GROUP_AGGREGATE_NODE {
        return Err(PlanError::InvalidPipeline);
    }
    let order_count = usize::from(reader.u8()?);
    if !(1..=9).contains(&order_count) {
        return Err(PlanError::InvalidColumnReference);
    }
    let mut order_items = Vec::with_capacity(order_count);
    for _ in 0..order_count {
        let source = reader.u8()?;
        let group_key_index = match source {
            0 => Some(reader.u8()?),
            1 => None,
            _ => return Err(PlanError::InvalidColumnReference),
        };
        let descending = match reader.u8()? {
            0 => false,
            1 => true,
            _ => return Err(PlanError::InvalidEncoding),
        };
        let nulls_first = match reader.u8()? {
            0 => false,
            1 => true,
            _ => return Err(PlanError::InvalidEncoding),
        };
        order_items.push(CompositeGroupCountOrderItem {
            source,
            group_key_index,
            descending,
            nulls_first,
        });
    }
    let limit_offset = reader.offset;
    let limit = match reader.u8()? {
        0 => None,
        1 => Some(reader.u32()?),
        _ => return Err(PlanError::InvalidEncoding),
    };
    if reader.offset != bytes.len() {
        return Err(PlanError::TrailingBytes);
    }
    let mut base_bytes = bytes[..base_prefix_end].to_vec();
    base_bytes.extend_from_slice(&bytes[limit_offset..]);
    base_bytes[2] = PLAN_KIND_COMPOSITE_GROUP_AGGREGATE;
    base_bytes[node_offset] = COMPOSITE_GROUP_AGGREGATE_NODE;
    let aggregate_plan = decode_composite_group_aggregate_plan_v1(&base_bytes)?;
    if aggregate_plan.aggregate_kind != aggregate_kind || aggregate_plan.limit != limit {
        return Err(PlanError::InvalidColumnReference);
    }
    let plan = OrderedCompositeGroupAggregatePlanV1 {
        aggregate_plan,
        order_items,
    };
    validate_ordered_composite_group_aggregate_plan_v1(&plan)?;
    Ok(plan)
}
