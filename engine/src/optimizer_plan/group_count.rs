use super::types::*;
use super::validate::*;
use super::wire::*;
use crate::optimizer_binding::{
    BoundCompositeGroupCount, BoundGroupCount, BoundOrderedCompositeGroupCount,
    BoundOrderedGroupCount,
};
use crate::optimizer_request::{CompositeGroupCountOrderItem, GroupCountOrderItem};
use std::collections::HashSet;

pub fn encode_group_count_plan_v1(plan: &GroupCountPlanV1) -> Result<Vec<u8>, PlanError> {
    validate_group_count_plan_v1(plan)?;
    let mut output = Vec::with_capacity(160);
    output.extend_from_slice(&crate::query_abi::encode_optimizer_plan_header_record(
        1,
        PLAN_KIND_GROUP_COUNT,
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
    output.push(GROUP_COUNT_NODE);
    put_column(&mut output, &plan.group_key);
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

pub fn decode_group_count_plan_v1(bytes: &[u8]) -> Result<GroupCountPlanV1, PlanError> {
    if bytes.len() > MAX_PLAN_BYTES {
        return Err(PlanError::TooLarge);
    }
    let (version, kind, reserved) =
        crate::query_abi::decode_optimizer_plan_header_record(bytes).ok_or(PlanError::Truncated)?;
    if version != 1 {
        return Err(PlanError::UnsupportedVersion(version));
    }
    if kind != PLAN_KIND_GROUP_COUNT || reserved != 0 {
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
    if reader.u8()? != GROUP_COUNT_NODE {
        return Err(PlanError::InvalidPipeline);
    }
    let group_key = reader.column()?;
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
    let plan = GroupCountPlanV1 {
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
        limit,
    };
    validate_group_count_plan_v1(&plan)?;
    Ok(plan)
}

pub fn validate_group_count_plan_v1(plan: &GroupCountPlanV1) -> Result<(), PlanError> {
    validate_relation(&plan.relation)?;
    validate_columns(&plan.scan_columns)?;
    if plan.scan_columns != [plan.group_key.clone()]
        || plan.group_key.column_id == [0; 16]
        || !valid_scalar_type(plan.group_key.scalar_type)
    {
        return Err(PlanError::InvalidColumnReference);
    }
    Ok(())
}

pub fn plan_group_count_v1(bound: BoundGroupCount) -> Result<GroupCountPlanV1, PlanError> {
    let relation = RelationReference {
        schema_name: bound.schema_name,
        schema_id: bound.schema_id,
        schema_version: bound.schema_version,
        relation_name: bound.relation_name,
        relation_id: bound.relation_id,
        table_epoch: bound.table_epoch,
    };
    let plan = GroupCountPlanV1 {
        relation,
        scan_columns: vec![bound.group_key.clone()],
        group_key: bound.group_key,
        limit: bound.limit,
    };
    validate_group_count_plan_v1(&plan)?;
    Ok(plan)
}

pub fn plan_ordered_group_count_v1(
    bound: BoundOrderedGroupCount,
) -> Result<OrderedGroupCountPlanV1, PlanError> {
    let relation = RelationReference {
        schema_name: bound.schema_name,
        schema_id: bound.schema_id,
        schema_version: bound.schema_version,
        relation_name: bound.relation_name,
        relation_id: bound.relation_id,
        table_epoch: bound.table_epoch,
    };
    let plan = OrderedGroupCountPlanV1 {
        relation,
        scan_columns: vec![bound.group_key.clone()],
        group_key: bound.group_key,
        order_items: bound.order_items,
        limit: bound.limit,
    };
    validate_ordered_group_count_plan_v1(&plan)?;
    Ok(plan)
}

pub fn encode_ordered_group_count_plan_v1(
    plan: &OrderedGroupCountPlanV1,
) -> Result<Vec<u8>, PlanError> {
    validate_ordered_group_count_plan_v1(plan)?;
    let mut output = Vec::with_capacity(192);
    output.extend_from_slice(&crate::query_abi::encode_optimizer_plan_header_record(
        1,
        PLAN_KIND_ORDERED_GROUP_COUNT,
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
    output.push(GROUP_COUNT_NODE);
    put_column(&mut output, &plan.group_key);
    output.push(plan.order_items.len() as u8);
    for item in &plan.order_items {
        output.extend_from_slice(&[
            item.source,
            u8::from(item.descending),
            u8::from(item.nulls_first),
        ]);
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

pub fn decode_ordered_group_count_plan_v1(
    bytes: &[u8],
) -> Result<OrderedGroupCountPlanV1, PlanError> {
    if bytes.len() > MAX_PLAN_BYTES {
        return Err(PlanError::TooLarge);
    }
    let (version, kind, reserved) =
        crate::query_abi::decode_optimizer_plan_header_record(bytes).ok_or(PlanError::Truncated)?;
    if version != 1 {
        return Err(PlanError::UnsupportedVersion(version));
    }
    if kind != PLAN_KIND_ORDERED_GROUP_COUNT || reserved != 0 {
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
    if scan_count != 1 {
        return Err(PlanError::InvalidPipeline);
    }
    let scan_columns = vec![reader.column()?];
    if reader.u8()? != GROUP_COUNT_NODE {
        return Err(PlanError::InvalidPipeline);
    }
    let group_key = reader.column()?;
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
    let plan = OrderedGroupCountPlanV1 {
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
        order_items,
        limit,
    };
    validate_ordered_group_count_plan_v1(&plan)?;
    Ok(plan)
}

pub fn validate_ordered_group_count_plan_v1(
    plan: &OrderedGroupCountPlanV1,
) -> Result<(), PlanError> {
    validate_relation(&plan.relation)?;
    validate_columns(&plan.scan_columns)?;
    if plan.scan_columns != [plan.group_key.clone()]
        || plan.group_key.column_id == [0; 16]
        || !valid_scalar_type(plan.group_key.scalar_type)
        || !(1..=2).contains(&plan.order_items.len())
    {
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

pub fn plan_ordered_composite_group_count_v1(
    bound: BoundOrderedCompositeGroupCount,
) -> Result<OrderedCompositeGroupCountPlanV1, PlanError> {
    let relation = RelationReference {
        schema_name: bound.schema_name,
        schema_id: bound.schema_id,
        schema_version: bound.schema_version,
        relation_name: bound.relation_name,
        relation_id: bound.relation_id,
        table_epoch: bound.table_epoch,
    };
    let mut scan_columns = bound.group_keys.clone();
    scan_columns.sort_by_key(|column| column.ordinal);
    let plan = OrderedCompositeGroupCountPlanV1 {
        relation,
        scan_columns,
        group_keys: bound.group_keys,
        order_items: bound.order_items,
        limit: bound.limit,
    };
    validate_ordered_composite_group_count_plan_v1(&plan)?;
    Ok(plan)
}

pub fn encode_ordered_composite_group_count_plan_v1(
    plan: &OrderedCompositeGroupCountPlanV1,
) -> Result<Vec<u8>, PlanError> {
    validate_ordered_composite_group_count_plan_v1(plan)?;
    let mut output = Vec::with_capacity(320);
    output.extend_from_slice(&crate::query_abi::encode_optimizer_plan_header_record(
        1,
        PLAN_KIND_ORDERED_COMPOSITE_GROUP_COUNT,
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
    output.push(ORDERED_COMPOSITE_GROUP_COUNT_NODE);
    output.push(plan.group_keys.len() as u8);
    for column in &plan.group_keys {
        put_column(&mut output, column);
    }
    output.push(plan.order_items.len() as u8);
    for item in &plan.order_items {
        output.extend_from_slice(&[
            item.source,
            item.group_key_index.unwrap_or(u8::MAX),
            u8::from(item.descending),
            u8::from(item.nulls_first),
        ]);
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

pub fn decode_ordered_composite_group_count_plan_v1(
    bytes: &[u8],
) -> Result<OrderedCompositeGroupCountPlanV1, PlanError> {
    if bytes.len() > MAX_PLAN_BYTES {
        return Err(PlanError::TooLarge);
    }
    let (version, kind, reserved) =
        crate::query_abi::decode_optimizer_plan_header_record(bytes).ok_or(PlanError::Truncated)?;
    if version != 1 {
        return Err(PlanError::UnsupportedVersion(version));
    }
    if kind != PLAN_KIND_ORDERED_COMPOSITE_GROUP_COUNT || reserved != 0 {
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
    if !(2..=8).contains(&scan_count) {
        return Err(PlanError::InvalidPipeline);
    }
    let mut scan_columns = Vec::with_capacity(scan_count);
    for _ in 0..scan_count {
        scan_columns.push(reader.column()?);
    }
    if reader.u8()? != ORDERED_COMPOSITE_GROUP_COUNT_NODE {
        return Err(PlanError::InvalidPipeline);
    }
    let group_count = usize::from(reader.u8()?);
    if !(2..=8).contains(&group_count) {
        return Err(PlanError::InvalidColumnReference);
    }
    let mut group_keys = Vec::with_capacity(group_count);
    for _ in 0..group_count {
        group_keys.push(reader.column()?);
    }
    let order_count = usize::from(reader.u8()?);
    if !(1..=9).contains(&order_count) {
        return Err(PlanError::InvalidColumnReference);
    }
    let mut order_items = Vec::with_capacity(order_count);
    for _ in 0..order_count {
        let source = reader.u8()?;
        let raw_index = reader.u8()?;
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
            group_key_index: (raw_index != u8::MAX).then_some(raw_index),
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
    let plan = OrderedCompositeGroupCountPlanV1 {
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
        order_items,
        limit,
    };
    validate_ordered_composite_group_count_plan_v1(&plan)?;
    Ok(plan)
}

pub fn validate_ordered_composite_group_count_plan_v1(
    plan: &OrderedCompositeGroupCountPlanV1,
) -> Result<(), PlanError> {
    validate_relation(&plan.relation)?;
    validate_columns(&plan.scan_columns)?;
    if !(2..=8).contains(&plan.group_keys.len())
        || plan.scan_columns.len() != plan.group_keys.len()
        || !(1..=9).contains(&plan.order_items.len())
    {
        return Err(PlanError::InvalidColumnReference);
    }
    let mut sorted = plan.group_keys.clone();
    sorted.sort_by_key(|column| column.ordinal);
    if sorted != plan.scan_columns
        || plan
            .group_keys
            .iter()
            .any(|column| column.column_id == [0; 16] || !valid_scalar_type(column.scalar_type))
    {
        return Err(PlanError::InvalidColumnReference);
    }
    let mut seen_keys = HashSet::new();
    let mut count_seen = false;
    for item in &plan.order_items {
        match (item.source, item.group_key_index) {
            (0, Some(index))
                if usize::from(index) < plan.group_keys.len() && seen_keys.insert(index) => {}
            (1, None) if !count_seen => count_seen = true,
            _ => return Err(PlanError::InvalidColumnReference),
        }
    }
    Ok(())
}

pub fn encode_composite_group_count_plan_v1(
    plan: &CompositeGroupCountPlanV1,
) -> Result<Vec<u8>, PlanError> {
    validate_composite_group_count_plan_v1(plan)?;
    let mut output = Vec::with_capacity(256);
    output.extend_from_slice(&crate::query_abi::encode_optimizer_plan_header_record(
        1,
        PLAN_KIND_COMPOSITE_GROUP_COUNT,
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
    output.push(COMPOSITE_GROUP_COUNT_NODE);
    output.push(plan.group_keys.len() as u8);
    for key in &plan.group_keys {
        put_column(&mut output, key);
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

pub fn decode_composite_group_count_plan_v1(
    bytes: &[u8],
) -> Result<CompositeGroupCountPlanV1, PlanError> {
    if bytes.len() > MAX_PLAN_BYTES {
        return Err(PlanError::TooLarge);
    }
    let (version, kind, reserved) =
        crate::query_abi::decode_optimizer_plan_header_record(bytes).ok_or(PlanError::Truncated)?;
    if version != 1 {
        return Err(PlanError::UnsupportedVersion(version));
    }
    if kind != PLAN_KIND_COMPOSITE_GROUP_COUNT || reserved != 0 {
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
    if !(crate::optimizer_request::MIN_GROUP_KEYS..=crate::optimizer_request::MAX_GROUP_KEYS)
        .contains(&scan_count)
    {
        return Err(PlanError::InvalidColumnReference);
    }
    let mut scan_columns = Vec::with_capacity(scan_count);
    for _ in 0..scan_count {
        scan_columns.push(reader.column()?);
    }
    if reader.u8()? != COMPOSITE_GROUP_COUNT_NODE {
        return Err(PlanError::InvalidPipeline);
    }
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
    let plan = CompositeGroupCountPlanV1 {
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
        limit,
    };
    validate_composite_group_count_plan_v1(&plan)?;
    Ok(plan)
}

pub fn validate_composite_group_count_plan_v1(
    plan: &CompositeGroupCountPlanV1,
) -> Result<(), PlanError> {
    validate_relation(&plan.relation)?;
    validate_columns(&plan.scan_columns)?;
    if !(crate::optimizer_request::MIN_GROUP_KEYS..=crate::optimizer_request::MAX_GROUP_KEYS)
        .contains(&plan.group_keys.len())
        || plan
            .group_keys
            .iter()
            .any(|key| key.column_id == [0; 16] || !valid_scalar_type(key.scalar_type))
    {
        return Err(PlanError::InvalidColumnReference);
    }
    let mut unique = std::collections::HashSet::with_capacity(plan.group_keys.len());
    if plan
        .group_keys
        .iter()
        .any(|key| !unique.insert(key.ordinal) || !plan.scan_columns.iter().any(|scan| scan == key))
        || unique.len() != plan.scan_columns.len()
    {
        return Err(PlanError::InvalidColumnReference);
    }
    Ok(())
}

pub fn plan_composite_group_count_v1(
    bound: BoundCompositeGroupCount,
) -> Result<CompositeGroupCountPlanV1, PlanError> {
    let relation = RelationReference {
        schema_name: bound.schema_name,
        schema_id: bound.schema_id,
        schema_version: bound.schema_version,
        table_epoch: bound.table_epoch,
        relation_id: bound.relation_id,
        relation_name: bound.relation_name,
    };
    let mut scan_columns = bound.group_keys.clone();
    scan_columns.sort_by_key(|column| column.ordinal);
    let plan = CompositeGroupCountPlanV1 {
        relation,
        scan_columns,
        group_keys: bound.group_keys,
        limit: bound.limit,
    };
    validate_composite_group_count_plan_v1(&plan)?;
    Ok(plan)
}
