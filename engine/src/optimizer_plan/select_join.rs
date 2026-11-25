use super::types::*;
use super::validate::*;
use super::wire::*;
use crate::optimizer_binding::{
    BoundColumn, BoundJoinFilter, BoundJoinSelect, BoundPredicate, BoundSelect,
};
use crate::optimizer_request::JoinRelationSide;
use crate::tuple::ScalarType;

/// Encode a validated SELECT plan as canonical version-1 bytes suitable for
/// transfer over a bounded optimizer/executor channel.
pub fn encode_plan_v1(plan: &SingleTablePlanV1) -> Result<Vec<u8>, PlanError> {
    validate_plan_v1(plan)?;
    let mut output = Vec::with_capacity(256);
    output.extend_from_slice(&crate::query_abi::encode_optimizer_plan_header_record(
        1,
        PLAN_KIND_SELECT,
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
        u8::try_from(plan.nodes.len()).map_err(|_| PlanError::InvalidPipeline)?,
    ));
    for node in &plan.nodes {
        match node {
            PlanNode::Scan { columns } => {
                put_column_list_header(&mut output, 1, columns.len())?;
                for column in columns {
                    put_column(&mut output, column);
                }
            }
            PlanNode::Filter(predicate) => {
                output.extend_from_slice(
                    &crate::query_abi::encode_optimizer_plan_filter_node_record(2),
                );
                put_predicate(&mut output, predicate)?;
            }
            PlanNode::Project(columns) => {
                put_column_list_header(&mut output, 3, columns.len())?;
                for column in columns {
                    put_column(&mut output, column);
                }
            }
            PlanNode::Limit(limit) => {
                output.extend_from_slice(
                    &crate::query_abi::encode_optimizer_plan_limit_node_record(4, *limit),
                );
            }
        }
        if output.len() > MAX_PLAN_BYTES {
            return Err(PlanError::TooLarge);
        }
    }
    let hint_count = usize::from(plan.unique_probe.is_some())
        + usize::from(plan.range_scan.is_some())
        + usize::from(plan.multi_point_probe.is_some());
    if hint_count > 1 {
        return Err(PlanError::InvalidPredicate);
    }
    if let Some(probe) = &plan.unique_probe {
        output.push(0xa1);
        output.extend_from_slice(&probe.index_id);
        put_column(&mut output, &probe.key_column);
        put_predicate(&mut output, &probe.value)?;
    } else if let Some(range) = &plan.range_scan {
        output.push(0xa2);
        output.extend_from_slice(&range.index_id);
        put_column(&mut output, &range.key_column);
        for bound in [&range.lower, &range.upper] {
            match bound {
                None => output.push(0),
                Some(bound) => {
                    output.push(if bound.inclusive { 1 } else { 2 });
                    put_predicate(&mut output, &bound.value)?;
                }
            }
        }
    } else if let Some(probe) = &plan.multi_point_probe {
        output.push(0xa4);
        output.extend_from_slice(&probe.index_id);
        put_column(&mut output, &probe.key_column);
        output.extend_from_slice(
            &u16::try_from(probe.values.len())
                .map_err(|_| PlanError::InvalidPredicate)?
                .to_be_bytes(),
        );
        for value in &probe.values {
            put_predicate(&mut output, value)?;
        }
    }
    if plan.empty_view_hint {
        if plan.unique_probe.is_some()
            || plan.range_scan.is_some()
            || plan.multi_point_probe.is_some()
        {
            return Err(PlanError::InvalidPredicate);
        }
        output.push(0xa3);
    }
    if output.len() > MAX_PLAN_BYTES {
        return Err(PlanError::TooLarge);
    }
    Ok(output)
}

/// Decode and independently validate a canonical optimizer v1 SELECT plan.
/// The executor must still resolve the logical relation through its own
/// invocation-bound storage connection before using it.
pub fn decode_plan_v1(bytes: &[u8]) -> Result<SingleTablePlanV1, PlanError> {
    if bytes.len() > MAX_PLAN_BYTES {
        return Err(PlanError::TooLarge);
    }
    let mut reader = PlanReader { bytes, offset: 0 };
    let (version, kind, reserved) =
        crate::query_abi::decode_optimizer_plan_header_record(bytes).ok_or(PlanError::Truncated)?;
    if version != 1 {
        return Err(PlanError::UnsupportedVersion(version));
    }
    reader.offset = crate::query_abi::OPTIMIZER_PLAN_HEADER_RECORD_HEADER_BYTES as usize;
    if kind != PLAN_KIND_SELECT || reserved != 0 {
        return Err(PlanError::InvalidEncoding);
    }
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
    let node_count = usize::from(reader.node_count()?);
    if !(2..=4).contains(&node_count) {
        return Err(PlanError::InvalidPipeline);
    }
    let mut nodes = Vec::with_capacity(node_count);
    let mut scan_columns = Vec::new();
    for index in 0..node_count {
        let tag_offset = reader.offset;
        let tag = reader.u8()?;
        let node = match tag {
            1 if index == 0 => {
                let count = reader.column_list_count(tag_offset, 1, MAX_PLAN_SCAN_COLUMNS)?;
                if count == 0 {
                    return Err(PlanError::InvalidColumnReference);
                }
                let mut columns = Vec::with_capacity(count);
                for _ in 0..count {
                    columns.push(reader.column()?);
                }
                scan_columns = columns.clone();
                PlanNode::Scan { columns }
            }
            2 => {
                reader.filter_node_tag(tag_offset)?;
                PlanNode::Filter(reader.predicate(&scan_columns)?)
            }
            3 => {
                let count = reader.column_list_count(
                    tag_offset,
                    3,
                    crate::optimizer_request::MAX_PROJECTIONS,
                )?;
                let mut columns = Vec::with_capacity(count);
                for _ in 0..count {
                    columns.push(reader.column()?);
                }
                PlanNode::Project(columns)
            }
            4 => PlanNode::Limit(reader.limit_node_value(tag_offset)?),
            _ => return Err(PlanError::InvalidPipeline),
        };
        nodes.push(node);
    }
    let mut empty_view_hint = false;
    let (unique_probe, range_scan, multi_point_probe) =
        if reader.is_empty() || reader.bytes.get(reader.offset) == Some(&0xa3) {
            (None, None, None)
        } else {
            let hint_tag = reader.u8()?;
            if !matches!(hint_tag, 0xa1 | 0xa2 | 0xa4) {
                return Err(PlanError::TrailingBytes);
            }
            let index_id: [u8; 16] = reader
                .take(16)?
                .try_into()
                .map_err(|_| PlanError::Truncated)?;
            let key_column = reader.column()?;
            match hint_tag {
                0xa1 => {
                    let value = reader.predicate(&scan_columns)?;
                    (
                        Some(UniquePointProbeV1 {
                            index_id,
                            key_column,
                            value,
                        }),
                        None,
                        None,
                    )
                }
                0xa2 => {
                    let mut decode_bound = || -> Result<Option<IndexRangeBoundV1>, PlanError> {
                        match reader.u8()? {
                            0 => Ok(None),
                            1 | 2 => {
                                let inclusive = reader.bytes[reader.offset - 1] == 1;
                                Ok(Some(IndexRangeBoundV1 {
                                    value: reader.predicate(&scan_columns)?,
                                    inclusive,
                                }))
                            }
                            _ => Err(PlanError::InvalidEncoding),
                        }
                    };
                    (
                        None,
                        Some(IndexRangeScanV1 {
                            index_id,
                            key_column,
                            lower: decode_bound()?,
                            upper: decode_bound()?,
                        }),
                        None,
                    )
                }
                0xa4 => {
                    let count = usize::from(reader.u16()?);
                    if !(1..=64).contains(&count) {
                        return Err(PlanError::InvalidPredicate);
                    }
                    let mut values = Vec::with_capacity(count);
                    for _ in 0..count {
                        values.push(reader.predicate(&scan_columns)?);
                    }
                    (
                        None,
                        None,
                        Some(MultiPointProbeV1 {
                            index_id,
                            key_column,
                            values,
                        }),
                    )
                }
                _ => return Err(PlanError::TrailingBytes),
            }
        };
    if !reader.is_empty() && reader.bytes.get(reader.offset) == Some(&0xa3) {
        if unique_probe.is_some() || range_scan.is_some() || multi_point_probe.is_some() {
            return Err(PlanError::InvalidEncoding);
        }
        reader.offset += 1;
        empty_view_hint = true;
    }
    if !reader.is_empty() {
        return Err(PlanError::TrailingBytes);
    }
    let plan = SingleTablePlanV1 {
        relation,
        nodes,
        unique_probe,
        range_scan,
        multi_point_probe,
        empty_view_hint,
    };
    validate_plan_v1(&plan)?;
    Ok(plan)
}

/// Encode an independently validated two-relation join plan. Unfiltered joins
/// use kind 4, filtered joins use kind 12, and authorized index nested-loop
/// joins use the additive kind 17.
pub fn encode_join_select_plan_v1(plan: &JoinSelectPlanV1) -> Result<Vec<u8>, PlanError> {
    validate_join_select_plan_v1(plan)?;
    let mut output = Vec::with_capacity(256);
    output.extend_from_slice(&crate::query_abi::encode_optimizer_plan_header_record(
        1,
        if let Some(index_join) = &plan.index_nested_loop {
            if index_join.prefix_columns.len() > 1 {
                PLAN_KIND_INDEX_PREFIX_NESTED_LOOP_JOIN
            } else {
                PLAN_KIND_INDEX_NESTED_LOOP_JOIN
            }
        } else if plan.filter.is_some() {
            PLAN_KIND_FILTERED_JOIN_SELECT
        } else {
            PLAN_KIND_JOIN_SELECT
        },
        0,
    ));
    put_name(&mut output, &plan.schema_name)?;
    output.extend_from_slice(
        &crate::query_abi::encode_optimizer_plan_schema_reference_record(
            plan.schema_id,
            plan.schema_version,
        ),
    );
    for relation in [&plan.left_relation, &plan.right_relation] {
        put_name(&mut output, &relation.relation_name)?;
        output.extend_from_slice(
            &crate::query_abi::encode_optimizer_plan_relation_reference_record(
                relation.relation_id,
                relation.table_epoch,
            ),
        );
    }
    for columns in [&plan.left_scan_columns, &plan.right_scan_columns] {
        output.extend_from_slice(&(columns.len() as u16).to_be_bytes());
        for column in columns {
            put_column(&mut output, column);
        }
    }
    output.extend_from_slice(&(plan.projections.len() as u16).to_be_bytes());
    for (side, column) in &plan.projections {
        output.push(match side {
            JoinRelationSide::Left => 0,
            JoinRelationSide::Right => 1,
        });
        put_column(&mut output, column);
    }
    output.push(plan.join_keys.len() as u8);
    for (left, right) in &plan.join_keys {
        put_column(&mut output, left);
        put_column(&mut output, right);
    }
    match plan.limit {
        None => output.push(0),
        Some(limit) => {
            output.push(1);
            output.extend_from_slice(&limit.to_be_bytes());
        }
    }
    if plan.index_nested_loop.is_some() {
        output.push(u8::from(plan.filter.is_some()));
    }
    if let Some(filter) = &plan.filter {
        output.push(match filter.relation_side {
            JoinRelationSide::Left => 0,
            JoinRelationSide::Right => 1,
        });
        put_column(&mut output, &filter.column);
        match filter.expression {
            crate::optimizer_request::JoinFilterExpression::Parameter { operator, index } => {
                output.push(0);
                output.push(match operator {
                    crate::optimizer_request::Comparison::Equal => 0,
                    crate::optimizer_request::Comparison::NotEqual => 1,
                    crate::optimizer_request::Comparison::Less => 2,
                    crate::optimizer_request::Comparison::LessOrEqual => 3,
                    crate::optimizer_request::Comparison::Greater => 4,
                    crate::optimizer_request::Comparison::GreaterOrEqual => 5,
                });
                output.extend_from_slice(&index.to_be_bytes());
            }
            crate::optimizer_request::JoinFilterExpression::IsNull { negated } => {
                output.push(if negated { 2 } else { 1 });
            }
        }
    }
    if let Some(index_join) = &plan.index_nested_loop {
        output.push(match index_join.outer_side {
            JoinRelationSide::Left => 0,
            JoinRelationSide::Right => 1,
        });
        output.push(match index_join.inner_side {
            JoinRelationSide::Left => 0,
            JoinRelationSide::Right => 1,
        });
        output.extend_from_slice(&index_join.index_id);
        if index_join.prefix_columns.len() > 1 {
            output.push(index_join.prefix_columns.len() as u8);
            for column in &index_join.prefix_columns {
                put_column(&mut output, column);
            }
        } else {
            put_column(&mut output, &index_join.key_column);
        }
    }
    if output.len() > MAX_PLAN_BYTES {
        return Err(PlanError::TooLarge);
    }
    Ok(output)
}

/// Decode and independently validate canonical plan kind 4.
pub fn decode_join_select_plan_v1(bytes: &[u8]) -> Result<JoinSelectPlanV1, PlanError> {
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
        PLAN_KIND_JOIN_SELECT
            | PLAN_KIND_FILTERED_JOIN_SELECT
            | PLAN_KIND_INDEX_NESTED_LOOP_JOIN
            | PLAN_KIND_INDEX_PREFIX_NESTED_LOOP_JOIN
    ) || reserved != 0
    {
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
    let mut relation_references = Vec::with_capacity(2);
    for _ in 0..2 {
        let relation_name = reader.name()?;
        let reference = reader.take(
            crate::query_abi::OPTIMIZER_PLAN_RELATION_REFERENCE_RECORD_HEADER_BYTES as usize,
        )?;
        let (relation_id, table_epoch) =
            crate::query_abi::decode_optimizer_plan_relation_reference_record(reference)
                .ok_or(PlanError::Truncated)?;
        relation_references.push(RelationReference {
            schema_name: schema_name.clone(),
            schema_id,
            schema_version,
            relation_name,
            relation_id,
            table_epoch,
        });
    }
    let mut scan_shapes = Vec::with_capacity(2);
    for _ in 0..2 {
        let count = usize::from(u16::from_be_bytes(
            reader
                .take(2)?
                .try_into()
                .map_err(|_| PlanError::Truncated)?,
        ));
        if count == 0 || count > MAX_PLAN_SCAN_COLUMNS {
            return Err(PlanError::InvalidColumnReference);
        }
        let mut columns = Vec::with_capacity(count);
        for _ in 0..count {
            columns.push(reader.column()?);
        }
        scan_shapes.push(columns);
    }
    let projection_count = usize::from(u16::from_be_bytes(
        reader
            .take(2)?
            .try_into()
            .map_err(|_| PlanError::Truncated)?,
    ));
    if !(1..=crate::optimizer_request::MAX_PROJECTIONS).contains(&projection_count) {
        return Err(PlanError::EmptyProjection);
    }
    let mut projections = Vec::with_capacity(projection_count);
    for _ in 0..projection_count {
        let side = match reader.u8()? {
            0 => JoinRelationSide::Left,
            1 => JoinRelationSide::Right,
            _ => return Err(PlanError::InvalidEncoding),
        };
        projections.push((side, reader.column()?));
    }
    let key_count = usize::from(reader.u8()?);
    if !(2..=8).contains(&key_count) {
        return Err(PlanError::InvalidColumnReference);
    }
    let mut join_keys = Vec::with_capacity(key_count);
    for _ in 0..key_count {
        join_keys.push((reader.column()?, reader.column()?));
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
    let has_filter = if matches!(
        kind,
        PLAN_KIND_INDEX_NESTED_LOOP_JOIN | PLAN_KIND_INDEX_PREFIX_NESTED_LOOP_JOIN
    ) {
        match reader.u8()? {
            0 => false,
            1 => true,
            _ => return Err(PlanError::InvalidEncoding),
        }
    } else {
        kind == PLAN_KIND_FILTERED_JOIN_SELECT
    };
    let filter = if has_filter {
        let relation_side = match reader.u8()? {
            0 => JoinRelationSide::Left,
            1 => JoinRelationSide::Right,
            _ => return Err(PlanError::InvalidEncoding),
        };
        let column = reader.column()?;
        let expression = match reader.u8()? {
            0 => {
                let operator = match reader.u8()? {
                    0 => crate::optimizer_request::Comparison::Equal,
                    1 => crate::optimizer_request::Comparison::NotEqual,
                    2 => crate::optimizer_request::Comparison::Less,
                    3 => crate::optimizer_request::Comparison::LessOrEqual,
                    4 => crate::optimizer_request::Comparison::Greater,
                    5 => crate::optimizer_request::Comparison::GreaterOrEqual,
                    _ => return Err(PlanError::InvalidEncoding),
                };
                let index = u32::from_be_bytes(
                    reader
                        .take(4)?
                        .try_into()
                        .map_err(|_| PlanError::Truncated)?,
                );
                if index == 0 || index > crate::optimizer_request::MAX_PARAMETER_INDEX {
                    return Err(PlanError::InvalidEncoding);
                }
                crate::optimizer_request::JoinFilterExpression::Parameter { operator, index }
            }
            1 => crate::optimizer_request::JoinFilterExpression::IsNull { negated: false },
            2 => crate::optimizer_request::JoinFilterExpression::IsNull { negated: true },
            _ => return Err(PlanError::InvalidEncoding),
        };
        Some(BoundJoinFilter {
            relation_side,
            column,
            expression,
        })
    } else {
        None
    };
    let index_nested_loop = if matches!(
        kind,
        PLAN_KIND_INDEX_NESTED_LOOP_JOIN | PLAN_KIND_INDEX_PREFIX_NESTED_LOOP_JOIN
    ) {
        let outer_side = match reader.u8()? {
            0 => JoinRelationSide::Left,
            1 => JoinRelationSide::Right,
            _ => return Err(PlanError::InvalidEncoding),
        };
        let inner_side = match reader.u8()? {
            0 => JoinRelationSide::Left,
            1 => JoinRelationSide::Right,
            _ => return Err(PlanError::InvalidEncoding),
        };
        let index_id = reader
            .take(16)?
            .try_into()
            .map_err(|_| PlanError::Truncated)?;
        let prefix_columns = if kind == PLAN_KIND_INDEX_PREFIX_NESTED_LOOP_JOIN {
            let count = usize::from(reader.u8()?);
            if !(2..=8).contains(&count) {
                return Err(PlanError::InvalidColumnReference);
            }
            let mut columns = Vec::with_capacity(count);
            for _ in 0..count {
                columns.push(reader.column()?);
            }
            columns
        } else {
            Vec::new()
        };
        let key_column = if prefix_columns.is_empty() {
            reader.column()?
        } else {
            prefix_columns[0].clone()
        };
        Some(IndexNestedLoopJoinV1 {
            outer_side,
            inner_side,
            index_id,
            key_column,
            prefix_columns,
        })
    } else {
        None
    };
    if reader.offset != bytes.len() {
        return Err(PlanError::TrailingBytes);
    }
    let plan = JoinSelectPlanV1 {
        schema_name,
        schema_id,
        schema_version,
        left_relation: relation_references.remove(0),
        right_relation: relation_references.remove(0),
        left_scan_columns: scan_shapes.remove(0),
        right_scan_columns: scan_shapes.remove(0),
        projections,
        join_keys,
        limit,
        filter,
        index_nested_loop,
    };
    validate_join_select_plan_v1(&plan)?;
    Ok(plan)
}

/// Check relation identity, key shape/type compatibility, projection
/// membership, and all collection bounds before an executor interprets a
/// decoded join plan.
pub fn validate_join_select_plan_v1(plan: &JoinSelectPlanV1) -> Result<(), PlanError> {
    if plan.schema_id == [0; 16]
        || plan.schema_version == 0
        || !valid_identifier(&plan.schema_name)
        || plan.left_relation.schema_name != plan.schema_name
        || plan.right_relation.schema_name != plan.schema_name
        || plan.left_relation.schema_id != plan.schema_id
        || plan.right_relation.schema_id != plan.schema_id
        || plan.left_relation.schema_version != plan.schema_version
        || plan.right_relation.schema_version != plan.schema_version
    {
        return Err(PlanError::InvalidRelationReference);
    }
    validate_relation(&plan.left_relation)?;
    validate_relation(&plan.right_relation)?;
    if plan.projections.is_empty() {
        return Err(PlanError::EmptyProjection);
    }
    if plan.projections.len() > crate::optimizer_request::MAX_PROJECTIONS
        || !(2..=8).contains(&plan.join_keys.len())
    {
        return Err(PlanError::InvalidColumnReference);
    }
    validate_columns(&plan.left_scan_columns)?;
    validate_columns(&plan.right_scan_columns)?;
    for (side, column) in &plan.projections {
        if !valid_scalar_type(column.scalar_type) || column.column_id == [0; 16] {
            return Err(PlanError::InvalidScalarType);
        }
        let scan_columns = match side {
            JoinRelationSide::Left => &plan.left_scan_columns,
            JoinRelationSide::Right => &plan.right_scan_columns,
        };
        if !scan_columns.contains(column) {
            return Err(PlanError::InvalidColumnReference);
        }
    }
    for (left, right) in &plan.join_keys {
        if left.column_id == [0; 16]
            || right.column_id == [0; 16]
            || !valid_scalar_type(left.scalar_type)
            || !valid_scalar_type(right.scalar_type)
            || !join_key_types_compatible(left.scalar_type, right.scalar_type)
        {
            return Err(PlanError::InvalidColumnReference);
        }
        for (side, key) in [
            (JoinRelationSide::Left, left),
            (JoinRelationSide::Right, right),
        ] {
            let scan_columns = match side {
                JoinRelationSide::Left => &plan.left_scan_columns,
                JoinRelationSide::Right => &plan.right_scan_columns,
            };
            if !scan_columns.contains(key) {
                return Err(PlanError::InvalidColumnReference);
            }
        }
    }
    if let Some(filter) = &plan.filter {
        let scan_columns = match filter.relation_side {
            JoinRelationSide::Left => &plan.left_scan_columns,
            JoinRelationSide::Right => &plan.right_scan_columns,
        };
        if filter.column.column_id == [0; 16]
            || !valid_scalar_type(filter.column.scalar_type)
            || !scan_columns.contains(&filter.column)
        {
            return Err(PlanError::InvalidColumnReference);
        }
        if matches!(
            filter.expression,
            crate::optimizer_request::JoinFilterExpression::Parameter { index, .. }
            if index == 0 || index > crate::optimizer_request::MAX_PARAMETER_INDEX
        ) {
            return Err(PlanError::InvalidEncoding);
        }
    }
    if let Some(index_join) = &plan.index_nested_loop {
        if index_join.outer_side == index_join.inner_side
            || index_join.index_id == [0; 16]
            || !is_supported_index_join_key(&index_join.key_column)
        {
            return Err(PlanError::InvalidColumnReference);
        }
        let (inner_scan, matching_key) = match index_join.inner_side {
            JoinRelationSide::Left => (
                &plan.left_scan_columns,
                plan.join_keys
                    .iter()
                    .find(|(inner, _)| inner == &index_join.key_column),
            ),
            JoinRelationSide::Right => (
                &plan.right_scan_columns,
                plan.join_keys
                    .iter()
                    .find(|(_, inner)| inner == &index_join.key_column),
            ),
        };
        if !inner_scan.contains(&index_join.key_column) || matching_key.is_none() {
            return Err(PlanError::InvalidColumnReference);
        }
        let expected_outer = match index_join.inner_side {
            JoinRelationSide::Left => JoinRelationSide::Right,
            JoinRelationSide::Right => JoinRelationSide::Left,
        };
        if index_join.outer_side != expected_outer {
            return Err(PlanError::InvalidColumnReference);
        }
        if index_join.prefix_columns.len() > 8
            || (!index_join.prefix_columns.is_empty()
                && (index_join.prefix_columns.len() < 2
                    || index_join.prefix_columns.first() != Some(&index_join.key_column)))
        {
            return Err(PlanError::InvalidColumnReference);
        }
        let prefix_columns = if index_join.prefix_columns.is_empty() {
            std::slice::from_ref(&index_join.key_column)
        } else {
            &index_join.prefix_columns
        };
        let mut seen_prefix_columns = std::collections::HashSet::new();
        for column in prefix_columns {
            if !inner_scan.contains(column)
                || !is_supported_index_join_key(column)
                || !seen_prefix_columns.insert(column.column_id)
            {
                return Err(PlanError::InvalidColumnReference);
            }
            let paired_key = match index_join.inner_side {
                JoinRelationSide::Left => plan.join_keys.iter().find(|(inner, _)| inner == column),
                JoinRelationSide::Right => plan.join_keys.iter().find(|(_, inner)| inner == column),
            };
            if paired_key.is_none() {
                return Err(PlanError::InvalidColumnReference);
            }
        }
    }
    Ok(())
}

/// Lower a bound request to a conservative scan/filter/project/limit
/// pipeline. Scan columns are the unique union of projected and predicate
/// columns, ordered by catalog ordinal.
pub fn plan_select_v1(bound: BoundSelect) -> Result<SingleTablePlanV1, PlanError> {
    if bound.projections.is_empty() {
        return Err(PlanError::EmptyProjection);
    }
    let relation = RelationReference {
        schema_name: bound.schema_name,
        schema_id: bound.schema_id,
        schema_version: bound.schema_version,
        relation_name: bound.relation_name,
        relation_id: bound.relation_id,
        table_epoch: bound.table_epoch,
    };
    validate_relation(&relation)?;
    let unique_probe = bound
        .predicate
        .as_ref()
        .and_then(|predicate| select_unique_point_probe(predicate, &bound.unique_access_paths));
    let range_scan = if unique_probe.is_none() {
        bound.predicate.as_ref().and_then(|predicate| {
            select_index_range_scan(
                predicate,
                &bound.unique_access_paths,
                &bound.range_access_paths,
            )
        })
    } else {
        None
    };
    let multi_point_probe = if unique_probe.is_none() && range_scan.is_none() {
        bound.predicate.as_ref().and_then(|predicate| {
            select_multi_point_probe(
                predicate,
                &bound.unique_access_paths,
                &bound.range_access_paths,
            )
        })
    } else {
        None
    };

    let mut scan_columns = Vec::with_capacity(bound.projections.len());
    for column in &bound.projections {
        add_column(&mut scan_columns, column)?;
    }
    if let Some(predicate) = &bound.predicate {
        collect_predicate_columns(predicate, &mut scan_columns)?;
    }
    scan_columns.sort_by_key(|column| column.ordinal);

    let mut nodes = Vec::with_capacity(4);
    nodes.push(PlanNode::Scan {
        columns: scan_columns,
    });
    if let Some(predicate) = bound.predicate {
        nodes.push(PlanNode::Filter(predicate));
    }
    nodes.push(PlanNode::Project(bound.projections));
    if let Some(limit) = bound.limit {
        nodes.push(PlanNode::Limit(limit));
    }
    let plan = SingleTablePlanV1 {
        relation,
        nodes,
        unique_probe,
        range_scan,
        multi_point_probe,
        empty_view_hint: false,
    };
    validate_plan_v1(&plan)?;
    Ok(plan)
}

pub(super) fn select_index_range_scan(
    predicate: &BoundPredicate,
    unique_paths: &[crate::metadata_discovery::AuthorizedUniqueAccessPath],
    range_paths: &[crate::metadata_discovery::AuthorizedRangeAccessPath],
) -> Option<IndexRangeScanV1> {
    let (key_column, lower, upper) = index_range_bounds(predicate)?;
    let index_id = unique_paths
        .iter()
        .map(|path| (&path.index_id, &path.column_ids))
        .chain(
            range_paths
                .iter()
                .map(|path| (&path.index_id, &path.column_ids)),
        )
        .find(|(index_id, column_ids)| {
            **index_id != [0; 16] && column_ids.first() == Some(&key_column.column_id)
        })
        .map(|(index_id, _)| *index_id)?;
    Some(IndexRangeScanV1 {
        index_id,
        key_column,
        lower,
        upper,
    })
}

pub(crate) fn index_range_bounds(
    predicate: &BoundPredicate,
) -> Option<(
    BoundColumn,
    Option<IndexRangeBoundV1>,
    Option<IndexRangeBoundV1>,
)> {
    match predicate {
        BoundPredicate::Compare { .. } => index_range_comparison(predicate),
        BoundPredicate::And(children) if children.len() == 2 => {
            let (left_column, left_lower, left_upper) = index_range_comparison(&children[0])?;
            let (right_column, right_lower, right_upper) = index_range_comparison(&children[1])?;
            if left_column != right_column
                || left_lower.is_some() == right_lower.is_some()
                || left_upper.is_some() == right_upper.is_some()
            {
                return None;
            }
            Some((
                left_column,
                left_lower.or(right_lower),
                left_upper.or(right_upper),
            ))
        }
        _ => None,
    }
}

fn index_range_comparison(
    predicate: &BoundPredicate,
) -> Option<(
    BoundColumn,
    Option<IndexRangeBoundV1>,
    Option<IndexRangeBoundV1>,
)> {
    let BoundPredicate::Compare {
        operator,
        left,
        right,
    } = predicate
    else {
        return None;
    };
    let (column, value, operator) = match (left.as_ref(), right.as_ref()) {
        (BoundPredicate::Column(column), value) if is_point_probe_literal(column, value) => {
            (column.clone(), value, *operator)
        }
        (value, BoundPredicate::Column(column)) if is_point_probe_literal(column, value) => {
            let reverse = match operator {
                crate::optimizer_request::Comparison::Equal => {
                    crate::optimizer_request::Comparison::Equal
                }
                crate::optimizer_request::Comparison::Less => {
                    crate::optimizer_request::Comparison::Greater
                }
                crate::optimizer_request::Comparison::LessOrEqual => {
                    crate::optimizer_request::Comparison::GreaterOrEqual
                }
                crate::optimizer_request::Comparison::Greater => {
                    crate::optimizer_request::Comparison::Less
                }
                crate::optimizer_request::Comparison::GreaterOrEqual => {
                    crate::optimizer_request::Comparison::LessOrEqual
                }
                _ => return None,
            };
            (column.clone(), value, reverse)
        }
        _ => return None,
    };
    let bound = |inclusive| {
        Some(IndexRangeBoundV1 {
            value: value.clone(),
            inclusive,
        })
    };
    let (lower, upper) = match operator {
        crate::optimizer_request::Comparison::Equal => (bound(true), bound(true)),
        crate::optimizer_request::Comparison::Less => (None, bound(false)),
        crate::optimizer_request::Comparison::LessOrEqual => (None, bound(true)),
        crate::optimizer_request::Comparison::Greater => (bound(false), None),
        crate::optimizer_request::Comparison::GreaterOrEqual => (bound(true), None),
        _ => return None,
    };
    Some((column, lower, upper))
}

pub(super) fn select_unique_point_probe(
    predicate: &BoundPredicate,
    paths: &[crate::metadata_discovery::AuthorizedUniqueAccessPath],
) -> Option<UniquePointProbeV1> {
    let BoundPredicate::Compare {
        operator: crate::optimizer_request::Comparison::Equal,
        left,
        right,
    } = predicate
    else {
        return None;
    };
    let (column, value) = match (left.as_ref(), right.as_ref()) {
        (BoundPredicate::Column(column), value) if is_point_probe_literal(column, value) => {
            (column, value)
        }
        (value, BoundPredicate::Column(column)) if is_point_probe_literal(column, value) => {
            (column, value)
        }
        _ => return None,
    };
    let path = paths.iter().find(|path| {
        path.index_id != [0; 16] && path.column_ids.as_slice() == [column.column_id]
    })?;
    Some(UniquePointProbeV1 {
        index_id: path.index_id,
        key_column: column.clone(),
        value: value.clone(),
    })
}

pub(super) fn select_multi_point_probe(
    predicate: &BoundPredicate,
    unique_paths: &[crate::metadata_discovery::AuthorizedUniqueAccessPath],
    range_paths: &[crate::metadata_discovery::AuthorizedRangeAccessPath],
) -> Option<MultiPointProbeV1> {
    fn collect(
        predicate: &BoundPredicate,
        leaves: &mut Vec<(BoundColumn, BoundPredicate)>,
    ) -> Option<()> {
        match predicate {
            BoundPredicate::Or(children) if !children.is_empty() => {
                for child in children {
                    collect(child, leaves)?;
                }
                Some(())
            }
            BoundPredicate::Compare {
                operator: crate::optimizer_request::Comparison::Equal,
                left,
                right,
            } => {
                let (column, value) = match (left.as_ref(), right.as_ref()) {
                    (BoundPredicate::Column(column), BoundPredicate::Null)
                    | (BoundPredicate::Null, BoundPredicate::Column(column)) => {
                        leaves.push((column.clone(), BoundPredicate::Null));
                        return (leaves.len() <= 64).then_some(());
                    }
                    (BoundPredicate::Column(column), value)
                        if is_point_probe_literal(column, value) =>
                    {
                        (column, value)
                    }
                    (value, BoundPredicate::Column(column))
                        if is_point_probe_literal(column, value) =>
                    {
                        (column, value)
                    }
                    _ => return None,
                };
                leaves.push((column.clone(), value.clone()));
                (leaves.len() <= 64).then_some(())
            }
            _ => None,
        }
    }

    if !matches!(predicate, BoundPredicate::Or(_)) {
        return None;
    }
    let mut leaves = Vec::new();
    collect(predicate, &mut leaves)?;
    let (first_key_column, _) = leaves.first()?;
    let key_column = first_key_column.clone();
    if leaves.len() < 2 || leaves.iter().any(|(column, _)| column != &key_column) {
        return None;
    }
    let index_id = unique_paths
        .iter()
        .map(|path| (&path.index_id, &path.column_ids))
        .chain(
            range_paths
                .iter()
                .map(|path| (&path.index_id, &path.column_ids)),
        )
        .find(|(index_id, column_ids)| {
            **index_id != [0; 16] && column_ids.first() == Some(&key_column.column_id)
        })
        .map(|(index_id, _)| *index_id)?;
    let mut values = Vec::new();
    for (_, value) in leaves {
        if matches!(value, BoundPredicate::Null) {
            continue;
        }
        if !values.contains(&value) {
            values.push(value);
        }
    }
    if values.is_empty() {
        return None;
    }
    Some(MultiPointProbeV1 {
        index_id,
        key_column,
        values,
    })
}

pub(super) fn is_point_probe_literal(column: &BoundColumn, value: &BoundPredicate) -> bool {
    matches!(
        (column.scalar_type, value),
        (ScalarType::Boolean, BoundPredicate::Boolean(_))
            | (ScalarType::Text, BoundPredicate::Text(_))
            | (ScalarType::Date, BoundPredicate::Date(_))
            | (ScalarType::Uuid, BoundPredicate::Uuid(_))
            | (ScalarType::TimestampTz, BoundPredicate::TimestampTz(_))
            | (ScalarType::Bytea, BoundPredicate::Bytea(_))
    ) || matches!(
        (column.scalar_type, value),
        (ScalarType::Integer, BoundPredicate::Integer(value))
            if i32::try_from(*value).is_ok()
    ) || matches!(
        (column.scalar_type, value),
        (ScalarType::Decimal { precision, .. }, BoundPredicate::Decimal(value))
            if crate::tuple::decimal_coefficient_is_valid(*value, precision)
    )
}

/// Lower authorized join bindings to a canonical join plan. Each scan reads
/// the ordinal-sorted union of its projected and equality-key columns.
pub fn plan_join_select_v1(bound: BoundJoinSelect) -> Result<JoinSelectPlanV1, PlanError> {
    plan_join_select_v1_with_estimates(bound, None, None)
}

/// Lower a join and use exact authorized populations to choose a physical
/// strategy. A known small outer input may use an eligible index on the other
/// side. When both known inputs exceed the measured small-input region, prefer
/// hash join; unknown estimates retain the deterministic index preference.
pub fn plan_join_select_v1_with_estimates(
    bound: BoundJoinSelect,
    left_visible_rows: Option<u64>,
    right_visible_rows: Option<u64>,
) -> Result<JoinSelectPlanV1, PlanError> {
    plan_join_select_v1_with_cardinality_estimate(
        bound,
        left_visible_rows,
        right_visible_rows,
        None,
    )
}

pub fn plan_join_select_v1_with_cardinality_estimate(
    bound: BoundJoinSelect,
    left_visible_rows: Option<u64>,
    right_visible_rows: Option<u64>,
    joined_rows_estimate: Option<u64>,
) -> Result<JoinSelectPlanV1, PlanError> {
    let index_nested_loop = select_index_nested_loop_join(
        &bound,
        left_visible_rows,
        right_visible_rows,
        joined_rows_estimate,
    );
    let relation = |relation: crate::optimizer_binding::BoundJoinRelation| RelationReference {
        schema_name: bound.schema_name.clone(),
        schema_id: bound.schema_id,
        schema_version: bound.schema_version,
        relation_name: relation.relation_name,
        relation_id: relation.relation_id,
        table_epoch: relation.table_epoch,
    };
    let left_relation = relation(bound.left_relation);
    let right_relation = relation(bound.right_relation);
    let mut left_scan_columns = Vec::new();
    let mut right_scan_columns = Vec::new();
    for projection in &bound.projections {
        add_column(
            match projection.relation_side {
                JoinRelationSide::Left => &mut left_scan_columns,
                JoinRelationSide::Right => &mut right_scan_columns,
            },
            &projection.column,
        )?;
    }
    for key in &bound.join_keys {
        add_column(&mut left_scan_columns, &key.left_column)?;
        add_column(&mut right_scan_columns, &key.right_column)?;
    }
    if let Some(filter) = &bound.filter {
        add_column(
            match filter.relation_side {
                JoinRelationSide::Left => &mut left_scan_columns,
                JoinRelationSide::Right => &mut right_scan_columns,
            },
            &filter.column,
        )?;
    }
    left_scan_columns.sort_by_key(|column| column.ordinal);
    right_scan_columns.sort_by_key(|column| column.ordinal);
    let plan = JoinSelectPlanV1 {
        schema_name: bound.schema_name,
        schema_id: bound.schema_id,
        schema_version: bound.schema_version,
        left_relation,
        right_relation,
        left_scan_columns,
        right_scan_columns,
        projections: bound
            .projections
            .into_iter()
            .map(|projection| (projection.relation_side, projection.column))
            .collect(),
        join_keys: bound
            .join_keys
            .into_iter()
            .map(|key| (key.left_column, key.right_column))
            .collect(),
        limit: bound.limit,
        filter: bound.filter,
        index_nested_loop,
    };
    validate_join_select_plan_v1(&plan)?;
    Ok(plan)
}

fn select_index_nested_loop_join(
    bound: &BoundJoinSelect,
    left_visible_rows: Option<u64>,
    right_visible_rows: Option<u64>,
    joined_rows_estimate: Option<u64>,
) -> Option<IndexNestedLoopJoinV1> {
    fn eligible(
        paths: impl Iterator<Item = ([u8; 16], Vec<[u8; 16]>)>,
        key_columns: &[BoundColumn],
        side: JoinRelationSide,
    ) -> Option<IndexNestedLoopJoinV1> {
        let mut candidates = Vec::new();
        for (index_id, column_ids) in paths {
            if index_id == [0; 16] || column_ids.is_empty() {
                continue;
            }
            let mut prefix = Vec::new();
            for column_id in column_ids.iter().take(8) {
                let Some(column) = key_columns
                    .iter()
                    .find(|column| column.column_id == *column_id)
                else {
                    break;
                };
                if !is_supported_index_join_key(column) {
                    break;
                }
                prefix.push(column.clone());
            }
            if !prefix.is_empty() {
                candidates.push((index_id, prefix));
            }
        }
        candidates.sort_by(|(left_id, left_prefix), (right_id, right_prefix)| {
            right_prefix
                .len()
                .cmp(&left_prefix.len())
                .then_with(|| left_id.cmp(right_id))
        });
        candidates
            .into_iter()
            .next()
            .map(|(index_id, prefix)| IndexNestedLoopJoinV1 {
                outer_side: match side {
                    JoinRelationSide::Left => JoinRelationSide::Right,
                    JoinRelationSide::Right => JoinRelationSide::Left,
                },
                inner_side: side,
                index_id,
                key_column: prefix[0].clone(),
                prefix_columns: if prefix.len() > 1 { prefix } else { Vec::new() },
            })
    }

    let right = eligible(
        bound
            .right_relation
            .unique_access_paths
            .iter()
            .map(|path| (path.index_id, path.column_ids.clone()))
            .chain(
                bound
                    .right_relation
                    .range_access_paths
                    .iter()
                    .map(|path| (path.index_id, path.column_ids.clone())),
            ),
        &bound
            .join_keys
            .iter()
            .map(|key| key.right_column.clone())
            .collect::<Vec<_>>(),
        JoinRelationSide::Right,
    );
    let left = eligible(
        bound
            .left_relation
            .unique_access_paths
            .iter()
            .map(|path| (path.index_id, path.column_ids.clone()))
            .chain(
                bound
                    .left_relation
                    .range_access_paths
                    .iter()
                    .map(|path| (path.index_id, path.column_ids.clone())),
            ),
        &bound
            .join_keys
            .iter()
            .map(|key| key.left_column.clone())
            .collect::<Vec<_>>(),
        JoinRelationSide::Left,
    );

    if left_visible_rows == Some(0) || right_visible_rows == Some(0) {
        return None;
    }
    if let (Some(left_rows), Some(right_rows)) = (left_visible_rows, right_visible_rows)
        && joined_rows_estimate.is_some_and(|rows| rows <= INDEXED_JOIN_MAX_OUTPUT_PAIRS)
    {
        // Above the hash input bound, prefer an executable indexed plan when
        // the exact pair count and the indexed runner's row capacities fit.
        if left_rows <= INDEXED_JOIN_MAX_OUTER_ROWS
            && right_rows > BOUNDED_HASH_JOIN_MAX_ROWS_PER_INPUT
            && right_rows <= JOIN_POPULATION_ESTIMATE_MAX_ROWS
        {
            return right.as_ref().cloned();
        }
        if right_rows <= INDEXED_JOIN_MAX_OUTER_ROWS
            && left_rows > BOUNDED_HASH_JOIN_MAX_ROWS_PER_INPUT
            && left_rows <= JOIN_POPULATION_ESTIMATE_MAX_ROWS
        {
            return left.as_ref().cloned();
        }
    }
    match (left_visible_rows, right_visible_rows) {
        (Some(left_rows), Some(right_rows))
            if left_rows <= SMALL_POPULATION_SCAN_ROWS
                && right_rows <= SMALL_POPULATION_SCAN_ROWS =>
        {
            return None;
        }
        (Some(left_rows), Some(right_rows))
            if left_rows <= SMALL_POPULATION_SCAN_ROWS
                && right_rows > SMALL_POPULATION_SCAN_ROWS =>
        {
            return right;
        }
        (Some(left_rows), Some(right_rows))
            if right_rows <= SMALL_POPULATION_SCAN_ROWS
                && left_rows > SMALL_POPULATION_SCAN_ROWS =>
        {
            return left;
        }
        (Some(left_rows), None) if (1..=SMALL_POPULATION_SCAN_ROWS).contains(&left_rows) => {
            return right;
        }
        (None, Some(right_rows)) if (1..=SMALL_POPULATION_SCAN_ROWS).contains(&right_rows) => {
            return left;
        }
        (Some(left_rows), Some(right_rows))
            if left_rows > SMALL_POPULATION_SCAN_ROWS
                && left_rows <= SKEWED_JOIN_OUTER_MAX_ROWS
                && right_rows >= left_rows * SKEWED_JOIN_INPUT_RATIO =>
        {
            return right;
        }
        (Some(left_rows), Some(right_rows))
            if right_rows > SMALL_POPULATION_SCAN_ROWS
                && right_rows <= SKEWED_JOIN_OUTER_MAX_ROWS
                && left_rows >= right_rows * SKEWED_JOIN_INPUT_RATIO =>
        {
            return left;
        }
        (Some(left_rows), Some(right_rows))
            if fanout_aware_join_inputs_are_eligible(left_rows, right_rows)
                && joined_rows_estimate.is_some_and(|rows| {
                    rows <= left_rows * FANOUT_AWARE_JOIN_MAX_PAIRS_PER_OUTER_ROW
                }) =>
        {
            return right;
        }
        (Some(left_rows), Some(right_rows))
            if fanout_aware_join_inputs_are_eligible(right_rows, left_rows)
                && joined_rows_estimate.is_some_and(|rows| {
                    rows <= right_rows * FANOUT_AWARE_JOIN_MAX_PAIRS_PER_OUTER_ROW
                }) =>
        {
            return left;
        }
        (Some(left_rows), Some(right_rows))
            if joined_rows_estimate.is_none()
                && fanout_aware_join_inputs_are_eligible(left_rows, right_rows) =>
        {
            return right;
        }
        (Some(left_rows), Some(right_rows))
            if joined_rows_estimate.is_none()
                && fanout_aware_join_inputs_are_eligible(right_rows, left_rows) =>
        {
            return left;
        }
        (Some(_), Some(_)) => return None,
        _ => {}
    }

    right.or(left)
}
