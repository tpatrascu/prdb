#include "prdb_sdk_optimizer_plan_v1.h"
#include "prdb_query_abi_constants.h"

#include <assert.h>
#include <stdint.h>
#include <string.h>

int main(void) {
    uint8_t buf[4096];
    uint64_t len = 0;

    prdb_sdk_optimizer_expr_node_v1 expr_nodes[3];
    memset(expr_nodes, 0, sizeof(expr_nodes));
    expr_nodes[0].tag = 0x10;
    expr_nodes[0].relation_index = 0;
    expr_nodes[0].bytes = (const uint8_t *)"id";
    expr_nodes[0].bytes_len = 2;

    expr_nodes[1].tag = 0x11;
    expr_nodes[1].param_index = 1;

    expr_nodes[2].tag = 0x20;
    expr_nodes[2].op = 0;
    expr_nodes[2].children[0] = 0;
    expr_nodes[2].children[1] = 1;

    prdb_sdk_optimizer_col_ref_v1 proj_cols[1];
    proj_cols[0].relation_index = 0;
    proj_cols[0].column_name = (const uint8_t *)"id";
    proj_cols[0].column_name_len = 2;

    prdb_sdk_optimizer_logical_node_v1 nodes[3];
    memset(nodes, 0, sizeof(nodes));
    nodes[0].op_tag = PRDB_OPTIMIZER_LOGICAL_OP_SCAN;
    nodes[0].relation_index = 0;
    nodes[0].relation_name = (const uint8_t *)"employees";
    nodes[0].relation_name_len = 9;

    nodes[1].op_tag = PRDB_OPTIMIZER_LOGICAL_OP_FILTER;
    nodes[1].left_child = 0;
    nodes[1].expr_nodes = expr_nodes;
    nodes[1].expr_node_count = 3;
    nodes[1].expr_root = 2;

    nodes[2].op_tag = PRDB_OPTIMIZER_LOGICAL_OP_PROJECT;
    nodes[2].left_child = 1;
    nodes[2].columns = proj_cols;
    nodes[2].column_count = 1;

    int32_t rc = prdb_sdk_optimizer_encode_unified_request_v1(
        (const uint8_t *)"app", 3, nodes, 3, 2, buf, sizeof(buf), &len);
    assert(rc == PRDB_SDK_OPTIMIZER_ENCODE_OK);
    assert(len > 10);
    assert(buf[2] == PRDB_OPTIMIZER_UNIFIED_REQUEST_KIND);

    /* Negative test: invalid empty schema rejected */
    rc = prdb_sdk_optimizer_encode_unified_request_v1(
        (const uint8_t *)"", 0, nodes, 3, 2, buf, sizeof(buf), &len);
    assert(rc == PRDB_SDK_OPTIMIZER_ENCODE_INVALID_ARGUMENT);

    /* Negative test: truncated physical plan rejected */
    prdb_sdk_optimizer_unified_plan_v1 plan;
    uint8_t bad_plan[4] = {0, 1, PRDB_OPTIMIZER_UNIFIED_PLAN_KIND, 0};
    rc = prdb_sdk_optimizer_decode_unified_plan_v1(
        bad_plan, sizeof(bad_plan), &plan, sizeof(plan));
    assert(rc == PRDB_SDK_OPTIMIZER_PLAN_INVALID_PAYLOAD);

    /* Adaptive spill decision check */
    prdb_sdk_optimizer_physical_node_v1 node;
    memset(&node, 0, sizeof(node));
    prdb_sdk_workload_budgets_v1 budgets;
    memset(&budgets, 0, sizeof(budgets));
    budgets.sql_in_mem_max_rows = 128;
    budgets.sql_exec_max_materialized_bytes = 1048576;

    assert(prdb_sdk_optimizer_should_spill_unified_node_v1(&node, &budgets, 64, 1024) == 0);
    assert(prdb_sdk_optimizer_should_spill_unified_node_v1(&node, &budgets, 129, 1024) == 1);
    node.allow_spill = 1;
    assert(prdb_sdk_optimizer_should_spill_unified_node_v1(&node, &budgets, 10, 100) == 1);

    return 0;
}
