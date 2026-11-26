#include "prdb_sdk_optimizer_plan_v1.h"
#include "prdb_sdk_sql_result_v1.h"
#include "prdb_sdk_sql_spill_sort_v1.h"
#include "prdb_query_abi_constants.h"
#include "prdb_query_abi_payload_codecs.h"

#include <stddef.h>
#include <stdint.h>
#include <string.h>

typedef struct unified_writer {
    uint8_t *out;
    uint64_t capacity;
    uint64_t offset;
} unified_writer;

typedef struct unified_reader {
    const uint8_t *bytes;
    uint64_t length;
    uint64_t offset;
} unified_reader;

static int valid_ident(const uint8_t *bytes, uint32_t len) {
    uint32_t i;
    if (bytes == NULL || len == 0 || len > 63) return 0;
    if (!((bytes[0] >= 'A' && bytes[0] <= 'Z') ||
          (bytes[0] >= 'a' && bytes[0] <= 'z') || bytes[0] == '_')) {
        return 0;
    }
    for (i = 1; i < len; ++i) {
        uint8_t c = bytes[i];
        if (!((c >= 'A' && c <= 'Z') || (c >= 'a' && c <= 'z') ||
              (c >= '0' && c <= '9') || c == '_')) {
            return 0;
        }
    }
    return 1;
}

static int write_bytes(unified_writer *w, const uint8_t *src, uint64_t len) {
    if (w->offset > UINT64_MAX - len ||
        (w->out != NULL && (w->offset > w->capacity || len > w->capacity - w->offset))) {
        return 0;
    }
    if (w->out != NULL && len != 0) {
        memcpy(w->out + w->offset, src, (size_t)len);
    }
    w->offset += len;
    return 1;
}

static int write_u8(unified_writer *w, uint8_t v) {
    return write_bytes(w, &v, 1);
}

static int write_u16(unified_writer *w, uint16_t v) {
    uint8_t b[2] = {(uint8_t)(v >> 8), (uint8_t)v};
    return write_bytes(w, b, 2);
}

static int write_u32(unified_writer *w, uint32_t v) {
    uint8_t b[4] = {(uint8_t)(v >> 24), (uint8_t)(v >> 16), (uint8_t)(v >> 8),
                    (uint8_t)v};
    return write_bytes(w, b, 4);
}

static int write_u64(unified_writer *w, uint64_t v) {
    uint8_t b[8];
    uint32_t i;
    for (i = 0; i < 8; ++i) b[i] = (uint8_t)(v >> (56 - i * 8));
    return write_bytes(w, b, 8);
}

static int write_ident(unified_writer *w, const uint8_t *s, uint32_t len) {
    if (!valid_ident(s, len)) return 0;
    return write_u8(w, (uint8_t)len) && write_bytes(w, s, len);
}

static int write_col_ref(unified_writer *w, const prdb_sdk_optimizer_col_ref_v1 *c) {
    if (c == NULL) return 0;
    return write_u16(w, c->relation_index) &&
           write_ident(w, c->column_name, c->column_name_len);
}

static int encode_expr_node(
    unified_writer *w, const prdb_sdk_optimizer_expr_node_v1 *nodes,
    uint32_t node_count, uint16_t idx, uint32_t *visited, uint32_t depth) {
    const prdb_sdk_optimizer_expr_node_v1 *n;
    uint32_t i;

    if (depth > PRDB_SDK_OPTIMIZER_MAX_EXPR_DEPTH || idx >= node_count) return 0;
    if (*visited >= PRDB_SDK_OPTIMIZER_MAX_EXPR_NODES) return 0;
    (*visited)++;
    n = &nodes[idx];

    switch (n->tag) {
    case 0x00:
        return write_u8(w, 0x00);
    case 0x01:
        return write_u8(w, 0x01) && write_u8(w, n->op ? 1 : 0);
    case 0x02:
        return write_u8(w, 0x02) && write_u64(w, (uint64_t)n->int_val);
    case 0x03:
        if (n->bytes == NULL || n->bytes_len > 4096) return 0;
        return write_u8(w, 0x03) && write_u16(w, (uint16_t)n->bytes_len) &&
               write_bytes(w, n->bytes, n->bytes_len);
    case 0x04:
        return write_u8(w, 0x04) && write_u32(w, (uint32_t)n->int_val);
    case 0x05:
        if (n->bytes == NULL || n->bytes_len != 16) return 0;
        return write_u8(w, 0x05) && write_bytes(w, n->bytes, 16);
    case 0x06:
        return write_u8(w, 0x06) && write_u64(w, (uint64_t)n->int_val);
    case 0x07:
        if (n->bytes == NULL || n->bytes_len > 4096) return 0;
        return write_u8(w, 0x07) && write_u16(w, (uint16_t)n->bytes_len) &&
               write_bytes(w, n->bytes, n->bytes_len);
    case 0x08:
        if (n->bytes == NULL || n->bytes_len != 16) return 0;
        return write_u8(w, 0x08) && write_bytes(w, n->bytes, 16);
    case 0x10:
        return write_u8(w, 0x10) && write_u16(w, n->relation_index) &&
               write_ident(w, n->bytes, n->bytes_len);
    case 0x11:
        if (n->param_index == 0 || n->param_index > 256) return 0;
        return write_u8(w, 0x11) && write_u32(w, n->param_index);
    case 0x20:
        if (n->op > 5) return 0;
        return write_u8(w, 0x20) && write_u8(w, n->op) &&
               encode_expr_node(w, nodes, node_count, n->children[0], visited, depth + 1) &&
               encode_expr_node(w, nodes, node_count, n->children[1], visited, depth + 1);
    case 0x30:
    case 0x31:
        if (n->child_count < 2 || n->child_count > 16) return 0;
        if (!write_u8(w, n->tag) || !write_u8(w, n->child_count)) return 0;
        for (i = 0; i < n->child_count; ++i) {
            if (!encode_expr_node(w, nodes, node_count, n->children[i], visited, depth + 1)) {
                return 0;
            }
        }
        return 1;
    case 0x32:
    case 0x40:
    case 0x41:
        return write_u8(w, n->tag) &&
               encode_expr_node(w, nodes, node_count, n->children[0], visited, depth + 1);
    case 0x42:
        if (n->child_count == 0 || n->child_count > 16) return 0;
        if (!write_u8(w, 0x42) || !write_u8(w, n->op ? 1 : 0) ||
            !write_u8(w, n->child_count) ||
            !encode_expr_node(w, nodes, node_count, n->in_target, visited, depth + 1)) {
            return 0;
        }
        for (i = 0; i < n->child_count; ++i) {
            if (!encode_expr_node(w, nodes, node_count, n->children[i], visited, depth + 1)) {
                return 0;
            }
        }
        return 1;
    default:
        return 0;
    }
}

static int encode_logical_node(
    unified_writer *w, const prdb_sdk_optimizer_logical_node_v1 *nodes,
    uint32_t node_count, uint16_t idx, uint32_t *visited, uint32_t depth) {
    const prdb_sdk_optimizer_logical_node_v1 *n;
    uint32_t i;
    uint32_t expr_visited;

    if (depth > PRDB_SDK_OPTIMIZER_MAX_EXPR_DEPTH || idx >= node_count) return 0;
    if (*visited >= PRDB_SDK_OPTIMIZER_MAX_LOGICAL_NODES) return 0;
    (*visited)++;
    n = &nodes[idx];

    switch (n->op_tag) {
    case PRDB_OPTIMIZER_LOGICAL_OP_SCAN:
        return write_u8(w, n->op_tag) && write_u16(w, n->relation_index) &&
               write_ident(w, n->relation_name, n->relation_name_len);
    case PRDB_OPTIMIZER_LOGICAL_OP_FILTER:
        if (n->expr_nodes == NULL || n->expr_root >= n->expr_node_count) return 0;
        expr_visited = 0;
        return write_u8(w, n->op_tag) &&
               encode_expr_node(w, n->expr_nodes, n->expr_node_count, n->expr_root, &expr_visited, 1) &&
               encode_logical_node(w, nodes, node_count, n->left_child, visited, depth + 1);
    case PRDB_OPTIMIZER_LOGICAL_OP_PROJECT:
        if (n->columns == NULL || n->column_count == 0 ||
            n->column_count > PRDB_SDK_OPTIMIZER_PLAN_MAX_PROJECTIONS) {
            return 0;
        }
        if (!write_u8(w, n->op_tag) || !write_u16(w, (uint16_t)n->column_count)) return 0;
        for (i = 0; i < n->column_count; ++i) {
            if (!write_col_ref(w, &n->columns[i])) return 0;
        }
        return encode_logical_node(w, nodes, node_count, n->left_child, visited, depth + 1);
    case PRDB_OPTIMIZER_LOGICAL_OP_JOIN:
        if (n->join_type > 3 || n->columns == NULL || n->right_keys == NULL ||
            n->column_count == 0 || n->column_count > 8) {
            return 0;
        }
        if (!write_u8(w, n->op_tag) || !write_u8(w, n->join_type) ||
            !write_u8(w, (uint8_t)n->column_count)) {
            return 0;
        }
        for (i = 0; i < n->column_count; ++i) {
            if (!write_col_ref(w, &n->columns[i]) ||
                !write_col_ref(w, &n->right_keys[i])) {
                return 0;
            }
        }
        if (n->expr_nodes != NULL && n->expr_node_count > 0) {
            expr_visited = 0;
            if (!write_u8(w, 1) ||
                !encode_expr_node(w, n->expr_nodes, n->expr_node_count, n->expr_root, &expr_visited, 1)) {
                return 0;
            }
        } else {
            if (!write_u8(w, 0)) return 0;
        }
        return encode_logical_node(w, nodes, node_count, n->left_child, visited, depth + 1) &&
               encode_logical_node(w, nodes, node_count, n->right_child, visited, depth + 1);
    case PRDB_OPTIMIZER_LOGICAL_OP_AGGREGATE:
        if (n->column_count > 8 || n->agg_count > 8 ||
            (n->column_count == 0 && n->agg_count == 0)) {
            return 0;
        }
        if (n->column_count > 0 && n->columns == NULL) return 0;
        if (n->agg_count > 0 && n->aggs == NULL) return 0;
        if (!write_u8(w, n->op_tag) || !write_u8(w, (uint8_t)n->column_count)) {
            return 0;
        }
        for (i = 0; i < n->column_count; ++i) {
            if (!write_col_ref(w, &n->columns[i])) return 0;
        }
        if (!write_u8(w, (uint8_t)n->agg_count)) return 0;
        for (i = 0; i < n->agg_count; ++i) {
            if (!write_u8(w, n->aggs[i].kind)) return 0;
            if (n->aggs[i].has_arg) {
                if (!write_u8(w, 1) || !write_col_ref(w, &n->aggs[i].arg)) return 0;
            } else {
                if (!write_u8(w, 0)) return 0;
            }
        }
        if (n->expr_nodes != NULL && n->expr_node_count > 0) {
            expr_visited = 0;
            if (!write_u8(w, 1) ||
                !encode_expr_node(w, n->expr_nodes, n->expr_node_count, n->expr_root, &expr_visited, 1)) {
                return 0;
            }
        } else {
            if (!write_u8(w, 0)) return 0;
        }
        return encode_logical_node(w, nodes, node_count, n->left_child, visited, depth + 1);
    case PRDB_OPTIMIZER_LOGICAL_OP_SORT:
        if (n->order_items == NULL || n->order_item_count == 0 ||
            n->order_item_count > 16) {
            return 0;
        }
        if (!write_u8(w, n->op_tag) || !write_u8(w, (uint8_t)n->order_item_count)) {
            return 0;
        }
        for (i = 0; i < n->order_item_count; ++i) {
            const prdb_sdk_optimizer_order_item_v1 *oi = &n->order_items[i];
            if (oi->is_agg_index) {
                if (!write_u8(w, 1) || !write_u16(w, oi->agg_index)) return 0;
            } else {
                if (!write_u8(w, 0) || !write_col_ref(w, &oi->column)) return 0;
            }
            if (!write_u8(w, oi->descending ? 1 : 0) ||
                !write_u8(w, oi->nulls_first ? 1 : 0)) {
                return 0;
            }
        }
        return encode_logical_node(w, nodes, node_count, n->left_child, visited, depth + 1);
    case PRDB_OPTIMIZER_LOGICAL_OP_LIMIT:
        return write_u8(w, n->op_tag) && write_u32(w, n->limit_count) &&
               encode_logical_node(w, nodes, node_count, n->left_child, visited, depth + 1);
    case PRDB_OPTIMIZER_LOGICAL_OP_DELETE:
        if (n->expr_nodes == NULL || n->expr_root >= n->expr_node_count) return 0;
        expr_visited = 0;
        return write_u8(w, n->op_tag) && write_u16(w, n->relation_index) &&
               write_ident(w, n->relation_name, n->relation_name_len) &&
               encode_expr_node(w, n->expr_nodes, n->expr_node_count, n->expr_root, &expr_visited, 1);
    case PRDB_OPTIMIZER_LOGICAL_OP_UPDATE:
        if (n->assignments == NULL || n->assignment_count == 0 ||
            n->assignment_count > PRDB_SDK_OPTIMIZER_PLAN_MAX_UPDATE_ASSIGNMENTS ||
            n->expr_nodes == NULL || n->expr_root >= n->expr_node_count) {
            return 0;
        }
        if (!write_u8(w, n->op_tag) || !write_u16(w, n->relation_index) ||
            !write_ident(w, n->relation_name, n->relation_name_len) ||
            !write_u16(w, (uint16_t)n->assignment_count)) {
            return 0;
        }
        for (i = 0; i < n->assignment_count; ++i) {
            const prdb_sdk_optimizer_update_assign_v1 *a = &n->assignments[i];
            if (!write_ident(w, a->column_name, a->column_name_len)) return 0;
            if (a->is_null) {
                if (!write_u8(w, 1)) return 0;
            } else {
                if (a->param_index == 0 ||
                    a->param_index > PRDB_SDK_OPTIMIZER_PLAN_MAX_PARAMETER_INDEX ||
                    !write_u8(w, 0) || !write_u32(w, a->param_index)) {
                    return 0;
                }
            }
        }
        expr_visited = 0;
        return encode_expr_node(w, n->expr_nodes, n->expr_node_count, n->expr_root, &expr_visited, 1);
    default:
        return 0;
    }
}

int32_t prdb_sdk_optimizer_encode_unified_request_v1(
    const uint8_t *schema, uint32_t schema_len,
    const prdb_sdk_optimizer_logical_node_v1 *nodes, uint32_t node_count,
    uint16_t root_index,
    uint8_t *request, uint64_t request_capacity, uint64_t *request_len) {
    unified_writer w;
    uint32_t visited = 0;

    if (schema == NULL || nodes == NULL || node_count == 0 ||
        root_index >= node_count || request_len == NULL) {
        return PRDB_SDK_OPTIMIZER_ENCODE_INVALID_ARGUMENT;
    }
    w.out = request;
    w.capacity = request_capacity;
    w.offset = 0;

    if (!write_u16(&w, PRDB_OPTIMIZER_REQUEST_VERSION) ||
        !write_u8(&w, PRDB_OPTIMIZER_UNIFIED_REQUEST_KIND) ||
        !write_u8(&w, 0) ||
        !write_ident(&w, schema, schema_len) ||
        !encode_logical_node(&w, nodes, node_count, root_index, &visited, 1)) {
        if (w.out != NULL && w.offset > w.capacity) {
            return PRDB_SDK_OPTIMIZER_ENCODE_BUFFER_TOO_SMALL;
        }
        return PRDB_SDK_OPTIMIZER_ENCODE_INVALID_ARGUMENT;
    }
    if (w.offset > PRDB_SDK_OPTIMIZER_MAX_REQUEST_BYTES) {
        return PRDB_SDK_OPTIMIZER_ENCODE_LIMIT_EXCEEDED;
    }
    *request_len = w.offset;
    return PRDB_SDK_OPTIMIZER_ENCODE_OK;
}

static int take_bytes(unified_reader *r, uint64_t len, const uint8_t **out) {
    if (r->offset > r->length || len > r->length - r->offset) return 0;
    *out = r->bytes + r->offset;
    r->offset += len;
    return 1;
}

static int read_u8(unified_reader *r, uint8_t *out) {
    const uint8_t *p;
    if (!take_bytes(r, 1, &p)) return 0;
    *out = p[0];
    return 1;
}

static int read_u16(unified_reader *r, uint16_t *out) {
    const uint8_t *p;
    if (!take_bytes(r, 2, &p)) return 0;
    *out = (uint16_t)(((uint16_t)p[0] << 8) | p[1]);
    return 1;
}

static int read_u32(unified_reader *r, uint32_t *out) {
    const uint8_t *p;
    if (!take_bytes(r, 4, &p)) return 0;
    *out = ((uint32_t)p[0] << 24) | ((uint32_t)p[1] << 16) |
           ((uint32_t)p[2] << 8) | p[3];
    return 1;
}

static int read_u64(unified_reader *r, uint64_t *out) {
    const uint8_t *p;
    uint64_t v = 0;
    uint32_t i;
    if (!take_bytes(r, 8, &p)) return 0;
    for (i = 0; i < 8; ++i) v = (v << 8) | p[i];
    *out = v;
    return 1;
}

static int read_ident(unified_reader *r, const uint8_t **name_out, uint32_t *len_out) {
    uint8_t len;
    const uint8_t *p;
    if (!read_u8(r, &len) || !take_bytes(r, len, &p) || !valid_ident(p, len)) return 0;
    *name_out = p;
    *len_out = len;
    return 1;
}

static int read_bound_col(unified_reader *r, prdb_sdk_optimizer_plan_column_v1 *col) {
    const uint8_t *p;
    uint8_t nullable;
    if (!take_bytes(r, 16, &p)) return 0;
    memcpy(col->column_id, p, 16);
    if (!read_u32(r, &col->ordinal) || !read_u8(r, &col->type_code) ||
        !take_bytes(r, 4, &p) || !read_u8(r, &nullable) || nullable > 1) {
        return 0;
    }
    memcpy(col->type_parameters, p, 4);
    col->nullable = nullable;
    return 1;
}

static int read_rel_ref(unified_reader *r, prdb_sdk_optimizer_physical_node_v1 *node) {
    const uint8_t *p;
    if (!read_u16(r, &node->relation_index) ||
        !read_ident(r, &node->relation_name, &node->relation_name_len) ||
        !take_bytes(r, 16, &p)) {
        return 0;
    }
    memcpy(node->relation_id, p, 16);
    return read_u64(r, &node->table_epoch);
}

static int read_col_list(unified_reader *r, prdb_sdk_optimizer_plan_column_v1 *cols,
                         uint32_t max_cols, uint32_t *count_out) {
    uint16_t count;
    uint32_t i;
    if (!read_u16(r, &count) || count == 0 || count > max_cols) return 0;
    for (i = 0; i < count; ++i) {
        if (!read_bound_col(r, &cols[i])) return 0;
    }
    *count_out = count;
    return 1;
}

static int skip_bound_expr(unified_reader *r, uint32_t *nodes, uint32_t depth) {
    uint8_t tag, flag, count;
    uint16_t len;
    uint32_t i;
    const uint8_t *p;
    prdb_sdk_optimizer_plan_column_v1 col;

    if (depth > PRDB_SDK_OPTIMIZER_MAX_EXPR_DEPTH) return 0;
    if (*nodes >= PRDB_SDK_OPTIMIZER_MAX_EXPR_NODES) return 0;
    (*nodes)++;
    if (!read_u8(r, &tag)) return 0;
    switch (tag) {
    case 0x00:
        return 1;
    case 0x01:
        return read_u8(r, &flag) && flag <= 1;
    case 0x02:
    case 0x06:
        return take_bytes(r, 8, &p);
    case 0x03:
    case 0x07:
        return read_u16(r, &len) && len <= 4096 && take_bytes(r, len, &p);
    case 0x04:
    case 0x11:
        return take_bytes(r, 4, &p);
    case 0x05:
    case 0x08:
        return take_bytes(r, 16, &p);
    case 0x10:
        return take_bytes(r, 2, &p) && read_bound_col(r, &col);
    case 0x20:
        return read_u8(r, &flag) && flag <= 5 &&
               skip_bound_expr(r, nodes, depth + 1) &&
               skip_bound_expr(r, nodes, depth + 1);
    case 0x30:
    case 0x31:
        if (!read_u8(r, &count) || count < 2 || count > 16) return 0;
        for (i = 0; i < count; ++i) {
            if (!skip_bound_expr(r, nodes, depth + 1)) return 0;
        }
        return 1;
    case 0x32:
    case 0x40:
    case 0x41:
        return skip_bound_expr(r, nodes, depth + 1);
    case 0x42:
        if (!read_u8(r, &flag) || flag > 1 || !read_u8(r, &count) ||
            count == 0 || count > 64 ||
            !skip_bound_expr(r, nodes, depth + 1)) {
            return 0;
        }
        for (i = 0; i < count; ++i) {
            if (!skip_bound_expr(r, nodes, depth + 1)) return 0;
        }
        return 1;
    default:
        return 0;
    }
}

static int decode_physical_node(
    unified_reader *r, prdb_sdk_optimizer_unified_plan_v1 *plan,
    uint16_t *node_index_out, uint32_t depth) {
    uint16_t idx;
    prdb_sdk_optimizer_physical_node_v1 *n;
    const uint8_t *p;
    uint8_t flag;
    uint32_t expr_nodes;
    uint64_t start;

    if (depth > PRDB_SDK_OPTIMIZER_MAX_EXPR_DEPTH) return 0;
    if (plan->node_count >= PRDB_SDK_OPTIMIZER_MAX_PHYSICAL_NODES) return 0;
    idx = (uint16_t)plan->node_count++;
    n = &plan->nodes[idx];
    memset(n, 0, sizeof(*n));
    *node_index_out = idx;

    if (!read_u8(r, &n->op_tag)) return 0;
    switch (n->op_tag) {
    case PRDB_OPTIMIZER_PHYSICAL_OP_SEQ_SCAN:
        return read_rel_ref(r, n) &&
               read_col_list(r, n->scan_columns, PRDB_SDK_OPTIMIZER_PLAN_MAX_SCAN_COLUMNS, &n->scan_column_count);
    case PRDB_OPTIMIZER_PHYSICAL_OP_INDEX_ORDERED_SCAN: {
        uint8_t o_count;
        uint32_t i;
        if (!read_rel_ref(r, n) ||
            !read_col_list(r, n->scan_columns, PRDB_SDK_OPTIMIZER_PLAN_MAX_SCAN_COLUMNS, &n->scan_column_count) ||
            !take_bytes(r, 16, &p)) {
            return 0;
        }
        memcpy(n->index_id, p, 16);
        if (!read_u8(r, &o_count) || o_count == 0 || o_count > 8) return 0;
        n->key_count = o_count;
        for (i = 0; i < o_count; ++i) {
            if (!read_bound_col(r, &n->key_columns[i])) return 0;
        }
        if (!read_u8(r, &n->descending) || n->descending > 1 ||
            !read_u8(r, &flag) || flag > 1) {
            return 0;
        }
        n->has_limit = flag;
        if (flag && !read_u32(r, &n->limit)) return 0;
        return 1;
    }
    case PRDB_OPTIMIZER_PHYSICAL_OP_EMPTY_RESULT:
        return read_col_list(r, n->scan_columns, PRDB_SDK_OPTIMIZER_PLAN_MAX_PROJECTIONS, &n->scan_column_count);
    case PRDB_OPTIMIZER_PHYSICAL_OP_FILTER:
        start = r->offset;
        expr_nodes = 0;
        if (!skip_bound_expr(r, &expr_nodes, 1)) return 0;
        n->has_predicate = 1;
        n->predicate_prefix = r->bytes + start;
        n->predicate_prefix_len = (uint32_t)(r->offset - start);
        return decode_physical_node(r, plan, &n->left_child, depth + 1);
    case PRDB_OPTIMIZER_PHYSICAL_OP_PROJECT:
        return read_col_list(r, n->scan_columns, PRDB_SDK_OPTIMIZER_PLAN_MAX_PROJECTIONS, &n->scan_column_count) &&
               decode_physical_node(r, plan, &n->left_child, depth + 1);
    case PRDB_OPTIMIZER_PHYSICAL_OP_LIMIT:
        n->has_limit = 1;
        return read_u32(r, &n->limit) &&
               decode_physical_node(r, plan, &n->left_child, depth + 1);
    case PRDB_OPTIMIZER_PHYSICAL_OP_HASH_JOIN: {
        uint8_t k_count;
        uint32_t i;
        if (!read_u8(r, &n->join_type) || n->join_type > 3 ||
            !read_u8(r, &n->allow_spill) || n->allow_spill > 1 ||
            !read_u8(r, &k_count) || k_count == 0 || k_count > 8) {
            return 0;
        }
        n->key_count = k_count;
        for (i = 0; i < k_count; ++i) {
            if (!read_bound_col(r, &n->key_columns[i]) ||
                !read_bound_col(r, &n->right_key_columns[i])) {
                return 0;
            }
        }
        if (!read_u8(r, &flag) || flag > 1) return 0;
        if (flag) {
            start = r->offset;
            expr_nodes = 0;
            if (!skip_bound_expr(r, &expr_nodes, 1)) return 0;
            n->has_predicate = 1;
            n->predicate_prefix = r->bytes + start;
            n->predicate_prefix_len = (uint32_t)(r->offset - start);
        }
        return decode_physical_node(r, plan, &n->left_child, depth + 1) &&
               decode_physical_node(r, plan, &n->right_child, depth + 1);
    }
    case PRDB_OPTIMIZER_PHYSICAL_OP_INDEX_POINT_SCAN: {
        uint8_t k_count;
        uint32_t i;
        if (!read_rel_ref(r, n) ||
            !read_col_list(r, n->scan_columns, PRDB_SDK_OPTIMIZER_PLAN_MAX_SCAN_COLUMNS, &n->scan_column_count) ||
            !take_bytes(r, 16, &p) ||
            !read_u8(r, &k_count) || k_count == 0 || k_count > 8) {
            return 0;
        }
        memcpy(n->index_id, p, 16);
        n->key_count = k_count;
        for (i = 0; i < k_count; ++i) {
            if (!read_bound_col(r, &n->key_columns[i])) return 0;
        }
        for (i = 0; i < k_count; ++i) {
            start = r->offset;
            expr_nodes = 0;
            if (!skip_bound_expr(r, &expr_nodes, 1)) return 0;
            n->key_value_prefixes[i] = r->bytes + start;
            n->key_value_prefix_lens[i] = (uint32_t)(r->offset - start);
            if (i == 0) {
                n->has_predicate = 1;
                n->predicate_prefix = r->bytes + start;
                n->predicate_prefix_len = (uint32_t)(r->offset - start);
            }
        }
        return 1;
    }
    case PRDB_OPTIMIZER_PHYSICAL_OP_INDEX_RANGE_SCAN: {
        uint8_t k_count, has_rc, bflag;
        uint32_t i;
        prdb_sdk_optimizer_plan_column_v1 rc_col;
        if (!read_rel_ref(r, n) ||
            !read_col_list(r, n->scan_columns, PRDB_SDK_OPTIMIZER_PLAN_MAX_SCAN_COLUMNS, &n->scan_column_count) ||
            !take_bytes(r, 16, &p) ||
            !read_u8(r, &k_count) || k_count > 8) {
            return 0;
        }
        memcpy(n->index_id, p, 16);
        n->key_count = k_count;
        for (i = 0; i < k_count; ++i) {
            if (!read_bound_col(r, &n->key_columns[i])) return 0;
        }
        for (i = 0; i < k_count; ++i) {
            expr_nodes = 0;
            if (!skip_bound_expr(r, &expr_nodes, 1)) return 0;
        }
        if (!read_u8(r, &has_rc) || has_rc > 1) return 0;
        if (has_rc) {
            if (!read_bound_col(r, &rc_col)) return 0;
            if (n->key_count < 8) n->key_columns[n->key_count++] = rc_col;
        }
        for (i = 0; i < 2; ++i) {
            if (!read_u8(r, &bflag) || bflag > 2) return 0;
            if (bflag != 0) {
                start = r->offset;
                expr_nodes = 0;
                if (!skip_bound_expr(r, &expr_nodes, 1)) return 0;
                if (i == 0) {
                    n->has_lower_bound = 1;
                    n->lower_inclusive = (bflag == 1) ? 1 : 0;
                    n->lower_bound_prefix = r->bytes + start;
                    n->lower_bound_prefix_len = (uint32_t)(r->offset - start);
                } else {
                    n->has_upper_bound = 1;
                    n->upper_inclusive = (bflag == 1) ? 1 : 0;
                    n->upper_bound_prefix = r->bytes + start;
                    n->upper_bound_prefix_len = (uint32_t)(r->offset - start);
                }
            }
        }
        return 1;
    }
    case PRDB_OPTIMIZER_PHYSICAL_OP_COVERING_INDEX_SCAN: {
        uint8_t has_rc, bflag;
        uint32_t i;
        prdb_sdk_optimizer_plan_column_v1 rc_col;
        if (!read_rel_ref(r, n) ||
            !read_col_list(r, n->scan_columns, PRDB_SDK_OPTIMIZER_PLAN_MAX_SCAN_COLUMNS, &n->scan_column_count) ||
            !take_bytes(r, 16, &p) ||
            !read_u8(r, &has_rc) || has_rc > 1) {
            return 0;
        }
        memcpy(n->index_id, p, 16);
        if (has_rc) {
            if (!read_bound_col(r, &rc_col)) return 0;
            n->key_count = 1;
            n->key_columns[0] = rc_col;
        }
        for (i = 0; i < 2; ++i) {
            if (!read_u8(r, &bflag) || bflag > 2) return 0;
            if (bflag != 0) {
                expr_nodes = 0;
                if (!skip_bound_expr(r, &expr_nodes, 1)) return 0;
            }
        }
        return 1;
    }
    case PRDB_OPTIMIZER_PHYSICAL_OP_INDEX_NESTED_LOOP_JOIN: {
        uint8_t k_count;
        uint32_t i;
        if (!read_u8(r, &n->join_type) || n->join_type > 3 ||
            !read_rel_ref(r, n) ||
            !read_col_list(r, n->scan_columns, PRDB_SDK_OPTIMIZER_PLAN_MAX_SCAN_COLUMNS, &n->scan_column_count) ||
            !take_bytes(r, 16, &p) ||
            !read_u8(r, &k_count) || k_count == 0 || k_count > 8) {
            return 0;
        }
        memcpy(n->index_id, p, 16);
        n->key_count = k_count;
        for (i = 0; i < k_count; ++i) {
            if (!read_bound_col(r, &n->key_columns[i])) return 0;
        }
        for (i = 0; i < k_count; ++i) {
            if (!read_bound_col(r, &n->right_key_columns[i])) return 0;
        }
        if (!read_u8(r, &flag) || flag > 1) return 0;
        if (flag) {
            start = r->offset;
            expr_nodes = 0;
            if (!skip_bound_expr(r, &expr_nodes, 1)) return 0;
            n->has_predicate = 1;
            n->predicate_prefix = r->bytes + start;
            n->predicate_prefix_len = (uint32_t)(r->offset - start);
        }
        return decode_physical_node(r, plan, &n->left_child, depth + 1);
    }
    case PRDB_OPTIMIZER_PHYSICAL_OP_HASH_AGGREGATE:
    case PRDB_OPTIMIZER_PHYSICAL_OP_STREAM_AGGREGATE: {
        uint8_t g_count, a_count, kind, has_arg, has_having;
        uint32_t i;
        prdb_sdk_optimizer_plan_column_v1 tmp_col;
        if (n->op_tag == PRDB_OPTIMIZER_PHYSICAL_OP_HASH_AGGREGATE) {
            if (!read_u8(r, &n->allow_spill) || n->allow_spill > 1) return 0;
        }
        if (!read_u8(r, &g_count) || g_count > 8) return 0;
        n->key_count = g_count;
        for (i = 0; i < g_count; ++i) {
            if (!read_bound_col(r, &n->key_columns[i])) return 0;
        }
        if (!read_u8(r, &a_count) || a_count > 8) return 0;
        for (i = 0; i < a_count; ++i) {
            if (!read_u8(r, &kind) || !read_u8(r, &has_arg) || has_arg > 1) return 0;
            if (has_arg && !read_bound_col(r, &tmp_col)) return 0;
        }
        if (!read_u8(r, &has_having) || has_having > 1) return 0;
        if (has_having) {
            expr_nodes = 0;
            if (!skip_bound_expr(r, &expr_nodes, 1)) return 0;
        }
        return decode_physical_node(r, plan, &n->left_child, depth + 1);
    }
    case PRDB_OPTIMIZER_PHYSICAL_OP_SORT: {
        uint8_t o_count, target_kind, desc, nulls_first;
        uint32_t i;
        if (!read_u8(r, &n->allow_spill) || n->allow_spill > 1 ||
            !read_u8(r, &o_count) || o_count == 0 || o_count > 16) {
            return 0;
        }
        n->key_count = o_count > 8 ? 8 : o_count;
        for (i = 0; i < o_count; ++i) {
            if (!read_u8(r, &target_kind)) return 0;
            if (target_kind == 0) {
                uint16_t rel_idx;
                prdb_sdk_optimizer_plan_column_v1 tmp_col;
                if (!read_u16(r, &rel_idx) ||
                    !read_bound_col(r, i < 8 ? &n->key_columns[i] : &tmp_col)) return 0;
            } else if (target_kind == 1) {
                uint16_t agg_idx;
                if (!read_u16(r, &agg_idx)) return 0;
            } else {
                return 0;
            }
            if (!read_u8(r, &desc) || desc > 1 ||
                !read_u8(r, &nulls_first) || nulls_first > 1) {
                return 0;
            }
            if (i == 0) n->descending = desc;
        }
        return decode_physical_node(r, plan, &n->left_child, depth + 1);
    }
    case PRDB_OPTIMIZER_PHYSICAL_OP_DELETE:
        return read_rel_ref(r, n) &&
               decode_physical_node(r, plan, &n->left_child, depth + 1);
    case PRDB_OPTIMIZER_PHYSICAL_OP_UPDATE: {
        uint16_t a_count, i;
        prdb_sdk_optimizer_plan_column_v1 tmp_col;
        uint8_t vtag;
        uint32_t pidx;
        if (!read_rel_ref(r, n) || !read_u16(r, &a_count) ||
            a_count == 0 || a_count > PRDB_SDK_OPTIMIZER_PLAN_MAX_UPDATE_ASSIGNMENTS) {
            return 0;
        }
        for (i = 0; i < a_count; ++i) {
            if (!read_bound_col(r, &tmp_col) || !read_u8(r, &vtag)) return 0;
            if (vtag == 0) {
                if (!read_u32(r, &pidx)) return 0;
            } else if (vtag != 1) {
                return 0;
            }
        }
        return decode_physical_node(r, plan, &n->left_child, depth + 1);
    }
    case PRDB_OPTIMIZER_PHYSICAL_OP_TOPN_HEAP: {
        uint8_t o_count, target_kind, desc, nulls_first;
        uint32_t i;
        if (!read_u32(r, &n->limit) || !read_u8(r, &o_count) ||
            o_count == 0 || o_count > 8) {
            return 0;
        }
        n->has_limit = 1;
        n->key_count = o_count;
        for (i = 0; i < o_count; ++i) {
            if (!read_u8(r, &target_kind)) return 0;
            if (target_kind == 0) {
                uint16_t rel_idx;
                if (!read_u16(r, &rel_idx) || !read_bound_col(r, &n->key_columns[i])) return 0;
            } else if (target_kind == 1) {
                uint16_t agg_idx;
                if (!read_u16(r, &agg_idx)) return 0;
            } else {
                return 0;
            }
            if (!read_u8(r, &desc) || desc > 1 ||
                !read_u8(r, &nulls_first) || nulls_first > 1) {
                return 0;
            }
            if (i == 0) n->descending = desc;
        }
        return decode_physical_node(r, plan, &n->left_child, depth + 1);
    }
    default:
        return 0;
    }
}

int32_t prdb_sdk_optimizer_decode_unified_plan_v1(
    const uint8_t *payload, uint64_t payload_len,
    prdb_sdk_optimizer_unified_plan_v1 *out, uint32_t out_capacity) {
    unified_reader r;
    uint16_t version;
    uint8_t kind, reserved;
    const uint8_t *p;

    if (payload == NULL || out == NULL || out_capacity < sizeof(*out) ||
        payload_len > PRDB_SDK_OPTIMIZER_MAX_PLAN_BYTES) {
        return PRDB_SDK_OPTIMIZER_PLAN_INVALID_ARGUMENT;
    }
    memset(out, 0, sizeof(*out));
    out->struct_size = (uint32_t)sizeof(*out);
    out->abi_version = PRDB_SDK_OPTIMIZER_PLAN_ABI_V1_VERSION;

    r.bytes = payload;
    r.length = payload_len;
    r.offset = 0;

    if (!read_u16(&r, &version) || version != PRDB_OPTIMIZER_RESPONSE_VERSION ||
        !read_u8(&r, &kind) || kind != PRDB_OPTIMIZER_UNIFIED_PLAN_KIND ||
        !read_u8(&r, &reserved) || reserved != 0 ||
        !read_ident(&r, &out->schema_name, &out->schema_name_len) ||
        !take_bytes(&r, 16, &p)) {
        return PRDB_SDK_OPTIMIZER_PLAN_INVALID_PAYLOAD;
    }
    memcpy(out->schema_id, p, 16);
    if (!read_u64(&r, &out->schema_version) ||
        !decode_physical_node(&r, out, &out->root_index, 1) ||
        r.offset != r.length) {
        return PRDB_SDK_OPTIMIZER_PLAN_INVALID_PAYLOAD;
    }
    return PRDB_SDK_OPTIMIZER_PLAN_OK;
}

static int ranges_overlap_u(const void *a, size_t a_len, const void *b, size_t b_len) {
    uintptr_t as = (uintptr_t)a, bs = (uintptr_t)b;
    if (a_len == 0 || b_len == 0) return 0;
    return as < bs + b_len && bs < as + a_len;
}

int32_t prdb_sdk_optimizer_decode_response_v1(
    const uint8_t *payload, uint64_t payload_len,
    prdb_sdk_optimizer_response_v1 *out, uint32_t out_capacity) {
    prdb_sdk_optimizer_response_v1 decoded;
    uint32_t plan_len;
    size_t decoded_plan_len;
    uint8_t response_status;

    if (payload == NULL || out == NULL ||
        out_capacity < sizeof(prdb_sdk_optimizer_response_v1) ||
        payload_len > PRDB_QUERY_MAX_IPC_REQUEST_BYTES || payload_len < 3 ||
        ranges_overlap_u(payload, (size_t)payload_len, out, sizeof(*out))) {
        return PRDB_SDK_OPTIMIZER_RESULT_INVALID_ARGUMENT;
    }
    decoded.struct_size = sizeof(decoded);
    decoded.abi_version = PRDB_SDK_OPTIMIZER_ABI_V1_VERSION;
    decoded.status = payload[2];
    decoded.plan = NULL;
    decoded.plan_len = 0;
    if (decoded.status > PRDB_SDK_OPTIMIZER_STATUS_INTERNAL) {
        return PRDB_SDK_OPTIMIZER_RESULT_INVALID_RESPONSE;
    }
    if (decoded.status != PRDB_SDK_OPTIMIZER_STATUS_SUCCESS) {
        if (!prdb_decode_optimizer_response_failure(
                payload, (size_t)payload_len, &response_status) ||
            response_status != decoded.status) {
            return PRDB_SDK_OPTIMIZER_RESULT_INVALID_RESPONSE;
        }
        *out = decoded;
        return PRDB_SDK_OPTIMIZER_RESULT_OK;
    }
    if (!prdb_decode_optimizer_response_success(
            payload, (size_t)payload_len, &response_status, &plan_len) ||
        response_status != decoded.status) {
        return PRDB_SDK_OPTIMIZER_RESULT_INVALID_RESPONSE;
    }
    if (plan_len == 0 || plan_len > PRDB_SDK_OPTIMIZER_MAX_PLAN_BYTES ||
        !prdb_decode_optimizer_response_success_plan(
            payload + PRDB_OPTIMIZER_RESPONSE_SUCCESS_HEADER_BYTES,
            (size_t)payload_len - PRDB_OPTIMIZER_RESPONSE_SUCCESS_HEADER_BYTES,
            plan_len, &decoded.plan, &decoded_plan_len) ||
        decoded_plan_len != plan_len) {
        return PRDB_SDK_OPTIMIZER_RESULT_INVALID_RESPONSE;
    }
    decoded.plan_len = plan_len;
    *out = decoded;
    return PRDB_SDK_OPTIMIZER_RESULT_OK;
}

static int all_zero_16(const uint8_t id[16]) {
    uint32_t i;
    for (i = 0; i < 16; ++i) {
        if (id[i] != 0) return 0;
    }
    return 1;
}

static int validate_scan_cols_against_executor(
    const prdb_sdk_optimizer_plan_column_v1 *scan_cols, uint32_t scan_count,
    const prdb_sdk_sql_column_shape_v1 *exec_cols, uint32_t exec_count) {
    uint32_t i, j;
    if (scan_count == 0) return 0;
    for (i = 0; i < scan_count; ++i) {
        int found = 0;
        if (all_zero_16(scan_cols[i].column_id)) return 0;
        for (j = 0; j < exec_count; ++j) {
            if (scan_cols[i].ordinal == exec_cols[j].ordinal &&
                scan_cols[i].type_code == (uint8_t)exec_cols[j].type_code) {
                found = 1;
                break;
            }
        }
        if (!found) return 0;
    }
    return 1;
}

int32_t prdb_sdk_optimizer_validate_unified_plan_v1(
    const prdb_host_api_v1 *host,
    const prdb_sdk_optimizer_unified_plan_v1 *plan,
    uint8_t *response, uint64_t response_capacity,
    prdb_sdk_executor_relation_v1 *relation_out, uint32_t relation_out_capacity,
    prdb_sdk_sql_column_shape_v1 *columns_buf, uint32_t columns_capacity) {
    uint32_t i;
    uint64_t response_len = 0;

    if (host == NULL || plan == NULL || response == NULL ||
        relation_out == NULL || columns_buf == NULL ||
        plan->node_count == 0 ||
        plan->node_count > PRDB_SDK_OPTIMIZER_MAX_PHYSICAL_NODES ||
        plan->root_index >= plan->node_count ||
        all_zero_16(plan->schema_id) || plan->schema_version == 0) {
        return PRDB_SDK_OPTIMIZER_PLAN_INVALID_ARGUMENT;
    }

    for (i = 0; i < plan->node_count; ++i) {
        const prdb_sdk_optimizer_physical_node_v1 *n = &plan->nodes[i];
        int is_leaf_scan =
            n->op_tag == PRDB_OPTIMIZER_PHYSICAL_OP_SEQ_SCAN ||
            n->op_tag == PRDB_OPTIMIZER_PHYSICAL_OP_INDEX_POINT_SCAN ||
            n->op_tag == PRDB_OPTIMIZER_PHYSICAL_OP_INDEX_MULTI_POINT_SCAN ||
            n->op_tag == PRDB_OPTIMIZER_PHYSICAL_OP_INDEX_RANGE_SCAN ||
            n->op_tag == PRDB_OPTIMIZER_PHYSICAL_OP_INDEX_ORDERED_SCAN ||
            n->op_tag == PRDB_OPTIMIZER_PHYSICAL_OP_COVERING_INDEX_SCAN;
        int is_index_scan =
            n->op_tag == PRDB_OPTIMIZER_PHYSICAL_OP_INDEX_POINT_SCAN ||
            n->op_tag == PRDB_OPTIMIZER_PHYSICAL_OP_INDEX_MULTI_POINT_SCAN ||
            n->op_tag == PRDB_OPTIMIZER_PHYSICAL_OP_INDEX_RANGE_SCAN ||
            n->op_tag == PRDB_OPTIMIZER_PHYSICAL_OP_INDEX_ORDERED_SCAN ||
            n->op_tag == PRDB_OPTIMIZER_PHYSICAL_OP_COVERING_INDEX_SCAN ||
            n->op_tag == PRDB_OPTIMIZER_PHYSICAL_OP_INDEX_NESTED_LOOP_JOIN;

        if (is_index_scan && all_zero_16(n->index_id)) {
            return PRDB_SDK_OPTIMIZER_PLAN_INVALID_PAYLOAD;
        }
        if (is_leaf_scan || n->op_tag == PRDB_OPTIMIZER_PHYSICAL_OP_INDEX_NESTED_LOOP_JOIN) {
            int32_t rc;
            if (all_zero_16(n->relation_id) || n->table_epoch == 0) {
                return PRDB_SDK_OPTIMIZER_PLAN_INVALID_PAYLOAD;
            }
            rc = prdb_sdk_executor_resolve_relation_v1(
                host, plan->schema_name, plan->schema_name_len,
                n->relation_name, n->relation_name_len,
                response, response_capacity, &response_len,
                relation_out, relation_out_capacity,
                columns_buf, columns_capacity);
            if (rc != PRDB_SDK_EXECUTOR_OK) {
                return PRDB_SDK_OPTIMIZER_PLAN_UNAVAILABLE;
            }
            if (memcmp(relation_out->relation_id, n->relation_id, 16) != 0 ||
                !validate_scan_cols_against_executor(
                    n->scan_columns, n->scan_column_count,
                    columns_buf, relation_out->column_count)) {
                return PRDB_SDK_OPTIMIZER_PLAN_IDENTITY_MISMATCH;
            }
        }
    }
    return PRDB_SDK_EXECUTOR_OK == 0 ? PRDB_SDK_OPTIMIZER_PLAN_OK : PRDB_SDK_OPTIMIZER_PLAN_OK;
}

int32_t prdb_sdk_optimizer_should_spill_unified_node_v1(
    const prdb_sdk_optimizer_physical_node_v1 *node,
    const prdb_sdk_workload_budgets_v1 *budgets,
    uint64_t observed_rows, uint64_t observed_bytes) {
    if (node == NULL || budgets == NULL) return 0;
    if (node->allow_spill) return 1;
    if (budgets->sql_in_mem_max_rows > 0 && observed_rows > budgets->sql_in_mem_max_rows) {
        return 1;
    }
    if (budgets->sql_exec_max_materialized_bytes > 0 &&
        observed_bytes > budgets->sql_exec_max_materialized_bytes) {
        return 1;
    }
    return 0;
}

int32_t prdb_sdk_optimizer_submit_unified_v1(
    const prdb_host_api_v1 *host, uint16_t operation,
    const uint8_t *request, uint64_t request_len,
    uint8_t *response, uint64_t response_capacity, uint64_t *response_len,
    prdb_sdk_optimizer_response_v1 *out, uint32_t out_capacity) {
    int32_t rc;
    uint64_t len = 0;

    if (host == NULL || host->optimizer_request == NULL || request == NULL ||
        request_len == 0 || request_len > PRDB_SDK_OPTIMIZER_MAX_REQUEST_BYTES ||
        response == NULL || response_len == NULL || out == NULL ||
        out_capacity < sizeof(*out)) {
        return PRDB_SDK_OPTIMIZER_RESULT_INVALID_ARGUMENT;
    }
    *response_len = 0;
    rc = host->optimizer_request(host->context, operation, request, request_len,
                                 response, response_capacity, &len);
    if (rc != 0) {
        return PRDB_SDK_OPTIMIZER_RESULT_CALLBACK_ERROR;
    }
    *response_len = len;
    return prdb_sdk_optimizer_decode_response_v1(response, len, out, out_capacity);
}

static uint32_t ir_cmp_to_sql(uint8_t op) {
    switch (op) {
    case 0: return PRDB_SDK_SQL_COMPARE_EQUAL;
    case 1: return PRDB_SDK_SQL_COMPARE_NOT_EQUAL;
    case 2: return PRDB_SDK_SQL_COMPARE_LESS;
    case 3: return PRDB_SDK_SQL_COMPARE_LESS_EQUAL;
    case 4: return PRDB_SDK_SQL_COMPARE_GREATER;
    case 5: return PRDB_SDK_SQL_COMPARE_GREATER_EQUAL;
    default: return 0;
    }
}

static int32_t lookup_scan_cell(
    const prdb_sdk_optimizer_plan_column_v1 *scan_cols, uint32_t scan_count,
    const prdb_sdk_value_v1 *row_cells, uint32_t row_cell_count,
    const uint8_t column_id[16], const prdb_sdk_value_v1 **cell_out) {
    uint32_t i;
    for (i = 0; i < scan_count; ++i) {
        if (memcmp(scan_cols[i].column_id, column_id, 16) == 0) {
            uint32_t ord = scan_cols[i].ordinal;
            if (ord >= row_cell_count) return -1;
            if (row_cells[ord].state == PRDB_SDK_VALUE_STATE_REDACTED) return -2;
            *cell_out = &row_cells[ord];
            return 0;
        }
    }
    return -1;
}

static int32_t eval_bound_scalar_operand(
    unified_reader *r,
    const prdb_sdk_optimizer_plan_column_v1 *scan_cols, uint32_t scan_count,
    const prdb_sdk_value_v1 *row_cells, uint32_t row_cell_count,
    const prdb_sdk_value_v1 *params, uint32_t param_count,
    prdb_sdk_value_v1 *val_out, uint8_t buf_out[16], int64_t *i64_out, int *is_i64_out) {
    uint8_t tag, b;
    uint16_t len;
    const uint8_t *p;
    uint32_t i;

    *is_i64_out = 0;
    memset(val_out, 0, sizeof(*val_out));
    val_out->struct_size = (uint32_t)sizeof(*val_out);
    val_out->abi_version = PRDB_SDK_VALUE_ABI_V1_VERSION;

    if (!read_u8(r, &tag)) return -1;
    switch (tag) {
    case 0x00:
        val_out->state = PRDB_SDK_VALUE_STATE_NULL;
        return 0;
    case 0x01:
        if (!read_u8(r, &b) || b > 1) return -1;
        buf_out[0] = b;
        val_out->state = PRDB_SDK_VALUE_STATE_VALUE;
        val_out->type_code = PRDB_SDK_VALUE_TYPE_BOOLEAN;
        val_out->data = buf_out;
        val_out->data_len = 1;
        return 0;
    case 0x02:
        if (!take_bytes(r, 8, &p)) return -1;
        *i64_out = (int64_t)(
            ((uint64_t)p[0] << 56) | ((uint64_t)p[1] << 48) |
            ((uint64_t)p[2] << 40) | ((uint64_t)p[3] << 32) |
            ((uint64_t)p[4] << 24) | ((uint64_t)p[5] << 16) |
            ((uint64_t)p[6] << 8) | (uint64_t)p[7]);
        *is_i64_out = 1;
        memcpy(buf_out, p, 8);
        val_out->state = PRDB_SDK_VALUE_STATE_VALUE;
        val_out->type_code = PRDB_SDK_VALUE_TYPE_BIGINT;
        val_out->data = buf_out;
        val_out->data_len = 8;
        return 0;
    case 0x03:
    case 0x07:
        if (!read_u16(r, &len) || len > 4096 || !take_bytes(r, len, &p)) return -1;
        val_out->state = PRDB_SDK_VALUE_STATE_VALUE;
        val_out->type_code = (tag == 0x03) ? PRDB_SDK_VALUE_TYPE_TEXT : PRDB_SDK_VALUE_TYPE_BYTEA;
        val_out->data = p;
        val_out->data_len = len;
        return 0;
    case 0x04:
        if (!take_bytes(r, 4, &p)) return -1;
        val_out->state = PRDB_SDK_VALUE_STATE_VALUE;
        val_out->type_code = PRDB_SDK_VALUE_TYPE_DATE;
        val_out->data = p;
        val_out->data_len = 4;
        return 0;
    case 0x05:
        if (!take_bytes(r, 16, &p)) return -1;
        val_out->state = PRDB_SDK_VALUE_STATE_VALUE;
        val_out->type_code = PRDB_SDK_VALUE_TYPE_UUID;
        val_out->data = p;
        val_out->data_len = 16;
        return 0;
    case 0x06:
        if (!take_bytes(r, 8, &p)) return -1;
        val_out->state = PRDB_SDK_VALUE_STATE_VALUE;
        val_out->type_code = PRDB_SDK_VALUE_TYPE_TIMESTAMPTZ;
        val_out->data = p;
        val_out->data_len = 8;
        return 0;
    case 0x08:
        if (!take_bytes(r, 19, &p)) return -1;
        val_out->state = PRDB_SDK_VALUE_STATE_VALUE;
        val_out->type_code = PRDB_SDK_VALUE_TYPE_DECIMAL;
        val_out->data = p;
        val_out->data_len = 19;
        return 0;
    case 0x10: {
        uint16_t rel_idx;
        prdb_sdk_optimizer_plan_column_v1 col;
        const prdb_sdk_value_v1 *cell = NULL;
        int32_t rc;
        if (!read_u16(r, &rel_idx) || !read_bound_col(r, &col)) return -1;
        rc = lookup_scan_cell(scan_cols, scan_count, row_cells, row_cell_count,
                              col.column_id, &cell);
        if (rc < 0) return rc;
        *val_out = *cell;
        return 0;
    }
    case 0x11: {
        uint32_t pidx;
        if (!read_u32(r, &pidx) || pidx == 0 || pidx > param_count || params == NULL) return -1;
        *val_out = params[pidx - 1];
        return 0;
    }
    default:
        return -1;
    }
}

static int32_t compare_bound_scalars(const prdb_sdk_value_v1 *lhs_in, int lhs_is_i64, int64_t lhs_i64,
                                     const prdb_sdk_value_v1 *rhs_in, int rhs_is_i64, int64_t rhs_i64,
                                     uint8_t ir_op, int32_t *truth_out) {
    prdb_sdk_value_v1 lhs = *lhs_in, rhs = *rhs_in;
    uint8_t lbuf[4], rbuf[4];
    uint32_t truth = 0;
    uint32_t sql_cmp = ir_cmp_to_sql(ir_op);

    if (sql_cmp == 0) return -1;
    if (lhs.state == PRDB_SDK_VALUE_STATE_NULL || rhs.state == PRDB_SDK_VALUE_STATE_NULL) {
        *truth_out = -1;
        return 0;
    }
    if (lhs_is_i64 && rhs.type_code == PRDB_SDK_VALUE_TYPE_INTEGER &&
        lhs_i64 >= INT32_MIN && lhs_i64 <= INT32_MAX) {
        int32_t v = (int32_t)lhs_i64;
        lbuf[0] = (uint8_t)((uint32_t)v >> 24); lbuf[1] = (uint8_t)((uint32_t)v >> 16);
        lbuf[2] = (uint8_t)((uint32_t)v >> 8); lbuf[3] = (uint8_t)(uint32_t)v;
        lhs.type_code = PRDB_SDK_VALUE_TYPE_INTEGER;
        lhs.data = lbuf; lhs.data_len = 4;
    }
    if (rhs_is_i64 && lhs.type_code == PRDB_SDK_VALUE_TYPE_INTEGER &&
        rhs_i64 >= INT32_MIN && rhs_i64 <= INT32_MAX) {
        int32_t v = (int32_t)rhs_i64;
        rbuf[0] = (uint8_t)((uint32_t)v >> 24); rbuf[1] = (uint8_t)((uint32_t)v >> 16);
        rbuf[2] = (uint8_t)((uint32_t)v >> 8); rbuf[3] = (uint8_t)(uint32_t)v;
        rhs.type_code = PRDB_SDK_VALUE_TYPE_INTEGER;
        rhs.data = rbuf; rhs.data_len = 4;
    }
    if (prdb_sdk_sql_compare_v1(&lhs, &rhs, sql_cmp, 0, 0, &truth) != PRDB_SDK_SQL_EVAL_OK) {
        return -1;
    }
    if (truth == PRDB_SDK_SQL_TRUTH_TRUE) *truth_out = 1;
    else if (truth == PRDB_SDK_SQL_TRUTH_FALSE) *truth_out = 0;
    else *truth_out = -1;
    return 0;
}

static int32_t eval_unified_bound_expr(
    unified_reader *r,
    const prdb_sdk_optimizer_plan_column_v1 *scan_cols, uint32_t scan_count,
    const prdb_sdk_value_v1 *row_cells, uint32_t row_cell_count,
    const prdb_sdk_value_v1 *params, uint32_t param_count,
    int32_t *truth_out) {
    uint8_t tag, op, count;
    uint32_t i;
    const uint8_t *p;

    if (!read_u8(r, &tag)) return -1;
    switch (tag) {
    case 0x00:
        *truth_out = -1; /* UNKNOWN / NULL */
        return 0;
    case 0x01:
        if (!read_u8(r, &op) || op > 1) return -1;
        *truth_out = op ? 1 : 0;
        return 0;
    case 0x10: {
        uint16_t rel_idx;
        prdb_sdk_optimizer_plan_column_v1 col;
        const prdb_sdk_value_v1 *cell = NULL;
        int32_t rc;
        if (!read_u16(r, &rel_idx) || !read_bound_col(r, &col)) return -1;
        rc = lookup_scan_cell(scan_cols, scan_count, row_cells, row_cell_count,
                              col.column_id, &cell);
        if (rc < 0) return rc;
        if (cell->state == PRDB_SDK_VALUE_STATE_NULL) {
            *truth_out = -1;
            return 0;
        }
        if (cell->type_code == PRDB_SDK_VALUE_TYPE_BOOLEAN && cell->data_len == 1) {
            *truth_out = cell->data[0] ? 1 : 0;
            return 0;
        }
        return -1;
    }
    case 0x11: {
        uint32_t pidx;
        if (!read_u32(r, &pidx) || pidx == 0 || pidx > param_count || params == NULL) return -1;
        if (params[pidx - 1].state == PRDB_SDK_VALUE_STATE_NULL) {
            *truth_out = -1;
            return 0;
        }
        if (params[pidx - 1].type_code == PRDB_SDK_VALUE_TYPE_BOOLEAN &&
            params[pidx - 1].data_len == 1) {
            *truth_out = params[pidx - 1].data[0] ? 1 : 0;
            return 0;
        }
        return -1;
    }
    case 0x20: {
        uint8_t cmp_op;
        prdb_sdk_value_v1 lhs, rhs;
        uint8_t lbuf[16], rbuf[16];
        int64_t li = 0, ri = 0;
        int l_i64 = 0, r_i64 = 0;
        int32_t rc;
        if (!read_u8(r, &cmp_op)) return -1;
        rc = eval_bound_scalar_operand(r, scan_cols, scan_count, row_cells, row_cell_count,
                                       params, param_count, &lhs, lbuf, &li, &l_i64);
        if (rc < 0) return rc;
        rc = eval_bound_scalar_operand(r, scan_cols, scan_count, row_cells, row_cell_count,
                                       params, param_count, &rhs, rbuf, &ri, &r_i64);
        if (rc < 0) return rc;
        return compare_bound_scalars(&lhs, l_i64, li, &rhs, r_i64, ri, cmp_op, truth_out);
    }
    case 0x30: {
        int32_t acc = 1;
        if (!read_u8(r, &count) || count < 2 || count > 16) return -1;
        for (i = 0; i < count; ++i) {
            int32_t child_t = 0;
            int32_t rc = eval_unified_bound_expr(r, scan_cols, scan_count, row_cells,
                                                 row_cell_count, params, param_count, &child_t);
            if (rc < 0) return rc;
            if (child_t == 0) acc = 0;
            else if (child_t == -1 && acc == 1) acc = -1;
        }
        *truth_out = acc;
        return 0;
    }
    case 0x31: {
        int32_t acc = 0;
        if (!read_u8(r, &count) || count < 2 || count > 16) return -1;
        for (i = 0; i < count; ++i) {
            int32_t child_t = 0;
            int32_t rc = eval_unified_bound_expr(r, scan_cols, scan_count, row_cells,
                                                 row_cell_count, params, param_count, &child_t);
            if (rc < 0) return rc;
            if (child_t == 1) acc = 1;
            else if (child_t == -1 && acc == 0) acc = -1;
        }
        *truth_out = acc;
        return 0;
    }
    case 0x32: {
        int32_t child_t = 0;
        int32_t rc = eval_unified_bound_expr(r, scan_cols, scan_count, row_cells,
                                             row_cell_count, params, param_count, &child_t);
        if (rc < 0) return rc;
        *truth_out = child_t < 0 ? -1 : (child_t ? 0 : 1);
        return 0;
    }
    case 0x40:
    case 0x41: {
        uint8_t inner_tag;
        uint16_t rel_idx;
        prdb_sdk_optimizer_plan_column_v1 col;
        const prdb_sdk_value_v1 *cell = NULL;
        int32_t rc;
        int is_null;
        if (!read_u8(r, &inner_tag) || inner_tag != 0x10 ||
            !read_u16(r, &rel_idx) || !read_bound_col(r, &col)) {
            return -1;
        }
        rc = lookup_scan_cell(scan_cols, scan_count, row_cells, row_cell_count,
                              col.column_id, &cell);
        if (rc < 0) return rc;
        is_null = cell->state == PRDB_SDK_VALUE_STATE_NULL;
        *truth_out = (tag == 0x41) ? !is_null : is_null;
        return 0;
    }
    case 0x42: {
        uint8_t negated, cnt;
        prdb_sdk_value_v1 target, item;
        uint8_t tbuf[16], ibuf[16];
        int64_t ti = 0, ii = 0;
        int t_i64 = 0, i_i64 = 0;
        int32_t rc, matched = 0, has_null = 0;
        if (!read_u8(r, &negated) || !read_u8(r, &cnt) || cnt == 0 || cnt > 16) return -1;
        rc = eval_bound_scalar_operand(r, scan_cols, scan_count, row_cells, row_cell_count,
                                       params, param_count, &target, tbuf, &ti, &t_i64);
        if (rc < 0) return rc;
        for (i = 0; i < cnt; ++i) {
            int32_t eq_t = 0;
            rc = eval_bound_scalar_operand(r, scan_cols, scan_count, row_cells, row_cell_count,
                                           params, param_count, &item, ibuf, &ii, &i_i64);
            if (rc < 0) return rc;
            rc = compare_bound_scalars(&target, t_i64, ti, &item, i_i64, ii, 0, &eq_t);
            if (rc < 0) return rc;
            if (eq_t == 1) matched = 1;
            else if (eq_t == -1) has_null = 1;
        }
        if (matched) *truth_out = negated ? 0 : 1;
        else if (has_null) *truth_out = -1;
        else *truth_out = negated ? 1 : 0;
        return 0;
    }
    default:
        /* Skip unsupported scalar literal subtrees cleanly if needed */
        (void)p;
        return -1;
    }
}

static int32_t compare_sort_values(const prdb_sdk_value_v1 *a,
                                   const prdb_sdk_value_v1 *b,
                                   uint8_t descending) {
    int a_null = (a->state == PRDB_SDK_VALUE_STATE_NULL);
    int b_null = (b->state == PRDB_SDK_VALUE_STATE_NULL);
    uint32_t eq = 0, lt = 0;
    if (a_null && b_null) return 0;
    if (a_null) return 1;
    if (b_null) return -1;
    if (prdb_sdk_sql_compare_v1(a, b, PRDB_SDK_SQL_COMPARE_EQUAL, 0, 0, &eq) ==
            PRDB_SDK_SQL_EVAL_OK &&
        eq == PRDB_SDK_SQL_TRUTH_TRUE) {
        return 0;
    }
    if (prdb_sdk_sql_compare_v1(a, b, PRDB_SDK_SQL_COMPARE_LESS, 0, 0, &lt) ==
            PRDB_SDK_SQL_EVAL_OK &&
        (lt == PRDB_SDK_SQL_TRUTH_TRUE || lt == PRDB_SDK_SQL_TRUTH_FALSE)) {
        int32_t cmp = (lt == PRDB_SDK_SQL_TRUTH_TRUE) ? -1 : 1;
        return descending ? -cmp : cmp;
    }
    return 0;
}

typedef struct unified_sort_row {
    prdb_sdk_value_v1 sort_key;
    uint8_t sort_key_bytes[64];
    prdb_sdk_value_v1 proj[16];
    uint8_t proj_bytes[16][64];
} unified_sort_row;

static void copy_value_inline(prdb_sdk_value_v1 *dst, uint8_t buf[64],
                              const prdb_sdk_value_v1 *src) {
    *dst = *src;
    if (src->state == PRDB_SDK_VALUE_STATE_VALUE && src->data != NULL &&
        src->data_len <= 64) {
        if (src->data_len > 0) memcpy(buf, src->data, (size_t)src->data_len);
        dst->data = buf;
    }
}

static int32_t eval_scan_and_filter_match(
    const prdb_sdk_optimizer_physical_node_v1 *scan_node,
    const prdb_sdk_optimizer_physical_node_v1 *filter_node,
    const prdb_sdk_value_v1 *row_cells, uint32_t cell_count,
    const prdb_sdk_value_v1 *parameters, uint32_t parameter_count,
    int32_t *matched_out) {
    *matched_out = 0;
    if (scan_node != NULL &&
        scan_node->op_tag == PRDB_OPTIMIZER_PHYSICAL_OP_INDEX_POINT_SCAN) {
        uint32_t k;
        for (k = 0; k < scan_node->key_count && k < 8; ++k) {
            const prdb_sdk_value_v1 *cell = NULL;
            prdb_sdk_value_v1 rhs = {0};
            uint8_t rbuf[16];
            int64_t ri = 0;
            int r_i64 = 0;
            int32_t truth = 0;
            unified_reader kr;
            if (scan_node->key_value_prefixes[k] == NULL) {
                return PRDB_SDK_OPTIMIZER_PLAN_UNSUPPORTED;
            }
            if (lookup_scan_cell(scan_node->scan_columns, scan_node->scan_column_count,
                                 row_cells, cell_count,
                                 scan_node->key_columns[k].column_id, &cell) < 0) {
                return PRDB_SDK_OPTIMIZER_PLAN_UNSUPPORTED;
            }
            kr.bytes = scan_node->key_value_prefixes[k];
            kr.length = scan_node->key_value_prefix_lens[k];
            kr.offset = 0;
            if (eval_bound_scalar_operand(&kr, scan_node->scan_columns, scan_node->scan_column_count,
                                          row_cells, cell_count, parameters, parameter_count,
                                          &rhs, rbuf, &ri, &r_i64) < 0) {
                return PRDB_SDK_OPTIMIZER_PLAN_UNSUPPORTED;
            }
            if (compare_bound_scalars(cell, 0, 0, &rhs, r_i64, ri, 0, &truth) < 0) {
                return PRDB_SDK_OPTIMIZER_PLAN_UNSUPPORTED;
            }
            if (truth != 1) return PRDB_SDK_OPTIMIZER_PLAN_OK;
        }
    } else if (scan_node != NULL &&
               scan_node->op_tag == PRDB_OPTIMIZER_PHYSICAL_OP_INDEX_RANGE_SCAN &&
               scan_node->key_count >= 1) {
        const prdb_sdk_value_v1 *cell = NULL;
        if (lookup_scan_cell(scan_node->scan_columns, scan_node->scan_column_count,
                             row_cells, cell_count,
                             scan_node->key_columns[0].column_id, &cell) < 0) {
            return PRDB_SDK_OPTIMIZER_PLAN_UNSUPPORTED;
        }
        if (scan_node->has_lower_bound) {
            prdb_sdk_value_v1 lval = {0};
            uint8_t lbuf[16];
            int64_t li = 0;
            int l_i64 = 0;
            int32_t truth = 0;
            unified_reader lr = {scan_node->lower_bound_prefix, scan_node->lower_bound_prefix_len, 0};
            if (eval_bound_scalar_operand(&lr, scan_node->scan_columns, scan_node->scan_column_count,
                                          row_cells, cell_count, parameters, parameter_count,
                                          &lval, lbuf, &li, &l_i64) < 0 ||
                compare_bound_scalars(cell, 0, 0, &lval, l_i64, li,
                                      scan_node->lower_inclusive ? 5 : 4, &truth) < 0) {
                return PRDB_SDK_OPTIMIZER_PLAN_UNSUPPORTED;
            }
            if (truth != 1) return PRDB_SDK_OPTIMIZER_PLAN_OK;
        }
        if (scan_node->has_upper_bound) {
            prdb_sdk_value_v1 uval = {0};
            uint8_t ubuf[16];
            int64_t ui = 0;
            int u_i64 = 0;
            int32_t truth = 0;
            unified_reader ur = {scan_node->upper_bound_prefix, scan_node->upper_bound_prefix_len, 0};
            if (eval_bound_scalar_operand(&ur, scan_node->scan_columns, scan_node->scan_column_count,
                                          row_cells, cell_count, parameters, parameter_count,
                                          &uval, ubuf, &ui, &u_i64) < 0 ||
                compare_bound_scalars(cell, 0, 0, &uval, u_i64, ui,
                                      scan_node->upper_inclusive ? 3 : 2, &truth) < 0) {
                return PRDB_SDK_OPTIMIZER_PLAN_UNSUPPORTED;
            }
            if (truth != 1) return PRDB_SDK_OPTIMIZER_PLAN_OK;
        }
    }
    if (filter_node != NULL && filter_node->has_predicate) {
        unified_reader fr;
        int32_t truth = 0;
        int32_t eval_rc;
        fr.bytes = filter_node->predicate_prefix;
        fr.length = filter_node->predicate_prefix_len;
        fr.offset = 0;
        eval_rc = eval_unified_bound_expr(
            &fr, scan_node->scan_columns, scan_node->scan_column_count,
            row_cells, cell_count, parameters, parameter_count, &truth);
        if (eval_rc == -2) return PRDB_SDK_OPTIMIZER_PLAN_FILTER_ERROR;
        if (eval_rc < 0) return PRDB_SDK_OPTIMIZER_PLAN_UNSUPPORTED;
        if (truth != 1) return PRDB_SDK_OPTIMIZER_PLAN_OK;
    }
    *matched_out = 1;
    return PRDB_SDK_OPTIMIZER_PLAN_OK;
}

static int32_t execute_unified_select_pipeline_v1(
    const prdb_host_api_v1 *host,
    const prdb_sdk_optimizer_unified_plan_v1 *plan,
    const prdb_sdk_value_v1 *parameters, uint32_t parameter_count,
    uint8_t *relation_response, uint64_t relation_response_capacity,
    uint8_t *scan_response, uint64_t scan_response_capacity,
    uint8_t *batch_response, uint64_t batch_response_capacity,
    prdb_sdk_executor_relation_v1 *relation_out, uint32_t relation_out_capacity,
    prdb_sdk_sql_column_shape_v1 *relation_columns, uint32_t relation_column_capacity,
    prdb_sdk_value_v1 *source_cells, uint32_t source_cell_capacity,
    prdb_sdk_value_v1 *projected_values, uint32_t projected_value_capacity,
    uint8_t *result_record_buffer, uint64_t result_record_capacity,
    prdb_sdk_optimizer_projected_row_v1_fn on_row, void *row_context,
    uint32_t *delivered_rows_out) {
    int32_t rc;
    uint16_t cur;
    const prdb_sdk_optimizer_physical_node_v1 *scan_node = NULL;
    const prdb_sdk_optimizer_physical_node_v1 *filter_node = NULL;
    const prdb_sdk_optimizer_physical_node_v1 *project_node = NULL;
    const prdb_sdk_optimizer_physical_node_v1 *sort_node = NULL;
    uint32_t has_limit = 0;
    uint32_t limit = 0;
    uint32_t delivered = 0;
    uint8_t cursor[16];
    uint64_t resp_len = 0;
    uint32_t page = 0;
    uint32_t proj_ordinals[PRDB_SDK_OPTIMIZER_PLAN_MAX_PROJECTIONS];
    uint32_t sort_ordinal = 0;
    uint32_t i, j;

    if (host == NULL || plan == NULL || delivered_rows_out == NULL ||
        relation_response == NULL || scan_response == NULL || batch_response == NULL ||
        relation_out == NULL || relation_columns == NULL ||
        source_cells == NULL || projected_values == NULL ||
        (result_record_buffer == NULL && on_row == NULL)) {
        return PRDB_SDK_OPTIMIZER_PLAN_INVALID_ARGUMENT;
    }
    *delivered_rows_out = 0;

    rc = prdb_sdk_optimizer_validate_unified_plan_v1(
        host, plan, relation_response, relation_response_capacity,
        relation_out, relation_out_capacity, relation_columns, relation_column_capacity);
    if (rc != PRDB_SDK_OPTIMIZER_PLAN_OK) return rc;

    /* Walk linear pipeline from root down to leaf scan / EmptyResult. */
    cur = plan->root_index;
    for (i = 0; i < plan->node_count; ++i) {
        const prdb_sdk_optimizer_physical_node_v1 *n = &plan->nodes[cur];
        if (n->op_tag == PRDB_OPTIMIZER_PHYSICAL_OP_EMPTY_RESULT) {
            return PRDB_SDK_OPTIMIZER_PLAN_OK;
        }
        if (n->op_tag == PRDB_OPTIMIZER_PHYSICAL_OP_LIMIT) {
            has_limit = 1;
            limit = n->limit;
            if (limit == 0) return PRDB_SDK_OPTIMIZER_PLAN_OK;
            cur = n->left_child;
        } else if (n->op_tag == PRDB_OPTIMIZER_PHYSICAL_OP_TOPN_HEAP) {
            has_limit = 1;
            limit = n->limit;
            if (limit == 0) return PRDB_SDK_OPTIMIZER_PLAN_OK;
            sort_node = n;
            cur = n->left_child;
        } else if (n->op_tag == PRDB_OPTIMIZER_PHYSICAL_OP_SORT) {
            sort_node = n;
            cur = n->left_child;
        } else if (n->op_tag == PRDB_OPTIMIZER_PHYSICAL_OP_PROJECT) {
            project_node = n;
            cur = n->left_child;
        } else if (n->op_tag == PRDB_OPTIMIZER_PHYSICAL_OP_FILTER) {
            filter_node = n;
            cur = n->left_child;
        } else if (n->op_tag == PRDB_OPTIMIZER_PHYSICAL_OP_SEQ_SCAN ||
                   n->op_tag == PRDB_OPTIMIZER_PHYSICAL_OP_INDEX_POINT_SCAN ||
                   n->op_tag == PRDB_OPTIMIZER_PHYSICAL_OP_INDEX_RANGE_SCAN ||
                   n->op_tag == PRDB_OPTIMIZER_PHYSICAL_OP_INDEX_ORDERED_SCAN ||
                   n->op_tag == PRDB_OPTIMIZER_PHYSICAL_OP_COVERING_INDEX_SCAN) {
            scan_node = n;
            if (n->op_tag == PRDB_OPTIMIZER_PHYSICAL_OP_INDEX_ORDERED_SCAN) {
                if (sort_node == NULL) sort_node = n;
                if (n->has_limit && (!has_limit || n->limit < limit)) {
                    has_limit = 1;
                    limit = n->limit;
                }
            }
            break;
        } else {
            return PRDB_SDK_OPTIMIZER_PLAN_UNSUPPORTED;
        }
    }
    if (scan_node == NULL) return PRDB_SDK_OPTIMIZER_PLAN_UNSUPPORTED;

    const prdb_sdk_optimizer_plan_column_v1 *out_cols =
        project_node != NULL ? project_node->scan_columns : scan_node->scan_columns;
    uint32_t out_count =
        project_node != NULL ? project_node->scan_column_count : scan_node->scan_column_count;
    if (out_count > projected_value_capacity) return PRDB_SDK_OPTIMIZER_PLAN_INVALID_ARGUMENT;

    for (i = 0; i < out_count; ++i) {
        int found = 0;
        for (j = 0; j < relation_out->column_count; ++j) {
            if (relation_columns[j].ordinal == out_cols[i].ordinal) {
                proj_ordinals[i] = j;
                found = 1;
                break;
            }
        }
        if (!found) return PRDB_SDK_OPTIMIZER_PLAN_IDENTITY_MISMATCH;
    }

    if (sort_node != NULL) {
        int found_sort = 0;
        if (sort_node->key_count == 0 || out_count > 16) {
            return PRDB_SDK_OPTIMIZER_PLAN_UNSUPPORTED;
        }
        for (j = 0; j < relation_out->column_count; ++j) {
            if (relation_columns[j].ordinal == sort_node->key_columns[0].ordinal) {
                sort_ordinal = j;
                found_sort = 1;
                break;
            }
        }
        if (!found_sort) return PRDB_SDK_OPTIMIZER_PLAN_IDENTITY_MISMATCH;
    }

    if (sort_node == NULL && filter_node == NULL &&
        ((scan_node->op_tag == PRDB_OPTIMIZER_PHYSICAL_OP_INDEX_POINT_SCAN &&
          scan_node->key_count == 1 && scan_node->has_predicate) ||
         (scan_node->op_tag == PRDB_OPTIMIZER_PHYSICAL_OP_INDEX_RANGE_SCAN &&
          scan_node->key_count == 1))) {
        prdb_sdk_value_v1 lval = {0}, uval = {0};
        uint8_t lbuf[16], ubuf[16], li32[4], ui32[4];
        int64_t li = 0, ui = 0;
        int l_i64 = 0, u_i64 = 0;
        int has_l = 0, has_u = 0;
        uint8_t l_inc = 0, u_inc = 0;
        uint8_t col_ids[PRDB_SDK_OPTIMIZER_PLAN_MAX_PROJECTIONS * 16];
        prdb_sdk_index_range_v1 range = {0};
        prdb_sdk_index_range_row_iterator_v1 rit = {0};
        prdb_sdk_index_range_row_v1 rrow = {0};
        uint8_t result_buf_tmp[4096];
        uint8_t *rbuf = result_record_buffer ? result_record_buffer : result_buf_tmp;
        uint64_t rcap = result_record_buffer ? result_record_capacity : sizeof(result_buf_tmp);

        if (scan_node->op_tag == PRDB_OPTIMIZER_PHYSICAL_OP_INDEX_POINT_SCAN) {
            unified_reader kr = {scan_node->predicate_prefix, scan_node->predicate_prefix_len, 0};
            if (eval_bound_scalar_operand(&kr, NULL, 0, NULL, 0, parameters, parameter_count,
                                          &lval, lbuf, &li, &l_i64) < 0) {
                return PRDB_SDK_OPTIMIZER_PLAN_UNSUPPORTED;
            }
            if (lval.state != PRDB_SDK_VALUE_STATE_VALUE) {
                return PRDB_SDK_OPTIMIZER_PLAN_OK;
            }
            if (l_i64 && scan_node->key_columns[0].type_code == PRDB_SDK_VALUE_TYPE_INTEGER &&
                li >= INT32_MIN && li <= INT32_MAX) {
                int32_t v = (int32_t)li;
                li32[0] = (uint8_t)((uint32_t)v >> 24); li32[1] = (uint8_t)((uint32_t)v >> 16);
                li32[2] = (uint8_t)((uint32_t)v >> 8); li32[3] = (uint8_t)(uint32_t)v;
                lval.type_code = PRDB_SDK_VALUE_TYPE_INTEGER;
                lval.data = li32; lval.data_len = 4;
            }
            uval = lval;
            has_l = 1; has_u = 1;
            l_inc = 1; u_inc = 1;
        } else {
            if (scan_node->has_lower_bound) {
                unified_reader lr = {scan_node->lower_bound_prefix, scan_node->lower_bound_prefix_len, 0};
                if (eval_bound_scalar_operand(&lr, NULL, 0, NULL, 0, parameters, parameter_count,
                                              &lval, lbuf, &li, &l_i64) < 0) {
                    return PRDB_SDK_OPTIMIZER_PLAN_UNSUPPORTED;
                }
                if (lval.state != PRDB_SDK_VALUE_STATE_VALUE) {
                    return PRDB_SDK_OPTIMIZER_PLAN_OK;
                }
                if (l_i64 && scan_node->key_columns[0].type_code == PRDB_SDK_VALUE_TYPE_INTEGER &&
                    li >= INT32_MIN && li <= INT32_MAX) {
                    int32_t v = (int32_t)li;
                    li32[0] = (uint8_t)((uint32_t)v >> 24); li32[1] = (uint8_t)((uint32_t)v >> 16);
                    li32[2] = (uint8_t)((uint32_t)v >> 8); li32[3] = (uint8_t)(uint32_t)v;
                    lval.type_code = PRDB_SDK_VALUE_TYPE_INTEGER;
                    lval.data = li32; lval.data_len = 4;
                }
                has_l = 1;
                l_inc = scan_node->lower_inclusive ? 1 : 0;
            }
            if (scan_node->has_upper_bound) {
                unified_reader ur = {scan_node->upper_bound_prefix, scan_node->upper_bound_prefix_len, 0};
                if (eval_bound_scalar_operand(&ur, NULL, 0, NULL, 0, parameters, parameter_count,
                                              &uval, ubuf, &ui, &u_i64) < 0) {
                    return PRDB_SDK_OPTIMIZER_PLAN_UNSUPPORTED;
                }
                if (uval.state != PRDB_SDK_VALUE_STATE_VALUE) {
                    return PRDB_SDK_OPTIMIZER_PLAN_OK;
                }
                if (u_i64 && scan_node->key_columns[0].type_code == PRDB_SDK_VALUE_TYPE_INTEGER &&
                    ui >= INT32_MIN && ui <= INT32_MAX) {
                    int32_t v = (int32_t)ui;
                    ui32[0] = (uint8_t)((uint32_t)v >> 24); ui32[1] = (uint8_t)((uint32_t)v >> 16);
                    ui32[2] = (uint8_t)((uint32_t)v >> 8); ui32[3] = (uint8_t)(uint32_t)v;
                    uval.type_code = PRDB_SDK_VALUE_TYPE_INTEGER;
                    uval.data = ui32; uval.data_len = 4;
                }
                has_u = 1;
                u_inc = scan_node->upper_inclusive ? 1 : 0;
            }
        }

        for (i = 0; i < out_count; ++i) {
            memcpy(col_ids + (size_t)i * 16, out_cols[i].column_id, 16);
        }
        rc = prdb_sdk_executor_scan_index_range_v1(
            host, relation_out->executor_handle, scan_node->index_id,
            has_l ? &lval : NULL, l_inc, has_u ? &uval : NULL, u_inc,
            col_ids, (uint16_t)out_count, scan_response, scan_response_capacity,
            batch_response, batch_response_capacity, &resp_len,
            &range, sizeof(range));
        if (rc == PRDB_SDK_EXECUTOR_OK) {
            if (prdb_sdk_index_range_rows_begin_v1(&range, &rit, sizeof(rit)) != PRDB_SDK_BATCH_OK) {
                return PRDB_SDK_OPTIMIZER_PLAN_INVALID_PAYLOAD;
            }
            while (prdb_sdk_index_range_row_next_v1(&rit, &rrow, sizeof(rrow)) == PRDB_SDK_BATCH_OK) {
                prdb_sdk_cell_iterator_v1 cit;
                uint32_t cidx = 0;
                if (prdb_sdk_index_range_row_cells_begin_v1(&rrow, &cit, sizeof(cit)) != PRDB_SDK_BATCH_OK) {
                    return PRDB_SDK_OPTIMIZER_PLAN_INVALID_PAYLOAD;
                }
                while (cidx < out_count && cidx < projected_value_capacity &&
                       prdb_sdk_cell_next_v1(&cit, &projected_values[cidx], sizeof(projected_values[0])) == PRDB_SDK_BATCH_OK) {
                    cidx++;
                }
                if (cidx != out_count) return PRDB_SDK_OPTIMIZER_PLAN_INVALID_PAYLOAD;
                if (on_row != NULL) {
                    if (on_row(row_context, out_cols, projected_values, out_count) != 0) {
                        return PRDB_SDK_OPTIMIZER_PLAN_ROW_CALLBACK_ERROR;
                    }
                } else if (prdb_sdk_sql_write_projected_result_v1(
                               host, relation_columns, relation_out->column_count,
                               proj_ordinals, projected_values, out_count,
                               rbuf, rcap, &resp_len) != PRDB_SDK_SQL_RESULT_OK) {
                    return PRDB_SDK_OPTIMIZER_PLAN_ROW_CALLBACK_ERROR;
                }
                delivered++;
                if (has_limit && delivered >= limit) break;
            }
            *delivered_rows_out = delivered;
            return PRDB_SDK_OPTIMIZER_PLAN_OK;
        }
    }

    rc = prdb_sdk_executor_open_full_scan_v1(
        host, relation_out->executor_handle, scan_response, scan_response_capacity,
        &resp_len, cursor);
    if (rc != PRDB_SDK_EXECUTOR_OK) return PRDB_SDK_OPTIMIZER_PLAN_UNAVAILABLE;

    unified_sort_row sort_rows[128];
    uint32_t sort_row_count = 0;
    int cursor_finished = 0;

    for (page = 0; page < PRDB_SDK_OPTIMIZER_PLAN_MAX_NEXT_REQUESTS; ++page) {
        int32_t cancelled = 0;
        prdb_sdk_batch_v1 batch;
        prdb_sdk_row_iterator_v1 row_it;
        prdb_sdk_row_v1 row;

        if (prdb_sdk_executor_check_cancelled_v1(host, &cancelled) != PRDB_SDK_EXECUTOR_OK ||
            cancelled) {
            if (!cursor_finished) prdb_sdk_executor_close_cursor_v1(host, cursor, scan_response, scan_response_capacity, &resp_len);
            return PRDB_SDK_OPTIMIZER_PLAN_CANCELLED;
        }
        rc = prdb_sdk_executor_next_batch_v1(
            host, cursor, 128, (uint32_t)batch_response_capacity,
            batch_response, batch_response_capacity, &resp_len, &batch, sizeof(batch));
        if (rc != PRDB_SDK_EXECUTOR_OK) {
            if (!cursor_finished) prdb_sdk_executor_close_cursor_v1(host, cursor, scan_response, scan_response_capacity, &resp_len);
            return PRDB_SDK_OPTIMIZER_PLAN_CALLBACK_ERROR;
        }
        cursor_finished = batch.finished ? 1 : 0;
        if (prdb_sdk_batch_rows_begin_v1(&batch, &row_it, sizeof(row_it)) != PRDB_SDK_BATCH_OK) {
            if (!cursor_finished) prdb_sdk_executor_close_cursor_v1(host, cursor, scan_response, scan_response_capacity, &resp_len);
            return PRDB_SDK_OPTIMIZER_PLAN_INVALID_PAYLOAD;
        }
        while (prdb_sdk_batch_row_next_v1(&row_it, &row, sizeof(row)) == PRDB_SDK_BATCH_OK) {
            prdb_sdk_cell_iterator_v1 cell_it;
            uint32_t cell_idx = 0;
            if (prdb_sdk_row_cells_begin_v1(&row, &cell_it, sizeof(cell_it)) != PRDB_SDK_BATCH_OK) {
                if (!cursor_finished) prdb_sdk_executor_close_cursor_v1(host, cursor, scan_response, scan_response_capacity, &resp_len);
                return PRDB_SDK_OPTIMIZER_PLAN_INVALID_PAYLOAD;
            }
            while (cell_idx < source_cell_capacity &&
                   prdb_sdk_cell_next_v1(&cell_it, &source_cells[cell_idx], sizeof(source_cells[0])) == PRDB_SDK_BATCH_OK) {
                cell_idx++;
            }
            {
                int32_t row_matched = 0;
                int32_t match_rc = eval_scan_and_filter_match(
                    scan_node, filter_node, source_cells, cell_idx,
                    parameters, parameter_count, &row_matched);
                if (match_rc != PRDB_SDK_OPTIMIZER_PLAN_OK) {
                    if (!cursor_finished) prdb_sdk_executor_close_cursor_v1(host, cursor, scan_response, scan_response_capacity, &resp_len);
                    return match_rc;
                }
                if (!row_matched) continue;
            }
            if (sort_node != NULL) {
                if (sort_row_count >= 128) {
                    if (!cursor_finished) prdb_sdk_executor_close_cursor_v1(host, cursor, scan_response, scan_response_capacity, &resp_len);
                    return PRDB_SDK_OPTIMIZER_PLAN_UNSUPPORTED;
                }
                copy_value_inline(&sort_rows[sort_row_count].sort_key,
                                  sort_rows[sort_row_count].sort_key_bytes,
                                  &source_cells[sort_ordinal]);
                for (i = 0; i < out_count; ++i) {
                    copy_value_inline(&sort_rows[sort_row_count].proj[i],
                                      sort_rows[sort_row_count].proj_bytes[i],
                                      &source_cells[proj_ordinals[i]]);
                }
                sort_row_count++;
                continue;
            }
            for (i = 0; i < out_count; ++i) {
                projected_values[i] = source_cells[proj_ordinals[i]];
            }
            if (on_row != NULL) {
                if (on_row(row_context, out_cols, projected_values, out_count) != 0) {
                    if (!cursor_finished) prdb_sdk_executor_close_cursor_v1(host, cursor, scan_response, scan_response_capacity, &resp_len);
                    return PRDB_SDK_OPTIMIZER_PLAN_ROW_CALLBACK_ERROR;
                }
            } else if (prdb_sdk_sql_write_projected_result_v1(
                           host, relation_columns, relation_out->column_count,
                           proj_ordinals, projected_values, out_count,
                           result_record_buffer, result_record_capacity, &resp_len) != PRDB_SDK_SQL_RESULT_OK) {
                if (!cursor_finished) prdb_sdk_executor_close_cursor_v1(host, cursor, scan_response, scan_response_capacity, &resp_len);
                return PRDB_SDK_OPTIMIZER_PLAN_ROW_CALLBACK_ERROR;
            }
            delivered++;
            if (has_limit && delivered >= limit) {
                if (!cursor_finished) prdb_sdk_executor_close_cursor_v1(host, cursor, scan_response, scan_response_capacity, &resp_len);
                *delivered_rows_out = delivered;
                return PRDB_SDK_OPTIMIZER_PLAN_OK;
            }
        }
        if (batch.finished) break;
    }

    if (!cursor_finished) prdb_sdk_executor_close_cursor_v1(host, cursor, scan_response, scan_response_capacity, &resp_len);

    if (sort_node != NULL) {
        uint32_t order[128];
        for (i = 0; i < sort_row_count; ++i) order[i] = i;
        for (i = 1; i < sort_row_count; ++i) {
            uint32_t idx = order[i];
            j = i;
            while (j > 0 && compare_sort_values(&sort_rows[order[j - 1]].sort_key,
                                                &sort_rows[idx].sort_key,
                                                sort_node->descending) > 0) {
                order[j] = order[j - 1];
                j--;
            }
            order[j] = idx;
        }
        for (i = 0; i < sort_row_count; ++i) {
            uint32_t row_idx = order[i];
            if (has_limit && delivered >= limit) break;
            for (j = 0; j < out_count; ++j) {
                projected_values[j] = sort_rows[row_idx].proj[j];
                if (projected_values[j].state == PRDB_SDK_VALUE_STATE_VALUE &&
                    projected_values[j].data != NULL &&
                    projected_values[j].data_len <= 64) {
                    projected_values[j].data = sort_rows[row_idx].proj_bytes[j];
                }
            }
            if (on_row != NULL) {
                if (on_row(row_context, out_cols, projected_values, out_count) != 0) {
                    return PRDB_SDK_OPTIMIZER_PLAN_ROW_CALLBACK_ERROR;
                }
            } else if (prdb_sdk_sql_write_projected_result_v1(
                           host, relation_columns, relation_out->column_count,
                           proj_ordinals, projected_values, out_count,
                           result_record_buffer, result_record_capacity, &resp_len) != PRDB_SDK_SQL_RESULT_OK) {
                return PRDB_SDK_OPTIMIZER_PLAN_ROW_CALLBACK_ERROR;
            }
            delivered++;
        }
    }

    *delivered_rows_out = delivered;
    return PRDB_SDK_OPTIMIZER_PLAN_OK;
}

int32_t prdb_sdk_optimizer_execute_unified_plan_v1(
    const prdb_host_api_v1 *host,
    const prdb_sdk_optimizer_unified_plan_v1 *plan,
    const prdb_sdk_value_v1 *parameters, uint32_t parameter_count,
    uint8_t *relation_response, uint64_t relation_response_capacity,
    uint8_t *scan_response, uint64_t scan_response_capacity,
    uint8_t *batch_response, uint64_t batch_response_capacity,
    prdb_sdk_executor_relation_v1 *relation_out, uint32_t relation_out_capacity,
    prdb_sdk_sql_column_shape_v1 *relation_columns, uint32_t relation_column_capacity,
    prdb_sdk_value_v1 *source_cells, uint32_t source_cell_capacity,
    prdb_sdk_value_v1 *projected_values, uint32_t projected_value_capacity,
    uint8_t *result_record_buffer, uint64_t result_record_capacity,
    uint32_t *delivered_rows_out) {
    if (result_record_buffer == NULL) {
        return PRDB_SDK_OPTIMIZER_PLAN_INVALID_ARGUMENT;
    }
    return execute_unified_select_pipeline_v1(
        host, plan, parameters, parameter_count,
        relation_response, relation_response_capacity,
        scan_response, scan_response_capacity,
        batch_response, batch_response_capacity,
        relation_out, relation_out_capacity,
        relation_columns, relation_column_capacity,
        source_cells, source_cell_capacity,
        projected_values, projected_value_capacity,
        result_record_buffer, result_record_capacity,
        NULL, NULL, delivered_rows_out);
}

static uint8_t sql_cmp_to_ir(uint32_t cmp) {
    switch (cmp) {
    case PRDB_SDK_SQL_COMPARE_EQUAL: return 0;
    case PRDB_SDK_SQL_COMPARE_NOT_EQUAL: return 1;
    case PRDB_SDK_SQL_COMPARE_LESS: return 2;
    case PRDB_SDK_SQL_COMPARE_LESS_EQUAL: return 3;
    case PRDB_SDK_SQL_COMPARE_GREATER: return 4;
    case PRDB_SDK_SQL_COMPARE_GREATER_EQUAL: return 5;
    default: return 255;
    }
}

static int lower_sql_predicate_node(
    const prdb_sdk_select_v1 *stmt,
    const prdb_sdk_value_v1 *params, uint32_t param_count,
    uint32_t ast_idx,
    prdb_sdk_optimizer_expr_node_v1 *out_nodes, uint32_t max_nodes,
    uint32_t *out_count, uint16_t *out_idx) {
    const prdb_sdk_predicate_node_v1 *pn;
    uint16_t idx;

    if (ast_idx >= stmt->predicate_count || *out_count >= max_nodes) return 0;
    pn = &stmt->predicates[ast_idx];

    if (pn->node_kind == PRDB_SDK_SQL_PREDICATE_COMPARE) {
        uint8_t op = sql_cmp_to_ir(pn->comparison);
        uint16_t col_idx, param_idx;
        const prdb_sdk_value_v1 *pv;
        if (op > 5 || pn->parameter_index == 0 || pn->parameter_index > param_count ||
            params == NULL || *out_count + 3 > max_nodes) {
            return 0;
        }
        col_idx = (uint16_t)(*out_count)++;
        memset(&out_nodes[col_idx], 0, sizeof(out_nodes[col_idx]));
        out_nodes[col_idx].tag = 0x10;
        out_nodes[col_idx].relation_index = 0;
        out_nodes[col_idx].bytes = stmt->sql + pn->column.offset;
        out_nodes[col_idx].bytes_len = pn->column.length;

        pv = &params[pn->parameter_index - 1];
        param_idx = (uint16_t)(*out_count)++;
        memset(&out_nodes[param_idx], 0, sizeof(out_nodes[param_idx]));
        if (pv->state == PRDB_SDK_VALUE_STATE_NULL) {
            out_nodes[param_idx].tag = 0x00;
        } else if (pv->type_code == PRDB_SDK_VALUE_TYPE_BOOLEAN && pv->data_len == 1) {
            out_nodes[param_idx].tag = 0x01;
            out_nodes[param_idx].op = pv->data[0] ? 1 : 0;
        } else {
            out_nodes[param_idx].tag = 0x11;
            out_nodes[param_idx].param_index = pn->parameter_index;
        }

        idx = (uint16_t)(*out_count)++;
        memset(&out_nodes[idx], 0, sizeof(out_nodes[idx]));
        out_nodes[idx].tag = 0x20;
        out_nodes[idx].op = op;
        out_nodes[idx].children[0] = col_idx;
        out_nodes[idx].children[1] = param_idx;
        *out_idx = idx;
        return 1;
    }
    if (pn->node_kind == PRDB_SDK_SQL_PREDICATE_IS_NULL ||
        pn->node_kind == PRDB_SDK_SQL_PREDICATE_IS_NOT_NULL) {
        uint16_t col_idx;
        if (*out_count + 2 > max_nodes) return 0;
        col_idx = (uint16_t)(*out_count)++;
        memset(&out_nodes[col_idx], 0, sizeof(out_nodes[col_idx]));
        out_nodes[col_idx].tag = 0x10;
        out_nodes[col_idx].relation_index = 0;
        out_nodes[col_idx].bytes = stmt->sql + pn->column.offset;
        out_nodes[col_idx].bytes_len = pn->column.length;

        idx = (uint16_t)(*out_count)++;
        memset(&out_nodes[idx], 0, sizeof(out_nodes[idx]));
        out_nodes[idx].tag = (pn->node_kind == PRDB_SDK_SQL_PREDICATE_IS_NOT_NULL) ? 0x41 : 0x40;
        out_nodes[idx].children[0] = col_idx;
        *out_idx = idx;
        return 1;
    }
    if (pn->node_kind == PRDB_SDK_SQL_PREDICATE_AND ||
        pn->node_kind == PRDB_SDK_SQL_PREDICATE_OR) {
        uint16_t l_idx = 0, r_idx = 0;
        if (!lower_sql_predicate_node(stmt, params, param_count, pn->left_child,
                                      out_nodes, max_nodes, out_count, &l_idx) ||
            !lower_sql_predicate_node(stmt, params, param_count, pn->right_child,
                                      out_nodes, max_nodes, out_count, &r_idx) ||
            *out_count >= max_nodes) {
            return 0;
        }
        idx = (uint16_t)(*out_count)++;
        memset(&out_nodes[idx], 0, sizeof(out_nodes[idx]));
        out_nodes[idx].tag = (pn->node_kind == PRDB_SDK_SQL_PREDICATE_AND) ? 0x30 : 0x31;
        out_nodes[idx].child_count = 2;
        out_nodes[idx].children[0] = l_idx;
        out_nodes[idx].children[1] = r_idx;
        *out_idx = idx;
        return 1;
    }
    if (pn->node_kind == PRDB_SDK_SQL_PREDICATE_NOT) {
        uint16_t c_idx = 0;
        if (!lower_sql_predicate_node(stmt, params, param_count, pn->left_child,
                                      out_nodes, max_nodes, out_count, &c_idx) ||
            *out_count >= max_nodes) {
            return 0;
        }
        idx = (uint16_t)(*out_count)++;
        memset(&out_nodes[idx], 0, sizeof(out_nodes[idx]));
        out_nodes[idx].tag = 0x32;
        out_nodes[idx].children[0] = c_idx;
        *out_idx = idx;
        return 1;
    }
    return 0;
}

static int32_t execute_select_unified_internal_v1(
    const prdb_host_api_v1 *host, const prdb_sdk_select_v1 *statement,
    const prdb_sdk_value_v1 *parameters, uint32_t parameter_count,
    const uint8_t *schema, uint32_t schema_len,
    uint8_t *request, uint64_t request_capacity,
    uint8_t *optimizer_response, uint64_t optimizer_response_capacity,
    uint8_t *relation_response, uint64_t relation_response_capacity,
    uint8_t *scan_response, uint64_t scan_response_capacity,
    uint8_t *batch_response, uint64_t batch_response_capacity,
    prdb_sdk_executor_relation_v1 *relation_out, uint32_t relation_out_capacity,
    prdb_sdk_sql_column_shape_v1 *relation_columns, uint32_t relation_column_capacity,
    prdb_sdk_value_v1 *source_cells, uint32_t source_cell_capacity,
    prdb_sdk_value_v1 *projected_values, uint32_t projected_value_capacity,
    uint8_t *result_record_buffer, uint64_t result_record_capacity,
    prdb_sdk_optimizer_projected_row_v1_fn on_row, void *row_context,
    uint32_t *delivered_rows_out) {
    prdb_sdk_optimizer_logical_node_v1 l_nodes[6];
    prdb_sdk_optimizer_expr_node_v1 expr_nodes[PRDB_SDK_OPTIMIZER_MAX_EXPR_NODES];
    prdb_sdk_optimizer_col_ref_v1 proj_cols[PRDB_SDK_SQL_MAX_PROJECTION_COLUMNS];
    prdb_sdk_optimizer_order_item_v1 order_item;
    prdb_sdk_optimizer_response_v1 opt_resp = {0};
    prdb_sdk_optimizer_unified_plan_v1 plan;
    uint32_t l_count = 0;
    uint16_t cur_root = 0;
    uint64_t req_len = 0, resp_len = 0;
    uint32_t i;
    int32_t rc;

    if (host == NULL || statement == NULL || schema == NULL ||
        statement->projection_star != 0 || statement->projection_count == 0 ||
        statement->projection_count > PRDB_SDK_SQL_MAX_PROJECTION_COLUMNS) {
        return PRDB_SDK_OPTIMIZER_PLAN_UNSUPPORTED;
    }

    memset(l_nodes, 0, sizeof(l_nodes));
    l_nodes[l_count].op_tag = PRDB_OPTIMIZER_LOGICAL_OP_SCAN;
    l_nodes[l_count].relation_index = 0;
    l_nodes[l_count].relation_name = statement->sql + statement->relation.offset;
    l_nodes[l_count].relation_name_len = statement->relation.length;
    cur_root = (uint16_t)l_count++;

    if (statement->predicate_count > 0) {
        uint32_t expr_count = 0;
        uint16_t expr_root = 0;
        if (!lower_sql_predicate_node(statement, parameters, parameter_count,
                                      statement->predicate_root, expr_nodes,
                                      PRDB_SDK_OPTIMIZER_MAX_EXPR_NODES,
                                      &expr_count, &expr_root)) {
            return PRDB_SDK_OPTIMIZER_PLAN_UNSUPPORTED;
        }
        l_nodes[l_count].op_tag = PRDB_OPTIMIZER_LOGICAL_OP_FILTER;
        l_nodes[l_count].left_child = cur_root;
        l_nodes[l_count].expr_nodes = expr_nodes;
        l_nodes[l_count].expr_node_count = expr_count;
        l_nodes[l_count].expr_root = expr_root;
        cur_root = (uint16_t)l_count++;
    }

    for (i = 0; i < statement->projection_count; ++i) {
        proj_cols[i].relation_index = 0;
        proj_cols[i].column_name = statement->sql + statement->projection[i].offset;
        proj_cols[i].column_name_len = statement->projection[i].length;
    }
    l_nodes[l_count].op_tag = PRDB_OPTIMIZER_LOGICAL_OP_PROJECT;
    l_nodes[l_count].left_child = cur_root;
    l_nodes[l_count].columns = proj_cols;
    l_nodes[l_count].column_count = statement->projection_count;
    cur_root = (uint16_t)l_count++;

    if (statement->has_order_by) {
        memset(&order_item, 0, sizeof(order_item));
        order_item.is_agg_index = 0;
        order_item.descending = statement->order_descending ? 1 : 0;
        order_item.nulls_first = 0;
        order_item.column.relation_index = 0;
        order_item.column.column_name = statement->sql + statement->order_by_column.offset;
        order_item.column.column_name_len = statement->order_by_column.length;
        l_nodes[l_count].op_tag = PRDB_OPTIMIZER_LOGICAL_OP_SORT;
        l_nodes[l_count].left_child = cur_root;
        l_nodes[l_count].order_items = &order_item;
        l_nodes[l_count].order_item_count = 1;
        cur_root = (uint16_t)l_count++;
    }

    if (statement->has_limit) {
        l_nodes[l_count].op_tag = PRDB_OPTIMIZER_LOGICAL_OP_LIMIT;
        l_nodes[l_count].left_child = cur_root;
        l_nodes[l_count].limit_count = statement->limit;
        cur_root = (uint16_t)l_count++;
    }

    rc = prdb_sdk_optimizer_encode_unified_request_v1(
        schema, schema_len, l_nodes, l_count, cur_root,
        request, request_capacity, &req_len);
    if (rc != PRDB_SDK_OPTIMIZER_ENCODE_OK) {
        return PRDB_SDK_OPTIMIZER_PLAN_INVALID_ARGUMENT;
    }

    rc = prdb_sdk_optimizer_submit_unified_v1(
        host, PRDB_IPC_OP_OPTIMIZE_SELECT, request, req_len,
        optimizer_response, optimizer_response_capacity, &resp_len,
        &opt_resp, sizeof(opt_resp));
    if (rc != PRDB_SDK_OPTIMIZER_RESULT_OK ||
        opt_resp.status != PRDB_SDK_OPTIMIZER_STATUS_SUCCESS ||
        opt_resp.plan == NULL) {
        return PRDB_SDK_OPTIMIZER_PLAN_UNAVAILABLE;
    }

    rc = prdb_sdk_optimizer_decode_unified_plan_v1(
        opt_resp.plan, opt_resp.plan_len, &plan, sizeof(plan));
    if (rc != PRDB_SDK_OPTIMIZER_PLAN_OK) return rc;

    return execute_unified_select_pipeline_v1(
        host, &plan, parameters, parameter_count,
        relation_response, relation_response_capacity,
        scan_response, scan_response_capacity,
        batch_response, batch_response_capacity,
        relation_out, relation_out_capacity,
        relation_columns, relation_column_capacity,
        source_cells, source_cell_capacity,
        projected_values, projected_value_capacity,
        result_record_buffer, result_record_capacity,
        on_row, row_context, delivered_rows_out);
}

int32_t prdb_sdk_optimizer_execute_select_unified_v1(
    const prdb_host_api_v1 *host, const prdb_sdk_select_v1 *statement,
    const prdb_sdk_value_v1 *parameters, uint32_t parameter_count,
    const uint8_t *schema, uint32_t schema_len,
    uint8_t *request, uint64_t request_capacity,
    uint8_t *optimizer_response, uint64_t optimizer_response_capacity,
    uint8_t *relation_response, uint64_t relation_response_capacity,
    uint8_t *scan_response, uint64_t scan_response_capacity,
    uint8_t *batch_response, uint64_t batch_response_capacity,
    prdb_sdk_executor_relation_v1 *relation_out, uint32_t relation_out_capacity,
    prdb_sdk_sql_column_shape_v1 *relation_columns, uint32_t relation_column_capacity,
    prdb_sdk_value_v1 *source_cells, uint32_t source_cell_capacity,
    prdb_sdk_value_v1 *projected_values, uint32_t projected_value_capacity,
    uint8_t *result_record_buffer, uint64_t result_record_capacity,
    uint32_t *delivered_rows_out) {
    if (result_record_buffer == NULL) {
        return PRDB_SDK_OPTIMIZER_PLAN_INVALID_ARGUMENT;
    }
    return execute_select_unified_internal_v1(
        host, statement, parameters, parameter_count, schema, schema_len,
        request, request_capacity, optimizer_response, optimizer_response_capacity,
        relation_response, relation_response_capacity, scan_response, scan_response_capacity,
        batch_response, batch_response_capacity, relation_out, relation_out_capacity,
        relation_columns, relation_column_capacity, source_cells, source_cell_capacity,
        projected_values, projected_value_capacity,
        result_record_buffer, result_record_capacity,
        NULL, NULL, delivered_rows_out);
}

int32_t prdb_sdk_optimizer_execute_select_unified_cb_v1(
    const prdb_host_api_v1 *host, const prdb_sdk_select_v1 *statement,
    const prdb_sdk_value_v1 *parameters, uint32_t parameter_count,
    const uint8_t *schema, uint32_t schema_len,
    uint8_t *request, uint64_t request_capacity,
    uint8_t *optimizer_response, uint64_t optimizer_response_capacity,
    uint8_t *relation_response, uint64_t relation_response_capacity,
    uint8_t *scan_response, uint64_t scan_response_capacity,
    uint8_t *batch_response, uint64_t batch_response_capacity,
    prdb_sdk_executor_relation_v1 *relation_out, uint32_t relation_out_capacity,
    prdb_sdk_sql_column_shape_v1 *relation_columns, uint32_t relation_column_capacity,
    prdb_sdk_value_v1 *source_cells, uint32_t source_cell_capacity,
    prdb_sdk_value_v1 *projected_values, uint32_t projected_value_capacity,
    prdb_sdk_optimizer_projected_row_v1_fn on_row, void *row_context,
    uint32_t *delivered_rows_out) {
    if (on_row == NULL) {
        return PRDB_SDK_OPTIMIZER_PLAN_INVALID_ARGUMENT;
    }
    return execute_select_unified_internal_v1(
        host, statement, parameters, parameter_count, schema, schema_len,
        request, request_capacity, optimizer_response, optimizer_response_capacity,
        relation_response, relation_response_capacity, scan_response, scan_response_capacity,
        batch_response, batch_response_capacity, relation_out, relation_out_capacity,
        relation_columns, relation_column_capacity, source_cells, source_cell_capacity,
        projected_values, projected_value_capacity,
        NULL, 0, on_row, row_context, delivered_rows_out);
}

static int32_t collect_matching_row_handles_unified_v1(
    const prdb_host_api_v1 *host,
    const prdb_sdk_optimizer_unified_plan_v1 *plan,
    const prdb_sdk_value_v1 *parameters, uint32_t parameter_count,
    uint8_t *relation_response, uint64_t relation_response_capacity,
    uint8_t *scan_response, uint64_t scan_response_capacity,
    uint8_t *batch_response, uint64_t batch_response_capacity,
    prdb_sdk_executor_relation_v1 *relation_out, uint32_t relation_out_capacity,
    prdb_sdk_sql_column_shape_v1 *relation_columns, uint32_t relation_column_capacity,
    prdb_sdk_value_v1 *source_cells, uint32_t source_cell_capacity,
    uint8_t *target_row_handles, uint32_t target_row_handle_capacity,
    uint32_t *matched_count_out) {
    int32_t rc;
    uint16_t cur;
    const prdb_sdk_optimizer_physical_node_v1 *scan_node = NULL;
    const prdb_sdk_optimizer_physical_node_v1 *filter_node = NULL;
    uint8_t cursor[16];
    uint64_t resp_len = 0;
    uint32_t page = 0;
    uint32_t matched = 0;
    uint32_t i;

    *matched_count_out = 0;
    rc = prdb_sdk_optimizer_validate_unified_plan_v1(
        host, plan, relation_response, relation_response_capacity,
        relation_out, relation_out_capacity, relation_columns, relation_column_capacity);
    if (rc != PRDB_SDK_OPTIMIZER_PLAN_OK) return rc;

    cur = plan->root_index;
    for (i = 0; i < plan->node_count; ++i) {
        const prdb_sdk_optimizer_physical_node_v1 *n = &plan->nodes[cur];
        if (n->op_tag == PRDB_OPTIMIZER_PHYSICAL_OP_EMPTY_RESULT) {
            return PRDB_SDK_OPTIMIZER_PLAN_OK;
        }
        if (n->op_tag == PRDB_OPTIMIZER_PHYSICAL_OP_DELETE ||
            n->op_tag == PRDB_OPTIMIZER_PHYSICAL_OP_UPDATE) {
            cur = n->left_child;
        } else if (n->op_tag == PRDB_OPTIMIZER_PHYSICAL_OP_FILTER) {
            filter_node = n;
            cur = n->left_child;
        } else if (n->op_tag == PRDB_OPTIMIZER_PHYSICAL_OP_SEQ_SCAN ||
                   n->op_tag == PRDB_OPTIMIZER_PHYSICAL_OP_INDEX_POINT_SCAN ||
                   n->op_tag == PRDB_OPTIMIZER_PHYSICAL_OP_INDEX_RANGE_SCAN ||
                   n->op_tag == PRDB_OPTIMIZER_PHYSICAL_OP_COVERING_INDEX_SCAN) {
            scan_node = n;
            break;
        } else {
            return PRDB_SDK_OPTIMIZER_PLAN_UNSUPPORTED;
        }
    }
    if (scan_node == NULL) return PRDB_SDK_OPTIMIZER_PLAN_UNSUPPORTED;

    rc = prdb_sdk_executor_open_full_scan_v1(
        host, relation_out->executor_handle, scan_response, scan_response_capacity,
        &resp_len, cursor);
    if (rc != PRDB_SDK_EXECUTOR_OK) return PRDB_SDK_OPTIMIZER_PLAN_UNAVAILABLE;

    int cursor_finished = 0;
    for (page = 0; page < PRDB_SDK_OPTIMIZER_PLAN_MAX_NEXT_REQUESTS; ++page) {
        int32_t cancelled = 0;
        prdb_sdk_batch_v1 batch;
        prdb_sdk_row_iterator_v1 row_it;
        prdb_sdk_row_v1 row;

        if (prdb_sdk_executor_check_cancelled_v1(host, &cancelled) != PRDB_SDK_EXECUTOR_OK ||
            cancelled) {
            if (!cursor_finished) prdb_sdk_executor_close_cursor_v1(host, cursor, scan_response, scan_response_capacity, &resp_len);
            return PRDB_SDK_OPTIMIZER_PLAN_CANCELLED;
        }
        rc = prdb_sdk_executor_next_batch_v1(
            host, cursor, 128, (uint32_t)batch_response_capacity,
            batch_response, batch_response_capacity, &resp_len, &batch, sizeof(batch));
        if (rc != PRDB_SDK_EXECUTOR_OK) {
            if (!cursor_finished) prdb_sdk_executor_close_cursor_v1(host, cursor, scan_response, scan_response_capacity, &resp_len);
            return PRDB_SDK_OPTIMIZER_PLAN_CALLBACK_ERROR;
        }
        cursor_finished = batch.finished ? 1 : 0;
        if (prdb_sdk_batch_rows_begin_v1(&batch, &row_it, sizeof(row_it)) != PRDB_SDK_BATCH_OK) {
            if (!cursor_finished) prdb_sdk_executor_close_cursor_v1(host, cursor, scan_response, scan_response_capacity, &resp_len);
            return PRDB_SDK_OPTIMIZER_PLAN_INVALID_PAYLOAD;
        }
        while (prdb_sdk_batch_row_next_v1(&row_it, &row, sizeof(row)) == PRDB_SDK_BATCH_OK) {
            prdb_sdk_cell_iterator_v1 cell_it;
            uint32_t cell_idx = 0;
            if (prdb_sdk_row_cells_begin_v1(&row, &cell_it, sizeof(cell_it)) != PRDB_SDK_BATCH_OK) {
                if (!cursor_finished) prdb_sdk_executor_close_cursor_v1(host, cursor, scan_response, scan_response_capacity, &resp_len);
                return PRDB_SDK_OPTIMIZER_PLAN_INVALID_PAYLOAD;
            }
            while (cell_idx < source_cell_capacity &&
                   prdb_sdk_cell_next_v1(&cell_it, &source_cells[cell_idx], sizeof(source_cells[0])) == PRDB_SDK_BATCH_OK) {
                cell_idx++;
            }
            {
                int32_t row_matched = 0;
                int32_t match_rc = eval_scan_and_filter_match(
                    scan_node, filter_node, source_cells, cell_idx,
                    parameters, parameter_count, &row_matched);
                if (match_rc != PRDB_SDK_OPTIMIZER_PLAN_OK) {
                    if (!cursor_finished) prdb_sdk_executor_close_cursor_v1(host, cursor, scan_response, scan_response_capacity, &resp_len);
                    return match_rc;
                }
                if (!row_matched) continue;
            }
            if (matched >= target_row_handle_capacity) {
                if (!cursor_finished) prdb_sdk_executor_close_cursor_v1(host, cursor, scan_response, scan_response_capacity, &resp_len);
                return PRDB_SDK_OPTIMIZER_PLAN_REQUEST_LIMIT;
            }
            memcpy(target_row_handles + (size_t)matched * 16, row.handle, 16);
            matched++;
        }
        if (batch.finished) break;
    }

    if (!cursor_finished) prdb_sdk_executor_close_cursor_v1(host, cursor, scan_response, scan_response_capacity, &resp_len);
    *matched_count_out = matched;
    return PRDB_SDK_OPTIMIZER_PLAN_OK;
}

static int32_t fail_unified_statement(const prdb_host_api_v1 *host, int32_t status) {
    if (status != PRDB_SDK_OPTIMIZER_PLAN_OK &&
        host != NULL && host->statement_failure != NULL) {
        (void)host->statement_failure(host->context, PRDB_QUERY_STATEMENT_FAILURE_SQL);
    }
    return status;
}

static int32_t execute_update_unified_internal_v1(
    const prdb_host_api_v1 *host, const prdb_sdk_update_v1 *statement,
    const prdb_sdk_value_v1 *parameters, uint32_t parameter_count,
    const uint8_t *schema, uint32_t schema_len,
    uint8_t *request, uint64_t request_capacity,
    uint8_t *optimizer_response, uint64_t optimizer_response_capacity,
    uint8_t *relation_response, uint64_t relation_response_capacity,
    uint8_t *scan_response, uint64_t scan_response_capacity,
    uint8_t *batch_response, uint64_t batch_response_capacity,
    prdb_sdk_executor_relation_v1 *relation_out, uint32_t relation_out_capacity,
    prdb_sdk_sql_column_shape_v1 *relation_columns, uint32_t relation_column_capacity,
    prdb_sdk_value_v1 *source_cells, uint32_t source_cell_capacity,
    uint8_t *target_row_handles, uint32_t target_row_handle_capacity,
    uint32_t *updated_rows_out) {
    prdb_sdk_optimizer_logical_node_v1 l_node;
    prdb_sdk_optimizer_expr_node_v1 expr_nodes[PRDB_SDK_OPTIMIZER_MAX_EXPR_NODES];
    prdb_sdk_optimizer_update_assign_v1 assigns[PRDB_SDK_OPTIMIZER_PLAN_MAX_UPDATE_ASSIGNMENTS];
    prdb_sdk_executor_update_patch_v1 patches[PRDB_SDK_OPTIMIZER_PLAN_MAX_UPDATE_ASSIGNMENTS];
    prdb_sdk_optimizer_response_v1 opt_resp = {0};
    prdb_sdk_optimizer_unified_plan_v1 plan;
    uint32_t expr_count = 0, matched = 0, i, j;
    uint16_t expr_root = 0;
    uint64_t req_len = 0, resp_len = 0;
    int32_t rc;

    if (host == NULL || statement == NULL || schema == NULL ||
        target_row_handles == NULL || updated_rows_out == NULL ||
        statement->assignment_count == 0 ||
        statement->assignment_count > PRDB_SDK_OPTIMIZER_PLAN_MAX_UPDATE_ASSIGNMENTS ||
        statement->predicate_statement.predicate_count == 0) {
        return PRDB_SDK_OPTIMIZER_PLAN_INVALID_ARGUMENT;
    }
    *updated_rows_out = 0;

    if (!lower_sql_predicate_node(&statement->predicate_statement, parameters, parameter_count,
                                  statement->predicate_statement.predicate_root, expr_nodes,
                                  PRDB_SDK_OPTIMIZER_MAX_EXPR_NODES, &expr_count, &expr_root)) {
        return PRDB_SDK_OPTIMIZER_PLAN_UNSUPPORTED;
    }
    for (i = 0; i < statement->assignment_count; ++i) {
        assigns[i].column_name = statement->sql + statement->assignments[i].column.offset;
        assigns[i].column_name_len = statement->assignments[i].column.length;
        assigns[i].is_null = (statement->assignments[i].kind == PRDB_SDK_SQL_UPDATE_NULL) ? 1 : 0;
        assigns[i].param_index = statement->assignments[i].parameter_index;
    }

    memset(&l_node, 0, sizeof(l_node));
    l_node.op_tag = PRDB_OPTIMIZER_LOGICAL_OP_UPDATE;
    l_node.relation_index = 0;
    l_node.relation_name = statement->sql + statement->relation.offset;
    l_node.relation_name_len = statement->relation.length;
    l_node.expr_nodes = expr_nodes;
    l_node.expr_node_count = expr_count;
    l_node.expr_root = expr_root;
    l_node.assignments = assigns;
    l_node.assignment_count = statement->assignment_count;

    rc = prdb_sdk_optimizer_encode_unified_request_v1(
        schema, schema_len, &l_node, 1, 0, request, request_capacity, &req_len);
    if (rc != PRDB_SDK_OPTIMIZER_ENCODE_OK) return PRDB_SDK_OPTIMIZER_PLAN_INVALID_ARGUMENT;

    rc = prdb_sdk_optimizer_submit_unified_v1(
        host, PRDB_IPC_OP_OPTIMIZE_UPDATE, request, req_len,
        optimizer_response, optimizer_response_capacity, &resp_len,
        &opt_resp, sizeof(opt_resp));
    if (rc != PRDB_SDK_OPTIMIZER_RESULT_OK ||
        opt_resp.status != PRDB_SDK_OPTIMIZER_STATUS_SUCCESS ||
        opt_resp.plan == NULL) {
        return PRDB_SDK_OPTIMIZER_PLAN_UNAVAILABLE;
    }

    rc = prdb_sdk_optimizer_decode_unified_plan_v1(
        opt_resp.plan, opt_resp.plan_len, &plan, sizeof(plan));
    if (rc != PRDB_SDK_OPTIMIZER_PLAN_OK) return rc;

    rc = collect_matching_row_handles_unified_v1(
        host, &plan, parameters, parameter_count,
        relation_response, relation_response_capacity,
        scan_response, scan_response_capacity,
        batch_response, batch_response_capacity,
        relation_out, relation_out_capacity,
        relation_columns, relation_column_capacity,
        source_cells, source_cell_capacity,
        target_row_handles, target_row_handle_capacity, &matched);
    if (rc != PRDB_SDK_OPTIMIZER_PLAN_OK) return rc;

    for (i = 0; i < statement->assignment_count; ++i) {
        int found = 0;
        memset(&patches[i], 0, sizeof(patches[i]));
        patches[i].struct_size = (uint32_t)sizeof(patches[i]);
        patches[i].abi_version = PRDB_SDK_EXECUTOR_ABI_V1_VERSION;
        for (j = 0; j < relation_out->column_count; ++j) {
            if (relation_columns[j].name_len == assigns[i].column_name_len &&
                memcmp(relation_columns[j].name, assigns[i].column_name, assigns[i].column_name_len) == 0) {
                patches[i].ordinal = relation_columns[j].ordinal;
                found = 1;
                break;
            }
        }
        if (!found) return PRDB_SDK_OPTIMIZER_PLAN_IDENTITY_MISMATCH;
        if (assigns[i].is_null) {
            patches[i].value.struct_size = (uint32_t)sizeof(patches[i].value);
            patches[i].value.abi_version = PRDB_SDK_VALUE_ABI_V1_VERSION;
            patches[i].value.state = PRDB_SDK_VALUE_STATE_NULL;
        } else {
            if (assigns[i].param_index == 0 || assigns[i].param_index > parameter_count ||
                parameters == NULL) {
                return PRDB_SDK_OPTIMIZER_PLAN_INVALID_ARGUMENT;
            }
            patches[i].value = parameters[assigns[i].param_index - 1];
        }
    }

    for (i = 0; i < matched; ++i) {
        int32_t cancelled = 0;
        if (prdb_sdk_executor_check_cancelled_v1(host, &cancelled) != PRDB_SDK_EXECUTOR_OK ||
            cancelled) {
            return PRDB_SDK_OPTIMIZER_PLAN_CANCELLED;
        }
        rc = prdb_sdk_executor_stage_update_v1(
            host, target_row_handles + (size_t)i * 16, patches,
            (uint16_t)statement->assignment_count, request, request_capacity,
            batch_response, batch_response_capacity, &resp_len);
        if (rc != PRDB_SDK_EXECUTOR_OK) {
            return PRDB_SDK_OPTIMIZER_PLAN_CALLBACK_ERROR;
        }
    }
    *updated_rows_out = matched;
    return PRDB_SDK_OPTIMIZER_PLAN_OK;
}

int32_t prdb_sdk_optimizer_execute_update_unified_v1(
    const prdb_host_api_v1 *host, const prdb_sdk_update_v1 *statement,
    const prdb_sdk_value_v1 *parameters, uint32_t parameter_count,
    const uint8_t *schema, uint32_t schema_len,
    uint8_t *request, uint64_t request_capacity,
    uint8_t *optimizer_response, uint64_t optimizer_response_capacity,
    uint8_t *relation_response, uint64_t relation_response_capacity,
    uint8_t *scan_response, uint64_t scan_response_capacity,
    uint8_t *batch_response, uint64_t batch_response_capacity,
    prdb_sdk_executor_relation_v1 *relation_out, uint32_t relation_out_capacity,
    prdb_sdk_sql_column_shape_v1 *relation_columns, uint32_t relation_column_capacity,
    prdb_sdk_value_v1 *source_cells, uint32_t source_cell_capacity,
    uint8_t *target_row_handles, uint32_t target_row_handle_capacity,
    uint32_t *updated_rows_out) {
    int32_t rc = execute_update_unified_internal_v1(
        host, statement, parameters, parameter_count, schema, schema_len,
        request, request_capacity, optimizer_response, optimizer_response_capacity,
        relation_response, relation_response_capacity, scan_response, scan_response_capacity,
        batch_response, batch_response_capacity, relation_out, relation_out_capacity,
        relation_columns, relation_column_capacity, source_cells, source_cell_capacity,
        target_row_handles, target_row_handle_capacity, updated_rows_out);
    return fail_unified_statement(host, rc);
}

int32_t prdb_sdk_optimizer_execute_delete_unified_v1(
    const prdb_host_api_v1 *host, const prdb_sdk_delete_v1 *statement,
    const prdb_sdk_value_v1 *parameters, uint32_t parameter_count,
    const uint8_t *schema, uint32_t schema_len,
    uint8_t *request, uint64_t request_capacity,
    uint8_t *optimizer_response, uint64_t optimizer_response_capacity,
    uint8_t *relation_response, uint64_t relation_response_capacity,
    uint8_t *scan_response, uint64_t scan_response_capacity,
    uint8_t *batch_response, uint64_t batch_response_capacity,
    prdb_sdk_executor_relation_v1 *relation_out, uint32_t relation_out_capacity,
    prdb_sdk_sql_column_shape_v1 *relation_columns, uint32_t relation_column_capacity,
    prdb_sdk_value_v1 *source_cells, uint32_t source_cell_capacity,
    uint8_t *target_row_handles, uint32_t target_row_handle_capacity,
    uint32_t *deleted_rows_out) {
    prdb_sdk_optimizer_logical_node_v1 l_node;
    prdb_sdk_optimizer_expr_node_v1 expr_nodes[PRDB_SDK_OPTIMIZER_MAX_EXPR_NODES];
    prdb_sdk_optimizer_response_v1 opt_resp = {0};
    prdb_sdk_optimizer_unified_plan_v1 plan;
    uint32_t expr_count = 0, matched = 0, i;
    uint16_t expr_root = 0;
    uint64_t req_len = 0, resp_len = 0;
    int32_t rc;

    if (host == NULL || statement == NULL || schema == NULL ||
        target_row_handles == NULL || deleted_rows_out == NULL ||
        statement->predicate_statement.predicate_count == 0) {
        return PRDB_SDK_OPTIMIZER_PLAN_INVALID_ARGUMENT;
    }
    *deleted_rows_out = 0;

    if (!lower_sql_predicate_node(&statement->predicate_statement, parameters, parameter_count,
                                  statement->predicate_statement.predicate_root, expr_nodes,
                                  PRDB_SDK_OPTIMIZER_MAX_EXPR_NODES, &expr_count, &expr_root)) {
        return PRDB_SDK_OPTIMIZER_PLAN_UNSUPPORTED;
    }

    memset(&l_node, 0, sizeof(l_node));
    l_node.op_tag = PRDB_OPTIMIZER_LOGICAL_OP_DELETE;
    l_node.relation_index = 0;
    l_node.relation_name = statement->sql + statement->relation.offset;
    l_node.relation_name_len = statement->relation.length;
    l_node.expr_nodes = expr_nodes;
    l_node.expr_node_count = expr_count;
    l_node.expr_root = expr_root;

    rc = prdb_sdk_optimizer_encode_unified_request_v1(
        schema, schema_len, &l_node, 1, 0, request, request_capacity, &req_len);
    if (rc != PRDB_SDK_OPTIMIZER_ENCODE_OK) return PRDB_SDK_OPTIMIZER_PLAN_INVALID_ARGUMENT;

    rc = prdb_sdk_optimizer_submit_unified_v1(
        host, PRDB_IPC_OP_OPTIMIZE_DELETE, request, req_len,
        optimizer_response, optimizer_response_capacity, &resp_len,
        &opt_resp, sizeof(opt_resp));
    if (rc != PRDB_SDK_OPTIMIZER_RESULT_OK ||
        opt_resp.status != PRDB_SDK_OPTIMIZER_STATUS_SUCCESS ||
        opt_resp.plan == NULL) {
        return PRDB_SDK_OPTIMIZER_PLAN_UNAVAILABLE;
    }

    rc = prdb_sdk_optimizer_decode_unified_plan_v1(
        opt_resp.plan, opt_resp.plan_len, &plan, sizeof(plan));
    if (rc != PRDB_SDK_OPTIMIZER_PLAN_OK) return rc;

    rc = collect_matching_row_handles_unified_v1(
        host, &plan, parameters, parameter_count,
        relation_response, relation_response_capacity,
        scan_response, scan_response_capacity,
        batch_response, batch_response_capacity,
        relation_out, relation_out_capacity,
        relation_columns, relation_column_capacity,
        source_cells, source_cell_capacity,
        target_row_handles, target_row_handle_capacity, &matched);
    if (rc != PRDB_SDK_OPTIMIZER_PLAN_OK) return rc;

    for (i = 0; i < matched; ++i) {
        int32_t cancelled = 0;
        if (prdb_sdk_executor_check_cancelled_v1(host, &cancelled) != PRDB_SDK_EXECUTOR_OK ||
            cancelled) {
            return PRDB_SDK_OPTIMIZER_PLAN_CANCELLED;
        }
        rc = prdb_sdk_executor_stage_delete_v1(
            host, target_row_handles + (size_t)i * 16,
            batch_response, batch_response_capacity, &resp_len);
        if (rc != PRDB_SDK_EXECUTOR_OK) {
            return PRDB_SDK_OPTIMIZER_PLAN_CALLBACK_ERROR;
        }
    }
    *deleted_rows_out = matched;
    return PRDB_SDK_OPTIMIZER_PLAN_OK;
}

static uint16_t qualified_col_rel_index(const prdb_sdk_select_v1 *stmt,
                                        const prdb_sdk_sql_qualified_column_v1 *qc) {
    uint32_t len = qc->relation.length;
    const uint8_t *q = stmt->sql + qc->relation.offset;
    if (stmt->right_alias.length == len &&
        memcmp(stmt->sql + stmt->right_alias.offset, q, len) == 0) {
        return 1;
    }
    if (stmt->right_relation.length == len &&
        memcmp(stmt->sql + stmt->right_relation.offset, q, len) == 0 &&
        !(stmt->left_relation.length == len &&
          memcmp(stmt->sql + stmt->left_relation.offset, q, len) == 0)) {
        return 1;
    }
    return 0;
}

int32_t prdb_sdk_optimizer_plan_join_unified_v1(
    const prdb_host_api_v1 *host, const prdb_sdk_select_v1 *statement,
    const prdb_sdk_value_v1 *parameters, uint32_t parameter_count,
    const uint8_t *schema, uint32_t schema_len,
    uint8_t *request, uint64_t request_capacity,
    uint8_t *optimizer_response, uint64_t optimizer_response_capacity,
    uint8_t *left_relation_response, uint64_t left_relation_response_capacity,
    uint8_t *right_relation_response, uint64_t right_relation_response_capacity,
    prdb_sdk_executor_relation_v1 *left_relation_out,
    prdb_sdk_executor_relation_v1 *right_relation_out,
    prdb_sdk_sql_column_shape_v1 *left_columns, uint32_t left_column_capacity,
    prdb_sdk_sql_column_shape_v1 *right_columns, uint32_t right_column_capacity,
    prdb_sdk_optimizer_unified_plan_v1 *plan_out) {
    prdb_sdk_optimizer_logical_node_v1 l_nodes[6];
    prdb_sdk_optimizer_col_ref_v1 left_keys[PRDB_SDK_SQL_MAX_JOIN_KEYS];
    prdb_sdk_optimizer_col_ref_v1 right_keys[PRDB_SDK_SQL_MAX_JOIN_KEYS];
    prdb_sdk_optimizer_col_ref_v1 proj_cols[PRDB_SDK_SQL_MAX_PROJECTION_COLUMNS];
    prdb_sdk_optimizer_expr_node_v1 expr_nodes[4];
    prdb_sdk_optimizer_response_v1 opt_resp = {0};
    uint32_t l_count = 0, i;
    uint16_t left_idx, right_idx, cur_root;
    uint64_t req_len = 0, resp_len = 0;
    int32_t rc;

    if (host == NULL || statement == NULL || schema == NULL || plan_out == NULL ||
        left_relation_out == NULL || right_relation_out == NULL ||
        left_columns == NULL || right_columns == NULL ||
        statement->join_key_count == 0 ||
        statement->join_key_count > PRDB_SDK_SQL_MAX_JOIN_KEYS ||
        statement->join_projection_count == 0 ||
        statement->join_projection_count > PRDB_SDK_SQL_MAX_PROJECTION_COLUMNS) {
        return PRDB_SDK_OPTIMIZER_PLAN_INVALID_ARGUMENT;
    }

    memset(l_nodes, 0, sizeof(l_nodes));
    left_idx = (uint16_t)l_count++;
    l_nodes[left_idx].op_tag = PRDB_OPTIMIZER_LOGICAL_OP_SCAN;
    l_nodes[left_idx].relation_index = 0;
    l_nodes[left_idx].relation_name = statement->sql + statement->left_relation.offset;
    l_nodes[left_idx].relation_name_len = statement->left_relation.length;

    right_idx = (uint16_t)l_count++;
    l_nodes[right_idx].op_tag = PRDB_OPTIMIZER_LOGICAL_OP_SCAN;
    l_nodes[right_idx].relation_index = 1;
    l_nodes[right_idx].relation_name = statement->sql + statement->right_relation.offset;
    l_nodes[right_idx].relation_name_len = statement->right_relation.length;

    for (i = 0; i < statement->join_key_count; ++i) {
        left_keys[i].relation_index = qualified_col_rel_index(statement, &statement->left_join_keys[i]);
        left_keys[i].column_name = statement->sql + statement->left_join_keys[i].column.offset;
        left_keys[i].column_name_len = statement->left_join_keys[i].column.length;
        right_keys[i].relation_index = qualified_col_rel_index(statement, &statement->right_join_keys[i]);
        right_keys[i].column_name = statement->sql + statement->right_join_keys[i].column.offset;
        right_keys[i].column_name_len = statement->right_join_keys[i].column.length;
    }

    cur_root = (uint16_t)l_count++;
    l_nodes[cur_root].op_tag = PRDB_OPTIMIZER_LOGICAL_OP_JOIN;
    l_nodes[cur_root].join_type = 0;
    l_nodes[cur_root].left_child = left_idx;
    l_nodes[cur_root].right_child = right_idx;
    l_nodes[cur_root].columns = left_keys;
    l_nodes[cur_root].right_keys = right_keys;
    l_nodes[cur_root].column_count = statement->join_key_count;

    if (statement->has_join_where) {
        uint8_t op = sql_cmp_to_ir(statement->join_where_comparison);
        const prdb_sdk_value_v1 *pv;
        if (op > 5 || statement->join_where_parameter_index == 0 ||
            statement->join_where_parameter_index > parameter_count || parameters == NULL) {
            return PRDB_SDK_OPTIMIZER_PLAN_UNSUPPORTED;
        }
        memset(expr_nodes, 0, sizeof(expr_nodes));
        expr_nodes[0].tag = 0x10;
        expr_nodes[0].relation_index = qualified_col_rel_index(statement, &statement->join_where_column);
        expr_nodes[0].bytes = statement->sql + statement->join_where_column.column.offset;
        expr_nodes[0].bytes_len = statement->join_where_column.column.length;
        pv = &parameters[statement->join_where_parameter_index - 1];
        if (pv->state == PRDB_SDK_VALUE_STATE_VALUE && pv->type_code == PRDB_SDK_VALUE_TYPE_INTEGER &&
            pv->data != NULL && pv->data_len == 4) {
            int32_t v = (int32_t)(((uint32_t)pv->data[0] << 24) | ((uint32_t)pv->data[1] << 16) |
                                  ((uint32_t)pv->data[2] << 8) | (uint32_t)pv->data[3]);
            expr_nodes[1].tag = 0x02;
            expr_nodes[1].int_val = (int64_t)v;
        } else {
            expr_nodes[1].tag = 0x11;
            expr_nodes[1].param_index = statement->join_where_parameter_index;
        }
        expr_nodes[2].tag = 0x20;
        expr_nodes[2].op = op;
        expr_nodes[2].children[0] = 0;
        expr_nodes[2].children[1] = 1;

        l_nodes[l_count].op_tag = PRDB_OPTIMIZER_LOGICAL_OP_FILTER;
        l_nodes[l_count].left_child = cur_root;
        l_nodes[l_count].expr_nodes = expr_nodes;
        l_nodes[l_count].expr_node_count = 3;
        l_nodes[l_count].expr_root = 2;
        cur_root = (uint16_t)l_count++;
    }

    for (i = 0; i < statement->join_projection_count; ++i) {
        proj_cols[i].relation_index = qualified_col_rel_index(statement, &statement->join_projections[i]);
        proj_cols[i].column_name = statement->sql + statement->join_projections[i].column.offset;
        proj_cols[i].column_name_len = statement->join_projections[i].column.length;
    }
    l_nodes[l_count].op_tag = PRDB_OPTIMIZER_LOGICAL_OP_PROJECT;
    l_nodes[l_count].left_child = cur_root;
    l_nodes[l_count].columns = proj_cols;
    l_nodes[l_count].column_count = statement->join_projection_count;
    cur_root = (uint16_t)l_count++;

    if (statement->has_limit) {
        l_nodes[l_count].op_tag = PRDB_OPTIMIZER_LOGICAL_OP_LIMIT;
        l_nodes[l_count].left_child = cur_root;
        l_nodes[l_count].limit_count = statement->limit;
        cur_root = (uint16_t)l_count++;
    }

    rc = prdb_sdk_optimizer_encode_unified_request_v1(
        schema, schema_len, l_nodes, l_count, cur_root, request, request_capacity, &req_len);
    if (rc != PRDB_SDK_OPTIMIZER_ENCODE_OK) return PRDB_SDK_OPTIMIZER_PLAN_INVALID_ARGUMENT;

    rc = prdb_sdk_optimizer_submit_unified_v1(
        host, PRDB_IPC_OP_OPTIMIZE_JOIN_SELECT, request, req_len,
        optimizer_response, optimizer_response_capacity, &resp_len,
        &opt_resp, sizeof(opt_resp));
    if (rc != PRDB_SDK_OPTIMIZER_RESULT_OK ||
        opt_resp.status != PRDB_SDK_OPTIMIZER_STATUS_SUCCESS ||
        opt_resp.plan == NULL) {
        return PRDB_SDK_OPTIMIZER_PLAN_UNAVAILABLE;
    }

    rc = prdb_sdk_optimizer_decode_unified_plan_v1(
        opt_resp.plan, opt_resp.plan_len, plan_out, sizeof(*plan_out));
    if (rc != PRDB_SDK_OPTIMIZER_PLAN_OK) return rc;

    if (prdb_sdk_executor_resolve_relation_v1(
            host, plan_out->schema_name, plan_out->schema_name_len,
            l_nodes[left_idx].relation_name, l_nodes[left_idx].relation_name_len,
            left_relation_response, left_relation_response_capacity, &resp_len,
            left_relation_out, sizeof(*left_relation_out),
            left_columns, left_column_capacity) != PRDB_SDK_EXECUTOR_OK ||
        prdb_sdk_executor_resolve_relation_v1(
            host, plan_out->schema_name, plan_out->schema_name_len,
            l_nodes[right_idx].relation_name, l_nodes[right_idx].relation_name_len,
            right_relation_response, right_relation_response_capacity, &resp_len,
            right_relation_out, sizeof(*right_relation_out),
            right_columns, right_column_capacity) != PRDB_SDK_EXECUTOR_OK) {
        return PRDB_SDK_OPTIMIZER_PLAN_UNAVAILABLE;
    }

    for (i = 0; i < plan_out->node_count; ++i) {
        const prdb_sdk_optimizer_physical_node_v1 *n = &plan_out->nodes[i];
        if (n->op_tag == PRDB_OPTIMIZER_PHYSICAL_OP_SEQ_SCAN ||
            n->op_tag == PRDB_OPTIMIZER_PHYSICAL_OP_INDEX_NESTED_LOOP_JOIN) {
            const prdb_sdk_executor_relation_v1 *rel =
                (n->relation_index == 0) ? left_relation_out : right_relation_out;
            const prdb_sdk_sql_column_shape_v1 *cols =
                (n->relation_index == 0) ? left_columns : right_columns;
            if (memcmp(rel->relation_id, n->relation_id, 16) != 0 ||
                !validate_scan_cols_against_executor(
                    n->scan_columns, n->scan_column_count, cols, rel->column_count)) {
                return PRDB_SDK_OPTIMIZER_PLAN_IDENTITY_MISMATCH;
            }
        }
    }
    return PRDB_SDK_OPTIMIZER_PLAN_OK;
}

int32_t prdb_sdk_optimizer_plan_group_unified_v1(
    const prdb_host_api_v1 *host, const prdb_sdk_select_v1 *statement,
    const prdb_sdk_value_v1 *parameters, uint32_t parameter_count,
    const uint8_t *schema, uint32_t schema_len,
    uint8_t *request, uint64_t request_capacity,
    uint8_t *optimizer_response, uint64_t optimizer_response_capacity,
    uint8_t *relation_response, uint64_t relation_response_capacity,
    prdb_sdk_executor_relation_v1 *relation_out,
    prdb_sdk_sql_column_shape_v1 *relation_columns, uint32_t relation_column_capacity,
    prdb_sdk_optimizer_unified_plan_v1 *plan_out) {
    prdb_sdk_optimizer_logical_node_v1 l_nodes[6];
    prdb_sdk_optimizer_expr_node_v1 expr_nodes[PRDB_SDK_OPTIMIZER_MAX_EXPR_NODES];
    prdb_sdk_optimizer_col_ref_v1 group_cols[PRDB_SDK_SQL_MAX_GROUP_KEYS];
    prdb_sdk_optimizer_agg_item_v1 agg_item;
    prdb_sdk_optimizer_order_item_v1 order_items[PRDB_SDK_SQL_MAX_ORDER_KEYS];
    prdb_sdk_optimizer_response_v1 opt_resp = {0};
    uint32_t l_count = 0, g_count = 0, i;
    uint16_t cur_root = 0;
    uint64_t req_len = 0, resp_len = 0;
    int32_t rc;

    if (host == NULL || statement == NULL || schema == NULL || plan_out == NULL ||
        relation_out == NULL || relation_columns == NULL) {
        return PRDB_SDK_OPTIMIZER_PLAN_INVALID_ARGUMENT;
    }

    memset(l_nodes, 0, sizeof(l_nodes));
    l_nodes[l_count].op_tag = PRDB_OPTIMIZER_LOGICAL_OP_SCAN;
    l_nodes[l_count].relation_index = 0;
    l_nodes[l_count].relation_name = statement->sql + statement->relation.offset;
    l_nodes[l_count].relation_name_len = statement->relation.length;
    cur_root = (uint16_t)l_count++;

    if (statement->predicate_count > 0) {
        uint32_t expr_count = 0;
        uint16_t expr_root = 0;
        if (!lower_sql_predicate_node(statement, parameters, parameter_count,
                                      statement->predicate_root, expr_nodes,
                                      PRDB_SDK_OPTIMIZER_MAX_EXPR_NODES,
                                      &expr_count, &expr_root)) {
            return PRDB_SDK_OPTIMIZER_PLAN_UNSUPPORTED;
        }
        l_nodes[l_count].op_tag = PRDB_OPTIMIZER_LOGICAL_OP_FILTER;
        l_nodes[l_count].left_child = cur_root;
        l_nodes[l_count].expr_nodes = expr_nodes;
        l_nodes[l_count].expr_node_count = expr_count;
        l_nodes[l_count].expr_root = expr_root;
        cur_root = (uint16_t)l_count++;
    }

    if (statement->group_key_count > 0) {
        g_count = statement->group_key_count;
        for (i = 0; i < g_count; ++i) {
            group_cols[i].relation_index = 0;
            group_cols[i].column_name = statement->sql + statement->group_keys[i].offset;
            group_cols[i].column_name_len = statement->group_keys[i].length;
        }
    } else if (statement->has_group_by) {
        g_count = 1;
        group_cols[0].relation_index = 0;
        group_cols[0].column_name = statement->sql + statement->group_by_column.offset;
        group_cols[0].column_name_len = statement->group_by_column.length;
    }

    memset(&agg_item, 0, sizeof(agg_item));
    switch (statement->aggregate_kind) {
    case PRDB_SDK_SQL_AGGREGATE_MIN:
        agg_item.kind = 2;
        agg_item.has_arg = 1;
        break;
    case PRDB_SDK_SQL_AGGREGATE_MAX:
        agg_item.kind = 3;
        agg_item.has_arg = 1;
        break;
    case PRDB_SDK_SQL_AGGREGATE_SUM:
        agg_item.kind = 4;
        agg_item.has_arg = 1;
        break;
    case PRDB_SDK_SQL_AGGREGATE_AVG:
        agg_item.kind = 5;
        agg_item.has_arg = 1;
        break;
    case PRDB_SDK_SQL_AGGREGATE_COUNT_COLUMN:
        agg_item.kind = 6;
        agg_item.has_arg = 1;
        break;
    default:
        agg_item.kind = 1;
        agg_item.has_arg = 0;
        break;
    }
    if (agg_item.has_arg) {
        agg_item.arg.relation_index = 0;
        agg_item.arg.column_name = statement->sql + statement->aggregate_column.offset;
        agg_item.arg.column_name_len = statement->aggregate_column.length;
    }

    l_nodes[l_count].op_tag = PRDB_OPTIMIZER_LOGICAL_OP_AGGREGATE;
    l_nodes[l_count].left_child = cur_root;
    l_nodes[l_count].columns = group_cols;
    l_nodes[l_count].column_count = g_count;
    l_nodes[l_count].aggs = &agg_item;
    l_nodes[l_count].agg_count = 1;
    cur_root = (uint16_t)l_count++;

    if (statement->order_count > 0 && statement->order_count <= PRDB_SDK_SQL_MAX_ORDER_KEYS) {
        for (i = 0; i < statement->order_count; ++i) {
            memset(&order_items[i], 0, sizeof(order_items[i]));
            order_items[i].descending =
                (statement->order_items[i].direction == PRDB_SDK_SQL_ORDER_DESCENDING) ? 1 : 0;
            order_items[i].nulls_first = 0;
            if (statement->order_items[i].kind == PRDB_SDK_SQL_ORDER_AGGREGATE) {
                order_items[i].is_agg_index = 1;
                order_items[i].agg_index = 0;
            } else {
                uint32_t gidx = statement->order_items[i].group_key_index;
                if (gidx >= g_count) return PRDB_SDK_OPTIMIZER_PLAN_INVALID_ARGUMENT;
                order_items[i].is_agg_index = 0;
                order_items[i].column = group_cols[gidx];
            }
        }
        l_nodes[l_count].op_tag = PRDB_OPTIMIZER_LOGICAL_OP_SORT;
        l_nodes[l_count].left_child = cur_root;
        l_nodes[l_count].order_items = order_items;
        l_nodes[l_count].order_item_count = statement->order_count;
        cur_root = (uint16_t)l_count++;
    } else if (statement->has_order_by) {
        memset(&order_items[0], 0, sizeof(order_items[0]));
        order_items[0].descending = statement->order_descending ? 1 : 0;
        order_items[0].nulls_first = 0;
        order_items[0].is_agg_index = 0;
        order_items[0].column.relation_index = 0;
        order_items[0].column.column_name = statement->sql + statement->order_by_column.offset;
        order_items[0].column.column_name_len = statement->order_by_column.length;
        l_nodes[l_count].op_tag = PRDB_OPTIMIZER_LOGICAL_OP_SORT;
        l_nodes[l_count].left_child = cur_root;
        l_nodes[l_count].order_items = order_items;
        l_nodes[l_count].order_item_count = 1;
        cur_root = (uint16_t)l_count++;
    }

    if (statement->has_limit) {
        l_nodes[l_count].op_tag = PRDB_OPTIMIZER_LOGICAL_OP_LIMIT;
        l_nodes[l_count].left_child = cur_root;
        l_nodes[l_count].limit_count = statement->limit;
        cur_root = (uint16_t)l_count++;
    }

    rc = prdb_sdk_optimizer_encode_unified_request_v1(
        schema, schema_len, l_nodes, l_count, cur_root, request, request_capacity, &req_len);
    if (rc != PRDB_SDK_OPTIMIZER_ENCODE_OK) return PRDB_SDK_OPTIMIZER_PLAN_INVALID_ARGUMENT;

    rc = prdb_sdk_optimizer_submit_unified_v1(
        host, PRDB_IPC_OP_OPTIMIZE_GROUP_COUNT, request, req_len,
        optimizer_response, optimizer_response_capacity, &resp_len,
        &opt_resp, sizeof(opt_resp));
    if (rc != PRDB_SDK_OPTIMIZER_RESULT_OK ||
        opt_resp.status != PRDB_SDK_OPTIMIZER_STATUS_SUCCESS ||
        opt_resp.plan == NULL) {
        return PRDB_SDK_OPTIMIZER_PLAN_UNAVAILABLE;
    }

    rc = prdb_sdk_optimizer_decode_unified_plan_v1(
        opt_resp.plan, opt_resp.plan_len, plan_out, sizeof(*plan_out));
    if (rc != PRDB_SDK_OPTIMIZER_PLAN_OK) return rc;

    return prdb_sdk_optimizer_validate_unified_plan_v1(
        host, plan_out, relation_response, relation_response_capacity,
        relation_out, sizeof(*relation_out), relation_columns, relation_column_capacity);
}
