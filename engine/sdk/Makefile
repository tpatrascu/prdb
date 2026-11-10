TCC ?= tcc
TCC_AR ?= $(TCC) -ar
SHARED_LINKER ?= $(CC)
PYTHON ?= python3
TAR ?= tar
GZIP ?= gzip
BUILD_DIR ?= target/sdk-v1
INCLUDE_DIR ?= engine/include
SDK_VALUE_SOURCE := engine/sdk/value_v1.c
SDK_BATCH_SOURCE := engine/sdk/batch_v1.c
SDK_OWNED_BATCH_SOURCE := engine/sdk/owned_batch_v1.c
SDK_SQL_SOURCE := engine/sdk/sql_v1.c
SDK_SQL_ORDER_SOURCE := engine/sdk/sql_order_v1.c
SDK_SQL_GROUP_SOURCE := engine/sdk/sql_group_v1.c
SDK_SQL_JOIN_SOURCE := engine/sdk/sql_join_v1.c
SDK_SQL_JOIN_V2_SOURCE := engine/sdk/sql_join_v2.c
SDK_PLANNER_SOURCE := engine/sdk/planner_v1.c
SDK_EXECUTOR_SOURCE := engine/sdk/executor_v1.c
SDK_SQL_EXEC_SOURCE := engine/sdk/sql_exec_v2.c
SDK_SQL_EXEC_V4_SOURCE := engine/sdk/sql_exec_v4.c
SDK_SQL_EXEC_V5_SOURCE := engine/sdk/sql_exec_v5.c
SDK_SQL_EXEC_V6_SOURCE := engine/sdk/sql_exec_v6.c
SDK_SQL_EXEC_V7_SOURCE := engine/sdk/sql_exec_v7.c
SDK_SQL_EXEC_V8_SOURCE := engine/sdk/sql_exec_v8.c
SDK_SQL_EXEC_V16_SOURCE := engine/sdk/sql_exec_v16.c
SDK_SQL_RESULT_SOURCE := engine/sdk/sql_result_v1.c
SDK_MANIFEST_SOURCE := engine/sdk/manifest_v1.c
SDK_SCOPE_SOURCE := engine/sdk/scope_v1.c
SDK_OPTIMIZER_SOURCE := engine/sdk/optimizer_v1.c
SDK_OPTIMIZER_PLAN_SOURCE := engine/sdk/optimizer_plan_v1.c
SDK_VALUE_OBJECT := $(BUILD_DIR)/value_v1.o
SDK_BATCH_OBJECT := $(BUILD_DIR)/batch_v1.o
SDK_OWNED_BATCH_OBJECT := $(BUILD_DIR)/owned_batch_v1.o
SDK_SQL_OBJECT := $(BUILD_DIR)/sql_v1.o
SDK_SQL_ORDER_OBJECT := $(BUILD_DIR)/sql_order_v1.o
SDK_SQL_GROUP_OBJECT := $(BUILD_DIR)/sql_group_v1.o
SDK_SQL_JOIN_OBJECT := $(BUILD_DIR)/sql_join_v1.o
SDK_SQL_JOIN_V2_OBJECT := $(BUILD_DIR)/sql_join_v2.o
SDK_PLANNER_OBJECT := $(BUILD_DIR)/planner_v1.o
SDK_EXECUTOR_OBJECT := $(BUILD_DIR)/executor_v1.o
SDK_SQL_EXEC_OBJECT := $(BUILD_DIR)/sql_exec_v2.o
SDK_SQL_EXEC_V4_OBJECT := $(BUILD_DIR)/sql_exec_v4.o
SDK_SQL_EXEC_V5_OBJECT := $(BUILD_DIR)/sql_exec_v5.o
SDK_SQL_EXEC_V6_OBJECT := $(BUILD_DIR)/sql_exec_v6.o
SDK_SQL_EXEC_V7_OBJECT := $(BUILD_DIR)/sql_exec_v7.o
SDK_SQL_EXEC_V8_OBJECT := $(BUILD_DIR)/sql_exec_v8.o
SDK_SQL_EXEC_V16_OBJECT := $(BUILD_DIR)/sql_exec_v16.o
SDK_SQL_RESULT_OBJECT := $(BUILD_DIR)/sql_result_v1.o
SDK_MANIFEST_OBJECT := $(BUILD_DIR)/manifest_v1.o
SDK_SCOPE_OBJECT := $(BUILD_DIR)/scope_v1.o
SDK_OPTIMIZER_OBJECT := $(BUILD_DIR)/optimizer_v1.o
SDK_OPTIMIZER_PLAN_OBJECT := $(BUILD_DIR)/optimizer_plan_v1.o
SDK_OBJECTS := $(SDK_VALUE_OBJECT) $(SDK_BATCH_OBJECT) $(SDK_OWNED_BATCH_OBJECT) $(SDK_SQL_OBJECT) $(SDK_SQL_ORDER_OBJECT) $(SDK_SQL_GROUP_OBJECT) $(SDK_SQL_JOIN_OBJECT) $(SDK_SQL_JOIN_V2_OBJECT) $(SDK_PLANNER_OBJECT) $(SDK_EXECUTOR_OBJECT) $(SDK_SQL_EXEC_OBJECT) $(SDK_SQL_EXEC_V4_OBJECT) $(SDK_SQL_EXEC_V5_OBJECT) $(SDK_SQL_EXEC_V6_OBJECT) $(SDK_SQL_EXEC_V7_OBJECT) $(SDK_SQL_EXEC_V8_OBJECT) $(SDK_SQL_EXEC_V16_OBJECT) $(SDK_SQL_RESULT_OBJECT) $(SDK_MANIFEST_OBJECT) $(SDK_SCOPE_OBJECT) $(SDK_OPTIMIZER_OBJECT) $(SDK_OPTIMIZER_PLAN_OBJECT)
SDK_ARCHIVE := $(BUILD_DIR)/libprdb_sdk.a
SDK_VALUE_TEST := $(BUILD_DIR)/sdk-value-v1
SDK_BATCH_TEST := $(BUILD_DIR)/sdk-batch-v1
SDK_SQL_TEST := $(BUILD_DIR)/sdk-sql-select-v1
SDK_SQL_EXPRESSION_TEST := $(BUILD_DIR)/sdk-sql-expression-v1
SDK_SQL_V2_TEST := $(BUILD_DIR)/sdk-sql-select-v2
SDK_SQL_ORDER_TEST := $(BUILD_DIR)/sdk-sql-order-v1
SDK_SQL_GROUP_TEST := $(BUILD_DIR)/sdk-sql-group-v1
SDK_SQL_JOIN_TEST := $(BUILD_DIR)/sdk-sql-join-v1
SDK_SQL_JOIN_V2_TEST := $(BUILD_DIR)/sdk-sql-join-v2
SDK_SQL_V3_TEST := $(BUILD_DIR)/sdk-sql-select-v3
SDK_SQL_V4_TEST := $(BUILD_DIR)/sdk-sql-select-v4
SDK_SQL_V5_TEST := $(BUILD_DIR)/sdk-sql-select-v5
SDK_SQL_V6_TEST := $(BUILD_DIR)/sdk-sql-select-v6
SDK_SQL_V7_TEST := $(BUILD_DIR)/sdk-sql-select-v7
SDK_SQL_V8_TEST := $(BUILD_DIR)/sdk-sql-select-v8
SDK_SQL_V9_TEST := $(BUILD_DIR)/sdk-sql-select-v9
SDK_SQL_V10_TEST := $(BUILD_DIR)/sdk-sql-select-v10
SDK_SQL_V11_TEST := $(BUILD_DIR)/sdk-sql-select-v11
SDK_SQL_V12_TEST := $(BUILD_DIR)/sdk-sql-select-v12
SDK_SQL_V13_TEST := $(BUILD_DIR)/sdk-sql-select-v13
SDK_SQL_V14_TEST := $(BUILD_DIR)/sdk-sql-select-v14
SDK_SQL_V15_TEST := $(BUILD_DIR)/sdk-sql-select-v15
SDK_SQL_V16_TEST := $(BUILD_DIR)/sdk-sql-select-v16
SDK_SQL_V17_TEST := $(BUILD_DIR)/sdk-sql-select-v17
SDK_SQL_V18_TEST := $(BUILD_DIR)/sdk-sql-select-v18
SDK_PLANNER_TEST := $(BUILD_DIR)/sdk-planner-v1
SDK_EXECUTOR_TEST := $(BUILD_DIR)/sdk-executor-v1
SDK_SQL_EXEC_TEST := $(BUILD_DIR)/sdk-sql-execute-v2
SDK_SQL_EXEC_V5_TEST := $(BUILD_DIR)/sdk-sql-execute-v5
SDK_SQL_EXEC_V6_TEST := $(BUILD_DIR)/sdk-sql-execute-v6
SDK_SQL_EXEC_V7_TEST := $(BUILD_DIR)/sdk-sql-execute-v7
SDK_SQL_EXEC_V8_TEST := $(BUILD_DIR)/sdk-sql-execute-v8
SDK_SQL_EXEC_V9_TEST := $(BUILD_DIR)/sdk-sql-execute-v9
SDK_SQL_EXEC_V10_TEST := $(BUILD_DIR)/sdk-sql-execute-v10
SDK_SQL_INFORMATION_SCHEMA_TEST := $(BUILD_DIR)/sdk-sql-information-schema-v2
SDK_SQL_RESULT_TEST := $(BUILD_DIR)/sdk-sql-result-v1
SDK_SQL_RESULT_V13_TEST := $(BUILD_DIR)/sdk-sql-result-v13
SDK_SQL_DML_TEST := $(BUILD_DIR)/sdk-sql-dml-v1
SDK_MANIFEST_TEST := $(BUILD_DIR)/sdk-manifest-v1
SDK_SCOPE_TEST := $(BUILD_DIR)/sdk-scope-v1
SDK_IPC_FRAME_TEST := $(BUILD_DIR)/ipc-frame-v1
SDK_IPC_PAYLOAD_CODEC_TEST := $(BUILD_DIR)/ipc-payload-codec-v1
SQL_V3_CAUGHT_ERROR_QUERY := $(BUILD_DIR)/sql-v3-caught-error.so
SQL_V3_WRITE_READ_QUERY := $(BUILD_DIR)/sql-v3-write-read.so
SQL_V3_RESTRICTED_METADATA_QUERY := $(BUILD_DIR)/sql-v3-restricted-metadata.so
SQL_V3_CAUGHT_ERROR_OBJECT := $(BUILD_DIR)/sql-v3-caught-error.o
SQL_V3_WRITE_READ_OBJECT := $(BUILD_DIR)/sql-v3-write-read.o
SQL_V3_RESTRICTED_METADATA_OBJECT := $(BUILD_DIR)/sql-v3-restricted-metadata.o
QUERY_ABI_GENERATED_HEADERS := $(INCLUDE_DIR)/prdb_query_abi_layout_assertions.h $(INCLUDE_DIR)/prdb_query_abi_records_v1.h $(INCLUDE_DIR)/prdb_query_abi_records_v2.h $(INCLUDE_DIR)/prdb_query_abi_records_v3.h
QUERY_ABI_V4_TEST := $(BUILD_DIR)/query-abi-v4-callbacks
SDK_OPTIMIZER_TEST := $(BUILD_DIR)/sdk-optimizer-v1
SDK_OPTIMIZER_PLAN_TEST := $(BUILD_DIR)/sdk-optimizer-plan-v1
QUERY_ABI_GENERATED_HEADERS += $(INCLUDE_DIR)/prdb_query_abi_records_v4.h
RELEASE_PROFILE_DIR ?= target/release-profile
RELEASE_PROFILE_ARCHIVE ?= target/prdb-release-profile.tar.gz
RELEASE_BINARIES := prdb-client prdb-executor prdb-gateway prdb-optimizer prdb-packager prdb-sandbox-probe prdb-supervisor

.PHONY: all test

all: $(SDK_ARCHIVE)

$(BUILD_DIR):
	mkdir -p "$@"

$(SDK_VALUE_OBJECT): $(SDK_VALUE_SOURCE) $(INCLUDE_DIR)/prdb_sdk_value_v1.h | $(BUILD_DIR)
	$(TCC) -Wall -Wextra -Werror -I"$(INCLUDE_DIR)" -c "$<" -o "$@"

$(SDK_BATCH_OBJECT): $(SDK_BATCH_SOURCE) $(INCLUDE_DIR)/prdb_sdk_batch_v1.h $(INCLUDE_DIR)/prdb_sdk_value_v1.h | $(BUILD_DIR)
	$(TCC) -Wall -Wextra -Werror -I"$(INCLUDE_DIR)" -c "$<" -o "$@"

$(SDK_OWNED_BATCH_OBJECT): $(SDK_OWNED_BATCH_SOURCE) $(INCLUDE_DIR)/prdb_sdk_owned_batch_v1.h $(INCLUDE_DIR)/prdb_sdk_batch_v1.h | $(BUILD_DIR)
	$(TCC) -Wall -Wextra -Werror -I"$(INCLUDE_DIR)" -c "$<" -o "$@"

$(SDK_SQL_OBJECT): $(SDK_SQL_SOURCE) $(INCLUDE_DIR)/prdb_sdk_sql_v1.h $(INCLUDE_DIR)/prdb_sdk_sql_v2.h $(INCLUDE_DIR)/prdb_sdk_sql_v3.h $(INCLUDE_DIR)/prdb_sdk_sql_v4.h $(INCLUDE_DIR)/prdb_sdk_sql_v5.h $(INCLUDE_DIR)/prdb_sdk_sql_v6.h $(INCLUDE_DIR)/prdb_sdk_sql_v7.h $(INCLUDE_DIR)/prdb_sdk_sql_v8.h $(INCLUDE_DIR)/prdb_sdk_sql_v9.h $(INCLUDE_DIR)/prdb_sdk_sql_v10.h $(INCLUDE_DIR)/prdb_sdk_sql_v11.h $(INCLUDE_DIR)/prdb_sdk_sql_v12.h $(INCLUDE_DIR)/prdb_sdk_sql_v13.h $(INCLUDE_DIR)/prdb_sdk_sql_v14.h $(INCLUDE_DIR)/prdb_sdk_sql_v15.h $(INCLUDE_DIR)/prdb_sdk_sql_v16.h $(INCLUDE_DIR)/prdb_sdk_sql_v17.h $(INCLUDE_DIR)/prdb_sdk_sql_v18.h $(INCLUDE_DIR)/prdb_sdk_sql_dml_v1.h $(INCLUDE_DIR)/prdb_sdk_sql_dml_v2.h $(INCLUDE_DIR)/prdb_sdk_batch_v1.h | $(BUILD_DIR)
	$(TCC) -Wall -Wextra -Werror -I"$(INCLUDE_DIR)" -c "$<" -o "$@"

$(SDK_SQL_ORDER_OBJECT): $(SDK_SQL_ORDER_SOURCE) $(INCLUDE_DIR)/prdb_sdk_sql_order_v1.h $(INCLUDE_DIR)/prdb_sdk_sql_order_v2.h $(INCLUDE_DIR)/prdb_sdk_sql_order_v3.h $(INCLUDE_DIR)/prdb_sdk_sql_v1.h $(INCLUDE_DIR)/prdb_sdk_value_v1.h | $(BUILD_DIR)
	$(TCC) -Wall -Wextra -Werror -I"$(INCLUDE_DIR)" -c "$<" -o "$@"

$(SDK_SQL_GROUP_OBJECT): $(SDK_SQL_GROUP_SOURCE) $(INCLUDE_DIR)/prdb_sdk_sql_group_v1.h $(INCLUDE_DIR)/prdb_sdk_sql_v1.h $(INCLUDE_DIR)/prdb_sdk_value_v1.h | $(BUILD_DIR)
	$(TCC) -Wall -Wextra -Werror -I"$(INCLUDE_DIR)" -c "$<" -o "$@"

$(SDK_SQL_JOIN_OBJECT): $(SDK_SQL_JOIN_SOURCE) $(INCLUDE_DIR)/prdb_sdk_sql_join_v1.h $(INCLUDE_DIR)/prdb_sdk_sql_v1.h $(INCLUDE_DIR)/prdb_sdk_value_v1.h | $(BUILD_DIR)
	$(TCC) -Wall -Wextra -Werror -I"$(INCLUDE_DIR)" -c "$<" -o "$@"

$(SDK_SQL_JOIN_V2_OBJECT): $(SDK_SQL_JOIN_V2_SOURCE) $(INCLUDE_DIR)/prdb_sdk_sql_join_v2.h $(INCLUDE_DIR)/prdb_sdk_sql_join_v1.h $(INCLUDE_DIR)/prdb_sdk_sql_v1.h $(INCLUDE_DIR)/prdb_sdk_value_v1.h | $(BUILD_DIR)
	$(TCC) -Wall -Wextra -Werror -I"$(INCLUDE_DIR)" -c "$<" -o "$@"

$(SDK_PLANNER_OBJECT): $(SDK_PLANNER_SOURCE) $(INCLUDE_DIR)/prdb_sdk_planner_v1.h $(INCLUDE_DIR)/prdb_sdk_sql_v2.h $(INCLUDE_DIR)/prdb_sdk_batch_v1.h | $(BUILD_DIR)
	$(TCC) -Wall -Wextra -Werror -I"$(INCLUDE_DIR)" -c "$<" -o "$@"

$(SDK_EXECUTOR_OBJECT): $(SDK_EXECUTOR_SOURCE) $(INCLUDE_DIR)/prdb_sdk_executor_v1.h $(INCLUDE_DIR)/prdb_query_abi_v2.h $(INCLUDE_DIR)/prdb_query_abi_constants.h $(QUERY_ABI_GENERATED_HEADERS) $(INCLUDE_DIR)/prdb_sdk_planner_v1.h $(INCLUDE_DIR)/prdb_sdk_batch_v1.h | $(BUILD_DIR)
	$(TCC) -Wall -Wextra -Werror -I"$(INCLUDE_DIR)" -c "$<" -o "$@"

$(SDK_SQL_EXEC_OBJECT): $(SDK_SQL_EXEC_SOURCE) $(INCLUDE_DIR)/prdb_sdk_sql_exec_v2.h $(INCLUDE_DIR)/prdb_sdk_sql_exec_v3.h $(INCLUDE_DIR)/prdb_sdk_sql_dml_v1.h $(INCLUDE_DIR)/prdb_sdk_sql_dml_v2.h $(INCLUDE_DIR)/prdb_query_abi_v3.h $(INCLUDE_DIR)/prdb_query_abi_constants.h $(QUERY_ABI_GENERATED_HEADERS) $(INCLUDE_DIR)/prdb_sdk_executor_v1.h $(INCLUDE_DIR)/prdb_sdk_planner_v1.h $(INCLUDE_DIR)/prdb_sdk_sql_v2.h $(INCLUDE_DIR)/prdb_sdk_sql_v3.h $(INCLUDE_DIR)/prdb_sdk_sql_order_v1.h | $(BUILD_DIR)
	$(TCC) -Wall -Wextra -Werror -I"$(INCLUDE_DIR)" -c "$<" -o "$@"

$(SDK_SQL_EXEC_V4_OBJECT): $(SDK_SQL_EXEC_V4_SOURCE) $(INCLUDE_DIR)/prdb_sdk_sql_exec_v4.h $(INCLUDE_DIR)/prdb_sdk_sql_v4.h $(INCLUDE_DIR)/prdb_sdk_sql_group_v1.h $(INCLUDE_DIR)/prdb_sdk_executor_v1.h | $(BUILD_DIR)
	$(TCC) -Wall -Wextra -Werror -I"$(INCLUDE_DIR)" -c "$<" -o "$@"

$(SDK_SQL_EXEC_V5_OBJECT): $(SDK_SQL_EXEC_V5_SOURCE) $(INCLUDE_DIR)/prdb_sdk_sql_exec_v5.h $(INCLUDE_DIR)/prdb_sdk_sql_v5.h $(INCLUDE_DIR)/prdb_sdk_sql_group_v1.h $(INCLUDE_DIR)/prdb_sdk_executor_v1.h | $(BUILD_DIR)
	$(TCC) -Wall -Wextra -Werror -I"$(INCLUDE_DIR)" -c "$<" -o "$@"

$(SDK_SQL_EXEC_V6_OBJECT): $(SDK_SQL_EXEC_V6_SOURCE) $(INCLUDE_DIR)/prdb_sdk_sql_exec_v6.h $(INCLUDE_DIR)/prdb_sdk_sql_v6.h $(INCLUDE_DIR)/prdb_sdk_sql_group_v1.h $(INCLUDE_DIR)/prdb_sdk_executor_v1.h | $(BUILD_DIR)
	$(TCC) -Wall -Wextra -Werror -I"$(INCLUDE_DIR)" -c "$<" -o "$@"

$(SDK_SQL_EXEC_V7_OBJECT): $(SDK_SQL_EXEC_V7_SOURCE) $(INCLUDE_DIR)/prdb_sdk_sql_exec_v7.h $(INCLUDE_DIR)/prdb_sdk_sql_exec_v9.h $(INCLUDE_DIR)/prdb_sdk_sql_exec_v10.h $(INCLUDE_DIR)/prdb_sdk_sql_exec_v11.h $(INCLUDE_DIR)/prdb_sdk_sql_exec_v12.h $(INCLUDE_DIR)/prdb_sdk_sql_exec_v13.h $(INCLUDE_DIR)/prdb_sdk_sql_exec_v14.h $(INCLUDE_DIR)/prdb_sdk_sql_exec_v15.h $(INCLUDE_DIR)/prdb_sdk_sql_exec_v17.h $(INCLUDE_DIR)/prdb_sdk_sql_exec_v18.h $(INCLUDE_DIR)/prdb_sdk_sql_v7.h $(INCLUDE_DIR)/prdb_sdk_sql_v9.h $(INCLUDE_DIR)/prdb_sdk_sql_v10.h $(INCLUDE_DIR)/prdb_sdk_sql_v11.h $(INCLUDE_DIR)/prdb_sdk_sql_v12.h $(INCLUDE_DIR)/prdb_sdk_sql_v13.h $(INCLUDE_DIR)/prdb_sdk_sql_v14.h $(INCLUDE_DIR)/prdb_sdk_sql_v15.h $(INCLUDE_DIR)/prdb_sdk_sql_v17.h $(INCLUDE_DIR)/prdb_sdk_sql_v18.h $(INCLUDE_DIR)/prdb_sdk_sql_group_v1.h $(INCLUDE_DIR)/prdb_sdk_sql_order_v2.h $(INCLUDE_DIR)/prdb_sdk_sql_order_v3.h $(INCLUDE_DIR)/prdb_sdk_executor_v1.h | $(BUILD_DIR)
	$(TCC) -Wall -Wextra -Werror -I"$(INCLUDE_DIR)" -c "$<" -o "$@"

$(SDK_SQL_EXEC_V8_OBJECT): $(SDK_SQL_EXEC_V8_SOURCE) $(INCLUDE_DIR)/prdb_sdk_sql_exec_v8.h $(INCLUDE_DIR)/prdb_sdk_sql_v8.h $(INCLUDE_DIR)/prdb_sdk_sql_join_v1.h $(INCLUDE_DIR)/prdb_sdk_executor_v1.h | $(BUILD_DIR)
	$(TCC) -Wall -Wextra -Werror -I"$(INCLUDE_DIR)" -c "$<" -o "$@"

$(SDK_SQL_EXEC_V16_OBJECT): $(SDK_SQL_EXEC_V16_SOURCE) $(INCLUDE_DIR)/prdb_sdk_sql_exec_v16.h $(INCLUDE_DIR)/prdb_sdk_sql_exec_v8.h $(INCLUDE_DIR)/prdb_sdk_sql_v16.h $(INCLUDE_DIR)/prdb_sdk_sql_join_v2.h $(INCLUDE_DIR)/prdb_sdk_executor_v1.h | $(BUILD_DIR)
	$(TCC) -Wall -Wextra -Werror -I"$(INCLUDE_DIR)" -c "$<" -o "$@"

$(SDK_SQL_RESULT_OBJECT): $(SDK_SQL_RESULT_SOURCE) $(INCLUDE_DIR)/prdb_sdk_sql_result_v1.h $(INCLUDE_DIR)/prdb_sdk_sql_v2.h $(INCLUDE_DIR)/prdb_sdk_sql_v5.h $(INCLUDE_DIR)/prdb_sdk_sql_v6.h $(INCLUDE_DIR)/prdb_sdk_sql_v8.h $(INCLUDE_DIR)/prdb_sdk_sql_v13.h $(INCLUDE_DIR)/prdb_sdk_batch_v1.h $(INCLUDE_DIR)/prdb_sdk_value_v1.h | $(BUILD_DIR)
	$(TCC) -Wall -Wextra -Werror -I"$(INCLUDE_DIR)" -c "$<" -o "$@"

$(SDK_MANIFEST_OBJECT): $(SDK_MANIFEST_SOURCE) $(INCLUDE_DIR)/prdb_sdk_manifest_v1.h | $(BUILD_DIR)
	$(TCC) -Wall -Wextra -Werror -I"$(INCLUDE_DIR)" -c "$<" -o "$@"

$(SDK_SCOPE_OBJECT): $(SDK_SCOPE_SOURCE) $(INCLUDE_DIR)/prdb_sdk_scope_v1.h | $(BUILD_DIR)
	$(TCC) -Wall -Wextra -Werror -I"$(INCLUDE_DIR)" -c "$<" -o "$@"

$(SDK_OPTIMIZER_OBJECT): $(SDK_OPTIMIZER_SOURCE) $(INCLUDE_DIR)/prdb_sdk_optimizer_v1.h $(INCLUDE_DIR)/prdb_sdk_sql_v2.h $(INCLUDE_DIR)/prdb_query_abi_v4.h $(QUERY_ABI_GENERATED_HEADERS) | $(BUILD_DIR)
	$(TCC) -Wall -Wextra -Werror -I"$(INCLUDE_DIR)" -c "$<" -o "$@"

$(SDK_OPTIMIZER_PLAN_OBJECT): $(SDK_OPTIMIZER_PLAN_SOURCE) $(INCLUDE_DIR)/prdb_sdk_optimizer_plan_v1.h $(INCLUDE_DIR)/prdb_sdk_optimizer_v1.h | $(BUILD_DIR)
	$(TCC) -Wall -Wextra -Werror -I"$(INCLUDE_DIR)" -c "$<" -o "$@"

$(SDK_ARCHIVE): $(SDK_OBJECTS)
	$(TCC_AR) rcs "$@" $(SDK_OBJECTS)

$(SDK_VALUE_TEST): engine/tests/fixtures/sdk_value_v1.c $(SDK_ARCHIVE) | $(BUILD_DIR)
	$(TCC) -Wall -Wextra -Werror -I"$(INCLUDE_DIR)" "$<" "$(SDK_ARCHIVE)" -o "$@"

$(SDK_BATCH_TEST): engine/tests/fixtures/sdk_batch_v1.c $(INCLUDE_DIR)/prdb_sdk_owned_batch_v1.h $(SDK_ARCHIVE) | $(BUILD_DIR)
	$(TCC) -Wall -Wextra -Werror -I"$(INCLUDE_DIR)" "$<" "$(SDK_ARCHIVE)" -o "$@"

$(SDK_SQL_TEST): engine/tests/fixtures/sdk_sql_select_v1.c $(SDK_ARCHIVE) | $(BUILD_DIR)
	$(TCC) -Wall -Wextra -Werror -I"$(INCLUDE_DIR)" "$<" "$(SDK_ARCHIVE)" -o "$@"

$(SDK_SQL_EXPRESSION_TEST): engine/tests/fixtures/sdk_sql_expression_v1.c $(SDK_ARCHIVE) | $(BUILD_DIR)
	$(TCC) -Wall -Wextra -Werror -I"$(INCLUDE_DIR)" "$<" "$(SDK_ARCHIVE)" -o "$@"

$(SDK_SQL_V2_TEST): engine/tests/fixtures/sdk_sql_select_v2.c $(SDK_ARCHIVE) $(INCLUDE_DIR)/prdb_sdk_sql_v2.h $(INCLUDE_DIR)/prdb_sdk_batch_v1.h | $(BUILD_DIR)
	$(TCC) -Wall -Wextra -Werror -I"$(INCLUDE_DIR)" "$<" "$(SDK_ARCHIVE)" -o "$@"

$(SDK_SQL_ORDER_TEST): engine/tests/fixtures/sdk_sql_order_v1.c $(SDK_ARCHIVE) $(INCLUDE_DIR)/prdb_sdk_sql_order_v1.h $(INCLUDE_DIR)/prdb_sdk_sql_order_v2.h $(INCLUDE_DIR)/prdb_sdk_sql_order_v3.h | $(BUILD_DIR)
	$(TCC) -Wall -Wextra -Werror -I"$(INCLUDE_DIR)" "$<" "$(SDK_ARCHIVE)" -o "$@"

$(SDK_SQL_GROUP_TEST): engine/tests/fixtures/sdk_sql_group_v1.c $(SDK_ARCHIVE) $(INCLUDE_DIR)/prdb_sdk_sql_group_v1.h | $(BUILD_DIR)
	$(TCC) -Wall -Wextra -Werror -I"$(INCLUDE_DIR)" "$<" "$(SDK_ARCHIVE)" -o "$@"

$(SDK_SQL_JOIN_TEST): engine/tests/fixtures/sdk_sql_join_v1.c $(SDK_ARCHIVE) $(INCLUDE_DIR)/prdb_sdk_sql_join_v1.h | $(BUILD_DIR)
	$(TCC) -Wall -Wextra -Werror -I"$(INCLUDE_DIR)" "$<" "$(SDK_ARCHIVE)" -o "$@"

$(SDK_SQL_JOIN_V2_TEST): engine/tests/fixtures/sdk_sql_join_v2.c $(SDK_ARCHIVE) $(INCLUDE_DIR)/prdb_sdk_sql_join_v2.h | $(BUILD_DIR)
	$(TCC) -Wall -Wextra -Werror -I"$(INCLUDE_DIR)" "$<" "$(SDK_ARCHIVE)" -o "$@"

$(SDK_SQL_V3_TEST): engine/tests/fixtures/sdk_sql_select_v3.c $(SDK_ARCHIVE) $(INCLUDE_DIR)/prdb_sdk_sql_v3.h | $(BUILD_DIR)
	$(TCC) -Wall -Wextra -Werror -I"$(INCLUDE_DIR)" "$<" "$(SDK_ARCHIVE)" -o "$@"

$(SDK_SQL_V4_TEST): engine/tests/fixtures/sdk_sql_select_v4.c $(SDK_ARCHIVE) $(INCLUDE_DIR)/prdb_sdk_sql_v4.h | $(BUILD_DIR)
	$(TCC) -Wall -Wextra -Werror -I"$(INCLUDE_DIR)" "$<" "$(SDK_ARCHIVE)" -o "$@"

$(SDK_SQL_V5_TEST): engine/tests/fixtures/sdk_sql_select_v5.c $(SDK_ARCHIVE) $(INCLUDE_DIR)/prdb_sdk_sql_v5.h | $(BUILD_DIR)
	$(TCC) -Wall -Wextra -Werror -I"$(INCLUDE_DIR)" "$<" "$(SDK_ARCHIVE)" -o "$@"

$(SDK_SQL_V6_TEST): engine/tests/fixtures/sdk_sql_select_v6.c $(SDK_ARCHIVE) $(INCLUDE_DIR)/prdb_sdk_sql_v5.h $(INCLUDE_DIR)/prdb_sdk_sql_v6.h | $(BUILD_DIR)
	$(TCC) -Wall -Wextra -Werror -I"$(INCLUDE_DIR)" "$<" "$(SDK_ARCHIVE)" -o "$@"

$(SDK_SQL_V7_TEST): engine/tests/fixtures/sdk_sql_select_v7.c $(SDK_ARCHIVE) $(INCLUDE_DIR)/prdb_sdk_sql_v6.h $(INCLUDE_DIR)/prdb_sdk_sql_v7.h | $(BUILD_DIR)
	$(TCC) -Wall -Wextra -Werror -I"$(INCLUDE_DIR)" "$<" "$(SDK_ARCHIVE)" -o "$@"

$(SDK_SQL_V8_TEST): engine/tests/fixtures/sdk_sql_select_v8.c $(SDK_ARCHIVE) $(INCLUDE_DIR)/prdb_sdk_sql_v7.h $(INCLUDE_DIR)/prdb_sdk_sql_v8.h | $(BUILD_DIR)
	$(TCC) -Wall -Wextra -Werror -I"$(INCLUDE_DIR)" "$<" "$(SDK_ARCHIVE)" -o "$@"

$(SDK_SQL_V9_TEST): engine/tests/fixtures/sdk_sql_select_v9.c $(SDK_ARCHIVE) $(INCLUDE_DIR)/prdb_sdk_sql_v9.h $(INCLUDE_DIR)/prdb_sdk_sql_v7.h | $(BUILD_DIR)
	$(TCC) -Wall -Wextra -Werror -I"$(INCLUDE_DIR)" "$<" "$(SDK_ARCHIVE)" -o "$@"

$(SDK_SQL_V10_TEST): engine/tests/fixtures/sdk_sql_select_v10.c $(SDK_ARCHIVE) $(INCLUDE_DIR)/prdb_sdk_sql_v10.h $(INCLUDE_DIR)/prdb_sdk_sql_v9.h | $(BUILD_DIR)
	$(TCC) -Wall -Wextra -Werror -I"$(INCLUDE_DIR)" "$<" "$(SDK_ARCHIVE)" -o "$@"

$(SDK_SQL_V11_TEST): engine/tests/fixtures/sdk_sql_select_v11.c $(SDK_ARCHIVE) $(INCLUDE_DIR)/prdb_sdk_sql_v11.h $(INCLUDE_DIR)/prdb_sdk_sql_v10.h | $(BUILD_DIR)
	$(TCC) -Wall -Wextra -Werror -I"$(INCLUDE_DIR)" "$<" "$(SDK_ARCHIVE)" -o "$@"

$(SDK_SQL_V12_TEST): engine/tests/fixtures/sdk_sql_select_v12.c $(SDK_ARCHIVE) $(INCLUDE_DIR)/prdb_sdk_sql_v12.h $(INCLUDE_DIR)/prdb_sdk_sql_v11.h | $(BUILD_DIR)
	$(TCC) -Wall -Wextra -Werror -I"$(INCLUDE_DIR)" "$<" "$(SDK_ARCHIVE)" -o "$@"

$(SDK_SQL_V13_TEST): engine/tests/fixtures/sdk_sql_select_v13.c $(SDK_ARCHIVE) $(INCLUDE_DIR)/prdb_sdk_sql_v13.h $(INCLUDE_DIR)/prdb_sdk_sql_v12.h | $(BUILD_DIR)
	$(TCC) -Wall -Wextra -Werror -I"$(INCLUDE_DIR)" "$<" "$(SDK_ARCHIVE)" -o "$@"

$(SDK_SQL_V14_TEST): engine/tests/fixtures/sdk_sql_select_v14.c $(SDK_ARCHIVE) $(INCLUDE_DIR)/prdb_sdk_sql_v14.h | $(BUILD_DIR)
	$(TCC) -Wall -Wextra -Werror -I"$(INCLUDE_DIR)" "$<" "$(SDK_ARCHIVE)" -o "$@"

$(SDK_SQL_V15_TEST): engine/tests/fixtures/sdk_sql_select_v15.c $(SDK_ARCHIVE) $(INCLUDE_DIR)/prdb_sdk_sql_v15.h | $(BUILD_DIR)
	$(TCC) -Wall -Wextra -Werror -I"$(INCLUDE_DIR)" "$<" "$(SDK_ARCHIVE)" -o "$@"

$(SDK_SQL_V16_TEST): engine/tests/fixtures/sdk_sql_select_v16.c $(SDK_ARCHIVE) $(INCLUDE_DIR)/prdb_sdk_sql_v16.h | $(BUILD_DIR)
	$(TCC) -Wall -Wextra -Werror -I"$(INCLUDE_DIR)" "$<" "$(SDK_ARCHIVE)" -o "$@"

$(SDK_SQL_V17_TEST): engine/tests/fixtures/sdk_sql_select_v17.c $(SDK_ARCHIVE) $(INCLUDE_DIR)/prdb_sdk_sql_v17.h | $(BUILD_DIR)
	$(TCC) -Wall -Wextra -Werror -I"$(INCLUDE_DIR)" "$<" "$(SDK_ARCHIVE)" -o "$@"

$(SDK_SQL_V18_TEST): engine/tests/fixtures/sdk_sql_select_v18.c $(SDK_ARCHIVE) $(INCLUDE_DIR)/prdb_sdk_sql_v18.h | $(BUILD_DIR)
	$(TCC) -Wall -Wextra -Werror -I"$(INCLUDE_DIR)" "$<" "$(SDK_ARCHIVE)" -o "$@"

$(SDK_PLANNER_TEST): engine/tests/fixtures/sdk_planner_v1.c $(SDK_ARCHIVE) $(INCLUDE_DIR)/prdb_sdk_planner_v1.h $(INCLUDE_DIR)/prdb_sdk_sql_v2.h $(INCLUDE_DIR)/prdb_sdk_batch_v1.h | $(BUILD_DIR)
	$(TCC) -Wall -Wextra -Werror -I"$(INCLUDE_DIR)" "$<" "$(SDK_ARCHIVE)" -o "$@"

$(SDK_EXECUTOR_TEST): engine/tests/fixtures/sdk_executor_v1.c $(SDK_ARCHIVE) $(INCLUDE_DIR)/prdb_sdk_executor_v1.h $(INCLUDE_DIR)/prdb_query_abi_v2.h $(INCLUDE_DIR)/prdb_query_abi_constants.h $(QUERY_ABI_GENERATED_HEADERS) | $(BUILD_DIR)
	$(TCC) -Wall -Wextra -Werror -I"$(INCLUDE_DIR)" "$<" "$(SDK_ARCHIVE)" -o "$@"

$(SDK_SQL_EXEC_TEST): engine/tests/fixtures/sdk_sql_execute_v2.c $(SDK_ARCHIVE) $(INCLUDE_DIR)/prdb_sdk_sql_exec_v2.h $(INCLUDE_DIR)/prdb_sdk_sql_exec_v3.h $(INCLUDE_DIR)/prdb_sdk_sql_exec_v4.h $(INCLUDE_DIR)/prdb_sdk_sql_v3.h $(INCLUDE_DIR)/prdb_sdk_sql_result_v1.h $(INCLUDE_DIR)/prdb_sdk_executor_v1.h $(INCLUDE_DIR)/prdb_sdk_planner_v1.h | $(BUILD_DIR)
	$(TCC) -Wall -Wextra -Werror -I"$(INCLUDE_DIR)" "$<" "$(SDK_ARCHIVE)" -o "$@"

$(SDK_SQL_EXEC_V5_TEST): engine/tests/fixtures/sdk_sql_execute_v5.c $(SDK_ARCHIVE) $(INCLUDE_DIR)/prdb_sdk_sql_exec_v5.h $(INCLUDE_DIR)/prdb_sdk_sql_v5.h | $(BUILD_DIR)
	$(TCC) -Wall -Wextra -Werror -I"$(INCLUDE_DIR)" "$<" "$(SDK_ARCHIVE)" -o "$@"

$(SDK_SQL_EXEC_V6_TEST): engine/tests/fixtures/sdk_sql_execute_v6.c $(SDK_ARCHIVE) $(INCLUDE_DIR)/prdb_sdk_sql_exec_v6.h $(INCLUDE_DIR)/prdb_sdk_sql_v6.h | $(BUILD_DIR)
	$(TCC) -Wall -Wextra -Werror -I"$(INCLUDE_DIR)" "$<" "$(SDK_ARCHIVE)" -o "$@"

$(SDK_SQL_EXEC_V7_TEST): engine/tests/fixtures/sdk_sql_execute_v7.c $(SDK_ARCHIVE) $(INCLUDE_DIR)/prdb_sdk_sql_exec_v7.h $(INCLUDE_DIR)/prdb_sdk_sql_v7.h | $(BUILD_DIR)
	$(TCC) -Wall -Wextra -Werror -I"$(INCLUDE_DIR)" "$<" "$(SDK_ARCHIVE)" -o "$@"

$(SDK_SQL_EXEC_V8_TEST): engine/tests/fixtures/sdk_sql_execute_v8.c $(SDK_ARCHIVE) $(INCLUDE_DIR)/prdb_sdk_sql_exec_v8.h $(INCLUDE_DIR)/prdb_sdk_sql_exec_v16.h $(INCLUDE_DIR)/prdb_sdk_sql_v8.h | $(BUILD_DIR)
	$(TCC) -Wall -Wextra -Werror -I"$(INCLUDE_DIR)" "$<" "$(SDK_ARCHIVE)" -o "$@"

$(SDK_SQL_EXEC_V9_TEST): engine/tests/fixtures/sdk_sql_execute_v9.c $(SDK_ARCHIVE) $(INCLUDE_DIR)/prdb_sdk_sql_exec_v9.h $(INCLUDE_DIR)/prdb_sdk_sql_v9.h | $(BUILD_DIR)
	$(TCC) -Wall -Wextra -Werror -I"$(INCLUDE_DIR)" "$<" "$(SDK_ARCHIVE)" -o "$@"

$(SDK_SQL_EXEC_V10_TEST): engine/tests/fixtures/sdk_sql_execute_v10.c $(SDK_ARCHIVE) $(INCLUDE_DIR)/prdb_sdk_sql_exec_v10.h $(INCLUDE_DIR)/prdb_sdk_sql_exec_v11.h $(INCLUDE_DIR)/prdb_sdk_sql_exec_v12.h $(INCLUDE_DIR)/prdb_sdk_sql_exec_v13.h $(INCLUDE_DIR)/prdb_sdk_sql_exec_v14.h $(INCLUDE_DIR)/prdb_sdk_sql_exec_v15.h $(INCLUDE_DIR)/prdb_sdk_sql_exec_v17.h $(INCLUDE_DIR)/prdb_sdk_sql_exec_v18.h $(INCLUDE_DIR)/prdb_sdk_sql_v10.h $(INCLUDE_DIR)/prdb_sdk_sql_v11.h $(INCLUDE_DIR)/prdb_sdk_sql_v12.h $(INCLUDE_DIR)/prdb_sdk_sql_v13.h $(INCLUDE_DIR)/prdb_sdk_sql_v14.h $(INCLUDE_DIR)/prdb_sdk_sql_v15.h $(INCLUDE_DIR)/prdb_sdk_sql_v17.h $(INCLUDE_DIR)/prdb_sdk_sql_v18.h | $(BUILD_DIR)
	$(TCC) -Wall -Wextra -Werror -I"$(INCLUDE_DIR)" "$<" "$(SDK_ARCHIVE)" -o "$@"

$(SDK_SQL_INFORMATION_SCHEMA_TEST): engine/tests/fixtures/sdk_sql_information_schema_v2.c $(SDK_ARCHIVE) $(INCLUDE_DIR)/prdb_sdk_sql_exec_v2.h $(INCLUDE_DIR)/prdb_sdk_executor_v1.h $(INCLUDE_DIR)/prdb_sdk_planner_v1.h | $(BUILD_DIR)
	$(TCC) -Wall -Wextra -Werror -I"$(INCLUDE_DIR)" "$<" "$(SDK_ARCHIVE)" -o "$@"

$(SDK_SQL_RESULT_TEST): engine/tests/fixtures/sdk_sql_result_v1.c $(SDK_ARCHIVE) $(INCLUDE_DIR)/prdb_sdk_sql_result_v1.h $(INCLUDE_DIR)/prdb_sdk_sql_v2.h $(INCLUDE_DIR)/prdb_sdk_sql_v5.h $(INCLUDE_DIR)/prdb_sdk_sql_v8.h $(INCLUDE_DIR)/prdb_sdk_batch_v1.h | $(BUILD_DIR)
	$(TCC) -Wall -Wextra -Werror -I"$(INCLUDE_DIR)" "$<" "$(SDK_ARCHIVE)" -o "$@"

$(SDK_SQL_RESULT_V13_TEST): engine/tests/fixtures/sdk_sql_result_v13.c $(SDK_ARCHIVE) $(INCLUDE_DIR)/prdb_sdk_sql_result_v1.h $(INCLUDE_DIR)/prdb_sdk_sql_v13.h | $(BUILD_DIR)
	$(TCC) -Wall -Wextra -Werror -I"$(INCLUDE_DIR)" "$<" "$(SDK_ARCHIVE)" -o "$@"

$(SDK_SQL_DML_TEST): engine/tests/fixtures/sdk_sql_dml_v1.c $(SDK_ARCHIVE) $(INCLUDE_DIR)/prdb_sdk_sql_dml_v1.h $(INCLUDE_DIR)/prdb_sdk_sql_dml_v2.h | $(BUILD_DIR)
	$(TCC) -Wall -Wextra -Werror -I"$(INCLUDE_DIR)" "$<" "$(SDK_ARCHIVE)" -o "$@"

$(SDK_MANIFEST_TEST): engine/tests/fixtures/sdk_manifest_v1.c $(SDK_ARCHIVE) $(INCLUDE_DIR)/prdb_sdk_manifest_v1.h | $(BUILD_DIR)
	$(TCC) -Wall -Wextra -Werror -I"$(INCLUDE_DIR)" "$<" "$(SDK_ARCHIVE)" -o "$@"

$(SDK_SCOPE_TEST): engine/tests/fixtures/sdk_scope_v1.c $(SDK_ARCHIVE) $(INCLUDE_DIR)/prdb_sdk_scope_v1.h | $(BUILD_DIR)
	$(TCC) -Wall -Wextra -Werror -I"$(INCLUDE_DIR)" "$<" "$(SDK_ARCHIVE)" -o "$@"

$(SDK_IPC_FRAME_TEST): engine/tests/fixtures/ipc_frame_v1.c engine/csrc/ipc_frame_v1.c $(INCLUDE_DIR)/prdb_ipc_frame_v1.h $(INCLUDE_DIR)/prdb_query_abi_constants.h $(INCLUDE_DIR)/prdb_ipc_operation_cases_v1.h | $(BUILD_DIR)
	$(TCC) -Wall -Wextra -Werror -I"$(INCLUDE_DIR)" engine/tests/fixtures/ipc_frame_v1.c engine/csrc/ipc_frame_v1.c -o "$@"

$(SDK_IPC_PAYLOAD_CODEC_TEST): engine/tests/fixtures/ipc_payload_codec_v1.c $(INCLUDE_DIR)/prdb_query_abi_payload_codecs.h $(INCLUDE_DIR)/prdb_query_abi_constants.h | $(BUILD_DIR)
	$(TCC) -Wall -Wextra -Werror -I"$(INCLUDE_DIR)" "$<" -o "$@"

$(QUERY_ABI_V4_TEST): engine/tests/fixtures/query_abi_v4_callbacks.c $(INCLUDE_DIR)/prdb_query_abi_v4.h $(INCLUDE_DIR)/prdb_query_abi_constants.h $(QUERY_ABI_GENERATED_HEADERS) | $(BUILD_DIR)
	$(TCC) -Wall -Wextra -Werror -I"$(INCLUDE_DIR)" "$<" -o "$@"

$(SDK_OPTIMIZER_TEST): engine/tests/fixtures/sdk_optimizer_v1.c $(SDK_ARCHIVE) $(INCLUDE_DIR)/prdb_sdk_optimizer_v1.h $(INCLUDE_DIR)/prdb_sdk_sql_v2.h | $(BUILD_DIR)
	$(TCC) -Wall -Wextra -Werror -I"$(INCLUDE_DIR)" "$<" "$(SDK_ARCHIVE)" -o "$@"

$(SDK_OPTIMIZER_PLAN_TEST): engine/tests/fixtures/sdk_optimizer_plan_v1.c $(SDK_ARCHIVE) $(INCLUDE_DIR)/prdb_sdk_optimizer_plan_v1.h | $(BUILD_DIR)
	$(TCC) -Wall -Wextra -Werror -I"$(INCLUDE_DIR)" "$<" "$(SDK_ARCHIVE)" -o "$@"

# TinyCC 0.9.27's internal shared linker omits PT_GNU_STACK, so glibc treats
# its output as requiring an executable stack and rejects it in the worker.
# Compile/archive with TinyCC, then use the system linker with noexecstack.
# These untrusted artifacts are loaded only through the Rust sandbox.
$(SQL_V3_CAUGHT_ERROR_OBJECT): engine/tests/fixtures/sql_v3_caught_error.c $(INCLUDE_DIR)/prdb_query_abi_v3.h $(INCLUDE_DIR)/prdb_query_abi_constants.h $(QUERY_ABI_GENERATED_HEADERS) $(INCLUDE_DIR)/prdb_sdk_sql_exec_v2.h | $(BUILD_DIR)
	$(TCC) -Wall -Wextra -Werror -fPIC -I"$(INCLUDE_DIR)" -c "$<" -o "$@"

$(SQL_V3_CAUGHT_ERROR_QUERY): $(SQL_V3_CAUGHT_ERROR_OBJECT) $(SDK_ARCHIVE) | $(BUILD_DIR)
	env -u NIX_LDFLAGS NIX_DONT_SET_RPATH=1 $(SHARED_LINKER) -shared -Wl,--no-eh-frame-hdr,-z,noexecstack "$<" "$(SDK_ARCHIVE)" -o "$@"

# This is an untrusted query artifact; run it only with the sandbox acceptance test.
$(SQL_V3_WRITE_READ_OBJECT): engine/tests/fixtures/sql_v3_write_read.c $(INCLUDE_DIR)/prdb_query_abi_v3.h $(INCLUDE_DIR)/prdb_query_abi_v4.h $(INCLUDE_DIR)/prdb_query_abi_constants.h $(QUERY_ABI_GENERATED_HEADERS) $(INCLUDE_DIR)/prdb_sdk_sql_exec_v2.h $(INCLUDE_DIR)/prdb_sdk_sql_exec_v5.h $(INCLUDE_DIR)/prdb_sdk_sql_exec_v6.h $(INCLUDE_DIR)/prdb_sdk_sql_exec_v7.h $(INCLUDE_DIR)/prdb_sdk_sql_exec_v8.h $(INCLUDE_DIR)/prdb_sdk_sql_exec_v10.h $(INCLUDE_DIR)/prdb_sdk_sql_exec_v11.h $(INCLUDE_DIR)/prdb_sdk_sql_exec_v12.h $(INCLUDE_DIR)/prdb_sdk_sql_exec_v13.h $(INCLUDE_DIR)/prdb_sdk_sql_exec_v15.h $(INCLUDE_DIR)/prdb_sdk_sql_exec_v16.h $(INCLUDE_DIR)/prdb_sdk_sql_exec_v17.h $(INCLUDE_DIR)/prdb_sdk_sql_exec_v18.h $(INCLUDE_DIR)/prdb_sdk_sql_v10.h $(INCLUDE_DIR)/prdb_sdk_sql_v11.h $(INCLUDE_DIR)/prdb_sdk_sql_v12.h $(INCLUDE_DIR)/prdb_sdk_sql_v13.h $(INCLUDE_DIR)/prdb_sdk_sql_v15.h $(INCLUDE_DIR)/prdb_sdk_sql_v17.h $(INCLUDE_DIR)/prdb_sdk_sql_v18.h $(INCLUDE_DIR)/prdb_sdk_sql_result_v1.h $(INCLUDE_DIR)/prdb_sdk_optimizer_plan_v1.h | $(BUILD_DIR)
	$(TCC) -Wall -Wextra -Werror -fPIC -I"$(INCLUDE_DIR)" -c "$<" -o "$@"

$(SQL_V3_WRITE_READ_QUERY): $(SQL_V3_WRITE_READ_OBJECT) $(SDK_ARCHIVE) | $(BUILD_DIR)
	env -u NIX_LDFLAGS NIX_DONT_SET_RPATH=1 $(SHARED_LINKER) -shared -Wl,--no-eh-frame-hdr,-z,noexecstack "$<" "$(SDK_ARCHIVE)" -o "$@"

# This is an untrusted query artifact; run it only with the sandbox acceptance test.
$(SQL_V3_RESTRICTED_METADATA_OBJECT): engine/tests/fixtures/sql_v3_restricted_metadata.c $(INCLUDE_DIR)/prdb_query_abi_v3.h $(INCLUDE_DIR)/prdb_query_abi_constants.h $(QUERY_ABI_GENERATED_HEADERS) $(INCLUDE_DIR)/prdb_sdk_sql_exec_v2.h | $(BUILD_DIR)
	$(TCC) -Wall -Wextra -Werror -fPIC -I"$(INCLUDE_DIR)" -c "$<" -o "$@"

$(SQL_V3_RESTRICTED_METADATA_QUERY): $(SQL_V3_RESTRICTED_METADATA_OBJECT) $(SDK_ARCHIVE) | $(BUILD_DIR)
	env -u NIX_LDFLAGS NIX_DONT_SET_RPATH=1 $(SHARED_LINKER) -shared -Wl,--no-eh-frame-hdr,-z,noexecstack "$<" "$(SDK_ARCHIVE)" -o "$@"

.PHONY: sandbox-query-fixture
sandbox-query-fixture: $(SQL_V3_CAUGHT_ERROR_QUERY) $(SQL_V3_WRITE_READ_QUERY) $(SQL_V3_RESTRICTED_METADATA_QUERY)

.PHONY: release-profile
release-profile: sandbox-query-fixture
	test "$$(getconf GNU_LIBC_VERSION)" = "glibc 2.42"
	NIX_DONT_SET_RPATH=1 cargo build --workspace --release --locked
	mkdir -p "$(RELEASE_PROFILE_DIR)/bin" "$(RELEASE_PROFILE_DIR)/sdk/include" "$(RELEASE_PROFILE_DIR)/queries"
	for binary in $(RELEASE_BINARIES); do cp "target/release/$$binary" "$(RELEASE_PROFILE_DIR)/bin/$$binary"; patchelf --remove-rpath "$(RELEASE_PROFILE_DIR)/bin/$$binary"; done
	cp "$(SDK_ARCHIVE)" "$(RELEASE_PROFILE_DIR)/sdk/libprdb_sdk.a"
	cp -R "$(INCLUDE_DIR)/." "$(RELEASE_PROFILE_DIR)/sdk/include/"
	cp "$(SQL_V3_CAUGHT_ERROR_QUERY)" "$(SQL_V3_WRITE_READ_QUERY)" "$(SQL_V3_RESTRICTED_METADATA_QUERY)" "$(RELEASE_PROFILE_DIR)/queries/"
	$(PYTHON) engine/tools/validate_release_profile.py "$(RELEASE_PROFILE_DIR)"

.PHONY: release-profile-package
release-profile-package: release-profile
	$(TAR) --sort=name --mtime='@0' --owner=0 --group=0 --numeric-owner -C "$(RELEASE_PROFILE_DIR)" -cf "$(RELEASE_PROFILE_ARCHIVE).tmp" .
	$(GZIP) -n -c "$(RELEASE_PROFILE_ARCHIVE).tmp" > "$(RELEASE_PROFILE_ARCHIVE)"
	rm "$(RELEASE_PROFILE_ARCHIVE).tmp"
	sha256sum "$(RELEASE_PROFILE_ARCHIVE)" > "$(RELEASE_PROFILE_ARCHIVE).sha256"

abi-constants:
	$(PYTHON) engine/tools/generate_query_abi.py

abi-constants-check:
	$(PYTHON) engine/tools/generate_query_abi.py --check

abi-constants-test:
	PYTHONDONTWRITEBYTECODE=1 $(PYTHON) engine/tools/test_generate_query_abi.py

.PHONY: abi-constants abi-constants-check abi-constants-test

test: $(SDK_OPTIMIZER_TEST) $(SDK_OPTIMIZER_PLAN_TEST)

test: abi-constants-check abi-constants-test $(QUERY_ABI_V4_TEST) $(SDK_VALUE_TEST) $(SDK_BATCH_TEST) $(SDK_SQL_TEST) $(SDK_SQL_EXPRESSION_TEST) $(SDK_SQL_V2_TEST) $(SDK_SQL_ORDER_TEST) $(SDK_SQL_GROUP_TEST) $(SDK_SQL_JOIN_TEST) $(SDK_SQL_JOIN_V2_TEST) $(SDK_SQL_V3_TEST) $(SDK_SQL_V4_TEST) $(SDK_SQL_V5_TEST) $(SDK_SQL_V6_TEST) $(SDK_SQL_V7_TEST) $(SDK_SQL_V8_TEST) $(SDK_SQL_V9_TEST) $(SDK_SQL_V10_TEST) $(SDK_SQL_V11_TEST) $(SDK_SQL_V12_TEST) $(SDK_SQL_V13_TEST) $(SDK_SQL_V14_TEST) $(SDK_SQL_V15_TEST) $(SDK_SQL_V16_TEST) $(SDK_SQL_V17_TEST) $(SDK_SQL_V18_TEST) $(SDK_PLANNER_TEST) $(SDK_EXECUTOR_TEST) $(SDK_SQL_EXEC_TEST) $(SDK_SQL_EXEC_V5_TEST) $(SDK_SQL_EXEC_V6_TEST) $(SDK_SQL_EXEC_V7_TEST) $(SDK_SQL_EXEC_V8_TEST) $(SDK_SQL_EXEC_V9_TEST) $(SDK_SQL_EXEC_V10_TEST) $(SDK_SQL_INFORMATION_SCHEMA_TEST) $(SDK_SQL_RESULT_TEST) $(SDK_SQL_RESULT_V13_TEST) $(SDK_SQL_DML_TEST) $(SDK_MANIFEST_TEST) $(SDK_SCOPE_TEST) $(SDK_IPC_FRAME_TEST) $(SDK_IPC_PAYLOAD_CODEC_TEST)
	"$(SDK_OPTIMIZER_TEST)"
	"$(SDK_OPTIMIZER_PLAN_TEST)"
	"$(QUERY_ABI_V4_TEST)"
	"$(SDK_VALUE_TEST)"
	"$(SDK_BATCH_TEST)"
	"$(SDK_SQL_TEST)"
	"$(SDK_SQL_EXPRESSION_TEST)"
	"$(SDK_SQL_V2_TEST)"
	"$(SDK_SQL_ORDER_TEST)"
	"$(SDK_SQL_GROUP_TEST)"
	"$(SDK_SQL_JOIN_TEST)"
	"$(SDK_SQL_JOIN_V2_TEST)"
	"$(SDK_SQL_V3_TEST)"
	"$(SDK_SQL_V4_TEST)"
	"$(SDK_SQL_V5_TEST)"
	"$(SDK_SQL_V6_TEST)"
	"$(SDK_SQL_V7_TEST)"
	"$(SDK_SQL_V8_TEST)"
	"$(SDK_SQL_V9_TEST)"
	"$(SDK_SQL_V10_TEST)"
	"$(SDK_SQL_V11_TEST)"
	"$(SDK_SQL_V12_TEST)"
	"$(SDK_SQL_V13_TEST)"
	"$(SDK_SQL_V14_TEST)"
	"$(SDK_SQL_V15_TEST)"
	"$(SDK_SQL_V16_TEST)"
	"$(SDK_SQL_V17_TEST)"
	"$(SDK_SQL_V18_TEST)"
	"$(SDK_PLANNER_TEST)"
	"$(SDK_EXECUTOR_TEST)"
	"$(SDK_SQL_EXEC_TEST)"
	"$(SDK_SQL_EXEC_V5_TEST)"
	"$(SDK_SQL_EXEC_V6_TEST)"
	"$(SDK_SQL_EXEC_V7_TEST)"
	"$(SDK_SQL_EXEC_V8_TEST)"
	"$(SDK_SQL_EXEC_V9_TEST)"
	"$(SDK_SQL_EXEC_V10_TEST)"
	"$(SDK_SQL_INFORMATION_SCHEMA_TEST)"
	"$(SDK_SQL_RESULT_TEST)"
	"$(SDK_SQL_RESULT_V13_TEST)"
	"$(SDK_SQL_DML_TEST)"
	"$(SDK_MANIFEST_TEST)"
	"$(SDK_SCOPE_TEST)"
	"$(SDK_IPC_FRAME_TEST)"
	"$(SDK_IPC_PAYLOAD_CODEC_TEST)"
