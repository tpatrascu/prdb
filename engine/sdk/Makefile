TCC ?= tcc
TCC_AR ?= $(TCC) -ar
BUILD_DIR ?= target/sdk-v1
INCLUDE_DIR ?= engine/include
SDK_VALUE_SOURCE := engine/sdk/value_v1.c
SDK_BATCH_SOURCE := engine/sdk/batch_v1.c
SDK_OWNED_BATCH_SOURCE := engine/sdk/owned_batch_v1.c
SDK_SQL_SOURCE := engine/sdk/sql_v1.c
SDK_SQL_ORDER_SOURCE := engine/sdk/sql_order_v1.c
SDK_SQL_GROUP_SOURCE := engine/sdk/sql_group_v1.c
SDK_PLANNER_SOURCE := engine/sdk/planner_v1.c
SDK_EXECUTOR_SOURCE := engine/sdk/executor_v1.c
SDK_SQL_EXEC_SOURCE := engine/sdk/sql_exec_v2.c
SDK_SQL_EXEC_V4_SOURCE := engine/sdk/sql_exec_v4.c
SDK_SQL_EXEC_V5_SOURCE := engine/sdk/sql_exec_v5.c
SDK_SQL_EXEC_V6_SOURCE := engine/sdk/sql_exec_v6.c
SDK_SQL_RESULT_SOURCE := engine/sdk/sql_result_v1.c
SDK_MANIFEST_SOURCE := engine/sdk/manifest_v1.c
SDK_SCOPE_SOURCE := engine/sdk/scope_v1.c
SDK_VALUE_OBJECT := $(BUILD_DIR)/value_v1.o
SDK_BATCH_OBJECT := $(BUILD_DIR)/batch_v1.o
SDK_OWNED_BATCH_OBJECT := $(BUILD_DIR)/owned_batch_v1.o
SDK_SQL_OBJECT := $(BUILD_DIR)/sql_v1.o
SDK_SQL_ORDER_OBJECT := $(BUILD_DIR)/sql_order_v1.o
SDK_SQL_GROUP_OBJECT := $(BUILD_DIR)/sql_group_v1.o
SDK_PLANNER_OBJECT := $(BUILD_DIR)/planner_v1.o
SDK_EXECUTOR_OBJECT := $(BUILD_DIR)/executor_v1.o
SDK_SQL_EXEC_OBJECT := $(BUILD_DIR)/sql_exec_v2.o
SDK_SQL_EXEC_V4_OBJECT := $(BUILD_DIR)/sql_exec_v4.o
SDK_SQL_EXEC_V5_OBJECT := $(BUILD_DIR)/sql_exec_v5.o
SDK_SQL_EXEC_V6_OBJECT := $(BUILD_DIR)/sql_exec_v6.o
SDK_SQL_RESULT_OBJECT := $(BUILD_DIR)/sql_result_v1.o
SDK_MANIFEST_OBJECT := $(BUILD_DIR)/manifest_v1.o
SDK_SCOPE_OBJECT := $(BUILD_DIR)/scope_v1.o
SDK_OBJECTS := $(SDK_VALUE_OBJECT) $(SDK_BATCH_OBJECT) $(SDK_OWNED_BATCH_OBJECT) $(SDK_SQL_OBJECT) $(SDK_SQL_ORDER_OBJECT) $(SDK_SQL_GROUP_OBJECT) $(SDK_PLANNER_OBJECT) $(SDK_EXECUTOR_OBJECT) $(SDK_SQL_EXEC_OBJECT) $(SDK_SQL_EXEC_V4_OBJECT) $(SDK_SQL_EXEC_V5_OBJECT) $(SDK_SQL_EXEC_V6_OBJECT) $(SDK_SQL_RESULT_OBJECT) $(SDK_MANIFEST_OBJECT) $(SDK_SCOPE_OBJECT)
SDK_ARCHIVE := $(BUILD_DIR)/libprdb_sdk.a
SDK_VALUE_TEST := $(BUILD_DIR)/sdk-value-v1
SDK_BATCH_TEST := $(BUILD_DIR)/sdk-batch-v1
SDK_SQL_TEST := $(BUILD_DIR)/sdk-sql-select-v1
SDK_SQL_EXPRESSION_TEST := $(BUILD_DIR)/sdk-sql-expression-v1
SDK_SQL_V2_TEST := $(BUILD_DIR)/sdk-sql-select-v2
SDK_SQL_ORDER_TEST := $(BUILD_DIR)/sdk-sql-order-v1
SDK_SQL_GROUP_TEST := $(BUILD_DIR)/sdk-sql-group-v1
SDK_SQL_V3_TEST := $(BUILD_DIR)/sdk-sql-select-v3
SDK_SQL_V4_TEST := $(BUILD_DIR)/sdk-sql-select-v4
SDK_SQL_V5_TEST := $(BUILD_DIR)/sdk-sql-select-v5
SDK_SQL_V6_TEST := $(BUILD_DIR)/sdk-sql-select-v6
SDK_SQL_V7_TEST := $(BUILD_DIR)/sdk-sql-select-v7
SDK_PLANNER_TEST := $(BUILD_DIR)/sdk-planner-v1
SDK_EXECUTOR_TEST := $(BUILD_DIR)/sdk-executor-v1
SDK_SQL_EXEC_TEST := $(BUILD_DIR)/sdk-sql-execute-v2
SDK_SQL_EXEC_V5_TEST := $(BUILD_DIR)/sdk-sql-execute-v5
SDK_SQL_EXEC_V6_TEST := $(BUILD_DIR)/sdk-sql-execute-v6
SDK_SQL_INFORMATION_SCHEMA_TEST := $(BUILD_DIR)/sdk-sql-information-schema-v2
SDK_SQL_RESULT_TEST := $(BUILD_DIR)/sdk-sql-result-v1
SDK_SQL_DML_TEST := $(BUILD_DIR)/sdk-sql-dml-v1
SDK_MANIFEST_TEST := $(BUILD_DIR)/sdk-manifest-v1
SDK_SCOPE_TEST := $(BUILD_DIR)/sdk-scope-v1
SQL_V3_CAUGHT_ERROR_QUERY := $(BUILD_DIR)/sql-v3-caught-error.so
SQL_V3_WRITE_READ_QUERY := $(BUILD_DIR)/sql-v3-write-read.so
SQL_V3_RESTRICTED_METADATA_QUERY := $(BUILD_DIR)/sql-v3-restricted-metadata.so
SQL_V3_CAUGHT_ERROR_OBJECT := $(BUILD_DIR)/sql-v3-caught-error.o
SQL_V3_WRITE_READ_OBJECT := $(BUILD_DIR)/sql-v3-write-read.o
SQL_V3_RESTRICTED_METADATA_OBJECT := $(BUILD_DIR)/sql-v3-restricted-metadata.o

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

$(SDK_SQL_OBJECT): $(SDK_SQL_SOURCE) $(INCLUDE_DIR)/prdb_sdk_sql_v1.h $(INCLUDE_DIR)/prdb_sdk_sql_v2.h $(INCLUDE_DIR)/prdb_sdk_sql_v3.h $(INCLUDE_DIR)/prdb_sdk_sql_v4.h $(INCLUDE_DIR)/prdb_sdk_sql_v5.h $(INCLUDE_DIR)/prdb_sdk_sql_v6.h $(INCLUDE_DIR)/prdb_sdk_sql_v7.h $(INCLUDE_DIR)/prdb_sdk_sql_dml_v1.h $(INCLUDE_DIR)/prdb_sdk_batch_v1.h | $(BUILD_DIR)
	$(TCC) -Wall -Wextra -Werror -I"$(INCLUDE_DIR)" -c "$<" -o "$@"

$(SDK_SQL_ORDER_OBJECT): $(SDK_SQL_ORDER_SOURCE) $(INCLUDE_DIR)/prdb_sdk_sql_order_v1.h $(INCLUDE_DIR)/prdb_sdk_sql_v1.h $(INCLUDE_DIR)/prdb_sdk_value_v1.h | $(BUILD_DIR)
	$(TCC) -Wall -Wextra -Werror -I"$(INCLUDE_DIR)" -c "$<" -o "$@"

$(SDK_SQL_GROUP_OBJECT): $(SDK_SQL_GROUP_SOURCE) $(INCLUDE_DIR)/prdb_sdk_sql_group_v1.h $(INCLUDE_DIR)/prdb_sdk_sql_v1.h $(INCLUDE_DIR)/prdb_sdk_value_v1.h | $(BUILD_DIR)
	$(TCC) -Wall -Wextra -Werror -I"$(INCLUDE_DIR)" -c "$<" -o "$@"

$(SDK_PLANNER_OBJECT): $(SDK_PLANNER_SOURCE) $(INCLUDE_DIR)/prdb_sdk_planner_v1.h $(INCLUDE_DIR)/prdb_sdk_sql_v2.h $(INCLUDE_DIR)/prdb_sdk_batch_v1.h | $(BUILD_DIR)
	$(TCC) -Wall -Wextra -Werror -I"$(INCLUDE_DIR)" -c "$<" -o "$@"

$(SDK_EXECUTOR_OBJECT): $(SDK_EXECUTOR_SOURCE) $(INCLUDE_DIR)/prdb_sdk_executor_v1.h $(INCLUDE_DIR)/prdb_query_abi_v2.h $(INCLUDE_DIR)/prdb_sdk_planner_v1.h $(INCLUDE_DIR)/prdb_sdk_batch_v1.h | $(BUILD_DIR)
	$(TCC) -Wall -Wextra -Werror -I"$(INCLUDE_DIR)" -c "$<" -o "$@"

$(SDK_SQL_EXEC_OBJECT): $(SDK_SQL_EXEC_SOURCE) $(INCLUDE_DIR)/prdb_sdk_sql_exec_v2.h $(INCLUDE_DIR)/prdb_sdk_sql_exec_v3.h $(INCLUDE_DIR)/prdb_sdk_sql_dml_v1.h $(INCLUDE_DIR)/prdb_query_abi_v3.h $(INCLUDE_DIR)/prdb_sdk_executor_v1.h $(INCLUDE_DIR)/prdb_sdk_planner_v1.h $(INCLUDE_DIR)/prdb_sdk_sql_v2.h $(INCLUDE_DIR)/prdb_sdk_sql_v3.h $(INCLUDE_DIR)/prdb_sdk_sql_order_v1.h | $(BUILD_DIR)
	$(TCC) -Wall -Wextra -Werror -I"$(INCLUDE_DIR)" -c "$<" -o "$@"

$(SDK_SQL_EXEC_V4_OBJECT): $(SDK_SQL_EXEC_V4_SOURCE) $(INCLUDE_DIR)/prdb_sdk_sql_exec_v4.h $(INCLUDE_DIR)/prdb_sdk_sql_v4.h $(INCLUDE_DIR)/prdb_sdk_sql_group_v1.h | $(BUILD_DIR)
	$(TCC) -Wall -Wextra -Werror -I"$(INCLUDE_DIR)" -c "$<" -o "$@"

$(SDK_SQL_EXEC_V5_OBJECT): $(SDK_SQL_EXEC_V5_SOURCE) $(INCLUDE_DIR)/prdb_sdk_sql_exec_v5.h $(INCLUDE_DIR)/prdb_sdk_sql_v5.h $(INCLUDE_DIR)/prdb_sdk_sql_group_v1.h | $(BUILD_DIR)
	$(TCC) -Wall -Wextra -Werror -I"$(INCLUDE_DIR)" -c "$<" -o "$@"

$(SDK_SQL_EXEC_V6_OBJECT): $(SDK_SQL_EXEC_V6_SOURCE) $(INCLUDE_DIR)/prdb_sdk_sql_exec_v6.h $(INCLUDE_DIR)/prdb_sdk_sql_v6.h $(INCLUDE_DIR)/prdb_sdk_sql_group_v1.h | $(BUILD_DIR)
	$(TCC) -Wall -Wextra -Werror -I"$(INCLUDE_DIR)" -c "$<" -o "$@"

$(SDK_SQL_RESULT_OBJECT): $(SDK_SQL_RESULT_SOURCE) $(INCLUDE_DIR)/prdb_sdk_sql_result_v1.h $(INCLUDE_DIR)/prdb_sdk_sql_v2.h $(INCLUDE_DIR)/prdb_sdk_sql_v5.h $(INCLUDE_DIR)/prdb_sdk_sql_v6.h $(INCLUDE_DIR)/prdb_sdk_batch_v1.h $(INCLUDE_DIR)/prdb_sdk_value_v1.h | $(BUILD_DIR)
	$(TCC) -Wall -Wextra -Werror -I"$(INCLUDE_DIR)" -c "$<" -o "$@"

$(SDK_MANIFEST_OBJECT): $(SDK_MANIFEST_SOURCE) $(INCLUDE_DIR)/prdb_sdk_manifest_v1.h | $(BUILD_DIR)
	$(TCC) -Wall -Wextra -Werror -I"$(INCLUDE_DIR)" -c "$<" -o "$@"

$(SDK_SCOPE_OBJECT): $(SDK_SCOPE_SOURCE) $(INCLUDE_DIR)/prdb_sdk_scope_v1.h | $(BUILD_DIR)
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

$(SDK_SQL_ORDER_TEST): engine/tests/fixtures/sdk_sql_order_v1.c $(SDK_ARCHIVE) $(INCLUDE_DIR)/prdb_sdk_sql_order_v1.h | $(BUILD_DIR)
	$(TCC) -Wall -Wextra -Werror -I"$(INCLUDE_DIR)" "$<" "$(SDK_ARCHIVE)" -o "$@"

$(SDK_SQL_GROUP_TEST): engine/tests/fixtures/sdk_sql_group_v1.c $(SDK_ARCHIVE) $(INCLUDE_DIR)/prdb_sdk_sql_group_v1.h | $(BUILD_DIR)
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

$(SDK_PLANNER_TEST): engine/tests/fixtures/sdk_planner_v1.c $(SDK_ARCHIVE) $(INCLUDE_DIR)/prdb_sdk_planner_v1.h $(INCLUDE_DIR)/prdb_sdk_sql_v2.h $(INCLUDE_DIR)/prdb_sdk_batch_v1.h | $(BUILD_DIR)
	$(TCC) -Wall -Wextra -Werror -I"$(INCLUDE_DIR)" "$<" "$(SDK_ARCHIVE)" -o "$@"

$(SDK_EXECUTOR_TEST): engine/tests/fixtures/sdk_executor_v1.c $(SDK_ARCHIVE) $(INCLUDE_DIR)/prdb_sdk_executor_v1.h $(INCLUDE_DIR)/prdb_query_abi_v2.h | $(BUILD_DIR)
	$(TCC) -Wall -Wextra -Werror -I"$(INCLUDE_DIR)" "$<" "$(SDK_ARCHIVE)" -o "$@"

$(SDK_SQL_EXEC_TEST): engine/tests/fixtures/sdk_sql_execute_v2.c $(SDK_ARCHIVE) $(INCLUDE_DIR)/prdb_sdk_sql_exec_v2.h $(INCLUDE_DIR)/prdb_sdk_sql_exec_v3.h $(INCLUDE_DIR)/prdb_sdk_sql_exec_v4.h $(INCLUDE_DIR)/prdb_sdk_sql_v3.h $(INCLUDE_DIR)/prdb_sdk_sql_result_v1.h $(INCLUDE_DIR)/prdb_sdk_executor_v1.h $(INCLUDE_DIR)/prdb_sdk_planner_v1.h | $(BUILD_DIR)
	$(TCC) -Wall -Wextra -Werror -I"$(INCLUDE_DIR)" "$<" "$(SDK_ARCHIVE)" -o "$@"

$(SDK_SQL_EXEC_V5_TEST): engine/tests/fixtures/sdk_sql_execute_v5.c $(SDK_ARCHIVE) $(INCLUDE_DIR)/prdb_sdk_sql_exec_v5.h $(INCLUDE_DIR)/prdb_sdk_sql_v5.h | $(BUILD_DIR)
	$(TCC) -Wall -Wextra -Werror -I"$(INCLUDE_DIR)" "$<" "$(SDK_ARCHIVE)" -o "$@"

$(SDK_SQL_EXEC_V6_TEST): engine/tests/fixtures/sdk_sql_execute_v6.c $(SDK_ARCHIVE) $(INCLUDE_DIR)/prdb_sdk_sql_exec_v6.h $(INCLUDE_DIR)/prdb_sdk_sql_v6.h | $(BUILD_DIR)
	$(TCC) -Wall -Wextra -Werror -I"$(INCLUDE_DIR)" "$<" "$(SDK_ARCHIVE)" -o "$@"

$(SDK_SQL_INFORMATION_SCHEMA_TEST): engine/tests/fixtures/sdk_sql_information_schema_v2.c $(SDK_ARCHIVE) $(INCLUDE_DIR)/prdb_sdk_sql_exec_v2.h $(INCLUDE_DIR)/prdb_sdk_executor_v1.h $(INCLUDE_DIR)/prdb_sdk_planner_v1.h | $(BUILD_DIR)
	$(TCC) -Wall -Wextra -Werror -I"$(INCLUDE_DIR)" "$<" "$(SDK_ARCHIVE)" -o "$@"

$(SDK_SQL_RESULT_TEST): engine/tests/fixtures/sdk_sql_result_v1.c $(SDK_ARCHIVE) $(INCLUDE_DIR)/prdb_sdk_sql_result_v1.h $(INCLUDE_DIR)/prdb_sdk_sql_v2.h $(INCLUDE_DIR)/prdb_sdk_sql_v5.h $(INCLUDE_DIR)/prdb_sdk_batch_v1.h | $(BUILD_DIR)
	$(TCC) -Wall -Wextra -Werror -I"$(INCLUDE_DIR)" "$<" "$(SDK_ARCHIVE)" -o "$@"

$(SDK_SQL_DML_TEST): engine/tests/fixtures/sdk_sql_dml_v1.c $(SDK_ARCHIVE) $(INCLUDE_DIR)/prdb_sdk_sql_dml_v1.h | $(BUILD_DIR)
	$(TCC) -Wall -Wextra -Werror -I"$(INCLUDE_DIR)" "$<" "$(SDK_ARCHIVE)" -o "$@"

$(SDK_MANIFEST_TEST): engine/tests/fixtures/sdk_manifest_v1.c $(SDK_ARCHIVE) $(INCLUDE_DIR)/prdb_sdk_manifest_v1.h | $(BUILD_DIR)
	$(TCC) -Wall -Wextra -Werror -I"$(INCLUDE_DIR)" "$<" "$(SDK_ARCHIVE)" -o "$@"

$(SDK_SCOPE_TEST): engine/tests/fixtures/sdk_scope_v1.c $(SDK_ARCHIVE) $(INCLUDE_DIR)/prdb_sdk_scope_v1.h | $(BUILD_DIR)
	$(TCC) -Wall -Wextra -Werror -I"$(INCLUDE_DIR)" "$<" "$(SDK_ARCHIVE)" -o "$@"

# This is an untrusted query artifact. Build it for the Rust sandbox launcher;
# never execute it with tcc -run or load it from a trusted test process.
$(SQL_V3_CAUGHT_ERROR_OBJECT): engine/tests/fixtures/sql_v3_caught_error.c $(INCLUDE_DIR)/prdb_query_abi_v3.h $(INCLUDE_DIR)/prdb_sdk_sql_exec_v2.h | $(BUILD_DIR)
	$(TCC) -Wall -Wextra -Werror -fPIC -I"$(INCLUDE_DIR)" -c "$<" -o "$@"

$(SQL_V3_CAUGHT_ERROR_QUERY): $(SQL_V3_CAUGHT_ERROR_OBJECT) $(SDK_ARCHIVE) | $(BUILD_DIR)
	env -u NIX_LDFLAGS NIX_DONT_SET_RPATH=1 $(CC) -shared -Wl,--no-eh-frame-hdr,-z,noexecstack "$<" "$(SDK_ARCHIVE)" -o "$@"

# This is an untrusted query artifact; run it only with the sandbox acceptance test.
$(SQL_V3_WRITE_READ_OBJECT): engine/tests/fixtures/sql_v3_write_read.c $(INCLUDE_DIR)/prdb_query_abi_v3.h $(INCLUDE_DIR)/prdb_sdk_sql_exec_v2.h $(INCLUDE_DIR)/prdb_sdk_sql_exec_v5.h $(INCLUDE_DIR)/prdb_sdk_sql_exec_v6.h $(INCLUDE_DIR)/prdb_sdk_sql_result_v1.h | $(BUILD_DIR)
	$(TCC) -Wall -Wextra -Werror -fPIC -I"$(INCLUDE_DIR)" -c "$<" -o "$@"

$(SQL_V3_WRITE_READ_QUERY): $(SQL_V3_WRITE_READ_OBJECT) $(SDK_ARCHIVE) | $(BUILD_DIR)
	env -u NIX_LDFLAGS NIX_DONT_SET_RPATH=1 $(CC) -shared -Wl,--no-eh-frame-hdr,-z,noexecstack "$<" "$(SDK_ARCHIVE)" -o "$@"

# This is an untrusted query artifact; run it only with the sandbox acceptance test.
$(SQL_V3_RESTRICTED_METADATA_OBJECT): engine/tests/fixtures/sql_v3_restricted_metadata.c $(INCLUDE_DIR)/prdb_query_abi_v3.h $(INCLUDE_DIR)/prdb_sdk_sql_exec_v2.h | $(BUILD_DIR)
	$(TCC) -Wall -Wextra -Werror -fPIC -I"$(INCLUDE_DIR)" -c "$<" -o "$@"

$(SQL_V3_RESTRICTED_METADATA_QUERY): $(SQL_V3_RESTRICTED_METADATA_OBJECT) $(SDK_ARCHIVE) | $(BUILD_DIR)
	env -u NIX_LDFLAGS NIX_DONT_SET_RPATH=1 $(CC) -shared -Wl,--no-eh-frame-hdr,-z,noexecstack "$<" "$(SDK_ARCHIVE)" -o "$@"

.PHONY: sandbox-query-fixture
sandbox-query-fixture: $(SQL_V3_CAUGHT_ERROR_QUERY) $(SQL_V3_WRITE_READ_QUERY) $(SQL_V3_RESTRICTED_METADATA_QUERY)

test: $(SDK_VALUE_TEST) $(SDK_BATCH_TEST) $(SDK_SQL_TEST) $(SDK_SQL_EXPRESSION_TEST) $(SDK_SQL_V2_TEST) $(SDK_SQL_ORDER_TEST) $(SDK_SQL_GROUP_TEST) $(SDK_SQL_V3_TEST) $(SDK_SQL_V4_TEST) $(SDK_SQL_V5_TEST) $(SDK_SQL_V6_TEST) $(SDK_SQL_V7_TEST) $(SDK_PLANNER_TEST) $(SDK_EXECUTOR_TEST) $(SDK_SQL_EXEC_TEST) $(SDK_SQL_EXEC_V5_TEST) $(SDK_SQL_EXEC_V6_TEST) $(SDK_SQL_INFORMATION_SCHEMA_TEST) $(SDK_SQL_RESULT_TEST) $(SDK_SQL_DML_TEST) $(SDK_MANIFEST_TEST) $(SDK_SCOPE_TEST)
	"$(SDK_VALUE_TEST)"
	"$(SDK_BATCH_TEST)"
	"$(SDK_SQL_TEST)"
	"$(SDK_SQL_EXPRESSION_TEST)"
	"$(SDK_SQL_V2_TEST)"
	"$(SDK_SQL_ORDER_TEST)"
	"$(SDK_SQL_GROUP_TEST)"
	"$(SDK_SQL_V3_TEST)"
	"$(SDK_SQL_V4_TEST)"
	"$(SDK_SQL_V5_TEST)"
	"$(SDK_SQL_V6_TEST)"
	"$(SDK_SQL_V7_TEST)"
	"$(SDK_PLANNER_TEST)"
	"$(SDK_EXECUTOR_TEST)"
	"$(SDK_SQL_EXEC_TEST)"
	"$(SDK_SQL_EXEC_V5_TEST)"
	"$(SDK_SQL_EXEC_V6_TEST)"
	"$(SDK_SQL_INFORMATION_SCHEMA_TEST)"
	"$(SDK_SQL_RESULT_TEST)"
	"$(SDK_SQL_DML_TEST)"
	"$(SDK_MANIFEST_TEST)"
	"$(SDK_SCOPE_TEST)"
