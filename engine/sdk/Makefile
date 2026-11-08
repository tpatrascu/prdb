TCC ?= tcc
TCC_AR ?= $(TCC) -ar
BUILD_DIR ?= target/sdk-v1
INCLUDE_DIR ?= engine/include
SDK_VALUE_SOURCE := engine/sdk/value_v1.c
SDK_BATCH_SOURCE := engine/sdk/batch_v1.c
SDK_SQL_SOURCE := engine/sdk/sql_v1.c
SDK_PLANNER_SOURCE := engine/sdk/planner_v1.c
SDK_EXECUTOR_SOURCE := engine/sdk/executor_v1.c
SDK_SQL_EXEC_SOURCE := engine/sdk/sql_exec_v2.c
SDK_VALUE_OBJECT := $(BUILD_DIR)/value_v1.o
SDK_BATCH_OBJECT := $(BUILD_DIR)/batch_v1.o
SDK_SQL_OBJECT := $(BUILD_DIR)/sql_v1.o
SDK_PLANNER_OBJECT := $(BUILD_DIR)/planner_v1.o
SDK_EXECUTOR_OBJECT := $(BUILD_DIR)/executor_v1.o
SDK_SQL_EXEC_OBJECT := $(BUILD_DIR)/sql_exec_v2.o
SDK_OBJECTS := $(SDK_VALUE_OBJECT) $(SDK_BATCH_OBJECT) $(SDK_SQL_OBJECT) $(SDK_PLANNER_OBJECT) $(SDK_EXECUTOR_OBJECT) $(SDK_SQL_EXEC_OBJECT)
SDK_ARCHIVE := $(BUILD_DIR)/libprdb_sdk.a
SDK_VALUE_TEST := $(BUILD_DIR)/sdk-value-v1
SDK_BATCH_TEST := $(BUILD_DIR)/sdk-batch-v1
SDK_SQL_TEST := $(BUILD_DIR)/sdk-sql-select-v1
SDK_SQL_EXPRESSION_TEST := $(BUILD_DIR)/sdk-sql-expression-v1
SDK_SQL_V2_TEST := $(BUILD_DIR)/sdk-sql-select-v2
SDK_PLANNER_TEST := $(BUILD_DIR)/sdk-planner-v1
SDK_EXECUTOR_TEST := $(BUILD_DIR)/sdk-executor-v1
SDK_SQL_EXEC_TEST := $(BUILD_DIR)/sdk-sql-execute-v2

.PHONY: all test

all: $(SDK_ARCHIVE)

$(BUILD_DIR):
	mkdir -p "$@"

$(SDK_VALUE_OBJECT): $(SDK_VALUE_SOURCE) $(INCLUDE_DIR)/prdb_sdk_value_v1.h | $(BUILD_DIR)
	$(TCC) -Wall -Wextra -Werror -I"$(INCLUDE_DIR)" -c "$<" -o "$@"

$(SDK_BATCH_OBJECT): $(SDK_BATCH_SOURCE) $(INCLUDE_DIR)/prdb_sdk_batch_v1.h $(INCLUDE_DIR)/prdb_sdk_value_v1.h | $(BUILD_DIR)
	$(TCC) -Wall -Wextra -Werror -I"$(INCLUDE_DIR)" -c "$<" -o "$@"

$(SDK_SQL_OBJECT): $(SDK_SQL_SOURCE) $(INCLUDE_DIR)/prdb_sdk_sql_v1.h $(INCLUDE_DIR)/prdb_sdk_sql_v2.h $(INCLUDE_DIR)/prdb_sdk_batch_v1.h | $(BUILD_DIR)
	$(TCC) -Wall -Wextra -Werror -I"$(INCLUDE_DIR)" -c "$<" -o "$@"

$(SDK_PLANNER_OBJECT): $(SDK_PLANNER_SOURCE) $(INCLUDE_DIR)/prdb_sdk_planner_v1.h $(INCLUDE_DIR)/prdb_sdk_sql_v2.h $(INCLUDE_DIR)/prdb_sdk_batch_v1.h | $(BUILD_DIR)
	$(TCC) -Wall -Wextra -Werror -I"$(INCLUDE_DIR)" -c "$<" -o "$@"

$(SDK_EXECUTOR_OBJECT): $(SDK_EXECUTOR_SOURCE) $(INCLUDE_DIR)/prdb_sdk_executor_v1.h $(INCLUDE_DIR)/prdb_query_abi_v2.h $(INCLUDE_DIR)/prdb_sdk_planner_v1.h $(INCLUDE_DIR)/prdb_sdk_batch_v1.h | $(BUILD_DIR)
	$(TCC) -Wall -Wextra -Werror -I"$(INCLUDE_DIR)" -c "$<" -o "$@"

$(SDK_SQL_EXEC_OBJECT): $(SDK_SQL_EXEC_SOURCE) $(INCLUDE_DIR)/prdb_sdk_sql_exec_v2.h $(INCLUDE_DIR)/prdb_query_abi_v3.h $(INCLUDE_DIR)/prdb_sdk_executor_v1.h $(INCLUDE_DIR)/prdb_sdk_planner_v1.h $(INCLUDE_DIR)/prdb_sdk_sql_v2.h | $(BUILD_DIR)
	$(TCC) -Wall -Wextra -Werror -I"$(INCLUDE_DIR)" -c "$<" -o "$@"

$(SDK_ARCHIVE): $(SDK_OBJECTS)
	$(TCC_AR) rcs "$@" $(SDK_OBJECTS)

$(SDK_VALUE_TEST): engine/tests/fixtures/sdk_value_v1.c $(SDK_ARCHIVE) | $(BUILD_DIR)
	$(TCC) -Wall -Wextra -Werror -I"$(INCLUDE_DIR)" "$<" "$(SDK_ARCHIVE)" -o "$@"

$(SDK_BATCH_TEST): engine/tests/fixtures/sdk_batch_v1.c $(SDK_ARCHIVE) | $(BUILD_DIR)
	$(TCC) -Wall -Wextra -Werror -I"$(INCLUDE_DIR)" "$<" "$(SDK_ARCHIVE)" -o "$@"

$(SDK_SQL_TEST): engine/tests/fixtures/sdk_sql_select_v1.c $(SDK_ARCHIVE) | $(BUILD_DIR)
	$(TCC) -Wall -Wextra -Werror -I"$(INCLUDE_DIR)" "$<" "$(SDK_ARCHIVE)" -o "$@"

$(SDK_SQL_EXPRESSION_TEST): engine/tests/fixtures/sdk_sql_expression_v1.c $(SDK_ARCHIVE) | $(BUILD_DIR)
	$(TCC) -Wall -Wextra -Werror -I"$(INCLUDE_DIR)" "$<" "$(SDK_ARCHIVE)" -o "$@"

$(SDK_SQL_V2_TEST): engine/tests/fixtures/sdk_sql_select_v2.c $(SDK_ARCHIVE) $(INCLUDE_DIR)/prdb_sdk_sql_v2.h $(INCLUDE_DIR)/prdb_sdk_batch_v1.h | $(BUILD_DIR)
	$(TCC) -Wall -Wextra -Werror -I"$(INCLUDE_DIR)" "$<" "$(SDK_ARCHIVE)" -o "$@"

$(SDK_PLANNER_TEST): engine/tests/fixtures/sdk_planner_v1.c $(SDK_ARCHIVE) $(INCLUDE_DIR)/prdb_sdk_planner_v1.h $(INCLUDE_DIR)/prdb_sdk_sql_v2.h $(INCLUDE_DIR)/prdb_sdk_batch_v1.h | $(BUILD_DIR)
	$(TCC) -Wall -Wextra -Werror -I"$(INCLUDE_DIR)" "$<" "$(SDK_ARCHIVE)" -o "$@"

$(SDK_EXECUTOR_TEST): engine/tests/fixtures/sdk_executor_v1.c $(SDK_ARCHIVE) $(INCLUDE_DIR)/prdb_sdk_executor_v1.h $(INCLUDE_DIR)/prdb_query_abi_v2.h | $(BUILD_DIR)
	$(TCC) -Wall -Wextra -Werror -I"$(INCLUDE_DIR)" "$<" "$(SDK_ARCHIVE)" -o "$@"

$(SDK_SQL_EXEC_TEST): engine/tests/fixtures/sdk_sql_execute_v2.c $(SDK_ARCHIVE) $(INCLUDE_DIR)/prdb_sdk_sql_exec_v2.h $(INCLUDE_DIR)/prdb_sdk_executor_v1.h $(INCLUDE_DIR)/prdb_sdk_planner_v1.h | $(BUILD_DIR)
	$(TCC) -Wall -Wextra -Werror -I"$(INCLUDE_DIR)" "$<" "$(SDK_ARCHIVE)" -o "$@"

test: $(SDK_VALUE_TEST) $(SDK_BATCH_TEST) $(SDK_SQL_TEST) $(SDK_SQL_EXPRESSION_TEST) $(SDK_SQL_V2_TEST) $(SDK_PLANNER_TEST) $(SDK_EXECUTOR_TEST) $(SDK_SQL_EXEC_TEST)
	"$(SDK_VALUE_TEST)"
	"$(SDK_BATCH_TEST)"
	"$(SDK_SQL_TEST)"
	"$(SDK_SQL_EXPRESSION_TEST)"
	"$(SDK_SQL_V2_TEST)"
	"$(SDK_PLANNER_TEST)"
	"$(SDK_EXECUTOR_TEST)"
	"$(SDK_SQL_EXEC_TEST)"
