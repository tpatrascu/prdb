TCC ?= tcc
TCC_AR ?= $(TCC) -ar
BUILD_DIR ?= target/sdk-v1
INCLUDE_DIR ?= engine/include
SDK_SOURCE := engine/sdk/value_v1.c
SDK_OBJECT := $(BUILD_DIR)/value_v1.o
SDK_ARCHIVE := $(BUILD_DIR)/libprdb_sdk.a
SDK_TEST := $(BUILD_DIR)/sdk-value-v1

.PHONY: all test

all: $(SDK_ARCHIVE)

$(BUILD_DIR):
	mkdir -p "$@"

$(SDK_OBJECT): $(SDK_SOURCE) $(INCLUDE_DIR)/prdb_sdk_value_v1.h | $(BUILD_DIR)
	$(TCC) -Wall -Wextra -Werror -I"$(INCLUDE_DIR)" -c "$<" -o "$@"

$(SDK_ARCHIVE): $(SDK_OBJECT)
	$(TCC_AR) rcs "$@" "$<"

$(SDK_TEST): engine/tests/fixtures/sdk_value_v1.c $(SDK_ARCHIVE) | $(BUILD_DIR)
	$(TCC) -Wall -Wextra -Werror -I"$(INCLUDE_DIR)" "$<" "$(SDK_ARCHIVE)" -o "$@"

test: $(SDK_TEST)
	"$(SDK_TEST)"
