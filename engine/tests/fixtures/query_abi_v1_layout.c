#include "prdb_query_abi_v1.h"

#include <stddef.h>

_Static_assert(sizeof(prdb_host_api_v1) == 8, "host ABI size");
_Static_assert(_Alignof(prdb_host_api_v1) == 4, "host ABI alignment");
_Static_assert(sizeof(prdb_invocation_v1) == 24, "invocation ABI size");
_Static_assert(_Alignof(prdb_invocation_v1) == 8, "invocation ABI alignment");
_Static_assert(offsetof(prdb_invocation_v1, parameters) == 8, "parameters offset");
_Static_assert(offsetof(prdb_invocation_v1, parameters_len) == 16, "length offset");
_Static_assert(sizeof(prdb_query_descriptor_v1_record) == 16, "descriptor ABI size");
_Static_assert(_Alignof(prdb_query_descriptor_v1_record) == 4, "descriptor ABI alignment");

prdb_query_main_v1_fn prdb_query_abi_v1_layout_probe;
