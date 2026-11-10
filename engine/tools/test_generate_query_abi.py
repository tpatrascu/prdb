#!/usr/bin/env python3
"""Regression tests for shared IPC payload schema validation."""

import copy
import importlib.util
import json
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
GENERATOR_PATH = ROOT / "tools" / "generate_query_abi.py"
SCHEMA_PATH = ROOT / "abi" / "query_abi_constants.json"
SPEC = importlib.util.spec_from_file_location("query_abi_generator", GENERATOR_PATH)
GENERATOR = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(GENERATOR)


class PayloadSchemaValidationTests(unittest.TestCase):
    def setUp(self):
        self.schema = json.loads(SCHEMA_PATH.read_text())

    def test_current_schema_renders(self):
        rust, c, c_codecs, *_ = GENERATOR.render(self.schema)
        self.assertIn("EXECUTOR_NEXT_BATCH_RESPONSE_CONTINUATION_HEADER_BYTES", rust)
        self.assertIn("encode_supervisor_invocation_control_request", rust)
        self.assertIn("decode_supervisor_invocation_control_request", rust)
        self.assertIn("encode_supervisor_admin_cancel_request", rust)
        self.assertIn("decode_supervisor_admin_cancel_request", rust)
        self.assertIn("encode_supervisor_cancel_result_simple", rust)
        self.assertIn("decode_supervisor_cancel_result_simple", rust)
        self.assertIn("encode_supervisor_cancel_result_terminal", rust)
        self.assertIn("decode_supervisor_cancel_result_terminal", rust)
        self.assertIn("encode_supervisor_status_terminal_response", rust)
        self.assertIn("decode_supervisor_status_terminal_response", rust)
        self.assertIn("PRDB_PLANNER_ESTIMATE_SCAN_RESPONSE_HEADER_BYTES", c)
        self.assertIn("prdb_encode_supervisor_invocation_control_request", c_codecs)
        self.assertIn("prdb_decode_supervisor_invocation_control_request", c_codecs)
        self.assertIn("prdb_encode_supervisor_admin_cancel_request", c_codecs)
        self.assertIn("prdb_decode_supervisor_admin_cancel_request", c_codecs)
        self.assertIn("prdb_encode_supervisor_cancel_result_simple", c_codecs)
        self.assertIn("prdb_decode_supervisor_cancel_result_simple", c_codecs)
        self.assertIn("prdb_encode_supervisor_cancel_result_terminal", c_codecs)
        self.assertIn("prdb_decode_supervisor_cancel_result_terminal", c_codecs)
        self.assertIn("prdb_encode_supervisor_status_terminal_response", c_codecs)
        self.assertIn("prdb_decode_supervisor_status_terminal_response", c_codecs)
        self.assertIn("prdb_encode_supervisor_finalize_response", c_codecs)
        self.assertIn("prdb_decode_supervisor_finalize_response", c_codecs)
        self.assertIn("prdb_encode_supervisor_abort_response", c_codecs)
        self.assertIn("prdb_decode_supervisor_abort_response", c_codecs)
        self.assertIn("prdb_encode_supervisor_status_unknown_response", c_codecs)
        self.assertIn("prdb_decode_supervisor_status_unknown_response", c_codecs)
        self.assertIn("prdb_encode_supervisor_status_active_no_deadline_response", c_codecs)
        self.assertIn("prdb_decode_supervisor_status_active_no_deadline_response", c_codecs)
        self.assertIn("prdb_encode_supervisor_status_active_with_deadline_response", c_codecs)
        self.assertIn("prdb_decode_supervisor_status_active_with_deadline_response", c_codecs)
        self.assertIn("prdb_encode_optimizer_response_failure", c_codecs)
        self.assertIn("prdb_decode_optimizer_response_failure", c_codecs)

    def test_payload_version_must_be_a_leading_u16(self):
        malformed = copy.deepcopy(self.schema)
        malformed["payload_schemas"][0]["fields"][0]["encoding"] = "u32be"
        with self.assertRaisesRegex(ValueError, "start with its u16 version constant"):
            GENERATOR.render(malformed)

    def test_collection_count_must_be_u16(self):
        malformed = copy.deepcopy(self.schema)
        request = next(
            payload
            for payload in malformed["payload_schemas"]
            if payload["name"] == "EXECUTOR_OPEN_SCAN_REQUEST"
        )
        request["fields"][2]["encoding"] = "u32be"
        with self.assertRaisesRegex(ValueError, "must use a u16 count"):
            GENERATOR.render(malformed)

    def test_generated_fixed_codec_rejects_trailing_fields(self):
        malformed = copy.deepcopy(self.schema)
        request = next(
            payload
            for payload in malformed["payload_schemas"]
            if payload["name"] == "SUPERVISOR_INVOCATION_CONTROL_REQUEST"
        )
        request["fields"].append({"name": "tail_length", "encoding": "u32be"})
        request["trailing_fields"] = [
            {"name": "tail", "encoding": "bytes", "length_field": "tail_length"}
        ]
        with self.assertRaisesRegex(ValueError, "cannot encode trailing fields"):
            GENERATOR.render(malformed)


if __name__ == "__main__":
    unittest.main()
