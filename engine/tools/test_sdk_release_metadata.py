#!/usr/bin/env python3
"""Exercise SDK release metadata generation against a built shared library."""

from __future__ import annotations

import hashlib
import json
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path


SCRIPT = Path(__file__).with_name("write_sdk_release_metadata.py")


class SdkReleaseMetadataTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        if len(sys.argv) != 2:
            raise RuntimeError("usage: test_sdk_release_metadata.py SHARED_LIBRARY")
        cls.library = Path(sys.argv.pop(1)).resolve()
        if not cls.library.is_file():
            raise RuntimeError(f"shared library does not exist: {cls.library}")

    def run_generator(self, directory: Path, *extra: str) -> subprocess.CompletedProcess[str]:
        return subprocess.run(
            [
                sys.executable,
                str(SCRIPT),
                "--library",
                str(self.library),
                "--metadata",
                str(directory / "release.json"),
                "--checksum",
                str(directory / "release.sha256"),
                "--release",
                "1",
                "--soname",
                "libprdb_sql.so.1",
                "--target",
                "linux-x86_64",
                "--cpu-baseline",
                "x86_64-v1",
                "--runtime-profile",
                "nixos-x86_64-glibc-2.42",
                "--native-abis",
                "1,2,3,4,5,6",
                *extra,
            ],
            check=False,
            capture_output=True,
            text=True,
        )

    def test_metadata_and_digest_match_the_inspected_release_artifact(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            output = Path(temporary)
            result = self.run_generator(output)
            self.assertEqual(result.returncode, 0, result.stderr)
            metadata_path = output / "release.json"
            checksum_path = output / "release.sha256"
            metadata_bytes = metadata_path.read_bytes()
            metadata = json.loads(metadata_bytes)
            digest = hashlib.sha256(self.library.read_bytes()).hexdigest()
            self.assertEqual(metadata["format"], "PRDB-SDK-RELEASE-1")
            self.assertEqual(metadata["sdk_release"], 1)
            self.assertEqual(metadata["library"], self.library.name)
            self.assertEqual(metadata["soname"], "libprdb_sql.so.1")
            self.assertEqual(metadata["sha256"], digest)
            self.assertEqual(metadata["target"], "linux-x86_64")
            self.assertEqual(metadata["cpu_baseline"], "x86_64-v1")
            self.assertEqual(metadata["runtime_profile"], "nixos-x86_64-glibc-2.42")
            self.assertEqual(metadata["native_abis"], [1, 2, 3, 4, 5, 6])
            self.assertEqual(metadata["needed"], sorted(metadata["needed"]))
            self.assertEqual(metadata_bytes, (json.dumps(
                metadata, sort_keys=True, separators=(",", ":")
            ) + "\n").encode("ascii"))
            self.assertEqual(
                checksum_path.read_text(encoding="ascii"),
                f"{digest}  {self.library.name}\n",
            )

    def test_soname_mismatch_fails_before_publishing_metadata(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            output = Path(temporary)
            result = self.run_generator(output, "--soname", "libprdb_sql.so.2")
            self.assertNotEqual(result.returncode, 0)
            self.assertFalse((output / "release.json").exists())
            self.assertFalse((output / "release.sha256").exists())

    def test_unsupported_or_noncanonical_abi_list_is_rejected(self) -> None:
        for abi_list in ("1,3,2", "1,1", "0,1", "1,7", ""):
            with self.subTest(abi_list=abi_list), tempfile.TemporaryDirectory() as temporary:
                result = self.run_generator(
                    Path(temporary), "--native-abis", abi_list
                )
                self.assertNotEqual(result.returncode, 0)
                self.assertFalse((Path(temporary) / "release.json").exists())


if __name__ == "__main__":
    unittest.main(argv=[sys.argv[0]])
