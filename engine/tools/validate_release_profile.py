#!/usr/bin/env python3
"""Validate the initial Linux x86_64 NixOS/glibc release-profile staging tree."""

from __future__ import annotations

import hashlib
import re
import subprocess
import sys
from pathlib import Path


PROFILE = "nixos-x86_64-glibc-2.42"
CPU_BASELINE = "x86_64-v1"
ENGINE_BINARIES = (
    "prdb-client",
    "prdb-executor",
    "prdb-gateway",
    "prdb-optimizer",
    "prdb-packager",
    "prdb-sandbox-probe",
    "prdb-supervisor",
)
QUERY_FIXTURES = (
    "sql-v3-caught-error.so",
    "sql-v3-write-read.so",
    "sql-v3-restricted-metadata.so",
)
ENGINE_NEEDED = {"libgcc_s.so.1", "libc.so.6", "ld-linux-x86-64.so.2"}
QUERY_NEEDED = {"libc.so.6"}
ENGINE_GLIBC_CEILING = (2, 39)
QUERY_GLIBC_CEILING = (2, 14)


def readelf(*arguments: str) -> str:
    result = subprocess.run(
        ["readelf", *arguments], check=True, capture_output=True, text=True
    )
    return result.stdout


def fail(message: str) -> None:
    raise SystemExit(message)


def validate_elf(path: Path, *, query: bool) -> tuple[str, ...]:
    header = readelf("-h", str(path))
    if "Class:" not in header or "ELF64" not in header:
        fail(f"{path}: expected ELF64")
    if "Machine:" not in header or "X86-64" not in header:
        fail(f"{path}: expected x86_64 ELF machine")

    dynamic = readelf("-d", str(path))
    if "(RPATH)" in dynamic or "(RUNPATH)" in dynamic:
        fail(f"{path}: embedded RPATH/RUNPATH is not allowed")
    needed = set(re.findall(r"Shared library: \[([^]]+)\]", dynamic))
    expected_needed = QUERY_NEEDED if query else ENGINE_NEEDED
    if needed != expected_needed:
        fail(f"{path}: DT_NEEDED is {sorted(needed)}, expected {sorted(expected_needed)}")

    versions = readelf("--version-info", str(path))
    glibc_versions = [
        tuple(int(part) for part in version.split("."))
        for version in re.findall(r"GLIBC_([0-9]+(?:\.[0-9]+)+)", versions)
    ]
    glibc_ceiling = QUERY_GLIBC_CEILING if query else ENGINE_GLIBC_CEILING
    if glibc_versions and max(glibc_versions) > glibc_ceiling:
        fail(f"{path}: requires GLIBC_{'.'.join(map(str, max(glibc_versions)))} above profile")

    if query:
        program_headers = readelf("-l", str(path))
        lines = program_headers.splitlines()
        stack_index = next(
            (index for index, line in enumerate(lines) if "GNU_STACK" in line), None
        )
        stack_flags = (
            re.search(r"\b(RWE|RW)\b", lines[stack_index + 1]).group(1)
            if stack_index is not None
            and stack_index + 1 < len(lines)
            and re.search(r"\b(RWE|RW)\b", lines[stack_index + 1])
            else None
        )
        if stack_flags != "RW":
            fail(f"{path}: GNU_STACK is missing or executable")
    return tuple(sorted(needed))


def main() -> None:
    if len(sys.argv) != 2:
        fail("usage: validate_release_profile.py RELEASE_PROFILE_DIR")
    root = Path(sys.argv[1])
    binary_paths = [root / "bin" / name for name in ENGINE_BINARIES]
    query_paths = [root / "queries" / name for name in QUERY_FIXTURES]
    archive = root / "sdk" / "libprdb_sdk.a"
    include_dir = root / "sdk" / "include"
    for path in [*binary_paths, *query_paths, archive, include_dir / "prdb_sdk_executor_v1.h"]:
        if not path.exists():
            fail(f"release-profile input is missing: {path}")

    records = []
    for path in binary_paths:
        needed = validate_elf(path, query=False)
        records.append(f"{path.relative_to(root)}: NEEDED={','.join(needed)}")
    for path in query_paths:
        needed = validate_elf(path, query=True)
        records.append(f"{path.relative_to(root)}: NEEDED={','.join(needed)}")

    (root / "PROFILE.txt").write_text(
        "profile=" + PROFILE + "\n"
        + "cpu_baseline=" + CPU_BASELINE + "\n"
        + "glibc_runtime=2.42\n"
        + "rust_release_panic=abort\n"
        + "linker=GNU ld with -z noexecstack for query shared objects\n"
        + "\n".join(records)
        + "\n",
        encoding="utf-8",
    )
    files = sorted(
        path for path in root.rglob("*") if path.is_file() and path.name != "SHA256SUMS"
    )
    lines = []
    for path in files:
        digest = hashlib.sha256(path.read_bytes()).hexdigest()
        lines.append(f"{digest}  {path.relative_to(root).as_posix()}")
    (root / "SHA256SUMS").write_text("\n".join(lines) + "\n", encoding="ascii")
    print(f"validated {len(binary_paths)} engine binaries and {len(query_paths)} query fixtures")
    print(f"profile checksums: {root / 'SHA256SUMS'}")


if __name__ == "__main__":
    main()
