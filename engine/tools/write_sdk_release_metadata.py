#!/usr/bin/env python3
"""Validate and describe one immutable-style SQL SDK release artifact."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import subprocess
import sys
import tempfile
from pathlib import Path


PROFILE_TOKEN = re.compile(r"[A-Za-z0-9][A-Za-z0-9_.-]{0,63}\Z")
SONAME_TOKEN = re.compile(r"libprdb_sql\.so\.[1-9][0-9]*\Z")
REQUIRED_EXPORTS = (
    "prdb_sdk_manifest_encode_v1",
    "prdb_sdk_scope_encode_v1",
    "prdb_sdk_sql_parse_select_v1",
    "prdb_sdk_optimizer_execute_update_unified_v1",
    "prdb_sdk_optimizer_execute_select_unified_v1",
)


def run_readelf(*args: str) -> str:
    result = subprocess.run(
        ["readelf", *args], check=True, capture_output=True, text=True
    )
    return result.stdout


def check_elf(library: Path, soname: str) -> list[str]:
    header = run_readelf("-h", str(library))
    if "Class:" not in header or "ELF64" not in header:
        raise ValueError("SQL library must be ELF64")
    if "Machine:" not in header or "X86-64" not in header:
        raise ValueError("SQL library must target x86_64")
    if not re.search(r"Type:\s+DYN\b", header):
        raise ValueError("SQL library must be an ELF shared object")

    dynamic = run_readelf("-dW", str(library))
    found_sonames = re.findall(r"\(SONAME\).*?\[([^]]+)\]", dynamic)
    if found_sonames != [soname]:
        raise ValueError(f"ELF SONAME {found_sonames!r} does not match {soname!r}")
    if "(RPATH)" in dynamic or "(RUNPATH)" in dynamic:
        raise ValueError("SDK release library must not embed RPATH or RUNPATH")
    if "TEXTREL" in dynamic:
        raise ValueError("SDK release library contains text relocations")
    needed = sorted(re.findall(r"\(NEEDED\).*?\[([^]]+)\]", dynamic))
    if len(needed) != len(set(needed)):
        raise ValueError("SDK release library has duplicate DT_NEEDED entries")
    if any("/" in dependency or "\\" in dependency for dependency in needed):
        raise ValueError("DT_NEEDED entries must be SONAME basenames")

    symbols = run_readelf("--dyn-syms", "-W", str(library))
    defined = set()
    for line in symbols.splitlines():
        fields = line.split()
        if len(fields) >= 8 and fields[6] != "UND":
            defined.add(fields[7].split("@", 1)[0])
    missing = sorted(set(REQUIRED_EXPORTS) - defined)
    if missing:
        raise ValueError(f"SDK release is missing public API exports: {missing}")

    relocations = run_readelf("-rW", str(library))
    if re.search(r"\bR_X86_64_32S?\b", relocations):
        raise ValueError("SDK release contains non-position-independent x86_64 relocations")

    program_headers = run_readelf("-lW", str(library))
    stack_line = next(
        (line for line in program_headers.splitlines() if "GNU_STACK" in line), None
    )
    stack_flags = (
        re.search(r"\bGNU_STACK\s+\S+\s+\S+\s+\S+\s+\S+\s+\S+\s+(\S+)", stack_line)
        if stack_line is not None
        else None
    )
    if stack_flags is None or stack_flags.group(1) != "RW":
        raise ValueError("SDK release must have a non-executable GNU_STACK")
    return needed


def digest_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def atomic_write(path: Path, contents: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    descriptor, temporary = tempfile.mkstemp(prefix=f".{path.name}.", dir=path.parent)
    try:
        with os.fdopen(descriptor, "wb") as stream:
            stream.write(contents)
            stream.flush()
            os.fsync(stream.fileno())
        os.chmod(temporary, 0o644)
        os.replace(temporary, path)
    except BaseException:
        try:
            os.unlink(temporary)
        except FileNotFoundError:
            pass
        raise


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--library", required=True, type=Path)
    parser.add_argument("--metadata", required=True, type=Path)
    parser.add_argument("--checksum", required=True, type=Path)
    parser.add_argument("--release", required=True, type=int)
    parser.add_argument("--soname", required=True)
    parser.add_argument("--target", required=True)
    parser.add_argument("--cpu-baseline", required=True)
    parser.add_argument("--runtime-profile", required=True)
    parser.add_argument("--native-abis", required=True)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    try:
        if not args.library.is_file() or args.library.is_symlink():
            raise ValueError("library must be an existing regular file")
        if not 1 <= args.release <= 0xFFFFFFFF:
            raise ValueError("SDK release must be a nonzero u32")
        if not SONAME_TOKEN.fullmatch(args.soname):
            raise ValueError("SONAME must be libprdb_sql.so.<positive release>")
        if args.soname != f"libprdb_sql.so.{args.release}":
            raise ValueError("SONAME release must match --release")
        for name, value in (
            ("target", args.target),
            ("CPU baseline", args.cpu_baseline),
            ("runtime profile", args.runtime_profile),
        ):
            if not PROFILE_TOKEN.fullmatch(value):
                raise ValueError(f"{name} must be a bounded compatibility identifier")
        if args.target != "linux-x86_64":
            raise ValueError("this release builder supports only linux-x86_64")
        try:
            native_abis = [int(value) for value in args.native_abis.split(",")]
        except ValueError as error:
            raise ValueError("native ABI list must contain comma-separated integers") from error
        if (
            not native_abis
            or native_abis != sorted(set(native_abis))
            or any(abi != 1 for abi in native_abis)
        ):
            raise ValueError("native ABI list must be sorted unique versions in 1..=6")
        if len({args.library.resolve(), args.metadata.resolve(), args.checksum.resolve()}) != 3:
            raise ValueError("library, metadata, and checksum paths must be distinct")

        needed = check_elf(args.library, args.soname)
        sha256 = digest_file(args.library)
        metadata = {
            "format": "PRDB-SDK-RELEASE-1",
            "sdk_release": args.release,
            "library": args.library.name,
            "soname": args.soname,
            "sha256": sha256,
            "target": args.target,
            "cpu_baseline": args.cpu_baseline,
            "runtime_profile": args.runtime_profile,
            "native_abis": native_abis,
            "needed": needed,
        }
        metadata_bytes = (
            json.dumps(metadata, sort_keys=True, separators=(",", ":")) + "\n"
        ).encode("ascii")
        checksum_bytes = f"{sha256}  {args.library.name}\n".encode("ascii")
        atomic_write(args.checksum, checksum_bytes)
        # Publish metadata last; its presence marks the release description complete.
        atomic_write(args.metadata, metadata_bytes)
        return 0
    except (OSError, subprocess.CalledProcessError, ValueError) as error:
        print(f"write_sdk_release_metadata: {error}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
