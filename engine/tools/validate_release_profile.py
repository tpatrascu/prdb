#!/usr/bin/env python3
"""Validate the initial Linux x86_64 NixOS/glibc release-profile staging tree."""

from __future__ import annotations

import hashlib
import json
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
    "prdb-init",
    "prdb-landlock-run",
    "prdb-maintain",
    "prdb-optimizer",
    "prdb-packager",
    "prdb-sandbox-probe",
    "prdb-supervisor",
)
QUERY_FIXTURES = (
    "sql-v3-shared-library.so",
)
ENGINE_NEEDED = {"libgcc_s.so.1", "libc.so.6", "ld-linux-x86-64.so.2"}
QUERY_NEEDED = {"libprdb_sql.so.1", "libc.so.6"}
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
    if "(RPATH)" in dynamic:
        fail(f"{path}: DT_RPATH is not allowed")
    runpaths = re.findall(r"\(RUNPATH\).*?\[([^]]*)\]", dynamic)
    if query and runpaths != ["$ORIGIN"]:
        fail(f"{path}: shared query must have exactly $ORIGIN RUNPATH")
    if not query and runpaths:
        fail(f"{path}: engine binary must not embed RUNPATH")
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
        dynamic_symbols = readelf("--dyn-syms", "-W", str(path))
        if not any(
            " UND " in line and "prdb_sdk_sql_execute_select_v2" in line
            for line in dynamic_symbols.splitlines()
        ):
            fail(f"{path}: query does not import the SQL SDK from its shared library")
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
    include_dir = root / "sdk" / "include"
    sdk_release_dir = (
        root
        / "sdk"
        / "releases"
        / "linux-x86_64"
        / CPU_BASELINE
        / PROFILE
        / "sdk-1-abis1,2,3,4,5,6"
    )
    sdk_library = sdk_release_dir / "libprdb_sql.so.1"
    sdk_metadata_path = sdk_release_dir / "sdk-release.json"
    sdk_checksum_path = sdk_release_dir / "sdk-release.sha256"
    toolchain_dir = root / "toolchain"
    cache_dir = toolchain_dir / "tcc-cache"
    required_toolchain_files = (
        toolchain_dir / "TCC_CLOSURE.txt",
        toolchain_dir / "TCC_ROOT.txt",
        toolchain_dir / "restore-tcc-closure.sh",
        cache_dir / "nix-cache-info",
    )
    for path in [
        *binary_paths,
        *query_paths,
        include_dir / "prdb_sdk_executor_v1.h",
        sdk_library,
        sdk_metadata_path,
        sdk_checksum_path,
        *required_toolchain_files,
    ]:
        if not path.exists():
            fail(f"release-profile input is missing: {path}")

    tcc_root = (toolchain_dir / "TCC_ROOT.txt").read_text(encoding="ascii").strip()
    closure_paths = (toolchain_dir / "TCC_CLOSURE.txt").read_text(encoding="ascii").splitlines()
    if not re.fullmatch(r"/nix/store/[a-z0-9]{32}-tcc-[^/]+", tcc_root):
        fail("TinyCC root does not identify a pinned Nix store compiler")
    if not closure_paths or tcc_root not in closure_paths or len(set(closure_paths)) != len(closure_paths):
        fail("TinyCC Nix closure manifest is empty, duplicated, or missing its compiler root")
    if any(not re.fullmatch(r"/nix/store/[a-z0-9]{32}-[^/]+", path) for path in closure_paths):
        fail("TinyCC Nix closure manifest contains an invalid store path")
    narinfos = {path.name.removesuffix(".narinfo") for path in cache_dir.glob("*.narinfo")}
    expected_narinfos = {Path(path).name.split("-", 1)[0] for path in closure_paths}
    if narinfos != expected_narinfos:
        fail("TinyCC binary cache does not contain exactly the manifest closure")
    referenced_nars = set()
    for narinfo in cache_dir.glob("*.narinfo"):
        urls = [
            line.removeprefix("URL: ")
            for line in narinfo.read_text().splitlines()
            if line.startswith("URL: ")
        ]
        if len(urls) != 1:
            fail(f"{narinfo}: expected exactly one NAR URL")
        nar_path = cache_dir / urls[0]
        if Path(urls[0]).is_absolute() or ".." in Path(urls[0]).parts:
            fail(f"{narinfo}: NAR URL escapes the binary cache")
        try:
            nar_path.resolve().relative_to(cache_dir.resolve())
        except ValueError:
            fail(f"{narinfo}: NAR URL escapes the binary cache")
        if not nar_path.is_file():
            fail(f"{narinfo}: referenced NAR is missing")
        referenced_nars.add(nar_path.resolve())
    compressed_nars = {path.resolve() for path in cache_dir.rglob("*.nar.xz")}
    if compressed_nars != referenced_nars:
        fail("TinyCC binary cache has unreferenced or missing compressed NAR files")
    if not (toolchain_dir / "restore-tcc-closure.sh").stat().st_mode & 0o111:
        fail("TinyCC closure restore script is not executable")

    records = []
    for path in binary_paths:
        needed = validate_elf(path, query=False)
        records.append(f"{path.relative_to(root)}: NEEDED={','.join(needed)}")
    metadata = json.loads(sdk_metadata_path.read_text(encoding="ascii"))
    if (
        metadata.get("format") != "PRDB-SDK-RELEASE-1"
        or metadata.get("target") != "linux-x86_64"
        or metadata.get("cpu_baseline") != CPU_BASELINE
        or metadata.get("runtime_profile") != PROFILE
        or metadata.get("sdk_release") != 1
        or metadata.get("soname") != "libprdb_sql.so.1"
        or metadata.get("library") != "libprdb_sql.so.1"
        or metadata.get("sha256") != hashlib.sha256(sdk_library.read_bytes()).hexdigest()
        or metadata.get("needed") != ["libc.so.6"]
    ):
        fail("installed SQL SDK release metadata or digest is inconsistent")
    checksum_fields = sdk_checksum_path.read_text(encoding="ascii").split()
    if checksum_fields != [metadata["sha256"], "libprdb_sql.so.1"]:
        fail("installed SQL SDK checksum sidecar is inconsistent")
    sdk_dynamic = readelf("-d", str(sdk_library))
    if (
        "Library soname: [libprdb_sql.so.1]" not in sdk_dynamic
        or "(RPATH)" in sdk_dynamic
        or "(RUNPATH)" in sdk_dynamic
        or set(re.findall(r"Shared library: \[([^]]+)\]", sdk_dynamic)) != {"libc.so.6"}
    ):
        fail("installed SQL SDK release ELF profile is inconsistent")
    sdk_headers = readelf("-lW", str(sdk_library))
    sdk_stack = next(
        (line for line in sdk_headers.splitlines() if "GNU_STACK" in line), None
    )
    if sdk_stack is None or not re.search(r"\bRW\b", sdk_stack) or "RWE" in sdk_stack:
        fail("installed SQL SDK release has an executable or missing GNU_STACK")

    for path in query_paths:
        needed = validate_elf(path, query=True)
        records.append(f"{path.relative_to(root)}: NEEDED={','.join(needed)}")

    (root / "PROFILE.txt").write_text(
        "profile=" + PROFILE + "\n"
        + "cpu_baseline=" + CPU_BASELINE + "\n"
        + "glibc_runtime=2.42\n"
        + "rust_release_panic=abort\n"
        + "sdk_library=libprdb_sql.so.1 (shared, verified SHA-256)\n"
        + "static_sdk=disabled in release profile; release 1 compatibility is explicit and temporary\n"
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
