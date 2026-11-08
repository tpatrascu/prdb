use prdb_engine::submission_manifest::decode_manifest_v1;
use sha2::{Digest, Sha256};
use std::fs;
use std::path::PathBuf;
use std::process::Command;

fn unique_temp_dir() -> PathBuf {
    let mut random = [0u8; 12];
    getrandom::fill(&mut random).unwrap();
    let suffix = random
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect::<String>();
    std::env::temp_dir().join(format!(
        "prdb-client-package-{}-{suffix}",
        std::process::id()
    ))
}

#[test]
fn packager_compiles_and_atomically_publishes_metadata_without_loading_query() {
    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .unwrap()
        .to_path_buf();
    let temp = unique_temp_dir();
    fs::create_dir(&temp).unwrap();
    let sdk_build = temp.join("sdk");
    let source = temp.join("query.c");
    let scope = temp.join("scope.psc1");
    let package = temp.join("package");
    fs::write(
        &source,
        b"#include \"prdb_query_abi_v3.h\"\n\
          #include <stdlib.h>\n\
          const prdb_query_descriptor_v3_record prdb_query_descriptor_v3 = {\n\
            sizeof(prdb_query_descriptor_v3_record), 3, sizeof(prdb_host_api_v3), sizeof(prdb_invocation_v3)};\n\
          __attribute__((constructor)) static void must_not_load(void) { abort(); }\n\
          int32_t prdb_query_main_v3(const prdb_host_api_v3 *h, const prdb_invocation_v3 *i) { (void)h; (void)i; return 0; }\n",
    )
    .unwrap();
    let mut scope_bytes = b"PSC1\0\x01\x02\x02".to_vec();
    scope_bytes.extend([1; 16]);
    fs::write(&scope, &scope_bytes).unwrap();

    let build = Command::new("make")
        .current_dir(&root)
        .args([
            "-f",
            "engine/sdk/Makefile",
            &format!("BUILD_DIR={}", sdk_build.display()),
            "all",
        ])
        .output()
        .unwrap();
    assert!(
        build.status.success(),
        "{}",
        String::from_utf8_lossy(&build.stderr)
    );

    let archive = sdk_build.join("libprdb_sdk.a");
    let status = Command::new(env!("CARGO_BIN_EXE_prdb-packager"))
        .arg("--source")
        .arg(&source)
        .arg("--sdk-archive")
        .arg(&archive)
        .arg("--include-dir")
        .arg(root.join("engine/include"))
        .arg("--scope")
        .arg(&scope)
        .arg("--output-dir")
        .arg(&package)
        .args([
            "--abi",
            "3",
            "--sdk-release",
            "1",
            "--cpu-baseline",
            "x86_64-v1",
            "--runtime-profile",
            "test-runtime",
            "--compiler-identity",
            "TinyCC integration test",
        ])
        .output()
        .unwrap();
    assert!(
        status.status.success(),
        "stdout={} stderr={}",
        String::from_utf8_lossy(&status.stdout),
        String::from_utf8_lossy(&status.stderr)
    );

    let artifact = fs::read(package.join("query.so")).unwrap();
    let manifest_bytes = fs::read(package.join("manifest.pmf1")).unwrap();
    let manifest = decode_manifest_v1(&manifest_bytes).unwrap();
    assert_eq!(manifest.native_abi_version, 3);
    assert_eq!(manifest.sdk_release, 1);
    assert_eq!(manifest.cpu_baseline, "x86_64-v1");
    assert_eq!(manifest.runtime_profile, "test-runtime");
    assert_eq!(manifest.descriptor_symbol, "prdb_query_descriptor_v3");
    assert_eq!(manifest.entry_point_symbol, "prdb_query_main_v3");
    assert_eq!(
        fs::read(package.join("requested_scope.psc1")).unwrap(),
        scope_bytes
    );
    let digest = Sha256::digest(&artifact)
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect::<String>()
        + "\n";
    assert_eq!(
        fs::read_to_string(package.join("artifact.sha256")).unwrap(),
        digest
    );
    assert!(!temp.join("constructor-ran").exists());

    let second = Command::new(env!("CARGO_BIN_EXE_prdb-packager"))
        .arg("--source")
        .arg(&source)
        .arg("--sdk-archive")
        .arg(&archive)
        .arg("--include-dir")
        .arg(root.join("engine/include"))
        .arg("--scope")
        .arg(&scope)
        .arg("--output-dir")
        .arg(&package)
        .args([
            "--abi",
            "3",
            "--sdk-release",
            "1",
            "--cpu-baseline",
            "x86_64-v1",
            "--runtime-profile",
            "test-runtime",
            "--compiler-identity",
            "TinyCC integration test",
        ])
        .output()
        .unwrap();
    assert!(
        !second.status.success(),
        "existing package path was overwritten"
    );
    fs::remove_dir_all(temp).unwrap();
}
