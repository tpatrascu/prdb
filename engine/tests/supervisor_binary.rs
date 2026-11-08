use std::process::Command;

#[test]
fn supervisor_help_documents_explicit_runtime_and_compatibility_configuration() {
    let output = Command::new(env!("CARGO_BIN_EXE_prdb-supervisor"))
        .arg("--help")
        .output()
        .expect("run supervisor help");
    assert!(output.status.success());
    let help = String::from_utf8(output.stdout).expect("UTF-8 help");
    for option in [
        "--instance-dir",
        "--service-socket",
        "--cgroup-parent",
        "--executor",
        "--runtime-read-execute",
        "--cpu-baseline",
        "--runtime-profile",
        "--sdk-release",
        "--allowed-dependency",
    ] {
        assert!(help.contains(option), "missing {option} from help");
    }
}

#[test]
fn supervisor_refuses_to_start_without_explicit_instance_and_sandbox_profile() {
    let output = Command::new(env!("CARGO_BIN_EXE_prdb-supervisor"))
        .output()
        .expect("run supervisor without configuration");
    assert!(!output.status.success());
    let error = String::from_utf8(output.stderr).expect("UTF-8 error");
    assert!(error.contains("--instance-dir is required"));
}
