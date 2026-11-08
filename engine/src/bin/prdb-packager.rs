use prdb_engine::client_package::{PackageOptions, package_query};
use std::collections::HashMap;
use std::path::PathBuf;

const USAGE: &str = "Usage: prdb-packager \\
  --source FILE --sdk-archive FILE --include-dir DIR --scope FILE --output-dir DIR \\
  --abi 1|2|3 --sdk-release NUMBER --cpu-baseline NAME --runtime-profile NAME \\
  --compiler-identity TEXT [--tcc FILE]\n\n\
Compiles one C source with TinyCC, validates the compiled ELF without loading it, \\
and atomically publishes query.so, manifest.pmf1, requested_scope.psc1, and \\
artifact.sha256. The compiler subprocess has a cleared environment, resource \\
limits, and a 60-second wall-clock bound.";

fn main() -> std::process::ExitCode {
    match run() {
        Ok(()) => std::process::ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("prdb-packager: {error}");
            std::process::ExitCode::FAILURE
        }
    }
}

fn run() -> Result<(), String> {
    let mut arguments = std::env::args_os().skip(1);
    let mut fields = HashMap::<String, std::ffi::OsString>::new();
    while let Some(option) = arguments.next() {
        if option == "--help" || option == "-h" {
            println!("{USAGE}");
            return Ok(());
        }
        let option = option
            .into_string()
            .map_err(|_| "options must be UTF-8".to_owned())?;
        if !option.starts_with("--") {
            return Err(format!("unexpected argument {option}; use --help"));
        }
        let value = arguments
            .next()
            .ok_or_else(|| format!("missing value for {option}"))?;
        if fields.insert(option.clone(), value).is_some() {
            return Err(format!("duplicate option {option}"));
        }
    }
    let tcc = fields
        .remove("--tcc")
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from("tcc"));
    let source = take_path(&mut fields, "--source")?;
    let sdk_archive = take_path(&mut fields, "--sdk-archive")?;
    let include_dir = take_path(&mut fields, "--include-dir")?;
    let requested_scope = take_path(&mut fields, "--scope")?;
    let output_dir = take_path(&mut fields, "--output-dir")?;
    let native_abi_version = take_number(&mut fields, "--abi")?;
    let sdk_release = take_number(&mut fields, "--sdk-release")?;
    let cpu_baseline = take_text(&mut fields, "--cpu-baseline")?;
    let runtime_profile = take_text(&mut fields, "--runtime-profile")?;
    let compiler_identity = take_text(&mut fields, "--compiler-identity")?;
    if !fields.is_empty() {
        return Err(format!(
            "unknown option {}",
            fields.keys().next().expect("nonempty option map")
        ));
    }
    let summary = package_query(&PackageOptions {
        tcc,
        source,
        sdk_archive,
        include_dir,
        requested_scope,
        output_dir,
        native_abi_version,
        sdk_release,
        cpu_baseline,
        runtime_profile,
        compiler_identity,
    })
    .map_err(|error| error.to_string())?;
    println!(
        "package={} artifact_bytes={} sha256={}",
        summary.package_dir.display(),
        summary.artifact_bytes,
        lower_hex(&summary.artifact_sha256)
    );
    Ok(())
}

fn take_path(
    fields: &mut HashMap<String, std::ffi::OsString>,
    name: &str,
) -> Result<PathBuf, String> {
    fields
        .remove(name)
        .map(PathBuf::from)
        .ok_or_else(|| format!("missing required option {name}"))
}

fn take_text(
    fields: &mut HashMap<String, std::ffi::OsString>,
    name: &str,
) -> Result<String, String> {
    fields
        .remove(name)
        .ok_or_else(|| format!("missing required option {name}"))?
        .into_string()
        .map_err(|_| format!("{name} must be UTF-8"))
}

fn take_number<T: std::str::FromStr>(
    fields: &mut HashMap<String, std::ffi::OsString>,
    name: &str,
) -> Result<T, String> {
    let text = take_text(fields, name)?;
    text.parse()
        .map_err(|_| format!("invalid numeric value for {name}"))
}

fn lower_hex(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut output = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        output.push(HEX[(byte >> 4) as usize] as char);
        output.push(HEX[(byte & 0x0f) as usize] as char);
    }
    output
}
