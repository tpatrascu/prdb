use prdb_engine::client_config::{
    ClientConfig, ClientCredential, ClientInstance, ClientTools, SdkSelection, default_config_path,
    encode_token_secret, lock_sdk_selection,
};
use prdb_engine::client_credentials::{
    ClientCredentialProfiles, read_client_credentials, remove_client_credentials,
};
use prdb_engine::client_protocol::ClientCredentials;
use prdb_engine::client_submit::{AdminClient, SdkLibraryInfo};
use prdb_engine::supervisor::WorkloadClass;
use sha2::{Digest, Sha256};
use std::collections::HashMap;
use std::fs::{self};
use std::io::{self};
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use zeroize::{Zeroize, Zeroizing};

use super::*;

pub(super) fn run_client_config_command(
    command: &str,
    fields: &mut HashMap<String, std::ffi::OsString>,
    credential_profile_options: &[std::ffi::OsString],
) -> Result<(), String> {
    let config_path = fields
        .remove("--config")
        .map(PathBuf::from)
        .map(Ok)
        .unwrap_or_else(default_config_path)
        .map_err(|error| error.to_string())?;
    let mut config = match ClientConfig::load(&config_path) {
        Ok(config) => config,
        Err(prdb_engine::client_config::ClientConfigError::Io(error))
            if error.kind() == io::ErrorKind::NotFound =>
        {
            ClientConfig::empty()
        }
        Err(error) => return Err(error.to_string()),
    };

    match command {
        "instance list" => {
            reject_unknown(fields)?;
            for instance in &config.instances {
                let marker = if config.default_instance.as_deref() == Some(&instance.name) {
                    " *"
                } else {
                    ""
                };
                println!(
                    "{} {}:{}{}",
                    instance.name, instance.address, instance.port, marker
                );
            }
            Ok(())
        }
        "instance show" => {
            let name = take_optional_text(fields, "--instance")?;
            reject_unknown(fields)?;
            let instance = config
                .instance(name.as_deref())
                .map_err(|error| error.to_string())?;
            println!(
                "{} {}:{} sdk={} abi={} target={} cpu={} runtime={}",
                instance.name,
                instance.address,
                instance.port,
                instance.sdk.release,
                instance.sdk.native_abi,
                instance.sdk.target,
                instance.sdk.cpu_baseline,
                instance.sdk.runtime_profile
            );
            Ok(())
        }
        "instance set" => {
            let name = take_text(fields, "--instance")?;
            let address = take_text(fields, "--address")?;
            let port = take_text(fields, "--port")?
                .parse::<u16>()
                .map_err(|_| "--port must be in 1..65535".to_owned())?;
            let sdk_options = [
                "--release",
                "--target",
                "--cpu-baseline",
                "--runtime-profile",
                "--abi",
            ]
            .map(|option| take_optional_text(fields, option))
            .into_iter()
            .collect::<Result<Vec<_>, _>>()?;
            let old_sdk = config
                .instances
                .iter()
                .find(|instance| instance.name == name)
                .map(|instance| instance.sdk.clone());
            let sdk = if sdk_options.iter().all(Option::is_none) {
                old_sdk.ok_or_else(|| "new instances require --release, --target, --cpu-baseline, --runtime-profile, and --abi".to_owned())?
            } else if sdk_options.iter().all(Option::is_some) {
                SdkSelection {
                    release: sdk_options[0]
                        .as_ref()
                        .unwrap()
                        .parse::<u32>()
                        .map_err(|_| "--release must be a nonzero integer".to_owned())?,
                    target: sdk_options[1].clone().unwrap(),
                    cpu_baseline: sdk_options[2].clone().unwrap(),
                    runtime_profile: sdk_options[3].clone().unwrap(),
                    native_abi: sdk_options[4]
                        .as_ref()
                        .unwrap()
                        .parse::<u8>()
                        .map_err(|_| "--abi must be a supported integer".to_owned())?,
                }
            } else {
                return Err("supply all five SDK options together, or omit them to preserve the current selection".to_owned());
            };
            let tool_options = ["--include-dir", "--tcc", "--shared-linker", "--patchelf"]
                .map(|option| take_optional_text(fields, option))
                .into_iter()
                .collect::<Result<Vec<_>, _>>()?;
            if tool_options.iter().all(Option::is_some) {
                config.client_tools = Some(ClientTools {
                    include_dir: tool_options[0].clone().unwrap(),
                    tcc: tool_options[1].clone().unwrap(),
                    shared_linker: tool_options[2].clone().unwrap(),
                    patchelf: tool_options[3].clone().unwrap(),
                });
            } else if !tool_options.iter().all(Option::is_none) {
                return Err(
                    "supply --include-dir, --tcc, --shared-linker, and --patchelf together"
                        .to_owned(),
                );
            }
            let default_credential = take_text(fields, "--default-credential")?;
            let username = take_text(fields, "--username")?;
            let credentials_path = take_path(fields, "--credentials")?;
            let selection_changed = config
                .instances
                .iter()
                .find(|instance| instance.name == name)
                .is_none_or(|instance| instance.sdk != sdk);
            let make_default = fields
                .remove("--make-default")
                .map(|value| {
                    value
                        .into_string()
                        .map_err(|_| "--make-default must be true or false".to_owned())
                })
                .transpose()?
                .map(|value| match value.as_str() {
                    "true" => Ok(true),
                    "false" => Ok(false),
                    _ => Err("--make-default must be true or false".to_owned()),
                })
                .transpose()?
                .unwrap_or(config.default_instance.is_none());
            reject_unknown(fields)?;
            let credential = client_credential_from_file(
                default_credential.clone(),
                username,
                credentials_path,
            )?;
            let mut credentials = config
                .instances
                .iter()
                .find(|instance| instance.name == name)
                .map(|instance| instance.credentials.clone())
                .unwrap_or_default();
            if let Some(existing) = credentials
                .iter_mut()
                .find(|entry| entry.name == default_credential)
            {
                *existing = credential;
            } else {
                credentials.push(credential);
            }
            for profile_option in credential_profile_options {
                let value = profile_option
                    .clone()
                    .into_string()
                    .map_err(|_| "--credential-profile must be UTF-8".to_owned())?;
                let (profile_name, rest) = value
                    .split_once('=')
                    .ok_or_else(|| "--credential-profile must be NAME=USERNAME=FILE".to_owned())?;
                let (profile_username, profile_path) = rest
                    .split_once('=')
                    .ok_or_else(|| "--credential-profile must be NAME=USERNAME=FILE".to_owned())?;
                if profile_name.is_empty() || profile_username.is_empty() || profile_path.is_empty()
                {
                    return Err("--credential-profile must be NAME=USERNAME=FILE".to_owned());
                }
                let named = client_credential_from_file(
                    profile_name.to_owned(),
                    profile_username.to_owned(),
                    PathBuf::from(profile_path),
                )?;
                if let Some(existing) = credentials
                    .iter_mut()
                    .find(|entry| entry.name == profile_name)
                {
                    *existing = named;
                } else {
                    credentials.push(named);
                }
            }
            config.upsert_instance(
                ClientInstance {
                    name: name.clone(),
                    address,
                    port,
                    sdk,
                    default_credential,
                    credentials,
                },
                make_default,
            );
            let _selection_lock = if selection_changed {
                Some(
                    lock_sdk_selection(&config_path, &name, false)
                        .map_err(|error| error.to_string())?,
                )
            } else {
                None
            };
            config
                .save_atomic(&config_path)
                .map_err(|error| error.to_string())
        }
        "sdk-library list"
        | "sdk-library select"
        | "sdk-library install"
        | "sdk-library remove" => {
            let name = take_optional_text(fields, "--instance")?;
            let instance = config
                .instance(name.as_deref())
                .map_err(|error| error.to_string())?
                .clone();
            let credential = instance
                .credentials
                .iter()
                .find(|entry| entry.name == instance.default_credential)
                .ok_or_else(|| "default credential profile is missing".to_owned())?;
            let address = format!("{}:{}", instance.address, instance.port)
                .parse::<SocketAddr>()
                .map_err(|_| "configured endpoint is invalid".to_owned())?;
            let token_id = credential
                .token_id_bytes()
                .map_err(|error| error.to_string())?;
            let mut token_secret = credential
                .token_secret_bytes()
                .map_err(|error| error.to_string())?;
            let mut client =
                AdminClient::connect_authenticated(address, token_id, &mut token_secret)
                    .map_err(|error| error.to_string())?;
            match command {
                "sdk-library list" => {
                    reject_unknown(fields)?;
                    for release in client
                        .list_sdk_libraries()
                        .map_err(|error| error.to_string())?
                    {
                        println!(
                            "{} abi={:?} target={} cpu={} runtime={} bytes={} sha256={}",
                            release.sdk_release,
                            release.native_abis,
                            release.target,
                            release.cpu_baseline,
                            release.runtime_profile,
                            release.library_bytes,
                            lower_hex(&release.sha256)
                        );
                    }
                    Ok(())
                }
                "sdk-library select" => {
                    let release = take_text(fields, "--release")?
                        .parse::<u32>()
                        .map_err(|_| "--release must be a nonzero integer".to_owned())?;
                    let abi = take_text(fields, "--abi")?
                        .parse::<u8>()
                        .map_err(|_| "--abi must be a supported integer".to_owned())?;
                    reject_unknown(fields)?;
                    let listed = client
                        .list_sdk_libraries()
                        .map_err(|error| error.to_string())?;
                    let selected = listed
                        .into_iter()
                        .find(|entry| entry.sdk_release == release)
                        .ok_or_else(|| {
                            "requested SDK release is not installed on this instance".to_owned()
                        })?;
                    if !selected.native_abis.contains(&abi) {
                        return Err(
                            "requested native ABI is not provided by this release".to_owned()
                        );
                    }
                    verify_local_sdk_build(
                        &config_path,
                        config.client_tools.as_ref(),
                        &selected,
                        abi,
                    )?;
                    let _selection_lock = lock_sdk_selection(&config_path, &instance.name, false)
                        .map_err(|error| error.to_string())?;
                    let current = config
                        .instances
                        .iter_mut()
                        .find(|entry| entry.name == instance.name)
                        .ok_or_else(|| "configured instance was not found".to_owned())?;
                    current.sdk = SdkSelection {
                        release,
                        target: selected.target,
                        cpu_baseline: selected.cpu_baseline,
                        runtime_profile: selected.runtime_profile,
                        native_abi: abi,
                    };
                    config
                        .save_atomic(&config_path)
                        .map_err(|error| error.to_string())
                }
                "sdk-library install" => {
                    let metadata_path = take_path(fields, "--metadata")?;
                    let library_path = take_path(fields, "--library")?;
                    reject_unknown(fields)?;
                    let metadata = fs::read(metadata_path).map_err(|error| error.to_string())?;
                    let library = fs::read(library_path).map_err(|error| error.to_string())?;
                    let release = client
                        .install_sdk_library(&metadata, &library)
                        .map_err(|error| error.to_string())?;
                    println!(
                        "installed SDK release {} ({})",
                        release.sdk_release,
                        lower_hex(&release.sha256)
                    );
                    Ok(())
                }
                "sdk-library remove" => {
                    let release = take_text(fields, "--release")?
                        .parse::<u32>()
                        .map_err(|_| "--release must be a nonzero integer".to_owned())?;
                    reject_unknown(fields)?;
                    let removed = client
                        .remove_sdk_library(release)
                        .map_err(|error| error.to_string())?;
                    println!("removed SDK release {release} ({removed} bytes)");
                    Ok(())
                }
                _ => unreachable!(),
            }
        }
        _ => Err(format!("unknown command {command}\n{USAGE}")),
    }
}

pub(super) fn client_credential_from_file(
    name: String,
    username: String,
    credentials_path: PathBuf,
) -> Result<ClientCredential, String> {
    let mut material =
        read_client_credentials(credentials_path).map_err(|error| error.to_string())?;
    let credential = ClientCredential {
        name,
        username,
        token_id: lower_hex(&material.token_id),
        token_secret: encode_token_secret(&material.token_secret),
    };
    material.token_secret.zeroize();
    Ok(credential)
}

pub(super) fn run_configured_submission(
    fields: &mut HashMap<String, std::ffi::OsString>,
) -> Result<(), String> {
    let config_path = fields
        .remove("--config")
        .map(PathBuf::from)
        .map(Ok)
        .unwrap_or_else(default_config_path)
        .map_err(|error| error.to_string())?;
    let config = ClientConfig::load(&config_path).map_err(|error| error.to_string())?;
    let requested_instance = take_optional_text(fields, "--instance")?;
    let instance = config
        .instance(requested_instance.as_deref())
        .map_err(|error| error.to_string())?
        .clone();
    let credential_name = fields
        .remove("--credential")
        .map(|value| {
            value
                .into_string()
                .map_err(|_| "--credential must be UTF-8".to_owned())
        })
        .transpose()?
        .unwrap_or_else(|| instance.default_credential.clone());
    let credential = instance
        .credentials
        .iter()
        .find(|entry| entry.name == credential_name)
        .ok_or_else(|| format!("credential profile {credential_name:?} is not configured"))?;
    if fields.remove("--credentials").is_some() {
        return Err(
            "--credentials cannot be combined with persistent instance configuration".to_owned(),
        );
    }
    let configured_address = format!("{}:{}", instance.address, instance.port)
        .parse::<SocketAddr>()
        .map_err(|_| "configured instance endpoint is invalid".to_owned())?;
    if let Some(value) = fields.remove("--address") {
        let address = value
            .into_string()
            .map_err(|_| "--address must be UTF-8".to_owned())?
            .parse::<SocketAddr>()
            .map_err(|_| "--address must be a numeric IP:PORT socket address".to_owned())?;
        if address != configured_address {
            return Err(
                "explicit endpoint does not match the selected configured instance".to_owned(),
            );
        }
    }
    if let Some(value) = fields.remove("--port") {
        let port = value
            .into_string()
            .map_err(|_| "--port must be UTF-8".to_owned())?
            .parse::<u16>()
            .map_err(|_| "--port must be in 1..65535".to_owned())?;
        if port != instance.port {
            return Err("explicit port does not match the selected configured instance".to_owned());
        }
    }
    let token_id = credential
        .token_id_bytes()
        .map_err(|error| error.to_string())?;
    let token_secret = credential
        .token_secret_bytes()
        .map_err(|error| error.to_string())?;
    let credentials = ClientCredentials {
        token_id,
        token_secret,
    };

    let package = take_path(fields, "--package")?;
    let receipt = take_path(fields, "--receipt")?;
    let parameters_path = fields.remove("--parameters").map(PathBuf::from);
    let deadline_ms = fields
        .remove("--deadline-ms")
        .map(|value| parse_number(value, "--deadline-ms"))
        .transpose()?
        .unwrap_or(30_000u64);
    let workload = parse_workload(fields)?;
    reject_unknown(fields)?;
    validate_package_selection(&package, &instance.sdk)?;
    let mut parameters = read_parameters(parameters_path.as_ref())?;
    let result = submit_package_and_stream_with_profile(
        configured_address,
        credentials,
        &package,
        &receipt,
        &mut parameters,
        deadline_ms,
        workload,
        true,
    );
    parameters.fill(0);
    result
}

pub(super) fn run_configured_interactive(
    fields: &mut HashMap<String, std::ffi::OsString>,
) -> Result<(), String> {
    let config_path = fields
        .remove("--config")
        .map(PathBuf::from)
        .map(Ok)
        .unwrap_or_else(default_config_path)
        .map_err(|error| error.to_string())?;
    let config = ClientConfig::load(&config_path).map_err(|error| error.to_string())?;
    let requested_instance = take_optional_text(fields, "--instance")?;
    let instance = config
        .instance(requested_instance.as_deref())
        .map_err(|error| error.to_string())?
        .clone();
    let credential_name = fields
        .remove("--credential")
        .map(|value| {
            value
                .into_string()
                .map_err(|_| "--credential must be UTF-8".to_owned())
        })
        .transpose()?
        .unwrap_or_else(|| instance.default_credential.clone());
    let default_user = instance
        .credentials
        .iter()
        .find(|entry| entry.name == credential_name)
        .map(|entry| entry.username.clone())
        .ok_or_else(|| format!("credential profile {credential_name:?} is not configured"))?;
    if fields.remove("--credentials").is_some() {
        return Err(
            "--credentials cannot be combined with persistent instance configuration".to_owned(),
        );
    }
    let address = format!("{}:{}", instance.address, instance.port)
        .parse::<SocketAddr>()
        .map_err(|_| "configured instance endpoint is invalid".to_owned())?;
    if let Some(value) = fields.remove("--address") {
        let explicit = value
            .into_string()
            .map_err(|_| "--address must be UTF-8".to_owned())?
            .parse::<SocketAddr>()
            .map_err(|_| "--address must be a numeric IP:PORT socket address".to_owned())?;
        if explicit != address {
            return Err(
                "explicit endpoint does not match the selected configured instance".to_owned(),
            );
        }
    }
    if let Some(value) = fields.remove("--port") {
        let port = value
            .into_string()
            .map_err(|_| "--port must be UTF-8".to_owned())?
            .parse::<u16>()
            .map_err(|_| "--port must be in 1..65535".to_owned())?;
        if port != instance.port {
            return Err("explicit port does not match the selected configured instance".to_owned());
        }
    }
    if instance.sdk.native_abi != 3 {
        return Err("interactive-sql currently generates native ABI 3 queries; select ABI 3 for this instance".to_owned());
    }
    let tools = config
        .client_tools
        .as_ref()
        .ok_or_else(|| "client_tools must be configured before interactive-sql".to_owned())?;
    let (include_dir, sdk_library) = local_sdk_paths(&config_path, tools, &instance.sdk)?;
    for option in [
        "--sdk-library",
        "--include-dir",
        "--sdk-release",
        "--abi",
        "--cpu-baseline",
        "--runtime-profile",
    ] {
        if fields.remove(option).is_some() {
            return Err(format!(
                "{option} is read from persistent instance configuration"
            ));
        }
    }
    fields.insert("--sdk-library".to_owned(), sdk_library.into_os_string());
    fields.insert("--include-dir".to_owned(), include_dir.into_os_string());
    fields.insert(
        "--sdk-release".to_owned(),
        instance.sdk.release.to_string().into(),
    );
    fields.insert(
        "--cpu-baseline".to_owned(),
        instance.sdk.cpu_baseline.clone().into(),
    );
    fields.insert(
        "--runtime-profile".to_owned(),
        instance.sdk.runtime_profile.clone().into(),
    );
    fields.insert("--compiler-identity".to_owned(), "prdb-client/tcc".into());
    fields.insert(
        "--tcc".to_owned(),
        resolve_client_program(&config_path, &tools.tcc).into_os_string(),
    );
    fields.insert(
        "--shared-linker".to_owned(),
        resolve_client_program(&config_path, &tools.shared_linker).into_os_string(),
    );
    fields.insert(
        "--patchelf".to_owned(),
        resolve_client_program(&config_path, &tools.patchelf).into_os_string(),
    );
    let profiles = instance
        .credentials
        .iter()
        .map(|profile| {
            Ok((
                profile.username.clone(),
                profile
                    .token_id_bytes()
                    .map_err(|error| error.to_string())?,
                Zeroizing::new(
                    profile
                        .token_secret_bytes()
                        .map_err(|error| error.to_string())?,
                ),
            ))
        })
        .collect::<Result<Vec<_>, String>>()?;
    let profiles = ClientCredentialProfiles::from_material(&default_user, profiles)
        .map_err(|error| error.to_string())?;
    interactive_sql(
        address,
        InteractiveClientOptions {
            default_credentials_path: PathBuf::new(),
            default_user,
            additional_profiles: Vec::new(),
            configured_profiles: Some(profiles),
            repository_profile: true,
            selection_lock_context: Some((config_path, instance.name.clone())),
        },
        fields,
    )
}

pub(super) fn parse_workload(
    fields: &mut HashMap<String, std::ffi::OsString>,
) -> Result<WorkloadClass, String> {
    match fields.remove("--workload") {
        None => Ok(WorkloadClass::Operational),
        Some(value) if value == "operational" => Ok(WorkloadClass::Operational),
        Some(value) if value == "analytical" => Ok(WorkloadClass::Analytical),
        Some(_) => Err("--workload must be operational or analytical".to_owned()),
    }
}

pub(super) fn validate_package_selection(
    package: &Path,
    selection: &SdkSelection,
) -> Result<(), String> {
    let manifest_bytes = fs::read(package.join("manifest.pmf1"))
        .map_err(|error| format!("could not read package manifest: {error}"))?;
    let manifest = prdb_engine::submission_manifest::decode_manifest_v1(&manifest_bytes)
        .map_err(|error| format!("invalid package manifest: {error:?}"))?;
    if manifest.sdk_release != selection.release
        || manifest.native_abi_version != selection.native_abi
        || manifest.cpu_baseline != selection.cpu_baseline
        || manifest.runtime_profile != selection.runtime_profile
    {
        return Err(
            "package SDK release/profile/ABI does not match the selected instance settings"
                .to_owned(),
        );
    }
    for sidecar in ["sql.so", "sql-library.sha256"] {
        if fs::symlink_metadata(package.join(sidecar)).is_ok() {
            return Err(
                "repository-profile packages must not contain an SDK library or its digest sidecar"
                    .to_owned(),
            );
        }
    }
    Ok(())
}

pub(super) fn local_sdk_paths(
    config_path: &Path,
    tools: &ClientTools,
    selection: &SdkSelection,
) -> Result<(PathBuf, PathBuf), String> {
    let include_dir = resolve_config_path(config_path, &tools.include_dir);
    let sdk_root = include_dir
        .parent()
        .ok_or_else(|| "client_tools.include_dir must be inside an SDK directory".to_owned())?;
    let release_root = sdk_root
        .join("releases")
        .join(&selection.target)
        .join(&selection.cpu_baseline)
        .join(&selection.runtime_profile);
    let prefix = format!("sdk-{}-abis", selection.release);
    let entries = fs::read_dir(&release_root).map_err(|error| error.to_string())?;
    for entry in entries {
        let entry = entry.map_err(|error| error.to_string())?;
        let entry_name = entry.file_name();
        let Some(abi_list) = entry_name
            .to_string_lossy()
            .strip_prefix(&prefix)
            .map(str::to_owned)
        else {
            continue;
        };
        if abi_list
            .split(',')
            .any(|abi| abi == selection.native_abi.to_string())
        {
            let library = entry
                .path()
                .join(format!("libprdb_sql.so.{}", selection.release));
            if library.is_file() {
                return Ok((include_dir, library));
            }
        }
    }
    Err(format!(
        "matching local SDK release {} is unavailable",
        selection.release
    ))
}

pub(super) fn resolve_config_path(config_path: &Path, value: &str) -> PathBuf {
    let path = PathBuf::from(value);
    if path.is_absolute() {
        path
    } else {
        config_path
            .parent()
            .unwrap_or_else(|| Path::new("."))
            .join(path)
    }
}

pub(super) fn resolve_client_program(config_path: &Path, value: &str) -> PathBuf {
    let path = PathBuf::from(value);
    if path.is_absolute() || path.components().count() == 1 {
        path
    } else {
        resolve_config_path(config_path, value)
    }
}

pub(super) fn verify_local_sdk_build(
    config_path: &Path,
    tools: Option<&prdb_engine::client_config::ClientTools>,
    release_info: &SdkLibraryInfo,
    native_abi: u8,
) -> Result<(), String> {
    let release = release_info.sdk_release;
    let tools = tools.ok_or_else(|| {
        "configure client_tools.include_dir before selecting an SDK release".to_owned()
    })?;
    let include = PathBuf::from(&tools.include_dir);
    let include = if include.is_absolute() {
        include
    } else {
        config_path
            .parent()
            .unwrap_or_else(|| Path::new("."))
            .join(include)
    };
    let sdk_root = include
        .parent()
        .ok_or_else(|| "client_tools.include_dir must be inside an SDK directory".to_owned())?;
    let release_root = sdk_root
        .join("releases")
        .join(&release_info.target)
        .join(&release_info.cpu_baseline)
        .join(&release_info.runtime_profile);
    let prefix = format!("sdk-{release}-abis");
    let entries = fs::read_dir(&release_root).map_err(|_| {
        format!(
            "matching local SDK release {release} is not installed under {}",
            release_root.display()
        )
    })?;
    for entry in entries {
        let entry = entry.map_err(|error| error.to_string())?;
        let entry_name = entry.file_name();
        let Some(abi_list) = entry_name
            .to_string_lossy()
            .strip_prefix(&prefix)
            .map(str::to_owned)
        else {
            continue;
        };
        if !abi_list
            .split(',')
            .any(|value| value == native_abi.to_string())
        {
            continue;
        }
        let library_path = entry.path().join(format!("libprdb_sql.so.{release}"));
        let metadata = fs::metadata(&library_path).map_err(|error| error.to_string())?;
        if !metadata.is_file() || metadata.len() == 0 || metadata.len() > 128 * 1024 * 1024 {
            continue;
        }
        let library = fs::read(&library_path).map_err(|error| error.to_string())?;
        if Sha256::digest(&library).as_slice() == release_info.sha256 {
            return Ok(());
        }
    }
    Err(format!(
        "matching local SDK release {release} is not installed under {}",
        release_root.display()
    ))
}

pub(super) fn credential_remove(path: PathBuf) -> Result<(), String> {
    remove_client_credentials(&path).map_err(|error| error.to_string())?;
    println!("removed credential file {}", path.display());
    Ok(())
}
