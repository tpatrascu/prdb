use prdb_engine::client_config::lock_sdk_selection;
use prdb_engine::client_credentials::ClientCredentialProfiles;
use prdb_engine::client_package::{PackageOptions, package_query_repository};
use prdb_engine::client_protocol::ClientCredentials;
use prdb_engine::client_sql::{
    MAX_SQL_PARAMETER_BYTES, encode_parameter_file, read_sql_input, validate_schema_name,
};
use prdb_engine::client_submit::{
    ClientResultEvent, InvocationStatus, SubmissionClient, TerminalOutcome,
};
use prdb_engine::interactive_sql::{InteractiveStatement, parse_interactive_sql};
use prdb_engine::supervisor::WorkloadClass;
use std::collections::HashMap;
use std::fs::{self, OpenOptions};
use std::io::{self, BufRead, IsTerminal, Read, Write};
use std::net::SocketAddr;
use std::os::unix::fs::{DirBuilderExt, OpenOptionsExt};
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};
use zeroize::Zeroizing;

use super::*;

pub(super) struct InteractiveClientOptions {
    pub(super) default_credentials_path: PathBuf,
    pub(super) default_user: String,
    pub(super) additional_profiles: Vec<(String, PathBuf)>,
    pub(super) configured_profiles: Option<ClientCredentialProfiles>,
    pub(super) repository_profile: bool,
    pub(super) selection_lock_context: Option<(PathBuf, String)>,
}

pub(super) struct InteractiveExecutionSettings {
    pub(super) deadline_ms: u64,
    pub(super) workload: WorkloadClass,
    pub(super) repository_profile: bool,
}

pub(super) fn interactive_sql(
    address: SocketAddr,
    options: InteractiveClientOptions,
    fields: &mut HashMap<String, std::ffi::OsString>,
) -> Result<(), String> {
    let InteractiveClientOptions {
        default_credentials_path,
        default_user,
        additional_profiles,
        configured_profiles,
        repository_profile,
        selection_lock_context,
    } = options;
    if !address.ip().is_loopback() {
        return Err("interactive SQL v2 is available only on loopback addresses".to_owned());
    }
    let sql_path = fields.remove("--sql-file").map(PathBuf::from);
    let schema = take_text(fields, "--schema")?;
    validate_schema_name(&schema).map_err(|error| error.to_string())?;
    let requested_scope = take_path(fields, "--scope")?;
    let sdk_library = take_path(fields, "--sdk-library")?;
    let include_dir = take_path(fields, "--include-dir")?;
    let sdk_release = take_text(fields, "--sdk-release")?
        .parse::<u32>()
        .map_err(|_| "invalid numeric value for --sdk-release".to_owned())?;
    let cpu_baseline = take_text(fields, "--cpu-baseline")?;
    let runtime_profile = take_text(fields, "--runtime-profile")?;
    let compiler_identity = take_text(fields, "--compiler-identity")?;
    let tcc = fields
        .remove("--tcc")
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from("tcc"));
    let shared_linker = fields
        .remove("--shared-linker")
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from("cc"));
    let patchelf = fields
        .remove("--patchelf")
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from("patchelf"));
    let parameters_path = fields.remove("--parameters").map(PathBuf::from);
    let deadline_ms = fields
        .remove("--deadline-ms")
        .map(|value| parse_number(value, "--deadline-ms"))
        .transpose()?
        .unwrap_or(30_000u64);
    if deadline_ms == 0 {
        return Err("--deadline-ms must be positive".to_owned());
    }
    let workload = match fields.remove("--workload") {
        None => WorkloadClass::Operational,
        Some(value) if value == "operational" => WorkloadClass::Operational,
        Some(value) if value == "analytical" => WorkloadClass::Analytical,
        Some(_) => return Err("--workload must be operational or analytical".to_owned()),
    };
    reject_unknown(fields)?;

    let profiles = match configured_profiles {
        Some(profiles) => profiles,
        None => ClientCredentialProfiles::load(
            &default_user,
            &default_credentials_path,
            &additional_profiles,
        )
        .map_err(|error| error.to_string())?,
    };
    let mut parameters = read_sql_parameters(parameters_path.as_deref())?;
    let workspace = SqlWorkspace::create()?;
    let mut explicit_transaction = None;
    let mut abort_only = false;
    let mut selection_lock = None;
    let result = {
        let mut dispatch = |statement: &InteractiveStatement<'_>,
                            ordinal: usize|
         -> Result<(), String> {
            match statement {
                InteractiveStatement::Begin => {
                    if explicit_transaction.is_some() {
                        return Err("BEGIN cannot be nested".to_owned());
                    }
                    let pending_selection_lock = selection_lock_context
                        .as_ref()
                        .map(|(config_path, instance_name)| {
                            lock_sdk_selection(config_path, instance_name, true)
                                .map_err(|error| error.to_string())
                        })
                        .transpose()?;
                    let mut controller = profile_credentials(&profiles, &default_user)?;
                    let mut client = connect_interactive_client(
                        address,
                        controller.token_id,
                        &mut controller.token_secret,
                        repository_profile,
                    )
                    .map_err(|error| error.to_string())?;
                    let begin_request_id = random_nonzero_id()?;
                    let begin = match client.interactive_transaction_begin(begin_request_id) {
                        Ok(response) => response,
                        Err(
                            prdb_engine::client_submit::ClientSubmitError::InteractiveTransactionBeginOutcomeUncertain,
                        ) => {
                            drop(client);
                            let mut retry_controller =
                                profile_credentials(&profiles, &default_user)?;
                            let mut retry_client =
                                connect_interactive_client(address, retry_controller.token_id, &mut retry_controller.token_secret, repository_profile)
                                .map_err(|error| error.to_string())?;
                            retry_client
                                .interactive_transaction_begin(begin_request_id)
                                .map_err(|error| {
                                    format!(
                                        "BEGIN outcome is uncertain for request {}; retry with the same request ID to recover ({error})",
                                        lower_hex(&begin_request_id)
                                    )
                                })?
                        }
                        Err(error) => return Err(error.to_string()),
                    };
                    match begin {
                        prdb_engine::client_protocol::ClientV2Response::TransactionBegun {
                            transaction_id,
                            state: prdb_engine::client_protocol::ClientV2TransactionState::Active,
                            ..
                        } => {
                            selection_lock = pending_selection_lock;
                            explicit_transaction = Some(transaction_id);
                            abort_only = false;
                            eprintln!("began transaction {}", lower_hex(&transaction_id));
                        }
                        prdb_engine::client_protocol::ClientV2Response::TransactionBegun {
                            transaction_id,
                            state,
                            ..
                        } => {
                            return Err(format!(
                                "BEGIN request {} resolved to transaction {} in state {state:?}",
                                lower_hex(&begin_request_id),
                                lower_hex(&transaction_id)
                            ));
                        }
                        _ => return Err("invalid transaction begin response".to_owned()),
                    }
                }
                InteractiveStatement::Commit => {
                    let transaction_id = explicit_transaction
                        .ok_or_else(|| "COMMIT requires an active transaction".to_owned())?;
                    if abort_only {
                        return Err("transaction is abort-only; issue ROLLBACK".to_owned());
                    }
                    let mut controller = profile_credentials(&profiles, &default_user)?;
                    let mut client = connect_interactive_client(
                        address,
                        controller.token_id,
                        &mut controller.token_secret,
                        repository_profile,
                    )
                    .map_err(|error| error.to_string())?;
                    match client.interactive_transaction_commit(transaction_id) {
                        Ok(prdb_engine::client_protocol::ClientV2Response::Transaction {
                            transaction_id: response_id,
                            state: prdb_engine::client_protocol::ClientV2TransactionState::Committed,
                        }) if response_id == transaction_id => {
                            explicit_transaction = None;
                            selection_lock = None;
                            eprintln!("committed transaction {}", lower_hex(&transaction_id));
                        }
                        Ok(_) => return Err("transaction did not commit".to_owned()),
                        Err(error) => {
                            let mut controller = profile_credentials(&profiles, &default_user)?;
                            let mut status_client = connect_interactive_client(
                                address,
                                controller.token_id,
                                &mut controller.token_secret,
                                repository_profile,
                            )
                            .map_err(|connect_error| connect_error.to_string())?;
                            match status_client.interactive_transaction_status(transaction_id) {
                            Ok(prdb_engine::client_protocol::ClientV2Response::Transaction {
                                transaction_id: response_id,
                                state:
                                    prdb_engine::client_protocol::ClientV2TransactionState::Committed,
                            }) if response_id == transaction_id => {
                                explicit_transaction = None;
                                selection_lock = None;
                                eprintln!("committed transaction {}", lower_hex(&transaction_id));
                            }
                            _ => {
                                return Err(format!(
                                    "COMMIT outcome is uncertain for transaction {}; query status before continuing ({error})",
                                    lower_hex(&transaction_id)
                                ));
                            }
                        }
                        }
                    }
                }
                InteractiveStatement::Rollback => {
                    let transaction_id = explicit_transaction
                        .ok_or_else(|| "ROLLBACK requires an active transaction".to_owned())?;
                    let mut controller = profile_credentials(&profiles, &default_user)?;
                    let mut client = connect_interactive_client(
                        address,
                        controller.token_id,
                        &mut controller.token_secret,
                        repository_profile,
                    )
                    .map_err(|error| error.to_string())?;
                    match client.interactive_transaction_rollback(transaction_id) {
                        Ok(prdb_engine::client_protocol::ClientV2Response::Transaction {
                            transaction_id: response_id,
                            state: prdb_engine::client_protocol::ClientV2TransactionState::Aborted,
                        }) if response_id == transaction_id => {
                            explicit_transaction = None;
                            selection_lock = None;
                            eprintln!("rolled back transaction {}", lower_hex(&transaction_id));
                        }
                        Ok(_) => return Err("transaction did not roll back".to_owned()),
                        Err(error) => {
                            let mut controller = profile_credentials(&profiles, &default_user)?;
                            let mut status_client = connect_interactive_client(
                                address,
                                controller.token_id,
                                &mut controller.token_secret,
                                repository_profile,
                            )
                            .map_err(|connect_error| connect_error.to_string())?;
                            match status_client.interactive_transaction_status(transaction_id) {
                            Ok(prdb_engine::client_protocol::ClientV2Response::Transaction {
                                transaction_id: response_id,
                                state:
                                    prdb_engine::client_protocol::ClientV2TransactionState::Aborted
                                    | prdb_engine::client_protocol::ClientV2TransactionState::Expired,
                            }) if response_id == transaction_id => {
                                explicit_transaction = None;
                                selection_lock = None;
                                eprintln!("rolled back transaction {}", lower_hex(&transaction_id));
                            }
                            _ => {
                                return Err(format!(
                                    "ROLLBACK outcome is uncertain for transaction {}; query status before continuing ({error})",
                                    lower_hex(&transaction_id)
                                ));
                            }
                        }
                        }
                    }
                }
                InteractiveStatement::Ddl(ddl) => {
                    if explicit_transaction.is_some() {
                        return Err("DDL is not allowed inside an explicit transaction".to_owned());
                    }
                    let credentials = profile_credentials(&profiles, &default_user)?;
                    admin_sql_with_credentials(address, credentials, ddl.as_bytes())?;
                }
                InteractiveStatement::Query(query) => {
                    execute_interactive_package(
                        address,
                        &profiles,
                        &default_user,
                        explicit_transaction,
                        &mut abort_only,
                        query.as_bytes(),
                        false,
                        &schema,
                        &workspace.path,
                        ordinal,
                        &tcc,
                        &shared_linker,
                        &patchelf,
                        &sdk_library,
                        &include_dir,
                        &requested_scope,
                        sdk_release,
                        &cpu_baseline,
                        &runtime_profile,
                        &compiler_identity,
                        &mut parameters,
                        deadline_ms,
                        workload,
                        repository_profile,
                    )?;
                }
                InteractiveStatement::RunAs { username, sql, .. } => {
                    execute_interactive_package(
                        address,
                        &profiles,
                        username,
                        explicit_transaction,
                        &mut abort_only,
                        sql.as_bytes(),
                        true,
                        &schema,
                        &workspace.path,
                        ordinal,
                        &tcc,
                        &shared_linker,
                        &patchelf,
                        &sdk_library,
                        &include_dir,
                        &requested_scope,
                        sdk_release,
                        &cpu_baseline,
                        &runtime_profile,
                        &compiler_identity,
                        &mut parameters,
                        deadline_ms,
                        workload,
                        repository_profile,
                    )?;
                }
            }
            Ok(())
        };

        (|| {
            let interactive_terminal = sql_path.is_none() && io::stdin().is_terminal();
            if interactive_terminal {
                let stdin = io::stdin();
                let mut input = stdin.lock();
                run_interactive_repl(
                    &mut input,
                    &mut io::stdout(),
                    true,
                    &profiles,
                    &mut dispatch,
                )
            } else {
                let sql_bytes = if let Some(sql_path) = sql_path.as_deref() {
                    Zeroizing::new(read_sql_input(sql_path).map_err(|error| error.to_string())?)
                } else {
                    Zeroizing::new(
                        read_sql_input(Path::new("-")).map_err(|error| error.to_string())?,
                    )
                };
                let sql = std::str::from_utf8(&sql_bytes)
                    .map_err(|_| "SQL input must be valid UTF-8".to_owned())?;
                let statements = parse_interactive_sql(sql).map_err(|error| error.to_string())?;
                validate_interactive_script_placement(&statements)?;
                require_runas_profiles(&statements, &profiles)?;
                for (ordinal, statement) in statements.iter().enumerate() {
                    dispatch(statement, ordinal)?;
                }
                Ok(())
            }
        })()
    };

    if result.is_err()
        && let Some(transaction_id) = explicit_transaction
        && let Ok(mut controller) = profile_credentials(&profiles, &default_user)
        && let Ok(mut client) = connect_interactive_client(
            address,
            controller.token_id,
            &mut controller.token_secret,
            repository_profile,
        )
    {
        let _ = client.interactive_transaction_rollback(transaction_id);
    }
    if result.is_ok()
        && let Some(transaction_id) = explicit_transaction
        && let Ok(mut controller) = profile_credentials(&profiles, &default_user)
        && let Ok(mut client) = connect_interactive_client(
            address,
            controller.token_id,
            &mut controller.token_secret,
            repository_profile,
        )
    {
        let _ = client.interactive_transaction_rollback(transaction_id);
        return Err(format!(
            "input ended with active transaction {}; it was rolled back",
            lower_hex(&transaction_id)
        ));
    }
    parameters.fill(0);
    result
}

#[allow(clippy::too_many_arguments)]
pub(super) fn execute_interactive_package(
    address: SocketAddr,
    profiles: &ClientCredentialProfiles,
    actor_username: &str,
    transaction_id: Option<[u8; 16]>,
    abort_only: &mut bool,
    sql: &[u8],
    _is_batch: bool,
    schema: &str,
    workspace: &Path,
    ordinal: usize,
    tcc: &Path,
    shared_linker: &Path,
    patchelf: &Path,
    sdk_library: &Path,
    include_dir: &Path,
    requested_scope: &Path,
    sdk_release: u32,
    cpu_baseline: &str,
    runtime_profile: &str,
    compiler_identity: &str,
    parameters: &mut [u8],
    deadline_ms: u64,
    workload: WorkloadClass,
    repository_profile: bool,
) -> Result<(), String> {
    if *abort_only {
        return Err("transaction is abort-only; issue ROLLBACK".to_owned());
    }
    let generated_source = prdb_engine::client_sql::generate_query_batch_source(schema, sql)
        .map_err(|error| error.to_string())?;
    let source_path = workspace.join(format!("interactive-{ordinal}.c"));
    let package_dir = workspace.join(format!("interactive-package-{ordinal}"));
    write_private_file(&source_path, &generated_source)?;
    let package_options = PackageOptions {
        tcc: tcc.to_path_buf(),
        shared_linker: shared_linker.to_path_buf(),
        patchelf: patchelf.to_path_buf(),
        source: source_path,
        sdk_library: sdk_library.to_path_buf(),
        include_dir: include_dir.to_path_buf(),
        requested_scope: requested_scope.to_path_buf(),
        output_dir: package_dir.clone(),
        native_abi_version: 1,
        sdk_release,
        cpu_baseline: cpu_baseline.to_owned(),
        runtime_profile: runtime_profile.to_owned(),
        compiler_identity: compiler_identity.to_owned(),
    };
    package_query_repository(&package_options).map_err(|error| error.to_string())?;
    let receipt = workspace.join(format!("interactive-{ordinal}.receipt"));
    let mut package_parameters = Zeroizing::new(parameters.to_vec());
    if let Some(transaction_id) = transaction_id {
        let (controller_id, mut controller_secret) = profiles
            .authentication_material(profiles.default_username())
            .ok_or_else(|| "default credential profile disappeared".to_owned())?;
        let mut client = connect_interactive_client(
            address,
            controller_id,
            &mut controller_secret[..],
            repository_profile,
        )
        .map_err(|error| error.to_string())?;
        match client
            .interactive_transaction_attach(transaction_id)
            .map_err(|error| error.to_string())?
        {
            prdb_engine::client_protocol::ClientV2Response::Transaction {
                transaction_id: response_id,
                state: prdb_engine::client_protocol::ClientV2TransactionState::Active,
            } if response_id == transaction_id => {}
            _ => return Err("transaction is not active".to_owned()),
        }
        let package_id = random_nonzero_id()?;
        let invocation_id = client
            .upload_interactive_package_and_seal(
                &package_dir,
                &package_parameters,
                deadline_ms,
                workload,
                transaction_id,
                package_id,
            )
            .map_err(|error| error.to_string())?;
        let actor = if actor_username != profiles.default_username() {
            let (token_id, token_secret) = profiles
                .authentication_material(actor_username)
                .ok_or_else(|| format!("no credential profile is loaded for {actor_username}"))?;
            Some(prdb_engine::client_protocol::ClientStartActorCredentials {
                expected_name: actor_username.to_owned(),
                token_id,
                token_secret,
            })
        } else {
            None
        };
        eprintln!(
            "results from transaction {} are provisional until COMMIT",
            lower_hex(&transaction_id)
        );
        let stream_result = match client.start_interactive_package(
            invocation_id,
            transaction_id,
            package_id,
            actor,
            &receipt,
        ) {
            Ok(mut started) => stream_interactive_events(&mut started),
            Err(
                prdb_engine::client_submit::ClientSubmitError::InteractivePackageOutcomeUncertain {
                    ..
                },
            ) => Err("START reply was lost".to_owned()),
            Err(error) => return Err(error.to_string()),
        };
        let terminal = match stream_result {
            Ok(terminal) => terminal,
            Err(error) => reconcile_interactive_package(
                address,
                profiles,
                transaction_id,
                package_id,
                repository_profile,
            )
            .map_err(|reconcile_error| format!("{error}; {reconcile_error}"))?,
        };
        match terminal {
            InteractivePackageTerminal::Completed { result_lost: false } => Ok(()),
            InteractivePackageTerminal::Completed { result_lost: true } => Err(format!(
                "package {package_id:02x?} completed but its result was lost"
            )),
            InteractivePackageTerminal::Failed => {
                *abort_only = true;
                Err(format!(
                    "package {package_id:02x?} failed; transaction is abort-only"
                ))
            }
            InteractivePackageTerminal::Unknown => {
                Err(format!("package {package_id:02x?} outcome is unknown"))
            }
        }
    } else {
        let credentials = profile_credentials(profiles, actor_username)?;
        stream_autocommit_interactive_package(
            address,
            credentials,
            &package_dir,
            &receipt,
            &mut package_parameters,
            InteractiveExecutionSettings {
                deadline_ms,
                workload,
                repository_profile,
            },
        )
    }
}

pub(super) fn connect_interactive_client(
    address: SocketAddr,
    token_id: [u8; 16],
    token_secret: &mut [u8],
    _repository_profile: bool,
) -> Result<SubmissionClient, prdb_engine::client_submit::ClientSubmitError> {
    SubmissionClient::connect_repository_interactive_authenticated_with_timeout(
        address,
        token_id,
        token_secret,
        Duration::from_secs(10),
    )
}

pub(super) fn profile_credentials(
    profiles: &ClientCredentialProfiles,
    username: &str,
) -> Result<ClientCredentials, String> {
    let (token_id, token_secret) = profiles
        .authentication_material(username)
        .ok_or_else(|| format!("no credential profile is loaded for {username}"))?;
    Ok(ClientCredentials {
        token_id,
        token_secret: *token_secret,
    })
}

pub(super) fn stream_autocommit_interactive_package(
    address: SocketAddr,
    mut credentials: ClientCredentials,
    package: &Path,
    receipt: &Path,
    parameters: &mut [u8],
    settings: InteractiveExecutionSettings,
) -> Result<(), String> {
    let InteractiveExecutionSettings {
        deadline_ms,
        workload,
        repository_profile,
    } = settings;
    let result = (|| {
        let token_id = credentials.token_id;
        let mut reconciliation_secret = Zeroizing::new(credentials.token_secret);
        let mut client = connect_interactive_client(
            address,
            token_id,
            &mut credentials.token_secret,
            repository_profile,
        )
        .map_err(|error| error.to_string())?;
        let invocation_id = client
            .upload_and_seal(package, parameters, deadline_ms, workload)
            .map_err(|error| error.to_string())?;
        match client.start(invocation_id, receipt) {
            Ok(mut started) => {
                eprintln!("results are provisional until this package commits");
                match stream_interactive_events(&mut started)? {
                    InteractivePackageTerminal::Completed { result_lost: false } => Ok(()),
                    InteractivePackageTerminal::Completed { result_lost: true } => Err(format!(
                        "invocation {} completed but its result was lost",
                        lower_hex(&invocation_id)
                    )),
                    InteractivePackageTerminal::Failed => Err(format!(
                        "invocation {} did not commit",
                        lower_hex(&invocation_id)
                    )),
                    InteractivePackageTerminal::Unknown => Err(format!(
                        "invocation {} outcome is unknown",
                        lower_hex(&invocation_id)
                    )),
                }
            }
            Err(prdb_engine::client_submit::ClientSubmitError::OutcomeUncertain(_)) => {
                drop(client);
                let mut status_client = connect_interactive_client(
                    address,
                    token_id,
                    &mut reconciliation_secret[..],
                    repository_profile,
                )
                .map_err(|error| {
                    format!(
                        "START outcome is uncertain for invocation {}; status lookup failed ({error})",
                        lower_hex(&invocation_id)
                    )
                })?;
                let deadline = Instant::now() + Duration::from_millis(deadline_ms);
                loop {
                    match status_client
                        .status(invocation_id)
                        .map_err(|error| error.to_string())?
                    {
                        InvocationStatus::Committed => {
                            return Err(format!(
                                "invocation {} committed, but its result was lost after an uncertain START reply",
                                lower_hex(&invocation_id)
                            ));
                        }
                        InvocationStatus::Aborted => {
                            return Err(format!(
                                "invocation {} aborted after an uncertain START reply",
                                lower_hex(&invocation_id)
                            ));
                        }
                        InvocationStatus::Unknown | InvocationStatus::Expired => {
                            return Err(format!(
                                "invocation {} has no retained outcome after an uncertain START reply",
                                lower_hex(&invocation_id)
                            ));
                        }
                        InvocationStatus::Prepared
                        | InvocationStatus::Active
                        | InvocationStatus::Committing => {
                            if Instant::now() >= deadline {
                                return Err(format!(
                                    "invocation {} remains active after an uncertain START reply; query status before continuing",
                                    lower_hex(&invocation_id)
                                ));
                            }
                            std::thread::sleep(Duration::from_millis(25));
                        }
                    }
                }
            }
            Err(error) => Err(error.to_string()),
        }
    })();
    parameters.fill(0);
    result
}

pub(super) fn random_nonzero_id() -> Result<[u8; 16], String> {
    for _ in 0..8 {
        let mut id = [0; 16];
        getrandom::fill(&mut id).map_err(|error| error.to_string())?;
        if id != [0; 16] {
            return Ok(id);
        }
    }
    Err("could not generate a nonzero identifier".to_owned())
}

pub(super) fn reconcile_interactive_package(
    address: SocketAddr,
    profiles: &ClientCredentialProfiles,
    transaction_id: [u8; 16],
    package_id: [u8; 16],
    repository_profile: bool,
) -> Result<InteractivePackageTerminal, String> {
    let (token_id, mut token_secret) = profiles
        .authentication_material(profiles.default_username())
        .ok_or_else(|| "default credential profile disappeared".to_owned())?;
    let mut client =
        connect_interactive_client(address, token_id, &mut token_secret[..], repository_profile)
            .map_err(|error| error.to_string())?;
    match client
        .interactive_package_status(transaction_id, package_id)
        .map_err(|error| error.to_string())?
    {
        prdb_engine::client_protocol::ClientV2Response::Package {
            transaction_id: response_transaction,
            package_id: response_package,
            state,
            ..
        } if response_transaction == transaction_id && response_package == package_id => {
            Ok(match state {
                prdb_engine::client_protocol::ClientV2PackageState::Completed => {
                    InteractivePackageTerminal::Completed { result_lost: true }
                }
                prdb_engine::client_protocol::ClientV2PackageState::Failed => {
                    InteractivePackageTerminal::Failed
                }
                _ => InteractivePackageTerminal::Unknown,
            })
        }
        _ => Err("invalid package status response".to_owned()),
    }
}

pub(super) enum InteractivePackageTerminal {
    Completed { result_lost: bool },
    Failed,
    Unknown,
}

pub(super) fn stream_interactive_events(
    started: &mut prdb_engine::client_submit::StartedInvocation<'_>,
) -> Result<InteractivePackageTerminal, String> {
    let mut decoder = SqlBatchResultDecoder::default();
    while let Some(event) = started.next_event().map_err(|error| error.to_string())? {
        match event {
            ClientResultEvent::Data(data) => {
                for record in decoder.push(&data)? {
                    match record {
                        SqlBatchRecord::Data { ordinal, payload } => {
                            let _statement_ordinal = ordinal;
                            io::stdout()
                                .write_all(&payload)
                                .and_then(|()| io::stdout().flush())
                                .map_err(|error| error.to_string())?
                        }
                        SqlBatchRecord::Failed { ordinal, code } => {
                            eprintln!("SQL statement {} failed with code {code}", ordinal + 1);
                        }
                        SqlBatchRecord::Start { ordinal } => {
                            eprintln!("running statement {}", ordinal + 1);
                        }
                        SqlBatchRecord::Complete { ordinal } => {
                            eprintln!("statement {} complete", ordinal + 1);
                        }
                    }
                }
            }
            ClientResultEvent::PackageTerminal {
                state, result_lost, ..
            } => {
                decoder.finish()?;
                return Ok(match state {
                    prdb_engine::client_protocol::ClientV2PackageState::Completed => {
                        InteractivePackageTerminal::Completed { result_lost }
                    }
                    prdb_engine::client_protocol::ClientV2PackageState::Failed => {
                        InteractivePackageTerminal::Failed
                    }
                    _ => InteractivePackageTerminal::Unknown,
                });
            }
            ClientResultEvent::Terminal(terminal) => {
                decoder.finish()?;
                return match terminal.outcome {
                    TerminalOutcome::Committed => {
                        Ok(InteractivePackageTerminal::Completed { result_lost: false })
                    }
                    TerminalOutcome::Aborted => Ok(InteractivePackageTerminal::Failed),
                    TerminalOutcome::Unknown => Ok(InteractivePackageTerminal::Unknown),
                };
            }
        }
    }
    Err("result stream ended without a terminal outcome".to_owned())
}

#[derive(Default)]
pub(super) struct SqlBatchResultDecoder {
    pub(super) bytes: Vec<u8>,
}

pub(super) enum SqlBatchRecord {
    Start { ordinal: u16 },
    Data { ordinal: u16, payload: Vec<u8> },
    Complete { ordinal: u16 },
    Failed { ordinal: u16, code: u32 },
}

impl SqlBatchResultDecoder {
    pub(super) fn push(&mut self, chunk: &[u8]) -> Result<Vec<SqlBatchRecord>, String> {
        if self.bytes.len().saturating_add(chunk.len()) > 2 * 1024 * 1024 + 24 {
            return Err("SQL batch result record exceeds the client limit".to_owned());
        }
        self.bytes.extend_from_slice(chunk);
        let mut records = Vec::new();
        loop {
            if self.bytes.len() < 12 {
                break;
            }
            if &self.bytes[..4] != b"SQLB" || self.bytes[4] != 1 {
                return Err("invalid SQL batch result header".to_owned());
            }
            let event = self.bytes[5];
            let ordinal = u16::from_be_bytes([self.bytes[6], self.bytes[7]]);
            let payload_len =
                u32::from_be_bytes([self.bytes[8], self.bytes[9], self.bytes[10], self.bytes[11]])
                    as usize;
            if payload_len > 1024 * 1024 {
                return Err("SQL batch result payload exceeds the client limit".to_owned());
            }
            let record_len = 12 + payload_len;
            if self.bytes.len() < record_len {
                break;
            }
            let payload = self.bytes[12..record_len].to_vec();
            records.push(match event {
                1 if payload.is_empty() => SqlBatchRecord::Start { ordinal },
                2 => SqlBatchRecord::Data { ordinal, payload },
                3 if payload.is_empty() => SqlBatchRecord::Complete { ordinal },
                4 if payload.len() == 4 => SqlBatchRecord::Failed {
                    ordinal,
                    code: u32::from_be_bytes(payload.try_into().expect("length checked")),
                },
                _ => return Err("invalid SQL batch result event".to_owned()),
            });
            self.bytes.drain(..record_len);
        }
        Ok(records)
    }

    pub(super) fn finish(&self) -> Result<(), String> {
        if self.bytes.is_empty() {
            Ok(())
        } else {
            Err("truncated SQL batch result record".to_owned())
        }
    }
}

pub(super) fn validate_interactive_script_placement(
    statements: &[InteractiveStatement<'_>],
) -> Result<(), String> {
    let mut active = false;
    for statement in statements {
        match statement {
            InteractiveStatement::Begin if active => {
                return Err("BEGIN cannot be nested".to_owned());
            }
            InteractiveStatement::Begin => active = true,
            InteractiveStatement::Commit | InteractiveStatement::Rollback if !active => {
                return Err("COMMIT/ROLLBACK requires an active transaction".to_owned());
            }
            InteractiveStatement::Commit | InteractiveStatement::Rollback => active = false,
            InteractiveStatement::Ddl(_) if active => {
                return Err("DDL is not allowed inside an explicit transaction".to_owned());
            }
            _ => {}
        }
    }
    Ok(())
}

pub(super) fn require_runas_profiles(
    statements: &[InteractiveStatement<'_>],
    profiles: &ClientCredentialProfiles,
) -> Result<(), String> {
    for statement in statements {
        if let InteractiveStatement::RunAs { username, .. } = statement
            && !profiles.contains_username(username)
        {
            return Err(format!(
                "no credential profile is loaded for RUNAS user {username}"
            ));
        }
    }
    Ok(())
}

pub(super) fn run_interactive_repl<R, W, F>(
    input: &mut R,
    output: &mut W,
    show_prompt: bool,
    profiles: &ClientCredentialProfiles,
    dispatch: &mut F,
) -> Result<(), String>
where
    R: BufRead,
    W: Write,
    F: for<'sql> FnMut(&InteractiveStatement<'sql>, usize) -> Result<(), String>,
{
    let mut pending = Zeroizing::new(String::new());
    let mut ordinal = 0usize;
    loop {
        if show_prompt {
            write!(
                output,
                "{}",
                if pending.is_empty() { "sql> " } else { "...> " }
            )
            .map_err(|error| error.to_string())?;
            output.flush().map_err(|error| error.to_string())?;
        }
        let mut line = String::new();
        let bytes = input
            .read_line(&mut line)
            .map_err(|error| error.to_string())?;
        let at_eof = bytes == 0;
        if !at_eof {
            pending.push_str(&line);
            if pending.len() > prdb_engine::interactive_sql::MAX_INTERACTIVE_SQL_BYTES {
                writeln!(
                    output,
                    "interactive SQL input exceeds the configured byte limit"
                )
                .map_err(|error| error.to_string())?;
                pending.clear();
                continue;
            }
        } else if pending.trim().is_empty() {
            break;
        }

        if !at_eof && !interactive_input_complete(&pending) {
            continue;
        }
        match parse_interactive_sql(&pending) {
            Ok(statements) => {
                if let Err(error) = require_runas_profiles(&statements, profiles) {
                    writeln!(output, "{error}").map_err(|write_error| write_error.to_string())?;
                } else {
                    for statement in &statements {
                        if let Err(error) = dispatch(statement, ordinal) {
                            writeln!(output, "{error}")
                                .map_err(|write_error| write_error.to_string())?;
                        }
                        ordinal = ordinal.saturating_add(1);
                    }
                }
            }
            Err(
                error @ (prdb_engine::interactive_sql::InteractiveSqlError::MissingEndRunAs
                | prdb_engine::interactive_sql::InteractiveSqlError::UnterminatedString
                | prdb_engine::interactive_sql::InteractiveSqlError::UnterminatedComment),
            ) if !at_eof => {
                let _ = error;
                continue;
            }
            Err(error) => {
                writeln!(output, "{error}").map_err(|write_error| write_error.to_string())?;
            }
        }
        pending.clear();
        if at_eof {
            break;
        }
    }
    Ok(())
}

pub(super) fn interactive_input_complete(pending: &str) -> bool {
    if !pending.trim_end().ends_with(';') {
        return false;
    }
    match parse_interactive_sql(pending) {
        Ok(_) => true,
        Err(
            prdb_engine::interactive_sql::InteractiveSqlError::MissingEndRunAs
            | prdb_engine::interactive_sql::InteractiveSqlError::UnterminatedString
            | prdb_engine::interactive_sql::InteractiveSqlError::UnterminatedComment,
        ) => false,
        Err(_) => true,
    }
}

pub(super) struct SqlWorkspace {
    pub(super) path: PathBuf,
}

impl SqlWorkspace {
    fn create() -> Result<Self, String> {
        let parent = std::env::temp_dir();
        for _ in 0..8 {
            let mut random = [0u8; 16];
            getrandom::fill(&mut random).map_err(|error| error.to_string())?;
            let path = parent.join(format!(
                "prdb-interactive-sql-{}-{}",
                std::process::id(),
                lower_hex(&random)
            ));
            random.fill(0);
            let mut builder = fs::DirBuilder::new();
            builder.mode(0o700);
            match builder.create(&path) {
                Ok(()) => return Ok(Self { path }),
                Err(error) if error.kind() == io::ErrorKind::AlreadyExists => continue,
                Err(error) => return Err(error.to_string()),
            }
        }
        Err("could not allocate a private SQL build workspace".to_owned())
    }
}

impl Drop for SqlWorkspace {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.path);
    }
}

pub(super) fn read_sql_parameters(path: Option<&Path>) -> Result<Zeroizing<Vec<u8>>, String> {
    let Some(path) = path else {
        return Ok(Zeroizing::new(Vec::new()));
    };
    let mut file = OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC)
        .open(path)
        .map_err(|error| error.to_string())?;
    if !file
        .metadata()
        .map_err(|error| error.to_string())?
        .is_file()
    {
        return Err("SQL parameter input must be a regular file".to_owned());
    }
    let mut input = Zeroizing::new(Vec::new());
    Read::take(&mut file, (MAX_SQL_PARAMETER_BYTES + 1) as u64)
        .read_to_end(&mut input)
        .map_err(|error| error.to_string())?;
    if input.len() > MAX_SQL_PARAMETER_BYTES {
        return Err("SQL parameter input exceeds 1 MiB".to_owned());
    }
    encode_parameter_file(input.as_slice()).map_err(|error| error.to_string())
}

pub(super) fn write_private_file(path: &Path, bytes: &[u8]) -> Result<(), String> {
    let mut file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(0o600)
        .custom_flags(libc::O_CLOEXEC | libc::O_NOFOLLOW)
        .open(path)
        .map_err(|error| error.to_string())?;
    file.write_all(bytes)
        .and_then(|()| file.sync_all())
        .map_err(|error| error.to_string())
}
