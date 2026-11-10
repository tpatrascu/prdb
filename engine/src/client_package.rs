//! Client-side bounded TinyCC packaging for submission artifacts.

use crate::submission_manifest::{ManifestError, encode_manifest_v1, manifest_v1_from_elf};
use crate::submission_scope::decode_requested_scope_v1;
use sha2::{Digest, Sha256};
use std::ffi::CString;
use std::fs::{self, File, OpenOptions};
use std::io::{self, Read, Write};
use std::os::unix::ffi::OsStrExt;
use std::os::unix::fs::{OpenOptionsExt, PermissionsExt};
use std::os::unix::process::CommandExt;
use std::path::{Path, PathBuf};
use std::process::{Command, ExitStatus, Stdio};
use std::thread;
use std::time::{Duration, Instant};

pub const MAX_SOURCE_BYTES: u64 = 1024 * 1024;
pub const MAX_SDK_ARCHIVE_BYTES: u64 = 128 * 1024 * 1024;
pub const COMPILER_TIMEOUT: Duration = Duration::from_secs(60);
const MAX_COMPILER_DIAGNOSTIC_BYTES: usize = 8192;
const COMPILER_POLL_INTERVAL: Duration = Duration::from_millis(10);

#[derive(Clone, Debug)]
pub struct PackageOptions {
    pub tcc: PathBuf,
    pub source: PathBuf,
    pub sdk_archive: PathBuf,
    pub include_dir: PathBuf,
    pub requested_scope: PathBuf,
    pub output_dir: PathBuf,
    pub native_abi_version: u8,
    pub sdk_release: u32,
    pub cpu_baseline: String,
    pub runtime_profile: String,
    pub compiler_identity: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PackageSummary {
    pub package_dir: PathBuf,
    pub artifact_bytes: u64,
    pub artifact_sha256: [u8; 32],
}

#[derive(Debug)]
pub enum PackageError {
    InvalidConfiguration,
    InputTooLarge,
    InvalidScope,
    CompilerTimeout,
    CompilerFailed {
        status: ExitStatus,
        diagnostic: String,
    },
    AtomicPublishUnsupported,
    Manifest(ManifestError),
    Io(io::Error),
}

impl std::fmt::Display for PackageError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "query packaging failed: {self:?}")
    }
}

impl std::error::Error for PackageError {}

impl From<io::Error> for PackageError {
    fn from(error: io::Error) -> Self {
        Self::Io(error)
    }
}

impl From<ManifestError> for PackageError {
    fn from(error: ManifestError) -> Self {
        Self::Manifest(error)
    }
}

/// Compile one source with the prebuilt SDK archive, derive its manifest from
/// ELF data, and atomically publish query.so plus PMF1, PSC1 and a SHA-256
/// sidecar. The compiler gets no inherited environment, and the query library
/// is never loaded or executed here.
pub fn package_query(options: &PackageOptions) -> Result<PackageSummary, PackageError> {
    let source = canonical_regular_file(&options.source, MAX_SOURCE_BYTES)?;
    let sdk_archive = canonical_regular_file(&options.sdk_archive, MAX_SDK_ARCHIVE_BYTES)?;
    let tcc = canonical_compiler(&options.tcc)?;
    let include_dir = fs::canonicalize(&options.include_dir)?;
    if !include_dir.is_dir()
        || !(1..=4).contains(&options.native_abi_version)
        || options.sdk_release == 0
    {
        return Err(PackageError::InvalidConfiguration);
    }
    let scope_path = canonical_regular_file(
        &options.requested_scope,
        crate::supervisor::MAX_SCOPE_BYTES as u64,
    )?;
    let scope = read_bounded_file(&scope_path, crate::supervisor::MAX_SCOPE_BYTES as u64)?;
    decode_requested_scope_v1(&scope).map_err(|_| PackageError::InvalidScope)?;
    let package_dir = output_path(&options.output_dir)?;
    let mut staging = StagingDirectory::create(&package_dir)?;
    let artifact_path = staging.path.join("query.so");

    let source_parent = source.parent().ok_or(PackageError::InvalidConfiguration)?;
    let source_name = source
        .file_name()
        .ok_or(PackageError::InvalidConfiguration)?;
    let archive_arg = sdk_archive.as_os_str();
    let include_arg = include_dir.as_os_str();
    let output_arg = artifact_path.as_os_str();
    let source_arg = source_name;
    run_tinycc(
        &tcc,
        source_parent,
        include_arg,
        source_arg,
        archive_arg,
        output_arg,
    )?;

    let artifact = read_bounded_file(&artifact_path, crate::supervisor::MAX_ARTIFACT_BYTES)?;
    if artifact.is_empty() {
        return Err(PackageError::InvalidConfiguration);
    }
    let manifest = manifest_v1_from_elf(
        &artifact,
        options.native_abi_version,
        options.sdk_release,
        options.cpu_baseline.clone(),
        options.runtime_profile.clone(),
        options.compiler_identity.clone(),
    )?;
    let manifest_bytes = encode_manifest_v1(&manifest)?;
    let artifact_sha256: [u8; 32] = Sha256::digest(&artifact).into();
    fs::set_permissions(
        staging.path.join("query.so"),
        fs::Permissions::from_mode(0o600),
    )?;
    File::open(staging.path.join("query.so"))?.sync_all()?;
    write_private_file(&staging.path.join("manifest.pmf1"), &manifest_bytes)?;
    write_private_file(&staging.path.join("requested_scope.psc1"), &scope)?;
    write_private_file(
        &staging.path.join("artifact.sha256"),
        format!("{}\n", lower_hex(&artifact_sha256)).as_bytes(),
    )?;
    File::open(&staging.path)?.sync_all()?;
    staging.publish(&package_dir)?;

    Ok(PackageSummary {
        package_dir,
        artifact_bytes: artifact.len() as u64,
        artifact_sha256,
    })
}

fn canonical_regular_file(path: &Path, maximum_bytes: u64) -> Result<PathBuf, PackageError> {
    let path = fs::canonicalize(path)?;
    let metadata = fs::metadata(&path)?;
    if !metadata.is_file() {
        return Err(PackageError::InvalidConfiguration);
    }
    if metadata.len() > maximum_bytes {
        return Err(PackageError::InputTooLarge);
    }
    Ok(path)
}

fn canonical_compiler(path: &Path) -> Result<PathBuf, PackageError> {
    let candidate = if path.components().count() > 1 || path.is_absolute() {
        path.to_path_buf()
    } else {
        let search_path = std::env::var_os("PATH").ok_or(PackageError::InvalidConfiguration)?;
        std::env::split_paths(&search_path)
            .map(|directory| directory.join(path))
            .find(|candidate| candidate.is_file())
            .ok_or(PackageError::InvalidConfiguration)?
    };
    let candidate = fs::canonicalize(candidate)?;
    let metadata = fs::metadata(&candidate)?;
    if !metadata.is_file() || metadata.permissions().mode() & 0o111 == 0 {
        return Err(PackageError::InvalidConfiguration);
    }
    Ok(candidate)
}

fn read_bounded_file(path: &Path, maximum_bytes: u64) -> Result<Vec<u8>, PackageError> {
    let mut file = File::open(path)?;
    let mut bytes = Vec::new();
    Read::by_ref(&mut file)
        .take(maximum_bytes.saturating_add(1))
        .read_to_end(&mut bytes)?;
    if bytes.len() as u64 > maximum_bytes {
        return Err(PackageError::InputTooLarge);
    }
    Ok(bytes)
}

fn run_tinycc(
    tcc: &Path,
    working_directory: &Path,
    include_dir: &std::ffi::OsStr,
    source: &std::ffi::OsStr,
    sdk_archive: &std::ffi::OsStr,
    output: &std::ffi::OsStr,
) -> Result<(), PackageError> {
    let mut command = Command::new(tcc);
    command
        .current_dir(working_directory)
        .env_clear()
        .arg("-Wall")
        .arg("-Wextra")
        .arg("-Werror")
        .arg("-shared")
        .arg("-I")
        .arg(include_dir)
        .arg(source)
        .arg(sdk_archive)
        .arg("-o")
        .arg(output)
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    // Bound compiler CPU, memory, file output, core dumps and descriptor use
    // independently of the wall-clock timeout below.
    // SAFETY: the pre-exec closure uses only async-signal-safe setrlimit calls.
    unsafe {
        command.pre_exec(|| {
            let limits = [
                (libc::RLIMIT_CPU, 65 as libc::rlim_t),
                (libc::RLIMIT_AS, 2 * 1024 * 1024 * 1024u64 as libc::rlim_t),
                (
                    libc::RLIMIT_FSIZE,
                    crate::supervisor::MAX_ARTIFACT_BYTES as libc::rlim_t,
                ),
                (libc::RLIMIT_CORE, 0 as libc::rlim_t),
                (libc::RLIMIT_NOFILE, 64 as libc::rlim_t),
            ];
            for (resource, maximum) in limits {
                let limit = libc::rlimit {
                    rlim_cur: maximum,
                    rlim_max: maximum,
                };
                if libc::setrlimit(resource, &limit) != 0 {
                    return Err(io::Error::last_os_error());
                }
            }
            Ok(())
        });
        command.process_group(0);
    }
    let mut child = command.spawn()?;
    let stdout = child
        .stdout
        .take()
        .ok_or(PackageError::InvalidConfiguration)?;
    let stderr = child
        .stderr
        .take()
        .ok_or(PackageError::InvalidConfiguration)?;
    let stdout_reader = thread::spawn(move || drain_bounded(stdout));
    let stderr_reader = thread::spawn(move || drain_bounded(stderr));
    let deadline = Instant::now() + COMPILER_TIMEOUT;
    let status = loop {
        if let Some(status) = child.try_wait()? {
            break status;
        }
        if Instant::now() >= deadline {
            // Kill the compiler process group too, so a wrapper or compiler
            // helper cannot outlive the bounded compilation request.
            // SAFETY: the negative PID names the process group assigned above.
            unsafe {
                libc::kill(-(child.id() as libc::pid_t), libc::SIGKILL);
            }
            let _ = child.wait();
            let _ = stdout_reader.join();
            let _ = stderr_reader.join();
            return Err(PackageError::CompilerTimeout);
        }
        thread::sleep(COMPILER_POLL_INTERVAL);
    };
    let _ = stdout_reader.join();
    let diagnostic = stderr_reader
        .join()
        .unwrap_or_default()
        .into_iter()
        .map(char::from)
        .collect::<String>();
    if !status.success() {
        return Err(PackageError::CompilerFailed { status, diagnostic });
    }
    Ok(())
}

fn drain_bounded(mut reader: impl Read) -> Vec<u8> {
    let mut captured = Vec::with_capacity(MAX_COMPILER_DIAGNOSTIC_BYTES);
    let mut buffer = [0u8; 2048];
    while let Ok(read) = reader.read(&mut buffer) {
        if read == 0 {
            break;
        }
        let remaining = MAX_COMPILER_DIAGNOSTIC_BYTES.saturating_sub(captured.len());
        captured.extend_from_slice(&buffer[..read.min(remaining)]);
    }
    captured
}

fn output_path(requested: &Path) -> Result<PathBuf, PackageError> {
    let file_name = requested
        .file_name()
        .ok_or(PackageError::InvalidConfiguration)?;
    let parent = requested
        .parent()
        .filter(|path| !path.as_os_str().is_empty())
        .unwrap_or(Path::new("."));
    let parent = fs::canonicalize(parent)?;
    if !parent.is_dir() {
        return Err(PackageError::InvalidConfiguration);
    }
    Ok(parent.join(file_name))
}

struct StagingDirectory {
    path: PathBuf,
    published: bool,
}

impl StagingDirectory {
    fn create(final_path: &Path) -> Result<Self, PackageError> {
        if fs::symlink_metadata(final_path).is_ok() {
            return Err(PackageError::InvalidConfiguration);
        }
        let parent = final_path
            .parent()
            .ok_or(PackageError::InvalidConfiguration)?;
        let name = final_path
            .file_name()
            .ok_or(PackageError::InvalidConfiguration)?
            .to_string_lossy();
        for _ in 0..8 {
            let mut random = [0u8; 12];
            getrandom::fill(&mut random).map_err(|_| PackageError::InvalidConfiguration)?;
            let path = parent.join(format!(".{name}.tmp-{}", lower_hex(&random)));
            match fs::create_dir(&path) {
                Ok(()) => {
                    if let Err(error) =
                        fs::set_permissions(&path, fs::Permissions::from_mode(0o700))
                    {
                        let _ = fs::remove_dir(&path);
                        return Err(PackageError::Io(error));
                    }
                    return Ok(Self {
                        path,
                        published: false,
                    });
                }
                Err(error) if error.kind() == io::ErrorKind::AlreadyExists => continue,
                Err(error) => return Err(PackageError::Io(error)),
            }
        }
        Err(PackageError::InvalidConfiguration)
    }

    fn publish(&mut self, final_path: &Path) -> Result<(), PackageError> {
        let source = CString::new(self.path.as_os_str().as_bytes())
            .map_err(|_| PackageError::InvalidConfiguration)?;
        let destination = CString::new(final_path.as_os_str().as_bytes())
            .map_err(|_| PackageError::InvalidConfiguration)?;
        // RENAME_NOREPLACE prevents a concurrent caller from replacing an
        // existing package path after the earlier symlink_metadata check.
        // SAFETY: both C strings remain live for the syscall, and AT_FDCWD
        // resolves their absolute paths in the current process namespace.
        let result = unsafe {
            libc::syscall(
                libc::SYS_renameat2,
                libc::AT_FDCWD,
                source.as_ptr(),
                libc::AT_FDCWD,
                destination.as_ptr(),
                libc::RENAME_NOREPLACE,
            )
        };
        if result != 0 {
            let error = io::Error::last_os_error();
            if error.raw_os_error() == Some(libc::EINVAL) {
                return Err(PackageError::AtomicPublishUnsupported);
            }
            return Err(PackageError::Io(error));
        }
        self.published = true;
        Ok(())
    }
}

impl Drop for StagingDirectory {
    fn drop(&mut self) {
        if !self.published {
            let _ = fs::remove_dir_all(&self.path);
        }
    }
}

fn write_private_file(path: &Path, contents: &[u8]) -> Result<(), PackageError> {
    let mut file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(0o600)
        .open(path)?;
    file.write_all(contents)?;
    file.sync_all()?;
    Ok(())
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
