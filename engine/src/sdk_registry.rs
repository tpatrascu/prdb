//! Durable administrator-managed repository for immutable SQL SDK releases.

use crate::submission_manifest::{ArtifactManifestV1, decode_elf_dynamic_info};
use sha2::{Digest, Sha256};
use std::collections::{HashMap, HashSet};
use std::fs::{self, File, OpenOptions};
use std::io::{self, Read, Seek, SeekFrom, Write};
use std::os::unix::fs::{OpenOptionsExt, PermissionsExt};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, RwLock};
use std::time::{Duration, Instant};

const MAX_RELEASE_LIBRARY_BYTES: u64 = 128 * 1024 * 1024;
const MAX_RELEASE_METADATA_BYTES: u64 = 64 * 1024;
const MAX_PENDING_SDK_UPLOADS: usize = 8;
const SDK_UPLOAD_TTL: Duration = Duration::from_secs(15 * 60);
const SDK_OPERATION_RECORD_BYTES: usize = 65;

#[derive(Clone, Debug, Eq, PartialEq)]
struct ReleaseRecord {
    sdk_release: u32,
    soname: String,
    sha256: [u8; 32],
    cpu_baseline: String,
    runtime_profile: String,
    native_abis: Vec<u8>,
    needed: Vec<String>,
}

#[derive(Debug)]
struct RepositoryRelease {
    record: ReleaseRecord,
    library_path: PathBuf,
    library_bytes: u64,
    pins: AtomicUsize,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SdkReleaseInfo {
    pub sdk_release: u32,
    pub native_abis: Vec<u8>,
    pub target: String,
    pub cpu_baseline: String,
    pub runtime_profile: String,
    pub soname: String,
    pub sha256: [u8; 32],
    pub needed: Vec<String>,
    pub library_bytes: u64,
}

/// A strong reference keeps the immutable repository release from removal.
#[derive(Debug)]
pub struct SdkLibraryPin(Arc<RepositoryRelease>);

impl Clone for SdkLibraryPin {
    fn clone(&self) -> Self {
        self.0.pins.fetch_add(1, Ordering::AcqRel);
        Self(Arc::clone(&self.0))
    }
}

impl Drop for SdkLibraryPin {
    fn drop(&mut self) {
        self.0.pins.fetch_sub(1, Ordering::AcqRel);
    }
}

impl SdkLibraryPin {
    pub fn library_path(&self) -> &Path {
        &self.0.library_path
    }

    pub fn soname(&self) -> &str {
        &self.0.record.soname
    }

    pub fn sha256(&self) -> [u8; 32] {
        self.0.record.sha256
    }
}

/// A startup-validated registry for one target and runtime compatibility
/// profile. Lookups are read-only and return pins that protect files in use.
#[derive(Debug)]
pub struct SdkLibraryRepository {
    repository_root: PathBuf,
    target: String,
    cpu_baseline: String,
    runtime_profile: String,
    allowed_dependencies: HashSet<String>,
    max_repository_bytes: u64,
    releases: RwLock<HashMap<(u32, u8), Arc<RepositoryRelease>>>,
    uploads: Mutex<HashMap<[u8; 16], SdkUpload>>,
    completed_operations: RwLock<HashMap<[u8; 16], SdkLibraryOperationOutcome>>,
    operation_gate: Mutex<()>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum SdkLibraryOperationOutcome {
    Installed {
        owner_id: [u8; 16],
        sdk_release: u32,
        sha256: [u8; 32],
        library_bytes: u64,
    },
    Removed {
        owner_id: [u8; 16],
        sdk_release: u32,
        removed_bytes: u64,
    },
}

#[derive(Debug)]
struct SdkUpload {
    owner_id: [u8; 16],
    metadata_bytes: u64,
    library_bytes: u64,
    library_sha256: [u8; 32],
    allowed_dependencies: HashSet<String>,
    next_offset: u64,
    path: PathBuf,
    expires_at: Instant,
}

/// Durable administrator-managed repository for immutable SDK releases.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum SdkRegistryError {
    InvalidConfiguration,
    InvalidMetadata,
    IncompatibleProfile,
    UnavailableRelease,
    DigestMismatch,
    RepositoryFull,
    AtomicPublishUnsupported,
    ReleaseConflict,
    ReleasePinned,
    DependencyNotAllowed,
    UploadUnavailable,
    UploadOwnerMismatch,
    UploadIncomplete,
    UploadConflict,
    Io(io::ErrorKind),
}

impl From<io::Error> for SdkRegistryError {
    fn from(error: io::Error) -> Self {
        Self::Io(error.kind())
    }
}

impl SdkLibraryRepository {
    /// Open an instance-local durable repository and validate every published
    /// release before making it available for resolution.
    pub fn open(
        repository_root: &Path,
        target: String,
        cpu_baseline: String,
        runtime_profile: String,
        allowed_dependencies: HashSet<String>,
        max_repository_bytes: u64,
    ) -> Result<Self, SdkRegistryError> {
        if max_repository_bytes == 0
            || target != "linux-x86_64"
            || !is_profile_token(&cpu_baseline)
            || !is_profile_token(&runtime_profile)
        {
            return Err(SdkRegistryError::InvalidConfiguration);
        }
        let repository_root = prepare_repository_root(repository_root)?;
        let mut releases = HashMap::new();
        let mut completed_operations = HashMap::new();
        let mut repository_bytes = 0u64;
        for item in fs::read_dir(&repository_root)? {
            let item = item?;
            let name = item.file_name();
            let name = name.to_str().ok_or(SdkRegistryError::InvalidMetadata)?;
            let path = item.path();
            let file_type = item.file_type()?;
            if name.starts_with(".install-") {
                if !file_type.is_dir() {
                    return Err(SdkRegistryError::InvalidMetadata);
                }
                fs::set_permissions(&path, fs::Permissions::from_mode(0o700))?;
                fs::remove_dir_all(path)?;
                continue;
            }
            if name.starts_with(".upload-") {
                if !file_type.is_file() {
                    return Err(SdkRegistryError::InvalidMetadata);
                }
                fs::remove_file(path)?;
                continue;
            }
            if let Some(operation_id) = name.strip_prefix(".operation-tmp-") {
                if decode_lower_hex_id(operation_id).is_none() || !file_type.is_file() {
                    return Err(SdkRegistryError::InvalidMetadata);
                }
                fs::remove_file(path)?;
                continue;
            }
            if let Some(operation_id) = name.strip_prefix(".operation-") {
                let operation_id =
                    decode_lower_hex_id(operation_id).ok_or(SdkRegistryError::InvalidMetadata)?;
                if !file_type.is_file() {
                    return Err(SdkRegistryError::InvalidMetadata);
                }
                let outcome = read_operation_outcome(&path)?;
                if completed_operations.insert(operation_id, outcome).is_some() {
                    return Err(SdkRegistryError::InvalidMetadata);
                }
                continue;
            }
            if !name.starts_with("release-") || !file_type.is_dir() {
                return Err(SdkRegistryError::InvalidMetadata);
            }
            let directory_release = name["release-".len()..]
                .parse::<u32>()
                .map_err(|_| SdkRegistryError::InvalidMetadata)?;
            let children = fs::read_dir(&path)?
                .map(|child| {
                    child
                        .map(|child| child.file_name())
                        .map_err(SdkRegistryError::from)
                })
                .collect::<Result<HashSet<_>, _>>()?;
            let expected_children = HashSet::from([
                std::ffi::OsString::from("sdk-release.json"),
                std::ffi::OsString::from(format!("libprdb_sql.so.{directory_release}")),
            ]);
            if children != expected_children {
                return Err(SdkRegistryError::InvalidMetadata);
            }
            validate_immutable_permissions(&path)?;
            let metadata =
                read_regular(&path.join("sdk-release.json"), MAX_RELEASE_METADATA_BYTES)?;
            let record = parse_release_record(&metadata)?;
            if directory_release != record.sdk_release
                || record.cpu_baseline != cpu_baseline
                || record.runtime_profile != runtime_profile
                || record
                    .needed
                    .iter()
                    .any(|dependency| !allowed_dependencies.contains(dependency))
            {
                return Err(SdkRegistryError::IncompatibleProfile);
            }
            let library_path = path.join(&record.soname);
            validate_immutable_permissions(&path.join("sdk-release.json"))?;
            validate_immutable_permissions(&library_path)?;
            let library = read_regular(&library_path, MAX_RELEASE_LIBRARY_BYTES)?;
            validate_library(&record, &library)?;
            if sha256(&library) != record.sha256 {
                return Err(SdkRegistryError::DigestMismatch);
            }
            repository_bytes = repository_bytes
                .checked_add(library.len() as u64)
                .ok_or(SdkRegistryError::RepositoryFull)?;
            if repository_bytes > max_repository_bytes {
                return Err(SdkRegistryError::RepositoryFull);
            }
            let entry = Arc::new(RepositoryRelease {
                record: record.clone(),
                library_path,
                library_bytes: library.len() as u64,
                pins: AtomicUsize::new(0),
            });
            insert_release_abis(&mut releases, entry)?;
        }
        File::open(&repository_root)?.sync_all()?;
        Ok(Self {
            repository_root: repository_root.clone(),
            target,
            cpu_baseline,
            runtime_profile,
            allowed_dependencies,
            max_repository_bytes,
            releases: RwLock::new(releases),
            uploads: Mutex::new(HashMap::new()),
            completed_operations: RwLock::new(completed_operations),
            operation_gate: Mutex::new(()),
        })
    }

    /// Resolve and pin the release described by a validated PMF1 manifest.
    /// The artifact dependency list must include this exact SONAME and all
    /// dependencies recorded for the SDK shared object.
    pub fn resolve(
        &self,
        manifest: &ArtifactManifestV1,
    ) -> Result<SdkLibraryPin, SdkRegistryError> {
        if manifest.cpu_baseline != self.cpu_baseline
            || manifest.runtime_profile != self.runtime_profile
        {
            return Err(SdkRegistryError::IncompatibleProfile);
        }
        let releases = self
            .releases
            .read()
            .map_err(|_| SdkRegistryError::InvalidConfiguration)?;
        let release = releases
            .get(&(manifest.sdk_release, manifest.native_abi_version))
            .ok_or(SdkRegistryError::UnavailableRelease)?;
        if self.target != "linux-x86_64"
            || !manifest
                .dependencies
                .iter()
                .any(|name| name == &release.record.soname)
            || release
                .record
                .needed
                .iter()
                .any(|dependency| !manifest.dependencies.contains(dependency))
        {
            return Err(SdkRegistryError::IncompatibleProfile);
        }
        let library = read_regular(&release.library_path, MAX_RELEASE_LIBRARY_BYTES)?;
        if sha256(&library) != release.record.sha256 {
            return Err(SdkRegistryError::DigestMismatch);
        }
        validate_library(&release.record, &library)?;
        release.pins.fetch_add(1, Ordering::AcqRel);
        Ok(SdkLibraryPin(Arc::clone(release)))
    }

    /// Validate and atomically install one immutable release. Metadata and
    /// library bytes are supplied by the authenticated repository API.
    pub fn install(
        &self,
        metadata: &[u8],
        library: &[u8],
        allowed_dependencies: &HashSet<String>,
    ) -> Result<SdkReleaseInfo, SdkRegistryError> {
        if library.is_empty()
            || library.len() as u64 > MAX_RELEASE_LIBRARY_BYTES
            || metadata.is_empty()
            || metadata.len() as u64 > MAX_RELEASE_METADATA_BYTES
        {
            return Err(SdkRegistryError::InvalidMetadata);
        }
        let record = parse_release_record(metadata)?;
        if record.cpu_baseline != self.cpu_baseline
            || record.runtime_profile != self.runtime_profile
            || self.target != "linux-x86_64"
        {
            return Err(SdkRegistryError::IncompatibleProfile);
        }
        if record
            .needed
            .iter()
            .any(|dependency| !allowed_dependencies.contains(dependency))
        {
            return Err(SdkRegistryError::DependencyNotAllowed);
        }
        if sha256(library) != record.sha256 {
            return Err(SdkRegistryError::DigestMismatch);
        }
        validate_library(&record, library)?;

        let mut releases = self
            .releases
            .write()
            .map_err(|_| SdkRegistryError::InvalidConfiguration)?;
        if let Some(existing) = releases
            .values()
            .find(|entry| entry.record.sdk_release == record.sdk_release)
        {
            if existing.record == record && existing.library_bytes == library.len() as u64 {
                let installed = read_regular(&existing.library_path, MAX_RELEASE_LIBRARY_BYTES)?;
                if sha256(&installed) != record.sha256 {
                    return Err(SdkRegistryError::DigestMismatch);
                }
                return Ok(release_info(&record, library.len() as u64));
            }
            return Err(SdkRegistryError::ReleaseConflict);
        }
        let mut unique_releases = HashMap::<u32, u64>::new();
        for entry in releases.values() {
            unique_releases
                .entry(entry.record.sdk_release)
                .or_insert(entry.library_bytes);
        }
        let current_bytes = unique_releases
            .values()
            .try_fold(0u64, |total, bytes| total.checked_add(*bytes))
            .ok_or(SdkRegistryError::RepositoryFull)?;
        let next_bytes = current_bytes
            .checked_add(library.len() as u64)
            .ok_or(SdkRegistryError::RepositoryFull)?;
        if next_bytes > self.max_repository_bytes {
            return Err(SdkRegistryError::RepositoryFull);
        }

        let destination = self
            .repository_root
            .join(format!("release-{}", record.sdk_release));
        let published = publish_repository_entry(
            &self.repository_root,
            &destination,
            metadata,
            library,
            &record,
        )?;
        let entry = Arc::new(RepositoryRelease {
            record: record.clone(),
            library_path: published,
            library_bytes: library.len() as u64,
            pins: AtomicUsize::new(0),
        });
        insert_release_abis(&mut releases, entry)?;
        Ok(release_info(&record, library.len() as u64))
    }

    /// Reserve a bounded on-disk upload for one authenticated administrator.
    /// Metadata and SDK bytes arrive as one contiguous stream and are parsed
    /// only after the declared byte count has been received.
    pub fn begin_install_upload(
        &self,
        operation_id: [u8; 16],
        owner_id: [u8; 16],
        metadata_bytes: u64,
        library_bytes: u64,
        library_sha256: [u8; 32],
    ) -> Result<u64, SdkRegistryError> {
        let total_bytes = metadata_bytes
            .checked_add(library_bytes)
            .ok_or(SdkRegistryError::InvalidMetadata)?;
        if operation_id == [0; 16]
            || owner_id == [0; 16]
            || metadata_bytes == 0
            || metadata_bytes > MAX_RELEASE_METADATA_BYTES
            || library_bytes == 0
            || library_bytes > MAX_RELEASE_LIBRARY_BYTES
        {
            return Err(SdkRegistryError::InvalidMetadata);
        }
        if let Some(outcome) = self.operation_outcome(operation_id)? {
            return match outcome {
                SdkLibraryOperationOutcome::Installed {
                    owner_id: outcome_owner,
                    library_bytes: outcome_bytes,
                    sha256,
                    ..
                } if outcome_owner == owner_id
                    && outcome_bytes == library_bytes
                    && sha256 == library_sha256 =>
                {
                    Ok(total_bytes)
                }
                SdkLibraryOperationOutcome::Installed {
                    owner_id: outcome_owner,
                    ..
                } if outcome_owner != owner_id => Err(SdkRegistryError::UploadOwnerMismatch),
                _ => Err(SdkRegistryError::UploadConflict),
            };
        }
        let mut uploads = self
            .uploads
            .lock()
            .map_err(|_| SdkRegistryError::InvalidConfiguration)?;
        let now = Instant::now();
        let expired = uploads
            .iter()
            .filter_map(|(id, upload)| (upload.expires_at <= now).then_some(*id))
            .collect::<Vec<_>>();
        for id in expired {
            if let Some(upload) = uploads.remove(&id) {
                let _ = fs::remove_file(upload.path);
            }
        }
        if let Some(upload) = uploads.get(&operation_id) {
            if upload.owner_id == owner_id
                && upload.metadata_bytes == metadata_bytes
                && upload.library_bytes == library_bytes
                && upload.library_sha256 == library_sha256
                && upload.allowed_dependencies == self.allowed_dependencies
            {
                return Ok(upload.next_offset);
            }
            return Err(SdkRegistryError::UploadConflict);
        }
        if uploads.len() >= MAX_PENDING_SDK_UPLOADS {
            return Err(SdkRegistryError::RepositoryFull);
        }
        let pending_bytes = uploads.values().try_fold(0u64, |total, upload| {
            total
                .checked_add(upload.metadata_bytes)?
                .checked_add(upload.library_bytes)
        });
        if pending_bytes
            .and_then(|pending| pending.checked_add(total_bytes))
            .is_none_or(|pending| pending > self.max_repository_bytes)
        {
            return Err(SdkRegistryError::RepositoryFull);
        }
        let path = self
            .repository_root
            .join(format!(".upload-{}", lower_hex(&operation_id)));
        OpenOptions::new()
            .write(true)
            .create_new(true)
            .mode(0o600)
            .custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC)
            .open(&path)?;
        File::open(&self.repository_root)?.sync_all()?;
        uploads.insert(
            operation_id,
            SdkUpload {
                owner_id,
                metadata_bytes,
                library_bytes,
                library_sha256,
                allowed_dependencies: self.allowed_dependencies.clone(),
                next_offset: 0,
                path,
                expires_at: now + SDK_UPLOAD_TTL,
            },
        );
        Ok(0)
    }

    /// Append the next contiguous SDK upload chunk and acknowledge its offset.
    pub fn upload_install_chunk(
        &self,
        operation_id: [u8; 16],
        owner_id: [u8; 16],
        offset: u64,
        data: &[u8],
    ) -> Result<u64, SdkRegistryError> {
        if data.is_empty() || data.len() > 256 * 1024 {
            return Err(SdkRegistryError::InvalidMetadata);
        }
        let mut uploads = self
            .uploads
            .lock()
            .map_err(|_| SdkRegistryError::InvalidConfiguration)?;
        let upload = uploads
            .get_mut(&operation_id)
            .ok_or(SdkRegistryError::UploadUnavailable)?;
        if upload.owner_id != owner_id {
            return Err(SdkRegistryError::UploadOwnerMismatch);
        }
        if upload.expires_at <= Instant::now() {
            let path = uploads
                .remove(&operation_id)
                .ok_or(SdkRegistryError::UploadUnavailable)?
                .path;
            let _ = fs::remove_file(path);
            return Err(SdkRegistryError::UploadUnavailable);
        }
        if offset != upload.next_offset {
            return Err(SdkRegistryError::InvalidMetadata);
        }
        let total_bytes = upload.metadata_bytes + upload.library_bytes;
        let next_offset = offset
            .checked_add(u64::try_from(data.len()).map_err(|_| SdkRegistryError::InvalidMetadata)?)
            .filter(|next| *next <= total_bytes)
            .ok_or(SdkRegistryError::InvalidMetadata)?;
        let mut file = OpenOptions::new()
            .write(true)
            .custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC)
            .open(&upload.path)?;
        file.seek(SeekFrom::Start(offset))?;
        file.write_all(data)?;
        file.sync_data()?;
        upload.next_offset = next_offset;
        upload.expires_at = Instant::now() + SDK_UPLOAD_TTL;
        Ok(next_offset)
    }

    /// Validate and publish one complete streamed SDK upload.
    pub fn finish_install_upload(
        &self,
        operation_id: [u8; 16],
        owner_id: [u8; 16],
    ) -> Result<SdkReleaseInfo, SdkRegistryError> {
        let _operation_guard = self
            .operation_gate
            .lock()
            .map_err(|_| SdkRegistryError::InvalidConfiguration)?;
        if let Some(outcome) = self.operation_outcome(operation_id)? {
            return match outcome {
                SdkLibraryOperationOutcome::Installed {
                    owner_id: outcome_owner,
                    sdk_release,
                    sha256,
                    library_bytes,
                } if outcome_owner == owner_id => {
                    let releases = self
                        .releases
                        .read()
                        .map_err(|_| SdkRegistryError::InvalidConfiguration)?;
                    let entry = releases
                        .values()
                        .find(|entry| entry.record.sdk_release == sdk_release)
                        .ok_or(SdkRegistryError::UnavailableRelease)?;
                    if entry.record.sha256 != sha256 || entry.library_bytes != library_bytes {
                        return Err(SdkRegistryError::ReleaseConflict);
                    }
                    Ok(release_info(&entry.record, library_bytes))
                }
                _ => Err(SdkRegistryError::UploadOwnerMismatch),
            };
        }
        let upload = {
            let mut uploads = self
                .uploads
                .lock()
                .map_err(|_| SdkRegistryError::InvalidConfiguration)?;
            let upload = uploads
                .get(&operation_id)
                .ok_or(SdkRegistryError::UploadUnavailable)?;
            if upload.owner_id != owner_id {
                return Err(SdkRegistryError::UploadOwnerMismatch);
            }
            let total_bytes = upload.metadata_bytes + upload.library_bytes;
            if upload.next_offset != total_bytes {
                return Err(SdkRegistryError::UploadIncomplete);
            }
            uploads
                .remove(&operation_id)
                .ok_or(SdkRegistryError::UploadUnavailable)?
        };
        let result = (|| {
            let total_bytes = upload.metadata_bytes + upload.library_bytes;
            let bytes = read_regular(&upload.path, total_bytes)?;
            if bytes.len() as u64 != total_bytes {
                return Err(SdkRegistryError::UploadIncomplete);
            }
            let split = usize::try_from(upload.metadata_bytes)
                .map_err(|_| SdkRegistryError::InvalidMetadata)?;
            let (metadata, library) = bytes.split_at(split);
            if sha256(library) != upload.library_sha256 {
                return Err(SdkRegistryError::DigestMismatch);
            }
            self.install(metadata, library, &upload.allowed_dependencies)
        })();
        fs::remove_file(&upload.path)?;
        File::open(&self.repository_root)?.sync_all()?;
        let info = result?;
        let outcome = SdkLibraryOperationOutcome::Installed {
            owner_id,
            sdk_release: info.sdk_release,
            sha256: info.sha256,
            library_bytes: info.library_bytes,
        };
        self.record_operation_outcome(operation_id, outcome)?;
        Ok(info)
    }

    pub fn operation_outcome(
        &self,
        operation_id: [u8; 16],
    ) -> Result<Option<SdkLibraryOperationOutcome>, SdkRegistryError> {
        Ok(self
            .completed_operations
            .read()
            .map_err(|_| SdkRegistryError::InvalidConfiguration)?
            .get(&operation_id)
            .cloned())
    }

    fn record_operation_outcome(
        &self,
        operation_id: [u8; 16],
        outcome: SdkLibraryOperationOutcome,
    ) -> Result<(), SdkRegistryError> {
        let mut outcomes = self
            .completed_operations
            .write()
            .map_err(|_| SdkRegistryError::InvalidConfiguration)?;
        if let Some(existing) = outcomes.get(&operation_id) {
            return if existing == &outcome {
                Ok(())
            } else {
                Err(SdkRegistryError::UploadConflict)
            };
        }
        write_operation_outcome(&self.repository_root, operation_id, &outcome)?;
        outcomes.insert(operation_id, outcome);
        Ok(())
    }

    /// Return bounded compatibility metadata in stable release/ABI order.
    pub fn list(&self) -> Result<Vec<SdkReleaseInfo>, SdkRegistryError> {
        let releases = self
            .releases
            .read()
            .map_err(|_| SdkRegistryError::InvalidConfiguration)?;
        let mut unique = HashMap::<u32, SdkReleaseInfo>::new();
        for entry in releases.values() {
            unique
                .entry(entry.record.sdk_release)
                .or_insert_with(|| release_info(&entry.record, entry.library_bytes));
        }
        let mut result = unique.into_values().collect::<Vec<_>>();
        result.sort_by_key(|entry| entry.sdk_release);
        Ok(result)
    }

    /// Remove a complete SDK release after all ABI pins have drained.
    pub fn remove_release(&self, sdk_release: u32) -> Result<u64, SdkRegistryError> {
        let mut releases = self
            .releases
            .write()
            .map_err(|_| SdkRegistryError::InvalidConfiguration)?;
        let entry = releases
            .values()
            .find(|entry| entry.record.sdk_release == sdk_release)
            .cloned()
            .ok_or(SdkRegistryError::UnavailableRelease)?;
        if entry.pins.load(Ordering::Acquire) != 0 {
            return Err(SdkRegistryError::ReleasePinned);
        }
        let directory = entry
            .library_path
            .parent()
            .ok_or(SdkRegistryError::InvalidConfiguration)?;
        fs::set_permissions(directory, fs::Permissions::from_mode(0o700))?;
        fs::remove_dir_all(directory)?;
        releases.retain(|(release, _), _| *release != sdk_release);
        File::open(&self.repository_root)?.sync_all()?;
        Ok(entry.library_bytes)
    }

    pub fn remove_release_with_operation(
        &self,
        operation_id: [u8; 16],
        owner_id: [u8; 16],
        sdk_release: u32,
    ) -> Result<u64, SdkRegistryError> {
        if operation_id == [0; 16] || owner_id == [0; 16] || sdk_release == 0 {
            return Err(SdkRegistryError::InvalidMetadata);
        }
        let _operation_guard = self
            .operation_gate
            .lock()
            .map_err(|_| SdkRegistryError::InvalidConfiguration)?;
        if let Some(outcome) = self.operation_outcome(operation_id)? {
            return match outcome {
                SdkLibraryOperationOutcome::Removed {
                    owner_id: outcome_owner,
                    sdk_release: outcome_release,
                    removed_bytes,
                } if outcome_owner == owner_id && outcome_release == sdk_release => {
                    Ok(removed_bytes)
                }
                SdkLibraryOperationOutcome::Removed {
                    owner_id: outcome_owner,
                    ..
                } if outcome_owner != owner_id => Err(SdkRegistryError::UploadOwnerMismatch),
                _ => Err(SdkRegistryError::UploadConflict),
            };
        }
        let removed_bytes = self.remove_release(sdk_release)?;
        self.record_operation_outcome(
            operation_id,
            SdkLibraryOperationOutcome::Removed {
                owner_id,
                sdk_release,
                removed_bytes,
            },
        )?;
        Ok(removed_bytes)
    }

    pub fn sonames(&self) -> Result<Vec<String>, SdkRegistryError> {
        let releases = self
            .releases
            .read()
            .map_err(|_| SdkRegistryError::InvalidConfiguration)?;
        let mut names = releases
            .values()
            .map(|entry| entry.record.soname.clone())
            .collect::<Vec<_>>();
        names.sort();
        names.dedup();
        Ok(names)
    }
}

fn prepare_repository_root(path: &Path) -> Result<PathBuf, SdkRegistryError> {
    if path.exists() {
        let metadata = fs::symlink_metadata(path)?;
        if !metadata.file_type().is_dir() {
            return Err(SdkRegistryError::InvalidConfiguration);
        }
    } else {
        fs::create_dir_all(path)?;
    }
    let path = fs::canonicalize(path)?;
    fs::set_permissions(&path, fs::Permissions::from_mode(0o700))?;
    Ok(path)
}

fn validate_immutable_permissions(path: &Path) -> Result<(), SdkRegistryError> {
    let metadata = fs::symlink_metadata(path)?;
    if metadata.permissions().mode() & 0o222 != 0 {
        return Err(SdkRegistryError::InvalidMetadata);
    }
    Ok(())
}

fn insert_release_abis(
    releases: &mut HashMap<(u32, u8), Arc<RepositoryRelease>>,
    entry: Arc<RepositoryRelease>,
) -> Result<(), SdkRegistryError> {
    if releases
        .keys()
        .any(|(release, _)| *release == entry.record.sdk_release)
    {
        return Err(SdkRegistryError::InvalidMetadata);
    }
    for abi in &entry.record.native_abis {
        releases.insert((entry.record.sdk_release, *abi), Arc::clone(&entry));
    }
    Ok(())
}

fn release_info(record: &ReleaseRecord, library_bytes: u64) -> SdkReleaseInfo {
    SdkReleaseInfo {
        sdk_release: record.sdk_release,
        native_abis: record.native_abis.clone(),
        target: "linux-x86_64".to_owned(),
        cpu_baseline: record.cpu_baseline.clone(),
        runtime_profile: record.runtime_profile.clone(),
        soname: record.soname.clone(),
        sha256: record.sha256,
        needed: record.needed.clone(),
        library_bytes,
    }
}

fn publish_repository_entry(
    repository_root: &Path,
    destination: &Path,
    metadata: &[u8],
    library: &[u8],
    record: &ReleaseRecord,
) -> Result<PathBuf, SdkRegistryError> {
    let mut nonce = [0u8; 8];
    getrandom::fill(&mut nonce).map_err(|_| SdkRegistryError::InvalidConfiguration)?;
    let temporary = repository_root.join(format!(
        ".install-{}-{}",
        std::process::id(),
        lower_hex(&nonce)
    ));
    fs::create_dir(&temporary)?;
    let staged_library = temporary.join(&record.soname);
    let staged_metadata = temporary.join("sdk-release.json");
    let result = (|| {
        write_new_synced(&staged_metadata, metadata)?;
        write_new_synced(&staged_library, library)?;
        if sha256(&read_regular(&staged_library, MAX_RELEASE_LIBRARY_BYTES)?) != record.sha256 {
            return Err(SdkRegistryError::DigestMismatch);
        }
        validate_library(
            record,
            &read_regular(&staged_library, MAX_RELEASE_LIBRARY_BYTES)?,
        )?;
        File::open(&temporary)?.sync_all()?;
        fs::set_permissions(&staged_metadata, fs::Permissions::from_mode(0o444))?;
        fs::set_permissions(&staged_library, fs::Permissions::from_mode(0o444))?;
        fs::set_permissions(&temporary, fs::Permissions::from_mode(0o500))?;
        File::open(&temporary)?.sync_all()?;
        rename_noreplace(&temporary, destination)?;
        File::open(repository_root)?.sync_all()?;
        Ok(destination.join(&record.soname))
    })();
    if result.is_err() {
        let _ = fs::set_permissions(&temporary, fs::Permissions::from_mode(0o700));
        let _ = fs::remove_dir_all(&temporary);
    }
    result
}

fn write_new_synced(path: &Path, bytes: &[u8]) -> Result<(), SdkRegistryError> {
    let mut file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(0o600)
        .open(path)?;
    file.write_all(bytes)?;
    file.sync_all()?;
    Ok(())
}

fn write_operation_outcome(
    root: &Path,
    operation_id: [u8; 16],
    outcome: &SdkLibraryOperationOutcome,
) -> Result<(), SdkRegistryError> {
    let mut bytes = Vec::with_capacity(SDK_OPERATION_RECORD_BYTES);
    bytes.extend_from_slice(b"SDKO");
    let (kind, owner_id, sdk_release, count, digest) = match outcome {
        SdkLibraryOperationOutcome::Installed {
            owner_id,
            sdk_release,
            sha256,
            library_bytes,
        } => (1, owner_id, sdk_release, library_bytes, sha256),
        SdkLibraryOperationOutcome::Removed {
            owner_id,
            sdk_release,
            removed_bytes,
        } => (2, owner_id, sdk_release, removed_bytes, &[0; 32]),
    };
    bytes.push(kind);
    bytes.extend_from_slice(owner_id);
    bytes.extend_from_slice(&sdk_release.to_be_bytes());
    bytes.extend_from_slice(&count.to_be_bytes());
    bytes.extend_from_slice(digest);
    let temporary = root.join(format!(".operation-tmp-{}", lower_hex(&operation_id)));
    let destination = root.join(format!(".operation-{}", lower_hex(&operation_id)));
    write_new_synced(&temporary, &bytes)?;
    rename_noreplace(&temporary, &destination)?;
    File::open(root)?.sync_all()?;
    Ok(())
}

fn read_operation_outcome(path: &Path) -> Result<SdkLibraryOperationOutcome, SdkRegistryError> {
    let bytes = read_regular(path, SDK_OPERATION_RECORD_BYTES as u64)?;
    if bytes.len() != SDK_OPERATION_RECORD_BYTES || &bytes[..4] != b"SDKO" {
        return Err(SdkRegistryError::InvalidMetadata);
    }
    let owner_id: [u8; 16] = bytes[5..21]
        .try_into()
        .map_err(|_| SdkRegistryError::InvalidMetadata)?;
    let sdk_release = u32::from_be_bytes(
        bytes[21..25]
            .try_into()
            .map_err(|_| SdkRegistryError::InvalidMetadata)?,
    );
    let count = u64::from_be_bytes(
        bytes[25..33]
            .try_into()
            .map_err(|_| SdkRegistryError::InvalidMetadata)?,
    );
    let digest: [u8; 32] = bytes[33..65]
        .try_into()
        .map_err(|_| SdkRegistryError::InvalidMetadata)?;
    if owner_id == [0; 16] || sdk_release == 0 || count == 0 {
        return Err(SdkRegistryError::InvalidMetadata);
    }
    match bytes[4] {
        1 if digest != [0; 32] => Ok(SdkLibraryOperationOutcome::Installed {
            owner_id,
            sdk_release,
            sha256: digest,
            library_bytes: count,
        }),
        2 if digest == [0; 32] => Ok(SdkLibraryOperationOutcome::Removed {
            owner_id,
            sdk_release,
            removed_bytes: count,
        }),
        _ => Err(SdkRegistryError::InvalidMetadata),
    }
}

fn decode_lower_hex_id(value: &str) -> Option<[u8; 16]> {
    if value.len() != 32
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    {
        return None;
    }
    let mut id = [0; 16];
    for (index, byte) in id.iter_mut().enumerate() {
        *byte = u8::from_str_radix(&value[index * 2..index * 2 + 2], 16).ok()?;
    }
    Some(id)
}

fn read_regular(path: &Path, limit: u64) -> Result<Vec<u8>, SdkRegistryError> {
    let metadata = fs::symlink_metadata(path)?;
    if !metadata.file_type().is_file() || metadata.len() > limit {
        return Err(SdkRegistryError::InvalidMetadata);
    }
    let file = File::open(path)?;
    let mut bytes = Vec::new();
    file.take(limit.saturating_add(1)).read_to_end(&mut bytes)?;
    if bytes.len() as u64 > limit {
        return Err(SdkRegistryError::InvalidMetadata);
    }
    Ok(bytes)
}

fn parse_release_record(bytes: &[u8]) -> Result<ReleaseRecord, SdkRegistryError> {
    let text = std::str::from_utf8(bytes).map_err(|_| SdkRegistryError::InvalidMetadata)?;
    if !text.is_ascii() || !text.ends_with('\n') {
        return Err(SdkRegistryError::InvalidMetadata);
    }
    let sdk_release = json_integer(text, "sdk_release")?;
    let format = json_string(text, "format")?;
    let library_name = json_string(text, "library")?;
    let target = json_string(text, "target")?;
    let digest_text = json_string(text, "sha256")?;
    let record = ReleaseRecord {
        sdk_release,
        soname: json_string(text, "soname")?,
        sha256: parse_hex_digest(&digest_text)?,
        cpu_baseline: json_string(text, "cpu_baseline")?,
        runtime_profile: json_string(text, "runtime_profile")?,
        native_abis: json_integer_array(text, "native_abis")?
            .into_iter()
            .map(|abi| u8::try_from(abi).map_err(|_| SdkRegistryError::InvalidMetadata))
            .collect::<Result<_, _>>()?,
        needed: json_string_array(text, "needed")?,
    };
    let canonical = format!(
        "{{\"cpu_baseline\":\"{}\",\"format\":\"{}\",\"library\":\"{}\",\"native_abis\":[{}],\"needed\":[{}],\"runtime_profile\":\"{}\",\"sdk_release\":{},\"sha256\":\"{}\",\"soname\":\"{}\",\"target\":\"{}\"}}\n",
        record.cpu_baseline,
        format,
        library_name,
        record
            .native_abis
            .iter()
            .map(u8::to_string)
            .collect::<Vec<_>>()
            .join(","),
        record
            .needed
            .iter()
            .map(|name| format!("\"{name}\""))
            .collect::<Vec<_>>()
            .join(","),
        record.runtime_profile,
        record.sdk_release,
        digest_text,
        record.soname,
        target,
    );
    if canonical != text
        || format != "PRDB-SDK-RELEASE-1"
        || target != "linux-x86_64"
        || sdk_release == 0
        || record.soname != format!("libprdb_sql.so.{sdk_release}")
        || library_name != record.soname
        || record.native_abis.is_empty()
        || record.native_abis.iter().any(|abi| !(1..=6).contains(abi))
        || record.native_abis.windows(2).any(|pair| pair[0] >= pair[1])
        || record.needed.windows(2).any(|pair| pair[0] >= pair[1])
        || record
            .needed
            .iter()
            .any(|dependency| dependency.contains('/') || dependency.contains('\\'))
        || !is_profile_token(&record.cpu_baseline)
        || !is_profile_token(&record.runtime_profile)
    {
        return Err(SdkRegistryError::InvalidMetadata);
    }
    Ok(record)
}

fn validate_library(record: &ReleaseRecord, library: &[u8]) -> Result<(), SdkRegistryError> {
    let dynamic =
        decode_elf_dynamic_info(library).map_err(|_| SdkRegistryError::InvalidMetadata)?;
    if dynamic.soname.as_deref() != Some(record.soname.as_str())
        || dynamic.rpath.is_some()
        || dynamic.runpath.is_some()
        || dynamic.needed != record.needed
    {
        return Err(SdkRegistryError::InvalidMetadata);
    }
    Ok(())
}

fn rename_noreplace(source: &Path, destination: &Path) -> Result<(), SdkRegistryError> {
    use std::os::unix::ffi::OsStrExt;
    let source = std::ffi::CString::new(source.as_os_str().as_bytes())
        .map_err(|_| SdkRegistryError::InvalidConfiguration)?;
    let destination = std::ffi::CString::new(destination.as_os_str().as_bytes())
        .map_err(|_| SdkRegistryError::InvalidConfiguration)?;
    // SAFETY: both absolute C strings remain live through the syscall.
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
    if result == 0 {
        Ok(())
    } else {
        let error = io::Error::last_os_error();
        if error.raw_os_error() == Some(libc::EINVAL) {
            Err(SdkRegistryError::AtomicPublishUnsupported)
        } else {
            Err(error.into())
        }
    }
}

fn json_value<'a>(text: &'a str, key: &str) -> Result<&'a str, SdkRegistryError> {
    let needle = format!("\"{key}\":");
    if text.matches(&needle).count() != 1 {
        return Err(SdkRegistryError::InvalidMetadata);
    }
    let start = text
        .find(&needle)
        .ok_or(SdkRegistryError::InvalidMetadata)?
        + needle.len();
    let rest = &text[start..];
    let end = if let Some(content) = rest.strip_prefix('"') {
        let length = content.find('"').ok_or(SdkRegistryError::InvalidMetadata)?;
        if content[..length].contains('\\') {
            return Err(SdkRegistryError::InvalidMetadata);
        }
        length + 2
    } else if rest.starts_with('[') {
        rest.find(']').ok_or(SdkRegistryError::InvalidMetadata)? + 1
    } else {
        rest.find([',', '}'])
            .ok_or(SdkRegistryError::InvalidMetadata)?
    };
    Ok(&rest[..end])
}

fn json_string(text: &str, key: &str) -> Result<String, SdkRegistryError> {
    json_value(text, key)?
        .strip_prefix('"')
        .and_then(|value| value.strip_suffix('"'))
        .map(str::to_owned)
        .ok_or(SdkRegistryError::InvalidMetadata)
}

fn json_integer(text: &str, key: &str) -> Result<u32, SdkRegistryError> {
    json_value(text, key)?
        .parse()
        .map_err(|_| SdkRegistryError::InvalidMetadata)
}

fn json_integer_array(text: &str, key: &str) -> Result<Vec<u64>, SdkRegistryError> {
    parse_array(json_value(text, key)?)
        .into_iter()
        .map(|item| item.parse().map_err(|_| SdkRegistryError::InvalidMetadata))
        .collect()
}

fn json_string_array(text: &str, key: &str) -> Result<Vec<String>, SdkRegistryError> {
    parse_array(json_value(text, key)?)
        .into_iter()
        .map(|item| {
            item.strip_prefix('"')
                .and_then(|item| item.strip_suffix('"'))
                .filter(|item| !item.contains('\\'))
                .map(str::to_owned)
                .ok_or(SdkRegistryError::InvalidMetadata)
        })
        .collect()
}

fn parse_array(value: &str) -> Vec<&str> {
    let inner = value
        .strip_prefix('[')
        .and_then(|value| value.strip_suffix(']'))
        .unwrap_or("");
    if inner.is_empty() {
        Vec::new()
    } else {
        inner.split(',').collect()
    }
}

fn parse_hex_digest(value: &str) -> Result<[u8; 32], SdkRegistryError> {
    if value.len() != 64 {
        return Err(SdkRegistryError::InvalidMetadata);
    }
    let mut digest = [0; 32];
    for (index, byte) in digest.iter_mut().enumerate() {
        *byte = u8::from_str_radix(&value[index * 2..index * 2 + 2], 16)
            .map_err(|_| SdkRegistryError::InvalidMetadata)?;
        if value.as_bytes()[index * 2].is_ascii_uppercase()
            || value.as_bytes()[index * 2 + 1].is_ascii_uppercase()
        {
            return Err(SdkRegistryError::InvalidMetadata);
        }
    }
    Ok(digest)
}

fn sha256(bytes: &[u8]) -> [u8; 32] {
    Sha256::digest(bytes).into()
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

fn is_profile_token(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 64
        && value.as_bytes()[0].is_ascii_alphanumeric()
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || b"_.-".contains(&byte))
}
