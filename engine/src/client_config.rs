//! Strict, versioned configuration for persistent client instance settings.

use serde::{Deserialize, Serialize};
use std::fs::{self, File, OpenOptions};
use std::io::{self, Read, Write};
use std::net::IpAddr;
use std::os::fd::AsRawFd;
use std::os::unix::fs::{OpenOptionsExt, PermissionsExt};
use std::path::{Path, PathBuf};
use zeroize::Zeroize;

pub const CONFIG_FORMAT: &str = "PRDB-CLIENT-CONFIG-1";
pub const MAX_CONFIG_BYTES: u64 = 1024 * 1024;

/// A held SDK-selection lock. Interactive explicit transactions hold a shared
/// lock; commands that change a saved selection need the exclusive lock.
pub struct ClientSelectionLock {
    _file: File,
}

pub fn lock_sdk_selection(
    config_path: &Path,
    instance_name: &str,
    shared: bool,
) -> Result<ClientSelectionLock, ClientConfigError> {
    if !valid_identifier(instance_name) {
        return Err(invalid("invalid instance name for SDK-selection lock"));
    }
    let parent = config_parent(config_path)?;
    let existed = parent.exists();
    fs::create_dir_all(parent)?;
    if !existed {
        fs::set_permissions(parent, fs::Permissions::from_mode(0o700))?;
    }
    let directory = open_directory(parent)?;
    validate_directory_metadata(&directory.metadata()?)?;
    let lock_path = parent.join(format!(".prdb-sdk-selection-{instance_name}.lock"));
    let mut options = OpenOptions::new();
    options
        .read(true)
        .write(true)
        .create(true)
        .mode(0o600)
        .custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC);
    let file = options.open(&lock_path)?;
    file.set_permissions(fs::Permissions::from_mode(0o600))?;
    use std::os::unix::fs::MetadataExt;
    let metadata = file.metadata()?;
    if !metadata.is_file()
        || metadata.uid() != unsafe { libc::geteuid() }
        || metadata.mode() & 0o077 != 0
    {
        return Err(invalid(
            "SDK-selection lock must be owner-owned with mode 0600",
        ));
    }
    let operation = if shared {
        libc::LOCK_SH | libc::LOCK_NB
    } else {
        libc::LOCK_EX | libc::LOCK_NB
    };
    // SAFETY: flock only changes the advisory lock associated with this live fd.
    if unsafe { libc::flock(file.as_raw_fd(), operation) } != 0 {
        return Err(invalid(
            "SDK selection is locked by an active interactive transaction",
        ));
    }
    Ok(ClientSelectionLock { _file: file })
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ClientConfig {
    pub format: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub default_instance: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub client_tools: Option<ClientTools>,
    #[serde(default)]
    pub instances: Vec<ClientInstance>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ClientTools {
    pub include_dir: String,
    pub tcc: String,
    pub shared_linker: String,
    pub patchelf: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SdkSelection {
    pub release: u32,
    pub target: String,
    pub cpu_baseline: String,
    pub runtime_profile: String,
    pub native_abi: u8,
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ClientInstance {
    pub name: String,
    pub address: String,
    pub port: u16,
    pub sdk: SdkSelection,
    pub default_credential: String,
    pub credentials: Vec<ClientCredential>,
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ClientCredential {
    pub name: String,
    pub username: String,
    pub token_id: String,
    pub token_secret: String,
}

impl ClientCredential {
    pub fn token_id_bytes(&self) -> Result<[u8; 16], ClientConfigError> {
        let decoded =
            decode_lower_hex_16(&self.token_id).ok_or_else(|| invalid("invalid token id"))?;
        Ok(decoded)
    }

    pub fn token_secret_bytes(&self) -> Result<[u8; 32], ClientConfigError> {
        decode_base64url_32(&self.token_secret).ok_or_else(|| invalid("invalid token secret"))
    }
}

pub fn encode_token_secret(bytes: &[u8; 32]) -> String {
    const ALPHABET: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_";
    let mut out = String::with_capacity(43);
    for chunk in bytes.chunks_exact(3) {
        let n = (u32::from(chunk[0]) << 16) | (u32::from(chunk[1]) << 8) | u32::from(chunk[2]);
        out.push(ALPHABET[((n >> 18) & 63) as usize] as char);
        out.push(ALPHABET[((n >> 12) & 63) as usize] as char);
        out.push(ALPHABET[((n >> 6) & 63) as usize] as char);
        out.push(ALPHABET[(n & 63) as usize] as char);
    }
    let tail = &bytes[30..];
    out.push(ALPHABET[(tail[0] >> 2) as usize] as char);
    out.push(ALPHABET[(((tail[0] & 3) << 4) | (tail[1] >> 4)) as usize] as char);
    out.push(ALPHABET[((tail[1] & 15) << 2) as usize] as char);
    out
}

fn decode_lower_hex_16(value: &str) -> Option<[u8; 16]> {
    if value.len() != 32 {
        return None;
    }
    let mut out = [0; 16];
    for (index, chunk) in value.as_bytes().chunks_exact(2).enumerate() {
        out[index] = (hex_value(chunk[0])? << 4) | hex_value(chunk[1])?;
    }
    Some(out)
}

fn hex_value(value: u8) -> Option<u8> {
    match value {
        b'0'..=b'9' => Some(value - b'0'),
        b'a'..=b'f' => Some(value - b'a' + 10),
        _ => None,
    }
}

fn decode_base64url_32(value: &str) -> Option<[u8; 32]> {
    if !valid_token_secret(value) {
        return None;
    }
    let mut out = [0; 32];
    let mut accumulator = 0u32;
    let mut bits = 0u8;
    let mut written = 0;
    for byte in value.bytes() {
        accumulator = (accumulator << 6) | u32::from(base64_value(byte)?);
        bits += 6;
        if bits >= 8 {
            bits -= 8;
            out[written] = (accumulator >> bits) as u8;
            written += 1;
        }
    }
    (written == 32 && bits == 2 && accumulator & 3 == 0).then_some(out)
}

fn base64_value(value: u8) -> Option<u8> {
    match value {
        b'A'..=b'Z' => Some(value - b'A'),
        b'a'..=b'z' => Some(value - b'a' + 26),
        b'0'..=b'9' => Some(value - b'0' + 52),
        b'-' => Some(62),
        b'_' => Some(63),
        _ => None,
    }
}

impl std::fmt::Debug for ClientConfig {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ClientConfig")
            .field("format", &self.format)
            .field("default_instance", &self.default_instance)
            .field("client_tools", &self.client_tools)
            .field(
                "instances",
                &self.instances.iter().map(|i| &i.name).collect::<Vec<_>>(),
            )
            .finish()
    }
}

impl Drop for ClientConfig {
    fn drop(&mut self) {
        for instance in &mut self.instances {
            for credential in &mut instance.credentials {
                credential.token_secret.zeroize();
            }
        }
    }
}

impl std::fmt::Debug for ClientCredential {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ClientCredential")
            .field("name", &self.name)
            .field("username", &self.username)
            .field("token_id", &self.token_id)
            .field("token_secret", &"<redacted>")
            .finish()
    }
}

impl Drop for ClientCredential {
    fn drop(&mut self) {
        self.token_secret.zeroize();
    }
}

#[derive(Debug)]
pub enum ClientConfigError {
    Io(io::Error),
    Invalid(String),
}

impl std::fmt::Display for ClientConfigError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Io(error) => write!(formatter, "client config I/O error: {error}"),
            Self::Invalid(message) => write!(formatter, "invalid client config: {message}"),
        }
    }
}

impl std::error::Error for ClientConfigError {}

impl From<io::Error> for ClientConfigError {
    fn from(value: io::Error) -> Self {
        Self::Io(value)
    }
}

impl ClientConfig {
    pub fn empty() -> Self {
        Self {
            format: CONFIG_FORMAT.to_owned(),
            default_instance: None,
            client_tools: None,
            instances: Vec::new(),
        }
    }

    pub fn parse(bytes: &[u8]) -> Result<Self, ClientConfigError> {
        if bytes.len() as u64 > MAX_CONFIG_BYTES {
            return Err(invalid("file exceeds 1 MiB"));
        }
        let config: Self = serde_json::from_slice(bytes)
            .map_err(|_| invalid("malformed, duplicate-key, or unsupported JSON fields"))?;
        config.validate()?;
        Ok(config)
    }

    pub fn validate(&self) -> Result<(), ClientConfigError> {
        if self.format != CONFIG_FORMAT {
            return Err(invalid("unsupported format"));
        }
        if let Some(tools) = &self.client_tools {
            for value in [
                &tools.include_dir,
                &tools.tcc,
                &tools.shared_linker,
                &tools.patchelf,
            ] {
                if value.is_empty()
                    || value.contains('\0')
                    || value.contains('$')
                    || value.contains('`')
                {
                    return Err(invalid("client tool paths must be nonempty literal values"));
                }
            }
        }
        let mut names = std::collections::HashSet::new();
        for instance in &self.instances {
            if !valid_identifier(&instance.name) || !names.insert(instance.name.as_str()) {
                return Err(invalid(
                    "instance names must be unique normalized identifiers",
                ));
            }
            if instance.address.parse::<IpAddr>().is_err() || instance.port == 0 {
                return Err(invalid(
                    "instance endpoint must use an IP literal and port 1..65535",
                ));
            }
            let sdk = &instance.sdk;
            if sdk.release == 0
                || sdk.target != "linux-x86_64"
                || !valid_token(&sdk.cpu_baseline)
                || !valid_token(&sdk.runtime_profile)
                || !(1..=6).contains(&sdk.native_abi)
            {
                return Err(invalid("invalid SDK selection"));
            }
            let mut profile_names = std::collections::HashSet::new();
            let mut usernames = std::collections::HashSet::new();
            let mut default_found = false;
            if instance.credentials.is_empty() || instance.credentials.len() > 16 {
                return Err(invalid("each instance requires 1..=16 credential profiles"));
            }
            for credential in &instance.credentials {
                if !valid_identifier(&credential.name)
                    || !profile_names.insert(credential.name.as_str())
                    || !valid_database_username(&credential.username)
                    || !usernames.insert(credential.username.as_str())
                    || credential.token_id.len() != 32
                    || !credential
                        .token_id
                        .bytes()
                        .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
                    || !valid_token_secret(&credential.token_secret)
                {
                    return Err(invalid("invalid or duplicate credential profile"));
                }
                default_found |= credential.name == instance.default_credential;
            }
            if !default_found {
                return Err(invalid("default_credential must name a configured profile"));
            }
        }
        if self
            .default_instance
            .as_ref()
            .is_some_and(|name| !names.contains(name.as_str()))
        {
            return Err(invalid("default_instance must name a configured instance"));
        }
        Ok(())
    }

    pub fn instance(&self, name: Option<&str>) -> Result<&ClientInstance, ClientConfigError> {
        let name = name
            .or(self.default_instance.as_deref())
            .ok_or_else(|| invalid("no instance selected and no default_instance is configured"))?;
        self.instances
            .iter()
            .find(|instance| instance.name == name)
            .ok_or_else(|| invalid("configured instance was not found"))
    }

    pub fn upsert_instance(&mut self, instance: ClientInstance, make_default: bool) {
        let name = instance.name.clone();
        if let Some(existing) = self
            .instances
            .iter_mut()
            .find(|entry| entry.name == instance.name)
        {
            *existing = instance;
        } else {
            self.instances.push(instance);
        }
        if make_default {
            self.default_instance = Some(name);
        }
    }

    pub fn load(path: &Path) -> Result<Self, ClientConfigError> {
        let parent = config_parent(path)?;
        let directory = open_directory(parent)?;
        validate_directory_metadata(&directory.metadata()?)?;
        let mut options = OpenOptions::new();
        options
            .read(true)
            .custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC);
        let file = options.open(path)?;
        let metadata = file.metadata()?;
        validate_file_metadata(&metadata)?;
        let mut bytes = Vec::with_capacity(metadata.len() as usize);
        file.take(MAX_CONFIG_BYTES + 1).read_to_end(&mut bytes)?;
        let parsed = Self::parse(&bytes);
        bytes.zeroize();
        parsed
    }

    pub fn save_atomic(&self, path: &Path) -> Result<(), ClientConfigError> {
        self.validate()?;
        let parent = config_parent(path)?;
        let existed = parent.exists();
        fs::create_dir_all(parent)?;
        if !existed {
            fs::set_permissions(parent, fs::Permissions::from_mode(0o700))?;
        }
        let dir = open_directory(parent)?;
        validate_directory_metadata(&dir.metadata()?)?;
        let bytes = zeroize::Zeroizing::new(
            serde_json::to_vec_pretty(self)
                .map_err(|_| invalid("could not encode client config"))?,
        );
        if bytes.len() as u64 > MAX_CONFIG_BYTES {
            return Err(invalid("file exceeds 1 MiB"));
        }
        let temp = temporary_path(path);
        let mut options = OpenOptions::new();
        options
            .write(true)
            .create_new(true)
            .mode(0o600)
            .custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC);
        let mut file = options.open(&temp)?;
        file.set_permissions(fs::Permissions::from_mode(0o600))?;
        let result = (|| -> io::Result<()> {
            file.write_all(&bytes)?;
            file.sync_all()?;
            fs::rename(&temp, path)?;
            dir.sync_all()?;
            Ok(())
        })();
        if result.is_err() {
            let _ = fs::remove_file(&temp);
        }
        result?;
        Ok(())
    }
}

pub fn default_config_path() -> Result<PathBuf, ClientConfigError> {
    if let Some(path) = std::env::var_os("PRDB_CLIENT_CONFIG") {
        return Ok(PathBuf::from(path));
    }
    let base = std::env::var_os("XDG_CONFIG_HOME")
        .map(PathBuf::from)
        .or_else(|| std::env::var_os("HOME").map(|home| PathBuf::from(home).join(".config")))
        .ok_or_else(|| invalid("HOME or XDG_CONFIG_HOME is not set"))?;
    Ok(base.join("prdb/client.json"))
}

fn validate_file_metadata(metadata: &fs::Metadata) -> Result<(), ClientConfigError> {
    use std::os::unix::fs::MetadataExt;
    if !metadata.is_file()
        || metadata.uid() != unsafe { libc::geteuid() }
        || metadata.mode() & 0o077 != 0
        || metadata.len() > MAX_CONFIG_BYTES
    {
        return Err(invalid(
            "config must be an owner-owned regular file with mode 0600",
        ));
    }
    Ok(())
}

fn validate_directory_metadata(metadata: &fs::Metadata) -> Result<(), ClientConfigError> {
    use std::os::unix::fs::MetadataExt;
    if !metadata.is_dir()
        || metadata.uid() != unsafe { libc::geteuid() }
        || metadata.mode() & 0o077 != 0
    {
        return Err(invalid(
            "config directory must be owner-owned with mode 0700",
        ));
    }
    Ok(())
}

fn open_directory(path: &Path) -> Result<File, ClientConfigError> {
    let metadata = fs::symlink_metadata(path)?;
    if metadata.file_type().is_symlink() || !metadata.is_dir() {
        return Err(invalid("config directory must not be a symlink"));
    }
    let mut options = OpenOptions::new();
    options
        .read(true)
        .custom_flags(libc::O_DIRECTORY | libc::O_NOFOLLOW | libc::O_CLOEXEC);
    Ok(options.open(path)?)
}

fn config_parent(path: &Path) -> Result<&Path, ClientConfigError> {
    match path.parent() {
        Some(parent) if !parent.as_os_str().is_empty() => Ok(parent),
        Some(_) => Ok(Path::new(".")),
        None => Err(invalid("config path has no parent")),
    }
}

fn temporary_path(path: &Path) -> PathBuf {
    let suffix = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    let mut name = path.file_name().unwrap_or_default().to_os_string();
    name.push(format!(".tmp-{}-{suffix}", std::process::id()));
    path.with_file_name(name)
}

fn valid_identifier(value: &str) -> bool {
    (1..=64).contains(&value.len())
        && value.is_ascii()
        && (value.as_bytes()[0].is_ascii_lowercase() || value.as_bytes()[0].is_ascii_digit())
        && value
            .bytes()
            .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'-' || b == b'_')
}

fn valid_database_username(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 128
        && (value.as_bytes()[0].is_ascii_lowercase() || value.as_bytes()[0] == b'_')
        && value
            .bytes()
            .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'_')
}

fn valid_token(value: &str) -> bool {
    (1..=64).contains(&value.len())
        && value.is_ascii()
        && value
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b == b'-' || b == b'_')
}

fn valid_token_secret(value: &str) -> bool {
    if value.len() != 43
        || !value
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b == b'-' || b == b'_')
    {
        return false;
    }
    // A 32-byte value in unpadded base64url has two zero padding bits.
    let last = value.as_bytes()[42];
    let value = match last {
        b'A'..=b'Z' => last - b'A',
        b'a'..=b'z' => last - b'a' + 26,
        b'0'..=b'9' => last - b'0' + 52,
        b'-' => 62,
        b'_' => 63,
        _ => return false,
    };
    value & 0b11 == 0
}

fn invalid(message: &str) -> ClientConfigError {
    ClientConfigError::Invalid(message.to_owned())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn config() -> ClientConfig {
        ClientConfig {
            format: CONFIG_FORMAT.to_owned(),
            default_instance: Some("dev".to_owned()),
            client_tools: None,
            instances: vec![ClientInstance {
                name: "dev".to_owned(),
                address: "127.0.0.1".to_owned(),
                port: 7443,
                sdk: SdkSelection {
                    release: 1,
                    target: "linux-x86_64".to_owned(),
                    cpu_baseline: "x86_64-v1".to_owned(),
                    runtime_profile: "glibc-test".to_owned(),
                    native_abi: 5,
                },
                default_credential: "alice".to_owned(),
                credentials: vec![ClientCredential {
                    name: "alice".to_owned(),
                    username: "alice".to_owned(),
                    token_id: "a".repeat(32),
                    token_secret: "A".repeat(42) + "A",
                }],
            }],
        }
    }

    #[test]
    fn config_round_trips_and_rejects_duplicate_unknown_and_noncanonical_fields() {
        let value = config();
        let debug = format!("{value:?}");
        assert!(!debug.contains(&value.instances[0].credentials[0].token_secret));
        let encoded = serde_json::to_vec(&value).unwrap();
        ClientConfig::parse(&encoded).unwrap();
        let mut spill_abi = config();
        spill_abi.instances[0].sdk.native_abi = 6;
        assert!(spill_abi.validate().is_ok());
        assert!(ClientConfig::parse(br#"{"format":"PRDB-CLIENT-CONFIG-1","format":"PRDB-CLIENT-CONFIG-1","instances":[]}"#).is_err());
        assert!(
            ClientConfig::parse(
                br#"{"format":"PRDB-CLIENT-CONFIG-1","extra":true,"instances":[]}"#
            )
            .is_err()
        );
        let mut bad = config();
        bad.instances[0].address = "db.example".to_owned();
        assert!(bad.validate().is_err());
        let token: [u8; 32] = std::array::from_fn(|index| index as u8);
        let encoded_secret = encode_token_secret(&token);
        assert_eq!(decode_base64url_32(&encoded_secret), Some(token));
        assert!(decode_base64url_32(&(encoded_secret[..42].to_owned() + "B")).is_none());
    }

    #[test]
    fn config_update_is_owner_only_atomic_and_preserves_default_lookup() {
        let root = std::env::temp_dir().join(format!("prdb-client-config-{}", std::process::id()));
        let _ = fs::remove_dir_all(&root);
        fs::create_dir(&root).unwrap();
        fs::set_permissions(&root, fs::Permissions::from_mode(0o700)).unwrap();
        let path = root.join("private/client.json");
        config().save_atomic(&path).unwrap();
        let loaded = ClientConfig::load(&path).unwrap();
        assert_eq!(loaded.instance(None).unwrap().name, "dev");
        assert_eq!(
            fs::metadata(path.parent().unwrap())
                .unwrap()
                .permissions()
                .mode()
                & 0o777,
            0o700
        );
        assert_eq!(
            fs::metadata(&path).unwrap().permissions().mode() & 0o777,
            0o600
        );
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn config_reader_rejects_symlinks_broad_permissions_and_oversized_input() {
        let root =
            std::env::temp_dir().join(format!("prdb-client-config-safe-{}", std::process::id()));
        let _ = fs::remove_dir_all(&root);
        fs::create_dir(&root).unwrap();
        fs::set_permissions(&root, fs::Permissions::from_mode(0o700)).unwrap();
        let target = root.join("target.json");
        config().save_atomic(&target).unwrap();
        let link = root.join("linked.json");
        std::os::unix::fs::symlink(&target, &link).unwrap();
        assert!(ClientConfig::load(&link).is_err());
        fs::set_permissions(&target, fs::Permissions::from_mode(0o644)).unwrap();
        assert!(ClientConfig::load(&target).is_err());
        assert!(ClientConfig::parse(&vec![b' '; MAX_CONFIG_BYTES as usize + 1]).is_err());
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn active_interactive_selection_lock_blocks_sdk_changes() {
        let root =
            std::env::temp_dir().join(format!("prdb-client-config-lock-{}", std::process::id()));
        let _ = fs::remove_dir_all(&root);
        fs::create_dir(&root).unwrap();
        fs::set_permissions(&root, fs::Permissions::from_mode(0o700)).unwrap();
        let path = root.join("client.json");
        config().save_atomic(&path).unwrap();
        let shared = lock_sdk_selection(&path, "dev", true).unwrap();
        assert!(lock_sdk_selection(&path, "dev", false).is_err());
        drop(shared);
        let exclusive = lock_sdk_selection(&path, "dev", false).unwrap();
        drop(exclusive);
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn sdk_selection_lock_creates_private_config_directory() {
        let root = std::env::temp_dir().join(format!(
            "prdb-client-config-lock-parent-{}",
            std::process::id()
        ));
        let _ = fs::remove_dir_all(&root);
        let path = root.join("nested").join("client.json");
        let lock = lock_sdk_selection(&path, "dev", true).unwrap();
        let metadata = fs::metadata(path.parent().unwrap()).unwrap();
        assert_eq!(metadata.permissions().mode() & 0o777, 0o700);
        drop(lock);
        fs::remove_dir_all(root).unwrap();
    }
}
