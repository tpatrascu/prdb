//! Private on-disk storage for client bearer credentials.

use crate::client_protocol::ClientCredentials;
use crate::credentials::TOKEN_SECRET_BYTES;
use std::fs::{self, File, OpenOptions};
use std::io::{self, Read, Write};
use std::os::unix::fs::{MetadataExt, OpenOptionsExt, PermissionsExt};
use std::path::Path;
use zeroize::Zeroize;

const FILE_HEADER: &[u8] = b"PDB-CREDENTIALS-1\n";
const FILE_BYTES: usize = 18 + 32 + 1 + TOKEN_SECRET_BYTES * 2 + 1;

#[derive(Debug)]
pub enum ClientCredentialFileError {
    InvalidPath,
    InvalidFormat,
    UnsafePermissions,
    AlreadyExists,
    Io(io::Error),
}

impl std::fmt::Display for ClientCredentialFileError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "client credential file error: {self:?}")
    }
}

impl std::error::Error for ClientCredentialFileError {}

impl From<io::Error> for ClientCredentialFileError {
    fn from(error: io::Error) -> Self {
        Self::Io(error)
    }
}

/// Load a canonical credential file owned by this user with mode 0600.
pub fn read_client_credentials(
    path: impl AsRef<Path>,
) -> Result<ClientCredentials, ClientCredentialFileError> {
    let path = path.as_ref();
    let file = OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC)
        .open(path)
        .map_err(|error| {
            if error.raw_os_error() == Some(libc::ELOOP) {
                ClientCredentialFileError::UnsafePermissions
            } else {
                error.into()
            }
        })?;
    let metadata = file.metadata()?;
    validate_file_metadata(&metadata)?;
    let mut bytes = Vec::with_capacity(FILE_BYTES);
    file.take((FILE_BYTES + 1) as u64).read_to_end(&mut bytes)?;
    if bytes.len() != FILE_BYTES {
        bytes.zeroize();
        return Err(ClientCredentialFileError::InvalidFormat);
    }
    let parsed = parse_credentials(&bytes);
    bytes.zeroize();
    parsed
}

/// Atomically publish a new mode-0600 credential file without overwriting an
/// existing path. The bearer secret is zeroized from temporary buffers.
pub fn write_client_credentials(
    path: impl AsRef<Path>,
    credentials: &ClientCredentials,
) -> Result<(), ClientCredentialFileError> {
    let path = path.as_ref();
    let parent = parent_directory(path)?;
    let name = path
        .file_name()
        .ok_or(ClientCredentialFileError::InvalidPath)?;
    let parent = fs::canonicalize(parent)?;
    if !parent.is_dir() {
        return Err(ClientCredentialFileError::InvalidPath);
    }
    let target = parent.join(name);
    match fs::symlink_metadata(&target) {
        Ok(_) => return Err(ClientCredentialFileError::AlreadyExists),
        Err(error) if error.kind() == io::ErrorKind::NotFound => {}
        Err(error) => return Err(error.into()),
    }

    let mut random = [0u8; 16];
    getrandom::fill(&mut random).map_err(|_| {
        ClientCredentialFileError::Io(io::Error::other("secure random source unavailable"))
    })?;
    let temporary = parent.join(format!(".pdb-credential-{}.tmp", lower_hex(&random)));
    random.zeroize();

    let mut bytes = encode_credentials(credentials);
    let result = (|| {
        let mut file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .mode(0o600)
            .open(&temporary)?;
        file.set_permissions(fs::Permissions::from_mode(0o600))?;
        file.write_all(&bytes)?;
        file.sync_all()?;
        drop(file);
        fs::hard_link(&temporary, &target).map_err(|error| {
            if error.kind() == io::ErrorKind::AlreadyExists {
                ClientCredentialFileError::AlreadyExists
            } else {
                error.into()
            }
        })?;
        fs::remove_file(&temporary)?;
        File::open(&parent)?.sync_all()?;
        Ok(())
    })();
    bytes.zeroize();
    let _ = fs::remove_file(&temporary);
    result
}

/// Remove an owned mode-0600 credential file and synchronize its parent.
pub fn remove_client_credentials(path: impl AsRef<Path>) -> Result<(), ClientCredentialFileError> {
    let path = path.as_ref();
    let metadata = fs::symlink_metadata(path)?;
    validate_file_metadata(&metadata)?;
    fs::remove_file(path)?;
    File::open(parent_directory(path)?)?.sync_all()?;
    Ok(())
}

fn validate_file_metadata(metadata: &fs::Metadata) -> Result<(), ClientCredentialFileError> {
    // SAFETY: geteuid has no pointer arguments or memory preconditions.
    let effective_uid = unsafe { libc::geteuid() };
    if !metadata.file_type().is_file()
        || metadata.uid() != effective_uid
        || metadata.permissions().mode() & 0o7777 != 0o600
        || metadata.len() != FILE_BYTES as u64
    {
        return Err(ClientCredentialFileError::UnsafePermissions);
    }
    Ok(())
}

fn parse_credentials(bytes: &[u8]) -> Result<ClientCredentials, ClientCredentialFileError> {
    if bytes.len() != FILE_BYTES
        || !bytes.starts_with(FILE_HEADER)
        || bytes[bytes.len() - 1] != b'\n'
    {
        return Err(ClientCredentialFileError::InvalidFormat);
    }
    let token_id_start = FILE_HEADER.len();
    let token_id_end = token_id_start + 32;
    if bytes[token_id_end] != b'\n' {
        return Err(ClientCredentialFileError::InvalidFormat);
    }
    let secret_start = token_id_end + 1;
    let secret_end = secret_start + TOKEN_SECRET_BYTES * 2;
    if bytes[secret_end] != b'\n' {
        return Err(ClientCredentialFileError::InvalidFormat);
    }
    let token_id = parse_lower_hex::<16>(&bytes[token_id_start..token_id_end])?;
    if token_id == [0; 16] {
        return Err(ClientCredentialFileError::InvalidFormat);
    }
    let token_secret = parse_lower_hex::<TOKEN_SECRET_BYTES>(&bytes[secret_start..secret_end])?;
    Ok(ClientCredentials {
        token_id,
        token_secret,
    })
}

fn encode_credentials(credentials: &ClientCredentials) -> Vec<u8> {
    let mut bytes = Vec::with_capacity(FILE_BYTES);
    bytes.extend_from_slice(FILE_HEADER);
    append_lower_hex(&mut bytes, &credentials.token_id);
    bytes.push(b'\n');
    append_lower_hex(&mut bytes, &credentials.token_secret);
    bytes.push(b'\n');
    bytes
}

fn parse_lower_hex<const N: usize>(bytes: &[u8]) -> Result<[u8; N], ClientCredentialFileError> {
    if bytes.len() != N * 2 {
        return Err(ClientCredentialFileError::InvalidFormat);
    }
    let mut result = [0; N];
    for (index, pair) in bytes.chunks_exact(2).enumerate() {
        let high = match hex_nibble(pair[0]) {
            Ok(nibble) => nibble,
            Err(error) => {
                result.zeroize();
                return Err(error);
            }
        };
        let low = match hex_nibble(pair[1]) {
            Ok(nibble) => nibble,
            Err(error) => {
                result.zeroize();
                return Err(error);
            }
        };
        result[index] = (high << 4) | low;
    }
    Ok(result)
}

fn hex_nibble(byte: u8) -> Result<u8, ClientCredentialFileError> {
    match byte {
        b'0'..=b'9' => Ok(byte - b'0'),
        b'a'..=b'f' => Ok(byte - b'a' + 10),
        _ => Err(ClientCredentialFileError::InvalidFormat),
    }
}

fn lower_hex(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut result = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        result.push(HEX[(byte >> 4) as usize] as char);
        result.push(HEX[(byte & 15) as usize] as char);
    }
    result
}

fn append_lower_hex(output: &mut Vec<u8>, bytes: &[u8]) {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    for byte in bytes {
        output.push(HEX[(byte >> 4) as usize]);
        output.push(HEX[(byte & 15) as usize]);
    }
}

fn parent_directory(path: &Path) -> Result<&Path, ClientCredentialFileError> {
    match path.parent() {
        Some(parent) if !parent.as_os_str().is_empty() => Ok(parent),
        Some(_) => Ok(Path::new(".")),
        None => Err(ClientCredentialFileError::InvalidPath),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::os::unix::fs::symlink;
    use std::path::PathBuf;
    use std::time::{SystemTime, UNIX_EPOCH};

    fn temp_dir() -> PathBuf {
        let nonce = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let path =
            std::env::temp_dir().join(format!("prdb-credentials-{}-{nonce}", std::process::id()));
        fs::create_dir(&path).unwrap();
        path
    }

    fn credentials() -> ClientCredentials {
        ClientCredentials {
            token_id: [1; 16],
            token_secret: [2; TOKEN_SECRET_BYTES],
        }
    }

    #[test]
    fn private_credential_file_roundtrips_and_never_overwrites() {
        let directory = temp_dir();
        let path = directory.join("client.cred");
        let value = credentials();
        write_client_credentials(&path, &value).unwrap();
        let metadata = fs::metadata(&path).unwrap();
        assert_eq!(metadata.permissions().mode() & 0o777, 0o600);
        assert!(read_client_credentials(&path).unwrap() == value);
        assert!(matches!(
            write_client_credentials(&path, &value),
            Err(ClientCredentialFileError::AlreadyExists)
        ));
        remove_client_credentials(&path).unwrap();
        assert!(!path.exists());
        fs::remove_dir(directory).unwrap();
    }

    #[test]
    fn credential_file_rejects_symlink_and_broad_permissions() {
        let directory = temp_dir();
        let target = directory.join("target");
        write_client_credentials(&target, &credentials()).unwrap();
        let link = directory.join("link");
        symlink(&target, &link).unwrap();
        assert!(matches!(
            read_client_credentials(&link),
            Err(ClientCredentialFileError::UnsafePermissions)
        ));
        fs::set_permissions(&target, fs::Permissions::from_mode(0o640)).unwrap();
        assert!(matches!(
            read_client_credentials(&target),
            Err(ClientCredentialFileError::UnsafePermissions)
        ));
        fs::remove_file(link).unwrap();
        fs::remove_file(target).unwrap();
        fs::remove_dir(directory).unwrap();
    }
}
