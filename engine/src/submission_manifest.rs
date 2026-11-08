//! Bounded, versioned query artifact manifest decoding and compatibility checks.

use crate::supervisor::{SubmissionMetadata, SubmissionMetadataValidator};
use std::collections::HashSet;

pub const MANIFEST_V1_MAGIC: &[u8; 4] = b"PMF1";
pub const MANIFEST_V1_HEADER_BYTES: usize = 28;
pub const MAX_MANIFEST_DEPENDENCIES: usize = 32;
pub const MAX_MANIFEST_DEPENDENCY_NAME_BYTES: usize = 128;
pub const CLIENT_PROTOCOL_MAJOR: u16 = 1;
pub const CLIENT_PROTOCOL_MINOR: u16 = 0;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ArtifactManifestV1 {
    pub native_abi_version: u8,
    pub sdk_release: u32,
    pub cpu_baseline: String,
    pub runtime_profile: String,
    /// Diagnostic only; this value never authenticates or authorizes the artifact.
    pub compiler_identity: String,
    pub descriptor_symbol: String,
    pub entry_point_symbol: String,
    pub dependencies: Vec<String>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ManifestError {
    InvalidEncoding,
    InvalidArtifact,
    UnsupportedTarget,
    UnsupportedAbi,
    UnsupportedProtocol,
    IncompatibleProfile,
    UnavailableDependency,
    DependencyMismatch,
}

#[derive(Clone, Debug)]
pub struct ArtifactCompatibilityProfile {
    pub cpu_baseline: String,
    pub runtime_profile: String,
    pub sdk_release: u32,
    pub allowed_dependencies: HashSet<String>,
}

impl ArtifactCompatibilityProfile {
    pub fn new(
        cpu_baseline: String,
        runtime_profile: String,
        sdk_release: u32,
        allowed_dependencies: HashSet<String>,
    ) -> Result<Self, ManifestError> {
        if !is_profile_identifier(&cpu_baseline)
            || !is_profile_identifier(&runtime_profile)
            || sdk_release == 0
            || allowed_dependencies
                .iter()
                .any(|name| !is_dependency_name(name))
        {
            return Err(ManifestError::InvalidEncoding);
        }
        Ok(Self {
            cpu_baseline,
            runtime_profile,
            sdk_release,
            allowed_dependencies,
        })
    }

    pub fn validate(&self, manifest: &ArtifactManifestV1) -> Result<(), ManifestError> {
        if manifest.cpu_baseline != self.cpu_baseline
            || manifest.runtime_profile != self.runtime_profile
            || manifest.sdk_release != self.sdk_release
        {
            return Err(ManifestError::IncompatibleProfile);
        }
        if manifest
            .dependencies
            .iter()
            .any(|dependency| !self.allowed_dependencies.contains(dependency))
        {
            return Err(ManifestError::UnavailableDependency);
        }
        Ok(())
    }
}

pub struct VersionedSubmissionMetadataValidator {
    profile: ArtifactCompatibilityProfile,
}

impl VersionedSubmissionMetadataValidator {
    pub fn new(profile: ArtifactCompatibilityProfile) -> Self {
        Self { profile }
    }
}

impl SubmissionMetadataValidator for VersionedSubmissionMetadataValidator {
    fn supports_versions(&self, manifest_version: u32, scope_version: u32) -> bool {
        manifest_version == 1 && scope_version == 1
    }

    fn validate(&self, metadata: &SubmissionMetadata) -> bool {
        let Ok(manifest) = decode_manifest_v1(&metadata.manifest) else {
            return false;
        };
        self.profile.validate(&manifest).is_ok()
            && crate::submission_scope::decode_requested_scope_v1(&metadata.requested_scope).is_ok()
    }

    fn validate_artifact(&self, metadata: &SubmissionMetadata, artifact: &[u8]) -> bool {
        if !self.validate(metadata) {
            return false;
        }
        let Ok(manifest) = decode_manifest_v1(&metadata.manifest) else {
            return false;
        };
        decode_elf_needed_dependencies_internal(
            artifact,
            Some((
                &manifest.descriptor_symbol,
                1,
                &manifest.entry_point_symbol,
                2,
            )),
        )
        .is_ok_and(|dependencies| {
            dependencies.into_iter().collect::<HashSet<_>>()
                == manifest.dependencies.into_iter().collect::<HashSet<_>>()
        })
    }
}

pub fn decode_manifest_v1(bytes: &[u8]) -> Result<ArtifactManifestV1, ManifestError> {
    if bytes.len() < MANIFEST_V1_HEADER_BYTES || &bytes[..4] != MANIFEST_V1_MAGIC {
        return Err(ManifestError::InvalidEncoding);
    }
    if bytes[4] != 1 || bytes[5] != 1 {
        return Err(ManifestError::UnsupportedTarget);
    }
    let abi = bytes[6];
    if !(1..=3).contains(&abi) || bytes[7] != 0 {
        return Err(ManifestError::UnsupportedAbi);
    }
    let protocol_major = read_u16(bytes, 8)?;
    let protocol_minor = read_u16(bytes, 10)?;
    if (protocol_major, protocol_minor) != (CLIENT_PROTOCOL_MAJOR, CLIENT_PROTOCOL_MINOR) {
        return Err(ManifestError::UnsupportedProtocol);
    }
    let sdk_release = read_u32(bytes, 12)?;
    let dependency_count = read_u16(bytes, 16)? as usize;
    if dependency_count > MAX_MANIFEST_DEPENDENCIES {
        return Err(ManifestError::InvalidEncoding);
    }
    let cpu_len = read_u16(bytes, 18)? as usize;
    let runtime_len = read_u16(bytes, 20)? as usize;
    let compiler_len = read_u16(bytes, 22)? as usize;
    let descriptor_len = read_u16(bytes, 24)? as usize;
    let entry_len = read_u16(bytes, 26)? as usize;
    if cpu_len == 0
        || cpu_len > 64
        || runtime_len == 0
        || runtime_len > 64
        || compiler_len == 0
        || compiler_len > 128
        || descriptor_len == 0
        || descriptor_len > 64
        || entry_len == 0
        || entry_len > 64
        || sdk_release == 0
    {
        return Err(ManifestError::InvalidEncoding);
    }

    let mut cursor = MANIFEST_V1_HEADER_BYTES;
    let cpu_baseline = take_text(bytes, &mut cursor, cpu_len, is_profile_identifier)?;
    let runtime_profile = take_text(bytes, &mut cursor, runtime_len, is_profile_identifier)?;
    let compiler_identity = take_text(bytes, &mut cursor, compiler_len, is_compiler_identity)?;
    let descriptor_symbol = take_text(bytes, &mut cursor, descriptor_len, is_symbol)?;
    let entry_point_symbol = take_text(bytes, &mut cursor, entry_len, is_symbol)?;
    let (expected_descriptor, expected_entry) = abi_symbols(abi);
    if descriptor_symbol != expected_descriptor || entry_point_symbol != expected_entry {
        return Err(ManifestError::InvalidEncoding);
    }

    let mut dependencies = Vec::with_capacity(dependency_count);
    for _ in 0..dependency_count {
        let dependency_len = read_u16_at(bytes, &mut cursor)? as usize;
        if dependency_len == 0 || dependency_len > MAX_MANIFEST_DEPENDENCY_NAME_BYTES {
            return Err(ManifestError::InvalidEncoding);
        }
        let dependency = take_text(bytes, &mut cursor, dependency_len, is_dependency_name)?;
        if dependencies
            .last()
            .is_some_and(|previous: &String| previous.as_bytes() >= dependency.as_bytes())
        {
            return Err(ManifestError::InvalidEncoding);
        }
        dependencies.push(dependency);
    }
    if cursor != bytes.len() {
        return Err(ManifestError::InvalidEncoding);
    }
    Ok(ArtifactManifestV1 {
        native_abi_version: abi,
        sdk_release,
        cpu_baseline,
        runtime_profile,
        compiler_identity,
        descriptor_symbol,
        entry_point_symbol,
        dependencies,
    })
}

fn abi_symbols(abi: u8) -> (&'static str, &'static str) {
    match abi {
        1 => ("prdb_query_descriptor_v1", "prdb_query_main_v1"),
        2 => ("prdb_query_descriptor_v2", "prdb_query_main_v2"),
        3 => ("prdb_query_descriptor_v3", "prdb_query_main_v3"),
        _ => unreachable!("ABI was range-checked"),
    }
}

/// Read the ELF64 dynamic dependency table without mapping or loading the
/// artifact. Only the Linux x86_64 ET_DYN profile is accepted.
pub fn decode_elf_needed_dependencies(artifact: &[u8]) -> Result<Vec<String>, ManifestError> {
    decode_elf_needed_dependencies_internal(artifact, None)
}

fn decode_elf_needed_dependencies_internal(
    artifact: &[u8],
    required_symbols: Option<(&str, u8, &str, u8)>,
) -> Result<Vec<String>, ManifestError> {
    if artifact.len() < 64
        || &artifact[..4] != b"\x7fELF"
        || artifact[4] != 2
        || artifact[5] != 1
        || artifact[6] != 1
        || read_elf_u16(artifact, 16)? != 3
        || read_elf_u16(artifact, 18)? != 62
        || read_elf_u32(artifact, 20)? != 1
        || read_elf_u16(artifact, 52)? != 64
        || read_elf_u16(artifact, 54)? != 56
    {
        return Err(ManifestError::InvalidArtifact);
    }
    let program_header_offset =
        usize::try_from(read_elf_u64(artifact, 32)?).map_err(|_| ManifestError::InvalidArtifact)?;
    let program_header_count = read_elf_u16(artifact, 56)? as usize;
    if program_header_count == 0 || program_header_count > 128 {
        return Err(ManifestError::InvalidArtifact);
    }
    let program_headers_bytes = program_header_count
        .checked_mul(56)
        .ok_or(ManifestError::InvalidArtifact)?;
    let program_headers_end = program_header_offset
        .checked_add(program_headers_bytes)
        .ok_or(ManifestError::InvalidArtifact)?;
    if program_headers_end > artifact.len() {
        return Err(ManifestError::InvalidArtifact);
    }

    let mut load_segments = Vec::new();
    let mut dynamic_segment = None;
    for index in 0..program_header_count {
        let header = program_header_offset + index * 56;
        let kind = read_elf_u32(artifact, header)?;
        if kind != 1 && kind != 2 {
            continue;
        }
        let offset = usize::try_from(read_elf_u64(artifact, header + 8)?)
            .map_err(|_| ManifestError::InvalidArtifact)?;
        let virtual_address = read_elf_u64(artifact, header + 16)?;
        let file_size = usize::try_from(read_elf_u64(artifact, header + 32)?)
            .map_err(|_| ManifestError::InvalidArtifact)?;
        let memory_size = read_elf_u64(artifact, header + 40)?;
        let file_end = offset
            .checked_add(file_size)
            .ok_or(ManifestError::InvalidArtifact)?;
        if file_end > artifact.len() || file_size as u64 > memory_size {
            return Err(ManifestError::InvalidArtifact);
        }
        let segment = ElfSegment {
            offset,
            virtual_address,
            file_size,
        };
        match kind {
            1 => load_segments.push(segment),
            2 if dynamic_segment.replace(segment).is_none() => {}
            2 => return Err(ManifestError::InvalidArtifact),
            _ => unreachable!(),
        }
    }
    let dynamic = dynamic_segment.ok_or(ManifestError::InvalidArtifact)?;
    if dynamic.file_size == 0 || dynamic.file_size % 16 != 0 || dynamic.file_size / 16 > 4096 {
        return Err(ManifestError::InvalidArtifact);
    }
    let mut needed_offsets = Vec::new();
    let mut string_table = None;
    let mut string_table_size = None;
    let mut symbol_table = None;
    let mut symbol_entry_size = None;
    let mut hash_table = None;
    let mut found_null = false;
    for index in 0..dynamic.file_size / 16 {
        let entry = dynamic.offset + index * 16;
        let tag = read_elf_i64(artifact, entry)?;
        let value = read_elf_u64(artifact, entry + 8)?;
        match tag {
            0 => {
                found_null = true;
                break;
            }
            1 => needed_offsets.push(value),
            4 if hash_table.replace(value).is_none() => {}
            6 if symbol_table.replace(value).is_none() => {}
            5 if string_table.replace(value).is_none() => {}
            10 if string_table_size.replace(value).is_none() => {}
            11 if symbol_entry_size.replace(value).is_none() => {}
            15 | 29 | 0x6fff_fefb | 0x6fff_fefc | 0x7fff_fffd | 0x7fff_ffff => {
                return Err(ManifestError::InvalidArtifact);
            }
            4 | 5 | 6 | 10 | 11 => return Err(ManifestError::InvalidArtifact),
            _ => {}
        }
    }
    if !found_null || needed_offsets.len() > MAX_MANIFEST_DEPENDENCIES {
        return Err(ManifestError::InvalidArtifact);
    }
    let string_table_address = string_table.ok_or(ManifestError::InvalidArtifact)?;
    let string_table_size =
        usize::try_from(string_table_size.ok_or(ManifestError::InvalidArtifact)?)
            .map_err(|_| ManifestError::InvalidArtifact)?;
    if string_table_size == 0 || string_table_size > artifact.len() {
        return Err(ManifestError::InvalidArtifact);
    }
    let string_table_offset =
        map_virtual_address(string_table_address, string_table_size, &load_segments)?;
    let string_table_end = string_table_offset
        .checked_add(string_table_size)
        .ok_or(ManifestError::InvalidArtifact)?;
    let table = artifact
        .get(string_table_offset..string_table_end)
        .ok_or(ManifestError::InvalidArtifact)?;
    if let Some(required_symbols) = required_symbols {
        let hash_address = hash_table.ok_or(ManifestError::InvalidArtifact)?;
        let symbol_table_address = symbol_table.ok_or(ManifestError::InvalidArtifact)?;
        if symbol_entry_size != Some(24) {
            return Err(ManifestError::InvalidArtifact);
        }
        validate_required_dynamic_symbols(
            artifact,
            &load_segments,
            hash_address,
            symbol_table_address,
            table,
            required_symbols,
        )?;
    }
    let mut dependencies = Vec::with_capacity(needed_offsets.len());
    let mut unique = HashSet::with_capacity(needed_offsets.len());
    for needed_offset in needed_offsets {
        let start = usize::try_from(needed_offset).map_err(|_| ManifestError::InvalidArtifact)?;
        let suffix = table.get(start..).ok_or(ManifestError::InvalidArtifact)?;
        let end = suffix
            .iter()
            .position(|byte| *byte == 0)
            .ok_or(ManifestError::InvalidArtifact)?;
        let name =
            std::str::from_utf8(&suffix[..end]).map_err(|_| ManifestError::InvalidArtifact)?;
        if !is_dependency_name(name) || !unique.insert(name.to_owned()) {
            return Err(ManifestError::InvalidArtifact);
        }
        dependencies.push(name.to_owned());
    }
    dependencies.sort();
    Ok(dependencies)
}

fn validate_required_dynamic_symbols(
    artifact: &[u8],
    load_segments: &[ElfSegment],
    hash_address: u64,
    symbol_table_address: u64,
    string_table: &[u8],
    required: (&str, u8, &str, u8),
) -> Result<(), ManifestError> {
    const MAX_DYNAMIC_SYMBOLS: usize = 262_144;
    let hash_header = map_virtual_address(hash_address, 8, load_segments)?;
    let bucket_count = read_elf_u32(artifact, hash_header)? as usize;
    let symbol_count = read_elf_u32(artifact, hash_header + 4)? as usize;
    if bucket_count == 0 || symbol_count == 0 || symbol_count > MAX_DYNAMIC_SYMBOLS {
        return Err(ManifestError::InvalidArtifact);
    }
    let hash_words = bucket_count
        .checked_add(symbol_count)
        .and_then(|count| count.checked_add(2))
        .ok_or(ManifestError::InvalidArtifact)?;
    let hash_size = hash_words
        .checked_mul(4)
        .ok_or(ManifestError::InvalidArtifact)?;
    let hash_offset = map_virtual_address(hash_address, hash_size, load_segments)?;
    let hash_end = hash_offset
        .checked_add(hash_size)
        .ok_or(ManifestError::InvalidArtifact)?;
    let hash_bytes = artifact
        .get(hash_offset..hash_end)
        .ok_or(ManifestError::InvalidArtifact)?;
    for chunk in hash_bytes[8..].chunks_exact(4) {
        let index = u32::from_le_bytes(
            chunk
                .try_into()
                .map_err(|_| ManifestError::InvalidArtifact)?,
        ) as usize;
        if index >= symbol_count {
            return Err(ManifestError::InvalidArtifact);
        }
    }

    let symbol_table_size = symbol_count
        .checked_mul(24)
        .ok_or(ManifestError::InvalidArtifact)?;
    let symbol_table_offset =
        map_virtual_address(symbol_table_address, symbol_table_size, load_segments)?;
    let symbol_table_end = symbol_table_offset
        .checked_add(symbol_table_size)
        .ok_or(ManifestError::InvalidArtifact)?;
    let symbol_table = artifact
        .get(symbol_table_offset..symbol_table_end)
        .ok_or(ManifestError::InvalidArtifact)?;
    let mut descriptor_found = false;
    let mut entry_found = false;
    for index in 0..symbol_count {
        let symbol = &symbol_table[index * 24..(index + 1) * 24];
        let name_offset = u32::from_le_bytes(
            symbol[0..4]
                .try_into()
                .map_err(|_| ManifestError::InvalidArtifact)?,
        ) as usize;
        if name_offset == 0 {
            continue;
        }
        let name_suffix = string_table
            .get(name_offset..)
            .ok_or(ManifestError::InvalidArtifact)?;
        let name_end = name_suffix
            .iter()
            .position(|byte| *byte == 0)
            .ok_or(ManifestError::InvalidArtifact)?;
        let name = &name_suffix[..name_end];
        let binding = symbol[4] >> 4;
        let kind = symbol[4] & 0x0f;
        let visibility = symbol[5] & 0x03;
        let section_index = u16::from_le_bytes(
            symbol[6..8]
                .try_into()
                .map_err(|_| ManifestError::InvalidArtifact)?,
        );
        if !matches!(binding, 1 | 2) || !matches!(visibility, 0 | 3) || section_index == 0 {
            continue;
        }
        descriptor_found |= name == required.0.as_bytes() && kind == required.1;
        entry_found |= name == required.2.as_bytes() && kind == required.3;
    }
    if descriptor_found && entry_found {
        Ok(())
    } else {
        Err(ManifestError::InvalidArtifact)
    }
}

#[derive(Clone, Copy)]
struct ElfSegment {
    offset: usize,
    virtual_address: u64,
    file_size: usize,
}

fn map_virtual_address(
    address: u64,
    length: usize,
    segments: &[ElfSegment],
) -> Result<usize, ManifestError> {
    let length = u64::try_from(length).map_err(|_| ManifestError::InvalidArtifact)?;
    for segment in segments {
        let segment_end = segment
            .virtual_address
            .checked_add(segment.file_size as u64)
            .ok_or(ManifestError::InvalidArtifact)?;
        let requested_end = address
            .checked_add(length)
            .ok_or(ManifestError::InvalidArtifact)?;
        if address >= segment.virtual_address && requested_end <= segment_end {
            let delta = usize::try_from(address - segment.virtual_address)
                .map_err(|_| ManifestError::InvalidArtifact)?;
            return segment
                .offset
                .checked_add(delta)
                .ok_or(ManifestError::InvalidArtifact);
        }
    }
    Err(ManifestError::InvalidArtifact)
}

fn read_elf_u16(bytes: &[u8], offset: usize) -> Result<u16, ManifestError> {
    let value = bytes
        .get(
            offset
                ..offset
                    .checked_add(2)
                    .ok_or(ManifestError::InvalidArtifact)?,
        )
        .ok_or(ManifestError::InvalidArtifact)?;
    Ok(u16::from_le_bytes([value[0], value[1]]))
}

fn read_elf_u32(bytes: &[u8], offset: usize) -> Result<u32, ManifestError> {
    let value = bytes
        .get(
            offset
                ..offset
                    .checked_add(4)
                    .ok_or(ManifestError::InvalidArtifact)?,
        )
        .ok_or(ManifestError::InvalidArtifact)?;
    Ok(u32::from_le_bytes([value[0], value[1], value[2], value[3]]))
}

fn read_elf_u64(bytes: &[u8], offset: usize) -> Result<u64, ManifestError> {
    let value = bytes
        .get(
            offset
                ..offset
                    .checked_add(8)
                    .ok_or(ManifestError::InvalidArtifact)?,
        )
        .ok_or(ManifestError::InvalidArtifact)?;
    Ok(u64::from_le_bytes(
        value
            .try_into()
            .map_err(|_| ManifestError::InvalidArtifact)?,
    ))
}

fn read_elf_i64(bytes: &[u8], offset: usize) -> Result<i64, ManifestError> {
    Ok(read_elf_u64(bytes, offset)? as i64)
}

fn read_u16(bytes: &[u8], offset: usize) -> Result<u16, ManifestError> {
    let value = bytes
        .get(offset..offset + 2)
        .ok_or(ManifestError::InvalidEncoding)?;
    Ok(u16::from_be_bytes([value[0], value[1]]))
}

fn read_u32(bytes: &[u8], offset: usize) -> Result<u32, ManifestError> {
    let value = bytes
        .get(offset..offset + 4)
        .ok_or(ManifestError::InvalidEncoding)?;
    Ok(u32::from_be_bytes([value[0], value[1], value[2], value[3]]))
}

fn read_u16_at(bytes: &[u8], cursor: &mut usize) -> Result<u16, ManifestError> {
    let value = read_u16(bytes, *cursor)?;
    *cursor = cursor
        .checked_add(2)
        .ok_or(ManifestError::InvalidEncoding)?;
    Ok(value)
}

fn take_text(
    bytes: &[u8],
    cursor: &mut usize,
    length: usize,
    valid: fn(&str) -> bool,
) -> Result<String, ManifestError> {
    let end = cursor
        .checked_add(length)
        .ok_or(ManifestError::InvalidEncoding)?;
    let value = bytes
        .get(*cursor..end)
        .ok_or(ManifestError::InvalidEncoding)?;
    let text = std::str::from_utf8(value).map_err(|_| ManifestError::InvalidEncoding)?;
    if !valid(text) {
        return Err(ManifestError::InvalidEncoding);
    }
    *cursor = end;
    Ok(text.to_owned())
}

fn is_profile_identifier(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 64
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || b"._-".contains(&byte))
}

fn is_compiler_identity(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 128
        && value.bytes().all(|byte| (0x20..=0x7e).contains(&byte))
}

fn is_symbol(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 64
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || byte == b'_')
}

fn is_dependency_name(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= MAX_MANIFEST_DEPENDENCY_NAME_BYTES
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || b"._+-".contains(&byte))
        && !value.contains("..")
}

#[cfg(test)]
mod tests {
    use super::{
        ArtifactCompatibilityProfile, ArtifactManifestV1, CLIENT_PROTOCOL_MAJOR,
        CLIENT_PROTOCOL_MINOR, MANIFEST_V1_HEADER_BYTES, MAX_MANIFEST_DEPENDENCIES, ManifestError,
        VersionedSubmissionMetadataValidator, decode_elf_needed_dependencies, decode_manifest_v1,
    };
    use crate::supervisor::{
        ChunkKind, PrepareSubmission, StagingError, StagingLimits, StagingOwner, StagingStore,
        SubmissionMetadata, SubmissionMetadataValidator, WorkloadClass,
    };
    use sha2::{Digest, Sha256};
    use std::collections::HashSet;
    use std::sync::Arc;

    fn manifest() -> ArtifactManifestV1 {
        ArtifactManifestV1 {
            native_abi_version: 3,
            sdk_release: 1,
            cpu_baseline: "x86_64_v1".to_owned(),
            runtime_profile: "glibc_2_31".to_owned(),
            compiler_identity: "TinyCC-0.9.27".to_owned(),
            descriptor_symbol: "prdb_query_descriptor_v3".to_owned(),
            entry_point_symbol: "prdb_query_main_v3".to_owned(),
            dependencies: vec!["libc.so.6".to_owned(), "libm.so.6".to_owned()],
        }
    }

    fn encode(value: &ArtifactManifestV1) -> Vec<u8> {
        let fields = [
            value.cpu_baseline.as_bytes(),
            value.runtime_profile.as_bytes(),
            value.compiler_identity.as_bytes(),
            value.descriptor_symbol.as_bytes(),
            value.entry_point_symbol.as_bytes(),
        ];
        let mut bytes = b"PMF1".to_vec();
        bytes.extend([1, 1, value.native_abi_version, 0]);
        bytes.extend(CLIENT_PROTOCOL_MAJOR.to_be_bytes());
        bytes.extend(CLIENT_PROTOCOL_MINOR.to_be_bytes());
        bytes.extend(value.sdk_release.to_be_bytes());
        bytes.extend((value.dependencies.len() as u16).to_be_bytes());
        for field in fields {
            bytes.extend((field.len() as u16).to_be_bytes());
        }
        for field in fields {
            bytes.extend_from_slice(field);
        }
        for dependency in &value.dependencies {
            bytes.extend((dependency.len() as u16).to_be_bytes());
            bytes.extend_from_slice(dependency.as_bytes());
        }
        bytes
    }

    fn metadata(manifest: Vec<u8>) -> SubmissionMetadata {
        SubmissionMetadata {
            manifest_version: 1,
            manifest,
            scope_version: 1,
            requested_scope: requested_scope(),
            deadline_ms: 30_000,
            workload_class: WorkloadClass::Operational,
        }
    }

    fn requested_scope() -> Vec<u8> {
        let mut scope = b"PSC1\0\x01\x02\x02".to_vec();
        scope.extend([1; 16]);
        scope
    }

    fn profile() -> ArtifactCompatibilityProfile {
        ArtifactCompatibilityProfile::new(
            "x86_64_v1".to_owned(),
            "glibc_2_31".to_owned(),
            1,
            ["libc.so.6".to_owned(), "libm.so.6".to_owned()]
                .into_iter()
                .collect(),
        )
        .unwrap()
    }

    fn elf_with_dependencies(dependencies: &[&str]) -> Vec<u8> {
        elf_with_dependencies_and_symbols(
            dependencies,
            &[("prdb_query_descriptor_v3", 1), ("prdb_query_main_v3", 2)],
        )
    }

    fn elf_with_dependencies_and_symbols(dependencies: &[&str], symbols: &[(&str, u8)]) -> Vec<u8> {
        let program_headers_offset = 64usize;
        let dynamic_offset = program_headers_offset + 2 * 56;
        let dynamic_entry_count = dependencies.len() + 6;
        let dynamic_size = dynamic_entry_count * 16;
        let symbol_table_offset = dynamic_offset + dynamic_size;
        let symbol_table_size = (symbols.len() + 1) * 24;
        let hash_table_offset = symbol_table_offset + symbol_table_size;
        let hash_table_size = 8 + 4 * (1 + symbols.len() + 1);
        let string_table_offset = hash_table_offset + hash_table_size;
        let mut strings = vec![0];
        let mut dependency_offsets = Vec::new();
        for dependency in dependencies {
            dependency_offsets.push(strings.len() as u64);
            strings.extend_from_slice(dependency.as_bytes());
            strings.push(0);
        }
        let mut symbol_offsets = Vec::new();
        for (symbol, _) in symbols {
            symbol_offsets.push(strings.len() as u32);
            strings.extend_from_slice(symbol.as_bytes());
            strings.push(0);
        }
        let file_size = string_table_offset + strings.len();
        let mut bytes = vec![0; file_size];
        bytes[..4].copy_from_slice(b"\x7fELF");
        bytes[4] = 2;
        bytes[5] = 1;
        bytes[6] = 1;
        bytes[16..18].copy_from_slice(&3u16.to_le_bytes());
        bytes[18..20].copy_from_slice(&62u16.to_le_bytes());
        bytes[20..24].copy_from_slice(&1u32.to_le_bytes());
        bytes[32..40].copy_from_slice(&(program_headers_offset as u64).to_le_bytes());
        bytes[52..54].copy_from_slice(&64u16.to_le_bytes());
        bytes[54..56].copy_from_slice(&56u16.to_le_bytes());
        bytes[56..58].copy_from_slice(&2u16.to_le_bytes());

        let load = program_headers_offset;
        bytes[load..load + 4].copy_from_slice(&1u32.to_le_bytes());
        bytes[load + 4..load + 8].copy_from_slice(&5u32.to_le_bytes());
        bytes[load + 8..load + 16].copy_from_slice(&0u64.to_le_bytes());
        bytes[load + 16..load + 24].copy_from_slice(&0u64.to_le_bytes());
        bytes[load + 32..load + 40].copy_from_slice(&(file_size as u64).to_le_bytes());
        bytes[load + 40..load + 48].copy_from_slice(&(file_size as u64).to_le_bytes());

        let dynamic = load + 56;
        bytes[dynamic..dynamic + 4].copy_from_slice(&2u32.to_le_bytes());
        bytes[dynamic + 4..dynamic + 8].copy_from_slice(&4u32.to_le_bytes());
        bytes[dynamic + 8..dynamic + 16].copy_from_slice(&(dynamic_offset as u64).to_le_bytes());
        bytes[dynamic + 16..dynamic + 24].copy_from_slice(&(dynamic_offset as u64).to_le_bytes());
        bytes[dynamic + 32..dynamic + 40].copy_from_slice(&(dynamic_size as u64).to_le_bytes());
        bytes[dynamic + 40..dynamic + 48].copy_from_slice(&(dynamic_size as u64).to_le_bytes());

        let mut entry = dynamic_offset;
        let mut write_dynamic = |tag: i64, value: u64| {
            bytes[entry..entry + 8].copy_from_slice(&tag.to_le_bytes());
            bytes[entry + 8..entry + 16].copy_from_slice(&value.to_le_bytes());
            entry += 16;
        };
        write_dynamic(4, hash_table_offset as u64);
        write_dynamic(6, symbol_table_offset as u64);
        write_dynamic(5, string_table_offset as u64);
        write_dynamic(10, strings.len() as u64);
        write_dynamic(11, 24);
        for offset in dependency_offsets {
            write_dynamic(1, offset);
        }
        write_dynamic(0, 0);

        let hash = hash_table_offset;
        bytes[hash..hash + 4].copy_from_slice(&1u32.to_le_bytes());
        bytes[hash + 4..hash + 8]
            .copy_from_slice(&u32::try_from(symbols.len() + 1).unwrap().to_le_bytes());
        bytes[hash + 8..hash + 12].copy_from_slice(&u32::from(!symbols.is_empty()).to_le_bytes());

        for (index, ((_, symbol_type), name_offset)) in
            symbols.iter().zip(symbol_offsets).enumerate()
        {
            let symbol_offset = symbol_table_offset + (index + 1) * 24;
            bytes[symbol_offset..symbol_offset + 4].copy_from_slice(&name_offset.to_le_bytes());
            bytes[symbol_offset + 4] = (1 << 4) | symbol_type;
            bytes[symbol_offset + 6..symbol_offset + 8].copy_from_slice(&1u16.to_le_bytes());
            bytes[symbol_offset + 8..symbol_offset + 16].copy_from_slice(&0x1000u64.to_le_bytes());
            bytes[symbol_offset + 16..symbol_offset + 24].copy_from_slice(&8u64.to_le_bytes());
            let chain_offset = hash + 12 + (index + 1) * 4;
            bytes[chain_offset..chain_offset + 4].copy_from_slice(&0u32.to_le_bytes());
        }
        bytes[string_table_offset..].copy_from_slice(&strings);
        bytes
    }

    #[test]
    fn manifest_v1_matches_its_canonical_fixture_and_abi_symbols() {
        let value = manifest();
        let bytes = encode(&value);
        assert_eq!(
            bytes.len(),
            MANIFEST_V1_HEADER_BYTES
                + value.cpu_baseline.len()
                + value.runtime_profile.len()
                + value.compiler_identity.len()
                + value.descriptor_symbol.len()
                + value.entry_point_symbol.len()
                + value
                    .dependencies
                    .iter()
                    .map(|dependency| dependency.len() + 2)
                    .sum::<usize>()
        );
        assert_eq!(decode_manifest_v1(&bytes), Ok(value));
    }

    #[test]
    fn manifest_v1_rejects_incompatible_target_abi_protocol_and_symbols() {
        let valid = encode(&manifest());
        let mut wrong_target = valid.clone();
        wrong_target[4] = 2;
        assert_eq!(
            decode_manifest_v1(&wrong_target),
            Err(ManifestError::UnsupportedTarget)
        );
        let mut unsupported_abi = valid.clone();
        unsupported_abi[6] = 4;
        assert_eq!(
            decode_manifest_v1(&unsupported_abi),
            Err(ManifestError::UnsupportedAbi)
        );
        let mut unsupported_protocol = valid.clone();
        unsupported_protocol[11] = 1;
        assert_eq!(
            decode_manifest_v1(&unsupported_protocol),
            Err(ManifestError::UnsupportedProtocol)
        );
        let mut wrong_entry = manifest();
        wrong_entry.entry_point_symbol = "client_chosen_entry".to_owned();
        assert_eq!(
            decode_manifest_v1(&encode(&wrong_entry)),
            Err(ManifestError::InvalidEncoding)
        );
    }

    #[test]
    fn manifest_v1_rejects_malformed_strings_dependencies_and_trailing_bytes() {
        let valid = encode(&manifest());
        let mut trailing = valid.clone();
        trailing.push(0);
        assert_eq!(
            decode_manifest_v1(&trailing),
            Err(ManifestError::InvalidEncoding)
        );
        let mut unsorted = manifest();
        unsorted.dependencies.reverse();
        assert_eq!(
            decode_manifest_v1(&encode(&unsorted)),
            Err(ManifestError::InvalidEncoding)
        );
        let mut duplicate = manifest();
        duplicate.dependencies[1] = duplicate.dependencies[0].clone();
        assert_eq!(
            decode_manifest_v1(&encode(&duplicate)),
            Err(ManifestError::InvalidEncoding)
        );
        let mut too_many = manifest();
        too_many.dependencies = (0..=MAX_MANIFEST_DEPENDENCIES)
            .map(|index| format!("lib{index}.so"))
            .collect();
        too_many.dependencies.sort();
        assert_eq!(
            decode_manifest_v1(&encode(&too_many)),
            Err(ManifestError::InvalidEncoding)
        );
    }

    #[test]
    fn configured_metadata_validator_checks_profile_and_dependency_allowlist() {
        let validator = VersionedSubmissionMetadataValidator::new(profile());
        assert!(validator.supports_versions(1, 1));
        assert!(!validator.supports_versions(2, 1));
        assert!(validator.validate(&metadata(encode(&manifest()))));

        let mut incompatible_profile = manifest();
        incompatible_profile.runtime_profile = "musl_1_2".to_owned();
        assert!(!validator.validate(&metadata(encode(&incompatible_profile))));

        let mut unavailable_dependency = manifest();
        unavailable_dependency
            .dependencies
            .push("libpthread.so.0".to_owned());
        unavailable_dependency.dependencies.sort();
        assert!(!validator.validate(&metadata(encode(&unavailable_dependency))));

        let mut malformed_scope = metadata(encode(&manifest()));
        malformed_scope.requested_scope = b"scope".to_vec();
        assert!(!validator.validate(&malformed_scope));
    }

    #[test]
    fn compatibility_profile_rejects_invalid_allowed_dependency_names() {
        let invalid = ArtifactCompatibilityProfile::new(
            "x86_64_v1".to_owned(),
            "glibc_2_31".to_owned(),
            1,
            HashSet::from(["../libc.so".to_owned()]),
        );
        assert_eq!(invalid.err(), Some(ManifestError::InvalidEncoding));
    }

    #[test]
    fn elf_dependency_reader_checks_loaded_names_without_loading_the_artifact() {
        let artifact = elf_with_dependencies(&["libc.so.6", "libm.so.6"]);
        assert_eq!(
            decode_elf_needed_dependencies(&artifact),
            Ok(vec!["libc.so.6".to_owned(), "libm.so.6".to_owned()])
        );
        assert_eq!(
            decode_elf_needed_dependencies(b"not an ELF library"),
            Err(ManifestError::InvalidArtifact)
        );
        let mut search_path_artifact = artifact.clone();
        let dynamic_offset = 64 + 2 * 56;
        let first_needed_offset = dynamic_offset + 2 * 16;
        search_path_artifact[first_needed_offset..first_needed_offset + 8]
            .copy_from_slice(&15i64.to_le_bytes());
        assert_eq!(
            decode_elf_needed_dependencies(&search_path_artifact),
            Err(ManifestError::InvalidArtifact)
        );

        let validator = VersionedSubmissionMetadataValidator::new(profile());
        let valid_metadata = metadata(encode(&manifest()));
        assert!(validator.validate_artifact(&valid_metadata, &artifact));
        assert!(
            !validator.validate_artifact(&valid_metadata, &elf_with_dependencies(&["libc.so.6"]))
        );
        assert!(!validator.validate_artifact(
            &valid_metadata,
            &elf_with_dependencies_and_symbols(
                &["libc.so.6", "libm.so.6"],
                &[("prdb_query_main_v3", 2)],
            )
        ));
        assert!(!validator.validate_artifact(
            &valid_metadata,
            &elf_with_dependencies_and_symbols(
                &["libc.so.6", "libm.so.6"],
                &[("prdb_query_descriptor_v3", 2), ("prdb_query_main_v3", 2),],
            )
        ));
    }

    #[test]
    fn staging_seal_validates_manifest_against_its_pinned_artifact() {
        let owner = StagingOwner::authenticated([1; 16], [2; 16]);
        let validator = Arc::new(VersionedSubmissionMetadataValidator::new(profile()));

        let accepts = elf_with_dependencies(&["libc.so.6", "libm.so.6"]);
        let accept_store = StagingStore::new(StagingLimits::default(), validator.clone()).unwrap();
        let accepted = prepare_and_upload(&accept_store, owner, &manifest(), &accepts);
        assert_eq!(accept_store.seal(owner, accepted), Ok(()));

        let mismatched = elf_with_dependencies(&["libc.so.6"]);
        let reject_store = StagingStore::new(StagingLimits::default(), validator).unwrap();
        let rejected = prepare_and_upload(&reject_store, owner, &manifest(), &mismatched);
        assert_eq!(
            reject_store.seal(owner, rejected),
            Err(StagingError::InvalidMetadata)
        );
    }

    fn prepare_and_upload(
        store: &StagingStore,
        owner: StagingOwner,
        manifest: &ArtifactManifestV1,
        artifact: &[u8],
    ) -> [u8; 16] {
        let parameter_digest = Sha256::digest([]).into();
        let reservation = store
            .prepare(
                owner,
                PrepareSubmission {
                    manifest_version: 1,
                    manifest: encode(manifest),
                    artifact_bytes: artifact.len() as u64,
                    artifact_sha256: Sha256::digest(artifact).into(),
                    scope_version: 1,
                    requested_scope: requested_scope(),
                    parameter_bytes: 0,
                    parameter_sha256: parameter_digest,
                    deadline_ms: 30_000,
                    workload_class: WorkloadClass::Operational,
                },
            )
            .unwrap();
        store
            .upload_chunk(
                owner,
                reservation.invocation_id,
                ChunkKind::Artifact,
                0,
                artifact,
            )
            .unwrap();
        reservation.invocation_id
    }
}
