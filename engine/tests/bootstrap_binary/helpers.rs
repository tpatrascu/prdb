#![allow(dead_code)]
use prdb_engine::catalog::{decode_registered_envelope, encode_envelope, family_schema};
use prdb_engine::metadata::EngineMetadata;
use prdb_engine::schema::{CATALOG, ENGINE_META};
use redb::{Database, ReadableDatabase};
use std::process::Command;

pub struct Fixture(pub std::path::PathBuf);

impl Fixture {
    pub fn new() -> Self {
        let directory = std::env::temp_dir().join(format!(
            "prdb-init-cli-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .expect("system time")
                .as_nanos()
        ));
        std::fs::create_dir(&directory).expect("create fixture directory");
        Self(directory)
    }

    pub fn instance_dir(&self) -> std::path::PathBuf {
        self.0.join("instance")
    }

    pub fn credential_file(&self) -> std::path::PathBuf {
        self.0.join("admin.credentials")
    }
}

impl Drop for Fixture {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

pub fn initialize_command(fixture: &Fixture, resume: bool) -> std::process::Output {
    let mut command = Command::new(env!("CARGO_BIN_EXE_prdb-init"));
    command
        .arg("--instance-dir")
        .arg(fixture.instance_dir())
        .arg("--administrator")
        .arg("first_admin")
        .arg("--credential-file")
        .arg(fixture.credential_file())
        .arg("--expires-in-seconds")
        .arg("86400");
    if resume {
        command.arg("--resume");
    }
    command.output().expect("run prdb-init")
}

pub fn downgrade_ready_fixture_to_v1(database_path: &std::path::Path) -> EngineMetadata {
    let database = Database::open(database_path).expect("open fixture for v1 conversion");
    let mut metadata = {
        let read = database.begin_read().expect("read fixture metadata");
        let table = read.open_table(ENGINE_META).expect("open metadata table");
        let encoded = table.get(&b"format"[..]).expect("read metadata").unwrap();
        EngineMetadata::decode(encoded.value()).expect("decode fixture metadata")
    };
    metadata.catalog_version = 1;
    let encoded_metadata = metadata.encode().expect("encode v1 metadata");
    let mut database_key = 1u16.to_be_bytes().to_vec();
    database_key.push(1);
    database_key.extend_from_slice(&metadata.database_id);
    let mut fields = {
        let read = database.begin_read().expect("read fixture descriptor");
        let table = read.open_table(CATALOG).expect("open catalog table");
        decode_registered_envelope(
            table
                .get(database_key.as_slice())
                .expect("read database record")
                .unwrap()
                .value(),
        )
        .expect("decode database record")
        .fields
    };
    for field in &mut fields {
        if field.tag == 2 {
            field.payload = 1u32.to_le_bytes().to_vec();
        }
        if field.tag == 3 {
            assert_eq!(
                u32::from_le_bytes(field.payload[..4].try_into().unwrap()),
                31
            );
            field.payload[..4].copy_from_slice(&30u32.to_le_bytes());
            field.payload.truncate(4 + 30 * 20);
        }
    }
    let encoded_database = encode_envelope(
        &family_schema(1).expect("database family schema"),
        &database_key,
        &fields,
    )
    .expect("encode v1 database descriptor");
    let v2_only_keys = {
        use redb::ReadableTable;
        let read = database.begin_read().expect("read v2-only keys");
        let table = read.open_table(CATALOG).expect("open catalog table");
        let mut keys = Vec::new();
        for entry in table.iter().expect("iter catalog") {
            let (k, _) = entry.expect("entry");
            let kb = k.value();
            if kb.starts_with(&18u16.to_be_bytes())
                || (kb.starts_with(&2u16.to_be_bytes())
                    && kb.len() == 19
                    && kb[3..18] == [0x80; 15]
                    && (4..=7).contains(&kb[18]))
                || (kb.starts_with(&15u16.to_be_bytes())
                    && kb.len() == 39
                    && kb[37..39] == 6u16.to_be_bytes())
            {
                keys.push(kb.to_vec());
            }
        }
        keys
    };
    {
        let write = database.begin_write().expect("begin v1 fixture conversion");
        {
            let mut catalog = write.open_table(CATALOG).expect("open fixture catalog");
            for key in &v2_only_keys {
                catalog.remove(key.as_slice()).expect("remove v2-only key");
            }
            catalog
                .insert(database_key.as_slice(), encoded_database.as_slice())
                .expect("write v1 database descriptor");
        }
        write
            .open_table(ENGINE_META)
            .expect("open fixture metadata")
            .insert(&b"format"[..], &encoded_metadata[..])
            .expect("write v1 metadata");
        write.commit().expect("commit v1 fixture conversion");
    }
    drop(database);
    metadata
}
