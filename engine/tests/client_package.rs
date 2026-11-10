use prdb_engine::client_protocol::{
    ClientFrame, ClientFrameDecoder, decode_client_chunk, decode_client_credentials,
    decode_client_prepare,
};
use prdb_engine::client_submit::SubmissionClient;
use prdb_engine::submission_manifest::decode_manifest_v1;
use sha2::{Digest, Sha256};
use std::fs;
use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::path::PathBuf;
use std::process::Command;
use zeroize::Zeroize;

fn unique_temp_dir() -> PathBuf {
    let mut random = [0u8; 12];
    getrandom::fill(&mut random).unwrap();
    let suffix = random
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect::<String>();
    std::env::temp_dir().join(format!(
        "prdb-client-package-{}-{suffix}",
        std::process::id()
    ))
}

#[test]
fn packager_compiles_and_atomically_publishes_metadata_without_loading_query() {
    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .unwrap()
        .to_path_buf();
    let temp = unique_temp_dir();
    fs::create_dir(&temp).unwrap();
    let sdk_build = temp.join("sdk");
    let source = temp.join("query.c");
    let scope = temp.join("scope.psc1");
    let package = temp.join("package");
    fs::write(
        &source,
        b"#include \"prdb_query_abi_v4.h\"\n\
          #include <stdlib.h>\n\
          const prdb_query_descriptor_v4_record prdb_query_descriptor_v4 = {\n\
            sizeof(prdb_query_descriptor_v4_record), 4, sizeof(prdb_host_api_v4), sizeof(prdb_invocation_v4)};\n\
          __attribute__((constructor)) static void must_not_load(void) { abort(); }\n\
          int32_t prdb_query_main_v4(const prdb_host_api_v4 *h, const prdb_invocation_v4 *i) { (void)h; (void)i; return 0; }\n",
    )
    .unwrap();
    let mut scope_bytes = b"PSC1\0\x01\x02\x02".to_vec();
    scope_bytes.extend([1; 16]);
    fs::write(&scope, &scope_bytes).unwrap();

    let build = Command::new("make")
        .current_dir(&root)
        .args([
            "-f",
            "engine/sdk/Makefile",
            &format!("BUILD_DIR={}", sdk_build.display()),
            "all",
        ])
        .output()
        .unwrap();
    assert!(
        build.status.success(),
        "{}",
        String::from_utf8_lossy(&build.stderr)
    );

    let archive = sdk_build.join("libprdb_sdk.a");
    let status = Command::new(env!("CARGO_BIN_EXE_prdb-packager"))
        .arg("--source")
        .arg(&source)
        .arg("--sdk-archive")
        .arg(&archive)
        .arg("--include-dir")
        .arg(root.join("engine/include"))
        .arg("--scope")
        .arg(&scope)
        .arg("--output-dir")
        .arg(&package)
        .args([
            "--abi",
            "4",
            "--sdk-release",
            "1",
            "--cpu-baseline",
            "x86_64-v1",
            "--runtime-profile",
            "test-runtime",
            "--compiler-identity",
            "TinyCC integration test",
        ])
        .output()
        .unwrap();
    assert!(
        status.status.success(),
        "stdout={} stderr={}",
        String::from_utf8_lossy(&status.stdout),
        String::from_utf8_lossy(&status.stderr)
    );

    let artifact = fs::read(package.join("query.so")).unwrap();
    let manifest_bytes = fs::read(package.join("manifest.pmf1")).unwrap();
    let manifest = decode_manifest_v1(&manifest_bytes).unwrap();
    assert_eq!(manifest.native_abi_version, 4);
    assert_eq!(manifest.sdk_release, 1);
    assert_eq!(manifest.cpu_baseline, "x86_64-v1");
    assert_eq!(manifest.runtime_profile, "test-runtime");
    assert_eq!(manifest.descriptor_symbol, "prdb_query_descriptor_v4");
    assert_eq!(manifest.entry_point_symbol, "prdb_query_main_v4");
    assert_eq!(
        fs::read(package.join("requested_scope.psc1")).unwrap(),
        scope_bytes
    );
    let digest = Sha256::digest(&artifact)
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect::<String>()
        + "\n";
    assert_eq!(
        fs::read_to_string(package.join("artifact.sha256")).unwrap(),
        digest
    );
    assert!(!temp.join("constructor-ran").exists());

    let expected_artifact = artifact.clone();
    let expected_manifest = manifest_bytes.clone();
    let expected_scope = scope_bytes.clone();
    let parameters = vec![0x5a; 300_000];
    let expected_parameters = parameters.clone();
    let receipt_path = temp.join("invocation.receipt");
    let server_receipt_path = receipt_path.clone();
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    let server = std::thread::spawn(move || {
        let (mut stream, _) = listener.accept().unwrap();
        stream
            .set_read_timeout(Some(std::time::Duration::from_secs(10)))
            .unwrap();
        let mut decoder = ClientFrameDecoder::default();
        let hello = read_client_frame(&mut stream, &mut decoder);
        assert_eq!(hello.kind, 1);
        write_server_response(&mut stream, &hello, br#"{"transport":"plaintext_tcp","limits":{"chunk_bytes":131072,"control_bytes":32768,"payload_bytes":524288},"mode":"query","minor":0,"major":1}"#);
        let auth = read_client_frame(&mut stream, &mut decoder);
        assert_eq!(auth.kind, 2);
        let decoded_credentials = decode_client_credentials(&auth.payload).unwrap();
        assert_eq!(decoded_credentials.token_id, [7; 16]);
        assert_eq!(decoded_credentials.token_secret, [9; 32]);
        let mut decoded_credentials = decoded_credentials;
        decoded_credentials.token_secret.zeroize();
        write_server_response(
            &mut stream,
            &auth,
            br#"{"session_id":"abababababababababababababababab","authenticated":true}"#,
        );
        let prepare = read_client_frame(&mut stream, &mut decoder);
        assert_eq!(prepare.kind, 3);
        let decoded = decode_client_prepare(&prepare.payload).unwrap();
        assert_eq!(decoded.artifact_bytes as usize, expected_artifact.len());
        assert_eq!(decoded.manifest, expected_manifest);
        assert_eq!(decoded.requested_scope, expected_scope);
        assert_eq!(decoded.parameter_bytes as usize, expected_parameters.len());
        let expected_parameter_hash: [u8; 32] = Sha256::digest(&expected_parameters).into();
        assert_eq!(decoded.parameter_sha256, expected_parameter_hash);
        write_server_response(&mut stream, &prepare, br#"{"invocation_id":"01010101010101010101010101010101","artifact_chunk_bytes":131072,"parameter_chunk_bytes":131072}"#);
        let mut offset = 0usize;
        while offset < expected_artifact.len() {
            let frame = read_client_frame(&mut stream, &mut decoder);
            assert_eq!(frame.kind, 4);
            let chunk = decode_client_chunk(&frame.payload).unwrap();
            assert_eq!(chunk.invocation_id, [1; 16]);
            assert_eq!(chunk.offset, offset as u64);
            let next_offset = offset + chunk.data.len();
            assert_eq!(chunk.data, expected_artifact[offset..next_offset]);
            offset = next_offset;
            write_server_response(
                &mut stream,
                &frame,
                format!("{{\"next_offset\":\"{offset}\"}}").as_bytes(),
            );
        }
        let mut parameter_offset = 0usize;
        while parameter_offset < expected_parameters.len() {
            let frame = read_client_frame(&mut stream, &mut decoder);
            assert_eq!(frame.kind, 5);
            let chunk = decode_client_chunk(&frame.payload).unwrap();
            assert_eq!(chunk.invocation_id, [1; 16]);
            assert_eq!(chunk.offset, parameter_offset as u64);
            let next_offset = parameter_offset + chunk.data.len();
            assert_eq!(
                chunk.data,
                expected_parameters[parameter_offset..next_offset]
            );
            parameter_offset = next_offset;
            write_server_response(
                &mut stream,
                &frame,
                format!("{{\"next_offset\":\"{parameter_offset}\"}}").as_bytes(),
            );
        }
        let seal = read_client_frame(&mut stream, &mut decoder);
        assert_eq!(seal.kind, 6);
        write_server_response(&mut stream, &seal, br#"{"state":"prepared"}"#);
        let start = read_client_frame(&mut stream, &mut decoder);
        assert_eq!(start.kind, 7);
        assert_eq!(
            fs::read_to_string(&server_receipt_path).unwrap(),
            "PDBR1 01010101010101010101010101010101\n"
        );
        let mut stream_bytes = ClientFrame {
            kind: 100,
            request_id: start.request_id,
            response_sequence: 0,
            payload: br#"{"state":"accepted"}"#.to_vec(),
        }
        .encode()
        .unwrap();
        stream_bytes.extend(
            ClientFrame {
                kind: 102,
                request_id: start.request_id,
                response_sequence: 1,
                payload: b"provisional-row".to_vec(),
            }
            .encode()
            .unwrap(),
        );
        stream_bytes.extend(
            ClientFrame {
                kind: 103,
                request_id: start.request_id,
                response_sequence: 2,
                payload: br#"{"invocation_id":"01010101010101010101010101010101","outcome":"committed","reason_code":"none"}"#.to_vec(),
            }
            .encode()
            .unwrap(),
        );
        stream.write_all(&stream_bytes).unwrap();
        let status = read_client_frame(&mut stream, &mut decoder);
        assert_eq!(status.kind, 8);
        write_server_response(&mut stream, &status, br#"{"state":"committed"}"#);
    });
    let mut token_secret = [9; 32];
    let mut client =
        SubmissionClient::connect_authenticated(address, [7; 16], &mut token_secret).unwrap();
    assert_eq!(token_secret, [0; 32]);
    assert_eq!(
        client
            .upload_and_seal(
                &package,
                &parameters,
                30_000,
                prdb_engine::supervisor::WorkloadClass::Operational,
            )
            .unwrap(),
        [1; 16]
    );
    {
        let mut started = client.start([1; 16], &receipt_path).unwrap();
        assert_eq!(
            started.next_event().unwrap(),
            Some(prdb_engine::client_submit::ClientResultEvent::Data(
                b"provisional-row".to_vec()
            ))
        );
        assert!(matches!(
            started.next_event().unwrap(),
            Some(prdb_engine::client_submit::ClientResultEvent::Terminal(terminal))
                if terminal.invocation_id == [1; 16]
                    && terminal.outcome == prdb_engine::client_submit::TerminalOutcome::Committed
                    && terminal.reason_code == "none"
        ));
        assert_eq!(started.next_event().unwrap(), None);
    }
    assert!(client.start([1; 16], &receipt_path).is_err());
    assert_eq!(
        client.status([1; 16]).unwrap(),
        prdb_engine::client_submit::InvocationStatus::Committed
    );
    server.join().unwrap();

    let second = Command::new(env!("CARGO_BIN_EXE_prdb-packager"))
        .arg("--source")
        .arg(&source)
        .arg("--sdk-archive")
        .arg(&archive)
        .arg("--include-dir")
        .arg(root.join("engine/include"))
        .arg("--scope")
        .arg(&scope)
        .arg("--output-dir")
        .arg(&package)
        .args([
            "--abi",
            "3",
            "--sdk-release",
            "1",
            "--cpu-baseline",
            "x86_64-v1",
            "--runtime-profile",
            "test-runtime",
            "--compiler-identity",
            "TinyCC integration test",
        ])
        .output()
        .unwrap();
    assert!(
        !second.status.success(),
        "existing package path was overwritten"
    );
    fs::remove_dir_all(temp).unwrap();
}

fn read_client_frame(stream: &mut TcpStream, decoder: &mut ClientFrameDecoder) -> ClientFrame {
    let mut bytes = [0; 4096];
    loop {
        let count = stream.read(&mut bytes).unwrap();
        assert_ne!(count, 0, "client closed before expected frame");
        let mut frames = decoder.push(&bytes[..count]).unwrap();
        if !frames.is_empty() {
            assert_eq!(frames.len(), 1, "client pipelined requests");
            return frames.remove(0);
        }
    }
}

fn write_server_response(stream: &mut TcpStream, request: &ClientFrame, payload: &[u8]) {
    let response = ClientFrame {
        kind: 100,
        request_id: request.request_id,
        response_sequence: 0,
        payload: payload.to_vec(),
    };
    stream.write_all(&response.encode().unwrap()).unwrap();
}
