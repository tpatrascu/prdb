use super::{
    InvocationStatus, StorageManagerConfig, SupervisorChannelState, SupervisorInvocation,
    SupervisorInvocationError,
};
use std::collections::HashSet;
use std::os::unix::net::UnixStream;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

static NEXT_TEMP_DIRECTORY: AtomicU64 = AtomicU64::new(0);

#[test]
fn worker_exit_waits_for_both_channel_handlers_to_drain() -> Result<(), Box<dyn std::error::Error>>
{
    let invocation = SupervisorInvocation {
        invocation_id: [0x81; 16],
        principal_id: [0x82; 16],
        role_ids: HashSet::new(),
        transaction: Arc::new(Mutex::new(None)),
        channels: Arc::new(Mutex::new(SupervisorChannelState::default())),
        deadline: None,
        abort_on_drop: true,
    };
    let (planner_worker, planner_storage) = UnixStream::pair()?;
    let (executor_worker, executor_storage) = UnixStream::pair()?;
    let (optimizer_worker, optimizer_storage) = UnixStream::pair()?;

    std::thread::scope(|scope| -> Result<(), Box<dyn std::error::Error>> {
        let service = scope.spawn(|| {
            invocation.serve_worker_channels(planner_storage, executor_storage, optimizer_storage)
        });
        let active_channel_deadline = Instant::now() + Duration::from_secs(2);
        loop {
            let active = invocation
                .channels
                .lock()
                .map_err(|_| std::io::Error::other("channel state poisoned"))?
                .active_worker_channels;
            if active == 3 {
                break;
            }
            assert!(Instant::now() < active_channel_deadline);
            std::thread::yield_now();
        }

        let mut worker = std::process::Command::new("true").spawn()?;
        assert!(matches!(
            invocation.observe_worker_exit(&mut worker),
            Err(SupervisorInvocationError::WorkerChannelsActive)
        ));
        drop(planner_worker);
        drop(executor_worker);
        drop(optimizer_worker);
        let service_result = service
            .join()
            .map_err(|_| std::io::Error::other("worker-channel service panicked"))?;
        assert!(
            service_result.is_ok(),
            "worker-channel service failed: {service_result:?}"
        );
        {
            let channels = invocation.channels.lock().unwrap();
            assert_eq!(channels.active_worker_channels, 0);
            assert!(!channels.worker_exit_succeeded);
            assert!(!channels.terminal_requested);
        }
        assert!(worker.wait()?.success());
        let exit_result = invocation.observe_worker_exit(&mut worker);
        assert!(
            exit_result.is_ok(),
            "worker exit observation failed: {exit_result:?}"
        );
        assert!(exit_result?.success());
        Ok(())
    })?;
    Ok(())
}

#[test]
fn cancellation_keeps_snapshot_pinned_until_in_flight_storage_request_drains()
-> Result<(), Box<dyn std::error::Error>> {
    use crate::outcome::OutcomeStatus;
    use crate::storage_manager::OwnerCancellationResult;
    use std::sync::mpsc;
    use std::time::{SystemTime, UNIX_EPOCH};
    use zeroize::Zeroize;

    struct TempDirectory(std::path::PathBuf);
    impl Drop for TempDirectory {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }

    let nonce = SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos();
    let directory = TempDirectory(std::env::temp_dir().join(format!(
        "prdb-worker-storage-pin-{}-{nonce}-{}",
        std::process::id(),
        NEXT_TEMP_DIRECTORY.fetch_add(1, Ordering::Relaxed)
    )));
    std::fs::create_dir(&directory.0)?;
    let instance_directory = directory.0.join("instance");
    let mut bootstrap = crate::bootstrap::initialize(&instance_directory, "pin_admin", i64::MAX)?;
    let admin_id = bootstrap.admin_user_id();
    let token_id = bootstrap.token_id();
    let mut token_secret = bootstrap.take_secret().ok_or("bootstrap secret missing")?;
    let instance = bootstrap.acknowledge()?;
    let storage = instance.storage_manager(StorageManagerConfig::default())?;
    let now_us = i64::try_from(SystemTime::now().duration_since(UNIX_EPOCH)?.as_micros())?;
    let invocation_id = [0x91; 16];
    let invocation = storage.admit_supervisor_invocation(
        invocation_id,
        token_id,
        &token_secret,
        now_us + 60_000_000,
        4,
        1024,
    )?;
    token_secret.zeroize();

    let (request_entered_tx, request_entered_rx) = mpsc::channel();
    let (resume_request_tx, resume_request_rx) = mpsc::channel();
    let (checkpoint_started_tx, checkpoint_started_rx) = mpsc::channel();
    let (checkpoint_done_tx, checkpoint_done_rx) = mpsc::channel();
    std::thread::scope(|scope| -> Result<(), Box<dyn std::error::Error>> {
        let invocation_ref = &invocation;
        let request = scope.spawn(move || {
            invocation_ref.with_owned_transaction(|transaction| {
                request_entered_tx
                    .send(())
                    .expect("signal in-flight storage request");
                resume_request_rx
                    .recv()
                    .expect("resume in-flight storage request");
                transaction
                    .read(crate::overlay::RowId {
                        table: [0x92; 16],
                        row: [0x93; 16],
                    })
                    .map(|_| ())
            })
        });
        request_entered_rx.recv_timeout(Duration::from_secs(2))?;
        assert_eq!(
            storage.cancel_owned_invocation(admin_id, invocation_id, now_us + 1)?,
            OwnerCancellationResult::Accepted
        );

        let invocation_ref = &invocation;
        let checkpoint = scope.spawn(move || {
            checkpoint_started_tx
                .send(())
                .expect("signal cancellation checkpoint");
            let result = invocation_ref.check_deadline();
            checkpoint_done_tx
                .send(result.is_err())
                .expect("report cancellation checkpoint completion");
            result
        });
        checkpoint_started_rx.recv_timeout(Duration::from_secs(2))?;

        let pinned_page = storage.list_runtime_invocations(admin_id, now_us + 2, 128, None)?;
        let pinned = pinned_page
            .invocations
            .iter()
            .find(|item| item.invocation_id == invocation_id)
            .ok_or("cancellation target disappeared while storage request was active")?;
        assert!(matches!(
            &pinned.status,
            InvocationStatus::Active {
                cancellation_requested: true,
                ..
            }
        ));
        assert_eq!(pinned.snapshot_pin_count, 1);

        let checkpoint_wait = checkpoint_done_rx.recv_timeout(Duration::from_millis(25));
        resume_request_tx.send(())?;
        let request_result = request.join().map_err(|_| "storage request panicked")?;
        assert!(matches!(
            request_result,
            Ok(()) | Err(SupervisorInvocationError::Transaction(_))
        ));
        assert!(
            checkpoint_wait.is_err(),
            "checkpoint must wait for the request lock"
        );
        assert!(
            checkpoint
                .join()
                .map_err(|_| "cancellation checkpoint panicked")?
                .is_err()
        );
        Ok(())
    })?;

    assert!(matches!(
        storage.lookup_invocation_outcome(invocation_id, admin_id, now_us + 3)?,
        Some(outcome) if outcome.status == OutcomeStatus::Aborted
    ));
    drop(invocation);
    drop(storage);
    drop(instance);
    Ok(())
}
