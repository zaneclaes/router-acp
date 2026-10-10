//! Many router-shaped processes writing sharded state at once lose no
//! load-bearing write and drop no streaming row. The one-file comparison
//! (`--checkouts 1`) runs through `router-acp state-bench` directly.

use serde_json::Value;

fn bench(processes: usize, checkouts: usize) -> Value {
    let dir = tempfile::tempdir().unwrap();
    let output = std::process::Command::new(env!("CARGO_BIN_EXE_router-acp"))
        .args(["state-bench", "--seconds", "3"])
        .args(["--processes", &processes.to_string()])
        .args(["--checkouts", &checkouts.to_string()])
        .arg("--state")
        .arg(dir.path().join("sessions.db"))
        .output()
        .unwrap();
    assert!(output.status.success(), "{output:?}");
    serde_json::from_slice(&output.stdout).unwrap()
}

fn assert_nothing_lost(report: &Value) {
    for field in [
        "failed_workers",
        "load_bearing_failures",
        "dropped_rows",
        "unflushed_rows",
    ] {
        assert_eq!(report[field], 0, "{field}: {report:#}");
    }
    assert!(report["total_ops"].as_u64().unwrap() > 0, "{report:#}");
}

#[test]
fn thirty_two_sharded_processes_lose_nothing() {
    assert_nothing_lost(&bench(32, 8));
}

#[test]
fn sixty_four_sharded_processes_lose_nothing() {
    assert_nothing_lost(&bench(64, 8));
}

/// Every process opens the same new shard while the others write to it.
#[test]
fn sixty_four_processes_in_one_checkout_lose_nothing() {
    assert_nothing_lost(&bench(64, 1));
}
