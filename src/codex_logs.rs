//! Keep Codex's diagnostic log DB (`$CODEX_HOME/logs_2.sqlite`) bounded.
//!
//! Codex persists TRACE-level logs there regardless of `RUST_LOG`, so the file
//! grows without limit. Every `codex app-server` on an account shares it, and
//! once it is large their batch flushes fail ("Codex couldn't save diagnostic
//! logs to its local database" in the agent's reply). Before launching a Codex
//! adapter, move an oversized log DB aside; Codex recreates it on start. Only
//! the log DB is touched — never `state_*.sqlite`, sessions, or auth.
//!
//! Every `router-acp serve` on a box (one per client session) spawns Codex, so
//! "is Codex running" is answered from `/proc`: any process holding one of the
//! files open blocks the rotation. Without `/proc` the rotation never runs.

use std::path::{Path, PathBuf};

const LOG_DB: &str = "logs_2.sqlite";
const SIDECARS: [&str; 3] = ["", "-wal", "-shm"];
const ROTATED: &str = "logs_2.sqlite.rotated-";
/// Combined size of the DB and its WAL/SHM sidecars that triggers a rotation.
pub const ROTATE_BYTES: u64 = 1 << 30;

/// Rotate `home`'s log DB when it is over `ROTATE_BYTES` and no process holds
/// it open.
pub fn rotate_if_oversized(home: &Path) {
    rotate(home, ROTATE_BYTES, &open_by_any_process);
}

fn rotate(home: &Path, limit: u64, in_use: &dyn Fn(&[PathBuf]) -> bool) -> bool {
    let files: Vec<(PathBuf, u64)> = SIDECARS
        .iter()
        .filter_map(|suffix| {
            let path = home.join(format!("{LOG_DB}{suffix}"));
            let len = std::fs::metadata(&path).ok()?.len();
            Some((path, len))
        })
        .collect();
    let bytes: u64 = files.iter().map(|(_, len)| len).sum();
    if bytes <= limit {
        return false;
    }
    let paths: Vec<PathBuf> = files.iter().map(|(p, _)| p.clone()).collect();
    if in_use(&paths) {
        tracing::info!(home = %home.display(), bytes, "Codex log DB is oversized but open; rotation skipped");
        return false;
    }
    // Keep one rotation: drop the previous one first.
    if let Ok(entries) = std::fs::read_dir(home) {
        for entry in entries.flatten() {
            if entry.file_name().to_string_lossy().starts_with(ROTATED) {
                let _ = std::fs::remove_file(entry.path());
            }
        }
    }
    let stamp = chrono::Utc::now().format("%Y%m%dT%H%M%SZ");
    for (path, len) in &files {
        let suffix = path.file_name().unwrap_or_default().to_string_lossy();
        let suffix = suffix.trim_start_matches(LOG_DB);
        let to = home.join(format!("{ROTATED}{stamp}{suffix}"));
        match std::fs::rename(path, &to) {
            Ok(()) => {
                tracing::warn!(from = %path.display(), to = %to.display(), bytes = len, "rotated oversized Codex log DB")
            }
            Err(err) => {
                tracing::warn!(path = %path.display(), %err, "failed to rotate Codex log DB")
            }
        }
    }
    true
}

/// True when any process has one of `files` open, or when that cannot be
/// determined (no `/proc`).
fn open_by_any_process(files: &[PathBuf]) -> bool {
    let targets: Vec<PathBuf> = files.iter().filter_map(|p| p.canonicalize().ok()).collect();
    let Ok(procs) = std::fs::read_dir("/proc") else {
        return true;
    };
    procs.flatten().any(|proc| {
        std::fs::read_dir(proc.path().join("fd")).is_ok_and(|fds| {
            fds.flatten()
                .filter_map(|fd| std::fs::read_link(fd.path()).ok())
                .any(|link| targets.contains(&link))
        })
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn write(dir: &Path, name: &str, bytes: usize) {
        std::fs::write(dir.join(name), vec![0u8; bytes]).unwrap();
    }

    fn names(dir: &Path) -> Vec<String> {
        let mut names: Vec<String> = std::fs::read_dir(dir)
            .unwrap()
            .map(|e| e.unwrap().file_name().to_string_lossy().into_owned())
            .collect();
        names.sort();
        names
    }

    #[test]
    fn oversized_idle_log_db_is_rotated_and_other_state_kept() {
        let home = tempfile::tempdir().unwrap();
        write(home.path(), "logs_2.sqlite", 80);
        write(home.path(), "logs_2.sqlite-wal", 30);
        write(home.path(), "logs_2.sqlite-shm", 10);
        write(home.path(), "logs_2.sqlite.rotated-20260101T000000Z", 1);
        write(home.path(), "state_5.sqlite", 500);
        write(home.path(), "auth.json", 5);

        assert!(rotate(home.path(), 100, &|_| false));

        let names = names(home.path());
        let rotated: Vec<&String> = names.iter().filter(|n| n.starts_with(ROTATED)).collect();
        assert_eq!(rotated.len(), 3, "{names:?}");
        assert!(
            rotated.iter().all(|n| !n.contains("20260101")),
            "previous rotation dropped"
        );
        assert!(rotated.iter().any(|n| n.ends_with("-wal")));
        assert!(rotated.iter().any(|n| n.ends_with("-shm")));
        for kept in ["auth.json", "state_5.sqlite"] {
            assert!(names.contains(&kept.to_string()), "{names:?}");
        }
        assert_eq!(names.len(), 5, "live log DB moved aside: {names:?}");
    }

    #[test]
    fn small_log_db_is_left_alone() {
        let home = tempfile::tempdir().unwrap();
        write(home.path(), "logs_2.sqlite", 60);
        write(home.path(), "logs_2.sqlite-wal", 40);
        assert!(!rotate(home.path(), 100, &|_| panic!(
            "size check comes first"
        )));
        assert_eq!(names(home.path()), ["logs_2.sqlite", "logs_2.sqlite-wal"]);
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn log_db_held_open_by_a_live_process_is_not_rotated() {
        let home = tempfile::tempdir().unwrap();
        write(home.path(), "logs_2.sqlite", 200);
        let held = std::fs::File::open(home.path().join("logs_2.sqlite")).unwrap();
        assert!(!rotate(home.path(), 100, &open_by_any_process));
        assert_eq!(names(home.path()), ["logs_2.sqlite"]);
        drop(held);
        assert!(rotate(home.path(), 100, &open_by_any_process));
    }
}
