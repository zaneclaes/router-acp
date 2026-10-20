//! Machine-local files never belong beside transferable configuration/logins.
use std::fs::File;
use std::path::{Path, PathBuf};

pub fn root() -> Result<PathBuf, String> {
    if let Some(dir) = std::env::var_os("XDG_STATE_HOME").map(PathBuf::from)
        && dir.is_absolute()
    {
        return identity(&dir.join("router-acp"));
    }
    let home = std::env::var_os("HOME").ok_or("Local state directory unavailable")?;
    identity(&PathBuf::from(home).join(".local/state/router-acp"))
}

// Resolve existing ancestors too: a lock identity must not change after its
// account directory is first created, or when reached through a symlink.
fn identity(path: &Path) -> Result<PathBuf, String> {
    if let Ok(canonical) = path.canonicalize() {
        return Ok(canonical);
    }
    if !path.is_absolute() {
        return identity(
            &std::env::current_dir()
                .map_err(|e| e.to_string())?
                .join(path),
        );
    }
    let parent = path.parent().ok_or("Local state identity unavailable")?;
    Ok(identity(parent)?.join(path.file_name().ok_or("Local state identity unavailable")?))
}

pub fn directory(kind: &str, owner: &Path) -> Result<PathBuf, String> {
    use sha2::{Digest, Sha256};
    let key = Sha256::digest(identity(owner)?.as_os_str().as_encoded_bytes());
    Ok(root()?.join(kind).join(format!("{key:x}")))
}

pub fn account(owner: &Path) -> Result<PathBuf, String> {
    directory("accounts", owner)
}

pub(crate) fn private_dir(path: &Path) -> Result<(), String> {
    let mut builder = std::fs::DirBuilder::new();
    builder.recursive(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::DirBuilderExt;
        builder.mode(0o700);
    }
    builder
        .create(path)
        .map_err(|_| "Could not create local state directory")?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o700))
            .map_err(|_| "Could not protect local state directory")?;
    }
    Ok(())
}

/// Older routers lock some files inside the portable configuration, and they
/// keep serving sessions across an upgrade. A newer router therefore takes
/// such a file as well while it exists, always after its machine-local lock,
/// so both versions stay serialized. The file is never deleted: an older
/// router that reopened a deleted path would lock a new inode and stop
/// excluding its successor. `create` adds the file when an older router may
/// still be running without having created it yet.
pub(crate) fn legacy_lock(path: &Path, create: bool) -> Result<Option<File>, String> {
    let mut options = std::fs::OpenOptions::new();
    options
        .read(true)
        .write(true)
        .create(create)
        .truncate(false);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    match options.open(path) {
        Ok(file) => Ok(Some(file)),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(_) => Err("Could not open legacy router lock".into()),
    }
}

/// Locks taken in order and released together when dropped.
pub struct HeldLocks(#[allow(dead_code)] Vec<File>);

pub(crate) enum LockFailure {
    Unavailable,
    TimedOut,
}

/// Wait for every lock in order. A waiter keeps the earlier locks, which is
/// safe because every router version acquires them in the same order.
pub(crate) async fn hold(
    files: Vec<File>,
    wait: std::time::Duration,
) -> Result<HeldLocks, LockFailure> {
    tokio::time::timeout(wait, async {
        for file in &files {
            loop {
                match file.try_lock() {
                    Ok(()) => break,
                    Err(std::fs::TryLockError::WouldBlock) => {
                        tokio::time::sleep(std::time::Duration::from_millis(25)).await
                    }
                    Err(_) => return Err(LockFailure::Unavailable),
                }
            }
        }
        Ok(())
    })
    .await
    .map_err(|_| LockFailure::TimedOut)??;
    Ok(HeldLocks(files))
}

/// Copy across filesystems without replacing a newer local record. Never
/// expose file contents or permissions while migrating private OAuth state.
pub(crate) fn move_file(from: &Path, to: &Path) -> Result<(), String> {
    let mut input = match std::fs::File::open(from) {
        Ok(file) => file,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(_) => return Err("Could not read legacy local state".into()),
    };
    private_dir(to.parent().ok_or("Local state directory unavailable")?)?;
    if !to.exists() {
        let temp = to.with_extension(format!("{}.tmp", uuid::Uuid::new_v4()));
        let mut options = std::fs::OpenOptions::new();
        options.write(true).create_new(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            options.mode(0o600);
        }
        let result = (|| {
            let mut output = options.open(&temp)?;
            std::io::copy(&mut input, &mut output)?;
            output.sync_all()?;
            match std::fs::hard_link(&temp, to) {
                Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => Ok(()),
                other => other,
            }
        })();
        let _ = std::fs::remove_file(temp);
        result.map_err(|_| "Could not migrate local state")?;
    }
    match std::fs::remove_file(from) {
        Ok(()) => Ok(()),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(_) => Err("Could not retire legacy local state".into()),
    }
}

pub(crate) fn migrate_account(dir: &Path) -> Result<(), String> {
    // An older router that is still serving keeps its runtime here, and its
    // locks and records in place. Leave what is live; never block on it.
    let legacy = dir.join(".router-acp-runtime");
    crate::credentials::sweep_runtimes(&legacy);
    let _ = std::fs::remove_dir(&legacy); // only succeeds when empty
    crate::credentials::migrate_record(dir)?;
    let local = account(dir)?;
    let usage = dir.join(".router-acp-usage");
    if let Ok(entries) = std::fs::read_dir(&usage) {
        for entry in entries {
            let entry = entry.map_err(|_| "Could not read legacy usage state")?;
            move_file(&entry.path(), &local.join("usage").join(entry.file_name()))?;
        }
        let _ = std::fs::remove_dir(usage);
    }
    Ok(())
}

/// Move machine-local state out of portable configuration. Best effort: an
/// upgrade runs beside older routers, so anything still in use stays where
/// it is and the next router start tries again. A failure here must never
/// stop a router from serving.
pub fn migrate(config: &crate::config::Config) {
    let mut accounts = std::collections::BTreeSet::new();
    let mut roots = std::collections::BTreeSet::new();
    if let Some(path) = &config.source_path
        && let Some(parent) = path.parent()
    {
        roots.insert(parent.join("accounts"));
    }
    for agent in &config.agents {
        if let Some(dir) = crate::credentials::directory(agent) {
            if let Some(parent) = dir.parent()
                && parent.file_name().is_some_and(|name| name == "accounts")
            {
                roots.insert(parent.to_path_buf());
            }
            accounts.insert(dir);
        }
    }
    // Deleted/disabled accounts may still have abandoned runtime directories;
    // the whole configuration tree must be transferable, not just active seats.
    for root in roots {
        if let Ok(entries) = std::fs::read_dir(root) {
            for entry in entries
                .flatten()
                .filter(|e| e.file_type().is_ok_and(|t| t.is_dir()))
            {
                accounts.insert(entry.path());
            }
        }
    }
    for dir in accounts {
        if let Err(err) = migrate_account(&dir) {
            tracing::warn!(
                "local state migration deferred for {}: {err}",
                dir.display()
            );
        }
    }
    let mcp = crate::mcp_oauth::store_root();
    if let Ok(entries) = std::fs::read_dir(mcp) {
        for entry in entries
            .flatten()
            .filter(|e| e.file_type().is_ok_and(|t| t.is_dir()))
        {
            let dir = entry.path();
            let moved = directory("mcp", &dir).and_then(|local| {
                ["flow.json", "status.json"]
                    .iter()
                    .try_for_each(|name| move_file(&dir.join(name), &local.join(name)))
            });
            if let Err(err) = moved {
                tracing::warn!("MCP state migration deferred for {}: {err}", dir.display());
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn migration_moves_only_local_state_and_never_waits_for_older_routers() {
        let home = tempfile::tempdir().unwrap();
        let dir = home.path().join(".config/router-acp/accounts/codex@test");
        std::fs::create_dir_all(dir.join(".router-acp-usage")).unwrap();
        std::fs::write(dir.join("auth.json"), "portable-login").unwrap();
        std::fs::write(dir.join(".router-acp-auth.json"), "old-record").unwrap();
        std::fs::write(dir.join(".router-acp-usage/codex.json"), "snapshot").unwrap();
        // An older router is mid-refresh under its in-place lock.
        let old_lock = dir.join(".router-acp-auth.lock");
        let held = std::fs::File::create(&old_lock).unwrap();
        held.lock().unwrap();
        migrate_account(&dir).unwrap();
        drop(held);
        // The older router may lock it again; deleting it would split the lock.
        assert!(old_lock.exists());
        assert_eq!(
            std::fs::read_to_string(dir.join("auth.json")).unwrap(),
            "portable-login"
        );
        let local = account(&dir).unwrap();
        assert_eq!(
            std::fs::read_to_string(local.join("auth.json")).unwrap(),
            "old-record"
        );
        assert_eq!(
            std::fs::read_to_string(local.join("usage/codex.json")).unwrap(),
            "snapshot"
        );
        assert!(!dir.join(".router-acp-usage").exists());
        // A copied legacy snapshot must not overwrite a newer local result.
        std::fs::write(dir.join(".router-acp-auth.json"), "stale-copy").unwrap();
        migrate_account(&dir).unwrap();
        assert_eq!(
            std::fs::read_to_string(local.join("auth.json")).unwrap(),
            "old-record"
        );
        assert!(!dir.join(".router-acp-auth.json").exists());
    }

    #[test]
    fn a_live_older_runtime_never_blocks_migration() {
        let home = tempfile::tempdir().unwrap();
        let dir = home.path().join(".config/router-acp/accounts/grok@test");
        // Older routers created runtimes without a lock file. This one was
        // written moments ago, so it may belong to a router still serving.
        let live = dir.join(".router-acp-runtime/session");
        std::fs::create_dir_all(live.join("sessions")).unwrap();
        std::fs::write(live.join("sessions/events.jsonl"), "turn").unwrap();
        std::fs::write(dir.join(".router-acp-auth.json"), "old-record").unwrap();
        migrate_account(&dir).unwrap();
        assert!(live.join("sessions/events.jsonl").exists());
        assert_eq!(
            std::fs::read_to_string(account(&dir).unwrap().join("auth.json")).unwrap(),
            "old-record"
        );
    }

    #[test]
    fn a_later_record_from_an_older_router_replaces_the_local_one() {
        let home = tempfile::tempdir().unwrap();
        let dir = home.path().join(".config/router-acp/accounts/claude@test");
        std::fs::create_dir_all(&dir).unwrap();
        let record = |outcome: &str, at: &str| {
            format!(
                r#"{{"generation":"g","epoch":"e","outcome":"{outcome}","attempted_at":"{at}"}}"#
            )
        };
        let local = account(&dir).unwrap();
        private_dir(&local).unwrap();
        std::fs::write(
            local.join("auth.json"),
            record("Unknown", "2026-10-20T14:00:00+00:00"),
        )
        .unwrap();
        // The older router refreshed after this router migrated.
        std::fs::write(
            dir.join(".router-acp-auth.json"),
            record("Repaired", "2026-10-20T14:05:00+00:00"),
        )
        .unwrap();
        migrate_account(&dir).unwrap();
        let merged = std::fs::read_to_string(local.join("auth.json")).unwrap();
        assert!(merged.contains("Repaired"), "{merged}");
        // An earlier one, such as a copy from another machine, never wins.
        std::fs::write(
            dir.join(".router-acp-auth.json"),
            record("Rejected", "2026-10-20T13:00:00+00:00"),
        )
        .unwrap();
        migrate_account(&dir).unwrap();
        let kept = std::fs::read_to_string(local.join("auth.json")).unwrap();
        assert!(kept.contains("Repaired"), "{kept}");
        assert!(!dir.join(".router-acp-auth.json").exists());
    }

    #[tokio::test]
    async fn newer_routers_wait_for_the_lock_an_older_router_holds() {
        let home = tempfile::tempdir().unwrap();
        let legacy = home.path().join(".router-acp-auth.lock");
        let older = std::fs::File::create(&legacy).unwrap();
        older.lock().unwrap();
        let local = std::fs::File::create(home.path().join("auth.lock")).unwrap();
        let ours = legacy_lock(&legacy, false).unwrap().unwrap();
        let waiting = tokio::spawn(hold(vec![local, ours], std::time::Duration::from_secs(5)));
        tokio::time::sleep(std::time::Duration::from_millis(200)).await;
        assert!(!waiting.is_finished());
        drop(older);
        assert!(waiting.await.unwrap().is_ok());
        // Without an older router there is nothing extra to take.
        assert!(
            legacy_lock(&home.path().join("absent.lock"), false)
                .unwrap()
                .is_none()
        );
    }

    #[test]
    fn missing_and_symlinked_owners_keep_the_same_local_identity() {
        let home = tempfile::tempdir().unwrap();
        let owner = home.path().join("account");
        let before = account(&owner).unwrap();
        std::fs::create_dir(&owner).unwrap();
        assert_eq!(before, account(&owner).unwrap());
        assert!(!before.starts_with(home.path()));
        #[cfg(unix)]
        {
            let alias = home.path().join("alias");
            std::os::unix::fs::symlink(&owner, &alias).unwrap();
            assert_eq!(before, account(&alias).unwrap());
        }
    }
}
