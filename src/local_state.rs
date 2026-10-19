//! Machine-local files never belong beside transferable configuration/logins.
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

/// Retire an old lock only when nobody holds it. Upgrade requires restarting
/// older router processes; they do not participate in the new lock namespace.
pub(crate) fn retire_lock(path: &Path) -> Result<(), String> {
    let file = match std::fs::OpenOptions::new().write(true).open(path) {
        Ok(file) => file,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(_) => return Err("Could not open legacy router lock".into()),
    };
    file.try_lock()
        .map_err(|_| "Restart older router processes before migrating local state")?;
    match std::fs::remove_file(path) {
        Ok(()) => Ok(()),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(_) => Err("Could not retire legacy router lock".into()),
    }
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
    let legacy = dir.join(".router-acp-runtime");
    crate::credentials::sweep_runtimes(&legacy);
    match std::fs::remove_dir(&legacy) {
        Ok(()) => {}
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
        Err(_) => {
            return Err(
                "Restart older router sessions before migrating their runtime state".into(),
            );
        }
    }
    retire_lock(&dir.join(".router-acp-auth.lock"))?;
    let local = account(dir)?;
    move_file(&dir.join(".router-acp-auth.json"), &local.join("auth.json"))?;
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

pub fn migrate(config: &crate::config::Config) -> Result<(), String> {
    let mut accounts = std::collections::BTreeSet::new();
    let mut roots = std::collections::BTreeSet::new();
    if let Some(path) = &config.source_path {
        retire_lock(&path.with_extension("accounts.lock"))?;
        if let Some(parent) = path.parent() {
            roots.insert(parent.join("accounts"));
        }
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
        migrate_account(&dir)?;
    }
    let mcp = crate::mcp_oauth::store_root();
    if let Ok(entries) = std::fs::read_dir(mcp) {
        for entry in entries
            .flatten()
            .filter(|e| e.file_type().is_ok_and(|t| t.is_dir()))
        {
            let dir = entry.path();
            retire_lock(&dir.join(".lock"))?;
            let local = directory("mcp", &dir)?;
            for name in ["flow.json", "status.json"] {
                move_file(&dir.join(name), &local.join(name))?;
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn migration_moves_only_local_state_and_preserves_live_locks() {
        let home = tempfile::tempdir().unwrap();
        let dir = home.path().join(".config/router-acp/accounts/codex@test");
        std::fs::create_dir_all(dir.join(".router-acp-usage")).unwrap();
        std::fs::write(dir.join("auth.json"), "portable-login").unwrap();
        std::fs::write(dir.join(".router-acp-auth.json"), "old-record").unwrap();
        std::fs::write(dir.join(".router-acp-usage/codex.json"), "snapshot").unwrap();
        let old_lock = dir.join(".router-acp-auth.lock");
        let held = std::fs::File::create(&old_lock).unwrap();
        held.lock().unwrap();
        assert!(migrate_account(&dir).is_err());
        assert!(old_lock.exists());
        drop(held);
        migrate_account(&dir).unwrap();
        assert!(!old_lock.exists());
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
