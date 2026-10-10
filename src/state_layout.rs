//! Where a router session's state lives.
//!
//! `state_file` is the legacy database. With `state_sharding: cwd`, each new
//! session id carries a tag derived from its canonical cwd, and that
//! session's rows live in `<state_file parent>/shards/sessions-<tag>.db`.
//! Routing is always by id shape, never by mode, so turning sharding on or
//! off keeps every existing session resumable.
//!
//! Id shapes:
//! * legacy: `rtr-<uuid>` (a `-` follows the first 8 hex chars)
//! * tagged: `rtr-<32 hex tag>-<uuid>`
//! * delegate: `<parent id>::delegate-<downstream id>`, homed with the parent

use std::path::{Component, Path, PathBuf};

use sha2::{Digest, Sha256};

const ID_PREFIX: &str = "rtr-";
const TAG_LEN: usize = 32;
const SHARD_PREFIX: &str = "sessions-";
const SHARD_SUFFIX: &str = ".db";

/// How NEW session ids pick their database.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ShardingMode {
    /// Every new session goes to the legacy `state_file`.
    #[default]
    Off,
    /// Every new session goes to the shard for its canonical cwd.
    Cwd,
}

/// 32 lowercase hex chars: the first 16 bytes of SHA-256 over a canonical cwd.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct ShardTag(String);

impl ShardTag {
    pub fn parse(text: &str) -> Option<Self> {
        (text.len() == TAG_LEN
            && text
                .bytes()
                .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b)))
        .then(|| Self(text.to_string()))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl std::fmt::Display for ShardTag {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

/// The database a session id belongs to.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SessionHome {
    Legacy,
    Shard(ShardTag),
}

/// Resolve symlinks when the directory exists; otherwise make the path
/// absolute and drop `.`, `..`, and trailing separators lexically, so the
/// same spelling always yields the same tag.
pub fn canonical_cwd(cwd: &Path) -> PathBuf {
    if let Ok(path) = std::fs::canonicalize(cwd) {
        return path;
    }
    let absolute = if cwd.is_absolute() {
        cwd.to_path_buf()
    } else {
        std::env::current_dir()
            .unwrap_or_else(|_| PathBuf::from("/"))
            .join(cwd)
    };
    let mut out = PathBuf::new();
    for component in absolute.components() {
        match component {
            Component::CurDir => {}
            Component::ParentDir => {
                out.pop();
            }
            other => out.push(other),
        }
    }
    out
}

pub fn tag_for_cwd(cwd: &Path) -> ShardTag {
    let digest = Sha256::digest(canonical_cwd(cwd).as_os_str().as_encoded_bytes());
    ShardTag(
        digest[..TAG_LEN / 2]
            .iter()
            .map(|b| format!("{b:02x}"))
            .collect(),
    )
}

pub fn legacy_session_id() -> String {
    format!("{ID_PREFIX}{}", uuid::Uuid::new_v4())
}

pub fn tagged_session_id(tag: &ShardTag) -> String {
    format!("{ID_PREFIX}{tag}-{}", uuid::Uuid::new_v4())
}

/// Delegates inherit their parent's home; anything that is not a tagged id
/// (including ids from older routers and tests) lives in the legacy file.
pub fn home_of(router_session_id: &str) -> SessionHome {
    let root = router_session_id
        .split_once("::")
        .map_or(router_session_id, |(parent, _)| parent);
    root.strip_prefix(ID_PREFIX)
        .and_then(|rest| {
            let (tag, uuid) = rest.split_at_checked(TAG_LEN)?;
            uuid.strip_prefix('-').filter(|u| !u.is_empty())?;
            ShardTag::parse(tag)
        })
        .map_or(SessionHome::Legacy, SessionHome::Shard)
}

/// Every path derived from the configured `state_file`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StateLayout {
    pub legacy_path: PathBuf,
    pub shards_dir: PathBuf,
}

impl StateLayout {
    pub fn new(state_file: &Path) -> Self {
        let parent = state_file.parent().unwrap_or(Path::new("."));
        Self {
            legacy_path: state_file.to_path_buf(),
            shards_dir: parent.join("shards"),
        }
    }

    pub fn shard_path(&self, tag: &ShardTag) -> PathBuf {
        self.shards_dir
            .join(format!("{SHARD_PREFIX}{tag}{SHARD_SUFFIX}"))
    }

    /// Stable file whose nonblocking exclusive lock elects the one process
    /// that runs maintenance. Never unlinked.
    pub fn maintenance_lock_path(&self) -> PathBuf {
        self.legacy_path
            .parent()
            .unwrap_or(Path::new("."))
            .join("maintenance.lock")
    }

    /// Shards on disk, sorted by tag. A missing directory has none.
    pub fn list_shards(&self) -> Vec<ShardTag> {
        let Ok(entries) = std::fs::read_dir(&self.shards_dir) else {
            return Vec::new();
        };
        let mut tags: Vec<_> = entries
            .flatten()
            .filter_map(|entry| {
                let name = entry.file_name();
                let name = name.to_str()?;
                ShardTag::parse(
                    name.strip_prefix(SHARD_PREFIX)?
                        .strip_suffix(SHARD_SUFFIX)?,
                )
            })
            .collect();
        tags.sort();
        tags
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn legacy_and_tagged_ids_resolve_by_shape() {
        let legacy = legacy_session_id();
        assert_eq!(home_of(&legacy), SessionHome::Legacy);
        assert_eq!(home_of("rtr-1"), SessionHome::Legacy);
        assert_eq!(home_of("not-a-router-id"), SessionHome::Legacy);

        let tag = tag_for_cwd(Path::new("/opt/dev/hickory-ai4"));
        let tagged = tagged_session_id(&tag);
        assert_eq!(home_of(&tagged), SessionHome::Shard(tag.clone()));
        // A tag with no uuid after it is not a tagged id.
        assert_eq!(home_of(&format!("rtr-{tag}")), SessionHome::Legacy);
        assert_eq!(home_of(&format!("rtr-{tag}-")), SessionHome::Legacy);
        assert_eq!(
            home_of(&format!("rtr-{}-x", tag.as_str().to_uppercase())),
            SessionHome::Legacy
        );
    }

    #[test]
    fn a_delegate_lives_with_its_parent() {
        let tag = tag_for_cwd(Path::new("/work/a"));
        let parent = tagged_session_id(&tag);
        assert_eq!(
            home_of(&format!("{parent}::delegate-abc")),
            SessionHome::Shard(tag)
        );
        let legacy = legacy_session_id();
        assert_eq!(
            home_of(&format!("{legacy}::delegate-abc")),
            SessionHome::Legacy
        );
    }

    #[test]
    fn spellings_of_one_directory_share_a_tag() {
        let dir = tempfile::tempdir().unwrap();
        let real = dir.path().join("checkout");
        std::fs::create_dir(&real).unwrap();
        std::fs::create_dir(real.join("sub")).unwrap();
        let link = dir.path().join("link");
        std::os::unix::fs::symlink(&real, &link).unwrap();

        let tag = tag_for_cwd(&real);
        assert_eq!(tag_for_cwd(&real.join("")), tag, "trailing slash");
        assert_eq!(tag_for_cwd(&real.join("sub").join("..")), tag, "..");
        assert_eq!(tag_for_cwd(&link), tag, "symlink");
        assert_ne!(tag_for_cwd(&real.join("sub")), tag);
    }

    #[test]
    fn a_missing_directory_still_has_a_stable_tag() {
        let missing = Path::new("/does/not/exist/../exist/./x/");
        assert_eq!(canonical_cwd(missing), PathBuf::from("/does/not/exist/x"));
        assert_eq!(
            tag_for_cwd(missing),
            tag_for_cwd(Path::new("/does/not/exist/x"))
        );
    }

    #[test]
    fn shards_live_beside_the_legacy_file_and_are_discovered_by_name() {
        let dir = tempfile::tempdir().unwrap();
        let layout = StateLayout::new(&dir.path().join("sessions.db"));
        assert_eq!(layout.list_shards(), Vec::<ShardTag>::new());

        let a = tag_for_cwd(Path::new("/a"));
        let b = tag_for_cwd(Path::new("/b"));
        assert_eq!(
            layout.shard_path(&a),
            dir.path().join("shards").join(format!("sessions-{a}.db"))
        );
        std::fs::create_dir_all(&layout.shards_dir).unwrap();
        for tag in [&a, &b] {
            std::fs::write(layout.shard_path(tag), b"").unwrap();
        }
        // SQLite side files and strangers are not shards.
        std::fs::write(layout.shards_dir.join(format!("sessions-{a}.db-wal")), b"").unwrap();
        std::fs::write(layout.shards_dir.join("sessions-zz.db"), b"").unwrap();

        let mut expected = vec![a, b];
        expected.sort();
        assert_eq!(layout.list_shards(), expected);
        assert_eq!(
            layout.maintenance_lock_path(),
            dir.path().join("maintenance.lock")
        );
    }
}
