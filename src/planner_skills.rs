//! Repository policy resolution. The stored snapshot, not a changed file,
//! supplies the policy when a run resumes.

use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::config::{PlannerRouterConfig, PlannerSkillConfig};

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum PlannerRole {
    CreatePlan,
    SelectPlan,
    ImplementWork,
    ReviewWork,
    FinishWork,
    IntegratePlan,
}

impl PlannerRole {
    pub const ALL: [Self; 6] = [
        Self::CreatePlan,
        Self::SelectPlan,
        Self::ImplementWork,
        Self::ReviewWork,
        Self::FinishWork,
        Self::IntegratePlan,
    ];

    pub fn name(self) -> &'static str {
        match self {
            Self::CreatePlan => "create-plan",
            Self::SelectPlan => "select-plan",
            Self::ImplementWork => "implement-work",
            Self::ReviewWork => "review-work",
            Self::FinishWork => "finish-work",
            Self::IntegratePlan => "integrate-plan",
        }
    }

    fn mapping(self, cfg: &PlannerRouterConfig) -> &PlannerSkillConfig {
        match self {
            Self::CreatePlan => &cfg.create_plan,
            Self::SelectPlan => &cfg.select_plan,
            Self::ImplementWork => &cfg.implement_work,
            Self::ReviewWork => &cfg.review_work,
            Self::FinishWork => &cfg.finish_work,
            Self::IntegratePlan => &cfg.integrate_plan,
        }
    }

    fn bundled(self) -> &'static str {
        match self {
            Self::CreatePlan => include_str!("../skills/create-plan.md"),
            Self::SelectPlan => include_str!("../skills/select-plan.md"),
            Self::ImplementWork => include_str!("../skills/implement-work.md"),
            Self::ReviewWork => include_str!("../skills/review-work.md"),
            Self::FinishWork => include_str!("../skills/finish-work.md"),
            Self::IntegratePlan => include_str!("../skills/integrate-plan.md"),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ResolvedSkill {
    pub name: String,
    pub source: String,
    pub hash: String,
    pub text: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ResolvedPlanner {
    pub schema: u32,
    pub identity: String,
    pub policy: ResolvedSkill,
    pub roles: BTreeMap<PlannerRole, ResolvedSkill>,
    pub roadmap: Option<PathBuf>,
    pub host_instructions: String,
    pub workspace: Option<crate::config::PlannerWorkspaceConfig>,
}

pub fn content_hash(text: &str) -> String {
    format!("{:x}", Sha256::digest(text.as_bytes()))
}

pub fn valid_skill_name(name: &str) -> bool {
    !name.is_empty()
        && name
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b == b'-' || b == b'_')
}

pub fn repository_skill(cwd: &Path, name: &str) -> Result<Option<ResolvedSkill>, String> {
    if !valid_skill_name(name) {
        return Err(format!("invalid planner skill name `{name}`"));
    }
    let mut files = BTreeSet::new();
    for root in [".agents/skills", ".claude/skills", ".codex/skills"] {
        let file = cwd.join(root).join(name).join("SKILL.md");
        match std::fs::symlink_metadata(&file) {
            Ok(_) => {
                files.insert(std::fs::canonicalize(&file).map_err(|e| {
                    format!("cannot resolve planner skill {}: {e}", file.display())
                })?);
            }
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
            Err(e) => {
                return Err(format!(
                    "cannot inspect planner skill {}: {e}",
                    file.display()
                ));
            }
        }
    }
    if files.len() > 1 {
        return Err(format!(
            "ambiguous planner skill `{name}`: {}",
            files
                .iter()
                .map(|p| p.display().to_string())
                .collect::<Vec<_>>()
                .join(", ")
        ));
    }
    files
        .into_iter()
        .next()
        .map(|p| read_skill(name, &p))
        .transpose()
}

fn read_skill(name: &str, file: &Path) -> Result<ResolvedSkill, String> {
    let text = std::fs::read_to_string(file)
        .map_err(|e| format!("cannot read planner skill {}: {e}", file.display()))?;
    if text.trim().is_empty() {
        return Err(format!("planner skill {} is empty", file.display()));
    }
    Ok(ResolvedSkill {
        name: name.into(),
        source: file.display().to_string(),
        hash: content_hash(&text),
        text,
    })
}

pub fn resolve(cfg: &PlannerRouterConfig, cwd: &Path) -> Result<ResolvedPlanner, String> {
    let policy = match cfg.profile.as_deref() {
        None | Some("markdown") => {
            let text = include_str!("../skills/markdown.md").to_string();
            ResolvedSkill {
                name: "markdown".into(),
                source: "bundled:markdown".into(),
                hash: content_hash(&text),
                text,
            }
        }
        Some(path) => {
            if Path::new(path).is_absolute()
                || Path::new(path)
                    .components()
                    .any(|c| matches!(c, std::path::Component::ParentDir))
            {
                return Err("planner profile must be a repository-relative path".into());
            }
            let root = std::fs::canonicalize(cwd).map_err(|e| e.to_string())?;
            let path = std::fs::canonicalize(cwd.join(path))
                .map_err(|e| format!("cannot resolve explicit planner profile `{path}`: {e}"))?;
            if !path.starts_with(root) {
                return Err("planner profile resolves outside the session repository".into());
            }
            read_skill("repository-policy", &path)?
        }
    };
    let mut roles = BTreeMap::new();
    for role in PlannerRole::ALL {
        let mapped = role.mapping(cfg).skill.as_deref();
        let name = mapped.unwrap_or(role.name());
        let skill = match repository_skill(cwd, name)? {
            Some(skill) => skill,
            None if mapped.is_some() => {
                return Err(format!(
                    "explicit planner mapping {}.skill: `{name}` was not found in the session repository",
                    role.name()
                ));
            }
            None => {
                let text = role.bundled().to_string();
                ResolvedSkill {
                    name: role.name().into(),
                    source: format!("bundled:{}", role.name()),
                    hash: content_hash(&text),
                    text,
                }
            }
        };
        roles.insert(role, skill);
    }
    let mut resolved = ResolvedPlanner {
        schema: 1,
        identity: String::new(),
        policy,
        roles,
        roadmap: cfg.roadmap.clone(),
        host_instructions: cfg.planning_instructions.clone(),
        workspace: cfg.workspace.clone(),
    };
    resolved.identity = content_hash(&serde_json::to_string(&resolved).map_err(|e| e.to_string())?);
    Ok(resolved)
}

impl ResolvedPlanner {
    pub fn instructions(&self, role: PlannerRole) -> String {
        let skill = &self.roles[&role];
        format!(
            "[router-acp planner role: {}]\nPolicy identity: {}\nSkill: {}\nSource: {}\nContent hash: {}\nThis is an internal role invocation, not a user mode command. Execute its guidance once for this assignment. Mode selection never grants merge, deploy, or publication permission.\n{}\n{}\n{}",
            role.name(),
            self.identity,
            skill.name,
            skill.source,
            skill.hash,
            self.policy.text,
            skill.text,
            self.host_instructions
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn write(root: &Path, dir: &str, name: &str, text: &str) -> PathBuf {
        let p = root.join(dir).join(name).join("SKILL.md");
        std::fs::create_dir_all(p.parent().unwrap()).unwrap();
        std::fs::write(&p, text).unwrap();
        p
    }

    #[test]
    fn mapping_exact_and_bundled_precedence() {
        let tmp = tempfile::tempdir().unwrap();
        let mut cfg = PlannerRouterConfig::default();
        assert!(
            resolve(&cfg, tmp.path()).unwrap().roles[&PlannerRole::FinishWork]
                .source
                .starts_with("bundled:")
        );
        write(tmp.path(), ".agents/skills", "finish-work", "exact role");
        assert_eq!(
            resolve(&cfg, tmp.path()).unwrap().roles[&PlannerRole::FinishWork].text,
            "exact role"
        );
        cfg.finish_work.skill = Some("ship-pr".into());
        assert!(
            resolve(&cfg, tmp.path())
                .unwrap_err()
                .contains("explicit planner mapping")
        );
        write(tmp.path(), ".claude/skills", "ship-pr", "mapped skill");
        assert_eq!(
            resolve(&cfg, tmp.path()).unwrap().roles[&PlannerRole::FinishWork].text,
            "mapped skill"
        );
    }

    #[test]
    fn canonical_alias_is_one_source_distinct_files_are_ambiguous() {
        let tmp = tempfile::tempdir().unwrap();
        let file = write(tmp.path(), ".agents/skills", "review-work", "review");
        let alias = tmp.path().join(".claude/skills/review-work");
        std::fs::create_dir_all(alias.parent().unwrap()).unwrap();
        std::os::unix::fs::symlink(file.parent().unwrap(), &alias).unwrap();
        assert!(resolve(&PlannerRouterConfig::default(), tmp.path()).is_ok());
        std::fs::remove_file(alias).unwrap();
        write(tmp.path(), ".claude/skills", "review-work", "different");
        assert!(
            resolve(&PlannerRouterConfig::default(), tmp.path())
                .unwrap_err()
                .contains("ambiguous")
        );
    }

    #[test]
    fn profile_errors_and_policy_changes_are_visible() {
        let tmp = tempfile::tempdir().unwrap();
        let mut cfg = PlannerRouterConfig {
            profile: Some("missing.md".into()),
            ..Default::default()
        };
        assert!(resolve(&cfg, tmp.path()).is_err());
        std::fs::write(tmp.path().join("policy.md"), "Policy one").unwrap();
        cfg.profile = Some("policy.md".into());
        let first = resolve(&cfg, tmp.path()).unwrap();
        std::fs::write(tmp.path().join("policy.md"), "Policy two").unwrap();
        assert_ne!(first.identity, resolve(&cfg, tmp.path()).unwrap().identity);
        assert!(first.policy.text.contains("one"));
        assert!(repository_skill(tmp.path(), "../escape").is_err());
    }
}
