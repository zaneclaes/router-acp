use std::io::Write;
use std::path::PathBuf;
use std::process::{Command, Output, Stdio};

use serde_json::{Value, json};

struct Fixture {
    _temp: tempfile::TempDir,
    home: PathBuf,
    project: PathBuf,
    user: PathBuf,
    overlay: PathBuf,
}

impl Fixture {
    fn new() -> Self {
        let temp = tempfile::tempdir().unwrap();
        let root = temp.path().canonicalize().unwrap();
        let home = root.join("home");
        let project = root.join("project");
        let user = home.join(".config/router-acp/router.yaml");
        let overlay = project.join(".router-acp/router.yaml");
        std::fs::create_dir_all(user.parent().unwrap()).unwrap();
        std::fs::create_dir_all(overlay.parent().unwrap()).unwrap();
        std::fs::create_dir_all(project.join("src/nested")).unwrap();
        std::fs::write(project.join(".git"), "gitdir: fixture").unwrap();
        std::fs::write(&user, "history: 30d\nagents:\n- name: claude@shared\n  reserve_capacity: {weekly: 10, session: 5}\n- name: codex@shared\n").unwrap();
        std::fs::write(&overlay, "agents:\n- name: claude@shared\n  reserve_capacity: {weekly: 40}\n- name: codex@project\n").unwrap();
        Self {
            _temp: temp,
            home,
            project,
            user,
            overlay,
        }
    }

    fn command(&self, verb: &str) -> Command {
        let mut command = Command::new(env!("CARGO_BIN_EXE_router-acp"));
        command
            .arg(verb)
            .env_clear()
            .env("HOME", &self.home)
            .env("PATH", self.home.join("empty-bin"))
            .env("XDG_STATE_HOME", self.home.join(".local/state"))
            .current_dir(&self.project);
        command
    }

    fn show(&self) -> Value {
        parsed(
            self.command("config-show")
                .arg("--config")
                .arg(&self.overlay)
                .output()
                .unwrap(),
        )
    }
}

fn parsed(output: Output) -> Value {
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    serde_json::from_slice(&output.stdout).unwrap()
}

fn selected(output: Output) -> PathBuf {
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    String::from_utf8(output.stdout).unwrap().trim().into()
}

#[test]
fn discovery_prefers_explicit_then_environment_then_project_then_user() {
    let f = Fixture::new();
    assert_eq!(
        selected(
            f.command("config-path")
                .args(["--cwd", "src/nested"])
                .output()
                .unwrap()
        ),
        f.overlay
    );
    assert_eq!(
        selected(
            f.command("config-path")
                .env("ROUTER_ACP_CONFIG", "$HOME/special.yaml")
                .output()
                .unwrap()
        ),
        f.home.join("special.yaml")
    );
    assert_eq!(
        selected(
            f.command("config-path")
                .env("ROUTER_ACP_CONFIG", "ignored.yaml")
                .args(["--config", "~/explicit.yaml"])
                .output()
                .unwrap()
        ),
        f.home.join("explicit.yaml")
    );
    std::fs::write(f.project.join("src/.git"), "nested repo").unwrap();
    assert_eq!(
        selected(
            f.command("config-path")
                .args(["--cwd", "src/nested"])
                .output()
                .unwrap()
        ),
        f.user
    );
    assert!(
        !f.command("config-path")
            .env("ROUTER_ACP_CONFIG", "")
            .output()
            .unwrap()
            .status
            .success()
    );
}

#[test]
fn reserve_and_priority_edits_preserve_inheritance_and_global_credentials() {
    let f = Fixture::new();
    let global = std::fs::read(&f.user).unwrap();
    let credential = f
        .home
        .join(".config/router-acp/accounts/codex@shared/auth.json");
    std::fs::create_dir_all(credential.parent().unwrap()).unwrap();
    std::fs::write(
        &credential,
        serde_json::to_vec(
            &json!({"tokens":{"access_token":"fixture","refresh_token":"fixture-refresh"}}),
        )
        .unwrap(),
    )
    .unwrap();
    let shown = f.show();
    assert_eq!(shown["config"]["agents"].as_array().unwrap().len(), 3);
    assert_eq!(
        shown["config"]["agents"][0]["reserve_capacity"],
        json!({"weekly":40,"session":5})
    );
    let status = parsed(
        f.command("account-status")
            .arg("--config")
            .arg(&f.overlay)
            .output()
            .unwrap(),
    );
    assert!(
        status["accounts"]
            .as_array()
            .unwrap()
            .iter()
            .any(|a| a["id"] == "codex@shared" && a["credentialPresent"] == true)
    );
    parsed(
        f.command("account-reserve")
            .arg("--config")
            .arg(&f.overlay)
            .args([
                "--account",
                "claude@shared",
                "--weekly",
                "75",
                "--session",
                "20",
            ])
            .output()
            .unwrap(),
    );
    parsed(
        f.command("account-order")
            .arg("--config")
            .arg(&f.overlay)
            .args([
                "--provider",
                "codex",
                "--account",
                "codex@project",
                "--account",
                "codex@shared",
            ])
            .output()
            .unwrap(),
    );
    assert_eq!(std::fs::read(&f.user).unwrap(), global);
    let saved: Value = serde_yaml::from_str(&std::fs::read_to_string(&f.overlay).unwrap()).unwrap();
    assert!(saved.get("history").is_none());
    assert!(saved["agents"][0].get("command").is_none());
    assert_eq!(
        f.show()["config"]["agents"][0]["reserve_capacity"]["weekly"],
        75.0
    );
    std::fs::write(
        &f.user,
        String::from_utf8(global).unwrap().replace("30d", "45d"),
    )
    .unwrap();
    let updated = f.show();
    assert_eq!(updated["config"]["history"], "45d");
    assert_ne!(updated["revision"], shown["revision"]);
    assert!(credential.exists());
    assert!(!f.project.join(".router-acp/accounts").exists());
    assert!(!f.project.join(".router-acp/router.lock").exists());
}

#[test]
fn settings_saves_write_only_the_overlay_and_detect_user_config_changes() {
    let f = Fixture::new();
    // This explicit override must survive an unrelated settings save, even
    // though it equals the inherited value today.
    std::fs::write(
        &f.overlay,
        "agents:\n- name: claude@shared\n  reserve_capacity: {weekly: 10}\n",
    )
    .unwrap();
    let shown = f.show();
    let mut proposed = shown["config"].clone();
    proposed["history"] = json!("60d");
    let mut child = f
        .command("config-apply")
        .arg("--config")
        .arg(&f.overlay)
        .arg("--expect-revision")
        .arg(shown["revision"].as_str().unwrap())
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    child
        .stdin
        .take()
        .unwrap()
        .write_all(&serde_json::to_vec(&proposed).unwrap())
        .unwrap();
    let saved = parsed(child.wait_with_output().unwrap());
    assert_eq!(saved["config"]["history"], "60d");
    let global = std::fs::read_to_string(&f.user).unwrap();
    assert!(global.contains("30d"));
    std::fs::write(
        &f.user,
        global
            .replace("weekly: 10", "weekly: 12")
            .replace("session: 5", "session: 6"),
    )
    .unwrap();
    let mut child = f
        .command("config-apply")
        .arg("--config")
        .arg(&f.overlay)
        .arg("--expect-revision")
        .arg(saved["revision"].as_str().unwrap())
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    child
        .stdin
        .take()
        .unwrap()
        .write_all(&serde_json::to_vec(&proposed).unwrap())
        .unwrap();
    assert_eq!(child.wait_with_output().unwrap().status.code(), Some(3));
    assert_eq!(
        f.show()["config"]["agents"][0]["reserve_capacity"]["session"],
        6
    );
    assert_eq!(
        f.show()["config"]["agents"][0]["reserve_capacity"]["weekly"],
        10
    );
}

#[test]
fn duplicate_overlay_accounts_are_rejected_and_missing_user_config_is_supported() {
    let f = Fixture::new();
    std::fs::write(
        &f.overlay,
        "agents:\n- name: claude@shared\n- name: claude@shared\n",
    )
    .unwrap();
    assert!(
        !f.command("config-show")
            .arg("--config")
            .arg(&f.overlay)
            .output()
            .unwrap()
            .status
            .success()
    );
    std::fs::remove_file(&f.user).unwrap();
    std::fs::write(&f.overlay, "agents:\n- name: codex@project\n").unwrap();
    let status = parsed(
        f.command("account-status")
            .arg("--config")
            .arg(&f.overlay)
            .output()
            .unwrap(),
    );
    assert_eq!(status["accounts"].as_array().unwrap().len(), 1);
}

#[cfg(unix)]
#[test]
fn symlinked_project_file_stays_an_overlay_after_account_edits() {
    let f = Fixture::new();
    let target = f.home.join("synced-project.yaml");
    std::fs::rename(&f.overlay, &target).unwrap();
    std::os::unix::fs::symlink(&target, &f.overlay).unwrap();
    parsed(
        f.command("account-reserve")
            .arg("--config")
            .arg(&f.overlay)
            .args([
                "--account",
                "claude@shared",
                "--weekly",
                "80",
                "--session",
                "25",
            ])
            .output()
            .unwrap(),
    );
    assert!(f.overlay.is_symlink());
    assert_eq!(f.show()["config"]["agents"].as_array().unwrap().len(), 3);
    let saved: Value = serde_yaml::from_str(&std::fs::read_to_string(target).unwrap()).unwrap();
    assert!(saved.get("history").is_none());
}
