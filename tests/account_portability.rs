//! A config-directory transfer must preserve accounts without the source HOME,
//! provider CLIs, OS keychains, or any machine-wide provider configuration.
use std::path::Path;
use std::process::Command;

use serde_json::{Value, json};

fn status(home: &Path) -> Value {
    let output = Command::new(env!("CARGO_BIN_EXE_router-acp"))
        .args(["account-status", "--config"])
        .arg(home.join(".config/router-acp/router.yaml"))
        .env_clear()
        .env("HOME", home)
        // No provider executables, credential environment, or keychain helper.
        .env("PATH", home.join("empty-bin"))
        .output()
        .unwrap();
    assert!(output.status.success(), "account-status failed");
    serde_json::from_slice(&output.stdout).unwrap()
}

fn copy_tree(from: &Path, to: &Path) {
    std::fs::create_dir_all(to).unwrap();
    for entry in std::fs::read_dir(from).unwrap() {
        let entry = entry.unwrap();
        let target = to.join(entry.file_name());
        if entry.file_type().unwrap().is_dir() {
            copy_tree(&entry.path(), &target);
        } else {
            std::fs::copy(entry.path(), target).unwrap();
        }
    }
}

#[test]
fn copying_only_config_directory_preserves_four_accounts_in_a_different_home() {
    let root = tempfile::tempdir().unwrap();
    let source = root.path().join("Users/source-user");
    let destination = root.path().join("home/linux-user");
    let config = source.join(".config/router-acp");
    std::fs::create_dir_all(&config).unwrap();
    std::fs::write(
        config.join("router.yaml"),
        r#"agents:
- name: claude@personal
  account_priority: 1
  reserve_capacity: {weekly: 10, session: 10}
- name: codex@work
  reserve_capacity: {weekly: 20, session: 0}
- name: grok@work
  account_plan: SuperGrok
- name: claude@work
  account_priority: 0
  reserve_capacity: {weekly: 15, session: 5}
"#,
    )
    .unwrap();
    for (name, credential) in [
        (
            "claude@personal",
            json!({"claudeAiOauth": {
                "accessToken": "fixture-personal-access", "refreshToken": "fixture-personal-refresh",
                "subscriptionType": "max"
            }}),
        ),
        (
            "codex@work",
            json!({"email": "codex@example.test", "plan_type": "pro", "tokens": {
                "access_token": "fixture-codex-access", "refresh_token": "fixture-codex-refresh",
                "account_id": "fixture-codex-account"
            }}),
        ),
        (
            "grok@work",
            json!({"https://accounts.x.ai/sign-in": {
                "key": "fixture-grok-access", "refresh_token": "fixture-grok-refresh",
                "email": "grok@example.test"
            }}),
        ),
        (
            "claude@work",
            json!({"claudeAiOauth": {
                "accessToken": "fixture-work-access", "refreshToken": "fixture-work-refresh",
                "subscriptionType": "team"
            }}),
        ),
    ] {
        let dir = config.join("accounts").join(name);
        std::fs::create_dir_all(&dir).unwrap();
        let claude = name.starts_with("claude@");
        let file = if claude {
            ".credentials.json"
        } else {
            "auth.json"
        };
        std::fs::write(dir.join(file), serde_json::to_vec(&credential).unwrap()).unwrap();
        let material = if claude {
            &credential["claudeAiOauth"]
        } else {
            &credential
        };
        std::fs::write(dir.join(".router-acp-auth.json"), serde_json::to_vec(&json!({
            "generation": router_acp::usage_cache::fingerprint(&material.to_string()),
            "epoch": "fixture-epoch", "outcome": "Repaired", "attempted_at": "2026-10-18T00:00:00Z"
        })).unwrap()).unwrap();
        if claude {
            std::fs::write(
                dir.join(".claude.json"),
                serde_json::to_vec(&json!({
                    "oauthAccount": {"emailAddress": format!("{name}@example.test")}
                }))
                .unwrap(),
            )
            .unwrap();
        }
    }
    let before = status(&source);
    let accounts = before["accounts"].as_array().unwrap();
    assert_eq!(accounts.len(), 4);
    for account in accounts {
        assert_eq!(account["credentialPresent"], true);
        assert_eq!(account["authState"], "authenticated");
        assert_eq!(account["sharedDirectory"], false);
        assert_ne!(account["label"], account["id"]);
    }
    assert_eq!(accounts[0]["priority"], 1);
    assert_eq!(accounts[3]["priority"], 0);
    assert_eq!(accounts[2]["plan"], "SuperGrok");
    assert_eq!(accounts[1]["reserveCapacity"]["weekly"], 20.0);
    assert_eq!(accounts[3]["reserveCapacity"]["session"], 5.0);

    copy_tree(&config, &destination.join(".config/router-acp"));
    std::fs::remove_dir_all(&source).unwrap();
    assert_eq!(status(&destination), before);
}
