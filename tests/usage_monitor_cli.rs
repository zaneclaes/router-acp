//! Exercise usage polling through the native executable, without ACP sessions.
use serde_json::{Value, json};
use std::time::Duration;

#[cfg(unix)]
#[tokio::test]
async fn standalone_monitor_refreshes_isolated_accounts_without_starting_adapters() {
    use std::os::unix::fs::PermissionsExt;

    let root = tempfile::tempdir().unwrap();
    let home = root.path();
    let bin = home.join("bin");
    std::fs::create_dir(&bin).unwrap();
    let cli = bin.join("codex");
    std::fs::write(
        &cli,
        r#"#!/bin/sh
while IFS= read -r frame; do
  case "$frame" in
    *'"method":"initialize"'*) printf '%s\n' '{"id":1,"result":{}}' ;;
    *'"method":"account/rateLimits/read"'*)
      printf '%s\n' "$CODEX_HOME" >> "$HOME/usage-reads"
      printf '%s\n' '{"id":2,"result":{"rateLimits":{"primary":{"usedPercent":42,"windowDurationMins":300,"resetsAt":4070908800}}}}'
      ;;
  esac
done
"#,
    )
    .unwrap();
    std::fs::set_permissions(&cli, std::fs::Permissions::from_mode(0o700)).unwrap();

    let mut agents = Vec::new();
    for name in ["first", "second"] {
        let dir = home.join(name);
        std::fs::create_dir(&dir).unwrap();
        std::fs::write(
            dir.join("auth.json"),
            json!({"OPENAI_API_KEY":name}).to_string(),
        )
        .unwrap();
        agents.push(format!(
            "  - name: codex@{name}\n    lineage: openai\n    command:\n      type: stdio\n      command: /bin/false\n      env: [{{name: CODEX_HOME, value: {}}}]\n    model_selection: {{type: config-option}}\n    models: [{{id: gpt-5.5, cost_rank: 1}}]\n    usage_source: {{type: codex-rollout}}\n",
            dir.display()
        ));
    }
    let config = home.join("router.yaml");
    std::fs::write(
        &config,
        format!(
            "state_file: {}\ncordon: {{poll_secs: 30, min_refresh_secs: 60}}\nagents:\n{}",
            home.join("state.db").display(),
            agents.join("")
        ),
    )
    .unwrap();

    let spawn_monitor = || {
        tokio::process::Command::new(env!("CARGO_BIN_EXE_router-acp"))
            .args(["usage-monitor", "--config", config.to_str().unwrap()])
            .env("HOME", home)
            .env("PATH", format!("{}:/usr/bin:/bin", bin.display()))
            .stdin(std::process::Stdio::null())
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::inherit())
            .kill_on_drop(true)
            .spawn()
            .unwrap()
    };
    let mut child = spawn_monitor();

    let usage = home.join(".local/state/router-acp/usage");
    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            let snapshots: Vec<Value> = std::fs::read_dir(&usage)
                .into_iter()
                .flatten()
                .flatten()
                .filter(|entry| entry.path().extension().is_some_and(|ext| ext == "json"))
                .filter_map(|entry| std::fs::read(entry.path()).ok())
                .filter_map(|raw| serde_json::from_slice(&raw).ok())
                .collect();
            if snapshots.len() == 2 {
                assert_ne!(snapshots[0]["account"], snapshots[1]["account"]);
                for snap in snapshots {
                    assert_eq!(snap["payload"]["rateLimits"]["primary"]["usedPercent"], 42);
                    assert!(snap["last_error"].is_null());
                    assert!(snap["fetched_at"].as_u64().unwrap() > 0);
                }
                break;
            }
            assert!(
                child.try_wait().unwrap().is_none(),
                "monitor exited before publishing usage"
            );
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("native usage monitor did not publish both account snapshots");

    stop_monitor(&mut child).await;
    let reads = std::fs::read_to_string(home.join("usage-reads")).unwrap();
    assert_eq!(reads.lines().count(), 2);
    for name in ["first", "second"] {
        assert!(reads.contains(home.join(name).to_str().unwrap()));
        let auth: Value =
            serde_json::from_slice(&std::fs::read(home.join(name).join("auth.json")).unwrap())
                .unwrap();
        assert_eq!(auth["OPENAI_API_KEY"], name);
    }

    // Interrupt a native poll with a live Codex reader. The reader must die,
    // rather than becoming another orphaned credential writer after restart.
    std::fs::remove_dir_all(&usage).unwrap();
    std::fs::write(
        &cli,
        r#"#!/bin/sh
while IFS= read -r frame; do
  case "$frame" in
    *'"method":"initialize"'*) printf '%s\n' '{"id":1,"result":{}}' ;;
    *'"method":"account/rateLimits/read"'*)
      printf '%s\n' "$$" > "$HOME/usage-child"
      IFS= read -r blocked
      ;;
  esac
done
"#,
    )
    .unwrap();
    let mut child = spawn_monitor();
    let reader_pid = tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            if let Ok(pid) = std::fs::read_to_string(home.join("usage-child")) {
                break pid.trim().parse::<u32>().unwrap();
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    stop_monitor(&mut child).await;
    #[cfg(target_os = "linux")]
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let state = std::fs::read_to_string(format!("/proc/{reader_pid}/stat"));
            if state.is_err() || state.unwrap().split_whitespace().nth(2) == Some("Z") {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("Codex reader survived monitor shutdown");
    #[cfg(not(target_os = "linux"))]
    let _ = reader_pid;
}

#[cfg(unix)]
async fn stop_monitor(child: &mut tokio::process::Child) {
    let status = std::process::Command::new("kill")
        .args(["-TERM", &child.id().unwrap().to_string()])
        .status()
        .unwrap();
    assert!(status.success());
    let exit = tokio::time::timeout(Duration::from_secs(5), child.wait())
        .await
        .expect("native monitor did not stop")
        .unwrap();
    assert!(exit.success());
}
