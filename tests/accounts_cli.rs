//! Exercise the installed-shaped router executable through standalone ACP.
use serde_json::{Value, json};
use std::path::{Path, PathBuf};
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::process::{Child, ChildStdin, ChildStdout};

struct Client {
    child: Child,
    input: Option<ChildStdin>,
    output: tokio::io::Lines<BufReader<ChildStdout>>,
    events: Vec<Value>,
    next_id: u64,
    answers: std::collections::VecDeque<String>,
}
impl Client {
    async fn request(&mut self, method: &str, params: Value) -> Value {
        self.next_id += 1;
        let id = self.next_id;
        let frame = json!({"jsonrpc":"2.0","id":id,"method":method,"params":params});
        self.input
            .as_mut()
            .unwrap()
            .write_all(format!("{frame}\n").as_bytes())
            .await
            .unwrap();
        self.input.as_mut().unwrap().flush().await.unwrap();
        tokio::time::timeout(Duration::from_secs(20), async {
            loop {
                let line = self
                    .output
                    .next_line()
                    .await
                    .unwrap()
                    .expect("router closed stdout");
                let frame: Value = serde_json::from_str(&line).unwrap();
                if frame["method"] == "elicitation/create" {
                    let result = match self.answers.pop_front() {
                        Some(answer) => json!({"action":"accept","content":{"choice":answer}}),
                        None => json!({"action":"cancel"}),
                    };
                    let response = json!({"jsonrpc":"2.0","id":frame["id"],"result":result});
                    self.input
                        .as_mut()
                        .unwrap()
                        .write_all(format!("{response}\n").as_bytes())
                        .await
                        .unwrap();
                    self.input.as_mut().unwrap().flush().await.unwrap();
                    self.events.push(frame);
                    continue;
                }
                if frame.get("id") == Some(&json!(id)) {
                    return frame;
                }
                self.events.push(frame);
            }
        })
        .await
        .expect("ACP response timeout")
    }
    async fn prompt(&mut self, sid: &str, text: &str) -> Value {
        self.request(
            "session/prompt",
            json!({"sessionId":sid,"prompt":[{"type":"text","text":text}]}),
        )
        .await
    }
    async fn notify(&mut self, method: &str, params: Value) {
        let frame = json!({"jsonrpc":"2.0","method":method,"params":params});
        self.input
            .as_mut()
            .unwrap()
            .write_all(format!("{frame}\n").as_bytes())
            .await
            .unwrap();
        self.input.as_mut().unwrap().flush().await.unwrap();
    }
    fn text(&self) -> String {
        self.events
            .iter()
            .filter_map(|v| {
                v.pointer("/params/update/content/text")
                    .and_then(Value::as_str)
            })
            .collect::<Vec<_>>()
            .join("\n")
    }
    async fn close(mut self) {
        self.input.take();
        tokio::time::timeout(Duration::from_secs(5), self.child.wait())
            .await
            .unwrap()
            .unwrap();
    }
}

fn fixture() -> (tempfile::TempDir, std::path::PathBuf) {
    let dir = tempfile::tempdir().unwrap();
    let config = dir.path().join("router.yaml");
    // A rejected probe must not make account-management ACP unreachable.
    std::fs::write(
        &config,
        format!(
            r#"
state_file: {}
delegation: {{enabled: false}}
agents:
  - name: claude
    command:
      type: stdio
      command: {}
      env: [{{name: MOCK_MODELS, value: sonnet}}, {{name: HOME, value: {}}}]
    model_selection: {{type: config-option}}
    models: [{{id: sonnet, cost_rank: 2}}]
    auth_probe: {{command: /bin/sh, args: [-c, "echo not logged in; exit 1"]}}
    accounts: [{{name: expired, priority: 0}}]
"#,
            dir.path().join("state.db").display(),
            env!("CARGO_BIN_EXE_mock-agent"),
            dir.path().display()
        ),
    )
    .unwrap();
    (dir, config)
}
fn spawn(config: &std::path::Path) -> Client {
    spawn_with_env(config, &[])
}

fn spawn_with_env(config: &std::path::Path, env: &[(&str, &str)]) -> Client {
    let mut command = tokio::process::Command::new(env!("CARGO_BIN_EXE_router-acp"));
    command
        .args(["serve", "--config", config.to_str().unwrap()])
        .stdin(std::process::Stdio::piped())
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::null())
        .kill_on_drop(true);
    for (name, value) in env {
        command.env(name, value);
    }
    let mut child = command.spawn().unwrap();
    Client {
        input: child.stdin.take(),
        output: BufReader::new(child.stdout.take().unwrap()).lines(),
        child,
        events: Vec::new(),
        next_id: 0,
        answers: Default::default(),
    }
}

struct LoginFixture {
    _dir: tempfile::TempDir,
    config: PathBuf,
    home: PathBuf,
    account_root: PathBuf,
    login_state: PathBuf,
    probe_log: PathBuf,
    ambient_log: PathBuf,
    original_log: PathBuf,
    added_log: PathBuf,
    existing_dir: Option<PathBuf>,
    login: PathBuf,
}

fn write_executable(path: &Path, body: &str) {
    std::fs::write(path, body).unwrap();
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o700)).unwrap();
    }
}

async fn wait_for_file(path: &Path) -> String {
    let path = path.to_path_buf();
    tokio::time::timeout(Duration::from_secs(3), async move {
        loop {
            if let Ok(value) = tokio::fs::read_to_string(&path).await
                && !value.trim().is_empty()
            {
                return value;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("fixture did not produce the expected file")
}

async fn wait_for_ambient_paths(path: &Path, account_dir: &Path) -> String {
    let path = path.to_path_buf();
    let account_dir = account_dir.to_string_lossy().to_string();
    let diagnostic_path = path.clone();
    let result = tokio::time::timeout(Duration::from_secs(3), async move {
        loop {
            if let Ok(value) = tokio::fs::read_to_string(&path).await
                && ["login", "probe", "downstream"].iter().all(|label| {
                    value
                        .lines()
                        .any(|line| line.starts_with(&format!("{label}|{account_dir}|")))
                })
            {
                return value;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await;
    result.unwrap_or_else(|_| {
        panic!(
            "added account did not exercise login, probe, and downstream paths: {}",
            std::fs::read_to_string(diagnostic_path).unwrap_or_default()
        )
    })
}

async fn wait_for_ambient_observations(
    path: &Path,
    label: &str,
    account_dir: &Path,
    minimum: usize,
) {
    let path = path.to_path_buf();
    let prefix = format!("{label}|{}|", account_dir.to_string_lossy());
    tokio::time::timeout(Duration::from_secs(3), async move {
        loop {
            if std::fs::read_to_string(&path)
                .unwrap_or_default()
                .lines()
                .filter(|line| line.starts_with(&prefix))
                .count()
                >= minimum
            {
                return;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("account adapter did not restart after login management");
}

async fn wait_for_event_count(path: &Path, event: &str, minimum: usize) {
    let path = path.to_path_buf();
    let event = event.to_string();
    tokio::time::timeout(Duration::from_secs(3), async move {
        loop {
            if log_events(&path)
                .iter()
                .filter(|entry| entry["event"] == event)
                .count()
                >= minimum
            {
                return;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("restarted account adapter did not complete its ACP probe");
}

async fn wait_for_process_exit(pid_path: &Path) {
    let pid: u32 = wait_for_file(pid_path).await.trim().parse().unwrap();
    tokio::time::timeout(Duration::from_secs(3), async move {
        loop {
            if !Path::new(&format!("/proc/{pid}")).exists() {
                return;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("login process did not exit after cancellation");
}

async fn wait_for_empty_dir(path: &Path) {
    let path = path.to_path_buf();
    tokio::time::timeout(Duration::from_secs(3), async move {
        loop {
            let empty = std::fs::read_dir(&path)
                .map(|entries| entries.count() == 0)
                .unwrap_or(true);
            if empty {
                return;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("cancelled add did not remove its isolated directory");
}

fn login_fixture(provider: &str, existing: Option<&str>) -> LoginFixture {
    let dir = tempfile::tempdir().unwrap();
    let home = dir.path().join("home");
    let account_root = home.join(".config/router-acp/accounts");
    let login_state = dir.path().join("login-state");
    let probe_log = dir.path().join("probe.log");
    let ambient_log = dir.path().join("ambient.log");
    let original_log = dir.path().join("original.jsonl");
    let added_log = dir.path().join("added.jsonl");
    std::fs::create_dir_all(&home).unwrap();
    std::fs::create_dir_all(&login_state).unwrap();

    let login = dir.path().join("mock-login");
    write_executable(
        &login,
        r#"#!/bin/sh
set -eu
state=${ROUTER_LOGIN_STATE:?}
dir=${CLAUDE_CONFIG_DIR:-${CODEX_HOME:-}}
printf 'login|%s|%s|%s|%s|%s|%s\n' "$dir" "${ANTHROPIC_API_KEY-}" "${ANTHROPIC_AUTH_TOKEN-}" "${CLAUDE_CODE_OAUTH_TOKEN-}" "${OPENAI_API_KEY-}" "${XAI_API_KEY-}" >> "$ROUTER_AMBIENT_LOG"
printf '%s\n' "$$" > "$state/pid"
printf 'started\n' >> "$state/events"
printf '%s\n' "$dir" >> "$state/dirs"
printf 'https://login.example/de'
sleep 0.03
printf 'vice?state=fixture\nDEVICE-CODE-4242\n'
sleep 0.1
if [ -n "${CLAUDE_CONFIG_DIR:-}" ]; then
    IFS= read -r code || true
    mkdir -p "$dir"
    printf '{"oauthAccount":{"emailAddress":"fixture-new@example.test"}}\n' > "$dir/.claude.json"
    printf '{"claudeAiOauth":{"rateLimitTier":"pro"}}\n' > "$dir/.credentials.json"
else
    mkdir -p "$dir"
    printf '{"email":"fixture-new@example.test","plan_type":"pro"}\n' > "$dir/auth.json"
fi
printf 'success\n' >> "$state/events"
exit 0
"#,
    );

    let probe = dir.path().join("mock-auth-probe");
    write_executable(
        &probe,
        r#"#!/bin/sh
set -eu
if [ -n "${CLAUDE_CONFIG_DIR:-}" ]; then
    dir="$CLAUDE_CONFIG_DIR"
elif [ -n "${CODEX_HOME:-}" ]; then
    dir="$CODEX_HOME"
elif [ -d "$HOME/.codex" ]; then
    dir="$HOME/.codex"
else
    dir="$HOME/.claude"
fi
printf 'probe|%s|%s|%s|%s|%s|%s\n' "$dir" "${ANTHROPIC_API_KEY-}" "${ANTHROPIC_AUTH_TOKEN-}" "${CLAUDE_CODE_OAUTH_TOKEN-}" "${OPENAI_API_KEY-}" "${XAI_API_KEY-}" >> "$ROUTER_AMBIENT_LOG"
printf '%s\n' "$dir" >> "$ROUTER_PROBE_LOG"
if [ -f "$dir/.credentials.json" ] || [ -f "$dir/auth.json" ]; then
    exit 0
fi
echo 'not signed in'
exit 1
"#,
    );

    let wrapper = dir.path().join("mock-downstream");
    write_executable(
        &wrapper,
        r#"#!/bin/sh
set -eu
dir=${CLAUDE_CONFIG_DIR:-${CODEX_HOME:-}}
printf 'downstream|%s|%s|%s|%s|%s|%s\n' "$dir" "${ANTHROPIC_API_KEY-}" "${ANTHROPIC_AUTH_TOKEN-}" "${CLAUDE_CODE_OAUTH_TOKEN-}" "${OPENAI_API_KEY-}" "${XAI_API_KEY-}" >> "$ROUTER_AMBIENT_LOG"
case "$dir" in
    */claude@*|*/codex@*) export MOCK_LOG="$ROUTER_ADDED_LOG" ;;
    *) export MOCK_LOG="$ROUTER_ORIGINAL_LOG" ;;
esac
exec "$@"
"#,
    );

    let existing_dir = existing.map(|name| {
        let path = dir.path().join(format!("existing-{name}"));
        std::fs::create_dir_all(&path).unwrap();
        if provider == "claude" {
            std::fs::write(
                path.join(".credentials.json"),
                r#"{"claudeAiOauth":{"rateLimitTier":"existing"}}"#,
            )
            .unwrap();
            std::fs::write(
                path.join(".claude.json"),
                r#"{"oauthAccount":{"emailAddress":"existing@example.test"}}"#,
            )
            .unwrap();
        } else {
            std::fs::write(
                path.join("auth.json"),
                r#"{"email":"existing@example.test","plan_type":"existing"}"#,
            )
            .unwrap();
        }
        path
    });

    if existing.is_none() {
        let base = home.join(if provider == "claude" {
            ".claude"
        } else {
            ".codex"
        });
        std::fs::create_dir_all(&base).unwrap();
        if provider == "claude" {
            std::fs::write(
                base.join(".credentials.json"),
                r#"{"claudeAiOauth":{"rateLimitTier":"home"}}"#,
            )
            .unwrap();
            std::fs::write(
                home.join(".claude.json"),
                r#"{"oauthAccount":{"emailAddress":"home@example.test"}}"#,
            )
            .unwrap();
        } else {
            std::fs::write(
                base.join("auth.json"),
                r#"{"email":"home@example.test","plan_type":"home"}"#,
            )
            .unwrap();
        }
    }

    let config = dir.path().join("router.yaml");
    let account_block = match (&existing_dir, provider) {
        (Some(existing_dir), "codex") => format!(
            "    usage_source: {{type: codex-rollout}}\n    accounts: [{{name: old, priority: 7, reserve_capacity: {{weekly: 12, session: 34}}, env: [{{name: CODEX_HOME, value: {}}}]}}]",
            existing_dir.display()
        ),
        (Some(existing_dir), "claude") => format!(
            "    accounts: [{{name: old, priority: 7, env: [{{name: CLAUDE_CONFIG_DIR, value: {}}}]}}]",
            existing_dir.display()
        ),
        _ => String::new(),
    };
    let provider_env = format!(
        "        - {{name: HOME, value: {}}}\n        - {{name: MOCK_MODELS, value: sonnet}}\n",
        home.display()
    );
    let yaml = format!(
        r#"
state_file: {}
delegation: {{enabled: false}}
cordon: {{enabled: true, poll_secs: 3600, min_refresh_secs: 3600}}
agents:
  - name: {}
    command:
      type: stdio
      command: {}
      args: [{}]
      env:
{}        - {{name: ROUTER_LOGIN_STATE, value: {}}}
        - {{name: ROUTER_PROBE_LOG, value: {}}}
        - {{name: ROUTER_AMBIENT_LOG, value: {}}}
        - {{name: ROUTER_ORIGINAL_LOG, value: {}}}
        - {{name: ROUTER_ADDED_LOG, value: {}}}
    model_selection: {{type: config-option}}
    models: [{{id: sonnet, cost_rank: 2}}]
    auth_probe: {{command: {}}}
    login_command:
      type: stdio
      command: {}
      args: []
{}
"#,
        dir.path().join("state.db").display(),
        provider,
        wrapper.display(),
        env!("CARGO_BIN_EXE_mock-agent"),
        provider_env,
        login_state.display(),
        probe_log.display(),
        ambient_log.display(),
        original_log.display(),
        added_log.display(),
        probe.display(),
        login.display(),
        account_block,
    );
    std::fs::write(&config, yaml).unwrap();
    LoginFixture {
        _dir: dir,
        config,
        home,
        account_root,
        login_state,
        probe_log,
        ambient_log,
        original_log,
        added_log,
        existing_dir,
        login,
    }
}

fn log_events(path: &Path) -> Vec<Value> {
    std::fs::read_to_string(path)
        .unwrap_or_default()
        .lines()
        .filter_map(|line| serde_json::from_str(line).ok())
        .collect()
}

fn prompt_count(path: &Path) -> usize {
    log_events(path)
        .iter()
        .filter(|event| event["event"] == "prompt")
        .count()
}

fn cache_usage_fixture() -> (
    tempfile::TempDir,
    PathBuf,
    PathBuf,
    PathBuf,
    PathBuf,
    PathBuf,
) {
    let dir = tempfile::tempdir().unwrap();
    let home = dir.path().join("home");
    let work = home.join("work-claude");
    let personal = home.join("personal-claude");
    let bin = dir.path().join("bin");
    let config = dir.path().join("router.yaml");
    let provider_marker = dir.path().join("provider.marker");
    let curl_marker = dir.path().join("curl.marker");
    std::fs::create_dir_all(&work).unwrap();
    std::fs::create_dir_all(&personal).unwrap();
    std::fs::create_dir_all(&bin).unwrap();

    for (name, path) in [("work", &work), ("personal", &personal)] {
        std::fs::write(
            path.join(".credentials.json"),
            format!(
                r#"{{"claudeAiOauth":{{"accessToken":"{name}-access","refreshToken":"{name}-refresh","expiresAt":4070908800000,"rateLimitTier":"pro"}}}}"#
            ),
        )
        .unwrap();
        std::fs::write(
            path.join(".claude.json"),
            format!(r#"{{"oauthAccount":{{"emailAddress":"{name}@example.test"}}}}"#),
        )
        .unwrap();
    }

    let provider = dir.path().join("fake-provider");
    write_executable(
        &provider,
        "#!/bin/sh\nprintf 'provider\\n' >> \"$ROUTER_PROVIDER_MARKER\"\n",
    );
    let curl = bin.join("curl");
    write_executable(
        &curl,
        "#!/bin/sh\ncat >/dev/null\nprintf 'curl\\n' >> \"$ROUTER_CURL_MARKER\"\nprintf '{\"limits\":[]}'\n",
    );

    std::fs::write(
        &config,
        format!(
            r#"
state_file: {}
delegation: {{enabled: false}}
cordon: {{enabled: true, poll_secs: 3600, min_refresh_secs: 3600}}
agents:
  - name: claude
    command:
      type: stdio
      command: {}
      env:
        - {{name: HOME, value: {}}}
        - {{name: ROUTER_PROVIDER_MARKER, value: {}}}
        - {{name: ROUTER_CURL_MARKER, value: {}}}
    model_selection: {{type: config-option}}
    models: [{{id: sonnet, cost_rank: 2}}]
    usage_source: {{type: anthropic-oauth}}
    auth_probe: {{command: {}}}
    accounts:
      - name: work
        env: [{{name: CLAUDE_CONFIG_DIR, value: {}}}]
      - name: personal
        env: [{{name: CLAUDE_CONFIG_DIR, value: {}}}]
"#,
            dir.path().join("state.db").display(),
            env!("CARGO_BIN_EXE_mock-agent"),
            home.display(),
            provider_marker.display(),
            curl_marker.display(),
            provider.display(),
            work.display(),
            personal.display(),
        ),
    )
    .unwrap();

    let cfg = router_acp::config::Config::from_file(&config).unwrap();
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs();
    let snapshot_dir = home.join(".local/state/router-acp/usage");
    std::fs::create_dir_all(&snapshot_dir).unwrap();
    for (agent, (credential_name, percent)) in
        cfg.agents.iter().zip([("work", 12), ("personal", 34)])
    {
        let config_dir = PathBuf::from(agent.env_var("CLAUDE_CONFIG_DIR").unwrap())
            .canonicalize()
            .unwrap();
        let access = format!("{credential_name}-access");
        let refresh = format!("{credential_name}-refresh");
        let snapshot = router_acp::usage_cache::Snapshot {
            source: "anthropic-oauth".into(),
            account: router_acp::usage_cache::fingerprint(&refresh),
            access_generation: Some(router_acp::usage_cache::fingerprint(&access)),
            fetched_at: now,
            attempted_at: now,
            consecutive_failures: 0,
            last_error: None,
            payload: Some(json!({"limits":[{"kind":"weekly","percent":percent}]})),
        };
        let path = snapshot_dir.join(format!(
            "anthropic-oauth-{}.json",
            router_acp::usage_cache::fingerprint(&config_dir.to_string_lossy())
        ));
        router_acp::usage_cache::write_snapshot(&path, &snapshot).unwrap();
    }
    (dir, config, home, bin, provider_marker, curl_marker)
}

#[tokio::test]
async fn standalone_usage_reads_two_account_snapshots_without_spawning_provider_commands() {
    let (_dir, config, home, bin, provider_marker, curl_marker) = cache_usage_fixture();
    let path = format!(
        "{}:{}",
        bin.display(),
        std::env::var("PATH").unwrap_or_default()
    );
    let home = home.to_string_lossy().to_string();
    let mut client = spawn_with_env(&config, &[("HOME", &home), ("PATH", &path)]);
    client
        .request(
            "initialize",
            json!({"protocolVersion":1,"clientCapabilities":{}}),
        )
        .await;
    let created = client
        .request("session/new", json!({"cwd":"/tmp","mcpServers":[]}))
        .await;
    let sid = created["result"]["sessionId"].as_str().unwrap().to_string();
    tokio::time::sleep(Duration::from_millis(100)).await;
    std::fs::write(&provider_marker, "").unwrap();
    std::fs::write(&curl_marker, "").unwrap();

    client.prompt(&sid, "/usage").await;
    let text = client.text();
    assert!(text.contains("work@example.test"), "{text}");
    assert!(text.contains("personal@example.test"), "{text}");
    assert!(text.contains("12%"), "{text}");
    assert!(text.contains("34%"), "{text}");
    assert_eq!(std::fs::read_to_string(&provider_marker).unwrap(), "");
    assert_eq!(std::fs::read_to_string(&curl_marker).unwrap(), "");
    client.close().await;
}

#[tokio::test]
async fn standalone_add_uses_isolated_login_and_routes_current_and_restarted() {
    let fixture = login_fixture("codex", None);
    let home_credentials = std::fs::read(fixture.home.join(".codex/auth.json")).unwrap();
    let mut client = spawn(&fixture.config);
    client
        .request(
            "initialize",
            json!({"protocolVersion":1,"clientCapabilities":{"elicitation":{"form":{}}}}),
        )
        .await;
    let created = client
        .request("session/new", json!({"cwd":"/tmp","mcpServers":[]}))
        .await;
    let sid = created["result"]["sessionId"].as_str().unwrap().to_string();

    client.answers.extend(["2".into(), "2".into()]);
    let response = client.prompt(&sid, "/login").await;
    assert!(response["result"].is_object(), "{response}");
    let management = client.text();
    assert!(
        management.contains("Open https://login.example/device"),
        "{management}"
    );
    assert!(
        management.contains("Device code: DEVICE-CODE-4242"),
        "{management}"
    );
    assert!(
        management.contains("Signed in as fixture-new@example.test"),
        "{management}"
    );
    assert_eq!(prompt_count(&fixture.original_log), 0);
    assert_eq!(prompt_count(&fixture.added_log), 0);

    let cfg = router_acp::config::Config::from_file(&fixture.config).unwrap();
    let added = cfg
        .agents
        .iter()
        .find(|agent| agent.name.starts_with("codex@"))
        .unwrap();
    assert_eq!(
        cfg.agents
            .iter()
            .filter(|a| a.name.starts_with("codex@"))
            .count(),
        1
    );
    assert_eq!(added.account_priority, Some(1));
    let added_dir = added.env_var("CODEX_HOME").unwrap();
    assert!(added_dir.starts_with(fixture.account_root.to_string_lossy().as_ref()));
    assert_eq!(
        std::fs::read(fixture.home.join(".codex/auth.json")).unwrap(),
        home_credentials
    );
    let recorded_dirs = wait_for_file(&fixture.login_state.join("dirs")).await;
    assert!(
        recorded_dirs.lines().any(|line| line == added_dir),
        "{recorded_dirs}"
    );
    assert!(
        std::fs::read_to_string(fixture.login_state.join("events"))
            .unwrap()
            .contains("success")
    );

    let candidate = format!(
        "[router: candidate={}/sonnet]\nuse the new login",
        added.name
    );
    client.prompt(&sid, &candidate).await;
    assert_eq!(prompt_count(&fixture.added_log), 1);
    assert!(log_events(&fixture.added_log).iter().any(|event| {
        event["text"]
            .as_str()
            .is_some_and(|text| text.contains("use the new login"))
    }));
    client.close().await;

    let mut restarted = spawn(&fixture.config);
    restarted
        .request(
            "initialize",
            json!({"protocolVersion":1,"clientCapabilities":{}}),
        )
        .await;
    let created = restarted
        .request("session/new", json!({"cwd":"/tmp","mcpServers":[]}))
        .await;
    let sid = created["result"]["sessionId"].as_str().unwrap();
    restarted.prompt(sid, &candidate).await;
    assert_eq!(prompt_count(&fixture.added_log), 2);
    assert!(
        std::fs::read_to_string(&fixture.probe_log)
            .unwrap()
            .lines()
            .any(|line| line == added_dir)
    );
    restarted.close().await;
}

#[tokio::test]
async fn standalone_delete_isolated_claude_account_preserves_sibling_and_mcp_credentials() {
    let fixture = login_fixture("claude", Some("old"));
    let sibling_dir = fixture.existing_dir.clone().unwrap();
    std::fs::write(
        sibling_dir.join(".credentials.json"),
        r#"{"mcpOAuth":{"sibling":"keep"}}"#,
    )
    .unwrap();
    std::fs::write(
        sibling_dir.join(".claude.json"),
        r#"{"mcpServers":{"sibling":{"token":"keep"}}}"#,
    )
    .unwrap();
    let sibling_credentials = std::fs::read(sibling_dir.join(".credentials.json")).unwrap();
    let sibling_claude = std::fs::read(sibling_dir.join(".claude.json")).unwrap();

    // Provider logout deletes OAuth data but leaves the configured membership.
    let mut client = spawn_with_env(
        &fixture.config,
        &[
            ("ANTHROPIC_API_KEY", "ambient-anthropic"),
            ("ANTHROPIC_AUTH_TOKEN", "ambient-auth-token"),
            ("CLAUDE_CODE_OAUTH_TOKEN", "ambient-claude"),
            ("OPENAI_API_KEY", "ambient-openai"),
            ("XAI_API_KEY", "ambient-xai"),
        ],
    );
    client
        .request(
            "initialize",
            json!({"protocolVersion":1,"clientCapabilities":{}}),
        )
        .await;
    let created = client
        .request("session/new", json!({"cwd":"/tmp","mcpServers":[]}))
        .await;
    let sid = created["result"]["sessionId"].as_str().unwrap().to_string();
    client.prompt(&sid, "/login").await;
    client.prompt(&sid, "1").await;
    let text = client.text();
    assert!(text.contains("claude (1 account)"), "{text}");
    assert!(text.contains("claude@old"), "{text}");
    client.close().await;

    let mut client = spawn_with_env(
        &fixture.config,
        &[
            ("ANTHROPIC_API_KEY", "ambient-anthropic"),
            ("ANTHROPIC_AUTH_TOKEN", "ambient-auth-token"),
            ("CLAUDE_CODE_OAUTH_TOKEN", "ambient-claude"),
            ("OPENAI_API_KEY", "ambient-openai"),
            ("XAI_API_KEY", "ambient-xai"),
        ],
    );
    client
        .request(
            "initialize",
            json!({"protocolVersion":1,"clientCapabilities":{"elicitation":{"form":{}}}}),
        )
        .await;
    let created = client
        .request("session/new", json!({"cwd":"/tmp","mcpServers":[]}))
        .await;
    let sid = created["result"]["sessionId"].as_str().unwrap().to_string();
    client
        .answers
        .extend(["1".into(), "2".into(), "AUTH-CODE".into()]);
    client.prompt(&sid, "/login").await;

    let cfg = router_acp::config::Config::from_file(&fixture.config).unwrap();
    let added = cfg
        .agents
        .iter()
        .find(|agent| agent.name.starts_with("claude@") && agent.name != "claude@old")
        .unwrap();
    let added_name = added.name.clone();
    let added_dir = PathBuf::from(added.env_var("CLAUDE_CONFIG_DIR").unwrap());
    std::fs::write(
        added_dir.join(".credentials.json"),
        r#"{"claudeAiOauth":{"accessToken":"added-access","refreshToken":"added-refresh","expiresAt":4070908800000,"rateLimitTier":"pro"},"mcpOAuth":{"added":"keep"}}"#,
    )
    .unwrap();
    std::fs::write(
        added_dir.join(".claude.json"),
        r#"{"oauthAccount":{"emailAddress":"added@example.test"},"mcpServers":{"added":{"token":"keep"}}}"#,
    )
    .unwrap();

    let ambient = wait_for_file(&fixture.ambient_log).await;
    let added_lines: Vec<_> = ambient
        .lines()
        .filter(|line| line.contains(added_dir.to_string_lossy().as_ref()))
        .collect();
    assert!(!added_lines.is_empty(), "{ambient}");
    for label in ["login", "downstream"] {
        assert!(
            added_lines
                .iter()
                .any(|line| line.starts_with(&format!("{label}|"))),
            "missing {label} ambient observation: {added_lines:?}"
        );
    }
    assert!(
        added_lines.iter().all(|line| {
            let fields: Vec<_> = line.split('|').collect();
            fields.len() == 7 && fields[2..].iter().all(|value| value.is_empty())
        }),
        "{added_lines:?}"
    );

    client.close().await;
    let mut client = spawn_with_env(
        &fixture.config,
        &[
            ("ANTHROPIC_API_KEY", "ambient-anthropic"),
            ("ANTHROPIC_AUTH_TOKEN", "ambient-auth-token"),
            ("CLAUDE_CODE_OAUTH_TOKEN", "ambient-claude"),
            ("OPENAI_API_KEY", "ambient-openai"),
            ("XAI_API_KEY", "ambient-xai"),
        ],
    );
    client
        .request(
            "initialize",
            json!({"protocolVersion":1,"clientCapabilities":{"elicitation":{"form":{}}}}),
        )
        .await;
    let created = client
        .request("session/new", json!({"cwd":"/tmp","mcpServers":[]}))
        .await;
    let sid = created["result"]["sessionId"].as_str().unwrap().to_string();
    let ambient = wait_for_ambient_paths(&fixture.ambient_log, &added_dir).await;
    let added_lines: Vec<_> = ambient
        .lines()
        .filter(|line| line.contains(added_dir.to_string_lossy().as_ref()))
        .collect();
    for label in ["login", "probe", "downstream"] {
        assert!(
            added_lines
                .iter()
                .any(|line| line.starts_with(&format!("{label}|"))),
            "missing {label} ambient observation: {added_lines:?}"
        );
    }
    assert!(
        added_lines.iter().all(|line| {
            let fields: Vec<_> = line.split('|').collect();
            fields.len() == 7 && fields[2..].iter().all(|value| value.is_empty())
        }),
        "{added_lines:?}"
    );

    client
        .answers
        .extend(["1".into(), "2".into(), "2".into(), "2".into()]);
    client.prompt(&sid, "/login").await;
    let text = client.text();
    assert!(text.contains("Account removed"), "{text}");

    let cfg = router_acp::config::Config::from_file(&fixture.config).unwrap();
    let deleted = cfg
        .agents
        .iter()
        .find(|agent| agent.name == added_name)
        .unwrap();
    assert!(deleted.account_disabled);
    assert_eq!(
        cfg.agents
            .iter()
            .filter(|agent| agent.name.starts_with("claude@") && !agent.account_disabled)
            .count(),
        1
    );
    let added_credentials: Value =
        serde_json::from_slice(&std::fs::read(added_dir.join(".credentials.json")).unwrap())
            .unwrap();
    assert!(added_credentials.get("claudeAiOauth").is_none());
    assert_eq!(added_credentials["mcpOAuth"]["added"], "keep");
    let added_claude: Value =
        serde_json::from_slice(&std::fs::read(added_dir.join(".claude.json")).unwrap()).unwrap();
    assert!(added_claude.get("oauthAccount").is_none());
    assert_eq!(added_claude["mcpServers"]["added"]["token"], "keep");
    assert_eq!(
        std::fs::read(sibling_dir.join(".credentials.json")).unwrap(),
        sibling_credentials
    );
    assert_eq!(
        std::fs::read(sibling_dir.join(".claude.json")).unwrap(),
        sibling_claude
    );
    client.close().await;
}

#[tokio::test]
async fn standalone_relogin_reuses_codex_directory_name_reserve_and_priority() {
    let fixture = login_fixture("codex", Some("old"));
    let existing_dir = fixture.existing_dir.clone().unwrap();
    let before = std::fs::read(existing_dir.join("auth.json")).unwrap();
    let mut client = spawn(&fixture.config);
    client
        .request(
            "initialize",
            json!({"protocolVersion":1,"clientCapabilities":{"elicitation":{"form":{}}}}),
        )
        .await;
    let created = client
        .request("session/new", json!({"cwd":"/tmp","mcpServers":[]}))
        .await;
    let sid = created["result"]["sessionId"].as_str().unwrap().to_string();

    client.answers.extend(["2".into(), "1".into(), "1".into()]);
    client.prompt(&sid, "/login").await;
    let text = client.text();
    assert!(text.contains("Device code: DEVICE-CODE-4242"), "{text}");
    assert!(
        text.contains("Signed in as fixture-new@example.test"),
        "{text}"
    );
    let cfg = router_acp::config::Config::from_file(&fixture.config).unwrap();
    assert_eq!(cfg.agents.len(), 1);
    let account = &cfg.agents[0];
    assert_eq!(account.name, "codex@old");
    assert_eq!(account.account_priority, Some(7));
    assert_eq!(account.reserve_capacity.weekly, 12.0);
    assert_eq!(account.reserve_capacity.session, 34.0);
    assert_eq!(
        account.env_var("CODEX_HOME").as_deref(),
        Some(existing_dir.to_str().unwrap())
    );
    let after = std::fs::read(existing_dir.join("auth.json")).unwrap();
    assert_ne!(after, before);
    assert!(
        String::from_utf8(after)
            .unwrap()
            .contains("fixture-new@example.test")
    );
    assert!(
        wait_for_file(&fixture.login_state.join("dirs"))
            .await
            .lines()
            .any(|line| line == existing_dir.to_string_lossy())
    );
    assert_eq!(prompt_count(&fixture.original_log), 0);
    client.close().await;

    let mut restarted = spawn(&fixture.config);
    restarted
        .request(
            "initialize",
            json!({"protocolVersion":1,"clientCapabilities":{}}),
        )
        .await;
    let created = restarted
        .request("session/new", json!({"cwd":"/tmp","mcpServers":[]}))
        .await;
    let sid = created["result"]["sessionId"].as_str().unwrap();
    restarted
        .prompt(
            sid,
            "[router: candidate=codex@old/sonnet]\nverify saved login",
        )
        .await;
    assert_eq!(prompt_count(&fixture.original_log), 1);
    assert!(
        std::fs::read_to_string(&fixture.probe_log)
            .unwrap()
            .lines()
            .any(|line| line == existing_dir.to_string_lossy())
    );
    restarted.close().await;
}

#[tokio::test]
async fn standalone_relogin_cancel_keeps_membership_and_kills_login() {
    let fixture = login_fixture("claude", Some("old"));
    let existing_dir = fixture.existing_dir.clone().unwrap();
    let before = std::fs::read(existing_dir.join(".credentials.json")).unwrap();
    let mut client = spawn(&fixture.config);
    client
        .request(
            "initialize",
            json!({"protocolVersion":1,"clientCapabilities":{}}),
        )
        .await;
    let created = client
        .request("session/new", json!({"cwd":"/tmp","mcpServers":[]}))
        .await;
    let sid = created["result"]["sessionId"].as_str().unwrap().to_string();
    let initial = client
        .prompt(
            &sid,
            "[router: candidate=claude@old/sonnet]\ncontext before cancelled re-login",
        )
        .await;
    assert!(initial["result"].is_object(), "{initial}");
    assert_eq!(prompt_count(&fixture.original_log), 1);
    for choice in ["/login", "1", "1", "1"] {
        client.prompt(&sid, choice).await;
    }
    wait_for_file(&fixture.login_state.join("pid")).await;
    client.prompt(&sid, "/login cancel").await;
    wait_for_process_exit(&fixture.login_state.join("pid")).await;
    assert!(client.text().contains("Login management closed"));
    assert!(
        !std::fs::read_to_string(fixture.login_state.join("events"))
            .unwrap()
            .contains("success")
    );
    assert_eq!(
        std::fs::read(existing_dir.join(".credentials.json")).unwrap(),
        before
    );
    let cfg = router_acp::config::Config::from_file(&fixture.config).unwrap();
    assert_eq!(cfg.agents[0].name, "claude@old");
    wait_for_ambient_observations(&fixture.ambient_log, "downstream", &existing_dir, 2).await;
    wait_for_event_count(&fixture.original_log, "session_new", 3).await;
    let response = client.prompt(&sid, "after cancelled re-login").await;
    assert!(
        response["result"].is_object(),
        "{response}\n{}",
        client.text()
    );
    assert!(
        log_events(&fixture.original_log).iter().any(|event| {
            event["event"] == "prompt"
                && event["text"].as_str().is_some_and(|text| {
                    text.contains("context before cancelled re-login")
                        && text.contains("after cancelled re-login")
                })
        }),
        "cancelled re-login did not hand off the pinned transcript: {:?}",
        log_events(&fixture.original_log)
    );
    client.close().await;
}

#[tokio::test]
async fn standalone_failed_relogin_preserves_credentials_and_hands_off_pinned_session() {
    let fixture = login_fixture("claude", Some("old"));
    let existing_dir = fixture.existing_dir.clone().unwrap();
    let before = std::fs::read(existing_dir.join(".credentials.json")).unwrap();
    write_executable(
        &fixture.login,
        "#!/bin/sh\necho 'fixture login failed' >&2\nexit 23\n",
    );

    let mut client = spawn(&fixture.config);
    client
        .request(
            "initialize",
            json!({"protocolVersion":1,"clientCapabilities":{}}),
        )
        .await;
    let created = client
        .request("session/new", json!({"cwd":"/tmp","mcpServers":[]}))
        .await;
    let sid = created["result"]["sessionId"].as_str().unwrap().to_string();
    let initial = client
        .prompt(
            &sid,
            "[router: candidate=claude@old/sonnet]\ncontext before failed re-login",
        )
        .await;
    assert!(initial["result"].is_object(), "{initial}");
    assert_eq!(prompt_count(&fixture.original_log), 1);

    for choice in ["/login", "1", "1", "1"] {
        client.prompt(&sid, choice).await;
    }
    assert!(client.text().contains("login failed"), "{}", client.text());
    assert_eq!(
        std::fs::read(existing_dir.join(".credentials.json")).unwrap(),
        before
    );
    wait_for_ambient_observations(&fixture.ambient_log, "downstream", &existing_dir, 2).await;
    wait_for_event_count(&fixture.original_log, "session_new", 3).await;

    let response = client.prompt(&sid, "after failed re-login").await;
    assert!(
        response["result"].is_object(),
        "{response}\n{}",
        client.text()
    );
    assert!(
        log_events(&fixture.original_log).iter().any(|event| {
            event["event"] == "prompt"
                && event["text"].as_str().is_some_and(|text| {
                    text.contains("context before failed re-login")
                        && text.contains("after failed re-login")
                })
        }),
        "failed re-login did not hand off the pinned transcript: {:?}",
        log_events(&fixture.original_log)
    );
    let cfg = router_acp::config::Config::from_file(&fixture.config).unwrap();
    assert_eq!(cfg.agents[0].name, "claude@old");
    client.close().await;
}

#[tokio::test]
async fn standalone_session_cancel_removes_pending_add_but_keeps_existing_membership() {
    let fixture = login_fixture("claude", None);
    let home_credentials = std::fs::read(fixture.home.join(".claude/.credentials.json")).unwrap();
    let mut client = spawn(&fixture.config);
    client
        .request(
            "initialize",
            json!({"protocolVersion":1,"clientCapabilities":{}}),
        )
        .await;
    let created = client
        .request("session/new", json!({"cwd":"/tmp","mcpServers":[]}))
        .await;
    let sid = created["result"]["sessionId"].as_str().unwrap().to_string();
    for choice in ["/login", "1", "2"] {
        client.prompt(&sid, choice).await;
    }
    wait_for_file(&fixture.login_state.join("pid")).await;
    client
        .notify("session/cancel", json!({"sessionId":sid}))
        .await;
    wait_for_process_exit(&fixture.login_state.join("pid")).await;
    client.prompt(&sid, "/login").await;
    wait_for_empty_dir(&fixture.account_root).await;

    let cfg = router_acp::config::Config::from_file(&fixture.config).unwrap();
    assert_eq!(
        cfg.agents
            .iter()
            .filter(|a| a.name.starts_with("claude@"))
            .count(),
        0
    );
    assert_eq!(
        std::fs::read(fixture.home.join(".claude/.credentials.json")).unwrap(),
        home_credentials
    );
    if fixture.account_root.exists() {
        assert_eq!(std::fs::read_dir(&fixture.account_root).unwrap().count(), 0);
    }
    assert!(
        !std::fs::read_to_string(fixture.login_state.join("events"))
            .unwrap()
            .contains("success")
    );
    assert_eq!(prompt_count(&fixture.original_log), 0);
    client.close().await;
}

#[tokio::test]
async fn standalone_structured_login_has_provider_account_and_action_forms() {
    let (_dir, config) = fixture();
    let mut client = spawn(&config);
    client.answers.extend(["1".into(), "1".into()]);
    client
        .request(
            "initialize",
            json!({"protocolVersion":1,"clientCapabilities":{"elicitation":{"form":{}}}}),
        )
        .await;
    let created = client
        .request("session/new", json!({"cwd":"/tmp","mcpServers":[]}))
        .await;
    let sid = created["result"]["sessionId"].as_str().unwrap();
    client.prompt(sid, "/login").await;
    let forms: Vec<_> = client
        .events
        .iter()
        .filter(|v| v["method"] == "elicitation/create")
        .collect();
    assert_eq!(forms.len(), 3);
    let rendered = serde_json::to_string(&forms).unwrap();
    assert!(rendered.contains("claude (1 account)"));
    assert!(rendered.contains("Add Account"));
    assert!(rendered.contains("Re-login"));
    assert!(!client.text().contains("echo:"));
    client.close().await;
}

#[tokio::test]
async fn standalone_login_retains_expired_membership_and_deletes_persistently() {
    let (_dir, config) = fixture();
    let mut client = spawn(&config);
    assert!(
        client
            .request(
                "initialize",
                json!({"protocolVersion":1,"clientCapabilities":{}})
            )
            .await
            .get("result")
            .is_some()
    );
    let created = client
        .request("session/new", json!({"cwd":"/tmp","mcpServers":[]}))
        .await;
    let sid = created["result"]["sessionId"].as_str().unwrap().to_string();
    for command in ["/login", "1", "1", "2", "2", "/usage"] {
        assert!(
            client.prompt(&sid, command).await.get("result").is_some(),
            "{command}"
        );
    }
    let text = client.text();
    assert!(text.contains("claude (1 account)"), "{text}");
    assert!(text.contains("Re-login"));
    assert!(text.contains("Account removed"));
    assert!(text.contains("claude (0 accounts)"));
    assert!(!text.contains("echo:"));
    let cfg = router_acp::config::Config::from_file(&config).unwrap();
    assert!(cfg.agents[0].account_disabled);
    client.close().await;
    let mut client = spawn(&config);
    client
        .request(
            "initialize",
            json!({"protocolVersion":1,"clientCapabilities":{}}),
        )
        .await;
    let created = client
        .request("session/new", json!({"cwd":"/tmp","mcpServers":[]}))
        .await;
    let sid = created["result"]["sessionId"].as_str().unwrap();
    client.prompt(sid, "/login").await;
    assert!(client.text().contains("claude (0 accounts)"));
    client.close().await;
}
