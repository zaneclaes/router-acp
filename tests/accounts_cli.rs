//! Exercise the installed-shaped router executable through standalone ACP.
use serde_json::{Value, json};
use std::path::{Path, PathBuf};
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::process::{Child, ChildStdin, ChildStdout};
mod support;

struct Client {
    child: Child,
    input: Option<ChildStdin>,
    output: tokio::io::Lines<BufReader<ChildStdout>>,
    events: Vec<Value>,
    next_id: u64,
    answers: std::collections::VecDeque<String>,
}
impl Drop for Client {
    fn drop(&mut self) {
        support::stop_router(&mut self.child);
    }
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
        // A bound on a hung shutdown, not a speed check: stopping adapters and
        // flushing state took over 5 s under the parallel suite.
        tokio::time::timeout(Duration::from_secs(30), self.child.wait())
            .await
            .unwrap()
            .unwrap();
    }
}

/// A temp dir whose path is already canonical. The router canonicalizes
/// account directories, and macOS's temp dir is a symlink (`/var` →
/// `/private/var`), so a raw `tempdir()` path never matched what it recorded.
fn canonical_tempdir() -> tempfile::TempDir {
    tempfile::tempdir_in(std::env::temp_dir().canonicalize().unwrap()).unwrap()
}

fn fixture() -> (tempfile::TempDir, std::path::PathBuf) {
    let dir = canonical_tempdir();
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
    let runtime_prefix = runtime_prefix(account_dir);
    let diagnostic_path = path.clone();
    let result = tokio::time::timeout(Duration::from_secs(3), async move {
        loop {
            if let Ok(value) = tokio::fs::read_to_string(&path).await
                && ["login", "probe", "downstream"].iter().all(|label| {
                    value.lines().any(|line| {
                        let fields: Vec<_> = line.split('|').collect();
                        fields.first() == Some(label)
                            && fields
                                .get(1)
                                .is_some_and(|observed| observed.starts_with(&runtime_prefix))
                    })
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
    let runtime_prefix = runtime_prefix(account_dir);
    tokio::time::timeout(Duration::from_secs(3), async move {
        loop {
            if std::fs::read_to_string(&path)
                .unwrap_or_default()
                .lines()
                .filter(|line| {
                    let fields: Vec<_> = line.split('|').collect();
                    fields.first() == Some(&label)
                        && fields
                            .get(1)
                            .is_some_and(|observed| observed.starts_with(&runtime_prefix))
                })
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

fn runtime_prefix(account_dir: &Path) -> String {
    router_acp::local_state::root()
        .unwrap()
        .join("runtimes")
        .join(format!(
            "{}-",
            account_dir.file_name().unwrap().to_string_lossy()
        ))
        .to_string_lossy()
        .into_owned()
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

/// The accounts directory holds only the seats that remain: the base seat's
/// own directory (created at the router's start), never a cancelled add's.
fn remaining_seats(path: &Path) -> Vec<String> {
    let mut names: Vec<String> = std::fs::read_dir(path)
        .map(|entries| {
            entries
                .flatten()
                .map(|e| e.file_name().to_string_lossy().into_owned())
                .collect()
        })
        .unwrap_or_default();
    names.sort();
    names
}

async fn wait_for_only_base_seat(path: &Path, base: &str) {
    let path = path.to_path_buf();
    let base = base.to_string();
    tokio::time::timeout(Duration::from_secs(3), async move {
        loop {
            if remaining_seats(&path) == [base.clone()] {
                return;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("cancelled add did not remove its isolated directory");
}

fn login_fixture(provider: &str, existing: Option<&str>) -> LoginFixture {
    let dir = canonical_tempdir();
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
    printf '{"claudeAiOauth":{"accessToken":"fixture-access","expiresAt":4070908800000,"rateLimitTier":"pro"}}\n' > "$dir/.credentials.json"
else
    mkdir -p "$dir"
    printf '{"OPENAI_API_KEY":"fixture-access","email":"fixture-new@example.test","plan_type":"pro"}\n' > "$dir/auth.json"
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
                r#"{"claudeAiOauth":{"accessToken":"fixture-access","expiresAt":4070908800000,"rateLimitTier":"existing"}}"#,
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
                r#"{"OPENAI_API_KEY":"fixture-access","email":"existing@example.test","plan_type":"existing"}"#,
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
                r#"{"claudeAiOauth":{"accessToken":"fixture-access","expiresAt":4070908800000,"rateLimitTier":"home"}}"#,
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
                r#"{"OPENAI_API_KEY":"fixture-access","email":"home@example.test","plan_type":"home"}"#,
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
    let dir = canonical_tempdir();
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
        // Each account's usage lives in its own directory.
        let path = router_acp::usage_cache::account_usage_dir(&config_dir)
            .unwrap()
            .join("anthropic-oauth.json");
        router_acp::usage_cache::write_snapshot(&path, &snapshot).unwrap();
    }
    (dir, config, home, bin, provider_marker, curl_marker)
}

#[test]
fn account_status_reports_router_owned_capacity_outcomes() {
    let (_dir, config, home, _bin, _provider_marker, _curl_marker) = cache_usage_fixture();
    let cfg = router_acp::config::Config::from_file(&config).unwrap();
    let agent = cfg
        .agents
        .iter()
        .find(|agent| agent.name == "claude@work")
        .unwrap();
    let config_dir = PathBuf::from(agent.env_var("CLAUDE_CONFIG_DIR").unwrap())
        .canonicalize()
        .unwrap();
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs();
    let snapshot = router_acp::usage_cache::Snapshot {
        source: "anthropic-oauth".into(),
        account: router_acp::usage_cache::fingerprint("work-refresh"),
        access_generation: Some(router_acp::usage_cache::fingerprint("work-access")),
        fetched_at: now,
        attempted_at: now,
        consecutive_failures: 0,
        last_error: None,
        payload: Some(json!({"limits":[{
            "kind":"weekly",
            "percent":100,
            "resets_at":chrono::DateTime::from_timestamp((now + 3600) as i64, 0).unwrap().to_rfc3339()
        }]})),
    };
    let snapshot_path = router_acp::usage_cache::account_usage_dir(&config_dir)
        .unwrap()
        .join("anthropic-oauth.json");
    router_acp::usage_cache::write_snapshot(&snapshot_path, &snapshot).unwrap();

    let output = std::process::Command::new(env!("CARGO_BIN_EXE_router-acp"))
        .args(["account-status", "--config"])
        .arg(&config)
        .env("HOME", &home)
        .env(
            "XDG_STATE_HOME",
            router_acp::local_state::root().unwrap().parent().unwrap(),
        )
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let status: Value = serde_json::from_slice(&output.stdout).unwrap();
    let accounts = status["accounts"].as_array().unwrap();
    let work = accounts
        .iter()
        .find(|account| account["id"] == "claude@work")
        .unwrap();
    let personal = accounts
        .iter()
        .find(|account| account["id"] == "claude@personal")
        .unwrap();
    assert_eq!(work["routing"]["available"], false);
    assert_eq!(work["routing"]["reason"], "capacity");
    assert!(work["routing"]["resetsAt"].is_string());
    assert_eq!(personal["routing"]["available"], true);
    assert!(personal["routing"]["reason"].is_null());
    // Hosts read normalized usage here, never the snapshot files.
    assert_eq!(work["usage"]["windows"][0]["percent"], 100);
    assert_eq!(work["usage"]["windows"][0]["active"], true);
    assert_eq!(personal["usage"]["windows"][0]["percent"], 34);
    assert!(personal["usage"]["updatedAt"].is_string());
    assert!(personal["usage"]["error"].is_null());
    assert_eq!(personal["sharedDirectory"], false);
    assert_eq!(personal["priority"], 2);
    assert_eq!(
        status["revision"],
        router_acp::config_admin::revision(&std::fs::read(&config).unwrap())
    );
}

fn router(args: &[&str], config: &Path, stdin: Option<&Value>) -> std::process::Output {
    use std::io::Write;
    let mut child = std::process::Command::new(env!("CARGO_BIN_EXE_router-acp"))
        .args(args)
        .arg("--config")
        .arg(config)
        .stdin(std::process::Stdio::piped())
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .spawn()
        .unwrap();
    let mut input = child.stdin.take().unwrap();
    if let Some(value) = stdin {
        input.write_all(value.to_string().as_bytes()).unwrap();
    }
    drop(input);
    child.wait_with_output().unwrap()
}

fn json_of(output: &std::process::Output) -> Value {
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    serde_json::from_slice(&output.stdout).unwrap()
}

/// A host edits settings and account order only through router commands. A
/// settings save keeps the router's accounts, and a stale write is refused.
#[test]
fn config_commands_keep_router_accounts_and_refuse_stale_writes() {
    let (_dir, config, _home, _bin, _provider_marker, _curl_marker) = cache_usage_fixture();
    let shown = json_of(&router(&["config-show"], &config, None));
    let revision = shown["revision"].as_str().unwrap().to_string();
    let mut proposed = shown["config"].clone();
    proposed["agents"][0]["models"] =
        json!([{"id": "sonnet", "cost_rank": 2}, {"id": "opus", "cost_rank": 3}]);
    proposed["agents"][0]
        .as_object_mut()
        .unwrap()
        .remove("accounts");

    let applied = json_of(&router(
        &["config-apply", "--expect-revision", &revision],
        &config,
        Some(&proposed),
    ));
    let claude = &applied["config"]["agents"][0];
    assert_eq!(claude["models"].as_array().unwrap().len(), 2);
    assert_eq!(claude["accounts"].as_array().unwrap().len(), 2);

    let stale = router(
        &["config-apply", "--expect-revision", &revision],
        &config,
        Some(&proposed),
    );
    assert_eq!(stale.status.code(), Some(3));

    json_of(&router(
        &[
            "account-reserve",
            "--account",
            "claude@work",
            "--weekly",
            "15",
            "--session",
            "25",
        ],
        &config,
        None,
    ));
    json_of(&router(
        &[
            "account-order",
            "--provider",
            "claude",
            "--account",
            "claude@personal",
            "--account",
            "claude@work",
        ],
        &config,
        None,
    ));
    let missing = router(
        &[
            "account-order",
            "--provider",
            "claude",
            "--account",
            "claude@work",
        ],
        &config,
        None,
    );
    assert_eq!(missing.status.code(), Some(3));
    let status = json_of(&router(&["account-status"], &config, None));
    let account = |id: &str| {
        status["accounts"]
            .as_array()
            .unwrap()
            .iter()
            .find(|a| a["id"] == id)
            .unwrap()
            .clone()
    };
    assert_eq!(
        account("claude@work")["reserveCapacity"],
        json!({"weekly": 15.0, "session": 25.0})
    );
    assert_eq!(account("claude@personal")["priority"], 1);
    assert_eq!(account("claude@work")["priority"], 2);
}

#[tokio::test]
async fn standalone_grok_delete_preserves_a_shared_symlinked_store() {
    let root = canonical_tempdir();
    let home = root.path().join("home");
    let durable = root.path().join("durable-grok");
    std::fs::create_dir(&home).unwrap();
    std::fs::create_dir(&durable).unwrap();
    let alias = home.join(".grok");
    #[cfg(unix)]
    std::os::unix::fs::symlink(&durable, &alias).unwrap();
    let credential = durable.join("auth.json");
    let before = br#"{"https://accounts.x.ai/sign-in":{"key":"synthetic-access","refresh_token":"synthetic-refresh"}}"#;
    std::fs::write(&credential, before).unwrap();
    let config = root.path().join("router.yaml");
    std::fs::write(&config, format!(
        "state_file: {}\ndelegation: {{enabled: false}}\nagents:\n  - name: grok\n    command: {{type: stdio, command: {}, env: [{{name: HOME, value: {}}}, {{name: GROK_HOME, value: {}}}]}}\n    model_selection: {{type: config-option}}\n    models: [{{id: grok, cost_rank: 1}}]\n  - name: grok@alias\n    command: {{type: stdio, command: {}, env: [{{name: HOME, value: {}}}, {{name: GROK_HOME, value: {}}}]}}\n    model_selection: {{type: config-option}}\n    models: [{{id: grok, cost_rank: 1}}]\n",
        root.path().join("state.db").display(),
        env!("CARGO_BIN_EXE_mock-agent"), home.display(), durable.display(),
        env!("CARGO_BIN_EXE_mock-agent"), home.display(), alias.display()
    )).unwrap();
    let mut client = spawn_with_env(&config, &[("GROK_HOME", durable.to_str().unwrap())]);
    client
        .request(
            "initialize",
            json!({"protocolVersion":1,"clientCapabilities":{"elicitation":{"form":{}}}}),
        )
        .await;
    let created = client
        .request("session/new", json!({"cwd":home,"mcpServers":[]}))
        .await;
    let sid = created["result"]["sessionId"].as_str().unwrap().to_string();
    client
        .answers
        .extend(["3".into(), "1".into(), "2".into(), "2".into()]);
    client.prompt(&sid, "/login").await;
    assert!(
        client.text().contains(
            "Credentials were retained because another configured account shares that directory"
        ),
        "{}",
        client.text()
    );
    assert_eq!(std::fs::read(&credential).unwrap(), before);
    let saved = router_acp::config::Config::from_file(&config).unwrap();
    assert_eq!(
        saved
            .agents
            .iter()
            .filter(|agent| !agent.account_disabled)
            .map(|agent| agent.name.as_str())
            .collect::<Vec<_>>(),
        vec!["grok@alias"]
    );
    assert!(
        saved
            .agents
            .iter()
            .any(|agent| agent.name == "grok" && agent.account_disabled)
    );
    client
        .answers
        .extend(["3".into(), "1".into(), "2".into(), "2".into()]);
    client.prompt(&sid, "/login").await;
    assert!(!credential.exists());
    client.close().await;
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
    // The router's start already made the original seat an account.
    let original: Vec<String> = router_acp::config::Config::from_file(&fixture.config)
        .unwrap()
        .agents
        .iter()
        .map(|a| a.name.clone())
        .collect();
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
    let is_added = |a: &&router_acp::config::AgentConfig| {
        a.name.starts_with("codex@") && !original.contains(&a.name)
    };
    let added = cfg.agents.iter().find(is_added).unwrap();
    assert_eq!(cfg.agents.iter().filter(is_added).count(), 1);
    // 1-based: the original account is saved as 1, the added one as 2.
    assert_eq!(added.account_priority, Some(2));
    let added_dir = added.env_var("CODEX_HOME").unwrap();
    assert!(added_dir.starts_with(fixture.account_root.to_string_lossy().as_ref()));
    assert_eq!(
        std::fs::read(fixture.home.join(".codex/auth.json")).unwrap(),
        home_credentials
    );
    let recorded_dirs = wait_for_file(&fixture.login_state.join("dirs")).await;
    assert!(
        recorded_dirs
            .lines()
            .any(|line| line.starts_with(&runtime_prefix(Path::new(&added_dir)))),
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
            .any(|line| line.starts_with(&runtime_prefix(Path::new(&added_dir))))
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
        .filter(|line| line.contains(&runtime_prefix(&added_dir)))
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
            fields.len() == 7
                && fields[2..].iter().enumerate().all(|(index, value)| {
                    value.is_empty()
                        || (index == 2 && matches!(*value, "added-access" | "fixture-access"))
                })
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
        .filter(|line| line.contains(&runtime_prefix(&added_dir)))
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
            fields.len() == 7
                && fields[2..].iter().enumerate().all(|(index, value)| {
                    value.is_empty()
                        || (index == 2 && matches!(*value, "added-access" | "fixture-access"))
                })
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
    let updates = serde_json::to_string(&client.events).unwrap();
    assert!(
        !updates.contains("downstream connection failed"),
        "planned account replacement must not publish a downstream failure: {updates}"
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
            .any(|line| line.starts_with(&runtime_prefix(&existing_dir)))
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
            .any(|line| line.starts_with(&runtime_prefix(&existing_dir)))
    );
    restarted.close().await;
}

#[tokio::test]
async fn standalone_menu_sets_listed_account_priority_and_reserve() {
    let fixture = login_fixture("codex", Some("old"));
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
    // codex → codex@old → Set priority → position 1; then Set reserve
    // capacity → Weekly → 10%, and Session → 0%.
    for command in [
        "/login", "2", "1", "3", "1", "4", "1", "3", "2", "1", "/cancel",
    ] {
        assert!(
            client.prompt(&sid, command).await.get("result").is_some(),
            "{command}"
        );
    }
    let text = client.text();
    assert!(text.contains("Account moved to position 1."), "{text}");
    assert!(text.contains("weekly reserve set to 10%."), "{text}");
    assert!(text.contains("session reserve set to 0%."), "{text}");
    let cfg = router_acp::config::Config::from_file(&fixture.config).unwrap();
    let account = cfg.agents.iter().find(|a| a.name == "codex@old").unwrap();
    assert_eq!(account.account_priority, Some(1));
    assert_eq!(account.reserve_capacity.weekly, 10.0);
    assert_eq!(account.reserve_capacity.session, 0.0);
    assert_eq!(prompt_count(&fixture.original_log), 0);
    client.close().await;
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
    let delivered = log_events(&fixture.original_log)
        .into_iter()
        .rfind(|event| event["event"] == "prompt")
        .unwrap()["text"]
        .as_str()
        .unwrap()
        .to_string();
    assert!(delivered.contains(&sid) && delivered.contains("after cancelled re-login"));
    assert!(!delivered.contains("context before cancelled re-login"));
    let command = delivered
        .lines()
        .find(|line| line.contains(" transcript --state "))
        .unwrap();
    let output = std::process::Command::new("sh")
        .args(["-c", command.trim()])
        .output()
        .unwrap();
    assert!(output.status.success());
    assert!(
        String::from_utf8(output.stdout)
            .unwrap()
            .contains("context before cancelled re-login")
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
    let delivered = log_events(&fixture.original_log)
        .into_iter()
        .rfind(|event| event["event"] == "prompt")
        .unwrap()["text"]
        .as_str()
        .unwrap()
        .to_string();
    assert!(delivered.contains(&sid) && delivered.contains("after failed re-login"));
    assert!(!delivered.contains("context before failed re-login"));
    let command = delivered
        .lines()
        .find(|line| line.contains(" transcript --state "))
        .unwrap();
    let output = std::process::Command::new("sh")
        .args(["-c", command.trim()])
        .output()
        .unwrap();
    assert!(output.status.success());
    assert!(
        String::from_utf8(output.stdout)
            .unwrap()
            .contains("context before failed re-login")
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
    // The router's start already made the original seat an account.
    let original: Vec<String> = router_acp::config::Config::from_file(&fixture.config)
        .unwrap()
        .agents
        .iter()
        .map(|a| a.name.clone())
        .collect();
    let base = original
        .iter()
        .find(|name| name.starts_with("claude@"))
        .expect("the base seat is an account")
        .clone();
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
    wait_for_only_base_seat(&fixture.account_root, &base).await;

    let cfg = router_acp::config::Config::from_file(&fixture.config).unwrap();
    let names: Vec<String> = cfg.agents.iter().map(|a| a.name.clone()).collect();
    assert_eq!(names, original, "the cancelled add left no membership");
    assert_eq!(
        std::fs::read(fixture.home.join(".claude/.credentials.json")).unwrap(),
        home_credentials
    );
    assert_eq!(remaining_seats(&fixture.account_root), [base]);
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

/// A project naming one brokered server and one stdio server, and a router
/// whose MCP store is private to the test.
fn mcp_fixture() -> (tempfile::TempDir, PathBuf, PathBuf, String) {
    let (dir, config) = fixture();
    let project = dir.path().join("project");
    std::fs::create_dir_all(project.join(".git")).unwrap();
    std::fs::write(
        project.join(".mcp.json"),
        r#"{"mcpServers":{"kory":{"type":"http","url":"https://kory.example/mcp"},
            "local":{"command":"local-mcp"}}}"#,
    )
    .unwrap();
    let store = dir.path().join("mcp-store").display().to_string();
    (dir, config, project, store)
}

#[tokio::test]
async fn slash_mcp_lists_project_servers_with_their_sign_in_state() {
    let (_dir, config, project, store) = mcp_fixture();
    let mut client = spawn_with_env(&config, &[("ROUTER_ACP_MCP_DIR", &store)]);
    client
        .request(
            "initialize",
            json!({"protocolVersion":1,"clientCapabilities":{"elicitation":{"form":{}}}}),
        )
        .await;
    let created = client
        .request("session/new", json!({"cwd":project,"mcpServers":[]}))
        .await;
    let sid = created["result"]["sessionId"].as_str().unwrap().to_string();
    // kory → Back → local → Back → Close.
    client
        .answers
        .extend(["1", "2", "2", "1", "3"].map(String::from));
    let response = client.prompt(&sid, "/mcp").await;
    assert!(response["result"].is_object(), "{response}");
    let forms: Vec<(String, Vec<String>)> = client
        .events
        .iter()
        .filter(|f| f["method"] == "elicitation/create")
        .map(|f| {
            let choice = &f["params"]["requestedSchema"]["properties"]["choice"];
            let options = choice["oneOf"]
                .as_array()
                .map(|o| {
                    o.iter()
                        .map(|v| v["title"].as_str().unwrap().to_string())
                        .collect()
                })
                .unwrap_or_default();
            (
                choice["title"].as_str().unwrap_or_default().to_string(),
                options,
            )
        })
        .collect();
    assert_eq!(forms.len(), 5, "{forms:?}");
    assert!(forms[0].0.starts_with("MCP servers in "), "{forms:?}");
    assert_eq!(
        forms[0].1,
        vec![
            "kory: not signed in",
            "local: pass-through (stdio)",
            "Close"
        ]
    );
    assert!(
        forms[1].0.contains("URL: https://kory.example/mcp"),
        "{forms:?}"
    );
    assert_eq!(forms[1].1, vec!["Sign in", "Back"]);
    assert!(
        forms[3].0.contains("does not sign in to this entry"),
        "{forms:?}"
    );
    assert_eq!(forms[3].1, vec!["Back"]);
    client.close().await;
}

#[tokio::test]
async fn slash_mcp_works_as_a_numbered_menu_without_forms() {
    let (_dir, config, project, store) = mcp_fixture();
    let mut client = spawn_with_env(&config, &[("ROUTER_ACP_MCP_DIR", &store)]);
    client
        .request(
            "initialize",
            json!({"protocolVersion":1,"clientCapabilities":{}}),
        )
        .await;
    let created = client
        .request("session/new", json!({"cwd":project,"mcpServers":[]}))
        .await;
    let sid = created["result"]["sessionId"].as_str().unwrap().to_string();
    client.prompt(&sid, "/mcp").await;
    let text = client.text();
    assert!(text.contains("1. kory: not signed in"), "{text}");
    assert!(text.contains("2. local: pass-through (stdio)"), "{text}");
    client.prompt(&sid, "1").await;
    assert!(
        client.text().contains("1. Sign in\n2. Back"),
        "{}",
        client.text()
    );
    client.prompt(&sid, "/cancel").await;
    assert!(client.text().contains("MCP management closed."));
    let response = client
        .prompt(&sid, "/mcp callback http://localhost:1/callback?code=x")
        .await;
    assert!(response["result"].is_object(), "{response}");
    assert!(
        client.text().contains("No MCP sign-in is waiting"),
        "{}",
        client.text()
    );
    client.close().await;
}
