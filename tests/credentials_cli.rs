//! Drive credential workers across process boundaries, without live tokens.
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};
use std::time::Duration;

use axum::{
    Json, Router,
    routing::{get, post},
};
use serde_json::{Value, json};
use tokio::sync::Notify;

#[tokio::test]
async fn a_killed_grok_hook_cannot_cancel_rotation_or_repeat_a_siblings_repair() {
    let started = Arc::new(Notify::new());
    let release = Arc::new(Notify::new());
    let calls = Arc::new(AtomicUsize::new(0));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let issuer = format!("http://{}", listener.local_addr().unwrap());
    let endpoint = format!("{issuer}/token");
    let app = Router::new()
        .route("/.well-known/openid-configuration", get(move || async move {
            Json(json!({"token_endpoint":endpoint}))
        }))
        .route("/token", post({
            let started = started.clone();
            let release = release.clone();
            let calls = calls.clone();
            move || {
                let started = started.clone();
                let release = release.clone();
                let calls = calls.clone();
                async move {
                    calls.fetch_add(1, Ordering::SeqCst);
                    started.notify_one();
                    release.notified().await;
                    Json(json!({"access_token":"rotated-access", "refresh_token":"rotated-refresh", "expires_in":3600}))
                }
            }
        }));
    let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    let root = tempfile::tempdir().unwrap();
    let home = root.path().join("home");
    let dir = root.path().join("durable-grok");
    std::fs::create_dir(&home).unwrap();
    std::fs::create_dir(&dir).unwrap();
    #[cfg(unix)]
    std::os::unix::fs::symlink(&dir, home.join(".grok")).unwrap();
    let runtime = dir.join(".router-acp-runtime/test");
    std::fs::create_dir_all(&runtime).unwrap();
    let credentials = dir.join("auth.json");
    std::fs::write(
        &credentials,
        json!({"https://accounts.x.ai/sign-in":{
            "key":"original-access", "refresh_token":"original-refresh",
            "oidc_issuer":issuer, "oidc_client_id":"fixture-client",
        }})
        .to_string(),
    )
    .unwrap();
    let config = router_acp::config::Config::from_yaml(&format!(
        "agents:\n  - name: grok\n    command: {{type: stdio, command: grok, env: [{{name: HOME, value: {}}}]}}\n    model_selection: {{type: config-option}}\n    models: [{{id: grok, cost_rank: 1}}]\n",
        home.display()
    )).unwrap();
    let agent = &config.agents[0];
    let observed = router_acp::credentials::request_generation(agent).unwrap();
    let receipt = runtime.join(".router-acp-token.json");
    std::fs::write(&receipt, json!({"generation":observed}).to_string()).unwrap();
    let command = || {
        let mut command = tokio::process::Command::new(env!("CARGO_BIN_EXE_router-acp"));
        command
            .args(["credential-token", "--provider", "grok", "--directory"])
            .arg(&dir)
            .arg("--runtime-directory")
            .arg(&runtime)
            .env("GROK_AUTH_EXPIRED", "1")
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::piped());
        command
    };
    let mut helper = command().spawn().unwrap();
    tokio::time::timeout(Duration::from_secs(5), started.notified())
        .await
        .expect("refresh never started");
    helper.kill().await.unwrap();
    release.notify_one();
    tokio::time::timeout(Duration::from_secs(5), async {
        while router_acp::credentials::availability(agent)
            != router_acp::auth::AuthAvailability::Authenticated
        {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("hook termination cancelled token publication");
    let published: Value = serde_json::from_slice(&std::fs::read(&credentials).unwrap()).unwrap();
    assert_eq!(
        published["https://accounts.x.ai/sign-in"]["refresh_token"],
        "rotated-refresh"
    );
    // The killed hook did not acknowledge its runtime's old generation. The
    // next expired-token signal must reuse the worker's completed repair.
    let output = tokio::time::timeout(Duration::from_secs(5), command().output())
        .await
        .unwrap()
        .unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let token: Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(token["access_token"], "rotated-access");
    assert!(token.get("refresh_token").is_none());
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    assert_ne!(
        serde_json::from_slice::<Value>(&std::fs::read(receipt).unwrap()).unwrap()["generation"],
        observed
    );
    server.abort();
}

#[tokio::test]
async fn separate_router_processes_wait_for_one_kimi_rotation() {
    let started = Arc::new(Notify::new());
    let release = Arc::new(Notify::new());
    let calls = Arc::new(AtomicUsize::new(0));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let host = format!("http://{}", listener.local_addr().unwrap());
    let app = Router::new().route("/api/oauth/token", post({
        let started = started.clone();
        let release = release.clone();
        let calls = calls.clone();
        move || {
            let started = started.clone();
            let release = release.clone();
            let calls = calls.clone();
            async move {
                calls.fetch_add(1, Ordering::SeqCst);
                started.notify_one();
                release.notified().await;
                Json(json!({"access_token":"rotated-access", "refresh_token":"rotated-refresh", "expires_in":3600}))
            }
        }
    }));
    let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    let root = tempfile::tempdir().unwrap();
    let dir = root.path().join(".kimi");
    std::fs::create_dir_all(dir.join("credentials")).unwrap();
    let credentials = dir.join("credentials/kimi-code.json");
    std::fs::write(
        &credentials,
        json!({"access_token":"original-access", "refresh_token":"original-refresh"}).to_string(),
    )
    .unwrap();
    let config = router_acp::config::Config::from_yaml(&format!(
        "agents:\n  - name: kimi\n    command: {{type: stdio, command: kimi, env: [{{name: KIMI_SHARE_DIR, value: {}}}]}}\n    model_selection: {{type: config-option}}\n    models: [{{id: kimi, cost_rank: 1}}]\n", dir.display()
    )).unwrap();
    let agent = &config.agents[0];
    let observed = router_acp::credentials::request_generation(agent).unwrap();
    let spawn = || {
        tokio::process::Command::new(env!("CARGO_BIN_EXE_router-acp"))
            .args(["credential-repair", "--provider", "kimi", "--directory"])
            .arg(&dir)
            .args(["--observed", &observed])
            .env("KIMI_CODE_OAUTH_HOST", &host)
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .kill_on_drop(true)
            .spawn()
            .unwrap()
    };
    let mut first = spawn();
    tokio::time::timeout(Duration::from_secs(5), started.notified())
        .await
        .unwrap();
    let mut second = spawn();
    // The second process cannot finish or initiate a second refresh while
    // the first process holds this credential's OS lock.
    assert!(
        tokio::time::timeout(Duration::from_millis(100), second.wait())
            .await
            .is_err()
    );
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    release.notify_one();
    for child in [&mut first, &mut second] {
        assert!(
            tokio::time::timeout(Duration::from_secs(5), child.wait())
                .await
                .unwrap()
                .unwrap()
                .success()
        );
    }
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    assert_eq!(
        router_acp::credentials::availability(agent),
        router_acp::auth::AuthAvailability::Authenticated
    );
    let published: Value = serde_json::from_slice(&std::fs::read(credentials).unwrap()).unwrap();
    assert_eq!(published["refresh_token"], "rotated-refresh");
    server.abort();
}
