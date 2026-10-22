/// Give the router's signal handler time to reap its separately grouped adapters
/// on assertion failure too; kill_on_drop alone SIGKILLs only the router.
#[allow(dead_code)]
pub fn stop_router(child: &mut tokio::process::Child) {
    if child.try_wait().ok().flatten().is_some() {
        return;
    }
    #[cfg(unix)]
    if let Some(pid) = child.id() {
        // This is an owned, unreaped child, so the PID cannot be reused here.
        unsafe {
            libc::kill(pid as i32, libc::SIGTERM);
        }
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(1);
        while std::time::Instant::now() < deadline {
            if child.try_wait().ok().flatten().is_some() {
                return;
            }
            std::thread::sleep(std::time::Duration::from_millis(10));
        }
    }
    let _ = child.start_kill();
}

/// A router executable that cannot inherit the test process's provider
/// credential stores (`cargo test` run by a router-spawned agent).
#[allow(dead_code)]
pub fn router_command() -> std::process::Command {
    let mut command = std::process::Command::new(env!("CARGO_BIN_EXE_router-acp"));
    for name in [
        "CLAUDE_CONFIG_DIR",
        "CODEX_HOME",
        "GROK_HOME",
        "GROK_AUTH_PROVIDER_COMMAND",
        "KIMI_SHARE_DIR",
        "KIMI_CODE_HOME",
    ] {
        command.env_remove(name);
    }
    command
}
