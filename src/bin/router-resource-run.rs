//! Guard standalone builds/tools too, including monitor work outside an agent session.
//! Kept independent of the router library so bootstrap needs only libc, not the router build.
#[path = "../resource_guard.rs"]
mod resource_guard;

fn main() -> std::io::Result<()> {
    use std::os::unix::process::{CommandExt, ExitStatusExt};
    let mut args: Vec<String> = std::env::args().collect();
    if let Some(result) = resource_guard::internal_entry(&args) {
        return result;
    }
    let inherit_group = args.get(1).map(String::as_str) == Some("--inherit-group");
    if inherit_group {
        args.remove(1);
    }
    if args.get(1).map(String::as_str) != Some("--") || args.len() < 3 {
        return Err(std::io::Error::other(
            "usage: router-resource-run -- <command> [arguments]",
        ));
    }
    let mut command = std::process::Command::new(&args[2]);
    command.args(&args[3..]);
    if !resource_guard::enabled() {
        return Err(command.exec());
    }
    // The runner is the durable group anchor, including when a very short command
    // leaves grandchildren before the watcher's first sample.
    if !inherit_group
        && unsafe { libc::getpgrp() } != std::process::id() as i32
        && unsafe { libc::setsid() } < 0
    {
        return Err(std::io::Error::last_os_error());
    }
    let guard = resource_guard::start(false)?;
    let result = command.status()?;
    drop(guard);
    std::process::exit(
        result
            .code()
            .unwrap_or_else(|| 128 + result.signal().unwrap_or(1)),
    );
}
