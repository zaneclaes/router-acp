#![cfg(any(target_os = "macos", target_os = "linux"))]
use std::io::Read;
use std::path::Path;
use std::process::{Child, ChildStdin, ChildStdout, Command, Stdio};
use std::time::Duration;

const RUNNER: &str = env!("CARGO_BIN_EXE_router-resource-run");
const ROUTER: &str = env!("CARGO_BIN_EXE_router-acp");

#[test]
fn router_commands_do_not_probe_resources_or_start_a_resource_watcher() {
    let dir = tempfile::tempdir().unwrap();
    let config = dir.path().join("missing-router-config.yaml");
    for command in ["serve", "prompt", "usage-monitor"] {
        let mut cli = Command::new(ROUTER);
        cli.arg(command).arg("--config").arg(&config);
        if command == "prompt" {
            cli.args(["--message", "fixture"]);
        }
        // The removed watcher probes temp_dir at startup and fails before config
        // loading. Router commands must reach their own config error instead,
        // without requiring the worker-resource opt-out.
        let result = cli
            .env_remove("AGENT_GUARD_RESOURCES")
            .env("TMPDIR", dir.path().join("missing-temp-directory"))
            .env("TMP", dir.path().join("missing-temp-directory"))
            .env("TEMP", dir.path().join("missing-temp-directory"))
            .output()
            .unwrap();
        assert!(!result.status.success());
        let error = String::from_utf8(result.stderr).unwrap();
        assert!(
            error.contains("missing-router-config.yaml"),
            "{command}: {error}"
        );
        assert!(!error.contains("AGENT_RESOURCES"), "{command}: {error}");
    }
}

#[test]
fn router_cannot_act_as_a_resource_watcher() {
    let result = Command::new(ROUTER)
        .args(["__resource-watch", "0", "0", "0", "0", "0", "0"])
        .env_remove("AGENT_GUARD_RESOURCES")
        .output()
        .unwrap();
    assert_eq!(result.status.code(), Some(2));
    assert!(
        String::from_utf8(result.stderr)
            .unwrap()
            .contains("unrecognized subcommand")
    );
}

struct Job {
    child: Child,
    input: Option<ChildStdin>,
    output: ChildStdout,
}
impl Job {
    fn new(mut child: Child) -> Self {
        Self {
            input: Some(child.stdin.take().unwrap()),
            output: child.stdout.take().unwrap(),
            child,
        }
    }

    fn assert_cleaned(&mut self) {
        // The test owns the input writer until this assertion completes. A cat
        // descendant cannot expire or receive EOF when its launcher dies, and
        // keeps the output pipe open until cleanup terminates it.
        assert_eq!(self.output.read(&mut [0]).unwrap(), 0);
    }
}
impl Drop for Job {
    fn drop(&mut self) {
        // Releasing the owned pipe also cleans fixtures after a failed assertion.
        // Never signal remembered descendant PIDs, which may have been reused.
        self.input.take();
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

fn pid_file(file: &Path, child: &mut Child) -> i32 {
    let read = || {
        std::fs::read_to_string(file)
            .ok()
            .and_then(|s| s.trim().parse::<i32>().ok())
    };
    loop {
        if let Some(pid) = read() {
            return pid;
        }
        if let Some(result) = child.try_wait().unwrap() {
            // The command may have written its receipt between the first read
            // and wait. A refused/failed launch has no receipt and fails here.
            return read()
                .unwrap_or_else(|| panic!("host fixture exited before launch receipt: {result}"));
        }
        std::thread::sleep(Duration::from_millis(25));
    }
}

// Observe the external executable through the OS, independently of its private
// ownership implementation. Only the deliberate watchdog-death trigger signals
// a PID; ordinary fixture teardown uses owned pipes and Child handles.
fn process_identity(pid: i32) -> std::io::Result<(i32, u64)> {
    #[cfg(target_os = "macos")]
    {
        let mut info = std::mem::MaybeUninit::<libc::proc_bsdinfo>::uninit();
        let size = std::mem::size_of::<libc::proc_bsdinfo>() as i32;
        if unsafe {
            libc::proc_pidinfo(
                pid,
                libc::PROC_PIDTBSDINFO,
                0,
                info.as_mut_ptr().cast(),
                size,
            )
        } != size
        {
            return Err(std::io::Error::last_os_error());
        }
        let info = unsafe { info.assume_init() };
        if info.pbi_status == 5 {
            return Err(std::io::ErrorKind::NotFound.into());
        }
        Ok((
            info.pbi_ppid as i32,
            info.pbi_start_tvsec * 1_000_000 + info.pbi_start_tvusec,
        ))
    }
    #[cfg(target_os = "linux")]
    {
        let text = std::fs::read_to_string(format!("/proc/{pid}/stat"))?;
        let (_, rest) = text
            .rsplit_once(')')
            .ok_or_else(|| std::io::Error::other("invalid process identity"))?;
        let fields: Vec<_> = rest.split_whitespace().collect();
        if fields.first() == Some(&"Z") {
            return Err(std::io::ErrorKind::NotFound.into());
        }
        let parent = fields.get(1).and_then(|field| field.parse().ok());
        let birth = fields.get(19).and_then(|field| field.parse().ok());
        parent
            .zip(birth)
            .ok_or_else(|| std::io::Error::other("invalid process identity"))
    }
}

fn signal_owned_process(
    owner: i32,
    mut observe: impl FnMut() -> std::io::Result<(i32, u64)>,
    signal: impl FnOnce() -> std::io::Result<()>,
) -> std::io::Result<()> {
    let identity = observe()?;
    if identity.0 != owner || observe()? != identity {
        return Err(std::io::ErrorKind::PermissionDenied.into());
    }
    signal()
}

#[test]
fn watchdog_signal_requires_a_stable_child_identity() {
    for (first, second, permitted) in [
        (Some((10, 100)), Some((10, 100)), true),
        (Some((10, 100)), Some((10, 200)), false),
        (Some((20, 100)), Some((20, 100)), false),
        (Some((10, 100)), None, false),
    ] {
        let mut readings = [first, second].into_iter();
        let mut signals = 0;
        let result = signal_owned_process(
            10,
            || {
                readings
                    .next()
                    .unwrap()
                    .ok_or(std::io::ErrorKind::PermissionDenied.into())
            },
            || {
                signals += 1;
                Ok(())
            },
        );
        assert_eq!(result.is_ok(), permitted);
        assert_eq!(signals, i32::from(permitted));
    }
}

#[test]
fn opt_out_execs_directly_without_helpers_even_without_path() {
    let dir = tempfile::tempdir().unwrap();
    let child = Command::new(RUNNER)
        .env("AGENT_GUARD_RESOURCES", "false")
        .env("PATH", "/nonexistent")
        .current_dir(dir.path())
        .args(["--", "/bin/sh", "-c", "printf '%s' \"$$\"; exit 23"])
        .stdout(Stdio::piped())
        .spawn()
        .unwrap();
    let pid = child.id();
    let result = child.wait_with_output().unwrap();
    assert_eq!(result.status.code(), Some(23));
    assert_eq!(String::from_utf8(result.stdout).unwrap(), pid.to_string());
}

#[test]
#[ignore = "host capability smoke: requires allowed native telemetry and resource headroom; run explicitly for activation"]
fn host_short_command_cannot_leave_a_background_process() {
    let child = Command::new(RUNNER)
        .env_remove("AGENT_GUARD_RESOURCES")
        .args(["--", "/bin/sh", "-c", "exec 3<&0; /bin/cat <&3 3<&- &"])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .spawn()
        .unwrap();
    let mut job = Job::new(child);
    assert!(job.child.wait().unwrap().success());
    job.assert_cleaned();
}

#[test]
#[ignore = "host capability smoke: requires allowed native telemetry and resource headroom; run explicitly for activation"]
fn host_lost_watchdog_stops_owner_and_children_instead_of_freezing_itself() {
    let dir = tempfile::tempdir().unwrap();
    let file = dir.path().join("command");
    let child = Command::new(RUNNER)
        .env_remove("AGENT_GUARD_RESOURCES")
        .args([
            "--",
            "/bin/sh",
            "-c",
            "echo $$ > \"$1\"; exec /bin/cat",
            "fixture",
        ])
        .arg(&file)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .spawn()
        .unwrap();
    let mut job = Job::new(child);
    let command = pid_file(&file, &mut job.child);
    let children = Command::new("pgrep")
        .arg("-P")
        .arg(job.child.id().to_string())
        .output()
        .unwrap();
    let watcher = String::from_utf8(children.stdout)
        .unwrap()
        .lines()
        .map(|s| s.parse::<i32>().unwrap())
        .find(|pid| *pid != command)
        .unwrap();
    signal_owned_process(
        job.child.id() as i32,
        || process_identity(watcher),
        || {
            if unsafe { libc::kill(watcher, libc::SIGKILL) } == 0 {
                Ok(())
            } else {
                Err(std::io::Error::last_os_error())
            }
        },
    )
    .unwrap();
    job.child.wait().unwrap();
    job.assert_cleaned();
}

#[test]
#[ignore = "host capability smoke: requires allowed native telemetry and resource headroom; run explicitly for activation"]
fn host_terminal_launcher_death_stops_the_owned_tree() {
    let dir = tempfile::tempdir().unwrap();
    let file = dir.path().join("child");
    let child = Command::new("/bin/sh")
        .env_remove("AGENT_GUARD_RESOURCES")
        .args([
            "-c",
            "exec 3<&0; \"$1\" -- /bin/sh -c 'echo $$ > \"$1\"; exec /bin/cat' fixture \"$2\" <&3 3<&- & wait",
            "launcher",
            RUNNER,
        ])
        .arg(&file)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .spawn()
        .unwrap();
    let mut job = Job::new(child);
    pid_file(&file, &mut job.child);
    job.child.kill().unwrap();
    job.child.wait().unwrap();
    job.assert_cleaned();
}
