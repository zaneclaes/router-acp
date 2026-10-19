#![cfg(any(target_os = "macos", target_os = "linux"))]
use std::path::Path;
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

const RUNNER: &str = env!("CARGO_BIN_EXE_router-resource-run");

struct Job {
    child: Child,
    descendants: Vec<i32>,
}
impl Drop for Job {
    fn drop(&mut self) {
        for pid in &self.descendants {
            unsafe {
                libc::kill(*pid, libc::SIGKILL);
            }
        }
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

fn wait_for(mut condition: impl FnMut() -> bool) {
    let until = Instant::now() + Duration::from_secs(8);
    while Instant::now() < until {
        if condition() {
            return;
        }
        std::thread::sleep(Duration::from_millis(25));
    }
    assert!(condition(), "resource lifecycle did not settle");
}

fn pid_file(file: &Path) -> i32 {
    wait_for(|| {
        std::fs::read_to_string(file)
            .ok()
            .is_some_and(|s| s.trim().parse::<i32>().is_ok())
    });
    std::fs::read_to_string(file)
        .unwrap()
        .trim()
        .parse()
        .unwrap()
}

fn alive(pid: i32) -> bool {
    #[cfg(target_os = "macos")]
    {
        let mut info = std::mem::MaybeUninit::<libc::proc_bsdinfo>::uninit();
        let size = std::mem::size_of::<libc::proc_bsdinfo>() as i32;
        unsafe {
            libc::proc_pidinfo(
                pid,
                libc::PROC_PIDTBSDINFO,
                0,
                info.as_mut_ptr().cast(),
                size,
            ) == size
                && info.assume_init().pbi_status != 5
        }
    }
    #[cfg(target_os = "linux")]
    {
        std::fs::read_to_string(format!("/proc/{pid}/stat"))
            .is_ok_and(|s| !s.rsplit_once(')').unwrap().1.trim_start().starts_with('Z'))
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
fn short_command_cannot_leave_a_background_process() {
    let dir = tempfile::tempdir().unwrap();
    let file = dir.path().join("child");
    let child = Command::new(RUNNER)
        .env_remove("AGENT_GUARD_RESOURCES")
        .args([
            "--",
            "/bin/sh",
            "-c",
            "sleep 60 & echo $! > \"$1\"",
            "fixture",
        ])
        .arg(&file)
        .stdout(Stdio::null())
        .spawn()
        .unwrap();
    let mut job = Job {
        child,
        descendants: vec![],
    };
    let pid = pid_file(&file);
    job.descendants.push(pid);
    assert!(job.child.wait().unwrap().success());
    wait_for(|| !alive(pid));
}

#[test]
fn killed_owner_cleans_descendants_in_separate_sessions_and_groups() {
    let dir = tempfile::tempdir().unwrap();
    let child = Command::new(RUNNER)
        .env_remove("AGENT_GUARD_RESOURCES")
        .env("RESOURCE_FIXTURE_DIRECTORY", dir.path())
        .arg("--")
        .arg(std::env::current_exe().unwrap())
        .args(["--exact", "detached_fixture"])
        .stdout(Stdio::null())
        .spawn()
        .unwrap();
    let mut job = Job {
        child,
        descendants: vec![],
    };
    let detached = pid_file(&dir.path().join("detached"));
    let grandchild = pid_file(&dir.path().join("grandchild"));
    job.descendants.extend([detached, grandchild]);
    std::thread::sleep(Duration::from_secs(1));
    job.child.kill().unwrap();
    job.child.wait().unwrap();
    wait_for(|| !alive(detached) && !alive(grandchild));
}

#[test]
fn lost_watchdog_stops_owner_and_children_instead_of_freezing_itself() {
    let dir = tempfile::tempdir().unwrap();
    let file = dir.path().join("command");
    let child = Command::new(RUNNER)
        .env_remove("AGENT_GUARD_RESOURCES")
        .args([
            "--",
            "/bin/sh",
            "-c",
            "echo $$ > \"$1\"; exec sleep 60",
            "fixture",
        ])
        .arg(&file)
        .spawn()
        .unwrap();
    let mut job = Job {
        child,
        descendants: vec![],
    };
    let command = pid_file(&file);
    job.descendants.push(command);
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
    unsafe {
        libc::kill(watcher, libc::SIGKILL);
    }
    wait_for(|| job.child.try_wait().unwrap().is_some() && !alive(command));
}

#[test]
fn terminal_launcher_death_stops_the_owned_tree() {
    let dir = tempfile::tempdir().unwrap();
    let file = dir.path().join("child");
    let child = Command::new("/bin/sh")
        .env_remove("AGENT_GUARD_RESOURCES")
        .args([
            "-c",
            "\"$1\" -- /bin/sh -c 'echo $$ > \"$1\"; sleep 60' fixture \"$2\" & wait",
            "launcher",
            RUNNER,
        ])
        .arg(&file)
        .stdout(Stdio::null())
        .spawn()
        .unwrap();
    let mut job = Job {
        child,
        descendants: vec![],
    };
    let pid = pid_file(&file);
    job.descendants.push(pid);
    job.child.kill().unwrap();
    job.child.wait().unwrap();
    wait_for(|| !alive(pid));
}

#[test]
fn detached_fixture() {
    let Some(dir) = std::env::var_os("RESOURCE_FIXTURE_DIRECTORY") else {
        return;
    };
    use std::os::unix::process::CommandExt;
    assert!(unsafe { libc::setsid() } > 0);
    let mut child = Command::new("/bin/sleep")
        .arg("60")
        .process_group(0)
        .spawn()
        .unwrap();
    std::fs::write(
        Path::new(&dir).join("detached"),
        std::process::id().to_string(),
    )
    .unwrap();
    std::fs::write(Path::new(&dir).join("grandchild"), child.id().to_string()).unwrap();
    let _ = child.wait();
}
