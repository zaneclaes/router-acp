//! Resource containment for explicitly wrapped worker commands only.
//! Sampling is native (no ps/sysctl/df subprocesses), and only verified descendants
//! may be signalled. The detached watcher survives loss of the terminal/router.

use std::collections::BTreeMap;
use std::io::{self, Read, Write};
use std::path::{Path, PathBuf};
use std::process::{ChildStdin, Command, Stdio};
use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};
use std::time::Duration;

const GIB: u64 = 1024 * 1024 * 1024;
const HEADROOM: u64 = 8 * GIB;
const DISK_FLOOR: u64 = 10 * GIB;
const INTERVAL: Duration = Duration::from_millis(250);

pub fn enabled() -> bool {
    std::env::var_os("AGENT_GUARD_RESOURCES").as_deref() != Some(std::ffi::OsStr::new("false"))
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct Process {
    pid: i32,
    parent: i32,
    group: i32,
    birth: u64,
}

#[derive(Default)]
struct Tree(BTreeMap<i32, Process>, BTreeMap<i32, u64>);

impl Tree {
    fn recursive_launch(&self) -> bool {
        self.0.values().any(|process| {
            let mut pid = process.pid;
            for _ in 0..32 {
                let Some(parent) = self.0.get(&pid) else {
                    return false;
                };
                pid = parent.parent;
            }
            true
        })
    }
    fn new(root: Process) -> Self {
        let groups = if root.pid == root.group {
            BTreeMap::from([(root.pid, root.birth)])
        } else {
            BTreeMap::new()
        };
        Self(BTreeMap::from([(root.pid, root)]), groups)
    }

    fn refresh(&mut self, rows: &BTreeMap<i32, Process>, excluded: i32) {
        // Retain birth identities across reparenting; a recycled PID never inherits ownership.
        self.0.retain(|pid, old| {
            if let Some(current) = rows.get(pid).filter(|p| p.birth == old.birth) {
                *old = current.clone();
                true
            } else {
                false
            }
        });
        for row in self.0.values().filter(|p| p.pid == p.group) {
            self.1.insert(row.pid, row.birth);
        }
        // A kernel process group outlives its leader while its descendants are alive.
        // Follow that group too, so a short shell exiting between samples cannot orphan work.
        // Never inherit the enclosing terminal's group or adopt a new/reused group leader.
        self.1.retain(|group, birth| {
            rows.values().any(|p| p.group == *group)
                && rows.get(group).is_none_or(|leader| leader.birth == *birth)
        });
        loop {
            let mut added = false;
            for row in rows.values() {
                if row.pid == excluded || self.0.contains_key(&row.pid) {
                    continue;
                }
                if self.0.contains_key(&row.parent) || self.1.contains_key(&row.group) {
                    self.0.insert(row.pid, row.clone());
                    if row.pid == row.group {
                        self.1.insert(row.pid, row.birth);
                    }
                    added = true;
                }
            }
            if !added {
                break;
            }
        }
    }

    fn signal(&self, signal: i32) {
        for row in self.0.values() {
            // The emergency fallback runs in the owner when its watcher dies.
            // Stopping/killing ourselves first would strand the remaining children.
            if row.pid == std::process::id() as i32 {
                continue;
            }
            if native::process(row.pid).is_ok_and(|now| now.birth == row.birth) {
                // Never kill a whole inherited terminal group or signal a reused PID.
                unsafe {
                    libc::kill(row.pid, signal);
                }
            }
        }
    }

    fn stop(&mut self, excluded: i32) {
        // Freeze before killing so a live launcher cannot keep replacing children during cleanup.
        for _ in 0..3 {
            if let Ok(rows) = native::processes() {
                self.refresh(&rows, excluded);
            }
            self.signal(libc::SIGSTOP);
            std::thread::sleep(Duration::from_millis(20));
        }
        self.signal(libc::SIGKILL);
        if self.0.contains_key(&(std::process::id() as i32)) {
            unsafe {
                libc::kill(std::process::id() as i32, libc::SIGKILL);
            }
        }
    }
}

#[derive(Debug)]
struct Resources {
    total: u64,
    available: u64,
    pressure: u32,
    swap: u64,
    disk: u64,
}

impl Resources {
    fn violation(&self, footprint: u64) -> Option<&'static str> {
        if self.disk < DISK_FLOOR {
            Some("disk floor")
        } else if self.pressure != 1 {
            Some("memory pressure")
        } else if self.available < HEADROOM {
            Some("host memory headroom")
        } else if self.swap > HEADROOM {
            Some("swap ceiling")
        } else if footprint > self.total.saturating_sub(HEADROOM) {
            Some("process tree footprint")
        } else {
            None
        }
    }
}

pub struct Guard {
    control: Option<ChildStdin>,
    complete: Arc<AtomicBool>,
}

impl Drop for Guard {
    fn drop(&mut self) {
        self.complete.store(true, Ordering::SeqCst);
        self.control.take(); // EOF requests cleanup; the watcher reaps independently.
    }
}

/// Called only by router-resource-run before launching its explicit command.
pub fn start() -> io::Result<Option<Guard>> {
    if !enabled() {
        return Ok(None);
    } // No probes, helper, thread or timers when disabled.
    let owner = native::process(std::process::id() as i32)?;
    let root = owner.clone();
    let parent = native::process(root.parent)?;
    let mut command = Command::new(std::env::current_exe()?);
    command.args([
        "__resource-watch",
        &owner.pid.to_string(),
        &owner.birth.to_string(),
        &root.pid.to_string(),
        &root.birth.to_string(),
        &parent.pid.to_string(),
        &parent.birth.to_string(),
    ]);
    command
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::inherit());
    use std::os::unix::process::CommandExt;
    unsafe {
        command.pre_exec(|| {
            if libc::setsid() < 0 {
                return Err(io::Error::last_os_error());
            }
            Ok(())
        });
    }
    let mut child = command.spawn()?;
    let mut ready = child.stdout.take().unwrap();
    use std::os::fd::AsRawFd;
    let mut poll = libc::pollfd {
        fd: ready.as_raw_fd(),
        events: libc::POLLIN,
        revents: 0,
    };
    let mut receipt = [0];
    if unsafe { libc::poll(&mut poll, 1, 5000) } <= 0
        || ready.read_exact(&mut receipt).is_err()
        || receipt != *b"R"
    {
        let _ = child.kill();
        let _ = child.wait();
        return Err(io::Error::other(
            "resource watchdog could not establish safe telemetry",
        ));
    }
    let control = child.stdin.take();
    let complete = Arc::new(AtomicBool::new(false));
    let finished = complete.clone();
    std::thread::spawn(move || {
        let _ = child.wait();
        if !finished.load(Ordering::SeqCst) {
            let _ = writeln!(
                io::stderr(),
                "AGENT_RESOURCES_STOPPED: resource watchdog exited unexpectedly"
            );
            let mut tree = Tree::new(owner);
            tree.stop(-1);
            std::process::exit(75);
        }
    });
    Ok(Some(Guard { control, complete }))
}

/// Dispatch before constructing Tokio: the watchdog has no async runtime or config reads.
pub fn internal_entry(args: &[String]) -> Option<io::Result<()>> {
    if args.get(1).map(String::as_str) != Some("__resource-watch") {
        return None;
    }
    if !enabled() {
        return Some(Ok(()));
    }
    Some((|| {
        if args.len() != 8 {
            return Err(io::Error::other("invalid watchdog arguments"));
        }
        let number = |at: usize| {
            args[at]
                .parse::<u64>()
                .map_err(|_| io::Error::other("invalid process identity"))
        };
        watch(
            number(2)? as i32,
            number(3)?,
            number(4)? as i32,
            number(5)?,
            number(6)? as i32,
            number(7)?,
        )
    })())
}

fn watch(
    owner_pid: i32,
    owner_birth: u64,
    root_pid: i32,
    root_birth: u64,
    parent_pid: i32,
    parent_birth: u64,
) -> io::Result<()> {
    let owner = native::process(owner_pid)?;
    let root = native::process(root_pid)?;
    if owner.birth != owner_birth
        || root.birth != root_birth
        || !native::process(parent_pid).is_ok_and(|p| p.birth == parent_birth)
    {
        return Err(io::Error::other("watchdog owner changed"));
    }
    let cwd = std::env::current_dir()?;
    // Both startup/swap and working filesystems must retain space, even with work on another volume.
    let disks = [cwd, PathBuf::from("/"), std::env::temp_dir()];
    watch_with(
        owner,
        root,
        (parent_pid, parent_birth),
        std::process::id() as i32,
        &mut NativeWatch { disks },
    )
}

// Keep OS sampling and effects at one boundary. Tests supply readings directly;
// installed runners have no environment variable or command for fake telemetry.
trait WatchIo {
    fn resources(&mut self) -> io::Result<Resources>;
    fn processes(&mut self) -> io::Result<BTreeMap<i32, Process>>;
    fn footprint(&mut self, process: &Process) -> io::Result<u64>;
    fn control_closed(&mut self) -> io::Result<bool>;
    fn ready(&mut self) -> io::Result<()>;
    fn pause(&mut self);
    fn stop(&mut self, tree: &mut Tree, excluded: i32);
}

struct NativeWatch {
    disks: [PathBuf; 3],
}

impl WatchIo for NativeWatch {
    fn resources(&mut self) -> io::Result<Resources> {
        native::resources(&self.disks)
    }
    fn processes(&mut self) -> io::Result<BTreeMap<i32, Process>> {
        native::processes()
    }
    fn footprint(&mut self, process: &Process) -> io::Result<u64> {
        native::footprint(process)
    }
    fn control_closed(&mut self) -> io::Result<bool> {
        let mut control = libc::pollfd {
            fd: 0,
            events: libc::POLLIN,
            revents: 0,
        };
        if unsafe { libc::poll(&mut control, 1, 0) } < 0 {
            return Err(io::Error::last_os_error());
        }
        Ok(control.revents != 0)
    }
    fn ready(&mut self) -> io::Result<()> {
        io::stdout().write_all(b"R")?;
        io::stdout().flush()
    }
    fn pause(&mut self) {
        std::thread::sleep(INTERVAL);
    }
    fn stop(&mut self, tree: &mut Tree, excluded: i32) {
        tree.stop(excluded);
    }
}

fn watch_with(
    owner: Process,
    root: Process,
    parent: (i32, u64),
    me: i32,
    host: &mut impl WatchIo,
) -> io::Result<()> {
    let owner_pid = owner.pid;
    let owner_birth = owner.birth;
    let root_pid = root.pid;
    let root_birth = root.birth;
    let (parent_pid, parent_birth) = parent;
    let initial = host.resources()?;
    if initial.violation(0).is_some() {
        let _ = writeln!(io::stderr(), "AGENT_RESOURCES_BLOCKED: {initial:?}");
        return Err(io::Error::other("resource floor already exceeded"));
    }
    let mut entire = Tree::new(root);
    let mut owned = Tree::new(owner);
    host.ready()?;
    loop {
        let sample = (|| -> io::Result<_> {
            let rows = host.processes()?;
            entire.refresh(&rows, me);
            owned.refresh(&rows, me);
            if entire.recursive_launch() {
                let _ = writeln!(
                    io::stderr(),
                    "AGENT_RESOURCES_STOPPED: recursive launch exceeded 32 ancestor processes"
                );
                return Ok(Some((true, "recursive process launch")));
            }
            let owner_alive = rows.get(&owner_pid).is_some_and(|p| p.birth == owner_birth);
            let root_alive = rows.get(&root_pid).is_some_and(|p| p.birth == root_birth);
            let parent_alive = rows
                .get(&parent_pid)
                .is_some_and(|p| p.birth == parent_birth);
            let control_closed = host.control_closed()?;
            if !root_alive || !parent_alive {
                return Ok(Some((true, "root exited")));
            }
            if !owner_alive || control_closed {
                return Ok(Some((false, "router exited")));
            }
            let resources = host.resources()?;
            let footprint = entire.0.values().try_fold(0u64, |used, p| {
                host.footprint(p).map(|n| used.saturating_add(n))
            })?;
            if let Some(reason) = resources.violation(footprint) {
                let _ = writeln!(
                    io::stderr(),
                    "AGENT_RESOURCES_STOPPED: {reason}; footprint={footprint}; {resources:?}"
                );
                return Ok(Some((true, reason)));
            }
            Ok(None)
        })();
        match sample {
            Ok(None) => host.pause(),
            Ok(Some((all, _))) => {
                if all {
                    host.stop(&mut entire, me);
                } else {
                    owned.0.remove(&owner_pid);
                    host.stop(&mut owned, owner_pid);
                }
                return Ok(());
            }
            Err(error) => {
                let _ = writeln!(
                    io::stderr(),
                    "AGENT_RESOURCES_STOPPED: resource telemetry unavailable: {error}"
                );
                host.stop(&mut entire, me);
                return Err(error);
            }
        }
    }
}

mod native {
    use super::*;
    use std::ffi::CString;
    use std::os::unix::ffi::OsStrExt;

    #[allow(clippy::unnecessary_cast)] // libc's statvfs field widths differ across Unix targets.
    fn disk(path: &Path) -> io::Result<u64> {
        let path = CString::new(path.as_os_str().as_bytes())?;
        let mut info = std::mem::MaybeUninit::<libc::statvfs>::uninit();
        if unsafe { libc::statvfs(path.as_ptr(), info.as_mut_ptr()) } != 0 {
            return Err(io::Error::last_os_error());
        }
        let info = unsafe { info.assume_init() };
        Ok((info.f_bavail as u64).saturating_mul(info.f_frsize as u64))
    }

    // Only these fixed kernel metric names may appear in telemetry diagnostics.
    #[cfg(any(target_os = "macos", test))]
    #[derive(Clone, Copy)]
    enum MemoryMetric {
        Total,
        AvailablePercent,
        Swap,
        Pressure,
    }

    #[cfg(any(target_os = "macos", test))]
    impl MemoryMetric {
        fn name(self) -> &'static std::ffi::CStr {
            match self {
                Self::Total => c"hw.memsize",
                Self::AvailablePercent => c"kern.memorystatus_level",
                Self::Swap => c"vm.swapusage",
                Self::Pressure => c"kern.memorystatus_vm_pressure_level",
            }
        }
    }

    #[cfg(any(target_os = "macos", test))]
    // SAFETY: on success with the expected size, read must initialize a valid T.
    unsafe fn read_memory_metric<T: Copy>(
        metric: MemoryMetric,
        read: impl FnOnce(&mut std::mem::MaybeUninit<T>, &mut usize) -> io::Result<()>,
    ) -> io::Result<T> {
        let mut value = std::mem::MaybeUninit::<T>::uninit();
        let expected = std::mem::size_of::<T>();
        let mut size = expected;
        read(&mut value, &mut size).map_err(|error| {
            io::Error::new(
                error.kind(),
                format!(
                    "host memory telemetry unavailable: metric={} category=syscall errno={:?}",
                    metric.name().to_string_lossy(),
                    error.raw_os_error()
                ),
            )
        })?;
        if size != expected {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "host memory telemetry unavailable: metric={} category=size-mismatch expected={expected} actual={size}",
                    metric.name().to_string_lossy()
                ),
            ));
        }
        Ok(unsafe { value.assume_init() })
    }

    #[cfg(target_os = "macos")]
    fn sysctl<T: Copy>(metric: MemoryMetric) -> io::Result<T> {
        // The callers pair each allowlisted metric with its kernel ABI type.
        unsafe {
            read_memory_metric(metric, |value: &mut std::mem::MaybeUninit<T>, size| {
                if libc::sysctlbyname(
                    metric.name().as_ptr(),
                    value.as_mut_ptr().cast(),
                    size,
                    std::ptr::null_mut(),
                    0,
                ) != 0
                {
                    // Capture errno immediately, before formatting or any further OS call.
                    Err(io::Error::last_os_error())
                } else {
                    Ok(())
                }
            })
        }
    }

    #[cfg(target_os = "macos")]
    pub fn process(pid: i32) -> io::Result<Process> {
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
            return Err(io::Error::last_os_error());
        }
        let p = unsafe { info.assume_init() };
        if p.pbi_status == 5 {
            return Err(io::Error::from(io::ErrorKind::NotFound));
        }
        Ok(Process {
            pid,
            parent: p.pbi_ppid as i32,
            group: p.pbi_pgid as i32,
            birth: p.pbi_start_tvsec * 1_000_000 + p.pbi_start_tvusec,
        })
    }

    #[cfg(target_os = "macos")]
    pub fn processes() -> io::Result<BTreeMap<i32, Process>> {
        let count = unsafe { libc::proc_listallpids(std::ptr::null_mut(), 0) };
        if count <= 0 {
            return Err(io::Error::other("process list unavailable"));
        }
        let mut pids = vec![0i32; count as usize + 128];
        loop {
            let n = unsafe {
                libc::proc_listallpids(pids.as_mut_ptr().cast(), (pids.len() * 4) as i32)
            };
            if n <= 0 {
                return Err(io::Error::other("process list unavailable"));
            }
            if (n as usize) < pids.len() {
                pids.truncate(n as usize);
                break;
            }
            pids.resize(pids.len() * 2, 0);
        }
        Ok(pids
            .into_iter()
            .filter_map(|pid| process(pid).ok().map(|p| (pid, p)))
            .collect())
    }

    #[cfg(target_os = "macos")]
    pub fn footprint(p: &Process) -> io::Result<u64> {
        // rusage_info_v0 from sys/resource.h: UUID then 10 u64 counters.
        let mut usage = [0u64; 12];
        if unsafe { libc::proc_pid_rusage(p.pid, 0, usage.as_mut_ptr().cast()) } != 0 {
            return if process(p.pid).is_ok_and(|now| now.birth == p.birth) {
                Err(io::Error::other("live process footprint unavailable"))
            } else {
                Ok(0)
            };
        }
        Ok(usage[9].max(usage[8])) // phys_footprint includes compressed pages; RSS is only a lower bound.
    }

    #[cfg(target_os = "macos")]
    pub fn resources(disks: &[PathBuf]) -> io::Result<Resources> {
        let total = sysctl::<u64>(MemoryMetric::Total)?;
        let free = sysctl::<u32>(MemoryMetric::AvailablePercent)?;
        if free > 100 {
            return Err(io::Error::other("invalid available memory percentage"));
        }
        let swap = sysctl::<libc::xsw_usage>(MemoryMetric::Swap)?;
        Ok(Resources {
            total,
            available: total / 100 * u64::from(free),
            pressure: sysctl(MemoryMetric::Pressure)?,
            swap: swap.xsu_used,
            disk: disks
                .iter()
                .map(|p| disk(p))
                .collect::<io::Result<Vec<_>>>()?
                .into_iter()
                .min()
                .unwrap_or(0),
        })
    }

    #[cfg(target_os = "linux")]
    fn stat(pid: i32) -> io::Result<Vec<String>> {
        let text = std::fs::read_to_string(format!("/proc/{pid}/stat"))?;
        let (_, fields) = text
            .rsplit_once(')')
            .ok_or_else(|| io::Error::other("invalid process stat"))?;
        Ok(fields.split_whitespace().map(str::to_owned).collect())
    }

    #[cfg(target_os = "linux")]
    pub fn process(pid: i32) -> io::Result<Process> {
        let f = stat(pid)?;
        let number = |at: usize| {
            f.get(at)
                .and_then(|s| s.parse::<u64>().ok())
                .ok_or_else(|| io::Error::other("invalid process stat"))
        };
        if f.first().map(String::as_str) == Some("Z") {
            return Err(io::ErrorKind::NotFound.into());
        }
        Ok(Process {
            pid,
            parent: number(1)? as i32,
            group: number(2)? as i32,
            birth: number(19)?,
        })
    }

    #[cfg(target_os = "linux")]
    pub fn processes() -> io::Result<BTreeMap<i32, Process>> {
        let mut rows = BTreeMap::new();
        for entry in std::fs::read_dir("/proc")? {
            let entry = entry?;
            if let Some(pid) = entry.file_name().to_str().and_then(|s| s.parse().ok()) {
                if let Ok(p) = process(pid) {
                    rows.insert(pid, p);
                }
            }
        }
        Ok(rows)
    }

    #[cfg(target_os = "linux")]
    fn kb(text: &str, key: &str) -> io::Result<u64> {
        text.lines()
            .find_map(|line| {
                line.strip_prefix(key)
                    .and_then(|s| s.split_whitespace().next())
                    .and_then(|s| s.parse::<u64>().ok())
            })
            .map(|n| n * 1024)
            .ok_or_else(|| io::Error::other("memory counter unavailable"))
    }

    #[cfg(target_os = "linux")]
    pub fn footprint(p: &Process) -> io::Result<u64> {
        match std::fs::read_to_string(format!("/proc/{}/status", p.pid)) {
            Ok(text) => match (kb(&text, "VmRSS:"), kb(&text, "VmSwap:")) {
                (Ok(rss), Ok(swap)) => Ok(rss.saturating_add(swap)),
                _ if !process(p.pid).is_ok_and(|now| now.birth == p.birth) => Ok(0),
                _ => Err(io::Error::other("live process memory counters unavailable")),
            },
            Err(_) if !process(p.pid).is_ok_and(|now| now.birth == p.birth) => Ok(0),
            Err(e) => Err(e),
        }
    }

    #[cfg(target_os = "linux")]
    pub fn resources(disks: &[PathBuf]) -> io::Result<Resources> {
        let mem = std::fs::read_to_string("/proc/meminfo")?;
        Ok(Resources {
            total: kb(&mem, "MemTotal:")?,
            available: kb(&mem, "MemAvailable:")?,
            pressure: 1,
            swap: kb(&mem, "SwapTotal:")?.saturating_sub(kb(&mem, "SwapFree:")?),
            disk: disks
                .iter()
                .map(|p| disk(p))
                .collect::<io::Result<Vec<_>>>()?
                .into_iter()
                .min()
                .unwrap_or(0),
        })
    }
    #[cfg(test)]
    mod memory_tests {
        use super::*;

        #[test]
        fn denied_memory_metric_reports_which_probe_failed_and_why() {
            for metric in [
                MemoryMetric::Total,
                MemoryMetric::AvailablePercent,
                MemoryMetric::Swap,
                MemoryMetric::Pressure,
            ] {
                // This failure path never exposes the uninitialized value.
                let error = unsafe {
                    read_memory_metric::<u64>(metric, |_, size| {
                        // A failed call may also change the length: errno takes precedence.
                        *size = 0;
                        Err(io::Error::from_raw_os_error(libc::EPERM))
                    })
                }
                .unwrap_err();
                assert_eq!(error.kind(), io::ErrorKind::PermissionDenied);
                let receipt = error.to_string();
                // These fields are the operator's diagnostic contract, not prose wording.
                assert!(receipt.contains(&format!("metric={}", metric.name().to_string_lossy())));
                assert!(receipt.contains("category=syscall"));
                assert!(receipt.contains(&format!("errno=Some({})", libc::EPERM)));
            }
        }

        #[test]
        fn unexpected_memory_metric_size_is_rejected_before_reading_value() {
            for actual in [0, 4, 16] {
                // A mismatched length must fail before reading the value.
                let error = unsafe {
                    read_memory_metric::<u64>(MemoryMetric::Total, |_, size| {
                        *size = actual;
                        Ok(())
                    })
                }
                .unwrap_err();
                assert_eq!(error.kind(), io::ErrorKind::InvalidData);
                let receipt = error.to_string();
                assert!(receipt.contains("metric=hw.memsize"));
                assert!(receipt.contains("category=size-mismatch"));
                assert!(receipt.contains("expected=8"));
                assert!(receipt.contains(&format!("actual={actual}")));
                assert!(!receipt.contains("errno="));
            }
        }

        #[test]
        fn complete_memory_metric_preserves_the_sample() {
            // Initialize the complete u64 as a successful native read would.
            let value = unsafe {
                read_memory_metric::<u64>(MemoryMetric::Total, |value, _| {
                    value.write(64 * GIB);
                    Ok(())
                })
            }
            .unwrap();
            assert_eq!(value, 64 * GIB);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn p(pid: i32, parent: i32, birth: u64) -> Process {
        Process {
            pid,
            parent,
            group: pid,
            birth,
        }
    }

    #[test]
    fn retains_detached_children_but_never_reused_pids_or_siblings() {
        let mut tree = Tree::new(p(10, 1, 100));
        tree.refresh(
            &BTreeMap::from([
                (10, p(10, 1, 100)),
                (11, p(11, 10, 101)),
                (12, p(12, 11, 102)),
                (13, p(13, 1, 103)),
                (14, p(14, 10, 104)),
            ]),
            14,
        );
        assert_eq!(tree.0.keys().copied().collect::<Vec<_>>(), [10, 11, 12]);
        tree.refresh(
            &BTreeMap::from([
                (11, p(11, 1, 201)),
                (12, p(12, 1, 102)),
                (13, p(13, 11, 202)),
                (15, p(15, 12, 105)),
            ]),
            14,
        );
        assert_eq!(tree.0.keys().copied().collect::<Vec<_>>(), [12, 15]);
    }

    #[test]
    fn group_cleanup_outlives_shell_but_does_not_take_a_reused_group() {
        let mut tree = Tree::new(p(10, 1, 100));
        let mut orphan = p(12, 1, 102);
        orphan.group = 10;
        tree.refresh(&BTreeMap::from([(12, orphan)]), 14);
        assert!(tree.0.contains_key(&12));
        tree.refresh(
            &BTreeMap::from([(10, p(10, 1, 300)), (15, p(15, 10, 301))]),
            14,
        );
        assert!(tree.0.is_empty());
    }

    #[test]
    fn recursive_launch_is_bounded_without_limiting_parallel_workers() {
        let mut tree = Tree::new(p(10, 1, 100));
        let parallel = (11..1011)
            .map(|pid| (pid, p(pid, 10, pid as u64)))
            .chain([(10, p(10, 1, 100))])
            .collect();
        tree.refresh(&parallel, -1);
        assert!(!tree.recursive_launch());
        let recursive = (11..50)
            .map(|pid| (pid, p(pid, pid - 1, pid as u64)))
            .chain([(10, p(10, 1, 100))])
            .collect();
        tree.refresh(&recursive, -1);
        assert!(tree.recursive_launch());
    }

    #[test]
    fn resource_floors_apply_during_work_including_compression_and_swap() {
        let mut r = Resources {
            total: 64 * GIB,
            available: 32 * GIB,
            pressure: 1,
            swap: 0,
            disk: 30 * GIB,
        };
        assert_eq!(r.violation(20 * GIB), None);
        assert_eq!(r.violation(60 * GIB), Some("process tree footprint"));
        r.disk = 9 * GIB;
        assert_eq!(r.violation(0), Some("disk floor"));
        r.disk = 30 * GIB;
        r.swap = 9 * GIB;
        assert_eq!(r.violation(0), Some("swap ceiling"));
        r.swap = 0;
        r.pressure = 2;
        assert_eq!(r.violation(0), Some("memory pressure"));
        r.pressure = 1;
        r.available = 7 * GIB;
        assert_eq!(r.violation(0), Some("host memory headroom"));
    }

    #[test]
    #[ignore = "host capability smoke: requires native process/memory/disk telemetry; run explicitly for activation"]
    fn host_native_sampling_is_available_and_footprint_nonzero() {
        let me = native::process(std::process::id() as i32).unwrap();
        assert_eq!(native::processes().unwrap().get(&me.pid), Some(&me));
        assert!(native::footprint(&me).unwrap() > 0);
        let r = native::resources(&[std::env::current_dir().unwrap()]).unwrap();
        assert!(r.total > 0 && r.disk > 0);
    }

    // This adapter retains every native read and cleanup effect. The successful
    // sample's pause boundary replaces a timing guess with an observed event.
    struct NativeOwner {
        native: NativeWatch,
        owner: std::process::Child,
        // Held outside the killed tree until every cleanup assertion finishes.
        _fixture_input: ChildStdin,
        descendants: Vec<Process>,
        owner_killed: bool,
    }

    impl Drop for NativeOwner {
        fn drop(&mut self) {
            for process in &self.descendants {
                if native::process(process.pid).is_ok_and(|now| now.birth == process.birth) {
                    unsafe {
                        libc::kill(process.pid, libc::SIGKILL);
                    }
                }
            }
            let _ = self.owner.kill();
            let _ = self.owner.wait();
        }
    }

    impl WatchIo for NativeOwner {
        fn resources(&mut self) -> io::Result<Resources> {
            self.native.resources()
        }
        fn processes(&mut self) -> io::Result<BTreeMap<i32, Process>> {
            let rows = self.native.processes()?;
            if !self.owner_killed {
                for process in &self.descendants {
                    assert_eq!(
                        rows.get(&process.pid),
                        Some(process),
                        "host sample omitted a ready descendant"
                    );
                }
            }
            Ok(rows)
        }
        fn footprint(&mut self, process: &Process) -> io::Result<u64> {
            self.native.footprint(process)
        }
        fn control_closed(&mut self) -> io::Result<bool> {
            // The fixture keeps its control channel open; owner death is the event.
            Ok(false)
        }
        fn ready(&mut self) -> io::Result<()> {
            Ok(())
        }
        fn pause(&mut self) {
            assert!(!self.owner_killed, "watchdog missed the exited owner");
            self.owner.kill().unwrap();
            self.owner.wait().unwrap();
            self.owner_killed = true;
        }
        fn stop(&mut self, tree: &mut Tree, excluded: i32) {
            self.native.stop(tree, excluded);
        }
    }

    #[test]
    #[ignore = "host capability smoke: requires native telemetry, resource headroom, and process signalling; run explicitly for activation"]
    fn host_sampled_detached_descendants_are_cleaned_after_owner_death() {
        use std::io::BufRead;
        use std::os::unix::process::CommandExt;
        let mut owner = Command::new("/bin/sh")
            .args(["-c", "exec 3<&0; \"$1\" --ignored --exact resource_guard::tests::detached_fixture --nocapture <&3 3<&- & wait", "fixture"])
            .arg(std::env::current_exe().unwrap())
            .env("RESOURCE_DETACHED_FIXTURE", "1")
            .process_group(0)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .spawn()
            .unwrap();
        let fixture_input = owner.stdin.take().unwrap();
        let mut host = NativeOwner {
            native: NativeWatch {
                disks: [
                    std::env::current_dir().unwrap(),
                    PathBuf::from("/"),
                    std::env::temp_dir(),
                ],
            },
            owner,
            _fixture_input: fixture_input,
            descendants: vec![],
            owner_killed: false,
        };
        let stdout = host.owner.stdout.take().unwrap();
        let mut reader = io::BufReader::new(stdout);
        let mut line = String::new();
        loop {
            line.clear();
            assert!(
                reader.read_line(&mut line).unwrap() > 0,
                "fixture exited before its ready receipt"
            );
            if let Some((_, pids)) = line.split_once("DETACHED_READY ") {
                for pid in pids.split_whitespace() {
                    host.descendants
                        .push(native::process(pid.parse().unwrap()).unwrap());
                }
                break;
            }
        }
        assert_eq!(host.descendants.len(), 2);
        assert_ne!(host.descendants[0].group, host.owner.id() as i32);
        assert_ne!(host.descendants[0].group, host.descendants[1].group);
        let root = native::process(host.owner.id() as i32).unwrap();
        let parent = native::process(std::process::id() as i32).unwrap();
        watch_with(
            root.clone(),
            root,
            (parent.pid, parent.birth),
            parent.pid,
            &mut host,
        )
        .unwrap();
        assert!(host.owner_killed);
        // SIGKILL completion is observed, never inferred from a fixed sleep. The
        // enclosing smoke job supplies its hang backstop.
        for process in &host.descendants {
            loop {
                match native::process(process.pid) {
                    Ok(now) if now.birth != process.birth => break,
                    Ok(_) => std::thread::sleep(Duration::from_millis(20)),
                    Err(error)
                        if error.kind() == io::ErrorKind::NotFound
                            || error.raw_os_error() == Some(libc::ESRCH) =>
                    {
                        break;
                    }
                    Err(error) => panic!("cleanup observation unavailable: {error}"),
                }
            }
        }
    }

    #[test]
    #[ignore = "subprocess fixture, invoked only by the detached-descendant host smoke"]
    fn detached_fixture() {
        assert_eq!(
            std::env::var("RESOURCE_DETACHED_FIXTURE").as_deref(),
            Ok("1")
        );
        use std::os::unix::process::CommandExt;
        assert!(unsafe { libc::setsid() } > 0);
        let mut child = Command::new("/bin/cat")
            .stdin(Stdio::inherit())
            .process_group(0)
            .spawn()
            .unwrap();
        // The controller retains the pipe writer outside this entire tree.
        // Killing this fixture cannot make cat exit through EOF.
        println!("DETACHED_READY {} {}", std::process::id(), child.id());
        io::stdout().flush().unwrap();
        let _ = child.wait();
    }

    fn healthy() -> Resources {
        Resources {
            total: 64 * GIB,
            available: 32 * GIB,
            pressure: 1,
            swap: 0,
            disk: 30 * GIB,
        }
    }

    #[derive(Clone, Copy)]
    enum Unavailable {
        Resources,
        Processes,
        Footprint,
        Control,
    }

    struct Fixture {
        readings: std::collections::VecDeque<io::Result<Resources>>,
        unavailable: Option<Unavailable>,
        rounds: usize,
        ready: bool,
        pauses: usize,
        stopped: Vec<(Vec<i32>, i32)>,
        parent_exited: bool,
    }

    impl Fixture {
        fn new() -> Self {
            Self {
                readings: [Ok(healthy()), Ok(healthy())].into(),
                unavailable: None,
                rounds: 0,
                ready: false,
                pauses: 0,
                stopped: vec![],
                parent_exited: false,
            }
        }

        fn run(&mut self) -> io::Result<()> {
            watch_with(p(10, 1, 100), p(10, 1, 100), (1, 1), 99, self)
        }
    }

    impl WatchIo for Fixture {
        fn resources(&mut self) -> io::Result<Resources> {
            if self.rounds > 0 && matches!(self.unavailable, Some(Unavailable::Resources)) {
                return Err(io::ErrorKind::PermissionDenied.into());
            }
            self.readings
                .pop_front()
                .expect("unexpected resource sample")
        }

        fn processes(&mut self) -> io::Result<BTreeMap<i32, Process>> {
            self.rounds += 1;
            // A changed loop must fail deterministically rather than hang the test.
            assert!(self.rounds <= 2, "watchdog ignored the terminal event");
            if matches!(self.unavailable, Some(Unavailable::Processes)) {
                return Err(io::ErrorKind::PermissionDenied.into());
            }
            let mut rows = BTreeMap::from([
                (1, p(1, 0, 1)),
                (10, p(10, 1, 100)),
                (11, p(11, 10, 101)),
                (20, p(20, 1, 200)),
                (99, p(99, 10, 999)),
            ]);
            if self.parent_exited {
                rows.remove(&1);
            }
            Ok(rows)
        }

        fn footprint(&mut self, _: &Process) -> io::Result<u64> {
            if matches!(self.unavailable, Some(Unavailable::Footprint)) {
                return Err(io::ErrorKind::PermissionDenied.into());
            }
            Ok(GIB)
        }

        fn control_closed(&mut self) -> io::Result<bool> {
            if matches!(self.unavailable, Some(Unavailable::Control)) {
                return Err(io::ErrorKind::PermissionDenied.into());
            }
            Ok(self.rounds == 2)
        }

        fn ready(&mut self) -> io::Result<()> {
            self.ready = true;
            Ok(())
        }

        fn pause(&mut self) {
            self.pauses += 1;
        }

        fn stop(&mut self, tree: &mut Tree, excluded: i32) {
            self.stopped
                .push((tree.0.keys().copied().collect(), excluded));
        }
    }

    #[test]
    fn unavailable_startup_telemetry_never_releases_launch() {
        let mut host = Fixture::new();
        host.readings = [Err(io::ErrorKind::PermissionDenied.into())].into();
        assert_eq!(
            host.run().unwrap_err().kind(),
            io::ErrorKind::PermissionDenied
        );
        assert!(!host.ready);
        assert!(host.stopped.is_empty());
    }

    #[test]
    fn resource_pressure_prevents_launch_and_stops_an_active_tree() {
        let mut pressure = healthy();
        pressure.pressure = 2;
        let mut host = Fixture::new();
        host.readings = [Ok(pressure)].into();
        assert!(host.run().is_err());
        assert!(!host.ready);
        assert!(host.stopped.is_empty());

        let mut pressure = healthy();
        pressure.pressure = 2;
        let mut host = Fixture::new();
        host.readings = [Ok(healthy()), Ok(pressure)].into();
        host.run().unwrap();
        assert!(host.ready);
        assert_eq!(host.stopped, [(vec![10, 11], 99)]);
        assert_eq!(host.pauses, 0);
    }

    #[test]
    fn lost_telemetry_stops_the_owned_tree_without_touching_siblings() {
        for unavailable in [
            Unavailable::Resources,
            Unavailable::Processes,
            Unavailable::Footprint,
            Unavailable::Control,
        ] {
            let mut host = Fixture::new();
            host.unavailable = Some(unavailable);
            assert!(host.run().is_err());
            assert!(host.ready);
            let owned = if matches!(unavailable, Unavailable::Processes) {
                vec![10]
            } else {
                vec![10, 11]
            };
            assert_eq!(host.stopped, [(owned, 99)]);
            assert_eq!(host.pauses, 0);
        }
    }

    #[test]
    fn healthy_work_continues_until_control_closes_then_only_children_stop() {
        let mut host = Fixture::new();
        host.run().unwrap();
        assert!(host.ready);
        assert_eq!(host.pauses, 1);
        assert_eq!(host.stopped, [(vec![11], 10)]);
    }

    #[test]
    fn losing_the_launcher_stops_the_whole_owned_tree() {
        let mut host = Fixture::new();
        host.parent_exited = true;
        host.run().unwrap();
        assert!(host.ready);
        assert_eq!(host.stopped, [(vec![10, 11], 99)]);
        assert_eq!(host.pauses, 0);
    }
}
