//! Repeatable client-observed GET/SET benchmark. No results are bundled into
//! the binary; each CSV row records the measured run and its environment.

use std::fs::{self, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::{Arc, Barrier};
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use distributedb::{Client, Status};

#[derive(Clone)]
struct Config {
    addr: String,
    clients: usize,
    operations: usize,
    warmup: usize,
    read_bps: usize,
    keys: usize,
    value_bytes: usize,
    seed: u64,
    replicas: usize,
    durability: String,
    data_dir: Option<PathBuf>,
    server_pid: Option<u32>,
    output: Option<PathBuf>,
    filesystem: String,
    storage_medium: String,
    topology: String,
}

impl Config {
    fn parse(args: &[String]) -> Result<Self, String> {
        let mut config = Self {
            addr: "127.0.0.1:5555".into(),
            clients: 1,
            operations: 10_000,
            warmup: 1_000,
            read_bps: 5_000,
            keys: 1_000,
            value_bytes: 128,
            seed: 1,
            replicas: 0,
            durability: "fsync".into(),
            data_dir: None,
            server_pid: None,
            output: None,
            filesystem: "unspecified".into(),
            storage_medium: "unspecified".into(),
            topology: "unspecified".into(),
        };
        let mut pairs = args.iter();
        while let Some(flag) = pairs.next() {
            let value = pairs
                .next()
                .ok_or_else(|| format!("missing value for {flag}"))?;
            match flag.as_str() {
                "--addr" => config.addr = value.clone(),
                "--clients" => config.clients = parse_num(value, flag)?,
                "--operations" => config.operations = parse_num(value, flag)?,
                "--warmup" => config.warmup = parse_num(value, flag)?,
                "--keys" => config.keys = parse_num(value, flag)?,
                "--value-bytes" => config.value_bytes = parse_num(value, flag)?,
                "--seed" => config.seed = parse_num(value, flag)?,
                "--replicas" => config.replicas = parse_num(value, flag)?,
                "--server-pid" => config.server_pid = Some(parse_num(value, flag)?),
                "--data-dir" => config.data_dir = Some(PathBuf::from(value)),
                "--output" => config.output = Some(PathBuf::from(value)),
                "--filesystem" => config.filesystem = value.clone(),
                "--storage-medium" => config.storage_medium = value.clone(),
                "--topology" => config.topology = value.clone(),
                "--durability" => config.durability = value.clone(),
                "--read-ratio" => {
                    let ratio: f64 = value
                        .parse()
                        .map_err(|_| "--read-ratio must be a decimal in [0,1]".to_string())?;
                    if !ratio.is_finite() || !(0.0..=1.0).contains(&ratio) {
                        return Err("--read-ratio must be in [0,1]".into());
                    }
                    config.read_bps = (ratio * 10_000.0).round() as usize;
                }
                _ => return Err(format!("unknown option {flag}")),
            }
        }
        if !(1..=256).contains(&config.clients)
            || !(1..=10_000_000).contains(&config.operations)
            || config.warmup > 10_000_000
            || !(1..=1_000_000).contains(&config.keys)
            || config.value_bytes > 995_000
            || config.replicas > 16
            || !matches!(config.durability.as_str(), "fsync" | "os")
            || (config.durability == "os" && config.replicas > 0)
        {
            return Err("benchmark option outside supported bounds".into());
        }
        Ok(config)
    }
}

fn parse_num<T: std::str::FromStr>(value: &str, flag: &str) -> Result<T, String> {
    value
        .parse()
        .map_err(|_| format!("invalid value for {flag}"))
}

fn assigned(total: usize, worker: usize, workers: usize) -> usize {
    total / workers + usize::from(worker < total % workers)
}

fn next_random(state: &mut u64) -> u64 {
    *state = state.wrapping_add(0x9e37_79b9_7f4a_7c15);
    let mut value = *state;
    value = (value ^ (value >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    value = (value ^ (value >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    value ^ (value >> 31)
}

fn key(index: usize) -> Vec<u8> {
    format!("k:{index:08}").into_bytes()
}

fn write_ok(status: Status, durability: &str) -> bool {
    matches!(
        (status, durability),
        (Status::Ok, "fsync") | (Status::OkVolatile, "os")
    )
}

#[derive(Default)]
struct WorkerResult {
    latencies_ns: Vec<u64>,
    succeeded: usize,
    failed: usize,
    skipped: usize,
    reads: usize,
    writes: usize,
    read_misses: usize,
}

fn run_operations(
    client: &mut Client,
    config: &Config,
    count: usize,
    measured: bool,
    state: &mut u64,
) -> WorkerResult {
    let mut result = WorkerResult::default();
    if measured {
        result.latencies_ns.reserve(count);
    }
    let value = vec![b'x'; config.value_bytes];
    for index in 0..count {
        let selected = key((next_random(state) as usize) % config.keys);
        let is_read = (next_random(state) % 10_000) < config.read_bps as u64;
        // Build the request payload before timing so latency covers only the
        // client round trip.
        let payload = if is_read { Vec::new() } else { value.clone() };
        let started = Instant::now();
        let outcome = if is_read {
            result.reads += 1;
            match client.get(selected) {
                Ok(Some(_)) => true,
                Ok(None) => {
                    result.read_misses += 1;
                    false
                }
                Err(_) => false,
            }
        } else {
            result.writes += 1;
            matches!(client.set(selected, payload), Ok(status) if write_ok(status, &config.durability))
        };
        if outcome {
            result.succeeded += 1;
            if measured {
                result
                    .latencies_ns
                    .push(started.elapsed().as_nanos().min(u64::MAX as u128) as u64);
            }
        } else {
            result.failed += 1;
            // A network failure may leave the last write's outcome unknown.
            // Stop using this connection and report unattempted operations.
            result.skipped = count - index - 1;
            break;
        }
    }
    result
}

fn wait_replicas(client: &mut Client, count: usize) -> Result<(), String> {
    if count == 0 {
        return Ok(());
    }
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        let stats = client.stats().map_err(|error| error.to_string())?;
        let fields: Vec<(&str, &str)> = stats
            .lines()
            .filter_map(|line| line.split_once('='))
            .collect();
        let connected = fields
            .iter()
            .any(|&(name, value)| name == "replicas_connected" && value == count.to_string());
        let caught_up = fields
            .iter()
            .filter(|&&(name, value)| name.ends_with("_lag") && value == "0")
            .count();
        if connected && caught_up == count {
            return Ok(());
        }
        if Instant::now() >= deadline {
            return Err(format!("replicas did not catch up: {stats}"));
        }
        thread::sleep(Duration::from_millis(50));
    }
}

fn percentile(sorted: &[u64], bps: usize) -> Option<u64> {
    if sorted.is_empty() {
        return None;
    }
    let index = (sorted.len() * bps).div_ceil(10_000).saturating_sub(1);
    Some(sorted[index.min(sorted.len() - 1)])
}

fn command_output(name: &str, args: &[&str]) -> String {
    Command::new(name)
        .args(args)
        .output()
        .ok()
        .filter(|output| output.status.success())
        .map(|output| String::from_utf8_lossy(&output.stdout).trim().to_string())
        .filter(|text| !text.is_empty())
        .unwrap_or_else(|| "unknown".into())
}

fn directory_bytes(root: &Path, wal_only: bool) -> Option<u64> {
    fn visit(path: &Path, wal_only: bool, total: &mut u64) -> std::io::Result<()> {
        for entry in fs::read_dir(path)? {
            let entry = entry?;
            let path = entry.path();
            if path.is_dir() {
                visit(&path, wal_only, total)?;
            } else if !wal_only || path.extension().is_some_and(|ext| ext == "wal") {
                *total += entry.metadata()?.len();
            }
        }
        Ok(())
    }
    let mut total = 0;
    visit(root, wal_only, &mut total).ok()?;
    Some(total)
}

fn process_resources(pid: Option<u32>) -> (Option<u64>, Option<u64>, Option<u64>) {
    let Some(pid) = pid else {
        return (None, None, None);
    };
    let stat = fs::read_to_string(format!("/proc/{pid}/stat")).ok();
    let ticks = stat.and_then(|text| {
        let tail = text.rsplit_once(") ")?.1;
        let fields: Vec<&str> = tail.split_whitespace().collect();
        Some(fields.get(11)?.parse::<u64>().ok()? + fields.get(12)?.parse::<u64>().ok()?)
    });
    let status = fs::read_to_string(format!("/proc/{pid}/status")).ok();
    let peak_kib = status.as_ref().and_then(|text| {
        text.lines()
            .find(|line| line.starts_with("VmHWM:"))?
            .split_whitespace()
            .nth(1)?
            .parse::<u64>()
            .ok()
    });
    let clock_ticks = command_output("getconf", &["CLK_TCK"]).parse::<u64>().ok();
    (ticks, clock_ticks, peak_kib)
}

fn csv_field(value: &str) -> String {
    format!("\"{}\"", value.replace('"', "\"\""))
}

/// Write every key once, spread over one connection per client so the
/// server can group-commit them.
fn prepopulate(config: &Config) -> Result<(), String> {
    let workers: Vec<_> = (0..config.clients)
        .map(|worker| {
            let config = config.clone();
            thread::spawn(move || -> Result<(), String> {
                let mut client =
                    Client::connect(config.addr.as_str()).map_err(|error| error.to_string())?;
                let value = vec![b'x'; config.value_bytes];
                for index in (worker..config.keys).step_by(config.clients) {
                    let status = client
                        .set(key(index), value.clone())
                        .map_err(|error| error.to_string())?;
                    if !write_ok(status, &config.durability) {
                        return Err(format!("prepopulation SET returned {status:?}"));
                    }
                }
                Ok(())
            })
        })
        .collect();
    for worker in workers {
        worker
            .join()
            .map_err(|_| "prepopulation worker panicked".to_string())??;
    }
    Ok(())
}

fn run(config: Config) -> Result<(), String> {
    let mut setup = Client::connect(config.addr.as_str()).map_err(|error| error.to_string())?;
    prepopulate(&config)?;
    wait_replicas(&mut setup, config.replicas)?;
    let (cpu_before, clock_ticks, _) = process_resources(config.server_pid);
    let barrier = Arc::new(Barrier::new(config.clients + 1));
    let mut handles = Vec::new();
    for worker in 0..config.clients {
        let config = config.clone();
        let barrier = Arc::clone(&barrier);
        handles.push(thread::spawn(move || {
            let count = assigned(config.operations, worker, config.clients);
            let mut state = config.seed ^ ((worker as u64 + 1).wrapping_mul(0x9e37_79b9_7f4a_7c15));
            let mut client = Client::connect(config.addr.as_str()).ok();
            let mut warmup_failed = false;
            if let Some(ref mut client) = client {
                let warmup = assigned(config.warmup, worker, config.clients);
                warmup_failed =
                    run_operations(client, &config, warmup, false, &mut state).failed > 0;
            }
            barrier.wait();
            if warmup_failed {
                WorkerResult {
                    skipped: count,
                    ..WorkerResult::default()
                }
            } else if let Some(ref mut client) = client {
                run_operations(client, &config, count, true, &mut state)
            } else {
                WorkerResult {
                    failed: usize::from(count > 0),
                    skipped: count.saturating_sub(1),
                    ..WorkerResult::default()
                }
            }
        }));
    }
    barrier.wait();
    let started = Instant::now();
    let mut combined = WorkerResult::default();
    for handle in handles {
        let worker = handle.join().map_err(|_| "benchmark worker panicked")?;
        combined.succeeded += worker.succeeded;
        combined.failed += worker.failed;
        combined.skipped += worker.skipped;
        combined.reads += worker.reads;
        combined.writes += worker.writes;
        combined.read_misses += worker.read_misses;
        combined.latencies_ns.extend(worker.latencies_ns);
    }
    let elapsed = started.elapsed();
    // Time for every replica to apply the run's writes after the last client
    // finished: how far replication trailed the measured workload.
    let catch_up_started = Instant::now();
    let caught_up = wait_replicas(&mut setup, config.replicas).is_ok();
    let replica_catch_up_ms = (config.replicas > 0 && caught_up)
        .then(|| format!("{:.1}", catch_up_started.elapsed().as_secs_f64() * 1000.0));
    // Server-side percentiles are cumulative since the server started, so
    // they describe this run only when each run uses a fresh server.
    let server_stats = setup.stats().unwrap_or_default();
    let server_stat = |name: &str| {
        server_stats
            .lines()
            .find_map(|line| line.strip_prefix(name)?.strip_prefix('='))
            .filter(|value| value.parse::<u64>().is_ok())
            .unwrap_or_default()
            .to_string()
    };
    let (cpu_after, _, peak_rss_kib) = process_resources(config.server_pid);
    let cpu_percent = match (cpu_before, cpu_after, clock_ticks) {
        (Some(before), Some(after), Some(hz)) if hz > 0 && elapsed.as_secs_f64() > 0.0 => {
            Some((after.saturating_sub(before) as f64 / hz as f64) / elapsed.as_secs_f64() * 100.0)
        }
        _ => None,
    };
    combined.latencies_ns.sort_unstable();
    let avg_ns = if combined.latencies_ns.is_empty() {
        None
    } else {
        Some(
            combined
                .latencies_ns
                .iter()
                .map(|&v| v as u128)
                .sum::<u128>() as f64
                / combined.latencies_ns.len() as f64,
        )
    };
    let data_bytes = config
        .data_dir
        .as_deref()
        .and_then(|path| directory_bytes(path, false));
    let wal_bytes = config
        .data_dir
        .as_deref()
        .and_then(|path| directory_bytes(path, true));
    let revision = command_output("git", &["rev-parse", "HEAD"]);
    let kernel = command_output("uname", &["-r"]);
    let timestamp_ms = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|error| error.to_string())?
        .as_millis();
    let header = "timestamp_ms,revision,build_mode,os,arch,kernel,filesystem,storage_medium,topology,durability,replicas_configured,replicas_caught_up,clients,operations_requested,warmup_requested,read_ratio,keys,value_bytes,seed,elapsed_seconds,successful_ops,failed_ops,skipped_ops,read_ops,write_ops,read_misses,ops_per_second,avg_latency_ns,p50_latency_ns,p95_latency_ns,p99_latency_ns,server_cpu_percent,server_peak_rss_kib,data_bytes,wal_bytes,server_read_p99_us,server_read_lock_wait_p99_us,server_write_lock_hold_p99_us,server_wal_sync_avg_us,replica_catch_up_ms";
    let fields = vec![
        timestamp_ms.to_string(),
        csv_field(&revision),
        csv_field(if cfg!(debug_assertions) {
            "debug"
        } else {
            "release"
        }),
        csv_field(std::env::consts::OS),
        csv_field(std::env::consts::ARCH),
        csv_field(&kernel),
        csv_field(&config.filesystem),
        csv_field(&config.storage_medium),
        csv_field(&config.topology),
        csv_field(&config.durability),
        config.replicas.to_string(),
        caught_up.to_string(),
        config.clients.to_string(),
        config.operations.to_string(),
        config.warmup.to_string(),
        format!("{:.4}", config.read_bps as f64 / 10_000.0),
        config.keys.to_string(),
        config.value_bytes.to_string(),
        config.seed.to_string(),
        format!("{:.6}", elapsed.as_secs_f64()),
        combined.succeeded.to_string(),
        combined.failed.to_string(),
        combined.skipped.to_string(),
        combined.reads.to_string(),
        combined.writes.to_string(),
        combined.read_misses.to_string(),
        format!("{:.2}", combined.succeeded as f64 / elapsed.as_secs_f64()),
        avg_ns.map(|v| format!("{v:.0}")).unwrap_or_default(),
        percentile(&combined.latencies_ns, 5_000)
            .map(|v| v.to_string())
            .unwrap_or_default(),
        percentile(&combined.latencies_ns, 9_500)
            .map(|v| v.to_string())
            .unwrap_or_default(),
        percentile(&combined.latencies_ns, 9_900)
            .map(|v| v.to_string())
            .unwrap_or_default(),
        cpu_percent.map(|v| format!("{v:.2}")).unwrap_or_default(),
        peak_rss_kib.map(|v| v.to_string()).unwrap_or_default(),
        data_bytes.map(|v| v.to_string()).unwrap_or_default(),
        wal_bytes.map(|v| v.to_string()).unwrap_or_default(),
        server_stat("read_latency_p99_us"),
        server_stat("read_lock_wait_p99_us"),
        server_stat("write_lock_hold_p99_us"),
        server_stat("wal_sync_avg_us"),
        replica_catch_up_ms.unwrap_or_default(),
    ];
    let row = fields.join(",");
    if let Some(path) = config.output.as_deref() {
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent).map_err(|error| error.to_string())?;
        }
        let needs_header =
            !path.exists() || fs::metadata(path).map_err(|error| error.to_string())?.len() == 0;
        let mut output = OpenOptions::new()
            .create(true)
            .append(true)
            .open(path)
            .map_err(|error| error.to_string())?;
        if needs_header {
            writeln!(output, "{header}").map_err(|error| error.to_string())?;
        }
        writeln!(output, "{row}").map_err(|error| error.to_string())?;
    } else {
        println!("{header}\n{row}");
    }
    eprintln!("{} successful ops in {:.3}s ({:.1} ops/s); {} failed, {} skipped; p50={} ns, p95={} ns, p99={} ns", combined.succeeded, elapsed.as_secs_f64(), combined.succeeded as f64 / elapsed.as_secs_f64(), combined.failed, combined.skipped, percentile(&combined.latencies_ns, 5_000).unwrap_or(0), percentile(&combined.latencies_ns, 9_500).unwrap_or(0), percentile(&combined.latencies_ns, 9_900).unwrap_or(0));
    if combined.failed > 0 || combined.skipped > 0 || !caught_up {
        return Err("benchmark had failures or replicas did not catch up; inspect CSV row".into());
    }
    Ok(())
}

fn main() {
    let args: Vec<String> = std::env::args().skip(1).collect();
    match Config::parse(&args).and_then(run) {
        Ok(()) => {}
        Err(error) => {
            eprintln!("benchmark error: {error}");
            std::process::exit(1);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn allocation_preserves_total_operations() {
        assert_eq!((0..7).map(|i| assigned(100, i, 7)).sum::<usize>(), 100);
        assert_eq!(assigned(2, 4, 7), 0);
    }

    #[test]
    fn percentiles_use_nearest_rank() {
        let values = [10, 20, 30, 40];
        assert_eq!(percentile(&values, 5_000), Some(20));
        assert_eq!(percentile(&values, 9_500), Some(40));
        assert_eq!(percentile(&[], 5_000), None);
    }

    #[test]
    fn parser_rejects_invalid_ratios_and_topologies() {
        assert!(Config::parse(&["--read-ratio".into(), "1.1".into()]).is_err());
        assert!(Config::parse(&[
            "--durability".into(),
            "os".into(),
            "--replicas".into(),
            "1".into()
        ])
        .is_err());
    }
}
