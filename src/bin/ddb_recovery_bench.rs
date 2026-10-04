//! Repeatable snapshot-versus-WAL recovery measurement (SOW §§9, 17, 22).
//!
//! Both directories receive the same ordered mutations. One publishes a
//! snapshot before the shared tail. Repeated opens measure the same final map
//! with different recovery bases; correctness is checked before reporting.

use std::error::Error;
use std::fs::{self, File};
use std::io::{BufWriter, Write};
use std::path::{Path, PathBuf};
use std::time::{Instant, SystemTime, UNIX_EPOCH};

use distributedb::{Db, DurabilityMode, GetResult, Mutation, RealFs};

const GROUP_RECORDS: usize = 64;

struct Config {
    records: usize,
    tail: usize,
    keys: usize,
    value_bytes: usize,
    trials: usize,
    output: PathBuf,
}

impl Config {
    fn parse() -> Result<Self, Box<dyn Error>> {
        let mut config = Self {
            records: 10_000,
            tail: 500,
            keys: 2_000,
            value_bytes: 128,
            trials: 7,
            output: PathBuf::from("benchmarks/results/recovery.csv"),
        };
        let mut args = std::env::args().skip(1);
        while let Some(flag) = args.next() {
            let value = args
                .next()
                .ok_or_else(|| format!("missing value for {flag}"))?;
            match flag.as_str() {
                "--records" => config.records = value.parse()?,
                "--tail" => config.tail = value.parse()?,
                "--keys" => config.keys = value.parse()?,
                "--value-bytes" => config.value_bytes = value.parse()?,
                "--trials" => config.trials = value.parse()?,
                "--output" => config.output = PathBuf::from(value),
                _ => return Err(format!("unknown option {flag}").into()),
            }
        }
        if config.records == 0
            || config.keys == 0
            || config.keys > config.records
            || config.value_bytes == 0
            || config.value_bytes > 4096
            || config.trials == 0
            || config.trials > 100
            || config.records.checked_add(config.tail).is_none()
        {
            return Err("invalid benchmark size or trial count".into());
        }
        Ok(config)
    }
}

struct TempRoot(PathBuf);

impl TempRoot {
    fn new() -> Result<Self, Box<dyn Error>> {
        let nonce = SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos();
        let root =
            std::env::temp_dir().join(format!("ddb-recovery-bench-{}-{nonce}", std::process::id()));
        fs::create_dir(&root)?;
        Ok(Self(root))
    }
}

impl Drop for TempRoot {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}

fn key(index: usize) -> Vec<u8> {
    format!("key:{index:08}").into_bytes()
}

fn value(operation: usize, len: usize) -> Vec<u8> {
    let bytes = (operation as u64).to_le_bytes();
    (0..len).map(|index| bytes[index % bytes.len()]).collect()
}

fn append_range(
    db: &mut Db<RealFs>,
    start: usize,
    end: usize,
    config: &Config,
) -> Result<(), Box<dyn Error>> {
    for group_start in (start..end).step_by(GROUP_RECORDS) {
        let group_end = end.min(group_start + GROUP_RECORDS);
        let mutations: Vec<_> = (group_start..group_end)
            .map(|operation| Mutation::Set {
                key: key(operation % config.keys),
                value: value(operation, config.value_bytes),
            })
            .collect();
        db.apply_group(&mutations)?;
    }
    Ok(())
}

fn prepare(root: &Path, snapshot: bool, config: &Config) -> Result<(), Box<dyn Error>> {
    let mut db = Db::open(RealFs::new(), root, DurabilityMode::Fsync)?;
    append_range(&mut db, 0, config.records, config)?;
    if snapshot {
        let lsn = db.publish_snapshot()?;
        assert_eq!(lsn, config.records as u64);
    }
    append_range(
        &mut db,
        config.records,
        config.records + config.tail,
        config,
    )?;
    Ok(())
}

fn directory_bytes(path: &Path) -> Result<u64, Box<dyn Error>> {
    let mut total = 0u64;
    for entry in fs::read_dir(path)? {
        let entry = entry?;
        let metadata = entry.metadata()?;
        if metadata.is_dir() {
            total += directory_bytes(&entry.path())?;
        } else {
            total += metadata.len();
        }
    }
    Ok(total)
}

struct Sample {
    scenario: &'static str,
    trial: usize,
    snapshot_lsn: u64,
    replayed: u64,
    recovery_us: u128,
    open_us: u128,
    data_bytes: u64,
}

fn measure(
    scenario: &'static str,
    path: &Path,
    trial: usize,
    config: &Config,
) -> Result<Sample, Box<dyn Error>> {
    let started = Instant::now();
    let db = Db::open(RealFs::new(), path, DurabilityMode::Fsync)?;
    let open_us = started.elapsed().as_micros();
    let expected_replayed = if scenario == "full_wal" {
        config.records + config.tail
    } else {
        config.tail
    };
    if db.last_applied_lsn() != (config.records + config.tail) as u64
        || db.records_replayed() != expected_replayed as u64
    {
        return Err(format!("{scenario}: unexpected recovery LSN or replay count").into());
    }
    let sample = Sample {
        scenario,
        trial,
        snapshot_lsn: db.snapshot_lsn(),
        replayed: db.records_replayed(),
        recovery_us: db.recovery_duration().as_micros(),
        open_us,
        data_bytes: directory_bytes(path)?,
    };
    Ok(sample)
}

fn verify_state(path: &Path, config: &Config) -> Result<(), Box<dyn Error>> {
    let db = Db::open(RealFs::new(), path, DurabilityMode::Fsync)?;
    let mut expected = vec![Vec::new(); config.keys];
    for operation in 0..config.records + config.tail {
        expected[operation % config.keys] = value(operation, config.value_bytes);
    }
    for (index, wanted) in expected.into_iter().enumerate() {
        if db.get(&key(index)) != GetResult::Found(wanted) {
            return Err(format!("recovered value differs for key {index}").into());
        }
    }
    Ok(())
}

fn report(samples: &[Sample], scenario: &str) {
    let mut times: Vec<_> = samples
        .iter()
        .filter(|sample| sample.scenario == scenario)
        .map(|sample| sample.recovery_us)
        .collect();
    times.sort_unstable();
    println!(
        "{scenario}: recovery_us median={} range={}..{}",
        times[times.len() / 2],
        times[0],
        times[times.len() - 1]
    );
}

fn main() -> Result<(), Box<dyn Error>> {
    let config = Config::parse()?;
    let work = TempRoot::new()?;
    let full = work.0.join("full-wal");
    let snap = work.0.join("snapshot-tail");
    prepare(&full, false, &config)?;
    prepare(&snap, true, &config)?;

    // Warm each path before collecting trials. Alternate order to reduce
    // systematic page-cache and runner-load effects.
    measure("full_wal", &full, 0, &config)?;
    measure("snapshot_tail", &snap, 0, &config)?;
    let mut samples = Vec::with_capacity(config.trials * 2);
    for trial in 1..=config.trials {
        if trial % 2 == 0 {
            samples.push(measure("snapshot_tail", &snap, trial, &config)?);
            samples.push(measure("full_wal", &full, trial, &config)?);
        } else {
            samples.push(measure("full_wal", &full, trial, &config)?);
            samples.push(measure("snapshot_tail", &snap, trial, &config)?);
        }
    }
    verify_state(&full, &config)?;
    verify_state(&snap, &config)?;

    if let Some(parent) = config.output.parent().filter(|p| !p.as_os_str().is_empty()) {
        fs::create_dir_all(parent)?;
    }
    let mut output = BufWriter::new(File::create(&config.output)?);
    writeln!(output, "scenario,trial,initial_records,tail_records,keys,value_bytes,last_lsn,snapshot_lsn,replayed_records,recovery_us,open_us,data_bytes")?;
    for sample in &samples {
        writeln!(
            output,
            "{},{},{},{},{},{},{},{},{},{},{},{}",
            sample.scenario,
            sample.trial,
            config.records,
            config.tail,
            config.keys,
            config.value_bytes,
            config.records + config.tail,
            sample.snapshot_lsn,
            sample.replayed,
            sample.recovery_us,
            sample.open_us,
            sample.data_bytes
        )?;
    }
    output.flush()?;
    report(&samples, "full_wal");
    report(&samples, "snapshot_tail");
    println!("Wrote {}", config.output.display());
    Ok(())
}
