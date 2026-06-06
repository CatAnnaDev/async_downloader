use std::fmt::Write as _;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU8, AtomicU64, Ordering};
use std::time::Instant;

use futures_util::StreamExt;
use sha2::{Digest, Sha256};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::sync::watch;

#[cfg(feature = "egui")]
pub mod egui_view;
#[cfg(feature = "iced")]
pub mod iced_view;

#[derive(Clone, Copy, Debug)]
pub enum Threads {
    Auto,
    Fixed(usize),
}

impl Threads {
    fn resolve(self) -> usize {
        match self {
            Threads::Auto => std::thread::available_parallelism()
                .map(|n| n.get())
                .unwrap_or(4),
            Threads::Fixed(n) => n.max(1),
        }
    }
}

#[derive(Clone, Copy, Debug)]
pub struct Settings {
    pub threads: Threads,
    pub max_concurrent: usize,
}

impl Default for Settings {
    fn default() -> Self {
        Self {
            threads: Threads::Auto,
            max_concurrent: 3,
        }
    }
}

#[derive(Clone, Debug)]
pub struct Job {
    pub url: String,
    pub dest: PathBuf,
    pub expected_sha256: Option<String>,
}

impl Job {
    pub fn new(url: impl Into<String>, dest: impl Into<PathBuf>) -> Self {
        Self {
            url: url.into(),
            dest: dest.into(),
            expected_sha256: None,
        }
    }

    pub fn with_sha256(mut self, sha256: impl Into<String>) -> Self {
        self.expected_sha256 = Some(sha256.into());
        self
    }
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Verification {
    Skipped,
    Ok,
    Mismatch,
}

#[derive(Clone, Debug)]
pub struct Outcome {
    pub path: PathBuf,
    pub bytes: u64,
    pub sha256: String,
    pub verified: Verification,
}

#[derive(Clone, Debug)]
pub enum Progress {
    Queued,
    Downloading {
        received: u64,
        total: Option<u64>,
        speed: f64,
    },
    Paused { received: u64, total: Option<u64> },
    Verifying,
    Done(Outcome),
    Failed(String),
    Cancelled,
}

impl Progress {
    pub fn is_finished(&self) -> bool {
        matches!(self, Progress::Done(_) | Progress::Cancelled)
    }

    pub fn is_running(&self) -> bool {
        matches!(self, Progress::Downloading { .. } | Progress::Verifying)
    }

    pub fn received(&self) -> u64 {
        match self {
            Progress::Downloading { received, .. } | Progress::Paused { received, .. } => *received,
            Progress::Done(o) => o.bytes,
            _ => 0,
        }
    }

    pub fn speed(&self) -> f64 {
        match self {
            Progress::Downloading { speed, .. } => *speed,
            _ => 0.0,
        }
    }

    pub fn fraction(&self) -> Option<f32> {
        match self {
            Progress::Downloading {
                received,
                total: Some(total),
                ..
            }
            | Progress::Paused {
                received,
                total: Some(total),
            } if *total > 0 => Some(*received as f32 / *total as f32),
            Progress::Done(_) => Some(1.0),
            _ => None,
        }
    }
}

const CMD_RUN: u8 = 0;
const CMD_PAUSE: u8 = 1;
const CMD_CANCEL: u8 = 2;

struct Shared {
    id: u64,
    job: Job,
    command: AtomicU8,
    running: AtomicBool,
    state: watch::Sender<Progress>,
    on_change: Arc<dyn Fn() + Send + Sync>,
}

impl Shared {
    fn set(&self, progress: Progress) {
        let _ = self.state.send(progress);
        (self.on_change)();
    }

    fn command(&self) -> u8 {
        self.command.load(Ordering::SeqCst)
    }
}

#[derive(Clone)]
pub struct Download {
    shared: Arc<Shared>,
    state: watch::Receiver<Progress>,
    queue: async_channel::Sender<Arc<Shared>>,
}

impl Download {
    pub fn id(&self) -> u64 {
        self.shared.id
    }

    pub fn job(&self) -> &Job {
        &self.shared.job
    }

    pub fn progress(&self) -> Progress {
        self.state.borrow().clone()
    }

    pub fn watch(&self) -> watch::Receiver<Progress> {
        self.state.clone()
    }

    pub fn pause(&self) {
        self.shared.command.store(CMD_PAUSE, Ordering::SeqCst);
    }

    pub fn resume(&self) {
        if matches!(self.progress(), Progress::Paused { .. } | Progress::Failed(_)) {
            self.shared.command.store(CMD_RUN, Ordering::SeqCst);
            self.shared.set(Progress::Queued);
            let _ = self.queue.try_send(self.shared.clone());
        }
    }

    pub fn cancel(&self) {
        self.shared.command.store(CMD_CANCEL, Ordering::SeqCst);
        if !self.shared.running.load(Ordering::SeqCst) && !self.progress().is_finished() {
            let _ = self.queue.try_send(self.shared.clone());
        }
    }
}

#[derive(Clone, Debug)]
pub struct BatchProgress {
    pub files: usize,
    pub done: usize,
    pub failed: usize,
    pub cancelled: usize,
    pub bytes_received: u64,
    pub bytes_total: Option<u64>,
    pub speed: f64,
}

impl BatchProgress {
    pub fn settled(&self) -> usize {
        self.done + self.failed + self.cancelled
    }

    pub fn is_complete(&self) -> bool {
        self.files > 0 && self.settled() == self.files
    }

    pub fn all_done(&self) -> bool {
        self.files > 0 && self.done == self.files
    }

    pub fn fraction(&self) -> Option<f32> {
        match self.bytes_total {
            Some(total) if total > 0 => Some((self.bytes_received as f32 / total as f32).min(1.0)),
            _ if self.files > 0 => Some(self.settled() as f32 / self.files as f32),
            _ => None,
        }
    }
}

#[derive(Clone)]
pub struct Batch {
    downloads: Vec<Download>,
}

impl Batch {
    pub fn new(downloads: Vec<Download>) -> Self {
        Self { downloads }
    }

    pub fn downloads(&self) -> &[Download] {
        &self.downloads
    }

    pub fn is_finished(&self) -> bool {
        self.progress().is_complete()
    }

    pub fn pause_all(&self) {
        self.downloads.iter().for_each(Download::pause);
    }

    pub fn resume_all(&self) {
        self.downloads.iter().for_each(Download::resume);
    }

    pub fn cancel_all(&self) {
        self.downloads.iter().for_each(Download::cancel);
    }

    pub fn progress(&self) -> BatchProgress {
        let mut acc = BatchProgress {
            files: self.downloads.len(),
            done: 0,
            failed: 0,
            cancelled: 0,
            bytes_received: 0,
            bytes_total: Some(0),
            speed: 0.0,
        };

        for download in &self.downloads {
            match download.progress() {
                Progress::Done(outcome) => {
                    acc.done += 1;
                    acc.bytes_received += outcome.bytes;
                    if let Some(total) = acc.bytes_total.as_mut() {
                        *total += outcome.bytes;
                    }
                }
                Progress::Failed(_) => {
                    acc.failed += 1;
                    acc.bytes_total = None;
                }
                Progress::Cancelled => acc.cancelled += 1,
                Progress::Downloading {
                    received,
                    total,
                    speed,
                } => {
                    acc.bytes_received += received;
                    acc.speed += speed;
                    accumulate_total(&mut acc.bytes_total, total);
                }
                Progress::Paused { received, total } => {
                    acc.bytes_received += received;
                    accumulate_total(&mut acc.bytes_total, total);
                }
                Progress::Queued | Progress::Verifying => acc.bytes_total = None,
            }
        }

        acc
    }
}

fn accumulate_total(acc: &mut Option<u64>, total: Option<u64>) {
    match total {
        Some(total) => {
            if let Some(sum) = acc.as_mut() {
                *sum += total;
            }
        }
        None => *acc = None,
    }
}

pub struct Downloader {
    rt: tokio::runtime::Runtime,
    queue: async_channel::Sender<Arc<Shared>>,
    next_id: AtomicU64,
    worker_threads: usize,
    max_concurrent: usize,
    on_change: Arc<dyn Fn() + Send + Sync>,
}

impl Downloader {
    pub fn new(settings: Settings) -> std::io::Result<Self> {
        let worker_threads = settings.threads.resolve();
        let max_concurrent = settings.max_concurrent.max(1);

        let rt = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(worker_threads)
            .enable_all()
            .build()?;

        let (tx, rx) = async_channel::unbounded::<Arc<Shared>>();
        let client = reqwest::Client::new();

        for _ in 0..max_concurrent {
            let rx = rx.clone();
            let client = client.clone();
            rt.spawn(worker(client, rx));
        }

        Ok(Self {
            rt,
            queue: tx,
            next_id: AtomicU64::new(1),
            worker_threads,
            max_concurrent,
            on_change: Arc::new(|| {}),
        })
    }

    pub fn on_change(mut self, callback: impl Fn() + Send + Sync + 'static) -> Self {
        self.on_change = Arc::new(callback);
        self
    }

    pub fn worker_threads(&self) -> usize {
        self.worker_threads
    }

    pub fn max_concurrent(&self) -> usize {
        self.max_concurrent
    }

    pub fn enqueue(&self, job: Job) -> Download {
        let id = self.next_id.fetch_add(1, Ordering::SeqCst);
        let (state_tx, state_rx) = watch::channel(Progress::Queued);
        let shared = Arc::new(Shared {
            id,
            job,
            command: AtomicU8::new(CMD_RUN),
            running: AtomicBool::new(false),
            state: state_tx,
            on_change: self.on_change.clone(),
        });
        let _ = self.queue.try_send(shared.clone());
        let _ = &self.rt;
        Download {
            shared,
            state: state_rx,
            queue: self.queue.clone(),
        }
    }

    pub fn enqueue_batch(&self, jobs: impl IntoIterator<Item = Job>) -> Batch {
        Batch::new(jobs.into_iter().map(|job| self.enqueue(job)).collect())
    }
}

async fn worker(client: reqwest::Client, queue: async_channel::Receiver<Arc<Shared>>) {
    while let Ok(shared) = queue.recv().await {
        process(&client, &shared).await;
    }
}

async fn process(client: &reqwest::Client, shared: &Arc<Shared>) {
    match shared.command() {
        CMD_CANCEL => {
            remove_partial(&shared.job.dest).await;
            shared.set(Progress::Cancelled);
            return;
        }
        CMD_PAUSE => {
            let received = existing_size(&shared.job.dest).await;
            shared.set(Progress::Paused {
                received,
                total: None,
            });
            return;
        }
        _ => {}
    }

    shared.running.store(true, Ordering::SeqCst);
    let outcome = attempt(client, shared).await;
    shared.running.store(false, Ordering::SeqCst);

    match outcome {
        AttemptOutcome::Completed(outcome) => shared.set(Progress::Done(outcome)),
        AttemptOutcome::Paused { received, total } => {
            shared.set(Progress::Paused { received, total })
        }
        AttemptOutcome::Cancelled => {
            remove_partial(&shared.job.dest).await;
            shared.set(Progress::Cancelled);
        }
        AttemptOutcome::Failed(error) => shared.set(Progress::Failed(error)),
    }
}

enum AttemptOutcome {
    Completed(Outcome),
    Paused { received: u64, total: Option<u64> },
    Cancelled,
    Failed(String),
}

async fn attempt(client: &reqwest::Client, shared: &Arc<Shared>) -> AttemptOutcome {
    let job = &shared.job;

    if let Some(parent) = job.dest.parent() {
        if let Err(error) = tokio::fs::create_dir_all(parent).await {
            return AttemptOutcome::Failed(error.to_string());
        }
    }

    let mut offset = existing_size(&job.dest).await;
    let mut allow_resume = offset > 0;

    let response = loop {
        let mut request = client.get(&job.url);
        if allow_resume {
            request = request.header(reqwest::header::RANGE, format!("bytes={offset}-"));
        }

        let response = match request.send().await {
            Ok(response) => response,
            Err(error) => return AttemptOutcome::Failed(error.to_string()),
        };

        let status = response.status();

        if status == reqwest::StatusCode::RANGE_NOT_SATISFIABLE && allow_resume {
            shared.set(Progress::Verifying);
            match verify(job).await {
                Ok((_, Verification::Mismatch)) => {
                    offset = 0;
                    allow_resume = false;
                    continue;
                }
                Ok((sha256, verified)) => {
                    return AttemptOutcome::Completed(Outcome {
                        path: job.dest.clone(),
                        bytes: existing_size(&job.dest).await,
                        sha256,
                        verified,
                    });
                }
                Err(error) => return AttemptOutcome::Failed(error),
            }
        }

        match response.error_for_status() {
            Ok(response) => break (status, response),
            Err(error) => return AttemptOutcome::Failed(error.to_string()),
        }
    };

    let (status, response) = response;
    let resuming = allow_resume && status == reqwest::StatusCode::PARTIAL_CONTENT;
    if allow_resume && !resuming {
        offset = 0;
    }

    let total = response
        .content_length()
        .map(|len| if resuming { offset + len } else { len });

    let file = if resuming {
        tokio::fs::OpenOptions::new()
            .append(true)
            .open(&job.dest)
            .await
    } else {
        tokio::fs::File::create(&job.dest).await
    };
    let mut file = match file {
        Ok(file) => file,
        Err(error) => return AttemptOutcome::Failed(error.to_string()),
    };

    let mut received = offset;
    let mut speed = 0.0f64;
    shared.set(Progress::Downloading {
        received,
        total,
        speed,
    });

    let mut last_bytes = received;
    let mut last_instant = Instant::now();
    let mut stream = response.bytes_stream();

    loop {
        match shared.command() {
            CMD_PAUSE => {
                let _ = file.flush().await;
                return AttemptOutcome::Paused { received, total };
            }
            CMD_CANCEL => {
                let _ = file.flush().await;
                return AttemptOutcome::Cancelled;
            }
            _ => {}
        }

        match stream.next().await {
            Some(Ok(chunk)) => {
                if let Err(error) = file.write_all(&chunk).await {
                    return AttemptOutcome::Failed(error.to_string());
                }
                received += chunk.len() as u64;

                let elapsed = last_instant.elapsed().as_secs_f64();
                if received - last_bytes >= 64 * 1024 || elapsed >= 0.25 {
                    if elapsed > 0.0 {
                        let sample = (received - last_bytes) as f64 / elapsed;
                        speed = if speed == 0.0 {
                            sample
                        } else {
                            0.6 * speed + 0.4 * sample
                        };
                    }
                    last_bytes = received;
                    last_instant = Instant::now();
                    shared.set(Progress::Downloading {
                        received,
                        total,
                        speed,
                    });
                }
            }
            Some(Err(error)) => {
                let _ = file.flush().await;
                return AttemptOutcome::Failed(error.to_string());
            }
            None => break,
        }
    }

    if let Err(error) = file.flush().await {
        return AttemptOutcome::Failed(error.to_string());
    }
    drop(file);

    shared.set(Progress::Verifying);
    let (sha256, verified) = match verify(job).await {
        Ok(result) => result,
        Err(error) => return AttemptOutcome::Failed(error),
    };

    AttemptOutcome::Completed(Outcome {
        path: job.dest.clone(),
        bytes: received,
        sha256,
        verified,
    })
}

async fn verify(job: &Job) -> Result<(String, Verification), String> {
    let sha256 = sha256_of_file(&job.dest).await?;
    let verified = match &job.expected_sha256 {
        None => Verification::Skipped,
        Some(expected) if expected.trim().eq_ignore_ascii_case(&sha256) => Verification::Ok,
        Some(_) => Verification::Mismatch,
    };
    Ok((sha256, verified))
}

async fn existing_size(path: &Path) -> u64 {
    tokio::fs::metadata(path)
        .await
        .map(|meta| meta.len())
        .unwrap_or(0)
}

async fn remove_partial(path: &Path) {
    let _ = tokio::fs::remove_file(path).await;
}

async fn sha256_of_file(path: &Path) -> Result<String, String> {
    let mut file = tokio::fs::File::open(path)
        .await
        .map_err(|e| e.to_string())?;
    let mut hasher = Sha256::new();
    let mut buffer = vec![0u8; 64 * 1024];

    loop {
        let read = file.read(&mut buffer).await.map_err(|e| e.to_string())?;
        if read == 0 {
            break;
        }
        hasher.update(&buffer[..read]);
    }

    let mut hex = String::with_capacity(64);
    for byte in hasher.finalize() {
        let _ = write!(hex, "{byte:02x}");
    }
    Ok(hex)
}

pub fn human_bytes(bytes: u64) -> String {
    const UNITS: [&str; 5] = ["B", "KiB", "MiB", "GiB", "TiB"];
    let mut value = bytes as f64;
    let mut unit = 0;
    while value >= 1024.0 && unit < UNITS.len() - 1 {
        value /= 1024.0;
        unit += 1;
    }
    if unit == 0 {
        format!("{bytes} {}", UNITS[0])
    } else {
        format!("{value:.1} {}", UNITS[unit])
    }
}

pub fn human_speed(bytes_per_second: f64) -> String {
    if bytes_per_second <= 0.0 {
        return "—".to_owned();
    }
    format!("{}/s", human_bytes(bytes_per_second as u64))
}

pub fn file_name_from_url(url: &str) -> Option<String> {
    let trimmed = url.split(['?', '#']).next().unwrap_or(url);
    let name = trimmed.trim_end_matches('/').rsplit('/').next()?;
    (!name.is_empty()).then(|| name.to_owned())
}
