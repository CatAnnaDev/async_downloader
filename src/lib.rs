//! Async file downloader for Rust GUIs.
//!
//! The engine runs on its own multi-threaded Tokio runtime and never blocks the
//! UI thread: the interface only reads [`Download::progress`] and sends control
//! commands ([`Download::pause`], [`Download::resume`], [`Download::cancel`]).
//!
//! A prespawned worker pool pulls [`Job`]s from a queue, so the number of
//! workers is the number of simultaneous downloads. Each transfer streams to
//! disk, can resume after an interruption via an HTTP `Range` request, and is
//! verified with SHA-256 once finished.
//!
//! Optional ready-made UI integrations live behind the `egui` and `iced`
//! features ([`egui_view`] / [`iced_view`]).
//!
//! # Examples
//!
//! ```no_run
//! use async_downloader::{Downloader, Job, Progress, Settings, Threads};
//!
//! let downloader = Downloader::new(Settings {
//!     threads: Threads::Auto,
//!     max_concurrent: 3,
//! })
//! .expect("runtime");
//!
//! let download = downloader.enqueue(Job::new(
//!     "https://example.com/file.bin",
//!     "/tmp/file.bin",
//! ));
//!
//! if let Progress::Downloading { received, total, speed } = download.progress() {
//!     println!("{received} / {total:?} at {speed} B/s");
//! }
//! ```

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

/// How many worker threads the internal Tokio runtime should use.
///
/// This is independent from [`Settings::max_concurrent`]: it sizes the runtime
/// thread pool, not the number of simultaneous downloads.
#[derive(Clone, Copy, Debug)]
pub enum Threads {
    /// Use [`std::thread::available_parallelism`] (falls back to 4).
    Auto,
    /// Use exactly this many threads (clamped to at least 1).
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

/// Configuration for a [`Downloader`].
///
/// # Examples
///
/// ```
/// use async_downloader::{Settings, Threads};
///
/// let settings = Settings { threads: Threads::Fixed(8), max_concurrent: 4 };
/// assert_eq!(Settings::default().max_concurrent, 3);
/// ```
#[derive(Clone, Copy, Debug)]
pub struct Settings {
    /// Size of the runtime thread pool.
    pub threads: Threads,
    /// Maximum number of downloads running at the same time.
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

/// A single file to download: a source URL, a destination path, and an
/// optional expected SHA-256 digest to verify against.
#[derive(Clone, Debug)]
pub struct Job {
    /// Source URL.
    pub url: String,
    /// Destination path on disk (parent directories are created as needed).
    pub dest: PathBuf,
    /// Expected lowercase hex SHA-256, compared after download. `None` skips
    /// the comparison but still computes the digest.
    pub expected_sha256: Option<String>,
}

impl Job {
    /// Creates a job with no expected digest.
    ///
    /// # Examples
    ///
    /// ```
    /// use async_downloader::Job;
    ///
    /// let job = Job::new("https://example.com/a.bin", "/tmp/a.bin");
    /// assert_eq!(job.url, "https://example.com/a.bin");
    /// assert!(job.expected_sha256.is_none());
    /// ```
    pub fn new(url: impl Into<String>, dest: impl Into<PathBuf>) -> Self {
        Self {
            url: url.into(),
            dest: dest.into(),
            expected_sha256: None,
        }
    }

    /// Attaches an expected SHA-256 digest, enabling verification.
    ///
    /// # Examples
    ///
    /// ```
    /// use async_downloader::Job;
    ///
    /// let job = Job::new("https://example.com/a.bin", "/tmp/a.bin")
    ///     .with_sha256("e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855");
    /// assert!(job.expected_sha256.is_some());
    /// ```
    pub fn with_sha256(mut self, sha256: impl Into<String>) -> Self {
        self.expected_sha256 = Some(sha256.into());
        self
    }
}

/// Result of comparing a finished file against its expected SHA-256.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Verification {
    /// No expected digest was provided.
    Skipped,
    /// The computed digest matched the expected one.
    Ok,
    /// The computed digest did not match the expected one.
    Mismatch,
}

/// Details of a successfully downloaded and hashed file.
#[derive(Clone, Debug)]
pub struct Outcome {
    /// Path the file was written to.
    pub path: PathBuf,
    /// Total bytes on disk.
    pub bytes: u64,
    /// Computed lowercase hex SHA-256 of the file.
    pub sha256: String,
    /// Outcome of the comparison against the expected digest.
    pub verified: Verification,
}

/// Live state of a single download, read by the UI every frame.
///
/// # Examples
///
/// ```
/// use async_downloader::Progress;
///
/// let p = Progress::Downloading { received: 50, total: Some(100), speed: 1024.0 };
/// assert_eq!(p.fraction(), Some(0.5));
/// assert_eq!(p.speed(), 1024.0);
/// assert!(!p.is_finished());
/// ```
#[derive(Clone, Debug)]
pub enum Progress {
    /// Waiting in the queue for a free worker.
    Queued,
    /// Transfer in progress. `total` is `None` when the server sends no length.
    Downloading {
        /// Bytes written to disk so far.
        received: u64,
        /// Total size if known.
        total: Option<u64>,
        /// Smoothed speed in bytes per second.
        speed: f64,
    },
    /// Paused by the user; the partial file is kept for resuming.
    Paused {
        /// Bytes already on disk.
        received: u64,
        /// Total size if known.
        total: Option<u64>,
    },
    /// Download complete, computing the SHA-256 from disk.
    Verifying,
    /// Finished successfully. See [`Outcome`].
    Done(Outcome),
    /// Failed with this error message. The partial file is kept so the download
    /// can be resumed.
    Failed(String),
    /// Cancelled by the user; the partial file was removed.
    Cancelled,
}

impl Progress {
    /// Whether the download reached a terminal, non-failure state
    /// ([`Done`](Progress::Done) or [`Cancelled`](Progress::Cancelled)).
    pub fn is_finished(&self) -> bool {
        matches!(self, Progress::Done(_) | Progress::Cancelled)
    }

    /// Whether work is actively happening (downloading or verifying).
    pub fn is_running(&self) -> bool {
        matches!(self, Progress::Downloading { .. } | Progress::Verifying)
    }

    /// Bytes on disk for this download, or 0 when not applicable.
    pub fn received(&self) -> u64 {
        match self {
            Progress::Downloading { received, .. } | Progress::Paused { received, .. } => *received,
            Progress::Done(o) => o.bytes,
            _ => 0,
        }
    }

    /// Current speed in bytes per second, or 0 unless downloading.
    pub fn speed(&self) -> f64 {
        match self {
            Progress::Downloading { speed, .. } => *speed,
            _ => 0.0,
        }
    }

    /// Completion ratio in `0.0..=1.0`, or `None` when the total is unknown.
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

/// A cheap, clonable handle to one queued download.
///
/// Clones share the same underlying transfer, so you can keep one in your UI
/// state and pass others around. Read [`progress`](Download::progress) to draw,
/// and call [`pause`](Download::pause) / [`resume`](Download::resume) /
/// [`cancel`](Download::cancel) to control it.
///
/// # Examples
///
/// ```no_run
/// use async_downloader::{Downloader, Job, Settings};
///
/// let downloader = Downloader::new(Settings::default()).unwrap();
/// let download = downloader.enqueue(Job::new("https://example.com/f", "/tmp/f"));
///
/// download.pause();
/// download.resume();
/// download.cancel();
/// ```
#[derive(Clone)]
pub struct Download {
    shared: Arc<Shared>,
    state: watch::Receiver<Progress>,
    queue: async_channel::Sender<Arc<Shared>>,
}

impl Download {
    /// Unique id assigned when the job was enqueued. Stable across clones.
    pub fn id(&self) -> u64 {
        self.shared.id
    }

    /// The [`Job`] this download was created from.
    pub fn job(&self) -> &Job {
        &self.shared.job
    }

    /// Snapshot of the current [`Progress`]. Cheap; call it every frame.
    pub fn progress(&self) -> Progress {
        self.state.borrow().clone()
    }

    /// A [`watch::Receiver`] that yields on every progress change.
    ///
    /// Used by the `iced` integration to drive a subscription; most UIs can
    /// just poll [`progress`](Download::progress) instead.
    pub fn watch(&self) -> watch::Receiver<Progress> {
        self.state.clone()
    }

    /// Requests a pause. The worker stops after the current chunk and keeps the
    /// partial file. No effect on a finished download.
    pub fn pause(&self) {
        self.shared.command.store(CMD_PAUSE, Ordering::SeqCst);
    }

    /// Re-queues a paused or failed download, resuming from the bytes already on
    /// disk via an HTTP `Range` request. No effect in any other state.
    pub fn resume(&self) {
        if matches!(self.progress(), Progress::Paused { .. } | Progress::Failed(_)) {
            self.shared.command.store(CMD_RUN, Ordering::SeqCst);
            self.shared.set(Progress::Queued);
            let _ = self.queue.try_send(self.shared.clone());
        }
    }

    /// Cancels the download and removes the partial file.
    pub fn cancel(&self) {
        self.shared.command.store(CMD_CANCEL, Ordering::SeqCst);
        if !self.shared.running.load(Ordering::SeqCst) && !self.progress().is_finished() {
            let _ = self.queue.try_send(self.shared.clone());
        }
    }
}

/// Aggregated progress over a [`Batch`] of downloads.
///
/// # Examples
///
/// ```
/// use async_downloader::BatchProgress;
///
/// let p = BatchProgress {
///     files: 4,
///     done: 2,
///     failed: 0,
///     cancelled: 0,
///     bytes_received: 30,
///     bytes_total: Some(100),
///     speed: 2048.0,
/// };
/// assert_eq!(p.settled(), 2);
/// assert_eq!(p.fraction(), Some(0.3));
/// assert!(!p.all_done());
/// ```

#[derive(Clone, Debug)]
pub struct BatchProgress {
    /// Total number of files in the batch.
    pub files: usize,
    /// Files finished and verified.
    pub done: usize,
    /// Files that failed.
    pub failed: usize,
    /// Files that were cancelled.
    pub cancelled: usize,
    /// Sum of bytes received across all files.
    pub bytes_received: u64,
    /// Sum of total sizes, or `None` if any unfinished file's size is unknown.
    pub bytes_total: Option<u64>,
    /// Combined speed of the files currently downloading, in bytes per second.
    pub speed: f64,
}

impl BatchProgress {
    /// Number of files in a terminal state (done + failed + cancelled).
    pub fn settled(&self) -> usize {
        self.done + self.failed + self.cancelled
    }

    /// Whether every file has settled (in any terminal state).
    pub fn is_complete(&self) -> bool {
        self.files > 0 && self.settled() == self.files
    }

    /// Whether every file finished successfully.
    pub fn all_done(&self) -> bool {
        self.files > 0 && self.done == self.files
    }

    /// Overall ratio in `0.0..=1.0`. Uses bytes when all totals are known,
    /// otherwise falls back to the settled-files ratio; `None` when empty.
    pub fn fraction(&self) -> Option<f32> {
        match self.bytes_total {
            Some(total) if total > 0 => Some((self.bytes_received as f32 / total as f32).min(1.0)),
            _ if self.files > 0 => Some(self.settled() as f32 / self.files as f32),
            _ => None,
        }
    }
}

/// A group of downloads handled as one unit, e.g. a launcher pack.
///
/// Returned by [`Downloader::enqueue_batch`]. Read [`progress`](Batch::progress)
/// for an aggregate view, iterate [`downloads`](Batch::downloads) for per-file
/// state, and use the `*_all` methods for group control.
///
/// # Examples
///
/// ```no_run
/// use async_downloader::{Downloader, Job, Settings};
///
/// let downloader = Downloader::new(Settings::default()).unwrap();
/// let batch = downloader.enqueue_batch(vec![
///     Job::new("https://example.com/a.pak", "/data/a.pak"),
///     Job::new("https://example.com/b.pak", "/data/b.pak"),
/// ]);
///
/// let p = batch.progress();
/// println!("{}/{} files", p.done, p.files);
/// batch.pause_all();
/// ```
#[derive(Clone)]
pub struct Batch {
    downloads: Vec<Download>,
}

impl Batch {
    /// Wraps an existing set of [`Download`] handles, e.g. to aggregate
    /// downloads you enqueued individually.
    pub fn new(downloads: Vec<Download>) -> Self {
        Self { downloads }
    }

    /// The individual downloads, for per-file progress and controls.
    pub fn downloads(&self) -> &[Download] {
        &self.downloads
    }

    /// Whether every file in the batch has settled.
    pub fn is_finished(&self) -> bool {
        self.progress().is_complete()
    }

    /// Pauses every file in the batch.
    pub fn pause_all(&self) {
        self.downloads.iter().for_each(Download::pause);
    }

    /// Resumes every paused or failed file in the batch.
    pub fn resume_all(&self) {
        self.downloads.iter().for_each(Download::resume);
    }

    /// Cancels every file in the batch.
    pub fn cancel_all(&self) {
        self.downloads.iter().for_each(Download::cancel);
    }

    /// Recomputes the aggregate [`BatchProgress`] from each file's current
    /// state. Cheap; call it every frame.
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

/// Owns the Tokio runtime and the prespawned worker pool, and hands out
/// [`Download`] / [`Batch`] handles.
///
/// Keep it alive for as long as its downloads run: dropping it shuts the
/// runtime down and aborts in-flight transfers.
///
/// # Examples
///
/// ```no_run
/// use async_downloader::{Downloader, Job, Settings, Threads};
///
/// let downloader = Downloader::new(Settings {
///     threads: Threads::Auto,
///     max_concurrent: 3,
/// })
/// .expect("runtime");
///
/// let download = downloader.enqueue(Job::new("https://example.com/f", "/tmp/f"));
/// let _ = download.progress();
/// ```
pub struct Downloader {
    rt: tokio::runtime::Runtime,
    queue: async_channel::Sender<Arc<Shared>>,
    next_id: AtomicU64,
    worker_threads: usize,
    max_concurrent: usize,
    on_change: Arc<dyn Fn() + Send + Sync>,
}

impl Downloader {
    /// Builds the runtime and immediately spawns `max_concurrent` worker tasks.
    ///
    /// Returns an error only if the Tokio runtime fails to start.
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

    /// Sets a callback fired on every progress change, on a worker thread.
    ///
    /// Use it to wake the UI; with `egui` this is
    /// [`egui_view::repaint_notifier`].
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use async_downloader::{Downloader, Settings};
    ///
    /// let downloader = Downloader::new(Settings::default())
    ///     .unwrap()
    ///     .on_change(|| println!("progress changed"));
    /// ```
    pub fn on_change(mut self, callback: impl Fn() + Send + Sync + 'static) -> Self {
        self.on_change = Arc::new(callback);
        self
    }

    /// Number of runtime worker threads (the resolved [`Threads`] value).
    pub fn worker_threads(&self) -> usize {
        self.worker_threads
    }

    /// Maximum number of downloads running at once (the size of the worker pool).
    pub fn max_concurrent(&self) -> usize {
        self.max_concurrent
    }

    /// Queues a single [`Job`] and returns its [`Download`] handle.
    ///
    /// Returns immediately; the transfer starts as soon as a worker is free.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use async_downloader::{Downloader, Job, Settings};
    ///
    /// let downloader = Downloader::new(Settings::default()).unwrap();
    /// let download = downloader.enqueue(Job::new("https://example.com/f", "/tmp/f"));
    /// ```
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

    /// Queues many jobs as one [`Batch`] with aggregate progress and group
    /// controls.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use async_downloader::{Downloader, Job, Settings};
    ///
    /// let downloader = Downloader::new(Settings::default()).unwrap();
    /// let batch = downloader.enqueue_batch(vec![
    ///     Job::new("https://example.com/a", "/tmp/a"),
    ///     Job::new("https://example.com/b", "/tmp/b"),
    /// ]);
    /// assert_eq!(batch.downloads().len(), 2);
    /// ```
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

/// Formats a byte count with a binary unit (`B`, `KiB`, `MiB`, ...).
///
/// # Examples
///
/// ```
/// assert_eq!(async_downloader::human_bytes(512), "512 B");
/// assert_eq!(async_downloader::human_bytes(1024), "1.0 KiB");
/// assert_eq!(async_downloader::human_bytes(5 * 1024 * 1024), "5.0 MiB");
/// ```
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

/// Formats a speed as `"<size>/s"`, or `"—"` when zero or negative.
///
/// # Examples
///
/// ```
/// assert_eq!(async_downloader::human_speed(0.0), "—");
/// assert_eq!(async_downloader::human_speed(2048.0), "2.0 KiB/s");
/// ```
pub fn human_speed(bytes_per_second: f64) -> String {
    if bytes_per_second <= 0.0 {
        return "—".to_owned();
    }
    format!("{}/s", human_bytes(bytes_per_second as u64))
}

/// Extracts a file name from a URL, ignoring any query or fragment.
///
/// Returns `None` when there is no usable last path segment.
///
/// # Examples
///
/// ```
/// use async_downloader::file_name_from_url;
///
/// assert_eq!(file_name_from_url("https://host/dir/file.bin?v=1").as_deref(), Some("file.bin"));
/// assert_eq!(file_name_from_url(""), None);
/// ```
pub fn file_name_from_url(url: &str) -> Option<String> {
    let trimmed = url.split(['?', '#']).next().unwrap_or(url);
    let name = trimmed.trim_end_matches('/').rsplit('/').next()?;
    (!name.is_empty()).then(|| name.to_owned())
}
