# async_downloader

Async file downloader for Rust GUIs. The engine runs on its own multi-threaded
Tokio runtime and never blocks the UI thread; the interface only reads progress
and sends control commands.

It is UI-agnostic at its core, with optional ready-made integrations for `egui`
and `iced` behind feature flags.

## Features

- Prespawned worker pool: workers are spawned once and wait on a queue, so the
  reqwest connection pool stays warm and there is no per-download task spawn cost.
- Queue with a concurrency limit: the number of workers is the number of
  simultaneous downloads.
- Per-file pause, resume and cancel.
- HTTP range resume: an interrupted or paused transfer continues from the bytes
  already on disk using a `Range` request.
- SHA-256 verification: the finished file is re-read from disk and hashed,
  optionally compared against an expected digest.
- Live download speed, computed once in the engine and exposed to every UI.
- Batch downloads: enqueue a whole pack of files as one unit and read aggregate
  progress (files done, total bytes, combined speed) plus group controls. A
  single archive is just one job; a launcher-style set of files is one batch.

## Async model

`Downloader::new` builds a Tokio multi-threaded runtime and immediately spawns
`max_concurrent` worker tasks. `enqueue` pushes a `Job` onto an async MPMC queue
and returns a cheap, clonable `Download` handle. Workers pull jobs, stream the
body chunk by chunk with `reqwest` + `futures`, and publish state through a
`tokio::sync::watch` channel.

The UI never awaits anything. It reads `download.progress()` whenever it
repaints, and calls `pause` / `resume` / `cancel`, which set an atomic command
the worker observes between chunks. Two independent knobs: `Threads` controls
the runtime worker threads (`Auto` or `Fixed(n)`), `max_concurrent` controls how
many downloads run at once.

## Install

```toml
[dependencies]
async_downloader = { version = "0.1", features = ["egui"] } # or "iced"
```

Default features enable `egui`. Use `default-features = false` with
`features = ["iced"]` for an iced-only build, or no features for the bare engine.

## Core usage

```rust
use async_downloader::{Downloader, Job, Progress, Settings, Threads};

let downloader = Downloader::new(Settings {
    threads: Threads::Auto,
    max_concurrent: 3,
})?;

let download = downloader.enqueue(
    Job::new("https://example.com/file.bin", "/tmp/file.bin")
        .with_sha256("e3b0c44298fc1c149afbf4c8996fb924..."),
);

download.pause();
download.resume();
download.cancel();

match download.progress() {
    Progress::Downloading { received, total, speed } => { /* ... */ }
    Progress::Done(outcome) => { /* outcome.verified, outcome.sha256 */ }
    _ => {}
}
```

## Batch / pack

```rust
use async_downloader::Job;

let batch = downloader.enqueue_batch(vec![
    Job::new("https://example.com/a.pak", "/data/a.pak").with_sha256("..."),
    Job::new("https://example.com/b.pak", "/data/b.pak").with_sha256("..."),
    Job::new("https://example.com/c.zip", "/data/c.zip"),
]);

let p = batch.progress(); // files, done, failed, bytes_received, bytes_total, speed
let _ = p.fraction();     // overall 0.0..=1.0
let _ = p.all_done();     // every file downloaded and verified

batch.pause_all();
batch.resume_all();
batch.cancel_all();

for download in batch.downloads() {
    // per-file progress / controls
}
```

Files already present and matching their expected SHA-256 are not re-downloaded:
the engine issues one request, the server replies that the range is already
satisfied, the local file is verified, and it goes straight to done.

## egui

```rust
use async_downloader::{Downloader, Settings, Threads, egui_view};

let downloader = Downloader::new(Settings { threads: Threads::Auto, max_concurrent: 3 })?
    .on_change(egui_view::repaint_notifier(ctx)); // repaint when progress changes

// inside your update / ui callback:
egui_view::download_card(ui, &download); // progress bar, speed, pause/resume/cancel
```

Run the example:

```sh
cargo run --example egui_app
```

## iced

```rust
use async_downloader::iced_view;

// in subscription():
fn subscription(state: &State) -> iced::Subscription<Message> {
    iced_view::subscribe_all(&state.downloads, |_id, _progress| Message::Tick)
}

// in view(), read download.progress() and wire buttons to pause/resume/cancel.
```

`subscribe_all` builds one `iced::Subscription` per download from its `watch`
receiver, so the UI redraws on every progress change without polling.

Run the example:

```sh
cargo run --example iced_app --features iced
```

## License

Released into the public domain under the Unlicense. Do whatever you want with it.
See `LICENSE`.
