use iced::widget::{button, column, progress_bar, row, scrollable, text, text_input};
use iced::{Element, Subscription, Task};

use async_downloader::{
    Batch, Download, Downloader, Job, Progress, Settings, Threads, Verification, iced_view,
};

fn main() -> iced::Result {
    iced::application(State::new, State::update, State::view)
        .subscription(State::subscription)
        .title("async_downloader — iced")
        .run()
}

struct State {
    downloader: Downloader,
    url: String,
    downloads: Vec<Download>,
}

#[derive(Debug, Clone)]
enum Message {
    UrlChanged(String),
    Add,
    Pause(u64),
    Resume(u64),
    Cancel(u64),
    Tick,
}

impl State {
    fn new() -> Self {
        Self {
            downloader: Downloader::new(Settings {
                threads: Threads::Auto,
                max_concurrent: 3,
            })
            .expect("downloader"),
            url: "https://speed.hetzner.de/100MB.bin".to_owned(),
            downloads: Vec::new(),
        }
    }

    fn find(&self, id: u64) -> Option<&Download> {
        self.downloads.iter().find(|d| d.id() == id)
    }

    fn update(&mut self, message: Message) -> Task<Message> {
        match message {
            Message::UrlChanged(url) => self.url = url,
            Message::Add => {
                if !self.url.trim().is_empty() {
                    let name = async_downloader::file_name_from_url(&self.url)
                        .unwrap_or_else(|| format!("download-{}.bin", self.downloads.len() + 1));
                    let dest = std::env::temp_dir().join("async_downloader").join(name);
                    let job = Job::new(self.url.clone(), dest);
                    self.downloads.push(self.downloader.enqueue(job));
                }
            }
            Message::Pause(id) => {
                if let Some(d) = self.find(id) {
                    d.pause();
                }
            }
            Message::Resume(id) => {
                if let Some(d) = self.find(id) {
                    d.resume();
                }
            }
            Message::Cancel(id) => {
                if let Some(d) = self.find(id) {
                    d.cancel();
                }
            }
            Message::Tick => {}
        }
        Task::none()
    }

    fn subscription(&self) -> Subscription<Message> {
        iced_view::subscribe_all(&self.downloads, |_, _| Message::Tick)
    }

    fn view(&self) -> Element<'_, Message> {
        let header = row![
            text_input("URL", &self.url)
                .on_input(Message::UrlChanged)
                .width(380.0),
            button("Add").on_press(Message::Add),
        ]
        .spacing(8);

        let cards = self.downloads.iter().map(card).collect::<Vec<_>>();

        let runtime = text(format!(
            "{} threads · {} concurrent",
            self.downloader.worker_threads(),
            self.downloader.max_concurrent()
        ));

        let pack = Batch::new(self.downloads.clone()).progress();
        let overall = text(format!(
            "Pack — {}/{} files · {} · {}",
            pack.done,
            pack.files,
            pack.bytes_total
                .map(|total| format!(
                    "{} / {}",
                    async_downloader::human_bytes(pack.bytes_received),
                    async_downloader::human_bytes(total)
                ))
                .unwrap_or_else(|| format!("{}/{} settled", pack.settled(), pack.files)),
            async_downloader::human_speed(pack.speed)
        ));

        column![
            text("Async downloader").size(24),
            runtime,
            header,
            overall,
            scrollable(column(cards).spacing(8)),
        ]
        .spacing(12)
        .padding(16)
        .into()
    }
}

fn card(download: &Download) -> Element<'_, Message> {
    let id = download.id();
    let progress = download.progress();

    let title = download
        .job()
        .dest
        .file_name()
        .map(|name| name.to_string_lossy().into_owned())
        .unwrap_or_else(|| format!("download #{id}"));

    let bar = progress_bar(0.0..=1.0, progress.fraction().unwrap_or(0.0));

    let status = match &progress {
        Progress::Queued => "queued".to_owned(),
        Progress::Downloading {
            received,
            total,
            speed,
        } => format!(
            "{} · {}",
            detail("downloading", *received, *total),
            async_downloader::human_speed(*speed)
        ),
        Progress::Paused { received, total } => detail("paused", *received, *total),
        Progress::Verifying => "verifying SHA-256...".to_owned(),
        Progress::Done(outcome) => match outcome.verified {
            Verification::Ok => format!("✓ verified · {}", async_downloader::human_bytes(outcome.bytes)),
            Verification::Mismatch => "✗ SHA-256 mismatch".to_owned(),
            Verification::Skipped => format!("done · {}", async_downloader::human_bytes(outcome.bytes)),
        },
        Progress::Failed(error) => format!("failed: {error}"),
        Progress::Cancelled => "cancelled".to_owned(),
    };

    let mut actions = row![].spacing(6);
    match &progress {
        Progress::Downloading { .. } | Progress::Queued | Progress::Verifying => {
            actions = actions.push(button("Pause").on_press(Message::Pause(id)));
            actions = actions.push(button("Cancel").on_press(Message::Cancel(id)));
        }
        Progress::Paused { .. } | Progress::Failed(_) => {
            actions = actions.push(button("Resume").on_press(Message::Resume(id)));
            actions = actions.push(button("Cancel").on_press(Message::Cancel(id)));
        }
        Progress::Done(_) | Progress::Cancelled => {}
    }

    column![text(title), bar, text(status), actions]
        .spacing(4)
        .padding(8)
        .into()
}

fn detail(label: &str, received: u64, total: Option<u64>) -> String {
    match total {
        Some(total) => format!(
            "{label} · {} / {}",
            async_downloader::human_bytes(received),
            async_downloader::human_bytes(total)
        ),
        None => format!("{label} · {}", async_downloader::human_bytes(received)),
    }
}
