//! Ready-made `egui` widgets and helpers (enabled by the `egui` feature).

use egui::{Color32, ProgressBar, Ui};

use crate::{
    Batch, Download, Progress, Verification, file_name_from_url, human_bytes, human_speed,
};

/// Builds an [`Downloader::on_change`](crate::Downloader::on_change) callback
/// that repaints the egui context whenever progress changes.
///
/// # Examples
///
/// ```no_run
/// use async_downloader::{Downloader, Settings, egui_view};
/// # let ctx = egui::Context::default();
/// let downloader = Downloader::new(Settings::default())
///     .unwrap()
///     .on_change(egui_view::repaint_notifier(&ctx));
/// ```
pub fn repaint_notifier(ctx: &egui::Context) -> impl Fn() + Send + Sync + 'static {
    let ctx = ctx.clone();
    move || ctx.request_repaint()
}

/// Draws an aggregate card for a [`Batch`]: overall bar, combined speed, file
/// counts, and pause/resume/cancel-all buttons.
///
/// # Examples
///
/// ```no_run
/// # use async_downloader::{Batch, egui_view};
/// # fn ui(ui: &mut egui::Ui, batch: &Batch) {
/// egui_view::batch_card(ui, batch);
/// # }
/// ```
pub fn batch_card(ui: &mut Ui, batch: &Batch) {
    let progress = batch.progress();
    ui.group(|ui| {
        ui.horizontal(|ui| {
            ui.strong(format!("Pack — {}/{} files", progress.done, progress.files));
            ui.with_layout(egui::Layout::right_to_left(egui::Align::Center), |ui| {
                if ui.button("Cancel all").clicked() {
                    batch.cancel_all();
                }
                if ui.button("Resume all").clicked() {
                    batch.resume_all();
                }
                if ui.button("Pause all").clicked() {
                    batch.pause_all();
                }
            });
        });

        let bar = match progress.fraction() {
            Some(fraction) => {
                let label = match progress.bytes_total {
                    Some(total) => format!(
                        "{} / {}",
                        human_bytes(progress.bytes_received),
                        human_bytes(total)
                    ),
                    None => format!("{}/{} files", progress.settled(), progress.files),
                };
                ProgressBar::new(fraction).text(label)
            }
            None => ProgressBar::new(0.0).animate(true),
        };
        ui.add(bar);

        ui.horizontal(|ui| {
            ui.label(human_speed(progress.speed));
            if progress.failed > 0 {
                ui.colored_label(Color32::RED, format!("{} failed", progress.failed));
            }
        });
    });
}

/// Draws a card for one [`Download`]: title, progress bar, speed, and the
/// relevant control buttons for its current state.
///
/// # Examples
///
/// ```no_run
/// # use async_downloader::{Download, egui_view};
/// # fn ui(ui: &mut egui::Ui, download: &Download) {
/// egui_view::download_card(ui, download);
/// # }
/// ```
pub fn download_card(ui: &mut Ui, download: &Download) {
    let progress = download.progress();
    ui.group(|ui| {
        ui.horizontal(|ui| {
            ui.strong(card_title(download));
            controls(ui, download, &progress);
        });
        body(ui, &progress);
    });
}

fn card_title(download: &Download) -> String {
    download
        .job()
        .dest
        .file_name()
        .map(|name| name.to_string_lossy().into_owned())
        .or_else(|| file_name_from_url(&download.job().url))
        .unwrap_or_else(|| format!("download #{}", download.id()))
}

fn controls(ui: &mut Ui, download: &Download, progress: &Progress) {
    ui.with_layout(egui::Layout::right_to_left(egui::Align::Center), |ui| {
        match progress {
            Progress::Downloading { .. } | Progress::Queued | Progress::Verifying => {
                if ui.button("⏸").on_hover_text("Pause").clicked() {
                    download.pause();
                }
                if ui.button("✖").on_hover_text("Cancel").clicked() {
                    download.cancel();
                }
            }
            Progress::Paused { .. } | Progress::Failed(_) => {
                if ui.button("▶").on_hover_text("Resume").clicked() {
                    download.resume();
                }
                if ui.button("✖").on_hover_text("Cancel").clicked() {
                    download.cancel();
                }
            }
            Progress::Done(_) | Progress::Cancelled => {}
        }
    });
}

fn body(ui: &mut Ui, progress: &Progress) {
    match progress {
        Progress::Queued => {
            ui.label("queued...");
        }
        Progress::Downloading {
            received,
            total,
            speed,
        } => {
            ui.add(progress_bar(*received, *total).animate(total.is_none()));
            ui.label(human_speed(*speed));
        }
        Progress::Paused { received, total } => {
            ui.add(progress_bar(*received, *total));
            ui.colored_label(Color32::YELLOW, "paused");
        }
        Progress::Verifying => {
            ui.horizontal(|ui| {
                ui.spinner();
                ui.label("verifying SHA-256...");
            });
        }
        Progress::Done(outcome) => {
            ui.add(ProgressBar::new(1.0).text(human_bytes(outcome.bytes)));
            match outcome.verified {
                Verification::Ok => {
                    ui.colored_label(Color32::GREEN, "✓ verified");
                }
                Verification::Mismatch => {
                    ui.colored_label(Color32::RED, "✗ SHA-256 mismatch");
                }
                Verification::Skipped => {
                    ui.colored_label(Color32::GRAY, "no expected hash");
                }
            }
            ui.monospace(&outcome.sha256);
        }
        Progress::Failed(error) => {
            ui.colored_label(Color32::RED, format!("failed: {error}"));
        }
        Progress::Cancelled => {
            ui.colored_label(Color32::GRAY, "cancelled");
        }
    }
}

fn progress_bar(received: u64, total: Option<u64>) -> ProgressBar {
    match total {
        Some(total) if total > 0 => ProgressBar::new(received as f32 / total as f32)
            .text(format!("{} / {}", human_bytes(received), human_bytes(total))),
        _ => ProgressBar::new(0.0).text(human_bytes(received)),
    }
}
