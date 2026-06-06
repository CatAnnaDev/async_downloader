use egui::{Color32, ProgressBar, Ui};

use crate::{Download, Progress, Verification, file_name_from_url, human_bytes, human_speed};

pub fn repaint_notifier(ctx: &egui::Context) -> impl Fn() + Send + Sync + 'static {
    let ctx = ctx.clone();
    move || ctx.request_repaint()
}

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
