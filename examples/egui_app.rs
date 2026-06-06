use async_downloader::{Batch, Downloader, Job, Settings, Threads, egui_view};
use eframe::egui;

fn main() -> eframe::Result {
    eframe::run_native(
        "async_downloader — egui",
        eframe::NativeOptions::default(),
        Box::new(|cc| Ok(Box::new(App::new(&cc.egui_ctx)))),
    )
}

struct App {
    auto_threads: bool,
    fixed_threads: usize,
    max_concurrent: usize,
    downloader: Downloader,
    urls: String,
    batches: Vec<Batch>,
}

impl App {
    fn new(ctx: &egui::Context) -> Self {
        let auto_threads = true;
        let fixed_threads = 4;
        let max_concurrent = 3;
        Self {
            auto_threads,
            fixed_threads,
            max_concurrent,
            downloader: build_downloader(ctx, auto_threads, fixed_threads, max_concurrent),
            urls: "https://speed.hetzner.de/100MB.bin\nhttps://speed.hetzner.de/100MB.bin\nhttps://ash-speed.hetzner.com/1GB.bin".to_owned(),
            batches: Vec::new(),
        }
    }

    fn rebuild(&mut self, ctx: &egui::Context) {
        self.downloader =
            build_downloader(ctx, self.auto_threads, self.fixed_threads, self.max_concurrent);
        self.batches.clear();
    }

    fn launch(&mut self) {
        let jobs: Vec<Job> = self
            .urls
            .lines()
            .map(str::trim)
            .filter(|line| !line.is_empty())
            .enumerate()
            .map(|(index, url)| {
                let name = async_downloader::file_name_from_url(url)
                    .unwrap_or_else(|| format!("download-{index}.bin"));
                let dest = std::env::temp_dir().join("async_downloader").join(name);
                Job::new(url, dest)
            })
            .collect();

        if !jobs.is_empty() {
            self.batches.push(self.downloader.enqueue_batch(jobs));
        }
    }
}

fn build_downloader(
    ctx: &egui::Context,
    auto: bool,
    fixed: usize,
    max_concurrent: usize,
) -> Downloader {
    let threads = if auto {
        Threads::Auto
    } else {
        Threads::Fixed(fixed)
    };
    Downloader::new(Settings {
        threads,
        max_concurrent,
    })
    .expect("downloader")
    .on_change(egui_view::repaint_notifier(ctx))
}

impl eframe::App for App {
    fn ui(&mut self, ui: &mut egui::Ui, _frame: &mut eframe::Frame) {
        ui.heading("Async downloader");

        let mut rebuild = false;
        ui.group(|ui| {
            ui.label("Runtime");
            ui.horizontal(|ui| {
                rebuild |= ui.radio_value(&mut self.auto_threads, true, "Auto").changed();
                rebuild |= ui.radio_value(&mut self.auto_threads, false, "Fixed").changed();
                ui.add_enabled_ui(!self.auto_threads, |ui| {
                    rebuild |= ui
                        .add(egui::DragValue::new(&mut self.fixed_threads).range(1..=64))
                        .changed();
                });
            });
            ui.horizontal(|ui| {
                ui.label("Max concurrency");
                rebuild |= ui
                    .add(egui::DragValue::new(&mut self.max_concurrent).range(1..=16))
                    .changed();
            });
            ui.label(format!(
                "{} worker threads · {} concurrent downloads",
                self.downloader.worker_threads(),
                self.downloader.max_concurrent()
            ));
        });

        if rebuild {
            let ctx = ui.ctx().clone();
            self.rebuild(&ctx);
        }

        ui.add_space(8.0);
        ui.label("URLs (one per line)");
        ui.add(
            egui::TextEdit::multiline(&mut self.urls)
                .desired_width(420.0)
                .desired_rows(3),
        );
        if ui.button("Enqueue pack").clicked() {
            self.launch();
        }

        ui.separator();
        egui::ScrollArea::vertical().show(ui, |ui| {
            for batch in &self.batches {
                if batch.downloads().len() > 1 {
                    egui_view::batch_card(ui, batch);
                }
                for download in batch.downloads() {
                    egui_view::download_card(ui, download);
                    ui.add_space(4.0);
                }
                ui.add_space(8.0);
            }
        });
    }
}
