use eframe::egui;
use async_downloader::{Downloader, Download, Job, Settings, Threads, egui_view};

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
    url: String,
    expected: String,
    downloads: Vec<Download>,
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
            url: "https://ash-speed.hetzner.com/1GB.bin".to_owned(),
            expected: String::new(),
            downloads: Vec::new(),
        }
    }

    fn rebuild(&mut self, ctx: &egui::Context) {
        self.downloader = build_downloader(
            ctx,
            self.auto_threads,
            self.fixed_threads,
            self.max_concurrent,
        );
        self.downloads.clear();
    }

    fn launch(&mut self) {
        let name = async_downloader::file_name_from_url(&self.url)
            .unwrap_or_else(|| format!("download-{}.bin", self.downloads.len() + 1));
        let dest = std::env::temp_dir().join("async_downloader").join(name);
        let expected = self.expected.trim();
        let mut job = Job::new(self.url.clone(), dest);
        if !expected.is_empty() {
            job = job.with_sha256(expected);
        }
        self.downloads.push(self.downloader.enqueue(job));
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
        ui.horizontal(|ui| {
            ui.label("URL");
            ui.add(egui::TextEdit::singleline(&mut self.url).desired_width(380.0));
        });
        ui.horizontal(|ui| {
            ui.label("Expected SHA-256");
            ui.add(egui::TextEdit::singleline(&mut self.expected).desired_width(380.0));
        });
        if ui.button("Enqueue").clicked() {
            self.launch();
        }

        ui.separator();
        egui::ScrollArea::vertical().show(ui, |ui| {
            for download in &self.downloads {
                egui_view::download_card(ui, download);
                ui.add_space(4.0);
            }
        });
    }
}
