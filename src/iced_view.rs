//! Ready-made `iced` subscriptions (enabled by the `iced` feature).
//!
//! These turn a download's [`watch`] channel into an `iced::Subscription`, so
//! the UI redraws on every progress change without polling. Read the latest
//! state in your `view` with [`Download::progress`].

use std::hash::{Hash, Hasher};

use futures_util::StreamExt;
use tokio::sync::watch;
use tokio_stream::wrappers::WatchStream;

use crate::{Download, Progress};

/// Identity + state source for one download's subscription.
///
/// Hashed by id only, so iced keeps the same stream alive across redraws.
#[derive(Clone)]
pub struct Feed {
    id: u64,
    state: watch::Receiver<Progress>,
}

impl Hash for Feed {
    fn hash<H: Hasher>(&self, hasher: &mut H) {
        self.id.hash(hasher);
    }
}

/// A subscription yielding `(download id, progress)` on every change of one
/// [`Download`].
///
/// # Examples
///
/// ```no_run
/// # use async_downloader::{Download, iced_view};
/// # enum Message { Update(u64, async_downloader::Progress) }
/// # fn subscription(download: &Download) -> iced::Subscription<Message> {
/// iced_view::subscription(download).map(|(id, p)| Message::Update(id, p))
/// # }
/// ```
pub fn subscription(download: &Download) -> iced::Subscription<(u64, Progress)> {
    let feed = Feed {
        id: download.id(),
        state: download.watch(),
    };
    iced::Subscription::run_with(feed, build_stream)
}

fn build_stream(feed: &Feed) -> impl futures_util::Stream<Item = (u64, Progress)> + use<> {
    let id = feed.id;
    WatchStream::new(feed.state.clone()).map(move |progress| (id, progress))
}

/// Batches [`subscription`] over many downloads, mapping each update to your
/// message type. Call it from your `subscription` function.
///
/// # Examples
///
/// ```no_run
/// # use async_downloader::{Download, iced_view};
/// # #[derive(Clone)] enum Message { Tick }
/// # fn subscription(downloads: &[Download]) -> iced::Subscription<Message> {
/// iced_view::subscribe_all(downloads, |_id, _progress| Message::Tick)
/// # }
/// ```
pub fn subscribe_all<'a, M: 'static>(
    downloads: impl IntoIterator<Item = &'a Download>,
    map: impl Fn(u64, Progress) -> M + Clone + Send + Sync + 'static,
) -> iced::Subscription<M> {
    iced::Subscription::batch(downloads.into_iter().map(|download| {
        let map = map.clone();
        subscription(download).map(move |(id, progress)| map(id, progress))
    }))
}
