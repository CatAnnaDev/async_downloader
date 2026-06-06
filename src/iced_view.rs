use std::hash::{Hash, Hasher};

use futures_util::StreamExt;
use tokio::sync::watch;
use tokio_stream::wrappers::WatchStream;

use crate::{Download, Progress};

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

pub fn subscribe_all<'a, M: 'static>(
    downloads: impl IntoIterator<Item = &'a Download>,
    map: impl Fn(u64, Progress) -> M + Clone + Send + Sync + 'static,
) -> iced::Subscription<M> {
    iced::Subscription::batch(downloads.into_iter().map(|download| {
        let map = map.clone();
        subscription(download).map(move |(id, progress)| map(id, progress))
    }))
}
