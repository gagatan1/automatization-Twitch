use std::{collections::{HashSet, VecDeque}, sync::Arc, time::Duration};

use rand::RngExt;
use tokio::{sync::watch::Receiver, time::sleep};
use tracing::{debug, warn};
use twitch_gql_rs::TwitchClient;

// Since 2026-10-07 Twitch only credits drop minutes while the stream's HLS media segments
// are being requested, so minute-watched events alone no longer make progress.
// We poll the lowest-quality media playlist and send a HEAD for every new segment (no video is downloaded).

const PLAYLIST_POLL_SECS: u64 = 10;
const RETRY_SECS: u64 = 15;
const SEEN_LIMIT: usize = 64;

pub fn spawn_segment_watcher (client: Arc<TwitchClient>, mut channel_rx: Receiver<String>) {
    tokio::spawn(async move {
        let http = match reqwest::Client::builder().user_agent(client.user_agent.clone()).timeout(Duration::from_secs(20)).build() {
            Ok(http) => http,
            Err(e) => {
                tracing::error!("Failed to build HLS HTTP client: {e}");
                return;
            }
        };
        let login = client.login.clone().unwrap_or_default();

        loop {
            let channel = channel_rx.borrow_and_update().clone();
            if channel.is_empty() {
                if channel_rx.changed().await.is_err() {
                    return;
                }
                continue;
            }

            let playlist_url = match media_playlist_url(&client, &http, &channel).await {
                Ok(url) => url,
                Err(e) => {
                    debug!("HLS playlist unavailable for {channel} ({login}): {e}");
                    tokio::select! {
                        _ = sleep(Duration::from_secs(RETRY_SECS)) => {}
                        res = channel_rx.changed() => if res.is_err() { return; }
                    }
                    continue;
                }
            };

            let mut seen: VecDeque<String> = VecDeque::new();
            let mut seen_set: HashSet<String> = HashSet::new();
            loop {
                if channel_rx.has_changed().unwrap_or(true) {
                    break;
                }

                let playlist = match fetch_text(&http, &playlist_url).await {
                    Ok(text) => text,
                    Err(e) => {
                        debug!("HLS media playlist failed for {channel} ({login}): {e}; refreshing token");
                        break;
                    }
                };

                for segment in playlist.lines().filter(|line| line.starts_with("http")) {
                    if seen_set.contains(segment) {
                        continue;
                    }
                    if let Err(e) = http.head(segment).send().await {
                        debug!("HLS segment HEAD failed for {channel} ({login}): {e}");
                    }
                    seen_set.insert(segment.to_string());
                    seen.push_back(segment.to_string());
                    if seen.len() > SEEN_LIMIT {
                        if let Some(old) = seen.pop_front() {
                            seen_set.remove(&old);
                        }
                    }
                }

                tokio::select! {
                    _ = sleep(Duration::from_secs(PLAYLIST_POLL_SECS)) => {}
                    res = channel_rx.changed() => {
                        if res.is_err() { return; }
                        break;
                    }
                }
            }
        }
    });
}

async fn media_playlist_url (client: &TwitchClient, http: &reqwest::Client, channel: &str) -> Result<String, String> {
    let token = client.get_playback_access_token(channel).await.map_err(|e| e.to_string())?;
    let p = rand::rng().random_range(0..1_000_000u32).to_string();
    let response = http.get(format!("https://usher.ttvnw.net/api/channel/hls/{channel}.m3u8"))
        .query(&[("sig", token.signature.as_str()), ("token", token.value.as_str()), ("allow_source", "true"), ("p", p.as_str())])
        .send().await.map_err(|e| e.to_string())?;
    if !response.status().is_success() {
        return Err(format!("usher returned {}", response.status()));
    }
    let master = response.text().await.map_err(|e| e.to_string())?;
    // Variants are listed from highest to lowest quality; the last one is the cheapest to poll.
    master.lines().filter(|line| line.starts_with("http")).last().map(str::to_string).ok_or_else(|| {
        warn!("HLS master playlist for {channel} has no variants");
        "no variants in master playlist".to_string()
    })
}

async fn fetch_text (http: &reqwest::Client, url: &str) -> Result<String, String> {
    let response = http.get(url).send().await.map_err(|e| e.to_string())?;
    if !response.status().is_success() {
        return Err(format!("status {}", response.status()));
    }
    response.text().await.map_err(|e| e.to_string())
}
