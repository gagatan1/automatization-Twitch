use std::{collections::{BTreeMap, HashMap, HashSet, VecDeque}, error::Error, path::{Path, PathBuf}, sync::Arc, time::Duration};

use indicatif::{MultiProgress, ProgressBar, ProgressStyle};
use rand::{rng, seq::{IndexedRandom, SliceRandom}};
use tokio::{fs::{self}, sync::{Notify, Semaphore, broadcast::{self, Receiver, error::TryRecvError}, watch::Sender}, time::{sleep}};
use tracing::{debug, info, warn};
use tracing_appender::rolling;
use tracing_subscriber::fmt::{time::ChronoLocal, writer::BoxMakeWriter};
use twitch_gql_rs::{TwitchClient, client_type::ClientType, error::ClaimDropError, structs::DropCampaigns};

mod r#static;
mod stream;
mod config;
mod webhook;
mod hls;

use crate::{config::*, r#static::*, stream::*, webhook::{WebhookSendFormat, webhook_message_worker}};

const STREAM_SLEEP: u64 = 59;
const MAX_COUNT: u64 = 3;
const BROADCAST_CHANNEL_CAPACITY: usize = 4096;
const DROP_SYNC_POLL_SECS: u64 = 30;
// 10 polls * 30s = 5 minutes on a channel with no drop session before asking for another channel.
const EMPTY_DROP_POLLS_BEFORE_SWITCH: u32 = 10;
const CLAIM_CHECK_CONCURRENCY: usize = 50;
const CLAIM_CHECK_ROUNDS: u32 = 1;
const CLAIM_CHECK_ROUND_DELAY_SECS: u64 = 0;

async fn create_client(home_dir: &Path, proxies: &[String]) -> Result<(), Box<dyn Error>> {
    // ... (весь код create_client оставлен без изменений — он уже хороший)
    let random_proxy = proxies.choose(&mut rng()).cloned();

    let client_type = ClientType::android_app();
    let mut client = TwitchClient::new(&client_type, &random_proxy).await?;
    let mut count = 0;

    loop {
        count += 1;
        if count >= MAX_COUNT {
            tracing::warn!("Authentication failed: maximum retry attempts ({MAX_COUNT}) reached.");
            return Ok(());
        }
        info!("Starting Twitch device authentication (attempt {count}/{MAX_COUNT})");
        let get_auth = client.request_device_auth().await?;
        println!("To authenticate, open the following URL in your browser:\n{}", get_auth.verification_uri);
        match client.auth(get_auth).await {
            Ok(_) => break,
            Err(twitch_gql_rs::error::AuthError::DeviceTokenExpired) => {
                tracing::warn!("Device authentication token expired. Requesting a new one (attempt {count}/{MAX_COUNT})...");
                continue
            },
            Err(twitch_gql_rs::error::AuthError::TwitchError(e)) => {
                tracing::error!("Twitch returned an error during authentication: {e}");
                return Ok(());
            }
        }
    }
    let path = home_dir.join(format!("{}.json", client.login.clone().unwrap()));
    if !path.exists() {
        client.save_file(&path).await?;
    }
    let client = TwitchClient::load_from_file(&path, &random_proxy).await?;
    let login = client.login.clone().unwrap_or_default();

    let mut accounts = ACCOUNTS.lock().await;
    let already_exists = if let Some(accs) = &*accounts {
        accs.iter().any(|c| c.login.as_ref().map_or(false, |l| l == &login))
    } else {
        false
    };

    if already_exists {
        println!("Account {} has already been added", login);
        return Ok(());
    }

    match &mut *accounts {
        Some(account) => account.push(Arc::new(client.clone())),
        None => *accounts = Some(vec![Arc::new(client.clone())])
    }
    Ok(())
}

#[tokio::main]
async fn main() {
    if let Err(e) = run().await {
        tracing::error!("{e}");
        eprintln!("\nError: {e}");
        // Keep the console window open when the exe was started by double-click.
        eprintln!("Press Enter to exit...");
        let _ = std::io::stdin().read_line(&mut String::new());
    }
}

async fn run() -> Result<(), Box<dyn Error>> {
    // ... (весь main оставлен почти без изменений, только мелкие правки)
    let file_appender = rolling::never(".", "app.log");
    tracing_subscriber::fmt().with_writer(BoxMakeWriter::new(file_appender)).with_ansi(false).with_timer(ChronoLocal::new("%Y-%m-%d %H:%M:%S%.3f".into())).init();
    let home_dir = Path::new("data");
    if !home_dir.exists() {
        fs::create_dir_all(&home_dir).await?;
    }

    let config_path = home_dir.join("config.json");
    if !config_path.exists() {
        let config = Config::new().await?;
        config.save(&config_path).await?;
    }

    let config = Config::load(&config_path).await?;
    config.configure_autostart()?;

    let mut proxies = config.load_proxies_list().await?;
    let mut rng = rng();
    proxies.shuffle(&mut rng);

    let mut proxy_pool = proxies.iter().cycle();
    load_accounts_from_file(&home_dir, &config, &proxies).await?;
    let mut loaded_clients = Vec::new();
    let mut entries = fs::read_dir(&home_dir).await?;
    while let Some(entry) = entries.next_entry().await? {
        let path = entry.path();
        if path.is_file() && path.extension().map_or(false, |s| s == "json") 
           && path.file_name().unwrap_or_default() != "cash.json" 
           && path.file_name().unwrap_or_default() != "config.json" {
            let selected_proxy = proxy_pool.next().cloned();
            let client = TwitchClient::load_from_file(&path, &selected_proxy).await?;
            loaded_clients.push(Arc::new(client));
        }
    }

    if !loaded_clients.is_empty() {
        let mut lock = ACCOUNTS.lock().await;
        *lock = Some(loaded_clients);
    }

    let games = config.loaded_games().await?;

    let items = vec!["Add account", "Start farming", "Check & claim drops"];
    loop {
        let select = dialoguer::Select::new().with_prompt("Select option").items(&items).default(1).interact()?;

        let result: Result<(), Box<dyn Error>> = match select {
            0 => create_client(&home_dir, &proxies).await,
            1 => start_farming(home_dir, &games, &config, &proxies).await,
            2 => claim_check_logic(home_dir).await,
            _ => Ok(()),
        };

        if let Err(e) = result {
            tracing::error!("{e}");
            println!("\nError: {e}\n");
        }
    }
}

async fn start_farming(home_dir: &Path, games: &VecDeque<String>, config: &Config, proxies: &[String]) -> Result<(), Box<dyn Error>> {
    let clients = ACCOUNTS.lock().await;
    let client = if let Some(accounts) = &*clients {
        accounts.first().cloned().ok_or("No accounts loaded")?
    } else {
        return Err("Didn't find accounts".into());
    };
    drop(clients);

    let campaign = client.get_campaign().await?;
    let campaign = campaign.dropCampaigns;

    let mut id_to_index = HashMap::new();
    let mut grouped: BTreeMap<usize, VecDeque<DropCampaigns>> = BTreeMap::new();
    let mut next_index: usize = 0;
    for obj in campaign {
        if obj.status == "EXPIRED" {
            continue;
        }
        let idx = *id_to_index.entry(obj.game.id.clone()).or_insert_with(|| {
            let i = next_index;
            next_index += 1;
            i
        });
        grouped.entry(idx).or_default().push_front(obj);
    }

    main_logic(client, grouped, home_dir, games, config.discord_webhook_url.clone(), proxies).await
}

async fn main_logic(
    client: Arc<TwitchClient>,
    grouped: BTreeMap<usize, VecDeque<DropCampaigns>>,
    home_dir: &Path,
    games: &VecDeque<String>,
    webhook_url: String,
    proxies: &[String],
) -> Result<(), Box<dyn Error>> {
    let mut game_names: Vec<String> = Vec::new();
    let mut game_campaigns: Vec<VecDeque<DropCampaigns>> = Vec::new();
    for campaigns in grouped.values() {
        if let Some(campaign) = campaigns.front() {
            game_names.push(campaign.game.displayName.clone());
            game_campaigns.push(campaigns.clone());
        }
    }

    if game_names.is_empty() {
        return Err("Twitch returned no active drop campaigns for this account".into());
    }

    let mut options = Vec::new();
    if !games.is_empty() {
        options.push(format!("Games from games.txt ({})", games.iter().cloned().collect::<Vec<_>>().join(", ")));
    }
    options.extend(game_names.iter().cloned());

    println!("\n=== Available games for drops ===");
    let choice = dialoguer::Select::new().with_prompt("Select game").items(&options).default(0).interact()?;

    let current_campaigns: VecDeque<VecDeque<DropCampaigns>> = if !games.is_empty() && choice == 0 {
        games.iter().filter_map(|game_name| {
            let campaigns_for_game: VecDeque<_> = grouped.values().flat_map(|campaigns_vec| {
                campaigns_vec.iter().filter(|campaign| {
                    campaign.game.displayName.to_lowercase().trim() == game_name.to_lowercase().trim()
                }).cloned()
            }).collect();
            if campaigns_for_game.is_empty() { None } else { Some(campaigns_for_game) }
        }).collect()
    } else {
        let idx = if games.is_empty() { choice } else { choice - 1 };
        vec![game_campaigns[idx].clone()].into_iter().collect()
    };

    if current_campaigns.is_empty() {
        return Err("None of the games in games.txt has active drops right now. Pick a game from the list instead".into());
    }

    // ... (весь остальной код main_logic остаётся как у тебя, только исправляем опечатку)
    let (webhook_tx, webhook_rx) = tokio::sync::mpsc::channel(100);
    let (drop_id_tx, mut drop_id_rx) = tokio::sync::watch::channel(String::new());

    let notify = Arc::new(Notify::new());
    let drop_campaigns = Arc::new(current_campaigns.clone());
    let drop_cash_dir = home_dir.join("cash.json");

    let clients = ACCOUNTS.lock().await;
    let clients = if let Some(accounts) = clients.clone() {
        accounts
    } else {
        return Err("Didn't find accounts".into());
    };

    let broadcast_capacity = BROADCAST_CHANNEL_CAPACITY.max(clients.len().saturating_mul(2));
    let (channel_tx, channel_rx) = broadcast::channel(broadcast_capacity);
    let channel_rx2 = channel_tx.subscribe();

    let webhook_is_active = if !webhook_url.is_empty() {   // ← исправлено
        webhook_message_worker(webhook_url, webhook_rx, proxies).await;
        true
    } else {
        false
    };

    watch_sync(clients.clone(), channel_rx, notify.clone()).await;
    info!("Watch synchronization task has been successfully initiated");
    drop_sync(clients.clone(), drop_id_tx.clone(), drop_cash_dir.clone(), channel_rx2, notify.clone(), webhook_tx, webhook_is_active).await;
    info!("Drop progress tracker is active");
    filter_streams(client.clone(), drop_campaigns.clone()).await;
    info!("Stream filtering has begun");
    update_stream(channel_tx, notify).await;
    info!("Stream priority updated");

    let mut pending_drops: HashSet<String> = HashSet::new();
    {
        let cash = DROP_CACHE.lock().await.clone();
        for game_campaign in &current_campaigns {
            for campaign in game_campaign {
                let mut campaign_details = retry!(client.get_campaign_details(&campaign.id));
                for (_, claimed_drops) in &cash {
                    for drop_id_cache in claimed_drops {
                        if let Some(pos) = campaign_details.timeBasedDrops.iter().position(|d| d.id == *drop_id_cache) {
                            campaign_details.timeBasedDrops.remove(pos);
                        }
                    }
                }
                for drop in campaign_details.timeBasedDrops {
                    pending_drops.insert(drop.id);
                }
            }
        }
    }

    if pending_drops.is_empty() {
        // Campaign details could not be read (or every drop is already claimed). The watch
        // tasks are already running, so keep farming instead of reporting a false "complete".
        warn!("Could not determine pending drops from campaign details; farming continues until the window is closed");
        std::future::pending::<()>().await;
    }

    while !pending_drops.is_empty() {
        drop_id_rx.changed().await.ok();
        let drop_id = drop_id_rx.borrow().clone();
        if !drop_id.is_empty() && pending_drops.remove(&drop_id) {
            info!("Drop {} farmed (remaining: {})", drop_id, pending_drops.len());
        }
    }

    info!("✅ Farming complete. Use \"Check & claim drops\" in the menu to collect rewards.");
    // The watch/progress tasks spawned above cannot be stopped, so returning to the menu and
    // starting again would run every account twice. Exit instead; restart to claim drops.
    println!("\n✅ Farming complete. Restart the program and choose \"Check & claim drops\" to collect rewards.");
    println!("Press Enter to exit...");
    let _ = std::io::stdin().read_line(&mut String::new());
    std::process::exit(0);
}

async fn watch_sync (clients: Vec<Arc<TwitchClient>>, rx: Receiver<Channel>, notify: Arc<Notify>) {
    for client in clients {
        let mut rx = rx.resubscribe();
        let notify = notify.clone();
        let (segment_tx, segment_rx) = tokio::sync::watch::channel(String::new());
        hls::spawn_segment_watcher(client.clone(), segment_rx);
        tokio::spawn(async move {
            let mut old_stream_name = String::new();
            let mut now_watching_stream: Option<(String, String, String)> = None;

            let mut watching = rx.recv().await.unwrap();
            loop {
                match rx.try_recv() {
                    Err(TryRecvError::Closed) => break,
                    Ok(channel) => {
                        watching = channel;
                        while let Ok(latest) = rx.try_recv() {
                            watching = latest;
                        }
                    }
                    Err(_) => {}
                }

                if old_stream_name.is_empty() || old_stream_name != watching.channel_login {
                    info!("Now actively watching channel {}", watching.channel_login);
                    old_stream_name = watching.channel_login.clone();
                    now_watching_stream = None;
                    let _ = segment_tx.send(watching.channel_login.clone());
                }

                let (stream_id, game_name, game_id) = match &now_watching_stream {
                    Some(s) => s.clone(),
                    None => {
                        let stream_info = retry!(client.get_stream_info(&watching.channel_login));

                        if let Some(stream) = stream_info.stream {
                            let data = (stream.id, stream_info.broadcastSettings.game.name, stream_info.broadcastSettings.game.id);
                            now_watching_stream = Some(data.clone());
                            data
                        } else {
                            debug!("Stream is not live: {}", watching.channel_login);
                            notify.notify_one();
                            sleep(Duration::from_secs(STREAM_SLEEP)).await;
                            continue;
                        }
                    }
                };

                match client.send_watch(&watching.channel_login, &stream_id, &watching.channel_id, Some(&game_name), Some(&game_id)).await {
                    Ok(_) => {
                        sleep(Duration::from_secs(STREAM_SLEEP)).await
                    },
                    Err(e) => {
                        tracing::error!("{e}");
                        sleep(Duration::from_secs(STREAM_SLEEP)).await;
                    }
                }
            }
        });
    }
}

async fn drop_sync(clients: Vec<Arc<TwitchClient>>, tx: Sender<String>, cache_path: PathBuf, rx_watch: broadcast::Receiver<Channel>, notify: Arc<Notify>, webhook_tx: tokio::sync::mpsc::Sender<WebhookSendFormat>, webhook_is_active: bool) {
    load_drop_cache(&cache_path).await.expect("Failed to load drop cache");

    let bars = Arc::new(MultiProgress::new());

    for client in clients {
        let mut rx_watch = rx_watch.resubscribe();
        let notify = notify.clone();
        let tx = tx.clone();
        let webhook_tx = webhook_tx.clone();
        let bars = bars.clone();

        tokio::spawn(async move {
            let mut farmed_drops: HashSet<String> = HashSet::new();
            let mut last_message = String::new();

            let bar = bars.add(ProgressBar::new(1));
            bar.set_style(ProgressStyle::with_template("[{bar:40.cyan/blue}] {percent:.1}% ({pos}/{len} min) {msg}").unwrap());
            bar.set_message("Initialization...");
            bar.enable_steady_tick(Duration::from_millis(500));

            let mut watching = rx_watch.recv().await.unwrap();
            let mut last_drop_id = String::new();
            let mut last_known_drop_id = String::new();
            // Twitch reports no current drop session until a few minute-watched events have landed
            // on a new channel, so an empty dropID right after switching is expected, not a reason to switch again.
            let mut empty_polls: u32 = 0;
            let mut switch_requested = false;
            loop {
                match rx_watch.try_recv() {
                    Err(TryRecvError::Closed) => break,
                    Ok(new_watch) => {
                        let previous_login = watching.channel_login.clone();
                        watching = new_watch;
                        last_drop_id.clear();
                        while let Ok(channel) = rx_watch.try_recv() {
                            watching = channel;
                        }
                        if watching.channel_login != previous_login {
                            empty_polls = 0;
                            switch_requested = false;
                        }
                    }
                    Err(_) => {}
                }

                let Some(drop_progress) = retry_or_log(|| client.get_current_drop_progress_on_channel(&watching.channel_login)).await else {
                    sleep(Duration::from_secs(DROP_SYNC_POLL_SECS)).await;
                    continue;
                };

                // Mark previous drop as farmed when Twitch advances to the next one
                if !drop_progress.dropID.is_empty()
                    && !last_known_drop_id.is_empty()
                    && drop_progress.dropID != last_known_drop_id
                    && !farmed_drops.contains(&last_known_drop_id)
                {
                    info!(
                        "Drop farmed (ID changed {} -> {}): {}",
                        last_known_drop_id,
                        drop_progress.dropID,
                        client.login.as_deref().unwrap_or("unknown")
                    );
                    farmed_drops.insert(last_known_drop_id.clone());
                    let _ = tx.send(last_known_drop_id.clone());
                }

                let drop_ready = !drop_progress.dropID.is_empty()
                    && drop_progress.currentMinutesWatched >= drop_progress.requiredMinutesWatched
                    && !farmed_drops.contains(&drop_progress.dropID);

                if drop_ready {
                    info!(
                        "Drop farmed (ready): {} ({})",
                        drop_progress.dropID,
                        client.login.as_deref().unwrap_or("unknown")
                    );
                    farmed_drops.insert(drop_progress.dropID.clone());
                    let _ = tx.send(drop_progress.dropID.clone());
                }

                if drop_progress.dropID.is_empty()
                    && !last_known_drop_id.is_empty()
                    && !farmed_drops.contains(&last_known_drop_id)
                {
                    info!(
                        "Drop farmed (disappeared): {} ({})",
                        last_known_drop_id,
                        client.login.as_deref().unwrap_or("unknown")
                    );
                    farmed_drops.insert(last_known_drop_id.clone());
                    let _ = tx.send(last_known_drop_id.clone());
                }

                if !drop_progress.dropID.is_empty() {
                    last_known_drop_id = drop_progress.dropID.clone();
                }

                let message = if drop_progress.dropID.is_empty() {
                    "No active drop • waiting..."
                } else if farmed_drops.contains(&drop_progress.dropID)
                    || drop_progress.currentMinutesWatched >= drop_progress.requiredMinutesWatched
                {
                    "Farmed • claim later"
                } else {
                    "Watching"
                };

                if webhook_is_active {
                    if last_message != message {
                        let progress_percent = if drop_progress.requiredMinutesWatched > 0 {
                            ((drop_progress.currentMinutesWatched as f64 / drop_progress.requiredMinutesWatched as f64) * 100.0) as u8
                        } else { 0 };

                        let progress_text = format!("{}m / {}m", drop_progress.currentMinutesWatched, drop_progress.requiredMinutesWatched);

                        let (game_name, game_avatar_url) = if drop_progress.dropID.is_empty() {
                            ("None".to_string(), "None".to_string())
                        } else if let Some(inv) = retry_or_log(|| client.get_inventory()).await {
                            if let Some(found) = inv.inventory.dropCampaignsInProgress.as_ref().and_then(|campaigns| {
                                campaigns.iter().find(|campaign| {
                                    campaign.timeBasedDrops.iter().any(|time_based| {
                                        time_based.id == drop_progress.dropID
                                    })
                                })
                            }) {
                                (found.game.name.clone(), found.imageURL.clone())
                            } else {
                                (drop_progress.game.map(|game| game.displayName).unwrap_or_else(|| "Unknown".to_string()), "None".to_string())
                            }
                        } else {
                            (drop_progress.game.map(|game| game.displayName).unwrap_or_else(|| "Unknown".to_string()), "None".to_string())
                        };

                        let payload = WebhookSendFormat {
                            twitch_name: client.login.clone().unwrap_or("undefined".to_string()),
                            game_name,
                            game_avatar_url,
                            streamer_name: watching.channel_login.clone(),
                            progress_percent,
                            progress_text,
                            status: message.to_string()
                        };
                        let _ = webhook_tx.send(payload).await;
                    }
                }

                last_message = message.to_string();

                let message = format!("{} | {}", client.login.clone().unwrap_or_default(), message);

                if drop_progress.dropID != last_drop_id {
                    last_drop_id = drop_progress.dropID.clone();
                    bar.set_position(0);
                    bar.set_length(drop_progress.requiredMinutesWatched.max(1));
                    bar.set_message(message);
                } else {
                    bar.set_message(message);
                }

                bar.set_length(drop_progress.requiredMinutesWatched.max(1));
                bar.set_position(drop_progress.currentMinutesWatched);

                if drop_progress.dropID.is_empty() {
                    empty_polls += 1;
                } else {
                    empty_polls = 0;
                }

                let channel_exhausted = (drop_progress.dropID.is_empty() && empty_polls >= EMPTY_DROP_POLLS_BEFORE_SWITCH)
                    || (!drop_progress.dropID.is_empty()
                        && drop_progress.currentMinutesWatched >= drop_progress.requiredMinutesWatched);

                if channel_exhausted && !switch_requested {
                    switch_requested = true;
                    debug!(
                        "Farm progress: dropID: {}, currentMinutesWatched: {}, requiredMinutesWatched: {}, farmed: {:?}",
                        drop_progress.dropID,
                        drop_progress.currentMinutesWatched,
                        drop_progress.requiredMinutesWatched,
                        farmed_drops
                    );
                    notify.notify_one();
                }

                sleep(Duration::from_secs(DROP_SYNC_POLL_SECS)).await;
            }
        });
    }
}

fn account_cache_key(client: &Arc<TwitchClient>) -> String {
    client
        .user_id
        .clone()
        .or_else(|| client.login.clone())
        .unwrap_or_default()
}

async fn load_drop_cache(cache_path: &Path) -> Result<(), Box<dyn Error>> {
    if !cache_path.exists() {
        fs::write(cache_path, "{}").await?;
    } else {
        let mut cache = DROP_CACHE.lock().await;
        let cache_str = fs::read_to_string(cache_path).await?;
        let cache_vec: HashMap<String, HashSet<String>> = serde_json::from_str(&cache_str).unwrap_or_default();
        *cache = cache_vec;
    }
    Ok(())
}

async fn persist_drop_cache(cache_path: &Path) {
    let cache = DROP_CACHE.lock().await;
    if let Ok(cache_string) = serde_json::to_string_pretty(&*cache) {
        if let Err(error) = retry_backup(|| fs::write(cache_path, cache_string.as_bytes())).await {
            warn!("Failed to persist drop cache: {error}");
        }
    }
}

async fn verify_account_all_claimable(
    client: &Arc<TwitchClient>,
    cache_path: &Path,
) -> usize {
    let account_key = account_cache_key(client);
    let login = client.login.as_deref().unwrap_or("unknown");
    let mut claimed_drops: HashSet<String> = {
        let cache = DROP_CACHE.lock().await;
        cache.get(&account_key).cloned().unwrap_or_default()
    };

    let Some(inv) = retry_or_log(|| client.get_inventory()).await else {
        warn!("Failed to load inventory for {login}");
        return 0;
    };

    let Some(campaigns_in_progress) = inv.inventory.dropCampaignsInProgress else {
        return 0;
    };

    let mut ready_drops: Vec<(String, String, String)> = Vec::new();
    for in_progress in campaigns_in_progress {
        let game_name = in_progress.game.name.clone();
        for time_based in in_progress.timeBasedDrops {
            if let Some(instance_id) = &time_based.self_drop.dropInstanceID {
                if claimed_drops.contains(&time_based.id) {
                    continue;
                }
                ready_drops.push((time_based.id.clone(), instance_id.clone(), game_name.clone()));
            }
        }
    }

    if ready_drops.is_empty() {
        return 0;
    }

    let mut claimed_count = 0usize;
    let mut cache_updated = false;

    for (drop_id, instance_id, game_name) in ready_drops {
        match client.claim_drop(&instance_id).await {
            Ok(_) => {
                info!("Claimed drop from {game_name} ({login})");
                claimed_drops.insert(drop_id.clone());
                claimed_count += 1;
                cache_updated = true;
            }
            Err(ClaimDropError::DropAlreadyClaimed) => {
                claimed_drops.insert(drop_id);
                cache_updated = true;
            }
            Err(error) => {
                warn!("Failed to claim drop from {game_name} ({login}): {error}");
            }
        }
    }

    if cache_updated {
        let mut cache = DROP_CACHE.lock().await;
        cache.entry(account_key).or_default().extend(claimed_drops);
        drop(cache);
        persist_drop_cache(cache_path).await;
    }

    claimed_count
}

async fn claim_check_logic(home_dir: &Path) -> Result<(), Box<dyn Error>> {
    let clients = {
        let lock = ACCOUNTS.lock().await;
        lock.clone().ok_or("No accounts loaded")?
    };

    if clients.is_empty() {
        return Err("No accounts loaded".into());
    }

    let cache_path = home_dir.join("cash.json");
    load_drop_cache(&cache_path).await?;

    let semaphore = Arc::new(Semaphore::new(CLAIM_CHECK_CONCURRENCY));
    let progress = ProgressBar::new(clients.len() as u64);
    progress.set_style(
        ProgressStyle::with_template("{spinner:.green} [{bar:40.cyan/blue}] {pos}/{len} accounts | {msg}")
            .unwrap(),
    );
    progress.set_message("scanning inventory...");

    println!(
        "\n=== Claim check: scanning {} accounts (all games) ===\n",
        clients.len()
    );

    let mut total_claimed = 0usize;

    let mut handles = Vec::new();
    for client in clients.clone() {
        let semaphore = semaphore.clone();
        let cache_path = cache_path.clone();
        let progress = progress.clone();

        handles.push(tokio::spawn(async move {
            let _permit = semaphore.acquire().await.unwrap();
            let login = client.login.clone().unwrap_or_else(|| "unknown".to_string());
            let count = verify_account_all_claimable(&client, &cache_path).await;
            progress.inc(1);
            progress.set_message(format!("{login}: claimed {count}"));
            count
        }));
    }

    for handle in handles {
        if let Ok(count) = handle.await {
            total_claimed += count;
        }
    }

    if CLAIM_CHECK_ROUNDS > 1 && total_claimed > 0 {
        for round in 2..=CLAIM_CHECK_ROUNDS {
            info!("Claim check round {round}/{CLAIM_CHECK_ROUNDS}");
            if CLAIM_CHECK_ROUND_DELAY_SECS > 0 {
                sleep(Duration::from_secs(CLAIM_CHECK_ROUND_DELAY_SECS)).await;
            }
            progress.set_position(0);
            progress.set_message("retrying...");

            let mut retry_handles = Vec::new();
            for client in clients.clone() {
                let semaphore = semaphore.clone();
                let cache_path = cache_path.clone();
                let progress = progress.clone();

                retry_handles.push(tokio::spawn(async move {
                    let _permit = semaphore.acquire().await.unwrap();
                    let count = verify_account_all_claimable(&client, &cache_path).await;
                    progress.inc(1);
                    count
                }));
            }

            for handle in retry_handles {
                if let Ok(count) = handle.await {
                    total_claimed += count;
                }
            }
        }
    }

    progress.finish_with_message("done");
    println!("\n✅ Claim check complete: {total_claimed} drops claimed across {} accounts", clients.len());

    if total_claimed == 0 {
        println!("No unclaimed drops found in inventory (or drops are not ready yet).");
    }

    Ok(())
}
async fn load_accounts_from_file(home_dir: &Path, config: &Config, proxies: &[String]) -> Result<(), Box<dyn Error>> {
    let accounts = config.loaded_accounts().await?;
    if accounts.is_empty() {
        return Ok(());
    }

    let random_proxy = proxies.choose(&mut rng()).cloned();

    for (login, _password, access_token) in accounts {
        let client_type = ClientType::android_app();
        let mut client = TwitchClient::new(&client_type, &random_proxy).await?;

        // ← Основная магия: заполняем user_id через официальный validate
        client.access_token = Some(access_token.clone());
        client.login = Some(login.clone());

        // Запрашиваем user_id через официальный Twitch endpoint
        let http_client = reqwest::Client::new();
        if let Ok(resp) = http_client
            .get("https://id.twitch.tv/oauth2/validate")
            .header("Authorization", format!("OAuth {}", access_token))
            .send()
            .await
        {
            if let Ok(json) = resp.json::<serde_json::Value>().await {
                if let Some(uid) = json["user_id"].as_str() {
                    client.user_id = Some(uid.to_string());
                }
                if let Some(lg) = json["login"].as_str() {
                    client.login = Some(lg.to_string());
                }
            }
        }

        let path = home_dir.join(format!("{}.json", login));
        if !path.exists() {
            client.save_file(&path).await?;
        }

        let client = TwitchClient::load_from_file(&path, &random_proxy).await?;
        let login = client.login.clone().unwrap_or_default();

        let mut accounts_lock = ACCOUNTS.lock().await;
        let already_exists = if let Some(accs) = &*accounts_lock {
            accs.iter().any(|c| c.login.as_ref().map_or(false, |l| l == &login))
        } else {
            false
        };

        if already_exists {
            continue;
        }

        match &mut *accounts_lock {
            Some(account) => account.push(Arc::new(client.clone())),
            None => *accounts_lock = Some(vec![Arc::new(client.clone())]),
        }
    }
    Ok(())
}