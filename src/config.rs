use std::{collections::VecDeque, env, error::Error, path::Path};

use auto_launch::AutoLaunchBuilder;
use serde::{Deserialize, Serialize};
use tokio::{fs::{self, File}, io::{AsyncBufReadExt, BufReader, Lines}};
use tracing::error;

async fn open_lines(path: &str) -> Result<Lines<BufReader<File>>, Box<dyn Error>> {
    let path = Path::new(path);
    let file = match fs::File::open(path).await {
        Ok(f) => f,
        Err(e) => {
            error!("Failed to read file at: '{}'", path.display());
            return Err(e.into());
        }
    };
    let reader = BufReader::new(file).lines();
    Ok(reader)
}

fn default_games_path() -> String { "./lists/games.txt".to_string() }
fn default_proxies_path() -> String { "./lists/proxies.txt".to_string() }
fn default_accounts_path() -> String { "./lists/accounts.txt".to_string() }

#[derive(Debug, Serialize, Deserialize)]
pub struct Config {
    #[serde(default = "default_games_path")]
    games_path: String,
    #[serde(default)]
    autostart: bool,
    #[serde(default = "default_proxies_path")]
    proxies_path: String,
    #[serde(default = "default_accounts_path")]
    accounts_path: String,
    #[serde(default)]
    pub discord_webhook_url: String,
}

impl Config {
    pub fn configure_autostart(&self) -> Result<(), Box<dyn Error>> {
        let app_path = {
            let path = env::current_exe()?;
            path.to_str().ok_or("Unable to convert executable path to string")?.to_string()
        };
        let auto = AutoLaunchBuilder::new()
            .set_app_name("TwitchDropSentry")
            .set_app_path(&app_path)
            .set_macos_launch_mode(auto_launch::MacOSLaunchMode::LaunchAgent)
            .set_linux_launch_mode(auto_launch::LinuxLaunchMode::XdgAutostart)
            .set_windows_enable_mode(auto_launch::WindowsEnableMode::Dynamic)
            .build()?;

        if self.autostart {
            if !auto.is_enabled()? {
                auto.enable()?;
            }
        } else {
            auto.disable()?;
        }
        Ok(())
    }

    pub async fn new() -> Result<Self, Box<dyn Error>> {
        let lists_path = Path::new("lists");
        if !lists_path.exists() {
            fs::create_dir(&lists_path).await?;
        }

        for file in ["games.txt", "proxies.txt", "accounts.txt"] {
            let p = lists_path.join(file);
            if !p.exists() {
                fs::write(&p, "").await?;
            }
        }

        Ok(Config {
            games_path: default_games_path(),
            autostart: false,
            proxies_path: default_proxies_path(),
            accounts_path: default_accounts_path(),
            discord_webhook_url: String::new(),
        })
    }

    pub async fn save(&self, path: &Path) -> Result<(), Box<dyn Error>> {
        let to_write = serde_json::to_string_pretty(&self)?;
        fs::write(path, to_write).await?;
        Ok(())
    }

    pub async fn load(path: &Path) -> Result<Self, Box<dyn Error>> {
        let read = fs::read_to_string(path).await?;
        let mut config: Config = serde_json::from_str(&read)?;

        // Авто-миграция старых конфигов
        config.save(path).await?;

        Ok(config)
    }

    pub async fn load_proxies_list(&self) -> Result<Vec<String>, Box<dyn Error>> {
        let mut reader = open_lines(&self.proxies_path).await?;
        let mut proxies = Vec::new();
        while let Some(line) = reader.next_line().await? {
            let trimmed = line.trim();
            if !trimmed.is_empty() {
                proxies.push(trimmed.to_string());
            }
        }
        Ok(proxies)
    }

    pub async fn loaded_games(&self) -> Result<VecDeque<String>, Box<dyn Error>> {
        let mut reader = open_lines(&self.games_path).await?;
        let mut games = VecDeque::new();
        while let Some(line) = reader.next_line().await? {
            let trimmed = line.trim().trim_start_matches('\u{feff}');
            if !trimmed.is_empty() {
                games.push_back(trimmed.to_string());
            }
        }
        Ok(games)
    }

    pub async fn loaded_accounts(&self) -> Result<Vec<(String, String, String)>, Box<dyn Error>> {
        let mut reader = open_lines(&self.accounts_path).await?;
        let mut accounts = Vec::new();
        while let Some(line) = reader.next_line().await? {
            let trimmed = line.trim().trim_start_matches('\u{feff}');
            if !trimmed.is_empty() {
                let parts: Vec<&str> = trimmed.split(':').collect();
                if parts.len() >= 3 {
                    accounts.push((
                        parts[0].to_string(),
                        parts[1].to_string(),
                        parts[2].to_string(),
                    ));
                }
            }
        }
        Ok(accounts)
    }
}