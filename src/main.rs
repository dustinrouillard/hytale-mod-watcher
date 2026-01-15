use std::{env, str::FromStr, time::Duration};

use anyhow::{Context, Result, anyhow, bail};
use chrono::{DateTime, NaiveDateTime, TimeZone, Utc};
use dotenvy::dotenv;
use redis::AsyncCommands;
use redis::aio::ConnectionManager;
use reqwest::{Client, Url, header};
use serde::{Deserialize, Serialize};
use tokio::time::{self, MissedTickBehavior};
use tracing::{debug, error, info};
use tracing_subscriber::{EnvFilter, fmt};

const CURSEFORGE_SEARCH_ENDPOINT: &str = "https://api.curseforge.com/v1/mods/search";
const DISCORD_EMBED_DESCRIPTION_LIMIT: usize = 2_048;
const DISCORD_MAX_RETRIES: usize = 5;

#[tokio::main]
async fn main() -> Result<()> {
    dotenv().ok();
    init_tracing();

    let config = Config::from_env()?;
    info!(
        game_id = config.game_id,
        poll_interval_secs = config.poll_interval.as_secs(),
        prefill_on_startup = config.prefill_on_startup,
        sort_field = config.sort_field,
        sort_order = %config.sort_order,
        page_size = config.page_size,
        "starting hytale mod watcher"
    );

    let client = Client::builder()
        .user_agent(format!(
            "hytale-mod-watcher/{} (+https://github.com/example/hytale-mod-watcher)",
            env!("CARGO_PKG_VERSION")
        ))
        .build()
        .context("failed to build HTTP client")?;

    let redis_client =
        redis::Client::open(config.redis_url.as_str()).context("failed to create Redis client")?;
    let mut redis_conn = ConnectionManager::new(redis_client)
        .await
        .context("failed to connect to Redis")?;

    let mut interval = time::interval(config.poll_interval);
    interval.set_missed_tick_behavior(MissedTickBehavior::Delay);

    let mut skip_notifications = config.prefill_on_startup;

    loop {
        interval.tick().await;

        match poll_once(&client, &mut redis_conn, &config, skip_notifications).await {
            Ok(outcome) => {
                info!(
                    newly_seen = outcome.newly_seen,
                    notifications_sent = outcome.notifications_sent,
                    "poll cycle complete"
                );
            }
            Err(err) => {
                error!(error = ?err, "poll cycle failed");
            }
        }

        if skip_notifications {
            info!("prefill complete; subsequent new mods will trigger notifications");
            skip_notifications = false;
        }
    }
}

fn init_tracing() {
    let env_filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));
    let _ = fmt()
        .with_env_filter(env_filter)
        .with_target(false)
        .compact()
        .try_init();
}

#[derive(Debug, Clone)]
struct Config {
    curseforge_api_key: String,
    discord_webhook_url: String,
    discord_username: Option<String>,
    redis_url: String,
    redis_seen_key: String,
    poll_interval: Duration,
    game_id: u32,
    class_id: Option<u32>,
    category_id: Option<u32>,
    sort_field: u8,
    sort_order: SortOrder,
    page_size: u32,
    prefill_on_startup: bool,
}

impl Config {
    fn from_env() -> Result<Self> {
        let curseforge_api_key =
            env::var("CURSEFORGE_API_KEY").context("CURSEFORGE_API_KEY is not set")?;
        let discord_webhook_url =
            env::var("DISCORD_WEBHOOK_URL").context("DISCORD_WEBHOOK_URL is not set")?;

        let discord_username = env::var("DISCORD_USERNAME").ok().and_then(|value| {
            let trimmed = value.trim();
            if trimmed.is_empty() {
                None
            } else {
                Some(trimmed.to_string())
            }
        });

        let redis_url = env::var("REDIS_URL").unwrap_or_else(|_| "redis://127.0.0.1/0".to_string());
        let redis_seen_key = env::var("REDIS_SEEN_KEY")
            .unwrap_or_else(|_| "hytale-mod-watcher:seen-mods".to_string());

        let poll_interval_secs = parse_optional_env::<u64>("POLL_INTERVAL_SECS")?
            .unwrap_or(300)
            .max(10);
        let poll_interval = Duration::from_secs(poll_interval_secs);

        let game_id = parse_optional_env::<u32>("CURSEFORGE_GAME_ID")?.unwrap_or(70_216);
        let class_id = parse_optional_env::<u32>("CURSEFORGE_CLASS_ID")?;
        let category_id = parse_optional_env::<u32>("CURSEFORGE_CATEGORY_ID")?;
        let sort_field = parse_optional_env::<u8>("CURSEFORGE_SORT_FIELD")?.unwrap_or(11);
        let sort_order = SortOrder::try_from_env(env::var("CURSEFORGE_SORT_ORDER").ok())?;
        let page_size = parse_optional_env::<u32>("CURSEFORGE_PAGE_SIZE")?
            .unwrap_or(50)
            .clamp(1, 50);
        let prefill_on_startup = parse_bool_env("PREFILL_ON_STARTUP", true)?;

        Ok(Self {
            curseforge_api_key,
            discord_webhook_url,
            discord_username,
            redis_url,
            redis_seen_key,
            poll_interval,
            game_id,
            class_id,
            category_id,
            sort_field,
            sort_order,
            page_size,
            prefill_on_startup,
        })
    }
}

#[derive(Debug, Clone, Copy)]
enum SortOrder {
    Asc,
    Desc,
}

impl SortOrder {
    fn as_str(self) -> &'static str {
        match self {
            SortOrder::Asc => "asc",
            SortOrder::Desc => "desc",
        }
    }

    fn try_from_env(value: Option<String>) -> Result<Self> {
        match value {
            Some(raw) => {
                let normalized = raw.trim().to_ascii_lowercase();
                if normalized.is_empty() {
                    Ok(SortOrder::Desc)
                } else {
                    match normalized.as_str() {
                        "asc" => Ok(SortOrder::Asc),
                        "desc" => Ok(SortOrder::Desc),
                        other => Err(anyhow!("unsupported CURSEFORGE_SORT_ORDER value '{other}'")),
                    }
                }
            }
            None => Ok(SortOrder::Desc),
        }
    }
}

impl std::fmt::Display for SortOrder {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

fn parse_optional_env<T>(key: &str) -> Result<Option<T>>
where
    T: FromStr,
    T::Err: std::error::Error + Send + Sync + 'static,
{
    match env::var(key) {
        Ok(value) => {
            let trimmed = value.trim();
            if trimmed.is_empty() {
                Ok(None)
            } else {
                trimmed
                    .parse::<T>()
                    .with_context(|| format!("failed to parse {key}"))
                    .map(Some)
            }
        }
        Err(env::VarError::NotPresent) => Ok(None),
        Err(env::VarError::NotUnicode(_)) => Err(anyhow!("{key} contains invalid UTF-8")),
    }
}

fn parse_bool_env(key: &str, default: bool) -> Result<bool> {
    match env::var(key) {
        Ok(value) => {
            let normalized = value.trim().to_ascii_lowercase();
            if normalized.is_empty() {
                Ok(default)
            } else if matches!(normalized.as_str(), "1" | "true" | "yes" | "on") {
                Ok(true)
            } else if matches!(normalized.as_str(), "0" | "false" | "no" | "off") {
                Ok(false)
            } else {
                Err(anyhow!("invalid boolean value '{value}' for {key}"))
            }
        }
        Err(env::VarError::NotPresent) => Ok(default),
        Err(env::VarError::NotUnicode(_)) => Err(anyhow!("{key} contains invalid UTF-8")),
    }
}

#[derive(Debug, Default)]
struct PollOutcome {
    newly_seen: usize,
    notifications_sent: usize,
}

async fn poll_once(
    client: &Client,
    redis_conn: &mut ConnectionManager,
    config: &Config,
    skip_notifications: bool,
) -> Result<PollOutcome> {
    let mods = fetch_mods(client, config).await?;
    debug!(mod_count = mods.len(), "received mods from CurseForge");

    let mut outcome = PollOutcome::default();

    for mod_entry in mods {
        let added_count: i32 = redis_conn
            .sadd(&config.redis_seen_key, mod_entry.id)
            .await
            .context("failed to update Redis seen set")?;
        let was_added = added_count > 0;

        if was_added {
            outcome.newly_seen += 1;

            if skip_notifications {
                debug!(
                    mod_id = mod_entry.id,
                    mod_name = %mod_entry.name,
                    "prefill mode: marked mod as seen"
                );
                continue;
            }

            send_discord_notification(client, config, &mod_entry).await?;
            outcome.notifications_sent += 1;

            info!(
                mod_id = mod_entry.id,
                mod_name = %mod_entry.name,
                "sent discord notification for new mod"
            );
        } else {
            debug!(
                mod_id = mod_entry.id,
                mod_name = %mod_entry.name,
                "mod already seen; skipping"
            );
        }
    }

    Ok(outcome)
}

async fn fetch_mods(client: &Client, config: &Config) -> Result<Vec<Mod>> {
    let mut url = Url::parse(CURSEFORGE_SEARCH_ENDPOINT)
        .context("failed to parse CurseForge search endpoint")?;

    {
        let mut pairs = url.query_pairs_mut();
        pairs.append_pair("gameId", &config.game_id.to_string());
        pairs.append_pair("sortField", &config.sort_field.to_string());
        pairs.append_pair("sortOrder", config.sort_order.as_str());
        pairs.append_pair("pageSize", &config.page_size.to_string());
        if let Some(class_id) = config.class_id {
            pairs.append_pair("classId", &class_id.to_string());
        }
        if let Some(category_id) = config.category_id {
            pairs.append_pair("categoryId", &category_id.to_string());
        }
    }

    let response = client
        .get(url)
        .header("x-api-key", &config.curseforge_api_key)
        .send()
        .await
        .context("failed to send CurseForge search request")?;

    let status = response.status();
    if !status.is_success() {
        let body = response
            .text()
            .await
            .unwrap_or_else(|_| "<unable to read body>".to_string());
        bail!("CurseForge search request failed ({status}): {body}");
    }

    let parsed = response
        .json::<ModsSearchResponse>()
        .await
        .context("failed to deserialize CurseForge search response")?;

    Ok(parsed.data)
}

async fn send_discord_notification(
    client: &Client,
    config: &Config,
    mod_entry: &Mod,
) -> Result<()> {
    let mod_url = build_mod_url(mod_entry);
    let description = sanitize_summary(&mod_entry.summary);

    let mut fields = Vec::new();

    if !mod_entry.authors.is_empty() {
        let authors = mod_entry
            .authors
            .iter()
            .map(|author| author.name.as_str())
            .collect::<Vec<_>>()
            .join(", ");

        fields.push(DiscordEmbedField {
            name: "Author(s)".to_string(),
            value: authors,
            inline: true,
        });
    }

    if let Some(total_downloads) = mod_entry.total_downloads {
        fields.push(DiscordEmbedField {
            name: "Total Downloads".to_string(),
            value: format_number(total_downloads),
            inline: true,
        });
    }

    let release_datetime = mod_entry
        .date_released
        .as_deref()
        .and_then(parse_datetime)
        .or_else(|| mod_entry.date_created.as_deref().and_then(parse_datetime));

    if let Some(released_at) = release_datetime {
        fields.push(DiscordEmbedField {
            name: "Released".to_string(),
            value: released_at.format("%Y-%m-%d %H:%M UTC").to_string(),
            inline: true,
        });
    }

    let embed = DiscordEmbed {
        title: mod_entry.name.clone(),
        url: mod_url,
        description,
        timestamp: release_datetime.map(|dt| dt.to_rfc3339()),
        color: Some(0xf3_77_26),
        fields,
        thumbnail: mod_entry.logo.as_ref().map(|logo| DiscordEmbedThumbnail {
            url: logo.url.clone(),
        }),
    };

    let payload = DiscordWebhookPayload {
        username: config.discord_username.clone(),
        content: None,
        embeds: vec![embed],
        allowed_mentions: DiscordAllowedMentions { parse: Vec::new() },
    };

    let mut attempts = 0usize;

    loop {
        attempts += 1;

        let response = client
            .post(&config.discord_webhook_url)
            .json(&payload)
            .send()
            .await
            .context("failed to send Discord webhook request")?;

        let status = response.status();

        if status.is_success() {
            return Ok(());
        }

        let retry_after_header = response
            .headers()
            .get(header::RETRY_AFTER)
            .and_then(|value| value.to_str().ok())
            .map(str::to_owned);

        let body = response
            .text()
            .await
            .unwrap_or_else(|_| "<unable to read body>".to_string());

        if status.as_u16() == 429 && attempts < DISCORD_MAX_RETRIES {
            let wait = parse_retry_after_delay(retry_after_header.as_deref(), &body);
            debug!(
                attempt = attempts,
                wait_ms = wait.as_millis() as u64,
                "discord rate limited; retrying"
            );
            tokio::time::sleep(wait).await;
            continue;
        }

        bail!("Discord webhook request failed ({status}): {body}");
    }
}

fn build_mod_url(mod_entry: &Mod) -> Option<String> {
    mod_entry
        .links
        .as_ref()
        .and_then(|links| links.website_url.clone())
        .or_else(|| mod_entry.website_url.clone())
        .or_else(|| {
            mod_entry
                .slug
                .as_ref()
                .map(|slug| format!("https://www.curseforge.com/hytale/mods/{slug}"))
        })
}

fn sanitize_summary(summary: &Option<String>) -> Option<String> {
    let text = summary.as_ref()?.trim();
    if text.is_empty() {
        return None;
    }

    let mut result = String::new();
    let mut count = 0;
    for ch in text.chars() {
        if count >= DISCORD_EMBED_DESCRIPTION_LIMIT {
            result.push('…');
            break;
        }
        result.push(ch);
        count += 1;
    }

    Some(result)
}

fn parse_datetime(input: &str) -> Option<DateTime<Utc>> {
    if let Ok(dt) = DateTime::parse_from_rfc3339(input) {
        return Some(dt.with_timezone(&Utc));
    }
    if let Ok(naive) = NaiveDateTime::parse_from_str(input, "%Y-%m-%dT%H:%M:%S") {
        return Some(Utc.from_utc_datetime(&naive));
    }
    if let Ok(naive) = NaiveDateTime::parse_from_str(input, "%Y-%m-%d %H:%M:%S") {
        return Some(Utc.from_utc_datetime(&naive));
    }
    None
}

fn parse_retry_after_delay(header_value: Option<&str>, body: &str) -> Duration {
    if let Some(value) = header_value {
        if let Some(duration) = parse_seconds_str(value) {
            return duration;
        }
    }

    if let Ok(json) = serde_json::from_str::<serde_json::Value>(body) {
        if let Some(seconds) = json.get("retry_after").and_then(|v| v.as_f64()) {
            if let Some(duration) = seconds_to_duration(seconds) {
                return duration;
            }
        }
    }

    Duration::from_millis(500)
}

fn parse_seconds_str(value: &str) -> Option<Duration> {
    value
        .trim()
        .parse::<f64>()
        .ok()
        .and_then(seconds_to_duration)
}

fn seconds_to_duration(seconds: f64) -> Option<Duration> {
    if !seconds.is_finite() || seconds < 0.0 {
        return None;
    }
    let clamped = seconds.clamp(0.1, 120.0);
    Some(Duration::from_secs_f64(clamped))
}

fn format_number(value: u64) -> String {
    let mut digits: Vec<char> = value.to_string().chars().collect();
    let mut i = digits.len();
    while i > 3 {
        i -= 3;
        digits.insert(i, ',');
    }
    digits.into_iter().collect()
}

#[derive(Debug, Deserialize)]
struct ModsSearchResponse {
    data: Vec<Mod>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct Mod {
    id: i64,
    name: String,
    #[serde(default)]
    slug: Option<String>,
    #[serde(default)]
    summary: Option<String>,
    #[serde(default)]
    links: Option<ModLinks>,
    #[serde(default)]
    website_url: Option<String>,
    #[serde(default)]
    date_created: Option<String>,
    #[serde(default)]
    date_modified: Option<String>,
    #[serde(default)]
    date_released: Option<String>,
    #[serde(default)]
    authors: Vec<ModAuthor>,
    #[serde(default)]
    logo: Option<ModLogo>,
    #[serde(default)]
    total_downloads: Option<u64>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct ModLinks {
    #[serde(default)]
    website_url: Option<String>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct ModLogo {
    url: String,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct ModAuthor {
    name: String,
    #[serde(default)]
    url: Option<String>,
}

#[derive(Debug, Serialize)]
struct DiscordWebhookPayload {
    #[serde(skip_serializing_if = "Option::is_none")]
    username: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    content: Option<String>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    embeds: Vec<DiscordEmbed>,
    allowed_mentions: DiscordAllowedMentions,
}

#[derive(Debug, Serialize)]
struct DiscordAllowedMentions {
    #[serde(default)]
    parse: Vec<String>,
}

#[derive(Debug, Serialize)]
struct DiscordEmbed {
    title: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    url: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    description: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    timestamp: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    color: Option<u32>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    fields: Vec<DiscordEmbedField>,
    #[serde(skip_serializing_if = "Option::is_none")]
    thumbnail: Option<DiscordEmbedThumbnail>,
}

#[derive(Debug, Serialize)]
struct DiscordEmbedField {
    name: String,
    value: String,
    inline: bool,
}

#[derive(Debug, Serialize)]
struct DiscordEmbedThumbnail {
    url: String,
}
