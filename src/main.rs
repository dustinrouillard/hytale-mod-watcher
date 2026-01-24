use std::{
    collections::{HashMap, HashSet, hash_map::Entry},
    env,
    str::FromStr,
    time::Duration,
};

use anyhow::{Context, Result, anyhow, bail};
use chrono::{DateTime, NaiveDateTime, TimeZone, Utc};
use dotenvy::dotenv;
use redis::AsyncCommands;
use redis::aio::ConnectionManager;
use reqwest::{Client, Url, header};
use serde::{Deserialize, Serialize};
use sqlx::{FromRow, Pool, Postgres, postgres::PgPoolOptions};
use tokio::time::{self, MissedTickBehavior};
use tracing::{debug, error, info, warn};
use tracing_subscriber::{EnvFilter, fmt};
use uuid::Uuid;

const CURSEFORGE_SEARCH_ENDPOINT: &str = "https://api.curseforge.com/v1/mods/search";
const DISCORD_EMBED_DESCRIPTION_LIMIT: usize = 2_048;
const DISCORD_MAX_RETRIES: usize = 5;

type PgPool = Pool<Postgres>;

#[tokio::main]
async fn main() -> Result<()> {
    dotenv().ok();
    init_tracing();

    let config = Config::from_env()?;
    info!(
        game_id = config.game_id,
        poll_interval_secs = config.poll_interval.as_secs(),
        sort_field = config.sort_field,
        sort_order = %config.sort_order,
        page_size = config.page_size,
        max_pages = config.max_pages,
        "starting hytale mod watcher"
    );

    let http_client = Client::builder()
        .user_agent(format!(
            "hytale-mod-watcher/{} (+https://github.com/dustinrouillard/hytale-mod-watcher)",
            env!("CARGO_PKG_VERSION")
        ))
        .build()
        .context("failed to build HTTP client")?;

    let redis_client =
        redis::Client::open(config.redis_url.as_str()).context("failed to create Redis client")?;
    let mut redis_conn = ConnectionManager::new(redis_client)
        .await
        .context("failed to connect to Redis")?;

    let db_pool = PgPoolOptions::new()
        .max_connections(config.database_max_connections)
        .connect(&config.database_url)
        .await
        .context("failed to connect to PostgreSQL")?;
    info!("connected to PostgreSQL and Redis");

    let mut interval = time::interval(config.poll_interval);
    interval.set_missed_tick_behavior(MissedTickBehavior::Delay);

    loop {
        interval.tick().await;

        match poll_once(&http_client, &mut redis_conn, &db_pool, &config).await {
            Ok(outcome) => {
                info!(
                    fetched_mods = outcome.fetched_mods,
                    new_mods = outcome.new_mods,
                    mod_updates = outcome.mod_updates,
                    notifications_sent = outcome.notifications_sent,
                    "poll cycle complete"
                );
            }
            Err(err) => {
                error!(error = ?err, "poll cycle failed");
            }
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
    database_url: String,
    database_max_connections: u32,
    redis_url: String,
    redis_last_seen_hash_key: String,
    poll_interval: Duration,
    game_id: u32,
    class_id: Option<u32>,
    category_id: Option<u32>,
    sort_field: u8,
    sort_order: SortOrder,
    page_size: u32,
    max_pages: u32,
    discord_username: Option<String>,
    discord_avatar_url: Option<String>,
}

impl Config {
    fn from_env() -> Result<Self> {
        let curseforge_api_key =
            env::var("CURSEFORGE_API_KEY").context("CURSEFORGE_API_KEY is not set")?;
        let database_url = env::var("DATABASE_URL").context("DATABASE_URL is not set")?;
        let database_max_connections = parse_optional_env::<u32>("DATABASE_MAX_CONNECTIONS")?
            .unwrap_or(5)
            .clamp(1, 32);

        let redis_url = env::var("REDIS_URL").unwrap_or_else(|_| "redis://127.0.0.1/0".to_string());
        let redis_last_seen_hash_key = env::var("REDIS_LAST_SEEN_HASH_KEY")
            .unwrap_or_else(|_| "hytale-mod-watcher:mods:last-fingerprint".to_string());

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
        let max_pages = parse_optional_env::<u32>("CURSEFORGE_MAX_PAGES")?
            .unwrap_or(1)
            .clamp(1, 20);

        let discord_username = env::var("DISCORD_USERNAME").ok().and_then(|value| {
            let trimmed = value.trim();
            if trimmed.is_empty() {
                None
            } else {
                Some(trimmed.to_string())
            }
        });

        let discord_avatar_url = env::var("DISCORD_AVATAR_URL").ok().and_then(|value| {
            let trimmed = value.trim();
            if trimmed.is_empty() {
                None
            } else {
                Some(trimmed.to_string())
            }
        });

        Ok(Self {
            curseforge_api_key,
            database_url,
            database_max_connections,
            redis_url,
            redis_last_seen_hash_key,
            poll_interval,
            game_id,
            class_id,
            category_id,
            sort_field,
            sort_order,
            page_size,
            max_pages,
            discord_username,
            discord_avatar_url,
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

#[derive(Debug, Default)]
struct PollOutcome {
    fetched_mods: usize,
    new_mods: usize,
    mod_updates: usize,
    notifications_sent: usize,
}

#[derive(Debug, Clone, Copy)]
enum NotificationKind {
    NewMod,
    ModUpdated,
}

impl NotificationKind {
    fn embed_color(self) -> u32 {
        match self {
            NotificationKind::NewMod => 0xF3_77_26,
            NotificationKind::ModUpdated => 0x24_67_F3,
        }
    }

    fn as_str(self) -> &'static str {
        match self {
            NotificationKind::NewMod => "new_mod",
            NotificationKind::ModUpdated => "mod_update",
        }
    }
}

impl std::fmt::Display for NotificationKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

async fn poll_once(
    client: &Client,
    redis_conn: &mut ConnectionManager,
    db_pool: &PgPool,
    config: &Config,
) -> Result<PollOutcome> {
    let webhook_configs = load_webhook_configs(db_pool).await?;
    if webhook_configs.is_empty() {
        debug!("no webhook configurations found; skipping cycle");
        return Ok(PollOutcome::default());
    }

    let mut watched_ids = HashSet::new();
    let mut legacy_watched_slugs = HashSet::new();

    for cfg in &webhook_configs {
        watched_ids.extend(cfg.included_mod_ids.iter().copied());
        legacy_watched_slugs.extend(cfg.legacy_included_mod_slugs.iter().cloned());
    }

    let mut tracked_mods: HashMap<i64, (Mod, bool)> = HashMap::new();

    let recent_mods = fetch_recent_mods(client, config).await?;
    for mod_entry in recent_mods {
        tracked_mods
            .entry(mod_entry.id)
            .or_insert((mod_entry, false));
    }

    for &mod_id in &watched_ids {
        match fetch_mod_by_id(client, config, mod_id).await? {
            Some(mod_entry) => match tracked_mods.entry(mod_entry.id) {
                Entry::Occupied(mut entry) => {
                    let entry_mut = entry.get_mut();
                    entry_mut.0 = mod_entry;
                    entry_mut.1 = true;
                }
                Entry::Vacant(entry) => {
                    entry.insert((mod_entry, true));
                }
            },
            None => warn!(mod_id = mod_id, "no CurseForge mod found for ID"),
        }
    }

    for slug in &legacy_watched_slugs {
        match fetch_mod_by_slug(client, config, slug).await? {
            Some(mod_entry) => match tracked_mods.entry(mod_entry.id) {
                Entry::Occupied(mut entry) => {
                    let entry_mut = entry.get_mut();
                    entry_mut.0 = mod_entry;
                    entry_mut.1 = true;
                }
                Entry::Vacant(entry) => {
                    entry.insert((mod_entry, true));
                }
            },
            None => warn!(slug = %slug, "no CurseForge mod found for slug"),
        }
    }

    if tracked_mods.is_empty() {
        debug!("no mods fetched this cycle");
        return Ok(PollOutcome::default());
    }

    let mut outcome = PollOutcome {
        fetched_mods: tracked_mods.len(),
        ..Default::default()
    };

    for (mod_id, (mod_entry, watchlist_sourced)) in tracked_mods.into_iter() {
        let mod_key = mod_id.to_string();
        let slug = mod_entry.slug.as_deref();

        let (latest_file, latest_file_error) =
            match resolve_latest_file(client, config, &mod_entry).await {
                Ok(info) => (info, None),
                Err(err) => (None, Some(err)),
            };

        let new_fingerprint = mod_fingerprint(&mod_entry, latest_file.as_ref());
        let legacy_fingerprint = legacy_mod_fingerprint(&mod_entry);

        let previous_fingerprint: Option<String> = redis_conn
            .hget(&config.redis_last_seen_hash_key, &mod_key)
            .await
            .context("failed to read mod fingerprint from Redis")?;

        let event_kind = match previous_fingerprint {
            None => {
                if watchlist_sourced {
                    debug!(
                        mod_id = mod_id,
                        mod_slug = slug.unwrap_or("<unknown>"),
                        "skipping initial notification for newly watched mod"
                    );
                    None
                } else {
                    Some(NotificationKind::NewMod)
                }
            }
            Some(ref prev) if prev != &new_fingerprint && prev != &legacy_fingerprint => {
                Some(NotificationKind::ModUpdated)
            }
            _ => None,
        };

        if let Some(kind) = event_kind {
            if matches!(kind, NotificationKind::ModUpdated)
                && !updates_allowed_for_mod(mod_entry.id, slug, &webhook_configs)
            {
                debug!(
                    mod_id = mod_id,
                    mod_slug = slug.unwrap_or("<unknown>"),
                    "update skipped; mod ID not in any watch list"
                );
                continue;
            }

            if let Some(err) = latest_file_error.as_ref() {
                warn!(
                    mod_id = mod_id,
                    error = ?err,
                    "failed to resolve latest file metadata; continuing without download link"
                );
            }

            match kind {
                NotificationKind::NewMod => outcome.new_mods += 1,
                NotificationKind::ModUpdated => outcome.mod_updates += 1,
            }

            let notifications_sent = dispatch_notifications(
                client,
                config,
                &webhook_configs,
                &mod_entry,
                kind,
                latest_file.as_ref(),
            )
            .await?;

            outcome.notifications_sent += notifications_sent;

            if notifications_sent == 0 {
                debug!(
                    mod_id = mod_id,
                    mod_slug = slug.unwrap_or("<unknown>"),
                    event = %kind,
                    "no webhooks interested in this event"
                );
            }
        }

        let _: () = redis_conn
            .hset(&config.redis_last_seen_hash_key, &mod_key, &new_fingerprint)
            .await
            .context("failed to persist mod fingerprint to Redis")?;
    }

    Ok(outcome)
}

async fn load_webhook_configs(pool: &PgPool) -> Result<Vec<WebhookConfig>> {
    const QUERY: &str = r#"
        SELECT
            id,
            webhook_id,
            webhook_url,
            notify_new_mods,
            notify_updates,
            COALESCE(included_mods, '{}'::text[]) AS included_mods
        FROM webhook_configs
        WHERE notify_new_mods = TRUE OR notify_updates = TRUE
    "#;

    let rows = sqlx::query_as::<_, WebhookConfigRow>(QUERY)
        .fetch_all(pool)
        .await
        .context("failed to fetch webhook configurations")?;

    let configs = rows
        .into_iter()
        .map(WebhookConfig::from_row)
        .collect::<Result<Vec<_>>>()?;

    Ok(configs)
}

async fn dispatch_notifications(
    client: &Client,
    config: &Config,
    webhook_configs: &[WebhookConfig],
    mod_entry: &Mod,
    kind: NotificationKind,
    latest_file: Option<&LatestFileInfo>,
) -> Result<usize> {
    let slug = mod_entry.slug.as_deref();
    let interested_configs = webhook_configs
        .iter()
        .filter(|cfg| cfg.is_interested_in(kind, mod_entry.id, slug))
        .collect::<Vec<_>>();

    if interested_configs.is_empty() {
        return Ok(0);
    }

    let mut sent = 0usize;

    for cfg in interested_configs {
        send_discord_notification(client, config, cfg, mod_entry, kind, latest_file).await?;
        sent += 1;
    }

    info!(
        mod_id = mod_entry.id,
        mod_slug = mod_entry.slug.as_deref().unwrap_or("<unknown>"),
        event = %kind,
        sent_to = sent,
        "dispatched notifications"
    );

    Ok(sent)
}

async fn fetch_recent_mods(client: &Client, config: &Config) -> Result<Vec<Mod>> {
    let mut collected = Vec::new();

    for page_index in 0..config.max_pages {
        let index = page_index * config.page_size;
        let mut url = Url::parse(CURSEFORGE_SEARCH_ENDPOINT)
            .context("failed to parse CurseForge search endpoint")?;

        {
            let mut pairs = url.query_pairs_mut();
            pairs.append_pair("gameId", &config.game_id.to_string());
            pairs.append_pair("sortField", &config.sort_field.to_string());
            pairs.append_pair("sortOrder", config.sort_order.as_str());
            pairs.append_pair("pageSize", &config.page_size.to_string());
            pairs.append_pair("index", &index.to_string());

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

        if parsed.data.is_empty() {
            break;
        }

        collected.extend(parsed.data);
    }

    Ok(collected)
}

async fn fetch_mod_by_id(client: &Client, config: &Config, mod_id: i64) -> Result<Option<Mod>> {
    let url = Url::parse(&format!("https://api.curseforge.com/v1/mods/{mod_id}"))
        .context("failed to parse CurseForge mod endpoint")?;

    let response = client
        .get(url)
        .header("x-api-key", &config.curseforge_api_key)
        .send()
        .await
        .with_context(|| format!("failed to send CurseForge mod request for ID {mod_id}"))?;

    let status = response.status();
    if status.as_u16() == 404 {
        return Ok(None);
    }
    if !status.is_success() {
        let body = response
            .text()
            .await
            .unwrap_or_else(|_| "<unable to read body>".to_string());
        bail!("CurseForge mod request for ID {mod_id} failed ({status}): {body}");
    }

    let parsed = response
        .json::<ModResponse>()
        .await
        .context("failed to deserialize CurseForge mod response")?;

    Ok(Some(parsed.data))
}

async fn fetch_mod_by_slug(client: &Client, config: &Config, slug: &str) -> Result<Option<Mod>> {
    let mut url = Url::parse(CURSEFORGE_SEARCH_ENDPOINT)
        .context("failed to parse CurseForge search endpoint")?;

    {
        let mut pairs = url.query_pairs_mut();
        pairs.append_pair("gameId", &config.game_id.to_string());
        pairs.append_pair("slug", slug);
        pairs.append_pair("pageSize", "1");
    }

    let response = client
        .get(url)
        .header("x-api-key", &config.curseforge_api_key)
        .send()
        .await
        .context("failed to send CurseForge slug search request")?;

    let status = response.status();
    if !status.is_success() {
        let body = response
            .text()
            .await
            .unwrap_or_else(|_| "<unable to read body>".to_string());
        bail!("CurseForge slug search failed ({status}): {body}");
    }

    let parsed = response
        .json::<ModsSearchResponse>()
        .await
        .context("failed to deserialize slug search response")?;

    Ok(parsed.data.into_iter().find(|mod_entry| {
        mod_entry
            .slug
            .as_deref()
            .map(|value| value == slug)
            .unwrap_or(false)
    }))
}

async fn send_discord_notification(
    client: &Client,
    config: &Config,
    webhook: &WebhookConfig,
    mod_entry: &Mod,
    kind: NotificationKind,
    latest_file: Option<&LatestFileInfo>,
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

    if let Some(file_info) = latest_file {
        if let Some(version) = file_info.version.as_ref() {
            fields.push(DiscordEmbedField {
                name: "Version".to_string(),
                value: version.clone(),
                inline: true,
            });
        }
        if let Some(download_url) = file_info.download_url.as_ref() {
            fields.push(DiscordEmbedField {
                name: "Download".to_string(),
                value: format!("[Direct link]({download_url})"),
                inline: true,
            });
        }
    }

    match kind {
        NotificationKind::NewMod => {
            if let Some(released_at) = mod_entry
                .date_released
                .as_deref()
                .and_then(parse_datetime)
                .or_else(|| mod_entry.date_created.as_deref().and_then(parse_datetime))
            {
                fields.push(DiscordEmbedField {
                    name: "Released".to_string(),
                    value: released_at.format("%Y-%m-%d %H:%M UTC").to_string(),
                    inline: true,
                });
            }
        }
        NotificationKind::ModUpdated => {
            if let Some(updated_at) = mod_entry.date_modified.as_deref().and_then(parse_datetime) {
                fields.push(DiscordEmbedField {
                    name: "Updated".to_string(),
                    value: updated_at.format("%Y-%m-%d %H:%M UTC").to_string(),
                    inline: true,
                });
            }
        }
    }

    let timestamp = mod_event_timestamp(mod_entry, kind).map(|dt| dt.to_rfc3339());

    let embed = DiscordEmbed {
        title: match kind {
            NotificationKind::NewMod => format!("New mod: {}", mod_entry.name),
            NotificationKind::ModUpdated => format!("Updated mod: {}", mod_entry.name),
        },
        url: mod_url,
        description,
        timestamp,
        color: Some(kind.embed_color()),
        fields,
        thumbnail: mod_entry.logo.as_ref().map(|logo| DiscordEmbedThumbnail {
            url: logo.url.clone(),
        }),
    };

    let payload = DiscordWebhookPayload {
        username: config.discord_username.clone(),
        avatar_url: config.discord_avatar_url.clone(),
        content: None,
        embeds: vec![embed],
        allowed_mentions: DiscordAllowedMentions { parse: Vec::new() },
    };

    let mut attempts = 0usize;

    loop {
        attempts += 1;

        let response = client
            .post(&webhook.webhook_url)
            .json(&payload)
            .send()
            .await
            .with_context(|| {
                format!(
                    "failed to send Discord webhook request to {}",
                    webhook.webhook_url
                )
            })?;

        let status = response.status();

        if status.is_success() {
            debug!(
                attempt = attempts,
                webhook_uuid = %webhook.id,
                webhook_discord_id = %webhook.webhook_id,
                "discord webhook delivered successfully"
            );
            break;
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
                webhook_uuid = %webhook.id,
                webhook_discord_id = %webhook.webhook_id,
                "discord rate limited; retrying"
            );
            tokio::time::sleep(wait).await;
            continue;
        }

        warn!(
            attempt = attempts,
            status = %status,
            webhook_uuid = %webhook.id,
            webhook_discord_id = %webhook.webhook_id,
            body = %body,
            "discord webhook request failed"
        );
        bail!(
            "Discord webhook request to {} failed after {attempts} attempts ({status}): {body}",
            webhook.webhook_url
        );
    }

    Ok(())
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

fn mod_event_timestamp(mod_entry: &Mod, kind: NotificationKind) -> Option<DateTime<Utc>> {
    match kind {
        NotificationKind::NewMod => mod_entry
            .date_created
            .as_deref()
            .and_then(parse_datetime)
            .or_else(|| mod_entry.date_released.as_deref().and_then(parse_datetime))
            .or_else(|| mod_entry.date_modified.as_deref().and_then(parse_datetime)),
        NotificationKind::ModUpdated => mod_entry
            .date_modified
            .as_deref()
            .and_then(parse_datetime)
            .or_else(|| mod_entry.date_released.as_deref().and_then(parse_datetime))
            .or_else(|| mod_entry.date_created.as_deref().and_then(parse_datetime)),
    }
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

fn best_mod_file<'a>(files: &'a [ModFile]) -> Option<&'a ModFile> {
    let mut candidates: Vec<&ModFile> = files
        .iter()
        .filter(|file| {
            file.download_url
                .as_ref()
                .map(|url| !url.is_empty())
                .unwrap_or(false)
        })
        .collect();

    if candidates.is_empty() {
        return None;
    }

    let jar_candidates: Vec<&ModFile> = candidates
        .iter()
        .copied()
        .filter(|file| {
            file.download_url
                .as_deref()
                .map(|url| url.ends_with(".jar"))
                .unwrap_or(false)
        })
        .collect();

    if !jar_candidates.is_empty() {
        candidates = jar_candidates;
    }

    candidates.into_iter().max_by(|a, b| {
        let a_date = a.file_date.as_deref().and_then(parse_datetime);
        let b_date = b.file_date.as_deref().and_then(parse_datetime);
        a_date.cmp(&b_date)
    })
}

fn non_empty_string(input: &str) -> Option<String> {
    let trimmed = input.trim();
    if trimmed.is_empty() {
        None
    } else {
        Some(trimmed.to_owned())
    }
}

fn legacy_mod_fingerprint(mod_entry: &Mod) -> String {
    mod_entry
        .date_modified
        .as_deref()
        .or_else(|| mod_entry.date_released.as_deref())
        .or_else(|| mod_entry.date_created.as_deref())
        .map(|value| value.to_owned())
        .unwrap_or_else(|| format!("id:{}:v1", mod_entry.id))
}

fn mod_fingerprint(mod_entry: &Mod, latest_file: Option<&LatestFileInfo>) -> String {
    let summary = sanitize_summary(&mod_entry.summary);

    let mut authors: Vec<String> = mod_entry
        .authors
        .iter()
        .map(|author| author.name.trim().to_owned())
        .filter(|name| !name.is_empty())
        .collect();
    authors.sort();
    authors.dedup();

    let best_file = match latest_file {
        Some(file) => {
            if file.version.is_some()
                || file
                    .file_name
                    .as_ref()
                    .map(|name| !name.is_empty())
                    .unwrap_or(false)
                || file
                    .display_name
                    .as_ref()
                    .map(|name| !name.is_empty())
                    .unwrap_or(false)
                || file
                    .download_url
                    .as_ref()
                    .map(|url| !url.is_empty())
                    .unwrap_or(false)
                || file.file_date.is_some()
                || file.file_id.is_some()
            {
                Some(serde_json::json!({
                    "id": file.file_id,
                    "version": file.version.clone(),
                    "file_name": file.file_name.clone(),
                    "display_name": file.display_name.clone(),
                    "download_url": file.download_url.clone(),
                    "file_date": file.file_date.clone(),
                }))
            } else {
                None
            }
        }
        None => best_mod_file(&mod_entry.latest_files).map(|file| {
            serde_json::json!({
                "id": if file.id > 0 { Some(file.id) } else { None },
                "display_name": non_empty_string(&file.display_name),
                "file_name": non_empty_string(&file.file_name),
                "download_url": file.download_url.clone(),
                "file_date": file.file_date.clone(),
            })
        }),
    };

    serde_json::json!({
        "v": 2,
        "id": mod_entry.id,
        "name": mod_entry.name.trim(),
        "slug": mod_entry.slug.clone(),
        "website_url": mod_entry.website_url.clone(),
        "links_website_url": mod_entry.links.as_ref().and_then(|links| links.website_url.clone()),
        "logo_url": mod_entry.logo.as_ref().map(|logo| logo.url.clone()),
        "summary": summary,
        "authors": authors,
        "total_downloads": mod_entry.total_downloads,
        "date_created": mod_entry.date_created.clone(),
        "date_modified": mod_entry.date_modified.clone(),
        "date_released": mod_entry.date_released.clone(),
        "best_file": best_file,
    })
    .to_string()
}

async fn resolve_latest_file(
    client: &Client,
    config: &Config,
    mod_entry: &Mod,
) -> Result<Option<LatestFileInfo>> {
    if let Some(info) = select_latest_file(&mod_entry.latest_files) {
        return Ok(Some(info));
    }

    let fetched_files = fetch_latest_files_from_api(client, config, mod_entry.id).await?;
    Ok(select_latest_file(&fetched_files))
}

fn select_latest_file(files: &[ModFile]) -> Option<LatestFileInfo> {
    let best = best_mod_file(files)?;

    let display_name = non_empty_string(&best.display_name);
    let file_name = non_empty_string(&best.file_name);
    let version = display_name.clone().or_else(|| file_name.clone());

    Some(LatestFileInfo {
        file_id: if best.id > 0 { Some(best.id) } else { None },
        version,
        file_name,
        display_name,
        download_url: best.download_url.clone(),
        file_date: best.file_date.clone(),
    })
}

async fn fetch_latest_files_from_api(
    client: &Client,
    config: &Config,
    mod_id: i64,
) -> Result<Vec<ModFile>> {
    let mut url = Url::parse(&format!(
        "https://api.curseforge.com/v1/mods/{mod_id}/files"
    ))
    .context("failed to parse CurseForge files endpoint")?;

    {
        let mut pairs = url.query_pairs_mut();
        pairs.append_pair("pageSize", "20");
        pairs.append_pair("sortOrder", config.sort_order.as_str());
    }

    let response = client
        .get(url)
        .header("x-api-key", &config.curseforge_api_key)
        .send()
        .await
        .context("failed to send CurseForge files request")?;

    let status = response.status();
    if !status.is_success() {
        let body = response
            .text()
            .await
            .unwrap_or_else(|_| "<unable to read body>".to_string());
        bail!("CurseForge files request failed ({status}): {body}");
    }

    let parsed = response
        .json::<FilesResponse>()
        .await
        .context("failed to deserialize CurseForge files response")?;

    Ok(parsed.data)
}

#[derive(Debug, Clone)]
struct LatestFileInfo {
    file_id: Option<i64>,
    version: Option<String>,
    file_name: Option<String>,
    display_name: Option<String>,
    download_url: Option<String>,
    file_date: Option<String>,
}

#[derive(Debug, Deserialize)]
struct FilesResponse {
    data: Vec<ModFile>,
}

#[derive(Debug, Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
struct ModFile {
    #[serde(default)]
    id: i64,
    #[serde(default)]
    display_name: String,
    #[serde(default)]
    file_name: String,
    #[serde(default)]
    download_url: Option<String>,
    #[serde(default)]
    file_date: Option<String>,
}

#[derive(Debug)]
struct WebhookConfig {
    id: Uuid,
    webhook_id: String,
    webhook_url: String,
    notify_new_mods: bool,
    notify_updates: bool,
    included_mod_ids: HashSet<i64>,
    legacy_included_mod_slugs: HashSet<String>,
}

#[derive(Debug, FromRow)]
struct WebhookConfigRow {
    id: Uuid,
    webhook_id: String,
    webhook_url: String,
    notify_new_mods: bool,
    notify_updates: bool,
    included_mods: Vec<String>,
}

impl WebhookConfig {
    fn from_row(row: WebhookConfigRow) -> Result<Self> {
        let mut included_ids = HashSet::new();
        let mut legacy_slugs = HashSet::new();

        for value in row.included_mods {
            let trimmed = value.trim();
            if trimmed.is_empty() {
                continue;
            }

            if let Ok(id) = trimmed.parse::<i64>() {
                included_ids.insert(id);
            } else {
                legacy_slugs.insert(trimmed.to_string());
            }
        }

        if !legacy_slugs.is_empty() {
            warn!(
                webhook_uuid = %row.id,
                slug_filters = legacy_slugs.len(),
                "webhook configuration still uses slug filters; please migrate to CurseForge mod IDs"
            );
        }

        Ok(Self {
            id: row.id,
            webhook_id: row.webhook_id,
            webhook_url: row.webhook_url,
            notify_new_mods: row.notify_new_mods,
            notify_updates: row.notify_updates,
            included_mod_ids: included_ids,
            legacy_included_mod_slugs: legacy_slugs,
        })
    }

    fn includes_all(&self) -> bool {
        self.included_mod_ids.is_empty() && self.legacy_included_mod_slugs.is_empty()
    }

    fn is_interested_in(&self, kind: NotificationKind, mod_id: i64, slug: Option<&str>) -> bool {
        let event_enabled = match kind {
            NotificationKind::NewMod => self.notify_new_mods,
            NotificationKind::ModUpdated => self.notify_updates,
        };

        if !event_enabled {
            return false;
        }

        if matches!(kind, NotificationKind::NewMod) {
            return true;
        }

        if self.includes_all() {
            return true;
        }

        if self.included_mod_ids.contains(&mod_id) {
            return true;
        }

        if let Some(slug_value) = slug {
            if self.legacy_included_mod_slugs.contains(slug_value) {
                return true;
            }
        }

        false
    }
}

fn updates_allowed_for_mod(mod_id: i64, slug: Option<&str>, configs: &[WebhookConfig]) -> bool {
    if configs
        .iter()
        .any(|cfg| cfg.notify_updates && cfg.includes_all())
    {
        return true;
    }

    configs.iter().any(|cfg| {
        if !cfg.notify_updates {
            return false;
        }

        if cfg.included_mod_ids.contains(&mod_id) {
            return true;
        }

        if let Some(slug_value) = slug {
            if cfg.legacy_included_mod_slugs.contains(slug_value) {
                return true;
            }
        }

        false
    })
}

#[derive(Debug, Deserialize)]
struct ModResponse {
    data: Mod,
}

#[derive(Debug, Deserialize)]
struct ModsSearchResponse {
    data: Vec<Mod>,
}

#[derive(Debug, Deserialize, Clone)]
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
    latest_files: Vec<ModFile>,
    #[serde(default)]
    total_downloads: Option<u64>,
}

#[derive(Debug, Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
struct ModLinks {
    #[serde(default)]
    website_url: Option<String>,
}

#[derive(Debug, Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
struct ModLogo {
    url: String,
}

#[derive(Debug, Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
#[allow(dead_code)]
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
    avatar_url: Option<String>,
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
