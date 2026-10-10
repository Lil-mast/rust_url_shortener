use async_graphql::{EmptySubscription, InputObject, Object, Schema, SimpleObject};
use async_graphql_axum::{GraphQLRequest, GraphQLResponse};
use axum::{
    extract::{Path, State},
    http::StatusCode,
    response::{IntoResponse, Redirect, Response},
    routing::get,
    Router,
};
use futures::StreamExt;
use lapin::{
    options::{BasicAckOptions, BasicConsumeOptions, BasicPublishOptions, QueueDeclareOptions},
    publisher_confirm::Confirmation,
    types::FieldTable,
    BasicProperties, Channel, Connection, ConnectionProperties,
};
use redis::AsyncCommands;
use reqwest::Client;
use serde::{Deserialize, Serialize};
use sqlx::{postgres::PgPoolOptions, PgPool, Row};
use std::{collections::HashSet, env, sync::Arc, time::Duration};
use tokio::{net::TcpListener, time::sleep};
use tracing::{error, info, warn};
use url::Url;
use uuid::Uuid;

#[derive(Clone)]
struct AppState {
    db: PgPool,
    redis: redis::Client,
    cache_enabled: bool,
    http: Client,
    elasticsearch_url: String,
    schema: AppSchema,
}

#[derive(Clone, SimpleObject, sqlx::FromRow, Serialize, Deserialize)]
struct UrlEntry {
    id: Uuid,
    short_code: String,
    long_url: String,
}

#[derive(Clone, SimpleObject, sqlx::FromRow, Serialize, Deserialize)]
struct SearchResult {
    short_code: String,
    long_url: String,
    domain: String,
    created_at: String,
    click_count: i64,
}

#[derive(InputObject)]
struct UrlSearchFilter {
    domain: Option<String>,
    created_after: Option<String>,
    created_before: Option<String>,
    min_clicks: Option<i64>,
}

#[derive(Serialize, Deserialize)]
struct OutboxEvent {
    kind: String,
    url_id: Uuid,
}

type AppSchema = Schema<QueryRoot, MutationRoot, EmptySubscription>;

struct QueryRoot;

#[Object]
impl QueryRoot {
    async fn get_url(
        &self,
        ctx: &async_graphql::Context<'_>,
        short_code: String,
    ) -> async_graphql::Result<Option<UrlEntry>> {
        let state = ctx.data::<Arc<AppState>>()?;
        lookup_url(state, &short_code).await.map_err(Into::into)
    }

    async fn search_urls(
        &self,
        ctx: &async_graphql::Context<'_>,
        filter: UrlSearchFilter,
        limit: Option<i32>,
    ) -> async_graphql::Result<Vec<SearchResult>> {
        let state = ctx.data::<Arc<AppState>>()?;
        let mut filters = Vec::new();
        if let Some(domain) = filter.domain {
            filters.push(serde_json::json!({"term": {"domain": domain.to_lowercase()}}));
        }
        if let Some(after) = filter.created_after {
            filters.push(serde_json::json!({"range": {"created_at": {"gte": after}}}));
        }
        if let Some(before) = filter.created_before {
            filters.push(serde_json::json!({"range": {"created_at": {"lte": before}}}));
        }
        if let Some(clicks) = filter.min_clicks {
            filters.push(serde_json::json!({"range": {"click_count": {"gte": clicks}}}));
        }

        let response = state
            .http
            .post(format!("{}/urls/_search", state.elasticsearch_url))
            .json(&serde_json::json!({
                "size": limit.unwrap_or(25).clamp(1, 100),
                "query": if filters.is_empty() { serde_json::json!({"match_all": {}}) }
                    else { serde_json::json!({"bool": {"filter": filters}}) },
                "sort": [{"created_at": {"order": "desc"}}]
            }))
            .send()
            .await?;
        if !response.status().is_success() {
            return Err(async_graphql::Error::new(format!(
                "Elasticsearch search failed: {}",
                response.status()
            )));
        }
        let body: serde_json::Value = response.json().await?;
        let hits = body
            .pointer("/hits/hits")
            .and_then(|v| v.as_array())
            .cloned()
            .unwrap_or_default();
        let mut results = Vec::with_capacity(hits.len());
        for hit in hits {
            if let Some(source) = hit.get("_source") {
                if let Ok(result) = serde_json::from_value(source.clone()) {
                    results.push(result);
                }
            }
        }
        Ok(results)
    }
}

struct MutationRoot;

#[Object]
impl MutationRoot {
    async fn create_url(
        &self,
        ctx: &async_graphql::Context<'_>,
        long_url: String,
        custom_code: Option<String>,
    ) -> async_graphql::Result<UrlEntry> {
        let state = ctx.data::<Arc<AppState>>()?;
        if let Err(reason) = validate_url(&long_url) {
            warn!(%reason, "URL rejected by validation");
            return Err(async_graphql::Error::new(reason));
        }
        let parsed = Url::parse(&long_url).map_err(|e| async_graphql::Error::new(e.to_string()))?;
        let denied = denied_domains();
        if denied.iter().any(|domain| domain_matches(&parsed, domain)) {
            warn!(
                domain = parsed.host_str().unwrap_or_default(),
                "URL rejected by domain denylist"
            );
            return Err(async_graphql::Error::new("URL domain is not allowed"));
        }
        info!(
            domain = parsed.host_str().unwrap_or_default(),
            "URL passed validation"
        );

        let code =
            custom_code.unwrap_or_else(|| Uuid::new_v4().simple().to_string()[..6].to_owned());
        if code.is_empty()
            || code.len() > 10
            || !code
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || b == b'-' || b == b'_')
        {
            return Err(async_graphql::Error::new(
                "Custom code must be 1–10 letters, numbers, hyphens, or underscores",
            ));
        }

        let mut tx = state.db.begin().await?;
        let entry = sqlx::query_as::<_, UrlEntry>(
            "INSERT INTO urls (short_code, long_url) VALUES ($1, $2) RETURNING id, short_code, long_url"
        ).bind(&code).bind(&long_url).fetch_one(&mut *tx).await?;
        sqlx::query("INSERT INTO outbox_events (payload) VALUES ($1)")
            .bind(serde_json::to_value(OutboxEvent {
                kind: "url_created".into(),
                url_id: entry.id,
            })?)
            .execute(&mut *tx)
            .await?;
        tx.commit().await?;
        Ok(entry)
    }
}

fn validate_url(value: &str) -> Result<(), String> {
    let parsed = Url::parse(value).map_err(|_| "URL is malformed".to_owned())?;
    if !matches!(parsed.scheme(), "http" | "https") || parsed.host_str().is_none() {
        return Err("Only absolute HTTP and HTTPS URLs are accepted".into());
    }
    Ok(())
}

fn denied_domains() -> HashSet<String> {
    env::var("DENIED_DOMAINS")
        .unwrap_or_default()
        .split(',')
        .map(|domain| {
            domain
                .trim()
                .trim_start_matches("*.")
                .trim_start_matches('.')
                .to_ascii_lowercase()
        })
        .filter(|domain| !domain.is_empty())
        .collect()
}

fn domain_matches(url: &Url, denied: &str) -> bool {
    url.host_str().is_some_and(|host| {
        host.eq_ignore_ascii_case(denied)
            || host.to_ascii_lowercase().ends_with(&format!(".{denied}"))
    })
}

async fn lookup_url(
    state: &AppState,
    code: &str,
) -> Result<Option<UrlEntry>, Box<dyn std::error::Error + Send + Sync>> {
    if !state.cache_enabled {
        return Ok(sqlx::query_as::<_, UrlEntry>(
            "SELECT id, short_code, long_url FROM urls WHERE short_code = $1",
        )
        .bind(code)
        .fetch_optional(&state.db)
        .await?);
    }
    let key = format!("url:{code}");
    let mut cache = match state.redis.get_multiplexed_async_connection().await {
        Ok(cache) => Some(cache),
        Err(error) => {
            warn!(%error, "Redis unavailable; looking up URL in PostgreSQL");
            None
        }
    };
    if let Some(connection) = cache.as_mut() {
        match connection.get::<_, Option<String>>(&key).await {
            Ok(Some(value)) => return Ok(Some(serde_json::from_str(&value)?)),
            Ok(None) => {}
            Err(error) => {
                warn!(%error, "Redis read failed; looking up URL in PostgreSQL");
                cache = None;
            }
        }
    }
    let result = sqlx::query_as::<_, UrlEntry>(
        "SELECT id, short_code, long_url FROM urls WHERE short_code = $1",
    )
    .bind(code)
    .fetch_optional(&state.db)
    .await?;
    if let (Some(entry), Some(connection)) = (&result, cache.as_mut()) {
        if let Err(error) = connection
            .set_ex::<_, _, ()>(&key, serde_json::to_string(entry)?, 300)
            .await
        {
            warn!(%error, "Redis write failed; continuing with PostgreSQL result");
        }
    }
    Ok(result)
}

async fn redirect(State(state): State<Arc<AppState>>, Path(code): Path<String>) -> Response {
    match lookup_url(&state, &code).await {
        Ok(Some(entry)) => {
            if let Err(error) = record_click(&state.db, entry.id, &code).await {
                error!(%error, "could not record redirect click");
            }
            Redirect::temporary(&entry.long_url).into_response()
        }
        Ok(None) => StatusCode::NOT_FOUND.into_response(),
        Err(error) => {
            error!(%error, "URL lookup failed");
            StatusCode::SERVICE_UNAVAILABLE.into_response()
        }
    }
}

async fn record_click(db: &PgPool, id: Uuid, code: &str) -> Result<(), sqlx::Error> {
    let mut tx = db.begin().await?;
    sqlx::query("UPDATE urls SET click_count = click_count + 1 WHERE id = $1")
        .bind(id)
        .execute(&mut *tx)
        .await?;
    sqlx::query("INSERT INTO outbox_events (payload) VALUES ($1)")
        .bind(serde_json::json!({"kind": "url_clicked", "url_id": id, "short_code": code}))
        .execute(&mut *tx)
        .await?;
    tx.commit().await
}

async fn graphql_handler(
    State(state): State<Arc<AppState>>,
    req: GraphQLRequest,
) -> GraphQLResponse {
    state.schema.execute(req.into_inner()).await.into()
}

async fn graphiql_handler() -> impl IntoResponse {
    async_graphql::http::GraphiQLSource::build()
        .endpoint("/")
        .finish()
}

async fn publish_outbox(pool: PgPool, channel: Channel) {
    loop {
        let result = async {
            let mut tx = pool.begin().await?;
            let row = sqlx::query("SELECT id, payload FROM outbox_events WHERE published_at IS NULL ORDER BY id FOR UPDATE SKIP LOCKED LIMIT 1")
                .fetch_optional(&mut *tx).await?;
            if let Some(row) = row {
                let id: i64 = row.try_get("id")?;
                let payload: serde_json::Value = row.try_get("payload")?;
                let confirm = channel.basic_publish("", "url_events", BasicPublishOptions::default(),
                    &serde_json::to_vec(&payload)?, BasicProperties::default().with_delivery_mode(2)).await?.await?;
                if matches!(confirm, Confirmation::Ack(_)) {
                    sqlx::query("UPDATE outbox_events SET published_at = NOW() WHERE id = $1").bind(id).execute(&mut *tx).await?;
                    tx.commit().await?;
                    return Ok::<bool, Box<dyn std::error::Error + Send + Sync>>(true);
                }
            }
            tx.rollback().await?;
            Ok(false)
        }.await;
        match result {
            Ok(true) => continue,
            Ok(false) => sleep(Duration::from_millis(500)).await,
            Err(error) => {
                error!(%error, "outbox publish failed; will retry");
                sleep(Duration::from_secs(2)).await;
            }
        }
    }
}

async fn consume_events(
    pool: PgPool,
    channel: Channel,
    http: Client,
    es_url: String,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let mut consumer = channel
        .basic_consume(
            "url_events",
            "url-indexer",
            BasicConsumeOptions::default(),
            FieldTable::default(),
        )
        .await?;
    while let Some(delivery) = consumer.next().await {
        let delivery = delivery?;
        let processed = async {
            let event: OutboxEvent = serde_json::from_slice(&delivery.data)?;
            let row = sqlx::query("SELECT id, short_code, long_url, created_at::text AS created_at, click_count FROM urls WHERE id = $1")
                .bind(event.url_id).fetch_optional(&pool).await?;
            if let Some(row) = row {
                let long_url: String = row.try_get("long_url")?;
                let domain = Url::parse(&long_url)?.host_str().unwrap_or_default().to_ascii_lowercase();
                let doc = serde_json::json!({
                    "short_code": row.try_get::<String, _>("short_code")?,
                    "long_url": long_url,
                    "domain": domain,
                    "created_at": row.try_get::<String, _>("created_at")?,
                    "click_count": row.try_get::<i64, _>("click_count")?
                });
                let id: Uuid = row.try_get("id")?;
                let response = http.put(format!("{es_url}/urls/_doc/{id}?refresh=false")).json(&doc).send().await?;
                if !response.status().is_success() { return Err(format!("Elasticsearch returned {}", response.status()).into()); }
            }
            Ok::<(), Box<dyn std::error::Error + Send + Sync>>(())
        }.await;
        match processed {
            Ok(()) => delivery.ack(BasicAckOptions::default()).await?,
            Err(error) => {
                error!(%error, "event indexing failed; requeuing message");
                delivery
                    .nack(lapin::options::BasicNackOptions {
                        requeue: true,
                        ..Default::default()
                    })
                    .await?;
            }
        }
    }
    Ok(())
}

async fn ensure_search_index(
    http: &Client,
    es_url: &str,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let response = http
        .put(format!("{es_url}/urls"))
        .json(&serde_json::json!({"mappings":{"properties":{
            "short_code":{"type":"keyword"}, "long_url":{"type":"keyword","ignore_above":2048},
            "domain":{"type":"keyword"}, "created_at":{"type":"date"}, "click_count":{"type":"long"}
        }}}))
        .send()
        .await?;
    if response.status().is_success()
        || response.status().as_u16() == StatusCode::BAD_REQUEST.as_u16()
    {
        Ok(())
    } else {
        Err(format!(
            "Could not create Elasticsearch index: {}",
            response.status()
        )
        .into())
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    dotenvy::dotenv().ok();
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .init();

    let database_url = env::var("DATABASE_URL")?;
    let redis_url = env::var("REDIS_URL").unwrap_or_else(|_| "redis://127.0.0.1/".into());
    let cache_enabled = env::var("CACHE_ENABLED")
        .map(|value| value != "false")
        .unwrap_or(true);
    let rabbit_url =
        env::var("RABBITMQ_URL").unwrap_or_else(|_| "amqp://127.0.0.1:5672/%2f".into());
    let es_url = env::var("ELASTICSEARCH_URL")
        .unwrap_or_else(|_| "http://127.0.0.1:9200".into())
        .trim_end_matches('/')
        .to_owned();
    let port = env::var("PORT").unwrap_or_else(|_| "8080".into());
    let pool = PgPoolOptions::new()
        .max_connections(20)
        .connect(&database_url)
        .await?;
    sqlx::migrate!().run(&pool).await?;
    let redis = redis::Client::open(redis_url)?;
    let http = Client::builder().timeout(Duration::from_secs(3)).build()?;
    ensure_search_index(&http, &es_url).await?;

    let rabbit = Connection::connect(&rabbit_url, ConnectionProperties::default()).await?;
    let channel = rabbit.create_channel().await?;
    channel.confirm_select(Default::default()).await?;
    channel
        .queue_declare(
            "url_events",
            QueueDeclareOptions {
                durable: true,
                ..Default::default()
            },
            FieldTable::default(),
        )
        .await?;
    tokio::spawn(publish_outbox(pool.clone(), channel.clone()));
    tokio::spawn({
        let pool = pool.clone();
        let channel = channel.clone();
        let http = http.clone();
        let es = es_url.clone();
        async move {
            if let Err(error) = consume_events(pool, channel, http, es).await {
                error!(%error, "event consumer stopped");
            }
        }
    });

    let schema = Schema::build(QueryRoot, MutationRoot, EmptySubscription).finish();
    let state = Arc::new(AppState {
        db: pool,
        redis,
        cache_enabled,
        http,
        elasticsearch_url: es_url,
        schema,
    });
    let addr = format!("0.0.0.0:{port}");
    let listener = TcpListener::bind(&addr).await?;
    info!(%addr, "URL shortener listening");
    let app = Router::new()
        .route("/", get(graphiql_handler).post(graphql_handler))
        .route("/{short_code}", get(redirect))
        .with_state(state);
    axum::serve(listener, app).await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{domain_matches, validate_url};
    use url::Url;

    #[test]
    fn accepts_absolute_http_and_https_urls() {
        assert!(validate_url("https://example.com/path").is_ok());
        assert!(validate_url("http://example.com").is_ok());
    }

    #[test]
    fn rejects_malformed_and_non_http_urls() {
        assert!(validate_url("not a URL").is_err());
        assert!(validate_url("ftp://example.com/file").is_err());
    }

    #[test]
    fn denylist_matches_domain_and_subdomains_without_suffix_collisions() {
        let denied = Url::parse("https://bad.example.com/path").unwrap();
        let suffix_collision = Url::parse("https://notbadexample.com/").unwrap();
        assert!(domain_matches(&denied, "example.com"));
        assert!(!domain_matches(&suffix_collision, "example.com"));
    }
}
