use async_graphql::{EmptySubscription, Object, Schema, SimpleObject};
use async_graphql_axum::{GraphQLRequest, GraphQLResponse};
use axum::{extract::State, response::IntoResponse, routing::get, Router};
use dotenvy::dotenv;
use std::str::FromStr;
use sqlx::postgres::{PgPool, PgPoolOptions, PgConnectOptions};
use sqlx::ConnectOptions;
use std::env;
use tracing::info;
use uuid::Uuid;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;
use std::sync::Arc;
use tokio::net::TcpListener;

// 1. MODELS
#[derive(SimpleObject, sqlx::FromRow)]
struct UrlEntry {
    // Corrected case sensitivity: UUID -> Uuid
    id: Uuid, 
    short_code: String,
    long_url: String,
}

// 2. DATABASE STATE
struct AppState {
    db: PgPool,
    // Added schema back to AppState, required for handler
    schema: AppSchema, 
}

// 3. GRAPHQL SCHEMA
struct QueryRoot;

#[Object]
impl QueryRoot {
    async fn get_url(&self, ctx: &async_graphql::Context<'_>, short_code: String) -> async_graphql::Result<Option<UrlEntry>> {
        let state = ctx.data::<Arc<AppState>>()?;
        
        let result = sqlx::query_as::<_, UrlEntry>(
            "SELECT id, short_code, long_url FROM urls WHERE short_code = $1"
        )
        .bind(&short_code)
        .fetch_optional(&state.db)
        .await?;

        Ok(result)
    }
}

struct MutationRoot;

#[Object]
impl MutationRoot {
    async fn create_url(&self, ctx: &async_graphql::Context<'_>, long_url: String, custom_code: Option<String>) -> async_graphql::Result<UrlEntry> {
        let state = ctx.data::<Arc<AppState>>()?;
        
        let short_code = custom_code.unwrap_or_else(||Uuid::new_v4().to_string()[..6].to_string());
        
        let rec = sqlx::query_as::<_, UrlEntry>(
            "INSERT INTO urls (short_code, long_url) VALUES ($1, $2) RETURNING id, short_code, long_url"
        )
        .bind(&short_code)
        .bind(&long_url)
        .fetch_one(&state.db)
        .await?;

        Ok(rec)
    }
}

// Defined Schema type alias for easier use
type AppSchema = Schema<QueryRoot, MutationRoot, EmptySubscription>;

// 4. WEB HANDLERS

// Updated handler signature to match Axum 0.7 Extension/State paradigm correctly with Arc
async fn graphql_handler(
    State(state): State<Arc<AppState>>,
    req: GraphQLRequest,
) -> GraphQLResponse {
    state.schema.execute(req.into_inner()).await.into()
}

async fn graphiql_handler() -> impl IntoResponse {
    async_graphql::http::GraphiQLSource::build().endpoint("/").finish()
}


// 5. MAIN
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    dotenv().ok();

    tracing_subscriber::registry()
        .with(tracing_subscriber::EnvFilter::from_default_env())
        .with(tracing_subscriber::fmt::layer())
        .init();

    let database_url = env::var("DATABASE_URL").expect("DATABASE_URL must be set");
    let port = env::var("PORT").unwrap_or_else(|_| "8080".to_string());

    // Robust Connection handling for Neon
    let mut connect_options = PgConnectOptions::from_str(&database_url)?;
    connect_options = connect_options.log_statements(tracing::log::LevelFilter::Debug);

    let pool = PgPoolOptions::new()
        .max_connections(5)
        .idle_timeout(std::time::Duration::from_secs(30))
        .connect_with(connect_options)
        .await?;

    // NOTE: sqlx::migrate!() macro requires 'migrate' feature enabled in Cargo.toml
    sqlx::migrate!().run(&pool).await?;

    // Construct Schema
    let schema = Schema::build(QueryRoot, MutationRoot, EmptySubscription)
        .finish();

    // Put schema inside AppState
    let app_state = Arc::new(AppState { db: pool, schema });

    let addr = format!("0.0.0.0:{}", port).parse()?;
    info!("Listening on {}", addr);
    
    let listener = TcpListener::bind(&addr).await?;
    let app = Router::new()
        .route("/", get(graphiql_handler).post(graphql_handler))
        .with_state(app_state);
    
    axum::serve(listener, app).await?;

    Ok(())
}