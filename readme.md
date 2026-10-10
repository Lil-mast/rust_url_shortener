# Rust URL Shortener

A Rust URL shortener built with Axum, GraphQL, PostgreSQL, Redis, RabbitMQ, and Elasticsearch. PostgreSQL is the durable source of truth; Redis accelerates redirects and RabbitMQ workers maintain the search index asynchronously.

## Local setup

Requirements: Rust/Cargo and Docker Compose.

```sh
docker compose up -d --wait
cp .env.example .env
cargo run
```

The application applies SQLx migrations at startup. It listens on port `8080` by default. Open [http://localhost:8080/](http://localhost:8080/) for GraphiQL.

Environment variables:

| Name | Default | Purpose |
| --- | --- | --- |
| `DATABASE_URL` | required | PostgreSQL connection string |
| `REDIS_URL` | `redis://127.0.0.1/` | Redis cache connection |
| `RABBITMQ_URL` | `amqp://127.0.0.1:5672/%2f` | RabbitMQ connection |
| `ELASTICSEARCH_URL` | `http://127.0.0.1:9200` | Elasticsearch endpoint |
| `PORT` | `8080` | HTTP listen port |
| `CACHE_ENABLED` | `true` | Set to `false` to benchmark database-only retrieval |
| `DENIED_DOMAINS` | empty | Comma-separated domains to reject; subdomains are also rejected |

The Compose PostgreSQL credentials are `shortener` / `shortener`, database `shortener`. Set `DATABASE_URL=postgres://shortener:shortener@localhost:5432/shortener` in `.env`.

## API

GraphQL remains at `POST /`. `createUrl` accepts absolute HTTP(S) URLs, rejects malformed URLs and domains in `DENIED_DOMAINS`, and accepts an optional custom code. Generated codes are six characters. Codes are 1–10 ASCII letters, numbers, hyphens, or underscores.

```graphql
mutation {
  createUrl(longUrl: "https://www.rust-lang.org/") {
    id
    shortCode
    longUrl
  }
}
```

`GET /{short_code}` redirects to the destination (temporary redirect). A missing code returns `404`. Redis caches full URL records for five minutes. Set `CACHE_ENABLED=false` and restart the application to bypass Redis.

URL lookup through GraphQL:

```graphql
query {
  getUrl(shortCode: "abc123") { id shortCode longUrl }
}
```

Search Elasticsearch by domain, creation time, or minimum click count:

```graphql
query {
  searchUrls(filter: { domain: "rust-lang.org", minClicks: 1 }, limit: 25) {
    shortCode
    longUrl
    domain
    createdAt
    clickCount
  }
}
```

## Async processing and reliability

URL creation and its outbox event commit in the same PostgreSQL transaction. A publisher retries unpublished outbox rows until RabbitMQ confirms delivery. The consumer indexes current URL metadata in the Elasticsearch `urls` index. Redirect counts are incremented in PostgreSQL and an outbox event prompts the consumer to refresh the indexed count. Elasticsearch can be rebuilt from PostgreSQL records and is not the source of truth.

## Compare redirect latency

Start the service once with `CACHE_ENABLED=true`, then again with `CACHE_ENABLED=false`, and benchmark the same existing code in both modes. Example request loop (replace `abc123` with a real code):

```sh
python3 scripts/benchmark.py http://localhost:8080/abc123 --requests 10000 --concurrency 50
```

The script reports p50, p95, p99, and average request latency. Run both modes on the same machine with the same Compose services and request count. Record the environment and measured p99 values here after running; this repository does not claim unmeasured performance numbers.
