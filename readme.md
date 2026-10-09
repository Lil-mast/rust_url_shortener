# Rust URL Shortener

A small URL shortener API built with Rust, Axum, GraphQL, and PostgreSQL. It lets you create short codes for long URLs and look up the saved URL by its code.

## Features

- Create a short URL with an optional custom code.
- Generate a six-character code when no custom code is provided.
- Look up a URL entry by its short code.
- Persist URL entries in PostgreSQL.
- Serve a GraphiQL interface and GraphQL API at `/`.
- Run the database migration automatically when the server starts.

## Requirements

- Rust and Cargo (Rust 2021 edition).
- A PostgreSQL database.

## Configuration

Set `DATABASE_URL` to a PostgreSQL connection string. `PORT` is optional and defaults to `8080`.

For local development, create a `.env` file in the project root:

```dotenv
DATABASE_URL=postgres://username:password@localhost:5432/url_shortener
PORT=8080
```

The server runs the migration in `migrations/` on startup, creating the `urls` table and its short-code index.

## Run

```sh
cargo run
```

Once running, open [http://localhost:8080/](http://localhost:8080/) to use GraphiQL. If you set a different `PORT`, use that port instead.

## GraphQL API

The API is available at `POST /`.

Create a URL entry, letting the service generate a code:

```graphql
mutation {
  createUrl(longUrl: "https://www.rust-lang.org/") {
    id
    shortCode
    longUrl
  }
}
```

Or provide a custom code:

```graphql
mutation {
  createUrl(longUrl: "https://www.rust-lang.org/", customCode: "rust") {
    id
    shortCode
    longUrl
  }
}
```

Look up an entry by its code:

```graphql
query {
  getUrl(shortCode: "rust") {
    id
    shortCode
    longUrl
  }
}
```

`getUrl` returns `null` when the code does not exist. Short codes must be unique; attempting to create an already-used code returns a database error.

## Current scope

This service creates and looks up short URL records. It does not currently provide an HTTP endpoint that redirects a browser from a short code to its destination.
