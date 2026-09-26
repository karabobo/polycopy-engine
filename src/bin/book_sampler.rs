//! Credential-free, independent observation process. No EngineLock or ledger writes.
#[cfg(feature = "execute")]
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    use std::{collections::HashSet, time::Duration};
    use chrono::Utc;
    use polycopy_engine::copytrading::{
        book_observation::observe_book,
        book_sampler::{init_output, recent_events, sample_due},
        open_read_only,
    };
    use polymarket_client_sdk_v2::clob::{types::request::OrderBookSummaryRequest, Client, Config};
    use sqlx::sqlite::{SqliteConnectOptions, SqlitePoolOptions};

    let mut args = std::env::args().skip(1);
    let (Some(source_path), Some(output_path), None) = (args.next(), args.next(), args.next()) else {
        return Err("usage: book_sampler <main-db-path> <separate-output-db-path>".into());
    };
    if std::fs::canonicalize(&source_path).ok() == std::fs::canonicalize(&output_path).ok()
        && std::fs::canonicalize(&source_path).is_ok() {
        return Err("sampler output must not be the main database".into());
    }
    let source = open_read_only(&source_path).await?;
    let output = SqlitePoolOptions::new().max_connections(1)
        .connect_with(SqliteConnectOptions::new().filename(&output_path).create_if_missing(true))
        .await?;
    init_output(&output).await?;
    let client = Client::new("https://clob.polymarket.com", Config::default())?;
    let mut seen: HashSet<(i64, i64)> = sqlx::query_as("SELECT event_id, offset_ms FROM book_samples")
        .fetch_all(&output).await?.into_iter().collect();
    loop {
        let now = Utc::now();
        match recent_events(&source, now).await {
            Ok(events) => {
                let fetch = |token_id: String, leader_price| {
                    let client = client.clone();
                    async move {
                        let token_id = token_id.parse().map_err(|_| "invalid token id".to_owned())?;
                        let request = OrderBookSummaryRequest::builder().token_id(token_id).build();
                        let book = client.order_book(&request).await.map_err(|error| error.to_string())?;
                        observe_book(
                            book.bids.into_iter().map(|level| (level.price, level.size)),
                            book.asks.into_iter().map(|level| (level.price, level.size)),
                            leader_price, Utc::now(),
                        )
                    }
                };
                match sample_due(&output, &events, &mut seen, now, &fetch).await {
                    Ok(rows) => for row in rows {
                        if let Some(error) = row.error { eprintln!("book sample event={} offset_ms={} error={error}", row.event_id, row.offset_ms); }
                    },
                    Err(error) => eprintln!("book sample output write failed: {error}"),
                }
            }
            Err(error) => eprintln!("book sampler source read failed: {error}"),
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

#[cfg(not(feature = "execute"))]
fn main() {
    eprintln!("book_sampler requires --features execute");
    std::process::exit(2);
}
