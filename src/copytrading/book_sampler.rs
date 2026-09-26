//! Independent, credential-free observer. Never writes to the execution ledger.
use std::{collections::HashSet, future::Future};

use chrono::{DateTime, Duration, SecondsFormat, Utc};
use rust_decimal::Decimal;
use serde::Serialize;
use sqlx::SqlitePool;

use super::book_observation::BookObservation;

pub const SLOTS_MS: [i64; 8] = [0, 500, 1_000, 2_000, 5_000, 15_000, 60_000, 200_000];

#[derive(Debug, Clone, Serialize)]
pub struct SampleRow {
    pub event_id: i64,
    pub token_id: String,
    pub observed_at: DateTime<Utc>,
    pub offset_ms: i64,
    pub scheduled_at: DateTime<Utc>,
    pub sampled_at: DateTime<Utc>,
    pub book: Option<BookObservation>,
    pub error: Option<String>,
}

#[derive(Debug, Clone)]
pub struct Event {
    pub id: i64,
    pub token_id: String,
    pub leader_price: Decimal,
    pub observed_at: DateTime<Utc>,
}

/// The source is a read-only pool; rows include non-planned realtime events.
/// Limit the scan to recent events: historical events cannot be sampled at
/// their original times after a restart or long outage.
pub async fn recent_events(source: &SqlitePool, now: DateTime<Utc>) -> Result<Vec<Event>, String> {
    let cutoff = (now - Duration::seconds(205)).to_rfc3339_opts(SecondsFormat::Millis, true);
    let rows: Vec<(i64, String, String, String)> = sqlx::query_as(
        "SELECT id, token_id, price, observed_at FROM leader_events \
         WHERE realtime_observed = 1 AND observed_at >= ? ORDER BY id",
    )
    .bind(cutoff)
    .fetch_all(source).await.map_err(|error| error.to_string())?;
    rows.into_iter().map(|(id, token_id, price, observed_at)| {
        Ok(Event {
            id, token_id,
            leader_price: price.parse().map_err(|error: rust_decimal::Error| error.to_string())?,
            observed_at: DateTime::parse_from_rfc3339(&observed_at)
                .map_err(|error| error.to_string())?.with_timezone(&Utc),
        })
    }).collect()
}

pub async fn init_output(output: &SqlitePool) -> Result<(), sqlx::Error> {
    sqlx::query(
        "CREATE TABLE IF NOT EXISTS book_samples (
            event_id INTEGER NOT NULL, token_id TEXT NOT NULL, observed_at TEXT NOT NULL,
            offset_ms INTEGER NOT NULL, scheduled_at TEXT NOT NULL, sampled_at TEXT NOT NULL,
            book_json TEXT, error TEXT, PRIMARY KEY(event_id, offset_ms))",
    ).execute(output).await?;
    Ok(())
}

/// A failed HTTP read is recorded and does not abort other slots/events.
/// The caller supplies the public-book reader so tests need no network.
pub async fn sample_due<F, Fut>(
    output: &SqlitePool,
    events: &[Event],
    seen: &mut HashSet<(i64, i64)>,
    now: DateTime<Utc>,
    fetch: &F,
) -> Result<Vec<SampleRow>, sqlx::Error>
where
    F: Fn(String, Decimal) -> Fut,
    Fut: Future<Output = Result<BookObservation, String>>,
{
    let mut results = Vec::new();
    for event in events {
        for offset in SLOTS_MS {
            let scheduled_at = event.observed_at + Duration::milliseconds(offset);
            if scheduled_at > now || !seen.insert((event.id, offset)) { continue; }
            // If the process first sees an event after its slot has passed,
            // retain an error, not a misleading delayed book as an on-time one.
            let book = if now - scheduled_at > Duration::seconds(3) {
                Err("sample slot missed before observation".to_owned())
            } else {
                fetch(event.token_id.clone(), event.leader_price).await
            };
            let row = SampleRow {
                event_id: event.id, token_id: event.token_id.clone(),
                observed_at: event.observed_at, offset_ms: offset, scheduled_at,
                sampled_at: Utc::now(),
                book: book.as_ref().ok().cloned(), error: book.err(),
            };
            let saved = sqlx::query("INSERT OR IGNORE INTO book_samples
                (event_id, token_id, observed_at, offset_ms, scheduled_at, sampled_at, book_json, error)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)")
                .bind(row.event_id).bind(&row.token_id).bind(row.observed_at.to_rfc3339())
                .bind(row.offset_ms).bind(row.scheduled_at.to_rfc3339())
                .bind(row.sampled_at.to_rfc3339())
                .bind(row.book.as_ref().map(serde_json::to_string).transpose().expect("book serializes"))
                .bind(&row.error).execute(output).await;
            if let Err(error) = saved {
                seen.remove(&(event.id, offset));
                return Err(error);
            }
            results.push(row);
        }
    }
    Ok(results)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::copytrading::db::{open_and_migrate, open_read_only};

    #[tokio::test]
    async fn schedule_records_errors_and_continues_without_writing_source() {
        let path = std::env::temp_dir().join(format!("book-source-{}.db", std::process::id()));
        let out = std::env::temp_dir().join(format!("book-output-{}.db", std::process::id()));
        let _ = std::fs::remove_file(&path);
        let _ = std::fs::remove_file(&out);
        let writer = open_and_migrate(&path).await.unwrap();
        sqlx::query("INSERT INTO leader_config (id, label) VALUES (1, 'fixture')")
            .execute(&writer).await.unwrap();
        sqlx::query("INSERT INTO leader_events
            (canonical_event_key, leader_id, condition_id, token_id, outcome_index, side,
             size, price, occurred_at, observed_at, realtime_observed)
             VALUES ('fixture-event', 1, 'condition', '101', 0, 'BUY',
             '0.5', '0.50', ?, ?, 1)")
            .bind(Utc::now().to_rfc3339_opts(SecondsFormat::Millis, true))
            .bind(Utc::now().to_rfc3339_opts(SecondsFormat::Millis, true))
            .execute(&writer).await.unwrap();
        writer.close().await;
        let source = open_read_only(&path).await.unwrap();
        let found = recent_events(&source, Utc::now()).await.unwrap();
        assert_eq!(found.len(), 1, "small unplanned realtime trade must be sampled");
        assert_eq!(found[0].token_id, "101");
        assert!(sqlx::query("CREATE TABLE forbidden(x INTEGER)").execute(&source).await.is_err());
        let output = SqlitePool::connect(&format!("sqlite://{}?mode=rwc", out.display())).await.unwrap();
        init_output(&output).await.unwrap();
        let now = Utc::now();
        let event = Event { id: 1, token_id: "101".into(), leader_price: Decimal::new(50, 2), observed_at: now };
        let mut seen = HashSet::new();
        let calls = std::sync::atomic::AtomicUsize::new(0);
        let fetch = |_: String, _: Decimal| {
            let count = calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            async move {
                if count == 0 { Err("HTTP 503".into()) }
                else { super::super::book_observation::observe_book([], [(Decimal::new(52, 2), Decimal::ONE)], Decimal::new(50, 2), Utc::now()) }
            }
        };
        let first = sample_due(&output, std::slice::from_ref(&event), &mut seen, now, &fetch).await.unwrap();
        assert_eq!(first.len(), 1);
        assert_eq!(first[0].error.as_deref(), Some("HTTP 503"));
        let second = sample_due(&output, std::slice::from_ref(&event), &mut seen, now + Duration::milliseconds(500), &fetch).await.unwrap();
        assert_eq!(second.len(), 1);
        assert_eq!(second[0].offset_ms, 500);
        assert!(second[0].book.is_some());
        let rows: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM book_samples").fetch_one(&output).await.unwrap();
        assert_eq!(rows, 2);
        for offset in SLOTS_MS.into_iter().skip(2) {
            let due = sample_due(&output, std::slice::from_ref(&event), &mut seen,
                now + Duration::milliseconds(offset), &fetch).await.unwrap();
            assert_eq!(due.len(), 1);
            assert_eq!(due[0].offset_ms, offset);
            assert_eq!(due[0].scheduled_at, now + Duration::milliseconds(offset));
        }
        assert_eq!(seen.len(), SLOTS_MS.len());
        let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM book_samples")
            .fetch_one(&output).await.unwrap();
        assert_eq!(count, SLOTS_MS.len() as i64);
        source.close().await;
        output.close().await;
        let _ = std::fs::remove_file(path);
        let _ = std::fs::remove_file(out);
    }
}
