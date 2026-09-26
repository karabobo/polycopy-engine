//! Diagnostic order-book arithmetic shared by execution and the separate sampler.
//! Prices and sizes stay exact decimals; none of these observations affects trading.
use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use serde::Serialize;

pub const OFFSETS: [i64; 5] = [0, 2, 4, 6, 10];

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct BookObservation {
    pub fetched_at: DateTime<Utc>,
    pub best_bid: Option<Decimal>,
    pub best_ask: Option<Decimal>,
    pub ask_size_at_best: Decimal,
    pub ask_size_within_leader: [Decimal; 5],
}

/// Every cumulative quantity includes asks priced at or below leader + offset
/// (offset is expressed in cents). Empty books have no best price and zero size.
/// Invalid levels fail the same way the old best-ask helper did for asks.
pub fn observe_book(
    bids: impl IntoIterator<Item = (Decimal, Decimal)>,
    asks: impl IntoIterator<Item = (Decimal, Decimal)>,
    leader_price: Decimal,
    fetched_at: DateTime<Utc>,
) -> Result<BookObservation, String> {
    let mut best_bid: Option<Decimal> = None;
    for (price, size) in bids {
        // Bad bid data is diagnostic-only: never change a maker decision
        // whose old path depended exclusively on the ask side.
        if price > Decimal::ZERO && price < Decimal::ONE && size > Decimal::ZERO {
            best_bid = Some(best_bid.map_or(price, |current| current.max(price)));
        }
    }
    let asks: Vec<_> = asks.into_iter().collect();
    // Preserve the old decision exactly, including its validation of only
    // the lowest displayed ask (not deeper diagnostic levels).
    let best_ask = super::orchestrate::best_ask_price(asks.iter().copied())?;
    let mut result = BookObservation {
        fetched_at, best_bid, best_ask, ask_size_at_best: Decimal::ZERO,
        ask_size_within_leader: [Decimal::ZERO; 5],
    };
    for (price, size) in asks {
        if price <= Decimal::ZERO || price >= Decimal::ONE || size <= Decimal::ZERO {
            continue;
        }
        match result.best_ask {
            Some(best) if price == best => result.ask_size_at_best += size,
            _ => {}
        }
        for (index, cents) in OFFSETS.iter().enumerate() {
            if price <= leader_price + Decimal::new(*cents, 2) {
                result.ask_size_within_leader[index] += size;
            }
        }
    }
    Ok(result)
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn unordered_levels_aggregate_best_and_inclusive_thresholds() {
        let now = Utc::now();
        let book = observe_book(
            [(Decimal::new(40, 2), Decimal::ONE), (Decimal::new(42, 2), Decimal::ONE)],
            [(Decimal::new(56, 2), Decimal::new(4, 0)),
             (Decimal::new(50, 2), Decimal::new(2, 0)),
             (Decimal::new(54, 2), Decimal::new(3, 0)),
             (Decimal::new(50, 2), Decimal::ONE),
             (Decimal::new(60, 2), Decimal::new(5, 0))],
            Decimal::new(50, 2), now,
        ).unwrap();
        assert_eq!(book.fetched_at, now);
        assert_eq!(book.best_bid, Some(Decimal::new(42, 2)));
        assert_eq!(book.best_ask, Some(Decimal::new(50, 2)));
        assert_eq!(book.ask_size_at_best, Decimal::new(3, 0));
        assert_eq!(book.ask_size_within_leader, [3, 3, 6, 10, 15].map(Decimal::from));
    }
}
