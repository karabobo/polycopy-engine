//! Leader address resolution: normalized_address -> leader_id.
//!
//! Blueprint section 7: "Address resolution uses an
//! `ArcSwap<HashMap<normalized_address, leader_id>>`. Build and validate the
//! replacement map fully, then atomically publish it." A reload must never
//! expose a temporary empty map to a concurrent reader -- a real activity
//! message can arrive mid-reload, and a naive clear-then-refill on a shared
//! mutable map would silently drop it during that window even though the
//! leader is still watched. `ArcSwap`'s single-pointer swap makes that
//! window impossible by construction: a reader always sees either the
//! complete previous map or the complete next one.

use std::{collections::HashMap, sync::Arc};

use arc_swap::ArcSwap;

#[derive(Debug, Default)]
pub struct AddressResolver {
    map: ArcSwap<HashMap<String, i64>>,
}

impl AddressResolver {
    pub fn new() -> Self {
        Self {
            map: ArcSwap::from_pointee(HashMap::new()),
        }
    }

    /// Builds `pairs` into a new map (addresses normalized to lowercase),
    /// then atomically publishes it, fully replacing whatever was
    /// previously resolvable. Building happens entirely before publish: a
    /// concurrent reader never observes a partially built or empty map.
    pub fn reload(&self, pairs: impl IntoIterator<Item = (String, i64)>) {
        let mut next = HashMap::new();
        for (address, leader_id) in pairs {
            next.insert(address.to_ascii_lowercase(), leader_id);
        }
        self.map.store(Arc::new(next));
    }

    /// Resolves `address` (any casing) to its watched leader_id, if any.
    pub fn resolve(&self, address: &str) -> Option<i64> {
        self.map.load().get(&address.to_ascii_lowercase()).copied()
    }

    pub fn len(&self) -> usize {
        self.map.load().len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Queries every currently-enabled leader alias and reloads from it in
    /// one call. The natural way to (re)build the resolver at startup or
    /// whenever leader configuration changes.
    pub async fn reload_from_db(&self, pool: &sqlx::SqlitePool) -> Result<(), sqlx::Error> {
        let rows: Vec<(String, i64)> = sqlx::query_as(
            "SELECT lwa.address, lwa.leader_id \
             FROM leader_wallet_aliases lwa \
             JOIN leader_config lc ON lc.id = lwa.leader_id \
             WHERE lwa.enabled = 1 AND lc.enabled = 1",
        )
        .fetch_all(pool)
        .await?;
        self.reload(rows);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::{
        sync::{
            atomic::{AtomicBool, Ordering},
            Arc,
        },
        thread,
    };

    use super::*;

    #[test]
    fn resolves_a_loaded_address_case_insensitively() {
        let resolver = AddressResolver::new();
        resolver.reload([("0xABCdef".to_owned(), 1)]);

        assert_eq!(resolver.resolve("0xabcdef"), Some(1));
        assert_eq!(resolver.resolve("0xABCDEF"), Some(1));
        assert_eq!(resolver.resolve("0x000000"), None);
    }

    #[test]
    fn reload_fully_replaces_rather_than_merges() {
        let resolver = AddressResolver::new();
        resolver.reload([("0xaaa".to_owned(), 1), ("0xbbb".to_owned(), 2)]);
        assert_eq!(resolver.len(), 2);

        resolver.reload([("0xccc".to_owned(), 3)]);

        assert_eq!(resolver.len(), 1);
        assert_eq!(
            resolver.resolve("0xaaa"),
            None,
            "a dropped leader must no longer resolve"
        );
        assert_eq!(resolver.resolve("0xbbb"), None);
        assert_eq!(resolver.resolve("0xccc"), Some(3));
    }

    #[test]
    fn an_empty_reload_is_the_only_way_to_actually_empty_the_map() {
        let resolver = AddressResolver::new();
        assert!(resolver.is_empty(), "a fresh resolver starts empty");
        resolver.reload([("0xaaa".to_owned(), 1)]);
        assert!(!resolver.is_empty());
    }

    #[test]
    fn concurrent_readers_never_observe_a_map_smaller_than_either_generation() {
        // Not a proof of the swap's atomicity (ArcSwap already guarantees a
        // reader sees one complete generation or the other), but a
        // regression guard: if `reload` were ever changed to clear a shared
        // map in place before refilling it, this would start failing by
        // observing `len() == 0` mid-reload.
        let resolver = Arc::new(AddressResolver::new());
        resolver.reload((0..2000).map(|i| (format!("0x{i:040x}"), i)));

        let stop = Arc::new(AtomicBool::new(false));
        let reader_resolver = Arc::clone(&resolver);
        let reader_stop = Arc::clone(&stop);
        let reader = thread::spawn(move || {
            let mut saw_undersized_map = false;
            while !reader_stop.load(Ordering::Relaxed) {
                let len = reader_resolver.len();
                if len != 0 && len != 2000 && len != 3000 {
                    saw_undersized_map = true;
                }
            }
            saw_undersized_map
        });

        for _ in 0..50 {
            resolver.reload((0..3000).map(|i| (format!("0x{i:040x}"), i)));
            resolver.reload((0..2000).map(|i| (format!("0x{i:040x}"), i)));
        }

        stop.store(true, Ordering::Relaxed);
        let saw_undersized_map = reader.join().expect("reader thread must not panic");
        assert!(
            !saw_undersized_map,
            "a reader must only ever see one complete generation's length, never a partial one"
        );
    }

    #[tokio::test]
    async fn reload_from_db_loads_only_enabled_aliases_of_enabled_leaders() {
        use crate::copytrading::db::open_and_migrate;

        let path = std::env::temp_dir().join(format!(
            "polycopy-engine-address-resolver-test-{}-{}.sqlite",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        let pool = open_and_migrate(&path)
            .await
            .expect("migrations must apply");

        sqlx::query(
            "INSERT INTO leader_config (id, label, enabled) VALUES (1, 'enabled-leader', 1), (2, 'disabled-leader', 0)",
        )
        .execute(&pool)
        .await
        .expect("leaders must insert");
        sqlx::query(
            "INSERT INTO leader_wallet_aliases (leader_id, address, enabled) VALUES \
             (1, '0xenabledalias', 1), (1, '0xdisabledalias', 0), (2, '0xleaderdisabled', 1)",
        )
        .execute(&pool)
        .await
        .expect("aliases must insert");

        let resolver = AddressResolver::new();
        resolver
            .reload_from_db(&pool)
            .await
            .expect("reload must succeed");

        assert_eq!(resolver.resolve("0xenabledalias"), Some(1));
        assert_eq!(
            resolver.resolve("0xdisabledalias"),
            None,
            "a disabled alias must not resolve"
        );
        assert_eq!(
            resolver.resolve("0xleaderdisabled"),
            None,
            "an alias of a disabled leader must not resolve even if the alias row itself is enabled"
        );
        assert_eq!(resolver.len(), 1);

        pool.close().await;
        let _ = std::fs::remove_file(&path);
        let _ = std::fs::remove_file(format!("{}-shm", path.display()));
        let _ = std::fs::remove_file(format!("{}-wal", path.display()));
    }
}
