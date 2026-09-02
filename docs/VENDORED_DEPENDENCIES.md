# Vendored dependencies

## `polymarket_client_sdk_v2`

This repository vendors the official `polymarket_client_sdk_v2` 0.7.0 source
at upstream commit `222143d321eba97d5711a848265eb9aab3bc7ff4`.  It is an
archive-only source import: the upstream Git history is not carried here.

The sole local patch is on `TradeResponse.fee_rate_bps`: it deserializes an
empty-string response as zero.  The live CLOB has returned that malformed value
for this optional fee metadata.  It is not used for position, receipt, or
reconciliation accounting in this engine.

`price`, `size`, order identifiers, status, and timestamps remain strict.  A
malformed value in any of those fields must still fail the trade-history query,
which leaves the account/token reconciliation-blocked rather than inventing a
zero position or fill.

The vendored SDK has a regression test proving the narrow empty-fee case.  Any
SDK upgrade must re-audit this patch and rerun the engine plus vendored-SDK test
suites.
