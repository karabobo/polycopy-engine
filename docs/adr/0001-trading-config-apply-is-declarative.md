# Trading Config apply disables any Leader left out of it

`copy_config_apply` reconciles the database against the supplied Trading Config every time it runs, not just on first setup. For a Leader that exists in the database but is missing from the config, we considered three behaviors: leave it untouched (patch semantics), disable it (declarative), or refuse to apply until every existing Leader is explicitly listed (fail-closed).

## Decision

Declarative: a Leader missing from the config is disabled (never deleted -- its Position Lots and history stay intact). This matches how a Leader's wallet-alias list already worked (an address left out is disabled too), so the whole Trading Config is now consistently "this file is the complete truth."

## Consequences

Forgetting to carry a Leader forward into an edited config silently disables it, even if it still holds open Position Lots or an unresolved reconciliation case -- deliberately accepted, with no extra warning, in favor of keeping every reapply a single self-contained file rather than an accumulating patch history.
