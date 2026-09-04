# Polycopy Engine

A Polymarket copy-trading engine that watches specific traders' on-chain activity and mirrors it into a bounded set of an operator's own orders.

## Language

**Account**:
The single Polymarket wallet this engine trades from, holding its own collateral and positions. There is exactly one Account per deployment.
_Avoid_: user, wallet (a wallet is an implementation detail of an Account)

**Leader**:
A real-world trader being copied. Identified by a stable label, independent of any on-chain address.
_Avoid_: trader, target

**Leader Wallet Alias**:
One on-chain address a Leader is known to trade from. A Leader may have more than one; it keeps its identity even if it moves to a new wallet.
_Avoid_: leader address (ambiguous once a Leader has more than one)

**Trading Config**:
The complete, declarative description of every Leader this Account follows and the policy for each, applied via `copy_config_apply`. Applying it makes the database match it exactly: a Leader left out of the config is disabled, not merely untouched.
_Avoid_: config file, setup (setup implies one-time; a Trading Config is reapplied over the Account's whole lifetime)

**Position Lot**:
The Account's own tracked holding of one outcome token copied from one Leader. Disabling a Leader never closes or erases its Position Lots.
_Avoid_: position, holding
