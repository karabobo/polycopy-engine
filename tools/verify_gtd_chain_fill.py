#!/usr/bin/env python3
"""Read-only independent Polygon evidence for one GTD maker order.

Example: python3 tools/verify_gtd_chain_fill.py --order-hash 0x... \
  --funder 0x... --token-id ... --from-block N --to-block M \
  --rpc https://polygon-bor-rpc.publicnode.com --rpc https://polygon.drpc.org

A zero result is NOT evidence of no fill: this tool refuses an empty result.
Never pass a URL containing a credential in a shell command or log.
"""
import argparse
import json
import sys
import urllib.request
from decimal import Decimal

TOPIC = "0xd543adfd945773f1a62f74f0ee55a5e3b9b1a28262980ba90b1a89f2ea84d8ee"
EXCHANGES = (
    "0xe111180000d2663c0091e4f400237545b87b996b",
    "0xe2222d279d744050d28e00520010520000310f59",
)


def rpc_call(url, method, params):
    payload = json.dumps({"jsonrpc": "2.0", "id": 1, "method": method, "params": params}).encode()
    request = urllib.request.Request(url, payload, {
        "Content-Type": "application/json", "User-Agent": "polycopy-readonly-reconcile/1.0",
    })
    with urllib.request.urlopen(request, timeout=20) as response:
        result = json.load(response)
    if result.get("error") or "result" not in result:
        raise ValueError(f"RPC {method} error: {result.get('error')}")
    return result["result"]


def verify_log(log, order_hash, funder, token_id):
    topics = log.get("topics", [])
    if log.get("removed") or len(topics) != 4 or topics[0].lower() != TOPIC:
        raise ValueError("unexpected or removed event for order hash")
    if topics[1].lower() != order_hash or topics[2].lower() != "0x" + "0" * 24 + funder[2:]:
        raise ValueError("order hash or maker address mismatch")
    if log["address"].lower() not in EXCHANGES:
        raise ValueError("unknown exchange emitter")
    data = log["data"]
    if len(data) != 2 + 7 * 64:
        raise ValueError("malformed OrderFilled event data")
    words = [int(data[2 + i * 64:2 + (i + 1) * 64], 16) for i in range(7)]
    if words[0] != 0 or words[1] != token_id or words[3] <= 0 or words[2] <= 0:
        raise ValueError("not a positive BUY of the expected outcome token")
    return words[3], words[2]


def collect(url, order_hash, funder, token_id, start, end):
    if start > end or end - start > 5000:
        raise ValueError("block range must be bounded to at most 5001 blocks")
    evidence = []
    # Small chunks avoid public-RPC block-range limits; any failure aborts the entire result.
    for first in range(start, end + 1, 100):
        last = min(first + 99, end)
        for exchange in EXCHANGES:
            logs = rpc_call(url, "eth_getLogs", [{
                "address": exchange,
                "fromBlock": hex(first), "toBlock": hex(last),
                "topics": [TOPIC, order_hash, "0x" + "0" * 24 + funder[2:]],
            }])
            if not isinstance(logs, list):
                raise ValueError("RPC returned non-list logs")
            for log in logs:
                shares, usdc = verify_log(log, order_hash, funder, token_id)
                tx = log["transactionHash"].lower()
                receipt = rpc_call(url, "eth_getTransactionReceipt", [tx])
                if not receipt or receipt.get("status") != "0x1" or receipt.get("blockHash") != log.get("blockHash"):
                    raise ValueError("log is not in a successful matching receipt")
                if not any(item.get("logIndex") == log.get("logIndex") and
                           item.get("transactionHash", "").lower() == tx for item in receipt["logs"]):
                    raise ValueError("receipt does not contain the matching log")
                evidence.append((tx, int(log["logIndex"], 16), shares, usdc))
    if not evidence:
        raise ValueError("no matching logs: never interpret absence as no fill")
    if len({(tx, index) for tx, index, _, _ in evidence}) != len(evidence):
        raise ValueError("duplicate log in RPC response")
    return sorted(evidence)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--order-hash", required=True)
    parser.add_argument("--funder", required=True)
    parser.add_argument("--token-id", required=True, type=int)
    parser.add_argument("--from-block", required=True, type=int)
    parser.add_argument("--to-block", required=True, type=int)
    parser.add_argument("--rpc", required=True, action="append")
    args = parser.parse_args()
    order_hash, funder = args.order_hash.lower(), args.funder.lower()
    if len(order_hash) != 66 or len(funder) != 42 or not all(c in "0123456789abcdef" for c in order_hash[2:] + funder[2:]):
        parser.error("invalid order hash or funder address")
    if len(args.rpc) < 2 or len(set(args.rpc)) != len(args.rpc):
        parser.error("provide at least two distinct RPC endpoints")
    results = [collect(url, order_hash, funder, args.token_id, args.from_block, args.to_block)
               for url in args.rpc]
    if any(result != results[0] for result in results[1:]):
        raise ValueError("independent RPC endpoints disagree: do not account a fill")
    evidence = results[0]
    shares = sum(entry[2] for entry in evidence)
    usdc = sum(entry[3] for entry in evidence)
    print(f"confirmed order_hash={order_hash} logs={len(evidence)} shares={Decimal(shares)/1000000} usdc={Decimal(usdc)/1000000}")
    for tx, index, quantity, principal in evidence:
        print(f"  tx={tx} log_index={index} shares={Decimal(quantity)/1000000} usdc={Decimal(principal)/1000000}")
    print("Read-only evidence only; no ledger update or fuse change was performed.")


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:
        print(f"INCOMPLETE CHAIN EVIDENCE: {exc}", file=sys.stderr)
        sys.exit(1)
