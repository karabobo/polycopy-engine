"""Offline failure-injection tests for strict v2 OrderFilled read-only evidence."""
import unittest
from unittest.mock import patch

from verify_gtd_chain_fill import TOPIC, EXCHANGES, collect, verify_log

HASH = "0x" + "a" * 64
FUNDER = "0x" + "b" * 40
TOKEN = 123456
TX = "0x" + "c" * 64
BLOCK_HASH = "0x" + "d" * 64


def fixture():
    words = [0, TOKEN, 355500, 2370000, 0, 0, 0]
    return {
        "address": EXCHANGES[0],
        "topics": [TOPIC, HASH, "0x" + "0" * 24 + FUNDER[2:], "0x" + "0" * 64],
        "data": "0x" + "".join(f"{word:064x}" for word in words),
        "transactionHash": TX,
        "logIndex": "0x1",
        "blockHash": BLOCK_HASH,
        "removed": False,
    }


class ChainFillEvidenceTests(unittest.TestCase):
    def test_exact_hash_maker_token_and_buy_amounts(self):
        self.assertEqual(verify_log(fixture(), HASH, FUNDER, TOKEN), (2370000, 355500))
        log = fixture()
        log["topics"][1] = "0x" + "1" * 64
        with self.assertRaisesRegex(ValueError, "mismatch"):
            verify_log(log, HASH, FUNDER, TOKEN)
        log = fixture()
        log["data"] = "0x" + "".join(f"{word:064x}" for word in [1, TOKEN, 355500, 2370000, 0, 0, 0])
        with self.assertRaisesRegex(ValueError, "BUY"):
            verify_log(log, HASH, FUNDER, TOKEN)

    def test_empty_or_failed_page_is_not_no_fill(self):
        with patch("verify_gtd_chain_fill.rpc_call", return_value=[]):
            with self.assertRaisesRegex(ValueError, "never interpret absence"):
                collect("https://rpc.example", HASH, FUNDER, TOKEN, 100, 100)
        with patch("verify_gtd_chain_fill.rpc_call", side_effect=TimeoutError("rpc unavailable")):
            with self.assertRaises(TimeoutError):
                collect("https://rpc.example", HASH, FUNDER, TOKEN, 100, 100)

    def test_receipt_failure_and_duplicate_logs_fail_closed(self):
        log = fixture()
        bad_receipt = {"status": "0x0", "blockHash": BLOCK_HASH, "logs": [log]}
        with patch("verify_gtd_chain_fill.rpc_call", side_effect=[[log], [], bad_receipt]):
            with self.assertRaisesRegex(ValueError, "successful"):
                collect("https://rpc.example", HASH, FUNDER, TOKEN, 100, 100)
        receipt = {"status": "0x1", "blockHash": BLOCK_HASH, "logs": [log]}
        with patch("verify_gtd_chain_fill.rpc_call", side_effect=[[log, log], receipt, receipt, []]):
            with self.assertRaisesRegex(ValueError, "duplicate"):
                collect("https://rpc.example", HASH, FUNDER, TOKEN, 100, 100)

    def test_unbounded_scan_rejected(self):
        with self.assertRaisesRegex(ValueError, "bounded"):
            collect("https://rpc.example", HASH, FUNDER, TOKEN, 1, 1000000)


if __name__ == "__main__":
    unittest.main()
