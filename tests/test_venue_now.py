"""On-demand PROD venue measurement from both public books (synthetic transport)."""

from __future__ import annotations

import asyncio
import json

import pytest
from kairos_core.enums import EvedexProfile

from kairos_quant import venue_now
from kairos_quant.venue_now import measure_prod_venue

NOW_MS = 1_000_000


def _fetch(requests):
    async def fetch(url, params):
        requests.append((url, params))
        if "evedex" in url:
            body = {
                "t": NOW_MS,
                "bids": [{"price": "99.95", "quantity": "20"}],
                "asks": [{"price": "100.05", "quantity": "20"}],
            }
            return body, 40.0
        return {"T": NOW_MS, "bids": [["99.95", "20"]], "asks": [["100.05", "20"]]}, 30.0

    return fetch


def test_measures_the_reviewed_prod_pair_from_both_books_now():
    requests = []
    venue = asyncio.run(
        measure_prod_venue(
            "ZECUSDT",
            taker_fee_bps=5.0,
            assessed_notional_usd=200.0,
            fetch=_fetch(requests),
            now_ms=lambda: NOW_MS + 100,
        )
    )
    assert venue.profile is EvedexProfile.PROD and venue.symbol == "ZECUSD"
    assert venue.observed_at_ms == NOW_MS + 100 and venue.expires_at_ms == NOW_MS + 5_100
    assert requests[0][0].endswith("/api/market/ZECUSD/deep")
    assert requests[1][1] == {"symbol": "ZECUSDT", "limit": "100"}


def test_refuses_an_unreviewed_pair_before_any_request():
    requests = []
    with pytest.raises(ValueError, match="outside the approved PROD symbol map"):
        asyncio.run(
            measure_prod_venue("LTCUSDT", taker_fee_bps=5, assessed_notional_usd=200, fetch=_fetch(requests))
        )
    assert requests == []


def test_command_line_failure_is_one_json_error_line(capsys):
    assert (
        venue_now.main(["--signal", "LTCUSDT", "--taker-fee-bps", "5", "--assessed-notional-usd", "200"]) == 1
    )
    assert "outside the approved PROD symbol map" in json.loads(capsys.readouterr().out)["error"]
