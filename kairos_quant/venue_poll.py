"""Strict operational facts for the read-only EVEDEX venue poller."""

from __future__ import annotations

from datetime import datetime
from enum import StrEnum
from typing import Literal, Self

from kairos_core.contracts.base import (
    StrictKairosMessage,
    canonical_sha256,
    datetime_from_unix_ms,
)
from pydantic import Field, model_validator

VENUE_POLL_TOPIC = "kairos.venue.poll.v1"


class VenuePollStatus(StrEnum):
    ATTEMPTED = "ATTEMPTED"
    SUCCEEDED = "SUCCEEDED"
    FAILED = "FAILED"


class VenuePollFactV1(StrictKairosMessage):
    """One durable, per-symbol poll attempt or terminal outcome.

    ``ATTEMPTED`` is persisted before public network I/O. A terminal fact is
    emitted only after the corresponding venue-quality fact is durable, so a
    reported success means the measurement is available to downstream audit
    and TCA consumers.
    """

    contract_version: Literal["venue-poll.v1"] = "venue-poll.v1"
    poll_id: str = Field(..., pattern=r"^[0-9a-f]{64}$")
    config_fingerprint: str = Field(..., pattern=r"^[0-9a-f]{64}$")
    status: VenuePollStatus
    binance_symbol: str = Field(..., pattern=r"^[A-Z0-9]{3,24}USDT$")
    venue_symbol: str = Field(..., pattern=r"^[A-Z0-9]{3,24}USD:DEV$")
    expected_symbols: tuple[str, ...] = Field(..., min_length=1, max_length=64)
    interval_ms: int = Field(..., gt=0, le=86_400_000)
    scheduled_at_ms: int = Field(..., gt=0)
    attempted_at_ms: int = Field(..., gt=0)
    completed_at_ms: int | None = Field(default=None, gt=0)
    failure_code: str | None = Field(default=None, pattern=r"^[A-Za-z][A-Za-z0-9_]{0,127}$")

    @model_validator(mode="after")
    def validate_outcome(self) -> Self:
        expected = tuple(sorted(set(self.expected_symbols)))
        if expected != self.expected_symbols:
            raise ValueError("expected_symbols must be sorted and unique")
        if self.venue_symbol not in self.expected_symbols:
            raise ValueError("venue_symbol must belong to expected_symbols")
        if self.attempted_at_ms < self.scheduled_at_ms:
            raise ValueError("attempt cannot predate its scheduled slot")
        if self.status is VenuePollStatus.ATTEMPTED:
            if self.completed_at_ms is not None or self.failure_code is not None:
                raise ValueError("attempt facts cannot contain an outcome")
        else:
            if self.completed_at_ms is None or self.completed_at_ms < self.attempted_at_ms:
                raise ValueError("terminal facts require a completion at or after the attempt")
            if (self.status is VenuePollStatus.FAILED) != (self.failure_code is not None):
                raise ValueError("failure_code is required only for failed outcomes")
        expected_message_id = venue_poll_message_id(
            self.poll_id,
            self.status,
            attempted_at_ms=self.attempted_at_ms,
            completed_at_ms=self.completed_at_ms,
            failure_code=self.failure_code,
        )
        if self.message_id != expected_message_id:
            raise ValueError("message_id does not match the poll outcome identity")
        return self


def venue_poll_id(
    *,
    source: str,
    config_fingerprint: str,
    scheduled_at_ms: int,
    binance_symbol: str,
    venue_symbol: str,
) -> str:
    return canonical_sha256(
        {
            "contract_version": "venue-poll-identity.v1",
            "source": source,
            "config_fingerprint": config_fingerprint,
            "scheduled_at_ms": scheduled_at_ms,
            "binance_symbol": binance_symbol,
            "venue_symbol": venue_symbol,
        }
    )


def venue_poll_message_id(
    poll_id: str,
    status: VenuePollStatus,
    *,
    attempted_at_ms: int,
    completed_at_ms: int | None,
    failure_code: str | None,
) -> str:
    return canonical_sha256(
        {
            "contract_version": "venue-poll-message-identity.v1",
            "poll_id": poll_id,
            "status": status.value,
            "attempted_at_ms": attempted_at_ms,
            "completed_at_ms": completed_at_ms,
            "failure_code": failure_code,
        }
    )


def build_venue_poll_fact(
    *,
    source: str,
    config_fingerprint: str,
    status: VenuePollStatus,
    binance_symbol: str,
    venue_symbol: str,
    expected_symbols: tuple[str, ...],
    interval_ms: int,
    scheduled_at_ms: int,
    attempted_at_ms: int,
    completed_at_ms: int | None = None,
    failure_code: str | None = None,
    produced_at: datetime | None = None,
) -> VenuePollFactV1:
    poll_id = venue_poll_id(
        source=source,
        config_fingerprint=config_fingerprint,
        scheduled_at_ms=scheduled_at_ms,
        binance_symbol=binance_symbol,
        venue_symbol=venue_symbol,
    )
    event_at_ms = completed_at_ms if completed_at_ms is not None else attempted_at_ms
    return VenuePollFactV1(
        source=source,
        message_id=venue_poll_message_id(
            poll_id,
            status,
            attempted_at_ms=attempted_at_ms,
            completed_at_ms=completed_at_ms,
            failure_code=failure_code,
        ),
        produced_at=produced_at or datetime_from_unix_ms(event_at_ms),
        poll_id=poll_id,
        config_fingerprint=config_fingerprint,
        status=status,
        binance_symbol=binance_symbol,
        venue_symbol=venue_symbol,
        expected_symbols=expected_symbols,
        interval_ms=interval_ms,
        scheduled_at_ms=scheduled_at_ms,
        attempted_at_ms=attempted_at_ms,
        completed_at_ms=completed_at_ms,
        failure_code=failure_code,
    )
