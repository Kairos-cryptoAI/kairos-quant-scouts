"""Explicit Binance UM WebSocket traffic routing; never a mixed legacy socket."""

from typing import Literal
from urllib.parse import urlsplit

StreamRoute = Literal["public", "market"]
STREAM_ROUTES: tuple[StreamRoute, ...] = ("public", "market")


def websocket_root(value: str) -> str:
    """Accept a root or the old /stream setting, but always dial routed URLs.

    A legacy configuration is migrated explicitly, not used as a network
    fallback. Routed paths, embedded subscriptions and credentials are invalid.
    Plain WS is reserved for loopback integration tests.
    """
    if not value or any(character.isspace() for character in value):
        raise ValueError("Binance WebSocket base must be an absolute root URL")
    parsed = urlsplit(value)
    if (
        parsed.scheme not in {"ws", "wss"}
        or not parsed.hostname
        or parsed.username is not None
        or parsed.password is not None
        or parsed.query
        or parsed.fragment
        or parsed.path not in {"", "/", "/stream", "/stream/"}
        or (parsed.scheme == "ws" and parsed.hostname not in {"localhost", "127.0.0.1", "::1"})
    ):
        raise ValueError("Binance WebSocket base must be a root URL without route, query or credentials")
    # Validate a malformed/out-of-range port before any connection is attempted.
    _ = parsed.port
    return f"{parsed.scheme}://{parsed.netloc}"
