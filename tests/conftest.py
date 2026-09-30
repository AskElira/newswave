from __future__ import annotations

import ipaddress
import os
import socket

import pytest

from newswave.clock import ReplayClock
from newswave.db import Database

_PREFIXES = ("ALPACA_", "APCA_", "ANTHROPIC_")


@pytest.fixture(autouse=True)
def _scrub_env(monkeypatch):
    for k in list(os.environ):
        if k.startswith(_PREFIXES):
            monkeypatch.delenv(k, raising=False)


def _is_local(addr) -> bool:
    if isinstance(addr, (str, bytes)):  # unix socket path
        return True
    host = addr[0]
    if host in ("localhost", ""):
        return True
    try:
        return ipaddress.ip_address(host).is_loopback
    except ValueError:
        return False


@pytest.fixture(autouse=True)
def _no_network(monkeypatch):
    real = socket.socket.connect

    def guarded(self, addr):
        if not _is_local(addr):
            raise RuntimeError(f"network blocked in tests: {addr!r}")
        return real(self, addr)

    monkeypatch.setattr(socket.socket, "connect", guarded)


@pytest.fixture
def tmp_db(tmp_path):
    db = Database(tmp_path / "t.db")
    db.init_schema()
    yield db
    db.close()


@pytest.fixture
def replay_clock():
    return ReplayClock()
