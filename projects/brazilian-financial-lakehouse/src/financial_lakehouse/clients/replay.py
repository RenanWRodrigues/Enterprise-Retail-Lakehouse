"""Record API responses once, replay them later: the fallback when a workspace has no internet.

``recording_transport`` wraps the real transport and stores each response (body, status,
headers) under a directory, keyed by the SHA-256 of the full URL. ``replay_transport`` serves
them back from that directory (e.g. a Unity Catalog Volume) through the same ``HttpClient``, so
the ingestion code that runs is the production code. Request windows depend on the run's clock,
so a replay must run with the recording instant (``recorded_at``): the same URLs are requested.

The replay is not the source: it reproduces what the APIs answered at the recording instant.
"""

import hashlib
import json
from datetime import datetime
from pathlib import Path
from typing import Final

from financial_lakehouse.clients.http import HttpResponse, Transport, urllib_transport
from financial_lakehouse.storage.files import atomic_write, atomic_write_text

RECORDED_AT_FILE: Final = "_recorded_at.txt"


class ReplayMissError(RuntimeError):
    """A request (or the recording itself) is not in the replay directory. Not retried."""


def _key(url: str) -> str:
    return hashlib.sha256(url.encode("utf-8")).hexdigest()


def recording_transport(inner: Transport, root: Path, *, at: datetime) -> Transport:
    """``inner`` that also saves every response under ``root`` (and the recording instant)."""
    atomic_write_text(root / RECORDED_AT_FILE, at.isoformat())

    def record(url: str, timeout: float) -> HttpResponse:
        response = inner(url, timeout)
        key = _key(url)
        atomic_write(root / f"{key}.body", response.body)
        meta = {"url": url, "status": response.status, "headers": dict(response.headers)}
        atomic_write_text(root / f"{key}.meta.json", json.dumps(meta, sort_keys=True))
        return response

    return record


def replay_transport(root: Path) -> Transport:
    """Serve the responses recorded under ``root``; an unrecorded URL raises ``ReplayMissError``."""

    def replay(url: str, timeout: float) -> HttpResponse:
        del timeout
        key = _key(url)
        meta_path, body_path = root / f"{key}.meta.json", root / f"{key}.body"
        if not meta_path.is_file() or not body_path.is_file():
            raise ReplayMissError(f"request not recorded under {root}: {url}")
        meta = json.loads(meta_path.read_text(encoding="utf-8"))
        return HttpResponse(url, int(meta["status"]), meta["headers"], body_path.read_bytes())

    return replay


def recorded_at(root: Path) -> datetime:
    """The instant the recording ran at: the clock to replay with."""
    path = root / RECORDED_AT_FILE
    if not path.is_file():
        raise ReplayMissError(f"no recording under {root} ({RECORDED_AT_FILE} missing)")
    return datetime.fromisoformat(path.read_text(encoding="utf-8").strip())


API, REPLAY = "api", "replay"
SOURCES: Final = (API, REPLAY)


def transport_for(source: str, replay_root: Path | None) -> tuple[Transport, datetime | None]:
    """The transport of a run and, for a replay, the instant to run at.

    ``api``: the network, current time. ``replay``: the recording under ``replay_root``, run at
    the recording instant (the same request windows, so every request is found).
    """
    if source == API:
        return urllib_transport, None
    if source == REPLAY:
        if replay_root is None:
            raise ValueError("a replay needs the directory of the recorded responses")
        return replay_transport(replay_root), recorded_at(replay_root)
    raise ValueError(f"unknown source {source!r}; expected one of {SOURCES}")
