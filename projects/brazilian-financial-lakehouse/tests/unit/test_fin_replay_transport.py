"""Recorded API responses replayed from a directory (Unity Catalog Volume fallback)."""

from datetime import UTC, datetime
from pathlib import Path

import pytest

from financial_lakehouse.clients.http import HttpClient, HttpResponse, Transport, urllib_transport
from financial_lakehouse.clients.replay import (
    RECORDED_AT_FILE,
    ReplayMissError,
    recorded_at,
    recording_transport,
    replay_transport,
    transport_for,
)

URL = "https://api.bcb.gov.br/dados/serie/bcdata.sgs.189/dados?formato=json"
AT = datetime(2026, 10, 8, 12, tzinfo=UTC)


def _network(calls: list[str]) -> Transport:
    def transport(url: str, timeout: float) -> HttpResponse:
        del timeout
        calls.append(url)
        return HttpResponse(url, 200, {"Content-Type": "application/json"}, b'[{"valor": "1.06"}]')

    return transport


def test_a_recorded_response_is_replayed_byte_for_byte_without_network(tmp_path: Path) -> None:
    calls: list[str] = []
    recorded = recording_transport(_network(calls), tmp_path, at=AT)

    original = recorded(URL, 30.0)
    replayed = replay_transport(tmp_path)(URL, 30.0)

    assert calls == [URL]  # replay made no request
    assert (replayed.url, replayed.status, replayed.body) == (URL, 200, original.body)
    assert replayed.headers == {"Content-Type": "application/json"}


def test_an_unrecorded_request_fails_without_retries(tmp_path: Path) -> None:
    replay = replay_transport(tmp_path)

    with pytest.raises(ReplayMissError, match="not recorded"):
        replay(URL, 30.0)
    assert not issubclass(ReplayMissError, (TimeoutError, ConnectionError))  # not retried


def test_the_replay_goes_through_the_real_http_client(tmp_path: Path) -> None:
    recording_transport(_network([]), tmp_path, at=AT)(URL, 30.0)

    client = HttpClient(transport=replay_transport(tmp_path))

    assert client.get_json(URL) == [{"valor": "1.06"}]


def test_the_recording_keeps_its_clock_for_the_replay(tmp_path: Path) -> None:
    """Request windows depend on 'now': the replay must run at the recording instant."""
    recording_transport(_network([]), tmp_path, at=AT)

    assert recorded_at(tmp_path) == AT
    assert (tmp_path / RECORDED_AT_FILE).is_file()


def test_a_directory_without_recording_has_no_clock(tmp_path: Path) -> None:
    with pytest.raises(ReplayMissError, match="no recording"):
        recorded_at(tmp_path)


def test_the_api_source_uses_the_network_and_the_current_time() -> None:
    assert transport_for("api", None) == (urllib_transport, None)


def test_the_replay_source_runs_at_the_recording_instant(tmp_path: Path) -> None:
    recording_transport(_network([]), tmp_path, at=AT)(URL, 30.0)

    transport, at = transport_for("replay", tmp_path)

    assert at == AT and transport(URL, 30.0).status == 200


@pytest.mark.parametrize(("source", "root"), [("replay", None), ("ftp", None)])
def test_an_incomplete_source_choice_is_refused(source: str, root: Path | None) -> None:
    with pytest.raises(ValueError, match=r"replay|unknown source"):
        transport_for(source, root)
