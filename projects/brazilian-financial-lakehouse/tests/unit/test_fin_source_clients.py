"""Source clients (BCB SGS, BCB PTAX, IBGE agregados) against recorded official payloads."""

import json
from collections.abc import Callable
from datetime import date
from typing import Any
from urllib.parse import unquote

import pytest

from financial_lakehouse.clients import (
    IbgeAgregadosClient,
    PtaxClient,
    ResponseSchemaError,
    SgsClient,
    date_windows,
)
from financial_lakehouse.clients.http import HttpClient, HttpResponse, HttpStatusError

Respond = Callable[..., HttpResponse]
Payload = Callable[[str], bytes]


# --- SGS -----------------------------------------------------------------------------------


def test_date_windows_never_exceed_ten_years() -> None:
    windows = date_windows(date(2000, 1, 1), date(2025, 6, 30), years=10)
    assert windows == [
        (date(2000, 1, 1), date(2009, 12, 31)),
        (date(2010, 1, 1), date(2019, 12, 31)),
        (date(2020, 1, 1), date(2025, 6, 30)),
    ]


def test_date_windows_handle_leap_days() -> None:
    assert date_windows(date(2016, 2, 29), date(2030, 1, 1), years=10)[0] == (
        date(2016, 2, 29),
        date(2026, 2, 27),
    )


def test_sgs_builds_the_documented_url_and_returns_raw_pages(
    http: HttpClient, transport: Any, respond: Respond, payload: Payload
) -> None:
    transport.outcomes.append(respond(200, payload("sgs_189_2025q1.json")))

    [page] = SgsClient(http).fetch(189, date(2025, 1, 1), date(2025, 3, 31))

    assert unquote(transport.calls[0]) == (
        "https://api.bcb.gov.br/dados/serie/bcdata.sgs.189/dados"
        "?formato=json&dataInicial=01/01/2025&dataFinal=31/03/2025"
    )
    assert page.body == payload("sgs_189_2025q1.json")  # raw bytes kept untouched
    assert page.records[0] == {"data": "01/01/2025", "valor": "0.27"}
    assert (page.window_start, page.window_end) == (date(2025, 1, 1), date(2025, 3, 31))


def test_sgs_requests_one_call_per_ten_year_window(
    http: HttpClient, transport: Any, respond: Respond
) -> None:
    transport.route(lambda url: "bcdata.sgs.11" in url, lambda url: respond(200, b"[]"))
    pages = SgsClient(http).fetch(11, date(2000, 1, 1), date(2025, 1, 1))
    assert len(pages) == len(transport.calls) == 3


def test_sgs_not_found_window_is_empty_only_with_the_documented_message(
    http: HttpClient, transport: Any, respond: Respond, payload: Payload
) -> None:
    transport.outcomes.append(respond(404, payload("sgs_not_found.json")))
    [page] = SgsClient(http).fetch(189, date(2030, 1, 1), date(2030, 1, 31))
    assert page.records == ()

    transport.outcomes.append(respond(404, b'{"erro": {"detail": "serie inexistente"}}'))
    with pytest.raises(HttpStatusError):
        SgsClient(http).fetch(999999, date(2030, 1, 1), date(2030, 1, 31))


@pytest.mark.parametrize(
    "body", [b'{"data": "x"}', b'[{"data": "01/01/2025"}]', b'[{"data": 1, "valor": "2"}]']
)
def test_sgs_rejects_unexpected_payloads(
    http: HttpClient, transport: Any, respond: Respond, body: bytes
) -> None:
    transport.outcomes.append(respond(200, body))
    with pytest.raises(ResponseSchemaError):
        SgsClient(http).fetch(189, date(2025, 1, 1), date(2025, 3, 31))


# --- PTAX ----------------------------------------------------------------------------------


def test_ptax_builds_the_odata_url_with_documented_date_format(
    http: HttpClient, transport: Any, respond: Respond, payload: Payload
) -> None:
    transport.outcomes.append(respond(200, payload("ptax_eur_2025_01_02.json")))

    [page] = PtaxClient(http, page_size=100).fetch("EUR", date(2025, 1, 2), date(2025, 1, 3))

    url = unquote(transport.calls[0])
    assert url.startswith(
        "https://olinda.bcb.gov.br/olinda/servico/PTAX/versao/v1/odata/"
        "CotacaoMoedaPeriodo(moeda=@moeda,dataInicial=@dataInicial,dataFinalCotacao=@dataFinalCotacao)?"
    )
    for part in (
        "@moeda='EUR'",
        "@dataInicial='01-02-2025'",
        "@dataFinalCotacao='01-03-2025'",
        "$format=json",
        "$top=100",
        "$skip=0",
        "$orderby=dataHoraCotacao",
    ):
        assert part in url
    assert page.records[0]["tipoBoletim"] == "Abertura"


def test_ptax_paginates_until_a_short_page(
    http: HttpClient, transport: Any, respond: Respond
) -> None:
    def item(n: int) -> dict[str, object]:
        return {
            "cotacaoCompra": n,
            "cotacaoVenda": n,
            "paridadeCompra": 1,
            "paridadeVenda": 1,
            "dataHoraCotacao": f"2025-01-02 10:00:0{n}.000",
            "tipoBoletim": "Abertura",
        }

    pages = [[item(1), item(2)], [item(3), item(4)], [item(5)]]
    transport.outcomes.extend(respond(200, json.dumps({"value": p}).encode()) for p in pages)

    result = PtaxClient(http, page_size=2).fetch("EUR", date(2025, 1, 2), date(2025, 1, 2))

    assert [len(p.records) for p in result] == [2, 2, 1]
    assert [unquote(c).split("$skip=")[1].split("&")[0] for c in transport.calls] == ["0", "2", "4"]


def test_ptax_splits_long_periods_into_yearly_windows(
    http: HttpClient, transport: Any, respond: Respond
) -> None:
    transport.route(
        lambda url: "CotacaoMoedaPeriodo" in url, lambda url: respond(200, b'{"value": []}')
    )
    PtaxClient(http).fetch("EUR", date(2023, 6, 1), date(2025, 2, 1))
    assert len(transport.calls) == 2  # [2023-06-01, 2024-05-31] and [2024-06-01, 2025-02-01]


def test_ptax_rejects_payload_without_value(
    http: HttpClient, transport: Any, respond: Respond
) -> None:
    transport.outcomes.append(respond(200, b'{"error": "x"}'))
    with pytest.raises(ResponseSchemaError):
        PtaxClient(http).fetch("EUR", date(2025, 1, 2), date(2025, 1, 3))


# --- IBGE ----------------------------------------------------------------------------------


def test_ibge_lists_available_periods(
    http: HttpClient, transport: Any, respond: Respond, payload: Payload
) -> None:
    transport.outcomes.append(respond(200, payload("ibge_6381_periodos.json")))

    periods = IbgeAgregadosClient(http).periods(6381)

    assert transport.calls == ["https://servicodados.ibge.gov.br/api/v3/agregados/6381/periodos"]
    assert periods == ["201203", "201204", "201205"]


def test_ibge_fetches_variable_values_for_explicit_periods(
    http: HttpClient, transport: Any, respond: Respond, payload: Payload
) -> None:
    transport.outcomes.append(respond(200, payload("ibge_1737_v63.json")))

    [page] = IbgeAgregadosClient(http).fetch(1737, 63, ["202401", "202402"])

    assert unquote(transport.calls[0]) == (
        "https://servicodados.ibge.gov.br/api/v3/agregados/1737/periodos/202401|202402"
        "/variaveis/63?localidades=N1[all]"
    )
    assert page.records == (
        {"periodo": "202401", "valor": "0.42", "localidade": "1"},
        {"periodo": "202402", "valor": "0.83", "localidade": "1"},
    )


def test_ibge_passes_the_classification_filter(
    http: HttpClient, transport: Any, respond: Respond
) -> None:
    transport.outcomes.append(respond(200, b"[]"))
    IbgeAgregadosClient(http).fetch(5932, 6561, ["202601"], classificacao="11255[90707]")
    assert unquote(transport.calls[0]).endswith("?localidades=N1[all]&classificacao=11255[90707]")


def test_ibge_chunks_periods_to_respect_the_value_limit(
    http: HttpClient, transport: Any, respond: Respond
) -> None:
    transport.route(lambda url: "/variaveis/" in url, lambda url: respond(200, b"[]"))
    periods = [f"{2000 + i // 12}{i % 12 + 1:02d}" for i in range(250)]

    pages = IbgeAgregadosClient(http, max_periods_per_request=100).fetch(1737, 63, periods)

    assert len(pages) == len(transport.calls) == 3


def test_ibge_rejects_unexpected_payloads(
    http: HttpClient, transport: Any, respond: Respond
) -> None:
    transport.outcomes.append(respond(200, b'[{"id": "63", "resultados": "x"}]'))
    with pytest.raises(ResponseSchemaError):
        IbgeAgregadosClient(http).fetch(1737, 63, ["202401"])
