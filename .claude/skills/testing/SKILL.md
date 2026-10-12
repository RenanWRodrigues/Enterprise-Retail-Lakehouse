---
name: testing
description: Use ao escrever, organizar ou executar testes (unit, transformation, data_quality, contract, integration), ao testar transformações PySpark com DataFrames pequenos e determinísticos, ou antes de declarar qualquer tarefa concluída.
---

# Testing

Estratégia completa e comandos: `docs/TESTING_STRATEGY.md`.

## Fluxo

1. **RED**: escreva o teste e rode — ele deve falhar pelo motivo esperado.
2. **GREEN**: implementação mínima até passar.
3. **REFACTOR**: limpe com a suíte verde.
4. Rode a categoria afetada, depois a suíte inteira, `ruff check`, `ruff format --check`, `mypy`.
5. Reporte números reais (passaram / falharam / ignorados).
6. **Bug corrigido = regression test** (quando aplicável): o teste reproduz o bug e falha
   antes da correção.

## Qual categoria?

| Estou testando… | Diretório |
|---|---|
| Função Python pura (config, parsing, calendário, cálculo) | `projects/<p>/tests/unit/` |
| Função `DataFrame -> DataFrame` | `projects/<p>/tests/transformation/` |
| Regra de qualidade ou o orquestrador de DQ | `projects/<p>/tests/data_quality/` |
| Schema de saída / contrato de tabela do projeto | `projects/<p>/tests/contract/` (criar no 1º contrato) |
| Pipeline entre camadas com Delta local; reexecução; lote incremental | `projects/<p>/tests/integration/` |
| Código de `lakehouse_common` | `shared/tests/<categoria>/` |
| Governança do repositório (estrutura, docs, skills, segredos) | `tests/contract/` (raiz) |

O marcador é aplicado pelo diretório abaixo de `tests/` (`conftest.py` da raiz);
não decore manualmente (teste fora de `<área>/tests/<categoria>/` é erro de coleta).
Loop curto: `pytest -m unit`; sem lentos: `pytest -m "not integration"`. Teste importa o pacote como produção: `from retail_lakehouse... import ...`.

## Teste de transformação: Arrange / Act / Assert

```python
def test_deduplicate_orders_keeps_latest_version(spark: SparkSession) -> None:
    # Arrange
    orders = spark.createDataFrame(
        [
            ("o-1", datetime(2026, 1, 1, tzinfo=UTC), "created"),
            ("o-1", datetime(2026, 1, 2, tzinfo=UTC), "paid"),
            ("o-2", datetime(2026, 1, 1, tzinfo=UTC), "created"),
        ],
        schema=ORDERS_SCHEMA,
    )

    # Act
    result = deduplicate_orders(orders)

    # Assert
    assertSchemaEqual(result.schema, ORDERS_SCHEMA)
    assertDataFrameEqual(
        result.select("order_id", "status"),
        [
            Row(order_id="o-1", status="paid"),
            Row(order_id="o-2", status="created"),
        ],
    )
```

Asserções nativas de `pyspark.testing` (ignoram ordem, tratam null); sem helpers próprios.
`collect()` é aceitável **somente em testes**, sobre DataFrames de poucas linhas — e
timestamps coletados vêm no fuso local do processo Python: compare via `F.date_format`.

## Regras para os dados de teste

- Pequenos: 2–10 linhas, cada uma com propósito.
- Determinísticos: valores literais, datas com timezone, sem `random`, sem `now()`
  (injete o relógio/valor de referência como parâmetro).
- Schema explícito no `createDataFrame` — nunca inferido.
- Constantes ou funções construtoras no módulo de teste; fixture só com setup/teardown real
  (hoje: `spark` e `repo_root`, no `conftest.py` da raiz). Sem arquivos opacos.
- Nenhum acesso a rede, workspace Databricks ou credencial; HTTP é simulado (skill `api-ingestion`).

## O que todo teste de transformação verifica

| Aspecto | Como |
|---|---|
| input | o Arrange representa exatamente o cenário nomeado no teste |
| output | `assertDataFrameEqual(result, expected)` |
| schema | `assertSchemaEqual(result.schema, EXPECTED_SCHEMA)` (nomes, tipos, nulabilidade) |
| edge cases | testes separados para: null, string vazia/espaços, duplicata, empate na ordenação, valor fora do domínio, limites de data/timezone, DataFrame vazio |

## Convenções

- Nome descreve o comportamento: `test_<função>_<comportamento>_<condição>`.
- Um comportamento por teste; `pytest.mark.parametrize` para variações do mesmo comportamento.
- Fixture `spark` (sessão, `local[2]`, UTC) do `conftest.py` da raiz; não crie
  `SparkSession` no teste nem altere `spark.conf`/temp views globais (sessão compartilhada).
- Lógica sem Spark é testada como `unit` (rápida); só use `spark` quando necessário.
- Nomes de arquivos de teste únicos no monorepo (exigência do mypy).
- Cobertura: `pytest --cov` (piso 85%); priorize lógica crítica, nunca teste só para o número.
- Integration: rodar o lote A duas vezes deve dar o mesmo estado; o lote B aplica só o delta.
- Teste quebrado não é "pulado" para passar a suíte; `skip`/`xfail` exige motivo e issue.
