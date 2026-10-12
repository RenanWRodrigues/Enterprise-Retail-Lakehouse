---
name: data-quality
description: Use ao definir, implementar ou testar checks de qualidade de dados (completeness, uniqueness, validity, consistency, referential integrity, freshness, volume) com o framework lakehouse_common.quality — status PASS/WARNING/ERROR/CRITICAL, quarentena e métricas data_quality_results / pipeline_runs.
---

# Data Quality

Modelo completo (onde as regras atuam, quarentena, tabela `data_quality_results`): `docs/DATA_QUALITY.md`;
execuções, alertas e quando o pipeline continua/alerta/falha: `docs/OBSERVABILITY.md`.

## Dimensões

| Dimensão | Definição operacional | Implementação típica (Spark nativo) |
|---|---|---|
| completeness | campo obrigatório preenchido (não null, não vazio após trim) | `F.col(c).isNotNull() & (F.trim(c) != "")` |
| uniqueness | chave única **no grain declarado** | `count` por chave `> 1` via `groupBy` ou janela |
| validity | valor dentro de domínio, faixa ou formato | `isin(...)`, `between`, `rlike(...)` |
| consistency | campos/linhas coerentes entre si | `delivered_at >= shipped_at`; total = soma das linhas |
| referential integrity | toda FK existe na tabela referenciada | `left_anti` join contra a dimensão/entidade |
| freshness | dado mais recente dentro do atraso aceito | `max(event_ts)` vs. agora (UTC) vs. limite configurado |

Volume (contagem plausível por lote) é tratado como `WARNING` adicional.

## Status de uma verificação

Cada regra tem uma **severidade** (`WARNING`, `ERROR` ou `CRITICAL`). O **status** resultante
de cada execução da regra é:

| Status | Quando | Ação no pipeline |
|---|---|---|
| `PASS` | taxa de falha ≤ limiar configurado | nenhuma; métrica registrada |
| `WARNING` | regra de severidade WARNING excedeu o limiar | registros seguem; métrica + alerta no log |
| `ERROR` | regra de severidade ERROR reprovou registros | registros reprovados → quarentena; pipeline continua |
| `CRITICAL` | regra de severidade CRITICAL reprovou | **pipeline falha** antes de escrever a camada |

Guia de severidade:
- `CRITICAL`: schema incompatível, chave primária nula/duplicada no destino, fonte vazia
  quando não deveria, reconciliação Gold × Silver divergente.
- `ERROR`: valor fora do domínio, formato inválido, cast que transforma valor em null.
- `WARNING`: outlier, volume atípico, freshness acima do esperado mas tolerável,
  FK sem correspondência na Silver (a entidade referenciada pode chegar atrasada).

Limiares e severidades ficam na configuração (`config/` do projeto), nunca hard-coded.
Framework: `lakehouse_common.quality` (domínio-agnóstico). Os checks de cada tabela são
declarados no projeto (`src/<pkg>/quality/`), com severidade/limiar vindos de `config/`.

## Como usar

1. Declarar checks do catálogo: `NotNull`, `Unique`, `AcceptedValues`, `Range`, `Regex`,
   `ReferentialIntegrity`, `RowCount`, `CustomCondition` (semântica de NULL e exemplos em
   `docs/DATA_QUALITY.md` §5).
2. `run_checks(df, checks, table_name=..., execution_timestamp=...)` — uma ação Spark.
3. `raise_on_critical(results)` antes de escrever a camada.
4. `split_by_quality(...)` → válidos + quarentena (`_dq_failed_checks`, `_dq_execution_timestamp`).
5. Dentro de `track_run(...)`: `step.record_quality(run_suite(...))` grava `data_quality_results` e `pipeline_runs`
   (CRITICAL levanta antes da escrita).
6. Check que não existe no catálogo: `CustomCondition`; se o padrão se repetir em 2+ tabelas,
   adicione um tipo ao framework (com testes em `shared/tests/`).

## Regras práticas

- Uniqueness sempre referenciando o grain (`(order_id, line_number)`, não "linhas distintas").
- Referential integrity: na Silver é `WARNING` por padrão (dado atrasado não é dado inválido;
  elevar para `ERROR` via configuração quando a fonte garantir a ordem); na Gold, FK ausente
  aponta para o membro "desconhecido" da dimensão e gera métrica (skill `dimensional-modeling`).
- Freshness compara timestamps em UTC; o limite é configuração por fonte.
- Nunca "consertar" dado inválido silenciosamente para fazê-lo passar.
- Nenhum `count()` por check; nenhum `collect()` de registros reprovados.
- NULL só falha em `NotNull`/`CustomCondition`: combine `NotNull` + outro check quando a
  coluna for obrigatória (cada defeito contado uma vez).

## Testes obrigatórios

- Novo tipo de check (em `shared/tests/`): zero falhas, algumas, 100%, NULL, duplicatas,
  fronteiras, dataset vazio; `CRITICAL` interrompe, `ERROR` quarentena, `WARNING` não bloqueia.
- Checks de uma tabela (em `projects/<p>/tests/data_quality/`): pegam o defeito esperado e
  não geram falso positivo nos dados válidos.
