# shared — `lakehouse_common`

Biblioteca comum aos três Lakehouses. **Começa vazia de propósito.**

## Regra de admissão

Código entra em `shared/` somente quando:

1. já existe (e está testado) em um projeto **e** um **segundo** projeto precisa do mesmo
   comportamento — **ou** há decisão explícita do responsável, registrada em ADR; **e**
2. não carrega regra de negócio de nenhum domínio específico.

A promoção move o código e os testes juntos, sem mudar comportamento, e é
registrada em `docs/ARCHITECTURE.md` §7.

## Áreas

| Subpacote | Candidatos (só após cumprir a regra) |
|---|---|
| `logging` | ✅ logging estruturado JSON, redação e webhook de alertas, usados pelos 3 projetos (decisão 082; `docs/OBSERVABILITY.md` §6) |
| `quality` | ✅ framework de checks de qualidade (ADR 016; `docs/DATA_QUALITY.md` §5) |
| `fingerprint` | ✅ impressão digital de linha (SHA-256, NULL posicional), usada por Retail (`_row_hash`) e IoT (`_content_hash`); contrato no módulo (decisão 088) |
| `spark` | helpers de escrita Delta idempotente (sessão de teste vive em `lakehouse_testing`) |
| `observability` | ✅ `pipeline_runs` / `data_quality_results`, usados pelos 3 projetos (ADR 044, 055; `docs/OBSERVABILITY.md`) |
| `incremental` | **proposto** (ADR 0004, aguarda aceite): descoberta de unidades novas do Bronze por CDF + checkpoint, usada por Retail e IoT |

## `lakehouse_testing` (somente testes)

`shared/src/lakehouse_testing/` contém infraestrutura de teste usada por todas as suítes
(sessão Spark local, categorização de testes por diretório). Não é biblioteca de runtime:
código em `lakehouse_common` e `projects/*/src` nunca a importa.

## Fronteiras

- `lakehouse_common` nunca importa de `projects/`.
- Testes ficam em `shared/tests/<categoria>/` (mesmas categorias dos projetos).
