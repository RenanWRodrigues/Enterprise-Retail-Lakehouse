# Roadmap

Cada fase termina com o **Completion Report** definido no `CLAUDE.md`.
Nenhuma fase começa antes de a anterior cumprir a *Definition of Done*.

## Fase 0 — Fundação de engenharia ✅ (atual)

- `CLAUDE.md`, documentação de arquitetura e padrões.
- `pyproject.toml` com pytest, pytest-cov, ruff, mypy.
- Guardrails: `.gitignore`, `.claude/settings.json`, testes de governança e de segredos.
- Biblioteca de skills (`.claude/skills/`).
- Estrutura definitiva do monorepo (`projects/*`, `shared/`), imports configurados.
- **Fora de escopo**: qualquer código de pipeline ou dataset.

## Fase 1 — Plataforma e ambiente

- Verificar o workspace Databricks Free Edition: versão do runtime serverless,
  versão do Python, limites de catalogs/schemas, Volumes, acesso de saída à internet, Secrets.
- ✅ Infraestrutura de testes: `pyspark==4.0.4` (dev), fixture `spark` no `conftest.py` da raiz,
  `lakehouse_testing`, markers automáticos, coverage. Re-fixar a versão do PySpark após
  verificar o runtime.
- ✅ Build backend (pacotes instaláveis, mesmos imports; ADR 0005, 2026-10-13).
- `delta-spark` junto com o primeiro teste `integration`.
- ✅ Notebooks importam o pacote instalado (`%pip` do wheel num Volume; ADR 0005). Execução no workspace pendente.
- **Fora de escopo**: módulos em `lakehouse_common` (ver regra de admissão em `shared/README.md`).

## Fase 2 — Enterprise Retail Lakehouse (`projects/enterprise-retail-lakehouse`)

- Primeiras implementações de configuração tipada, logging, metadados Bronze e escrita
  idempotente — **dentro de `retail_lakehouse`**. Qualidade: ✅ framework em
  `lakehouse_common.quality`; o Retail declara os checks de cada tabela.
- ✅ Contratos de fonte (11 datasets, 5 sistemas) e gerador determinístico com anomalias
  configuráveis e registradas (`projects/enterprise-retail-lakehouse/docs/`).
- Lotes incrementais e mudanças de cadastro (para SCD Type 2) no gerador.
- ✅ Bronze: 11 tabelas `bronze_<dataset>` append-only, idempotentes por lote
  (`projects/enterprise-retail-lakehouse/docs/BRONZE.md`).
- Bronze: ingestão de arquivos com metadados.
- ✅ Silver: padronização pelo contrato da fonte, deduplicação, conflitos, quarentena com motivos,
  versões de cliente (SCD Type 2 na Gold), reconciliação por lote e incremental por tabela de
  controle (`projects/enterprise-retail-lakehouse/docs/SILVER.md`).
- ✅ Gold: 5 dimensões (SCD2 em `dim_customer`) e 4 fatos com grain documentado, KPIs e
  consultas analíticas (`docs/DIMENSIONAL_MODEL.md`, `sql/gold/`) — sobre o contrato da Silver.
- ✅ Testes de integração raw → Bronze → Silver → Gold com reexecução (idempotência) e lote
  incremental.

## Fase 3 — Industrial IoT Lakehouse (`projects/industrial-iot-lakehouse`)

- ✅ Contratos, limites físicos, modos de falha e gerador (Python + Spark) com ground truth
  (`projects/industrial-iot-lakehouse/docs/`).
- Promover a estrutura de contratos de fonte para `lakehouse_common` (exige alterar o Retail).

- Primeiro reuso real: promover para `lakehouse_common` o que o IoT precisar do Retail
  (código + testes movidos juntos, sem mudança de comportamento, registrado em ADR).
- Simulador de telemetria de sensores (temperatura, vibração, pressão) com eventos
  tardios, duplicados e fora de ordem.
- Ingestão incremental (Structured Streaming / `availableNow`), watermarks.
- Silver: deduplicação por evento, detecção de leituras inválidas.
- Gold: agregados por janela, disponibilidade de equipamentos, alertas de anomalia.

## Fase 4 — Brazilian Financial Data Lakehouse (`projects/brazilian-financial-lakehouse`)

- ✅ Fontes oficiais documentadas (`docs/data_sources.md`) e ingestão raw com clientes HTTP
  resilientes (`docs/RAW_INGESTION.md`); validada contra as APIs reais em 2026-10-06.
- ✅ Bronze incremental: checkpoint lógico, lookback para revisões, MERGE insert-only de versões
  novas (revisão que volta ao valor anterior incluída), `reprocess_from` para revisões tardias,
  métricas e falha controlada (`docs/INCREMENTAL_STRATEGY.md`).
- ✅ Silver: séries padronizadas (decimais exatos, datas por frequência, nulls, duplicados, revisões,
  quarentena) com MERGE incremental (`docs/SILVER.md`).
- ✅ Gold: tabelas de juros, inflação, câmbio, atividade e trabalho, painel macro mensal com regras de
  reamostragem, métricas derivadas e consultas analíticas (`docs/GOLD.md`); reamostragem e
  métricas validadas contra séries agregadas oficiais do BCB/IBGE (GOLD.md §7).

- Fontes públicas sem credenciais: Banco Central (SGS: Selic, IPCA, câmbio),
  CVM (dados abertos de fundos).
- Calendário de dias úteis brasileiro, tratamento de séries com revisões.
- Gold: séries de indicadores, rentabilidade de fundos vs. benchmarks.

## Transversal — Data Quality + Observability

- ✅ `pipeline_runs` e `data_quality_results`, política continua/alerta/falha, checks de schema e de
  lacunas, suítes críticas de Retail, IoT e Financial (`docs/OBSERVABILITY.md`,
  `docs/DATA_QUALITY.md` §8). Ligada aos pipelines Silver e Gold do Financial; Retail e IoT
  usarão as suítes quando suas Silvers existirem.
- ✅ `pipeline_runs` padronizado (`project`, `table`, `started_at`, `finished_at`) e integrado aos
  três projetos: Retail Bronze/Gold, IoT landing, Financial Bronze/Silver/Gold; sink JSON Lines
  para execução local (ADR 055).

## Fase 5 — Produção e apresentação do portfólio

- CI (GitHub Actions): lint, mypy, testes, scanner de segredos (gitleaks).
- Databricks Asset Bundles para deploy de jobs.
- README de cada projeto com diagrama, decisões e resultados.

## Questões em aberto

| Questão | Decidir em |
|---|---|
| Versão de Spark/Python do runtime serverless do Free Edition | Fase 1 |
| Um catalog por projeto vs. catalog único com schemas prefixados | Fase 1 |
