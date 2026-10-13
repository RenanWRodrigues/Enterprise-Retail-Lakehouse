# Compatibility Matrix — código × Databricks Free Edition

Estado em 2026-10-08 (célula de tamanho dos dados corrigida em 2026-10-12). Versões e estratégia de
dependências: [compatibility.md](compatibility.md). Três colunas de evidência, que não se confundem:

- **Local**: verificado nesta máquina (Spark 4.0.4 + Delta 4.0.1, Linux/WSL), com o teste citado.
- **Free Edition**: o que se espera do workspace. "Documentado" = afirmado pela documentação da
  Databricks, **não executado aqui**.
- **Como confirmar**: o check do preflight (`shared/notebooks/00_preflight.py`) ou o passo do
  smoke que prova o item no workspace.

| Recurso usado | Onde | Local | Free Edition | Como confirmar |
|---|---|---|---|---|
| Python ≥ 3.12 | todo o código (`requires-python`) | ✅ 3.12 | depende da versão de ambiente serverless escolhida; **não verificado** | `runtime` |
| Dependências além de `pyspark` | — | ✅ nenhuma (varredura de imports) | nada a instalar | — |
| Spark Connect (serverless) | todo o Spark de produção | ✅ sem `sparkContext`, RDD, `_jvm`, UDF Python, cache (varredura + contrato `test_serverless_compatibility.py`) | serverless roda sobre Spark Connect (documentado) | qualquer etapa |
| Sem cache/persist/checkpoint | produção | ✅ contrato | APIs de cache lançam exceção no serverless (documentado; ADR 0001) | qualquer etapa |
| Catálogo `workspace` | ambiente `smoke` | n/a (catálogo local `spark_catalog`) | catálogo padrão do Free Edition (documentado) | `catalog` |
| `CREATE SCHEMA` | todas as camadas | ✅ | permissão no catálogo `workspace` **não verificada** | `create_schema` |
| Unity Catalog Volumes + E/S POSIX em `/Volumes` | landing Retail, landing/replay Financial | ❌ não existe localmente (o preflight reporta falha, testado) | documentado | `volume_files` |
| Delta `MERGE` | Silver, Bronze financeiro, checkpoints | ✅ | documentado | smoke |
| Delta Change Data Feed + `table_changes` | Silver financeira (ADR 0002) | ✅ (`test_fin_silver_progress_delta.py`, sondagem de 2026-10-08) | documentado para tabelas gerenciadas do UC; **não verificado** | `delta_change_feed` |
| `DESCRIBE HISTORY` / `VERSION AS OF` | Silver financeira, testes | ✅ | documentado | `delta_change_feed` |
| `COMMENT ON TABLE` | Gold | ✅ | documentado | smoke Retail |
| `createOrReplace` (DataFrameWriterV2) | Gold, staging | ✅ | documentado | smoke Retail |
| `dbutils.widgets` | todos os notebooks | n/a | documentado | qualquer notebook |
| `dbutils.notebook.run` | `00_run_pipeline.py` | n/a | **não verificado** no serverless do Free Edition | runner; alternativa: etapas à mão |
| Pacote instalado por `%pip install --no-deps` de um wheel num Volume (ADR 0005; antes, `sys.path`) | todos os notebooks | ✅ wheel construído e importado fora do repositório (2026-10-13) | `%pip` no escopo do notebook é suportado no serverless (documentado); **não verificado** com wheel em Volume no Free Edition | célula `%pip` + `restartPython` |
| Saída HTTPS para BCB e IBGE | Bronze financeiro | ✅ (execução real de 2026-10-06, máquina local) | **não confirmado**; o Free Edition pode restringir a saída | `outbound_https`; fallback por replay |
| Tamanho dos dados | smoke | ⚠️ `test_smoke_rehearsal.py` nunca terminou ([EXECUTION_REPORT.md](EXECUTION_REPORT.md) §3); gates da Silver e R3 ensaiados sem Delta em 2026-10-12 ([smoke-test-report.md](smoke-test-report.md) §4) | cotas do Free Edition não medidas | tempos do runner |

## Incompatível ou fora do Free Edition (identificado, não usado)

| Recurso | Situação |
|---|---|
| Clusters clássicos, `spark.conf` de cluster, init scripts | Free Edition é só serverless; o código não depende deles |
| APIs de cache (`cache`, `persist`, `CACHE TABLE`) | proibidas por contrato (ADR 0001) |
| Databricks Secrets | não necessário: APIs públicas e dados sintéticos |
| Jobs/Asset Bundles | não usados nesta fase (sem pedido; execução por notebooks) |
| IoT (~32 milhões de leituras) | fora do smoke: volume e cotas não avaliados |
