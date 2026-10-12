# Troubleshooting — Databricks

Erros esperados, como reconhecê-los e o que fazer. Os erros do código trazem o procedimento na
própria mensagem; esta página os reúne. Nenhum passo de recuperação apaga dados sem autorização.

| Sintoma | Causa provável | O que fazer |
|---|---|---|
| `ModuleNotFoundError: retail_lakehouse` / `financial_lakehouse` / `lakehouse_common` | notebook fora de um Git folder, ou diretório de trabalho diferente do diretório do notebook | abrir o notebook pelo Git folder do repositório; conferir `Path.cwd()` na primeira célula |
| preflight `runtime` falha (`Python 3.x < required (3, 12)`) | versão de ambiente serverless antiga | escolher no notebook uma versão de ambiente com Python ≥ 3.12 |
| preflight `catalog` falha (`catalog 'workspace' not found`) | o workspace usa outro catálogo padrão | trocar `workspace` pelo catálogo listado em `[smoke]` dos 5 arquivos de configuração (o contrato de isolamento acompanha o prefixo) |
| preflight `create_schema` falha (permissão) | sem `CREATE SCHEMA` no catálogo | pedir a permissão ou usar um catálogo onde o usuário a tem |
| preflight `volume_files` falha | Volume ausente, ou E/S POSIX bloqueada | criar o Volume (SQL do guia); conferir o caminho `/Volumes/<catálogo>/<schema>/<volume>` |
| preflight `outbound_https` falha (`ConnectionError`, timeout) | sem saída para a internet | `source = replay` ([DEPLOYMENT_GUIDE §5](DEPLOYMENT_GUIDE.md)) |
| `ReplayMissError: request not recorded` | replay com outra configuração (outro `backfill_start`, outros indicadores) ou outro relógio | regravar com o mesmo `[smoke]`; o notebook já usa o instante de `_recorded_at.txt` |
| `ReplayMissError: no recording` | Volume `replay` vazio ou caminho errado | copiar a gravação (`databricks fs cp -r ...`) e conferir `replay_root` |
| `ChangeFeedDisabledError` | Bronze criado sem `delta.enableChangeDataFeed` (tabela antiga) | `ALTER TABLE ... SET TBLPROPERTIES (delta.enableChangeDataFeed = true)` com autorização; apagar a linha do checkpoint; reexecutar |
| `ChangesUnavailableError` | change data removido por retenção/`VACUUM`, ou CDF habilitado depois | apagar a linha do pipeline em `silver_checkpoints`; reexecutar (relê o Bronze inteiro; os `MERGE`s são idempotentes) |
| `CheckpointAheadError` | Bronze recriado ou restaurado | conferir a tabela; apagar a linha do checkpoint; reexecutar |
| `BronzeContractError` (`delete`/`update_*`) | alguém alterou o Bronze append-only | restaurar as linhas ou reconstruir a Silver |
| `CriticalQualityError` na Silver | schema do Bronze mudou, chave duplicada, reconciliação não fecha (`reconciliation` em `data_quality_results`) | ler `details` do check em `data_quality_results`; nada foi gravado |
| `CriticalQualityError` na Gold do Retail | grain duplicado, chave nula ou FK pendente (gate M2) | a Gold anterior continua; as candidatas ficam em `smoke_retail_gold_staging` para diagnóstico |
| `unknown_customer_systemic` (CRITICAL) na Silver do Retail | mais de 5% de pedidos órfãos: dados mestres não carregados | conferir a ingestão de `customers`; com a amostra de smoke isso não deve ocorrer (ensaio local) |
| `dbutils.notebook.run` indisponível | recurso não suportado no ambiente | rodar as etapas à mão, na ordem do guia |
| erro de cota ou limite de compute | limites do Free Edition | rodar uma etapa por vez; não aumentar a amostra |
| `pipeline_runs` com `FAILED` e `error_message` | qualquer falha de etapa | a mensagem já tem o tipo e o motivo (segredos mascarados) |

## Diagnóstico rápido (só leitura)

```sql
SELECT pipeline_name, `table`, status, error_message, started_at
FROM workspace.<schema>.pipeline_runs WHERE status = 'FAILED' ORDER BY started_at DESC;
```
