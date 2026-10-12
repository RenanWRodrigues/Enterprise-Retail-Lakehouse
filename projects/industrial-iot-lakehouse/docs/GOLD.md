# Gold — confiabilidade, manutenção e sensores (Industrial IoT)

Entrada: Silver de leituras ([SILVER.md](SILVER.md)) e Silver do CMMS (`silver/cmms.py`: tipada
pelo contrato, quarentena com motivos, última entrega por chave, reconstruída a cada execução
porque é dado mestre pequeno). A Gold **não lê o Bronze**.

Código:

- `src/iot_lakehouse/gold/intervals.py`: paradas;
- `gold/reliability.py`: indicadores;
- `gold/build.py`: tabelas;
- `gold/pipeline.py`: gate e publicação;
- `quality/suites.py::iot_gold_suite`: o gate.

Consultas: `sql/gold/`. Notebook: `notebooks/gold/01_build_gold.py`. Configuração:
`config/gold.toml`.

## Tabelas

| Tabela | Grain | Observação |
|---|---|---|
| `dim_equipment` | um equipamento | membro `UNKNOWN` para evento ou leitura de equipamento desconhecido |
| `dim_date` | um dia UTC do período | `date_key = -1` para evento fora do período, cuja data fica na coluna do fato |
| `fact_equipment_failures` | uma falha | reparo (primeira ordem corretiva), `repair_status` (`completed`, `in_progress`, `missing`), `repair_seconds`, linhagem `_file_id`/`_batch_id` |
| `fact_maintenance` | uma ordem | `is_planned` (preventiva), `response_seconds` (abertura → início), `duration_seconds` (início → fim; NULL se aberta), linhagem |
| `agg_equipment_hourly` | equipamento × hora UTC × tipo de sensor | leituras válidas: contagem, média, mínimo, máximo, desvio, atrasadas, fora de ordem, convertidas |
| `agg_equipment_daily` | equipamento × dia UTC | paradas e indicadores do dia + qualidade das leituras (válidas, quarentena, impossíveis, atrasadas, fora de ordem) |
| `agg_equipment_reliability` | equipamento × período | indicadores do período |

**Sem `fact_sensor_readings`:** uma linha por leitura é exatamente `silver_sensor_readings`
(tipada, deduplicada, rastreável). Uma cópia na Gold seria redundante, então análises por leitura
leem a Silver.

## Definições

| Termo | Definição |
|---|---|
| Período de observação | `[period_start, period_end)` em dias UTC inteiros (`config/gold.toml`). Paradas são cortadas nele |
| Tempo observado | 24 h por dia do período em que o equipamento existe (dia ≥ `installed_at`) |
| Parada planejada | `[started_at, completed_at)` de uma ordem **preventiva**. Nunca é falha |
| Parada não planejada | `[failure_timestamp, completed_at)` da ordem **corretiva** da falha (o CMMS registra só o instante da falha; quem fecha a parada é o reparo) |
| Tempo programado | observado − planejado |
| Tempo operacional | programado − não planejado |
| Falha elegível | falha com `failure_timestamp` no dia ou período, com ou sem reparo |
| Reparo elegível | ordem corretiva **concluída** de uma falha elegível; tempo de reparo = `completed_at − failure_timestamp` (tempo até restabelecer) |

## Fórmulas

```
availability = operating_seconds / scheduled_seconds        (NULL se scheduled = 0)
MTBF         = operating_seconds / failures                  (NULL se failures = 0)
MTTR         = repair_seconds / repairs                      (NULL se repairs = 0)
```

Os valores do período são **recalculados das somas** dos dias, nunca médias de razões diárias. As
consultas por tipo e por dia fazem o mesmo.

## Casos de borda

| Caso | Tratamento |
|---|---|
| Falhas sobrepostas | as paradas são unidas por equipamento antes de somar (*gaps and islands*): duas falhas de 06–09 h e 08–10 h param 4 h, não 5 h. Cada reparo conta no MTTR (3 h e 2 h → 2,5 h) |
| Falha durante parada planejada | o trecho dentro da parada planejada conta como planejado, não como não planejado (sem dupla contagem). A falha continua contada |
| Evento sem término (ordem aberta) | a parada vai até o fim do período (término real desconhecido); o reparo não entra no MTTR (`in_progress`) |
| Falha sem ordem de reparo | conta como falha (MTBF), sem parada conhecida e fora do MTTR (`missing`) |
| Falha atravessando meia-noite | cortada no dia UTC: 23:00 → 01:00 dá 1 h em cada dia |
| Equipamento sem falhas | disponibilidade a partir das paradas planejadas; MTBF e MTTR **NULL** (sem falha observada: nem zero, nem infinito) |
| Denominador zero | razão NULL, nunca erro nem infinito: programado 0 (planejado o período inteiro) → disponibilidade NULL |
| Equipamento instalado no meio do período | observado a partir do dia de instalação |
| Dia sem leituras | linha diária com contagens 0; a hora sem leituras não tem linha horária |
| Leitura de sensor desconhecido | equipamento `UNKNOWN` nos agregados horários e diários de qualidade |

## Dados tardios

Cada execução **reconstrói** a Gold a partir da Silver, de forma determinística sobre o período.
Uma leitura, falha ou ordem tardia que chega à Silver muda, na execução seguinte, as janelas
horárias, diárias e do período a que pertence. Nenhuma janela publicada fica com valor velho
(teste `test_a_late_failure_changes_the_aggregates_of_its_day`).

**Custo:** a reconstrução lê a Silver inteira do período. No volume completo (~32 milhões de
leituras, estimativa da configuração) o recálculo incremental por janela (`replaceWhere` dos dias
afetados) é a evolução natural; não foi feito porque ainda não há medição de volume.

## Gate e publicação

`iot_gold_suite`, CRITICAL, tudo avaliado antes de substituir qualquer tabela:

- grain único e chaves não nulas em todas as tabelas;
- integridade referencial de fatos e agregados com `dim_equipment` e `dim_date` (membros
  desconhecidos incluídos);
- invariantes das fórmulas: disponibilidade entre 0 e 1; segundos não negativos; operacional ≤
  programado ≤ observado; MTBF NULL exatamente quando não há falha; MTTR NULL exatamente quando
  não há reparo.

Uma falha CRITICAL mantém a Gold anterior. A execução fica `FAILED` em `pipeline_runs`, com os
resultados em `data_quality_results`. As tabelas são substituídas dimensões primeiro, com o grain
no comentário. Não há transação entre tabelas no Delta: uma falha de infraestrutura no meio da
substituição é corrigida pela próxima execução (reconstrução).

## Rastreabilidade

Fatos levam `_file_id` e `_batch_id` da linha de origem, que levam ao arquivo da landing
(`bronze_files`). Os agregados levam ao dia e ao equipamento, e daí às linhas da Silver e da
quarentena.

## Testes

| Arquivo | Cobre |
|---|---|
| `tests/transformation/test_iot_gold_reliability.py` | cálculo manual conhecido (MTBF 21 h, MTTR 2 h, disponibilidade 42/46), uma e várias falhas, falha atravessando meia-noite, sem falhas, sobrepostas, reparo aberto, falha dentro de parada planejada, falha sem ordem, instalação no meio do período, divisão por zero |
| `tests/transformation/test_iot_gold_build.py` | gate aprovando a Gold válida, membros desconhecidos (equipamento e data), dia sem leituras, agregado horário, evento tardio alterando o agregado, reconstrução idêntica, gate bloqueando grain duplicado, consultas recalculando razões das somas |
| `tests/integration/test_iot_gold_delta.py` | da landing até a Gold em Delta, com valores esperados; reexecução idêntica; falha no gate mantendo a Gold anterior (versões Delta iguais) |
