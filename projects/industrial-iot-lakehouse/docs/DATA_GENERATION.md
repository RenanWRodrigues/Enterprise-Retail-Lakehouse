# Data Generation — Industrial IoT Lakehouse

Gerador **determinístico** das fontes IoT ([DATA_CONTRACTS.md](DATA_CONTRACTS.md)) e do
**ground truth** que permite validar, mais tarde, se a detecção de anomalias e a previsão de
falhas acertaram. Código: `src/iot_lakehouse/synthetic/`.

## Arquitetura do gerador

| Parte | Volume | Motor | Por quê |
|---|---|---|---|
| Equipamentos, sensores, falhas, ordens de manutenção, episódios | milhares | Python (streams `Random(f"{seed}:{domínio}")`) | pequeno, regras de negócio legíveis |
| `sensor_readings` | ~32 milhões (padrão) | **Spark nativo** | nunca materializado no driver |

Leituras = `spark.range(passos) × sensores` (broadcast). Todo sorteio é
`xxhash64(seed, sal, sensor_id, passo)` → (0, 1): o resultado **não depende do particionamento**
nem da ordem de execução. Ruído gaussiano por Box-Muller com funções nativas; episódios
(degradação, drift, offline) entram por joins broadcast com intervalos. Sem UDF, sem `collect()`.

## Modelo do sinal

```
leitura = nominal × (1 + amplitude_diurna × sen(2π·(hora_local + fase)/24))   ← operação normal
        + σ_ruído × N(0,1)                                                    ← ruído
        + nominal × drift × progresso                                         ← drift de calibração
        + nominal × efeito_da_falha × progresso²                              ← degradação progressiva
        + spike (6–12 σ, só para cima)                                        ← transiente
(ou valor fora dos limites físicos, para leituras impossíveis)
```

Perfis (nominal, σ, amplitude) e limites físicos: [DATA_CONTRACTS.md](DATA_CONTRACTS.md).
Ciclo diário no horário das plantas (UTC−3).

## Comportamentos e anomalias

| Comportamento | Como é gerado | Configuração | Ground truth |
|---|---|---|---|
| Operação normal | nominal + ciclo diurno | perfis em `physics.py` | — |
| Ruído | gaussiano, σ por (equipamento, sensor) | perfis | `residual_sd` (interno) |
| Spikes | +6 a +12 σ | `spike_rate` | `is_spike` |
| Drift | rampa linear de ±5–15% do nominal em um sensor, até a próxima manutenção preventiva (recalibração) | `drift_sensor_share` | `drift_episode_id`, `drift_episodes.csv` |
| Degradação progressiva | rampa quadrática nos sensores afetados pelo modo de falha, 2–7 dias antes da falha | `failure_rate_per_year` | `failure_id`, `failure_degradation.csv` |
| Períodos offline | equipamento parado (falha → fim do reparo; preventiva) e perda de comunicação de um sensor (10 min–6 h) | `comm_outages_per_sensor_month`, `preventive_interval_days` | `offline_periods.csv` |
| Duplicatas | reenvio do gateway: mesmo `reading_id` e valor, chegada 5–120 s depois | `duplicate_rate` | `is_duplicate` (cópia) |
| Fora de ordem | chega depois de leituras posteriores do mesmo sensor (atraso > intervalo, ≤ limiar) | `out_of_order_rate` | `is_out_of_order` |
| Atrasados (late) | atraso > `lateness_threshold_minutes` (até +47 h): caem em partições de ingestão posteriores | `late_rate` | `is_late` |
| Impossíveis | `2 × máximo + 1` ou `mínimo − amplitude − 1` | `impossible_rate` | `is_impossible` |

Garantias verificadas por teste:

- Operação normal + drift + ruído (±6σ) + spikes (+12σ) **nunca** saem dos limites físicos;
  só `is_impossible` sai, e todo `is_impossible` sai.
- Todo efeito de falha supera 6σ do ruído (detectável) e continua fisicamente possível.
- Falhas de um equipamento não se sobrepõem (degradação + parada); preventivas não colidem com falhas.

## Relação anomalias ↔ falhas (validação posterior)

```
failure_events (CMMS: só o que a manutenção sabe)
   └── _ground_truth/failure_degradation.csv  (início da degradação, sensores, efeito)
          └── sensor_reading_labels.failure_id  (cada leitura sob degradação)
maintenance_orders.related_failure_id  →  failure_events.failure_id
_ground_truth/offline_periods.csv.reference_id  →  failure_id | maintenance_order_id
```

Drift (`drift_episode_id`) é falha **do sensor**, não do equipamento: um bom detector deve
distingui-lo de degradação real.

## Saída

```
<root>/cmms/<dataset>/<dataset>_<batch>.csv
<root>/historian/sensor_readings/batch_id=<batch>/ingestion_date=<yyyy-MM-dd>/part-*.json.gz
<root>/_ground_truth/{failure_degradation,drift_episodes,offline_periods}.csv
<root>/_ground_truth/sensor_reading_labels/   (Parquet)
```

O ground truth fica **fora** das fontes: a Bronze ingere apenas `cmms/` e `historian/`.

## Como executar

Spark grava arquivos e, no Windows, isso exige `winutils`: use o container ou o Databricks.

```bash
# container (Linux)
docker run --rm -v "${PWD}:/workspace" lakehouse-tests \
  sh -c "PYTHONPATH=projects/industrial-iot-lakehouse/src:shared/src python -m iot_lakehouse.synthetic --output data/landing/iot"
```

Databricks: `notebooks/setup/00_generate_synthetic_sources.py` (widget `output` = Volume).
Volume padrão (`config/data_generation.toml`): 120 equipamentos, 750 sensores, 30 dias a 1 min
≈ 32,4 milhões de leituras (antes de lacunas offline e duplicatas).

## Casos de teste

| Caso | Teste | Categoria |
|---|---|---|
| contratos completos, FKs, documentação sincronizada | `unit/test_iot_contracts.py` | unit |
| limites físicos, perfis, efeitos de falha detectáveis e possíveis, doc = código | `unit/test_iot_physics.py` | unit |
| configuração: volume grande, validações | `unit/test_iot_config.py` | unit |
| dados mestres determinísticos; falhas sem sobreposição; ordens corretivas ligadas; preventivas sem colisão; episódios coerentes | `unit/test_iot_master_data.py` | unit |
| determinismo das leituras (checksum) e sensibilidade à semente | `transformation/test_iot_readings_behavior.py` | transformation |
| operação normal = ruído N(0,1) em torno do baseline (média, σ, máximo) | idem | transformation |
| limites físicos ⟺ `is_impossible` | idem | transformation |
| spikes ≥ 6σ | idem | transformation |
| degradação cresce até a falha, para toda falha observável; `failure_id` ligado ao CMMS | idem | transformation |
| drift cresce ao longo do episódio | idem | transformation |
| nenhuma leitura em período offline | idem | transformation |
| duplicatas = reenvios exatos | idem | transformation |
| atrasos coerentes com `is_late` / `is_out_of_order` | idem | transformation |
| landing + ground truth gravados; late events em partições posteriores | `integration/test_iot_landing_write.py` | integration (container) |

Todos os testes de leituras usam uma configuração mínima (4 equipamentos, 3 dias, 5 min, ~21 mil
leituras) e **uma agregação por teste** — nenhuma coleta de leituras brutas.

## Limitações conhecidas

- Sem correlação física entre sensores fora das falhas (ex.: corrente × potência variam independentes).
- Um único lote histórico; streaming contínuo fica para a ingestão incremental.
- Limiar de "late" é configuração do gerador; o watermark real será decidido na Bronze/Silver.
- Volume padrão não foi executado localmente (Windows não grava com Spark): estimativa calculada,
  não medida.
