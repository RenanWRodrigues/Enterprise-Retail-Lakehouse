# Bronze — Industrial IoT Lakehouse

Entrada: a landing zone gravada pelas fontes (CMMS em CSV, historian em JSON Lines gzip),
[DATA_CONTRACTS.md](DATA_CONTRACTS.md). Saída: tabelas Delta append-only `bronze_<dataset>` com o
dado **como chegou** (todo texto) e os metadados de ingestão, mais o registro de decisões por
arquivo `bronze_files`.

Código:

- `src/iot_lakehouse/bronze/landing.py`: descoberta, identidade e checagens sem Spark;
- `bronze/readers.py`: leitura e checagens por arquivo com Spark;
- `bronze/ingest.py`: plano, append e decisões;
- `bronze/pipeline.py`: execução observada.

Notebook: `notebooks/bronze/01_ingest_bronze.py`. Configuração: `config/bronze.toml`
(`local`, `databricks`, `smoke`).

## Por que por arquivo, e não por lote como no Retail

O Retail recebe um arquivo por dataset e lote, e pula um lote já presente na tabela. O historian
grava vários arquivos `part-*` por lote e por dia (`batch_id=<b>/ingestion_date=<d>/`). Controle
por lote teria dois defeitos:

- **Arquivo novo em partição antiga:** um arquivo que chega depois, num lote já ingerido, seria
  ignorado.
- **Um arquivo quebrado:** ele faria perder o lote inteiro.

O Bronze do IoT mantém o padrão do Retail (append-only, metadados, `pipeline_runs`, criação atômica
com `delta.appendOnly`) e muda a unidade de progresso para o **arquivo**.

## Identidade

| Item | Definição |
|---|---|
| `file_id` | SHA-256 dos bytes do arquivo: os mesmos bytes dão sempre o mesmo id, e um arquivo reescrito ganha outro |
| nome estável | caminho relativo à landing (`historian/sensor_readings/batch_id=b1/ingestion_date=2025-01-03/part-0.json.gz`) |
| `batch_id` | lido do próprio layout: `<dataset>_<batch>.csv` ou o diretório `batch_id=<b>` |

Arquivos ocultos ou temporários (`_SUCCESS`, `.crc`, `_*`) e fora do layout não são fonte.

## Metadados de cada linha

`_ingestion_timestamp`, `_source_file` (caminho completo), `_batch_id`, `_ingestion_date`,
`_file_id`, `_source_system` (`cmms` ou `historian`). As colunas da fonte são texto exatamente como
entregues: `12.5` continua `"12.5"`, e `"x"` também entra. Valores ruins em registros bem formados
são assunto da Silver.

## Idempotência e retry

1. Um `file_id` já decidido em `bronze_files` (ingerido **ou** em quarentena) é pulado. Por isso
   um arquivo em quarentena não é reavaliado nem realertado.
2. Um `file_id` sem decisão, mas já presente na tabela de dados (`_file_id`), é o caso de uma
   execução que caiu depois do append e antes de registrar a decisão. A decisão é **reparada** com
   as linhas da tabela, e nada é gravado de novo.
3. Os arquivos legíveis de um dataset entram num **único commit Delta**. As decisões são gravadas
   depois (MERGE insert-only por `dataset, file_id`).

Reexecutar o mesmo lote não muda nada. Uma falha num dataset para a execução; a próxima retoma dos
arquivos ainda não decididos. Bytes idênticos com outro nome contam como reentrega e são pulados.

## Erros de arquivo × erros de registro

Um erro de arquivo põe o **arquivo inteiro** em quarentena (decisão `quarantined` com motivo e
detalhe em `bronze_files`). Nada dele é carregado, e os demais arquivos seguem.

| Motivo | Como é detectado |
|---|---|
| `empty_file` | 0 bytes. Um gzip válido sem registros, ou um CSV só com cabeçalho, é arquivo **válido** com 0 linhas |
| `corrupt_gzip` | descompressão completa em Python (gzip truncado ou inválido) |
| `not_utf8` | decodificação do CSV |
| `schema_drift` | CSV: cabeçalho ≠ contrato (falta, sobra ou ordem). JSON: chave fora do contrato, ou chave do contrato ausente de todos os registros do arquivo |
| `malformed_records` | CSV: linha com número de campos diferente do cabeçalho, ou aspas não fechadas (validação completa em Python). JSON: linha impossível de analisar (`_corrupt_record`) |
| `changed_after_ingestion` | um caminho já ingerido reaparece com outros bytes; a landing é imutável |

**Política de schema drift:** quarentena do arquivo e alerta ERROR (`file_problems`). Nada é
"consertado" nem descartado em silêncio. Uma mudança real de contrato exige atualizar
`source_contracts.py` e reentregar os arquivos.

**Achado:** o leitor CSV do Spark descarta campos extras de uma linha **sem erro**, mesmo em
modo PERMISSIVE. Por isso a estrutura do CSV é validada em Python antes da leitura (teste de
regressão `test_a_csv_line_with_extra_fields_is_malformed`).

## Observabilidade e reconciliação

Uma linha de `pipeline_runs` por dataset (`iot_bronze`):

| Campo | Significado |
|---|---|
| `input_rows` | linhas lidas dos arquivos novos |
| `output_rows` | linhas do commit Delta |
| `rejected_rows` | linhas dos arquivos em quarentena que puderam ser contadas; um gzip corrompido não tem linhas contáveis |

Checks em `data_quality_results`:

- `file_problems` (ERROR): arquivos em quarentena por motivo;
- `reconciliation` (CRITICAL): linhas dos arquivos anexados = linhas do commit.

Log estruturado: `iot_bronze dataset=... files=... skipped=... ingested=... quarantined=... rows=...`.

Só agregados por arquivo chegam ao driver (contagens, nomes de chaves): nenhum registro é
coletado. A descoberta lê os bytes de cada arquivo novo para o hash e, nos gzip, para a checagem de
integridade: é leitura sequencial, sem acumular em memória, proporcional ao volume novo.

## Limitações

- Duas execuções simultâneas do mesmo dataset não são suportadas (como no Retail).
- A checagem de reconciliação roda **depois** do commit (Delta não tem transação entre tabelas):
  uma divergência deixa a execução `FAILED`, e a próxima execução registra os arquivos como
  ingeridos a partir da tabela.
- Uma chave JSON nula em todos os registros de um arquivo é ausente para o escritor do Spark
  (`ignoreNullFields`) e aparece como `schema_drift`.
- O volume completo (~32 milhões de leituras) ainda não passou pelo Bronze.

## Testes

| Arquivo | Cobre |
|---|---|
| `tests/unit/test_iot_bronze_landing.py` | descoberta, lote pelo layout, identidade pelos bytes, arquivo vazio, gzip vazio válido, gzip truncado, drift de cabeçalho, linha com campo extra, aspas abertas |
| `tests/transformation/test_iot_bronze_readers.py` | texto preservado, caminho relativo igual ao da descoberta, linha JSON malformada contada no seu arquivo, drift de chaves JSON por arquivo |
| `tests/integration/test_iot_bronze_delta.py` | arquivos válidos e cada tipo de arquivo quebrado numa execução, metadados, reconciliação, contagens; reexecução sem efeito nem alerta; arquivo novo em partição antiga; arquivo reescrito; queda depois do append com reparo sem duplicar |
