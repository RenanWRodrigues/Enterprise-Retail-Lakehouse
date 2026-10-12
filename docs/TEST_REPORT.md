# Relatório da campanha de testes — 2026-10-07

Objetivo: **encontrar bugs**, sem implementar funcionalidade nova. Foram executadas as
categorias unit, transformation, data_quality e contract, e integration onde o ambiente permitiu.
Para cada falha: causa raiz, reprodução, teste de regressão quando aplicável, correção, teste
específico e suíte relacionada.

## 1. Ambientes

| Ambiente | O que roda | Limitação |
|---|---|---|
| Windows 11 (`.venv` local) | unit, contract, data_quality, transformation | sem Hadoop: Delta e escrita de arquivos Spark pulados; datas 9999-12-31 não convertem (testes Gold do Retail pulados); pouca memória livre |
| Linux (WSL Ubuntu, cópia limpa do repositório, Python 3.12 + OpenJDK 21 isolados com micromamba, alvos do `Makefile`) | todas as categorias, inclusive integration com Delta | é o ambiente de referência, equivalente ao runner da CI |

A CI do GitHub ainda não rodou: o repositório não tem commits. Por isso o Linux local foi a
primeira execução de vários testes que o Windows pula, e foi lá que os bugs de teste das seções
3.4 a 3.6 apareceram.

## 2. Resultado final

| Execução (Linux, alvos do `Makefile`) | Escopo | Resultado |
|---|---|---|
| **Final**, com todas as correções (2026-10-07) | lint (ruff, format, mypy strict) | ok |
| | unit | **465 passed** |
| | transformation | **134 passed** |
| | data quality + contract | interrompida aos 57% pelo Claude Code por falta de memória na máquina; **0 falhas** até ali |
| | integration, cobertura | não executadas (mesmo motivo) |
| **Última completa** (2026-10-07, antes das 3 correções HIGH da auditoria de conformidade) | data quality + contract | **508 passed** |
| | integration (Spark + Delta) | 47 passed, 1 failed (expectativa de alerta ajustada depois, ver [TEST_REPORT.md §3.2](TEST_REPORT.md)) |
| | cobertura combinada (unit + transformation + quality + integration) | **96%** (piso: 85%) |

Testes coletados: **1.162** (unit 465, transformation 134, data_quality 121, contract 394,
integration 48). Depois das correções, os testes afetados foram rodados no Windows: os 4 testes
de leitura JSON do Retail e a suíte de contrato da raiz (§6 de
[CLAUDE_COMPLIANCE_AUDIT.md](CLAUDE_COMPLIANCE_AUDIT.md)). A revalidação completa no Linux está
pendente; para fazê-la, rode `make test-quality`, `make test-integration` e `make coverage`.

| Achado | Tipo | Estado |
|---|---|---|
| Backfill no meio do mês (3.1) | bug de código | corrigido + regressão |
| Corrida do watermark da Silver (3.2) | bug de código | corrigido + regressão; efeito colateral aberto (M6) |
| Revisão A → B → A (3.3) | bug de código | corrigido + regressão |
| `greatest` com uma coluna (3.4) | bug de teste | corrigido; confirmado no Linux (data quality 508 passed) |
| Membro desconhecido como FK órfã (3.5) | bug de teste | corrigido; confirmado no Linux |
| Join ambíguo no teste IoT (3.6) | bug de teste | corrigido; revalidação no Linux pendente (execução interrompida) |
| Falhas de shuffle no Windows (3.7) | ambiente | não reproduzidas; sem correção |

## 3. Falhas encontradas e tratamento

### 3.1 Backfill no meio do mês tornava o primeiro mês da Gold "completo" · bug de código

- **Causa raiz:** a Gold financeira marca como parcial só o mês em andamento e não distingue dia
  faltante de feriado (não há calendário de dias úteis). Com `backfill_start` em 2025-01-15, o
  histórico diário começa no meio de janeiro:
  - a Selic do mês é capitalizada sobre metade dos dias úteis;
  - o dólar médio cobre meio mês;
  - toda janela de 12 meses que inclui janeiro herda o erro.
- **Reprodução:** `IngestionSettings(backfill_start=date(2025, 1, 15))` e
  `--backfill-start 2024-01-15` eram aceitos.
- **Correção na origem:** `backfill_start` precisa ser o dia 1 de um mês. No CLI, isso vira erro
  de uso (código 2).
- **Regressão:** `test_backfill_must_start_on_the_first_day_of_a_month`,
  `test_mid_month_backfill_is_a_usage_error`.

### 3.2 Corrida do watermark da Silver · bug de código

- **Causa raiz:** todas as linhas de uma execução do Bronze recebem o mesmo `ingestion_timestamp`
  (o início da execução), e a Silver lia `ingestion_timestamp > watermark`. Se a Silver rodasse no
  meio de uma execução do Bronze, os indicadores gravados depois tinham o mesmo timestamp do
  watermark e **nunca** eram processados.
- **Reprodução:** `test_rows_of_the_watermark_run_are_read_again` falhou antes da correção.
- **Correção:** `>=`. A execução do watermark é relida; o MERGE ignora versões que já tem.
- **Efeito colateral aceito:** a reexecução reavalia linhas inválidas já em quarentena e alerta de
  novo. A quarentena não duplica, porque é insert-only. Registrado como M6 em
  [CLAUDE_COMPLIANCE_AUDIT.md](CLAUDE_COMPLIANCE_AUDIT.md). O teste de integração Delta foi
  atualizado com esse comportamento explícito.

### 3.3 Revisão A → B → A perdida no Bronze financeiro · bug de código (fase do Bronze)

A volta ao valor original tinha a mesma chave de conteúdo da primeira versão e era descartada; a
Silver mostrava B. Corrigido com a regra de versão por entrega. Regressão em
`test_fin_bronze_versions.py`, `test_fin_bronze_incremental_scenarios.py` e
`test_fin_bronze_to_silver_revisions.py`.

### 3.4 Grain de uma coluna quebrava o teste de contrato da Gold do Retail · bug de teste

- **Causa raiz:** o auxiliar usava `F.greatest(*colunas)`, que exige 2 ou mais colunas, e falhava
  com `WRONG_NUM_COLUMNS` em todo grain de uma coluna (8 testes).
- **Por que não apareceu antes:** o teste é pulado no Windows (datas 9999) e a CI nunca rodou.
- **Correção:** `reduce(operator.or_, ...)`. O código de produção estava correto.

### 3.5 Membro desconhecido da `dim_product` contado como FK órfã · bug de teste

O teste "fontes limpas nunca caem no membro desconhecido" verificava todas as linhas de
`dim_product`, inclusive o próprio membro -1, que aponta para o fornecedor -1 por desenho. O
código segue o modelo documentado; o teste agora exclui o membro desconhecido da dimensão.

### 3.6 Referência ambígua no teste de integração da landing IoT · bug de teste

O join entre as leituras gravadas e os rótulos trazia `event_timestamp` e `sensor_id` dos dois
lados (`AMBIGUOUS_REFERENCE`). Do lado dos rótulos, agora só entram `reading_id` e `is_late`.

### 3.7 6 falhas de transformação no Windows · ambiente, não reproduzidas

- **Sintoma:** `FileNotFoundError` em arquivos de shuffle do Spark (`%TEMP%\blockmgr-...`) no meio
  de uma execução longa de toda a categoria.
- **Investigação:** o mesmo arquivo isolado passou 10/10, e no Linux a categoria inteira passou.
  A causa é a remoção de arquivos temporários durante a sessão, não o código.
- Nenhuma correção; registrado como instabilidade de ambiente.

## 4. Investigado e descartado

| Suspeita | Verificação | Conclusão |
|---|---|---|
| Fan-out nos joins de intervalo do IoT (dois episódios sobrepostos duplicariam leituras) | gerador de falhas e drift | falhas nunca se sobrepõem; no máximo um drift por sensor |
| Fuso horário no epoch do IoT (`datetime.timestamp()` em datetime ingênuo) | `IotGenerationConfig` | `start` precisa ter timezone (validado) |
| SCD2 do Retail com dois `effective_from` iguais (surrogate key duplicada) | suíte de qualidade | `(customer_id, effective_from)` único é gate da Silver |
| Pedido anterior à primeira versão do cliente | gerador | primeira versão = `registered_at`; não há pedido antes do cadastro |
| Schema Bronze × leitura da Silver divergente (`reference_date` NOT NULL × nullable) | `SchemaMatches` | intencional e testado: a leitura nunca é mais estrita que a tabela |
| Double counting nas consultas do Retail | 7 consultas SQL | joins N:1 comentados; `COUNT(DISTINCT order_id)` no ticket médio |
| Datas dependentes do fuso da sessão | `lakehouse_testing.spark` e chaves de data | sessão em UTC; data de negócio do Retail por aritmética de epoch |
| Janelas IoT (degradação e parada) | gerador | degradação termina na falha; parada vai da falha ao fim do reparo (intervalos semiabertos) |
| Watermark do Bronze financeiro | `_date_range`, `_periods`, checkpoint | só avança depois do MERGE e nunca recua; falha não move o checkpoint |
| Reamostragem financeira | comparação com séries oficiais | idêntica em 24/24 meses e 252/252 dias ([GOLD.md §7](../projects/brazilian-financial-lakehouse/docs/GOLD.md)) |
