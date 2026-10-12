# Data Sources — Brazilian Financial Data Lakehouse

Somente APIs públicas **oficiais**, sem credenciais. Cada endpoint abaixo foi verificado na
documentação oficial e com uma chamada real em **2026-10-05** (data de acesso). Nenhum endpoint foi
inferido. Catálogo executável: `src/financial_lakehouse/ingestion/catalog.py` (sincronia verificada
por `tests/unit/test_fin_catalog.py`).

## 1. Fontes (APIs)

### 1.1 BCB — SGS (Sistema Gerenciador de Séries Temporais)

| Item | Valor |
|---|---|
| Instituição | Banco Central do Brasil |
| Documentação | https://dadosabertos.bcb.gov.br/dataset/11-taxa-de-juros---selic/resource/b73edc07-bbac-430c-a2cb-b1639e605fa8 (descrição do serviço BCData/SGS, igual em todas as séries) |
| Endpoint | `https://api.bcb.gov.br/dados/serie/bcdata.sgs.{codigo_serie}/dados?formato=json&dataInicial={dd/MM/aaaa}&dataFinal={dd/MM/aaaa}` |
| Parâmetros | `codigo_serie` (obrigatório), `dataInicial` / `dataFinal` (`dd/MM/aaaa`) |
| Schema da resposta | lista JSON de `{"data": "dd/MM/aaaa", "valor": "<decimal como texto>"}` |
| Limitações | intervalo `dataFinal − dataInicial` ≤ **10 anos** (erro se exceder); desde **26/03/2025** filtros de data são obrigatórios para séries diárias; `/dados/ultimos/{N}` limitado a 20 valores; sem paginação |
| Janela sem dados | **HTTP 404** com corpo `{"erro": {"statusCode": 404, "detail": "...SGSNegocioException: Value(s) not found"}}` (verificado em 2026-10-05): tratado como janela vazia **apenas** com essa mensagem |
| Datas codificadas | `dd%2FMM%2Faaaa` aceito (verificado) |
| Estratégia | janelas de no máximo 10 anos (paginação por data) |

### 1.2 BCB — PTAX (Olinda, OData)

| Item | Valor |
|---|---|
| Instituição | Banco Central do Brasil |
| Documentação | https://olinda.bcb.gov.br/olinda/servico/PTAX/versao/v1/documentacao · Swagger: https://olinda.bcb.gov.br/olinda/servico/PTAX/versao/v1/swagger-ui3 · Portal: https://dadosabertos.bcb.gov.br/dataset/taxas-de-cambio-todos-os-boletins-diarios |
| Endpoint | `https://olinda.bcb.gov.br/olinda/servico/PTAX/versao/v1/odata/CotacaoMoedaPeriodo(moeda=@moeda,dataInicial=@dataInicial,dataFinalCotacao=@dataFinalCotacao)?@moeda='EUR'&@dataInicial='MM-DD-AAAA'&@dataFinalCotacao='MM-DD-AAAA'&$format=json` |
| Parâmetros | `@moeda` (código ISO entre aspas simples), `@dataInicial` / `@dataFinalCotacao` (`'MM-DD-AAAA'`); opções OData `$format`, `$top`, `$skip`, `$orderby`, `$filter`, `$select` |
| Schema da resposta | `{"@odata.context": ..., "value": [{"paridadeCompra", "paridadeVenda", "cotacaoCompra", "cotacaoVenda": número, "dataHoraCotacao": "AAAA-MM-DD hh:mm:ss.fff", "tipoBoletim": "Abertura" \| "Intermediário" \| "Fechamento" ...}]}` |
| Limitações | vários boletins por dia (abertura, intermediários, fechamento): a escolha do boletim é regra da Silver; cotações a partir de 02/01/2002 para o euro |
| Estratégia | janelas anuais + paginação OData (`$top`/`$skip`, `$orderby=dataHoraCotacao`) |

### 1.3 IBGE — API de dados agregados (v3)

| Item | Valor |
|---|---|
| Instituição | Instituto Brasileiro de Geografia e Estatística |
| Documentação | https://servicodados.ibge.gov.br/api/docs/agregados?versao=3 |
| Endpoints | valores: `https://servicodados.ibge.gov.br/api/v3/agregados/{agregado}/periodos/{periodos}/variaveis/{variavel}?localidades=N1[all][&classificacao={id}[{categoria}]]` · períodos disponíveis: `.../agregados/{agregado}/periodos` · metadados: `.../agregados/{agregado}/metadados` |
| Parâmetros | `agregado`, `periodos` (ids separados por `\|`), `variavel`, `localidades` (obrigatório), `classificacao` (opcional) |
| Schema da resposta (valores) | `[{"id", "variavel", "unidade", "resultados": [{"classificacoes": [...], "series": [{"localidade": {"id", "nivel": {"id", "nome"}, "nome"}, "serie": {"<período>": "<valor como texto>"}}]}]}]` |
| Schema da resposta (períodos) | `[{"id": "AAAAMM", "literals": [...], "modificacao": "dd/MM/aaaa"}]` |
| Limitações | no máximo **100.000 valores** por requisição (categorias × períodos × localidades) |
| Período inexistente | HTTP 200 com `[]` (verificado em 2026-10-05) |
| Compressão | respostas com `Content-Encoding: gzip` **mesmo sem `Accept-Encoding`** (observado em 2026-10-06): o transporte descomprime |
| Estratégia | listar períodos disponíveis e pedir lotes de períodos explícitos |

### 1.4 IPEA (Ipeadata) — não utilizado

BCB e IBGE cobrem todos os indicadores selecionados com APIs oficiais documentadas; nenhuma série do
Ipeadata foi necessária. Reavaliar se surgir indicador disponível apenas lá.

## 2. Indicadores (datasets)

| Indicador (`indicator_id`) | Instituição / fonte | Série / tabela | Frequência | Unidade | Início | Verificação oficial |
|---|---|---|---|---|---|---|
| `selic_target` | BCB / SGS | 432 — Taxa de juros - Meta Selic definida pelo Copom | diária | % a.a. | — (não informado no portal) | https://dadosabertos.bcb.gov.br/dataset/432-taxa-de-juros---meta-selic-definida-pelo-copom |
| `selic_daily` | BCB / SGS | 11 — Taxa de juros - Selic | diária | % a.d. | 04/06/1986 | https://dadosabertos.bcb.gov.br/dataset/11-taxa-de-juros---selic |
| `ipca_monthly` | IBGE / agregados | 1737, variável 63 — IPCA - Variação mensal (N1) | mensal | % | 12/1979 | https://servicodados.ibge.gov.br/api/v3/agregados/1737/metadados |
| `igpm_monthly` | BCB / SGS (produzido pela FGV) | 189 — Índice geral de preços do mercado (IGP-M) | mensal | % variação mensal | 01/1989 | https://www3.bcb.gov.br/sgspub/consultarvalores/consultarValoresSeries.do?method=consultarGraficoPorId&hdOidSeriesSelecionadas=189 |
| `gdp_growth_yoy` | IBGE / agregados | 5932, variável 6561 — taxa trimestral (vs. mesmo período do ano anterior), classificação 11255 categoria 90707 (PIB a preços de mercado) | trimestral | % | 1996T1 | https://servicodados.ibge.gov.br/api/v3/agregados/5932/metadados |
| `usd_brl_sell` | BCB / SGS | 1 — Taxa de câmbio - Livre - Dólar americano (venda) - diário | diária | R$/US$ | 28/11/1984 | https://dadosabertos.bcb.gov.br/dataset/1-taxa-de-cambio---livre---dolar-americano-venda---diario |
| `eur_brl_ptax` | BCB / PTAX | CotacaoMoedaPeriodo, moeda `EUR` | diária (vários boletins) | R$/€ | 02/01/2002 | https://dadosabertos.bcb.gov.br/dataset/taxas-de-cambio-todos-os-boletins-diarios |
| `international_reserves` | BCB / SGS | 13982 — Reservas internacionais - Conceito liquidez - Total - Diária | diária | US$ milhões | 01/01/2008 | https://dadosabertos.bcb.gov.br/dataset/13982-reservas-internacionais---conceito-liquidez---total---diaria |
| `monetary_base` | BCB / SGS | 1788 — BM - Base monetária restrita (saldo em final de período) | mensal | milhares de unidades monetárias correntes | 1946 | https://dadosabertos.bcb.gov.br/dataset/1788-sgs |
| `credit_total` | BCB / SGS | 20539 — Saldo da carteira de crédito - Total | mensal | unidades monetárias correntes (conforme o portal) | 06/1988 | https://dadosabertos.bcb.gov.br/dataset/20539-saldo-da-carteira-de-credito---total |
| `unemployment_rate` | IBGE / agregados | 6381, variável 4099 — Taxa de desocupação, PNAD Contínua (N1) | trimestre móvel (mensal) | % | 2012-03 | https://servicodados.ibge.gov.br/api/v3/agregados/6381/metadados |

Data de acesso de todas as verificações: **2026-10-05**.

## 3. Limitações gerais e riscos

- **Revisões**: IBGE e BCB revisam valores (ex.: PIB, PNAD). A ingestão relê uma janela de
  *lookback* a partir do watermark; a Silver resolve versões por `MERGE`.
- **Databricks Free Edition**: acesso de saída à internet não confirmado (skill `databricks`). Se
  bloqueado, executar a ingestão fora do workspace e pousar os arquivos raw em um Volume.
- **Sem SLA**: APIs públicas podem responder 429/5xx; o cliente aplica retry com backoff exponencial.
- **Unidades monetárias correntes** (base monetária, crédito) não são deflacionadas nem ajustadas por
  mudanças de padrão monetário — tratamento da Silver/Gold.
