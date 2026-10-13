# Network preflight — APIs do Financial no Databricks Free Edition

Objetivo: saber, **no workspace**, se a ingestão Bronze do Financial alcança as APIs oficiais. Um
teste local prova só a rede local e não classifica nenhuma API como acessível pelo Databricks.

Estado em 2026-10-12: **nenhum endpoint testado**. A ferramenta está pronta; a execução aguarda
autorização (requisições externas) e um workspace.

## 1. Endpoints

Hosts derivados das URLs base dos clientes (`clients/bcb_sgs.py`, `clients/bcb_ptax.py`,
`clients/ibge.py`); o teste `test_endpoints_are_the_hosts_the_clients_use` garante que a lista
acompanha os clientes. Uma requisição por host: GET público, só leitura, sem credenciais, a menor
resposta disponível. O corpo é lido até 64 KiB e descartado.

| Endpoint | Host | Requisição de diagnóstico | Resposta esperada |
|---|---|---|---|
| `bcb_sgs` | `api.bcb.gov.br` | `/dados/serie/bcdata.sgs.432/dados/ultimos/1?formato=json` | último valor da meta Selic (1 registro) |
| `bcb_ptax` | `olinda.bcb.gov.br` | `/olinda/servico/PTAX/versao/v1/odata/Moedas?$top=1&$format=json` | 1 moeda do cadastro PTAX |
| `ibge_agregados` | `servicodados.ibge.gov.br` | `/api/v3/agregados/1737/periodos` | lista de períodos do IPCA (alguns KB) |

Nenhum endpoint privado, nenhuma tentativa de contornar bloqueios, nenhuma credencial.

## 2. Ferramenta

- Lógica: `projects/brazilian-financial-lakehouse/src/financial_lakehouse/clients/network_probe.py`
  (`probe_all`). Para cada host: resolução DNS (`getaddrinfo`, porta 443), depois um GET HTTPS
  com timeout de 10 s e sem retry. Mede o tempo da requisição HTTPS. Nunca lança exceção por falha
  de rede: a falha é o resultado.
- Notebook: `projects/brazilian-financial-lakehouse/notebooks/setup/00_network_preflight.py`.
  Mostra a tabela e a evidência do ambiente (data UTC, runtime do Databricks, Python, Spark) em
  JSON para colar aqui. Não grava nada.
- Testes sem rede: `tests/unit/test_fin_network_probe.py` (18 testes, DNS e HTTPS injetados).

### Como ler o resultado

| `outcome` | Significa | Indica |
|---|---|---|
| `reachable` | 2xx | API acessível deste ambiente |
| `dns_failure` | o nome não resolve | sem DNS externo, ou DNS filtrado |
| `network_blocked` | conexão recusada, resetada ou rede inalcançável | **saída bloqueada** pela plataforma |
| `timeout` | sem resposta em 10 s | ambíguo: firewall descartando pacotes **ou** API lenta; repetir mais tarde distingue |
| `tls_failure` | handshake/certificado falhou | proxy interceptando TLS ou cadeia de certificados quebrada |
| `auth_required` | 401, 403 ou 407 | as APIs são públicas: quem respondeu foi um proxy ou uma página de bloqueio |
| `endpoint_unavailable` | 429 ou 5xx | a rede funciona; a API está fora do ar ou limitando |
| `http_error` | outro status (ex.: 404) | a rede funciona; o endpoint mudou |

`detail` traz o texto do erro com valores que parecem segredo mascarados (`mask_secrets`). Não
imprime cabeçalhos, cookies nem o corpo.

## 3. Resultados

| Endpoint | Status real | Data do teste | Ambiente | Evidência |
|---|---|---|---|---|
| `bcb_sgs` | **PENDENTE** | — | — | — |
| `bcb_ptax` | **PENDENTE** | — | — | — |
| `ibge_agregados` | **PENDENTE** | — | — | — |

Para preencher: rodar o notebook no workspace e colar aqui a saída JSON (`tested_at_utc`,
`databricks_runtime`, `python`, `spark` e uma linha por endpoint).

Referência (não é evidência do Databricks): `projects/brazilian-financial-lakehouse/docs/data_sources.md`
registra uma chamada real a cada endpoint feita fora do Databricks em 2026-10-05.

## 4. Se a saída estiver bloqueada

Resultado `network_blocked`, `dns_failure`, `tls_failure` ou `auth_required` nos três hosts:
usar o **replay pelo Volume**, já implementado e documentado em
[DEPLOYMENT_GUIDE.md §5](DEPLOYMENT_GUIDE.md):

1. numa máquina com internet, gravar as respostas reais com o mesmo código
   (`python -m financial_lakehouse.ingestion ... --record-responses data/replay/financial`);
2. copiar para o Volume `/Volumes/workspace/smoke_financial/replay` (passo remoto, só com
   autorização);
3. rodar `notebooks/bronze/01_ingest_bronze.py` com `source = replay`: a execução usa o instante
   da gravação e falha com `ReplayMissError` se faltar alguma resposta, sem ir à rede.

Um bloqueio parcial (um host acessível, outro não) também cai no replay. A Bronze não mistura
fontes ao vivo e gravadas numa mesma execução.
