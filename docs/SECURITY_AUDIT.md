# Auditoria de segurança — 2026-10-07

Escopo: código-fonte, notebooks, documentação, testes, configuração, CI e arquivos que entrariam
no primeiro commit. Política de referência: [SECURITY.md](SECURITY.md) e skill `security`.

**Resultado: nenhum segredo real encontrado.** Foram corrigidas três fragilidades de prevenção
(`.gitignore`, máscara de mensagens de erro persistidas e cobertura do scanner da CI). Nada foi
commitado nem enviado.

## 1. Estado do repositório

| Item | Situação |
|---|---|
| Arquivos rastreados pelo Git | **0** (nenhum commit ainda) |
| Arquivos staged | 0 |
| Arquivos que entrariam no primeiro commit (não ignorados) | 264 |
| `.env` / `.env.*` presentes | nenhum |
| `.env.example` | não existe e não é necessário: o código não lê segredos do ambiente (§4) |
| Arquivos com nome sensível (`*.pem`, `*.key`, `.databrickscfg`, `credentials*`, `id_rsa*`, `.netrc`) | nenhum |

Como nada está rastreado, "arquivos do Git" foi interpretado como **tudo o que um `git add` sem
filtro levaria**: a lista de `git ls-files --others --exclude-standard`.

## 2. Varredura de segredos e dados sensíveis

Uma varredura ampla foi feita nos 264 arquivos candidatos. Ela registra só arquivo, linha e tipo,
nunca o valor. Depois, o scanner da CI (que também percorre `data/` e outros diretórios ignorados
presentes localmente) foi rodado com os padrões novos.

| Categoria | Padrões | Ocorrências |
|---|---|---:|
| Token Databricks | `dapi` + 32 hex; variante solta | 0 |
| Credenciais AWS | `AKIA`/`ASIA` + 16; atribuição de `aws_secret_access_key` | 0 |
| Outros tokens | GitHub (`ghp_`…), Slack (`xox?-`), Google (`AIza`), JWT (`eyJ….….…`) | 0 |
| Chaves privadas | `-----BEGIN … PRIVATE KEY-----` | 0 |
| Cabeçalho de autorização | `Authorization: Bearer …` | 0 |
| Connection strings com senha | `esquema://usuário:senha@`, `jdbc:…password=` | 0 |
| Atribuições de credencial | `password`/`secret`/`token`/`api_key`/`client_secret` = literal | 0 |
| `.env` com valor | `DATABRICKS_TOKEN=`, `*_PASSWORD=` | 0 |
| URL de workspace Databricks | `*.cloud.databricks.com`, `*.azuredatabricks.net` | 0 |
| Caminhos locais | `C:\Users\<usuário>`, `/home/<usuário>`, nome do usuário | 0 |
| E-mails | qualquer endereço | 4, todos em testes, domínios `example.com`/`example.net` (RFC 2606) |
| CPF / telefone | formatos brasileiros | 0 |

Caminhos locais absolutos existem só em arquivos **ignorados** (`.coverage*`, `data/`) e no
diretório temporário do agente, fora do repositório.

## 3. Dados pessoais (PII)

| Camada | Situação |
|---|---|
| Gerador sintético (Retail) | nomes fictícios; e-mails só em domínios reservados (`example.com/.org/.net`); **nenhum CPF, CNPJ ou telefone é gerado** (`synthetic/reference_data.py`) |
| Landing / Bronze | guardam o dado como entregue (nome, e-mail, nascimento), sintético e tratado como real; o diretório `data/` é ignorado |
| Silver | só `birth_date` permanece entre os campos PII; nome e e-mail não entram |
| Gold | `birth_date` vira `age_band`; nenhuma coluna identifica a pessoa |
| Financial / IoT | dados públicos do BCB/IBGE e telemetria sintética: sem PII de pessoa física |
| Notebooks | `display()` mostra apenas relatórios de execução e contagens |

## 4. Credenciais no código e na configuração

- Nenhum projeto usa credencial: as APIs (BCB SGS, PTAX, IBGE) são públicas, e Retail e IoT usam
  dados sintéticos.
- Variáveis de ambiente lidas: apenas `HADOOP_HOME` e `PYSPARK_PYTHON`, na infraestrutura de
  testes (`lakehouse_testing`). Nenhum `os.environ`, `getenv` ou `dbutils.secrets` em código de
  produção.
- Configurações versionadas (`config/*.toml`) contêm só caminhos, nomes de tabelas e parâmetros.
- CI (`.github/workflows/ci.yml`, `.github/actions/setup`): permissão `contents: read`, nenhum
  `secrets.*` (verificado por `tests/contract/test_ci_workflow.py`).
- O agente está impedido de ler `.env`, `.env.*`, `.databrickscfg` e `secrets/`
  (`.claude/settings.json`).

## 5. Logs e mensagens persistidas

| Saída | O que contém | Avaliação |
|---|---|---|
| `HttpClient` (Financial) | `GET <url> -> status (bytes, tentativa)` e avisos de retry | URLs de APIs públicas, sem autenticação, sem cabeçalhos. **Risco futuro:** uma API com token em query string apareceria no log; a regra (skill `security`) é passar credenciais por cabeçalho e nunca pela URL |
| `logger.exception` na ingestão | traceback de falhas por indicador | mensagens de HTTP e schema, sem credenciais hoje |
| `pipeline_runs.error_message` | erro do passo, persistido em tabela consultável | **corrigido** (abaixo) |
| Manifest / landing raw | URLs, SHA-256 e respostas das APIs públicas | sem dados sensíveis; ignorado pelo Git |

### Fragilidade corrigida: máscara de `error_message`

`safe_error_message` (em `lakehouse_common.observability`) prometia mascarar valores que parecem
segredos antes de gravá-los em `pipeline_runs`, mas a expressão só reconhecia o nome exato
precedido de fronteira de palavra. Testado com valores claramente fictícios, **5 de 8 formas
comuns passavam sem máscara**:

| Forma | Antes | Depois |
|---|---|---|
| `token=…`, `password: …` | mascarado | mascarado |
| `access_token=…`, `DATABRICKS_TOKEN=…`, `client_secret: …` | **exposto** | mascarado |
| `Authorization: Bearer …` | **exposto** | mascarado |
| `jdbc:postgresql://usuário:senha@host` | **exposto** | senha mascarada |
| `…?access_token=…&page=2` | mascarado, mas engolia `&page=2` | só o valor é mascarado |

Hoje nenhum pipeline manipula credenciais, então nada vazou. A correção fecha a lacuna antes que
uma fonte autenticada exista. Teste de regressão:
`shared/tests/unit/test_observability_runs.py::test_error_messages_mask_secret_assignments`
(6 casos novos) e `test_masking_keeps_the_useful_part_of_the_message`.

## 6. `.gitignore`

Verificado com `git check-ignore` em caminhos hipotéticos (sem criar arquivos):

| Caminho | Antes | Depois |
|---|---|---|
| `.env`, `.env.local`, `projects/x/.env`, `.databrickscfg`, `*.pem`, `*.key`, `secrets/`, `data/`, `.coverage*`, `.claude/settings.local.json` | ignorado | ignorado |
| `.env.example` | versionável (permitido) | versionável (permitido) |
| `*.p12`, `*.pfx` (certificados com chave) | **versionável** | ignorado |
| `id_rsa*`, `id_ed25519*` (chaves SSH) | **versionável** | ignorado |
| `.netrc`, `.aws/` | **versionável** | ignorado |
| `credentials*.json` | **versionável** | ignorado |
| `*.log` (podem conter URLs, erros e amostras de dados) | **versionável** (só `derby.log`) | ignorado |
| `.ipynb_checkpoints/` (cópias de notebooks com saídas) | **versionável** | ignorado |

A lista de arquivos candidatos ao primeiro commit continuou com 264 arquivos: nenhum arquivo
legítimo passou a ser ignorado.

## 7. Scanner da CI (`tests/contract/test_no_secrets.py`)

| Antes | Depois |
|---|---|
| 5 padrões: token Databricks, AWS access key, GitHub, chave privada, atribuição de `password`/`secret`/`api_key`/`access_token` | + `ASIA`, qualquer nome terminado em `token`/`secret`/`password` (`DATABRICKS_TOKEN`, `client_secret`), connection string com senha (placeholders como `password`, `***`, `<...>`, `{...}` aceitos), `Bearer`, JWT, Slack, Google |
| Extensões de texto comuns | + `.sh`, `.ps1`, `Makefile`, `Dockerfile`, `.gitignore`, `.dockerignore` |
| Autoteste com 3 amostras | 7 amostras positivas + 6 placeholders que **não** podem disparar (evita falso positivo em docs e testes) |

As amostras de teste são montadas por concatenação, para que o próprio arquivo de teste nunca
contenha a forma que o scanner procura.

## 8. Riscos remanescentes e recomendações

| Risco | Recomendação |
|---|---|
| Bronze do Retail guarda nome e e-mail (sintéticos) | no Unity Catalog, restringir `retail.bronze` a quem opera a ingestão; a Silver/Gold já minimizam PII |
| `logger.exception` registra o traceback completo (sem máscara) | enquanto não houver fonte autenticada, aceito; quando houver, passar credenciais só por cabeçalho e mascarar a URL no `HttpClient` |
| Scanner por expressões regulares (rede de segurança, não substituto) | antes do primeiro push, rodar um scanner dedicado no histórico (por exemplo, `gitleaks detect`); a adoção exige justificar a dependência (CLAUDE.md) |
| Primeiro commit ainda não feito | adicionar arquivos por nome ou revisar `git status` antes de `git add`; rodar `pytest -m contract` (inclui o scanner) |

## Arquivos alterados nesta auditoria

- `.gitignore`: padrões de credenciais, logs e checkpoints de notebook.
- `shared/src/lakehouse_common/observability/runs.py`: máscara de `error_message`.
- `shared/tests/unit/test_observability_runs.py`: testes de regressão da máscara.
- `tests/contract/test_no_secrets.py`: padrões e arquivos novos, autoteste de placeholders.
- `docs/OBSERVABILITY.md`: descrição atualizada da máscara.
