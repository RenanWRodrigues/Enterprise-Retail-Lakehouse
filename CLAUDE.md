# CLAUDE.md — Constituição Operacional do Projeto

Este arquivo governa todo trabalho feito neste repositório, por humanos ou agentes.
Em caso de conflito, a ordem de precedência é: instrução explícita do usuário →
este arquivo → documentos em `docs/` → convenções do código existente.

Documentos de apoio (detalham, nunca contradizem este arquivo):

- `docs/ARCHITECTURE.md` — arquitetura, layout do repositório, convenções de nomes
- `docs/ENGINEERING_STANDARDS.md` — padrões de Python, PySpark, SQL e notebooks
- `docs/TESTING_STRATEGY.md` — categorias de teste, fixtures, comandos
- `docs/DATA_QUALITY.md` — dimensões e regras de qualidade, quarentena
- `docs/SECURITY.md` — secrets, credenciais, dados sensíveis
- `docs/ROADMAP.md` — fases e escopo de cada uma
- `docs/ADR/` — decisões arquiteturais completas (as pequenas ficam em `ARCHITECTURE.md` §7)
- `docs/architecture/ARCHITECTURE_OVERVIEW.md` — mapa dos módulos, tabelas Delta e execução
- `docs/implementation/` — plano de melhorias, matriz de testes e registro de riscos
- `docs/audits/` — baseline e auditorias datadas (estado verificado, não intenção)
- `.claude/skills/` — guias operacionais por tema (ver seção *Skills*)

---

# Project Mission

Construir três Lakehouses profissionais usando **Databricks Free Edition**,
**PySpark**, **Spark SQL** e **Delta Lake**, demonstrando engenharia de dados
de nível de produção: arquitetura clara, código testável, qualidade de dados
mensurável, observabilidade e reprodutibilidade.

# Projects

| Projeto | Diretório planejado | Foco técnico |
|---|---|---|
| Enterprise Retail Lakehouse | `projects/enterprise-retail-lakehouse/` (`retail_lakehouse`) | modelagem dimensional, SCD Type 2, métricas de vendas |
| Industrial IoT Lakehouse | `projects/industrial-iot-lakehouse/` (`iot_lakehouse`) | ingestão incremental, séries temporais, eventos tardios |
| Brazilian Financial Data Lakehouse | `projects/brazilian-financial-lakehouse/` (`financial_lakehouse`) | dados públicos (BCB/CVM), calendário útil, indicadores |

Código comprovadamente reutilizado por 2+ projetos vive em `shared/src/lakehouse_common/`
(regra de admissão em `shared/README.md`). Layout completo em `docs/ARCHITECTURE.md` §3.

# Architecture Principles

- **Medallion Architecture**: Bronze → Silver → Gold, com fronteiras explícitas.
- **Bronze = raw + metadata**: dado como chegou (append-only), acrescido de colunas
  técnicas (`_ingestion_timestamp`, `_source_file`, `_batch_id`, `_ingestion_date`).
  Sem regras de negócio.
- **Silver = validated + standardized**: tipos corretos, nomes padronizados,
  deduplicação, regras de qualidade aplicadas, registros inválidos em quarentena.
- **Gold = business-ready**: modelos para consumo (fatos, dimensões, agregados)
  com **grain documentado** e colunas explícitas.
- **Idempotency**: reexecutar um pipeline com a mesma entrada produz o mesmo estado
  (MERGE por chave natural, `replaceWhere` por partição; nunca append cego em Silver/Gold).
- **Incremental processing**: processar apenas o que é novo/alterado
  (watermarks, Change Data Feed, checkpoints); full refresh é exceção documentada.
- **Observability**: logging estruturado, contagem de linhas por etapa,
  métricas de qualidade persistidas, duração por etapa.
- **Data quality**: regras declarativas e testadas; falhas críticas interrompem o pipeline.
- **Testability**: transformações são funções puras `DataFrame -> DataFrame`.
- **Reproducibility**: dependências fixadas, configuração versionada, dados de
  exemplo determinísticos, ambiente local reproduzível.

# Coding Rules

Python:

- Python 3.12 quando compatível (verificar o runtime do Databricks serverless);
- `snake_case` para funções, variáveis e módulos; `PascalCase` para classes;
- type hints em todas as funções públicas (mypy strict);
- funções pequenas, com uma responsabilidade;
- docstrings úteis (o *porquê* e o contrato, não a repetição do nome);
- `pathlib` em vez de `os.path`;
- `logging` em vez de `print`;
- configuração separada do código (arquivos em `config/` do projeto, dataclasses tipadas);
- imports absolutos a partir da raiz do pacote (`from retail_lakehouse.silver... import ...`);
- evitar estado global mutável;
- evitar números mágicos (constantes nomeadas);
- evitar lógica de negócio em notebooks.

PySpark:

- preferir funções Spark nativas (`pyspark.sql.functions`);
- evitar Python UDF (se inevitável, justificar e preferir Pandas UDF);
- evitar `collect()`;
- evitar `toPandas()` em datasets grandes;
- evitar loops sobre DataFrames (linha a linha ou coluna a coluna com `withColumn` em loop);
- schemas explícitos (`StructType`) na leitura de fontes;
- evitar inferência desnecessária (`inferSchema` proibido em pipelines);
- transformations devem ser testáveis: recebem e retornam `DataFrame`, sem I/O.

SQL:

- keywords consistentes (MAIÚSCULAS);
- CTEs com nomes descritivos (`orders_deduplicated`, não `t1`);
- evitar `SELECT *` em Gold;
- documentar o grain de toda tabela Gold (comentário no topo e em `docs/`);
- impedir double counting (agregar antes de juntar fatos de grains diferentes);
- joins precisam possuir justificativa de cardinalidade (1:1, N:1, 1:N) em comentário.

# Notebook Rules

Notebooks devem ser **thin orchestration layers**.

Eles podem:

- carregar configuração;
- chamar módulos;
- executar pipeline;
- exibir resultados.

Eles NÃO devem conter grandes blocos de lógica de negócio.

A lógica deverá ficar em `projects/<p>/src/<pacote>/` (ou em `shared/`, se comprovadamente
reutilizável). Notebooks ficam em `projects/<p>/notebooks/{bronze,silver,gold}/`.
Regra prática: se uma célula tem mais de ~15 linhas de transformação, ela pertence a um módulo.
Notebooks são versionados como arquivos-fonte `.py` (formato Databricks), não `.ipynb`.

# Testing Rules

Para qualquer mudança:

RED → GREEN → REFACTOR quando apropriado.

Toda correção de bug deve receber regression test quando aplicável (o teste reproduz o
bug e falha antes da correção; "não aplicável" é justificado no relato).

Antes de considerar uma tarefa concluída:

1. escrever/atualizar testes;
2. executar testes relacionados;
3. executar lint (`ruff check .`, `ruff format --check .`, `mypy`);
4. verificar regressões (suíte completa);
5. reportar resultados (números reais, nunca estimados).

Testes divididos em (diretórios `projects/<p>/tests/<categoria>/` e `shared/tests/<categoria>/`;
`tests/contract/` da raiz para governança do repositório; marcadores pytest automáticos):

- `unit` — lógica Python pura, sem Spark;
- `integration` — pipeline ponta a ponta entre camadas;
- `data_quality` — regras e expectativas de qualidade;
- `contract` — schemas, contratos de dados e governança do repositório;
- `transformation` — transformações PySpark/SQL sobre DataFrames pequenos.

Detalhes em `docs/TESTING_STRATEGY.md`.

# Security Rules

Nunca:

- expor tokens;
- commitar `.env`;
- inserir senhas;
- inserir API keys;
- registrar secrets em logs;
- criar credenciais aparentemente reais (nem em exemplos ou testes).

Credenciais vêm de Databricks Secrets ou variáveis de ambiente locais não versionadas.
Detalhes em `docs/SECURITY.md`.

# Git Safety

PROIBIDO executar sem autorização explícita:

```
git reset --hard
git clean -fd
git checkout -- .
git restore .
git push --force
rm -rf
```

(Esses comandos também estão bloqueados em `.claude/settings.json`.)

Não apagar arquivos silenciosamente.

Não sobrescrever trabalho do usuário.

Antes de alterações grandes:

```
git status
```

Depois:

```
git diff --stat
```

Commits e pushes apenas quando solicitados.

# Scope Discipline

Não implementar funcionalidades fora da fase atual (ver `docs/ROADMAP.md`).

Não fazer refatorações não relacionadas.

Não instalar dependências sem justificar (o quê, por quê, alternativa considerada).

Não alterar arquitetura silenciosamente — mudanças arquiteturais são registradas
em `docs/ARCHITECTURE.md` (seção de decisões).

# Evolution Rules

Regras para evoluir o código existente de forma incremental e segura. Complementam as seções
acima; o plano corrente está em `docs/implementation/IMPROVEMENT_PLAN.md`.

- **Preservar a arquitetura.** Evoluir dentro das fronteiras existentes (`src/<pacote>/<camada>`,
  `shared/` pela regra de admissão, notebooks finos). Reescrever um módulo exige motivo
  registrado (linha em `docs/ARCHITECTURE.md` §7 ou ADR em `docs/ADR/`); preferir mudanças
  pequenas, reversíveis e testadas.
- **Funções pequenas e com uma responsabilidade** (*Coding Rules*). Ao tocar uma função acima
  de ~50 linhas, extrair etapas nomeadas no mesmo passo, sem mudar comportamento.
- **Interfaces tipadas e documentadas.** Toda função pública tem type hints (mypy strict) e
  docstring com o contrato: entrada, saída, efeitos (I/O, tabelas gravadas) e erros levantados.
- **Toda alteração funcional vem com teste automatizado** (*Testing Rules*): RED antes da
  correção, regressão para bug corrigido, suíte da categoria afetada e lint antes de concluir.
- **Idempotência e determinismo.** Reexecutar com a mesma entrada produz o mesmo estado (MERGE
  por chave, lote já processado é pulado). Relógio, semente e caminhos são injetados; nada de
  `now()` ou aleatoriedade sem semente em transformações e testes.
- **Contratos de dados e qualidade.** Schemas explícitos (`StructType`) na leitura; contrato de
  saída por camada (`schemas.py`); mudança de contrato é CRITICAL até ser atualizada. Regras de
  qualidade são declaradas uma vez (`src/<pacote>/quality/`) e reaproveitadas; inválido vai para
  quarentena com motivo, nunca é corrigido em silêncio.
- **Separação Bronze/Silver/Gold** (skill `medallion`): Bronze guarda o dado como chegou, Silver
  valida e padroniza, Gold modela para consumo com grain documentado. Nenhuma camada lê uma
  camada posterior; a Gold não lê o Bronze.
- **Credenciais** (*Security Rules*): nunca em código, configuração, teste, log ou documentação.
- **Nada destrutivo sem autorização explícita**, também fora do Git:
  - não executar `DROP`, `DELETE`, `TRUNCATE`, `VACUUM`, `RESTORE`, `OPTIMIZE ... ZORDER` nem
    sobrescrever tabelas, Volumes ou o diretório `data/`; as gravações previstas pelos pipelines
    (MERGE, append, a reconstrução documentada da Gold) são a exceção;
  - não fazer deploy nem executar em workspace remoto;
  - não executar cargas de volume completo (milhões de linhas) sem pedido explícito: validar com
    os dados pequenos dos testes.
- **Compatibilidade local e Databricks.** O mesmo código roda no Spark local (testes) e no
  serverless do Free Edition: sem APIs de cache, `checkpoint` ou dependência de `spark.conf`
  (contrato `test_serverless_compatibility.py`); timestamps em UTC; leitura de texto com schema
  (`test_spark_reads.py`). Testes que exigem Delta ou Linux pulam no Windows com motivo explícito
  e rodam na CI.
- **Decisões arquiteturais documentadas.** Decisão pequena: linha em `docs/ARCHITECTURE.md` §7.
  Decisão com alternativas reais ou custo alto de reverter: ADR em `docs/ADR/NNNN-titulo.md`
  (skill `architecture`). Relatar resultados só com números de execuções reais, com a data.

# Definition of Done

Uma tarefa só está concluída quando:

- implementação finalizada;
- testes relevantes passando;
- lint passando;
- documentação atualizada;
- nenhuma credencial exposta;
- mudanças resumidas;
- limitações conhecidas documentadas.

# Completion Report

Toda fase deve terminar informando:

```
FILES CREATED
FILES MODIFIED
TESTS
VALIDATION
DECISIONS
KNOWN LIMITATIONS
NEXT STEP
```

# Skills

Guias curtos em `.claude/skills/<nome>/SKILL.md`. Consulte a skill do tema **antes** de
trabalhar nele. Skills detalham este arquivo; em conflito, este arquivo prevalece.

| Skill | Usar quando |
|---|---|
| `architecture` | decidir onde o código mora, fronteiras, configuração, ADR |
| `databricks` | Delta, Spark SQL, Unity Catalog, limites do Free Edition/serverless |
| `pyspark` | escrever/revisar transformações, joins, nulls, schemas, performance |
| `medallion` | decidir o que pode/não pode em Bronze, Silver, Gold |
| `data-quality` | regras de qualidade, status PASS/WARNING/ERROR/CRITICAL, quarentena |
| `testing` | escrever testes, AAA, DataFrames determinísticos, antes de concluir tarefa |
| `dimensional-modeling` | fatos, dimensões, grain, SCD, surrogate keys, double counting |
| `api-ingestion` | ingestão HTTP: timeout, retry, paginação, watermark, idempotência |
| `documentation` | README (WHAT/WHY/HOW/ARCHITECTURE/DATA/TESTS/RESULTS), docs, Completion Report |
| `security` | secrets, `.env`, tokens, logs, PII, operações Git |

Nova skill exige: entrada nesta tabela + `tests/contract/test_skills.py`.

# Commands

Com GNU make (os mesmos alvos que a CI chama; `make help` lista):

```bash
make install            # .venv + dependências de desenvolvimento, do lock (requirements/dev.lock.txt)
make lock               # após mudar o pyproject: atualiza uv.lock e o lock exportado
make lock-check         # pyproject, uv.lock e lock exportado concordam (roda na CI)
make lint               # ruff check, ruff format --check, mypy strict
make test               # todos os testes, exceto integration (o que a cadeia da CI roda)
make test-unit          # Python puro, sem Spark, sem rede
make test-transformation  # transformações PySpark (Java 17/21)
make test-quality       # data quality + contract (Spark)
make test-integration   # Spark + Delta locais de ponta a ponta (Linux/Docker; pulados no Windows)
make coverage           # unit + transformation + quality com cobertura, fail_under = 85
```

Sem make (Windows sem make instalado), os comandos equivalentes:

```bash
python -m venv .venv
.venv/Scripts/python -m pip install --require-hashes -r requirements/dev.lock.txt   # Windows (Linux/macOS: .venv/bin/python)
.venv/Scripts/python -m pip install --no-deps -e .                   # os pacotes do projeto (editável)

.venv/Scripts/python -m pytest                      # suíte completa
.venv/Scripts/python -m pytest -m "not integration" # sem os testes lentos
.venv/Scripts/python -m pytest -m contract          # uma categoria
.venv/Scripts/python -m pytest --cov                # com cobertura (fail_under = 85)
.venv/Scripts/python -m pytest projects/enterprise-retail-lakehouse   # um projeto
.venv/Scripts/python -m ruff check .
.venv/Scripts/python -m ruff format --check .
.venv/Scripts/python -m mypy
.venv/Scripts/python -m pre_commit install --install-hooks   # uma vez: hooks de commit/push
.venv/Scripts/python -m pre_commit run --all-files           # hooks de commit (higiene, segredos, ruff, mypy)
```

CI: `.github/workflows/ci.yml`, cadeia lint → unit → transformation → data quality → coverage,
mais integration em job separado; cada etapa é um alvo do `Makefile`; sem segredos. Detalhes em
`docs/TESTING_STRATEGY.md` §12.
