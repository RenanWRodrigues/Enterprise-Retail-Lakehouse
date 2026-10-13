# Compatibilidade: PySpark local × Databricks

Estado em 2026-10-12. **A versão real do runtime Databricks não é conhecida**: o smoke não foi
executado ([smoke-test-report.md](smoke-test-report.md)). Por isso o pin **não muda** (regra deste
documento, §5). Os recursos usados pelo código e o que cada um exige da plataforma estão em
[COMPATIBILITY_MATRIX.md](COMPATIBILITY_MATRIX.md); aqui ficam as versões e a estratégia de
dependências.

## 1. Três versões diferentes

| Versão | O que é | Local / CI | Databricks serverless |
|---|---|---|---|
| **Spark** (motor) | o Spark que executa o plano | o que vem embutido no pacote `pyspark`, em JVM local (modo clássico, py4j) | o Spark do **servidor**: o serverless roda Spark Connect, e `spark.version` mostra a versão do servidor |
| **PySpark** (biblioteca Python) | a API que o código importa | `pyspark==4.0.4`, a mesma versão do motor | o **cliente** Spark Connect que o runtime instala (`pyspark.__version__`); pode diferir do servidor |
| **Runtime Databricks** | o pacote da plataforma: Python, cliente e bibliotecas | não existe | `DATABRICKS_RUNTIME_VERSION`, que identifica a versão de ambiente serverless |

O preflight (`check_runtime` em `lakehouse_common/preflight.py`) passa a registrar as três, além
do Python: `Python 3.12; Spark <servidor> (server); PySpark <cliente> (client); runtime: <versão>`.
O primeiro preflight no workspace fornece a evidência que hoje falta.

## 2. Versões medidas

| Item | Local (Windows, `.venv`) | Instalação limpa (Windows, venv novo) | CI (`ubuntu-latest`) | Databricks |
|---|---|---|---|---|
| Python | 3.12.7 | 3.12.7 | 3.12 (`setup-python`) | **PENDENTE** |
| PySpark | 4.0.4 | 4.0.4 | 4.0.4 (mesmo grupo `dev`) | **PENDENTE** (cliente) |
| Spark | 4.0.4 (embutido) | 4.0.4 | 4.0.4 | **PENDENTE** (servidor) |
| Delta | `delta-spark` 4.0.1 (JARs via Ivy) | 4.0.1 | 4.0.1 | Delta do runtime (versão própria da Databricks) |
| py4j | 0.10.9.9 | 0.10.9.9 | idem | não usado (Spark Connect) |
| Java | Temurin 21.0.11 | (mesmo JDK) | Temurin 21 (`setup-java`) | não se aplica ao código |
| Evidência | `python -c` e `java -version`, 2026-10-12 | §4 | run `38253484478`: unit, transformation e data quality verdes com esses pins | — |

## 3. Incompatibilidades possíveis

| Área | Risco | Situação |
|---|---|---|
| API: Spark Connect | APIs da JVM (`sparkContext`, RDD, `_jvm`), cache, `checkpoint`, `spark.conf` | proibidas por contrato (`test_serverless_compatibility.py`). Os testes locais rodam no modo clássico, então um uso exclusivo do Connect não seria pego localmente; o smoke é a prova |
| API: versão do cliente | o cliente do runtime ser **mais antigo** que 4.0: `Column.try_cast` (Silver do Retail) só existe a partir do 4.0; `try_to_timestamp` e `F.replace` existem desde o 3.5 | a confirmar pelo preflight (versão do cliente) |
| Schemas e tipos | `DECIMAL(12,2)`, `TIMESTAMP` e conversões por fuso | o código não depende do fuso da sessão (`business_date` por aritmética de epoch; testes com `session.timeZone = UTC`) |
| SQL | ANSI mode (padrão no Spark 4.0; erros em cast inválido) | o código usa `try_cast`/`try_to_timestamp`; mesmo padrão nos dois lados se o servidor for ≥ 4.0 |
| Delta | o Delta do runtime não é o `delta-spark` OSS | `MERGE`, CDF (`table_changes`), `VERSION AS OF`, `createOrReplace` são documentados nos dois; a confirmar no preflight (`delta_change_feed`) e no smoke |
| Java | Spark 4.0 local exige Java 17 ou 21 | local e CI em 21; não se aplica ao Databricks |
| Python | código exige ≥ 3.12 (`requires-python`) | o preflight falha (`runtime`) se o ambiente serverless tiver Python menor |

## 4. Evidências locais desta revisão

| Escopo | Resultado |
|---|---|
| Instalação limpa: `python -m venv` + `pip install --upgrade "pip>=25.1"` + `pip install --group dev` (os mesmos passos do `make install`) | exit 0 em 51 s; `pip check`: sem conflitos; pins diretos iguais aos do `pyproject.toml` |
| Comparação com o `.venv` existente | **6 dependências transitivas diferentes**: `ast_serialize` 0.12.1→0.13.0, `filelock` 4.0.12→4.1.1, `iniconfig` 2.3.0→2.3.1, `platformdirs` 4.12.3→4.13.0, `python-discovery` 1.6.1→1.6.2, `virtualenv` 21.14.5→21.14.7. Nenhuma do Spark. Confirma o risco R12 (sem lock) |
| Venv limpo: `-m unit` / ruff / mypy | 591 passed / verde / verde |
| Venv limpo: Spark (`shared/tests/transformation`, `test_quality_checks.py`, `test_inventory_r3.py`) | 41 passed (transformation e data quality, Spark 4.0.4 local, Java 21) |
| Smoke no runtime real | **não executado** |

## 5. Estratégia de dependências

| Ambiente | Como o PySpark chega | Regra |
|---|---|---|
| **Local** | grupo `dev` do `pyproject.toml` (`make install`): `pyspark==4.0.4`, `delta-spark==4.0.1`, mais Java 17/21 | pins exatos (`test_pinned_dependencies.py`); Delta só nos testes que pedem `delta_spark` |
| **CI** | o mesmo `make install` (`.github/actions/setup`) com Java 21 | mesmos pins; integração Delta em job separado |
| **Databricks** | **do runtime**. `dependencies = []` no `pyproject.toml`; o `%pip` dos notebooks instala só o wheel do projeto, com `--no-deps` (ADR 0005) | nunca instalar `pyspark` nem `delta-spark` no workspace: um PySpark instalado por cima do cliente do runtime pode conflitar com ele e com a sessão Spark Connect que o runtime cria. O código chega pelo Git folder (`sys.path`) |

Os grupos já estão separados: `[project].dependencies` é o que o runtime precisaria instalar
(hoje nada), e `[dependency-groups].dev` é local e CI. Se um dia o código de produção precisar de
uma biblioteca, ela entra em `dependencies`, **sem** `pyspark`, e o notebook instala só ela.

### Quando trocar o pin

Só com evidência e com a suíte rodada. Nesta ordem:

1. o preflight no workspace registra o Spark do servidor e o cliente PySpark;
2. se a versão menor (`4.0`) for diferente, trocar `pyspark` (e o `delta-spark` correspondente) para
   a mesma versão menor do **servidor**, numa mudança só;
3. rodar `make lint test test-integration` (Linux) e registrar os números;
4. atualizar este documento e a decisão em `docs/ARCHITECTURE.md` §7.

Uma diferença só de patch (`4.0.x`) não exige troca: fica registrada aqui.

### Lacunas conhecidas

- **Sem lock das transitivas (M5):** já há divergência (§4). Proposta separada: lock com `uv` ou
  `pip-tools`, com a ferramenta justificada (CLAUDE.md, *Scope Discipline*).
- **Testes locais sem Spark Connect:** uma opção é rodar parte da suíte contra
  `SparkSession.builder.remote("local")`. Isso exige o extra `pyspark[connect]` (gRPC, pandas,
  pyarrow), dependência nova a justificar. Não foi adotado.
