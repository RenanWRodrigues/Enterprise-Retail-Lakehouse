---
name: databricks
description: Use ao escrever código que roda no Databricks (notebooks, Delta Lake, Spark SQL, Unity Catalog, Volumes, jobs) ou ao avaliar se um recurso existe no Databricks Free Edition / compute serverless.
---

# Databricks

## Regra zero: não inventar

O Free Edition roda **somente em compute serverless**, e serverless tem restrições
que o Spark local não tem. Antes de usar um recurso fora da lista "seguro", confirme
na documentação oficial da Databricks ou no workspace e registre o resultado em
`docs/ROADMAP.md` (Questões em aberto). Se não puder confirmar, **diga que não sabe**.

## Ambiente gratuito — o que assumir

| Seguro assumir | Verificar antes de usar |
|---|---|
| Compute serverless (notebooks, jobs, SQL warehouse) | versão exata de Spark/Python/Delta do runtime |
| Unity Catalog (`catalog.schema.table`) | quantos catalogs/schemas podemos criar |
| Delta Lake como formato padrão de tabela | acesso de saída à internet (APIs externas podem ser bloqueadas) |
| Volumes UC para arquivos (`/Volumes/<cat>/<schema>/<vol>/`) | Databricks Secrets (scopes) disponíveis para o usuário |
| Notebooks Python e SQL | Lakeflow Declarative Pipelines (DLT), Auto Loader, Jobs com múltiplas tasks |
| Cotas de uso limitadas (compute pode ser suspenso ao exceder) | limites de concorrência e duração de jobs |

Não usar (não suportado em serverless ou fora do Free Edition): clusters customizados,
init scripts, GPUs, Scala/R, APIs de RDD/`sparkContext`, DBFS root/mounts como
armazenamento de dados. A documentação de serverless restringe APIs de cache
(`df.cache()`/`persist()`) e a maioria dos `spark.conf.set` — não depender deles
(confirmar o comportamento exato no workspace na Fase 1).

Consequências de projeto: datasets pequenos/médios e determinísticos; sem dependência
de tuning de cluster; ingestão de APIs só depois de confirmar acesso de saída
(alternativa: pousar arquivos em Volume).

## Delta Lake

- Toda tabela é Delta gerenciada pelo Unity Catalog; nome completo sempre (`retail.silver.orders`).
- Escrita idempotente: `MERGE INTO` por chave natural; `replaceWhere` para sobrescrever
  partição/intervalo; `append` apenas em Bronze, com `_batch_id` para evitar reingestão.
- Schema enforcement é o padrão. Schema evolution só via mudança de contrato versionada
  (nunca `mergeSchema` implícito).
- Constraints como contrato: `NOT NULL` e `CHECK` em Silver/Gold quando a regra é invariável.
- `COMMENT` em tabelas Gold com o grain; `COMMENT` em colunas de negócio.
- Change Data Feed (`delta.enableChangeDataFeed = true`) quando a camada seguinte precisa
  ler só mudanças.
- Time travel (`VERSION AS OF`) para auditoria e depuração, não como lógica de pipeline.
- `OPTIMIZE`/`VACUUM`/clustering: só após confirmar o que o workspace já gerencia
  automaticamente; não é requisito com os volumes deste portfólio.

## Spark SQL

- DDL versionado em `projects/<p>/sql/`, idempotente (`CREATE ... IF NOT EXISTS`,
  `CREATE OR REPLACE VIEW`).
- Padrões de escrita SQL (CTEs, grain, cardinalidade): skill `dimensional-modeling` e
  `docs/ENGINEERING_STANDARDS.md` §3.
- Parâmetros via widgets/named parameters (`:param`), nunca concatenação de strings.

## Notebooks como orquestração

- Arquivo-fonte `.py` com cabeçalho `# Databricks notebook source` e células separadas
  por `# COMMAND ----------`.
- Ordem: widgets/config → imports de `src` → chamadas do pipeline → `display` de amostra/métricas.
- `spark`, `dbutils` e `display` existem só no Databricks: o código em `src/` recebe
  `spark` por parâmetro e nunca usa `dbutils` (exceto o módulo fino de secrets).
- Dependências do projeto instaladas como pacote (`%pip install` do wheel/diretório);
  nada de copiar código entre notebooks.

## Paridade local × Databricks

Testes rodam em Spark local com versão alinhada ao runtime serverless (decisão da Fase 1).
Código que só funciona no Databricks fica isolado em módulos finos e é coberto pela
categoria `integration` executada no workspace.
