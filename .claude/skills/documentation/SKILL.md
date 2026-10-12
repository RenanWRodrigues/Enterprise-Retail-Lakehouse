---
name: documentation
description: Use ao escrever ou atualizar README (raiz ou de projeto), docs/, docstrings, comentários de grain ou o Completion Report — define a estrutura WHAT/WHY/HOW/ARCHITECTURE/DATA/TESTS/RESULTS e quando cada documento deve ser atualizado.
---

# Documentation

## README de projeto (`projects/<p>/README.md`)

**Antes da implementação** (estado atual), o README usa o template inicial:
`Problem · Architecture · Data · Medallion Layers · Testing · Status · Roadmap`
(seções exigidas por `tests/contract/test_monorepo_structure.py`). Tudo é marcado como
*planejado*; nada de números ou resultados.

**Quando o projeto for implementado**, o README evolui para responder, nesta ordem
(Problem → WHAT/WHY; Medallion Layers → ARCHITECTURE/DATA; Status → RESULTS):

| Seção | Pergunta | Conteúdo mínimo |
|---|---|---|
| WHAT | O que é? | 2–3 frases: domínio, problema, saída principal |
| WHY | Por que existe? | perguntas de negócio respondidas; competências demonstradas |
| HOW | Como executar? | pré-requisitos, comandos locais, como rodar no Databricks |
| ARCHITECTURE | Como funciona? | diagrama Bronze → Silver → Gold, tabelas por camada, decisões (link p/ ADR) |
| DATA | Que dados? | fontes (link, licença, sintético ou público), volume, grain de cada tabela Gold |
| TESTS | Como sei que funciona? | categorias, comando, regras de qualidade principais |
| RESULTS | O que foi entregue? | números **reais** de uma execução: linhas por camada, % quarentena, status DQ, exemplos de consultas/insights |

- RESULTS nunca é inventado nem estimado. Sem execução real → escrever "pendente".
- README da raiz é o índice do portfólio; ganha a estrutura completa na Fase 5 (`docs/ROADMAP.md`).

## O que atualizar quando…

| Mudança | Documento |
|---|---|
| Nova tabela Gold | README (DATA: grain) + `COMMENT` da tabela |
| Nova decisão arquitetural | `docs/ARCHITECTURE.md` §7 ou ADR (skill `architecture`) |
| Nova regra de qualidade relevante | README (TESTS) |
| Novo comando/dependência | README (HOW) + `CLAUDE.md` § Commands se for comando geral |
| Nova fonte de dados | README (DATA) com licença/origem |
| Mudança de escopo de fase | `docs/ROADMAP.md` |
| Nova skill | `CLAUDE.md` § Skills + `tests/contract/test_skills.py` |

Documentação desatualizada é bug: atualizar no mesmo trabalho que muda o comportamento.

## Docstrings e comentários

- Docstring diz contrato e porquê: o que entra, o que sai, invariantes, por que existe.
  Não repete o nome da função.
- Funções Gold/SQL: comentário de grain e de cardinalidade de joins (obrigatório).
- Comentário explica decisões não óbvias; código óbvio não é comentado.
- Identificadores e docstrings de código em inglês; documentos em português (ADR 005).

## Estilo

- Curto, verificável, sem marketing ("robusto", "escalável" sem evidência).
- Tabelas para comparações, blocos de código para comandos reais e testados.
- Links relativos entre documentos; nada de caminhos absolutos da máquina local.
- Exemplos com credenciais usam marcadores óbvios (`<DATABRICKS_TOKEN>`), nunca valores plausíveis.

## Completion Report

Toda fase termina com as seções definidas no `CLAUDE.md`
(FILES CREATED … NEXT STEP). Em TESTS e VALIDATION, apenas resultados de comandos
efetivamente executados; em KNOWN LIMITATIONS, o que não foi verificado.
