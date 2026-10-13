# 0005 — Pacotes instaláveis e instalação nos notebooks

Status: aceito
Data: 2026-10-13
Substitui: a célula `sys.path[:0] = [...]` dos 13 notebooks (Fase 1 do ROADMAP: "Build backend" e
"Decidir como notebooks importam os pacotes no Databricks").

## Contexto

Os quatro pacotes de runtime (`lakehouse_common`, `retail_lakehouse`, `iot_lakehouse`,
`financial_lakehouse`) só usam imports absolutos e nunca foram instaláveis: os testes os achavam pelo
`pythonpath` do pytest, e os notebooks punham `projects/<p>/src` e `shared/src` no `sys.path` a partir
de `Path.cwd()`. Isso dependia do diretório de trabalho do notebook, e um notebook que esquecesse
`shared/src` quebrava só no Databricks (havia um teste de contrato para isso).

## Decisão

1. **Uma distribuição** na raiz, `databricks-lakehouse-portfolio`, com build backend `hatchling==1.32.4`
   (pinado). O wheel leva os quatro pacotes de runtime, e nada além deles: nem testes, nem
   `lakehouse_testing`. As dependências de runtime são vazias (o Databricks fornece PySpark e Delta).
2. **Local e CI:** `make install` instala o lock e depois `pip install --no-deps -e .` (editável).
   Os pacotes passam a ser importáveis de qualquer diretório. O `pythonpath` do pytest continua (os
   imports são os mesmos), e um teste que importasse do lugar errado continuaria pegando o código do
   repositório.
3. **Databricks (serverless):** cada notebook começa com
   `%pip install --quiet --no-deps /Volumes/workspace/libs/wheels/<wheel>` e
   `dbutils.library.restartPython()`, e só depois importa. Biblioteca no escopo do notebook por `%pip` é
   o mecanismo suportado no serverless. O wheel vem de um Volume, então não precisa de internet (que o
   Free Edition pode não ter) nem de build no workspace. O `--no-deps` garante que nada é instalado por
   cima do PySpark do runtime. O Git folder continua fornecendo os arquivos `config/*.toml`, lidos a
   partir de `Path.cwd()`.
4. **Artefato:** `make wheel` gera `dist/*.whl`, que é copiado para o Volume `workspace.libs.wheels`.
   O nome do wheel carrega a versão do `pyproject`, e `tests/contract/test_notebook_imports.py` exige
   que os notebooks instalem exatamente esse arquivo.

## Alternativas

| Alternativa | Por que não |
|---|---|
| Uma distribuição por projeto mais a compartilhada (quatro `pyproject`, workspace do uv) | separa os deploys, mas quadruplica configuração e lock num repositório que é um Git folder só; o código compartilhado continua não duplicado nas duas opções |
| `%pip install` do código-fonte do Git folder (`%pip install ../../..`) | faz o build no workspace: baixa o hatchling do PyPI (internet incerta no Free Edition) a cada execução |
| Ambiente serverless (painel *Environment* ou `environments` do job) com o wheel | também suportado e sem célula no notebook, mas é configuração fora do repositório, sem versão nem teste; fica como opção para jobs |
| Manter o `sys.path` | é o problema que motivou a mudança |

## Consequências

- Mudar a versão no `pyproject` exige reconstruir o wheel, copiá-lo para o Volume e atualizar os
  notebooks. O teste de contrato falha até que os notebooks estejam alinhados.
- O primeiro uso no workspace exige criar o Volume `workspace.libs.wheels` e enviar o wheel
  (`docs/databricks/DEPLOYMENT_GUIDE.md` §3). É uma ação remota e exige autorização.
- **Não verificado no Databricks** (nenhuma execução no workspace até agora): `%pip` de um Volume
  seguido de `restartPython` no serverless do Free Edition. Se falhar, a alternativa suportada é o
  ambiente serverless com o mesmo wheel.
