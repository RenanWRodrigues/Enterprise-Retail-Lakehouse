---
name: security
description: Use ao lidar com secrets, tokens, credenciais de API, arquivos .env, logging, dados pessoais (PII) ou operações Git (staging, commit, push) — e sempre antes de commitar.
---

# Security

Política completa e resposta a incidentes: `docs/SECURITY.md`.

## Secrets e API credentials

- Fonte única no Databricks: Databricks Secrets (`dbutils.secrets.get(scope, key)`),
  lido em um módulo fino, no ponto de uso. Disponibilidade no Free Edition: verificar (Fase 1).
- Local: variável de ambiente ou `~/.databrickscfg` — fora do repositório.
- Código recebe a credencial por parâmetro; nunca em constante, configuração versionada,
  default de função, widget, nome de arquivo ou URL commitada.
- Credencial ausente → erro claro dizendo **qual nome** falta, nunca o valor.
- Os projetos usam dados públicos/sintéticos: se algo "precisa" de credencial, questione antes.

## .env

- `.env` e `.env.*` estão no `.gitignore`; apenas `.env.example` pode ser versionado,
  com nomes de variáveis e valores marcadores (`DATABRICKS_HOST=<your-workspace-url>`).
- O agente não lê `.env` (negado em `.claude/settings.json`); peça ao usuário o nome da
  variável, não o conteúdo.

## Tokens

- Nunca colar, ecoar, imprimir ou registrar um token — nem parcialmente.
- Exemplos e testes usam marcadores (`<DATABRICKS_TOKEN>`) ou valores claramente falsos,
  montados de forma que não casem com padrões reais (o scanner `test_no_secrets.py` falha a suíte).
- Token exposto em qualquer lugar = rotacionar imediatamente (remover do código não basta).

## Logs

- Proibido logar: headers de autenticação, tokens, URLs com query de credencial,
  conteúdo de secrets, linhas com PII.
- Logar: identificadores técnicos (`_batch_id`, tabela, contagens, duração, status HTTP).
- Exceções de HTTP/conexão: logar mensagem e status, não o objeto de requisição completo.
- `display()`/`show()` em notebook também é saída: não exibir PII nem secrets
  (o Databricks mascara secrets lidos via `dbutils`, mas não confie nisso para transformações).

## PII

- Dados pessoais do Retail são **sintéticos**, mas tratados como reais.
- Campos PII (nome, e-mail, telefone, CPF, endereço) identificados no schema Silver.
- Gold: PII mascarada ou pseudonimizada (hash com salt vindo de secret, não constante);
  agregados preferidos a dados por pessoa.
- Nenhum dado real de pessoa física em fixtures, exemplos ou notebooks.
- Dados financeiros públicos (BCB/CVM) podem conter CNPJ de empresas/fundos: não é PII de
  pessoa física, mas não enriquecer com dados de pessoas.

## Git

- Antes de commitar: `git status` + `git diff --cached` revisados; adicionar arquivos
  por nome (evitar `git add -A` às cegas).
- Rodar `pytest -m contract` (inclui scanner de segredos) antes de commit.
- Commit e push somente quando o usuário pedir.
- Proibidos sem autorização explícita: `git reset --hard`, `git clean -fd`,
  `git checkout -- .`, `git restore .`, `git push --force`, `rm -rf` (lista do `CLAUDE.md`).
- Segredo já commitado: rotacionar primeiro; reescrever histórico só com autorização.

## Checklist rápido

- [ ] Nenhum valor sensível em código, config, teste, notebook ou documentação
- [ ] Logs e `display` sem tokens/PII
- [ ] `.env` não staged; `pytest -m contract` verde
