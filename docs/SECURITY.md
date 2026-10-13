# Security

## 1. Regras inegociáveis

- Nunca commitar tokens, senhas, API keys, chaves privadas ou arquivos `.env`.
- Nunca registrar secrets em logs, mensagens de erro ou saídas de notebook.
- Nunca criar credenciais *aparentemente reais*, nem em exemplos ou testes.
  Exemplos usam marcadores óbvios: `<DATABRICKS_TOKEN>`, `<your-workspace-url>`.

## 2. Onde credenciais vivem

| Contexto | Mecanismo |
|---|---|
| Databricks (notebooks/jobs) | Databricks Secrets: `dbutils.secrets.get(scope, key)` |
| Máquina local | variáveis de ambiente ou `~/.databrickscfg` (fora do repositório) |
| CI (futuro) | secrets do provedor de CI, nunca no YAML |
| Webhook de alertas (opcional) | `LAKEHOUSE_ALERT_WEBHOOK_URL` no ambiente do job, preenchida a partir do Databricks Secrets; só `https`; nunca registrada em log (`lakehouse_common.logging.alerts`) |

O valor de um secret é lido no ponto de uso e não é atribuído a variáveis globais,
impresso ou passado para `display()`.

> Os três projetos usam dados **públicos ou sintéticos** — a expectativa é que
> nenhum secret seja necessário para executá-los.

## 3. Mecanismos de proteção no repositório

| Mecanismo | O que faz |
|---|---|
| `.gitignore` | exclui `.env*` (exceto `.env.example`), `.databrickscfg`, `*.pem`, `*.key`, `secrets/` |
| `.claude/settings.json` | impede o agente de ler `.env`/`.databrickscfg`/`secrets/` e de executar comandos Git/shell destrutivos |
| `tests/contract/test_no_secrets.py` | varre o repositório por padrões de segredos e falha a suíte |
| ruff `S` (bandit) | detecta padrões inseguros no código Python |

O scanner de testes é uma rede de segurança, não um substituto para ferramentas dedicadas.
Recomendação para a fase de CI: adicionar `gitleaks` como hook de pre-commit e etapa de CI.

## 4. Dados sensíveis (PII)

- Dados de clientes no Retail são **sintéticos**; ainda assim, campos como e-mail e
  documento são tratados como PII: mascarados ou com hash nas camadas Gold.
- Dados financeiros brasileiros vêm de fontes públicas (BCB, CVM).
- Nenhum dado real de pessoa física é usado no portfólio.

## 5. Se um segredo vazar

1. **Revogar/rotacionar** a credencial imediatamente (remover do código não basta).
2. Remover do código e registrar o incidente.
3. Reescrever o histórico do Git somente com autorização explícita do responsável
   (operação destrutiva — ver *Git Safety* no `CLAUDE.md`).
