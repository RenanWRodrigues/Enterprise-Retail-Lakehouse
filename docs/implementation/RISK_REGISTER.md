# Risk Register

Riscos da evolução planejada em [IMPROVEMENT_PLAN.md](IMPROVEMENT_PLAN.md), registrados em
2026-10-08. Probabilidade e impacto: Baixo / Médio / Alto. Revisar ao fim de cada etapa.

| ID | Risco | Prob. | Impacto | Mitigação | Gatilho / sinal |
|---|---|---|---|---|---|
| R01 | Testes de integração não rodam no Windows; regressões Delta só aparecem no Linux | Alta | Alto | rodar `make test-integration` no WSL ao fim de cada etapa; CI no primeiro push | etapa encerrada sem execução Linux |
| R02 | Memória da máquina insuficiente para a suíte completa (duas execuções interrompidas em 2026-10-07) | Alta | Médio | executar por categoria e por projeto; registrar o que não rodou; não reiniciar execuções interrompidas sem decisão | processo encerrado pelo Claude Code |
| R03 | `pytest -m transformation` inteiro passa de 590 s no Windows | Alta | Baixo | dividir por área (já documentado) | timeout de comando |
| R04 | Runtime serverless difere do local (Spark/Delta/Python, `MERGE` no Unity Catalog, coluna `table`) | Média | Alto | Etapa 3 antes de VOL; contrato `test_serverless_compatibility.py`; registrar versões | erro de análise/execução no workspace |
| R05 | Free Edition sem saída à internet bloqueia a Bronze financeira | Média | Alto | teste pequeno de conectividade (3.1); plano B: landing enviada ao Volume | timeout/recusa de conexão |
| R06 | M6 (CDF) exige alterar propriedades de tabela Bronze existente | Média | Médio | ADR antes; `ALTER TABLE` só com autorização; tabelas novas já criadas com CDF | Bronze sem `delta.enableChangeDataFeed` |
| R07 | Troca de watermark (M6) perde ou duplica linhas | Baixa | Alto | M1 antes (reconciliação CRITICAL); regressão do bug original; MERGE idempotente | reconciliação falha |
| R08 | Gate da Gold (M2) bloqueia produção por falso positivo | Média | Médio | M3 antes; FK de dimensão SCD2 avaliada contra a versão vigente pelo join point-in-time já usado; testes com Gold válida | run `FAILED` com dado correto |
| R09 | Alteração em `lakehouse_common` quebra outro projeto | Média | Alto | testes do shared + suíte dos três projetos a cada mudança; regra de admissão do `shared/README.md` | falha fora do projeto alterado |
| R10 | Escopo do IoT cresce além de uma etapa | Alta | Médio | entregar 4.1–4.4 em incrementos separados, cada um com testes e documentação | etapa sem critério de aceite fechado |
| R11 | Volume completo (1,0 milhão Retail; ~32 milhões IoT, estimativa) estoura memória/tempo local | Alta | Médio | proibido sem autorização; medir primeiro no Databricks | pedido de carga completa |
| R12 | Dependências transitivas mudam entre instalações (sem lock) | Média | Médio | M5; CI instala versões fixadas diretas | falha só na CI ou só local **Mitigado em 2026-10-13** (decisão 085: lock com hashes e `make lock-check` na CI) |
| R13 | Exposição de credenciais ao configurar Databricks ou alertas | Baixa | Alto | Databricks Secrets; `.env` ignorado e não lido pelo agente; scanner `test_no_secrets.py` | token em diff ou log |
| R14 | E-mail pessoal no histórico ao publicar (sem commits ainda) | Alta | Baixo | configurar e-mail `noreply` do GitHub antes do primeiro commit | `git config user.email` não `noreply` |
| R15 | Repositório sem commits: nenhuma forma de reverter mudanças desta evolução | Alta | Alto | recomendar o commit inicial do estado atual antes da Etapa 1 (só com autorização) | primeira alteração de pipeline sem commit base |
| R16 | Flakes de shuffle do Spark no Windows mascaram resultado | Baixa | Baixo | repetir o arquivo isolado; Linux como referência | `FileNotFoundException` em temporário do Spark |
| R17 | Documentação diverge do código após cada etapa | Média | Médio | contratos de documentação; atualizar README/FINAL_AUDIT/PORTFOLIO no fim da etapa | número antigo em documento |

## Riscos que bloqueiam o início da Etapa 1

- **R15** é o principal: sem commit base, um erro na Etapa 1 só pode ser desfeito à mão. A decisão
  de commitar é do responsável.
