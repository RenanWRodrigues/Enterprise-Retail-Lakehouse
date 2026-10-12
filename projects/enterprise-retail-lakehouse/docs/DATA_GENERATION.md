# Data Generation — Enterprise Retail Lakehouse

Gerador **determinístico** dos 11 datasets de origem ([DATA_CONTRACTS.md](DATA_CONTRACTS.md)),
simulando os sistemas descritos em [SOURCE_SYSTEMS.md](SOURCE_SYSTEMS.md). Python puro
(stdlib): sem Spark, sem Faker. Código: `src/retail_lakehouse/synthetic/`.

## Como executar

```bash
# Landing zone completa (~1 min, ~130 MB) em data/landing/retail (ignorado pelo Git)
PYTHONPATH="projects/enterprise-retail-lakehouse/src;shared/src" \
  .venv/Scripts/python -m retail_lakehouse.synthetic --output data/landing/retail

# Outra configuração
... -m retail_lakehouse.synthetic --config caminho/para/config.toml --output <dir>

# Lote incremental (CDC) de um plano: mudanças de cadastro, novos clientes/produtos, novas vendas
... -m retail_lakehouse.synthetic --output <dir>   --plan projects/enterprise-retail-lakehouse/config/incremental.toml --batch batch_002
```

`PYTHONPATH` é necessário até os pacotes serem instaláveis (build backend, Fase 1).
No Databricks, `--output` aponta para um Volume (`/Volumes/<catalog>/<schema>/<volume>/landing`).

API Python:

```python
from retail_lakehouse.synthetic import FileSink, generate_retail_dataset, load_config

config = load_config(Path("projects/enterprise-retail-lakehouse/config/data_generation.toml"))
with FileSink(Path("data/landing/retail"), config=config) as sink:
    summary = generate_retail_dataset(config, sink)
```

## Configuração (`config/data_generation.toml`)

| Chave `[generation]` | Padrão | Significado |
|---|---|---|
| `seed` | `20240101` | semente única de toda a geração |
| `start_date` / `end_date` | `2024-01-01` / `2025-12-31` | janela de pedidos (24 meses) |
| `customers` / `products` / `stores` / `suppliers` / `orders` | 50.000 / 2.000 / 30 / 100 / 100.000 | volumes (stores ≥ 2: a última é a loja online) |
| `batch_id` | `initial` | sufixo dos arquivos |

Cada `[[anomalies]]` tem `type`, `dataset`, `column` (exceto duplicatas) e `rate`
(fração de registros selecionados, 0–1). `rate = 0` ou remover a entrada desativa.
Combinações não suportadas são rejeitadas no carregamento. `GenerationConfig()` sem
arquivo = mesmos volumes, **sem** anomalias.

## Garantias de determinismo

| Mecanismo | Efeito |
|---|---|
| Um stream aleatório por domínio: `Random(f"{seed}:{nome}")` (sementes `str` usam SHA-512, imunes a `PYTHONHASHSEED`) | mudar `orders` não altera clientes, produtos ou fornecedores |
| Seleção de anomalias por hash (`blake2b(seed, regra, business_key)`) | ligar/desligar/recalibrar uma regra não muda nenhum outro registro |
| Anomalias aplicadas só na escrita | valores derivados (pagamento, estoque) vêm sempre dos dados limpos |
| Sem timestamps de execução; `\n` fixo; ordem estável; manifest sem `generated_at` | mesma configuração ⇒ arquivos **byte a byte idênticos** (testado por SHA-256) |

## Como os dados são gerados

**Dados mestres** — 8 departamentos e 30 subcategorias (faixas de preço por subcategoria,
preço log-uniforme terminando em `,90`); 29 lojas em capitais/polos regionais (1 por cidade,
ordenadas por peso populacional) + 1 CD de e-commerce em São Paulo; clientes distribuídos
pelas 30 cidades conforme peso, cadastrados de 2019 até `end_date` (ids crescentes no tempo);
3% dos produtos inativos (nunca vendidos); popularidade de produto com cauda longa (Pareto).

**Sazonalidade da demanda** (`seasonality.py`):

| Fator | Valores |
|---|---|
| Mês | jan 0,80 · fev 0,85 · mar/abr 0,95 · mai 1,10 · jun/jul 1,00 · ago 1,05 · set 0,95 · out 1,05 · nov 1,35 · dez 1,50 |
| Dia da semana | seg/ter 0,90 · qua 0,95 · qui 1,00 · sex 1,15 · sáb 1,25 · dom 0,95 |
| Black Friday (sexta após a 4ª quinta de novembro) | ×4,0; sábado a Cyber Monday ×2,0; 6 dias antes ×1,5 |
| Natal | 15–23/dez ×1,4; 25/dez e 1/jan ×0,3 |
| Dia das Mães (2º domingo de maio) | semana anterior ×1,3 |
| Crescimento | +12% ao ano, linear |
| Hora do dia | picos 12–13h e 18–20h |
| Promoções | Black week: 60% dos itens com 15–40% de desconto; novembro 30% (5–15%); dezembro 25% (5–10%); demais 8% (5–10%) |

**Ciclo de vida de cada pedido**:

1. Canal: loja 60% · site 25% · app 15%. Pedidos `online`/`app` saem do CD de e-commerce;
   pedidos `store` vão para uma loja do estado do cliente (senão da região).
2. Cliente entre os já cadastrados no instante do pedido (viés para clientes antigos).
3. 1–8 itens distintos (média 3,26) do sortimento da loja (lojas físicas: ~50% do catálogo).
4. Frete por região (grátis acima de R$ 299, zero em loja); pagamento = itens líquidos + frete;
   cartão de crédito acima de R$ 100 parcela em 1–10x; boleto compensa em 1–2 dias.
5. Status: 3% cancelado (pagamento recusado); pedidos online recebem envio e entrega com
   prazos por região; 2% dos entregues são devolvidos 7 dias depois (estorno + entrada no
   estoque); pedidos cujo pagamento/envio/entrega cairia após `end_date` ficam
   `created`/`paid`/`shipped`. 2% dos pagamentos têm tentativa recusada antes da aprovada.
6. Estoque: saldo inicial por loja×produto; cada venda baixa estoque; se o saldo não
   cobre a venda, uma reposição (`purchase_receipt`) entra 1 h antes. A foto `inventory` em
   `end_date` é exatamente a soma dos movimentos.

## Anomalias intencionais

| Tipo | Alvos suportados | O que é injetado |
|---|---|---|
| `duplicate_record` | `customers`, `order_items` | linha repetida, mesma chave |
| `duplicate_payment` | `payments` | evento reenviado (mesmo `payment_id`) |
| `null_value` | `customers.email`, `customers.birth_date`, `products.supplier_id` | NULL em coluna obrigatória |
| `invalid_email` | `customers.email`, `suppliers.email` | `a#b`, `a@`, `@b`, `a@@b`, `a @b` |
| `negative_price` | `products.list_price`, `order_items.unit_price` | preço com sinal negativo |
| `invalid_quantity` | `order_items.quantity`, `inventory_movements.quantity` | itens: `0` ou negativo; movimentos: `0` |
| `orphan_foreign_key` | `products.category_id`, `orders.customer_id`, `order_items.product_id`, `payments.order_id` | id com o formato correto, número iniciado em 9 (fora da faixa gerada) |
| `invalid_date` | `orders.order_timestamp`, `customers.birth_date`, `shipments.delivered_at` | dia/mês impossível ou formato `dd/MM/yyyy`; nascimento após 2090; entrega antes do envio |

Regras de aplicação: no máximo uma anomalia de valor por célula (a primeira na ordem de
`SUPPORTED_ANOMALIES`); duplicatas são aplicadas por último e copiam a linha já alterada;
uma regra que não se aplica (ex.: entrega ainda `NULL`) é ignorada sem registro.

**Registro** (`_anomalies_<batch_id>.csv`): `anomaly_id, anomaly_type, dataset, column, record_key,
original_value, injected_value` — `record_key` é a business key unida por `|`. A Silver pode
usá-lo como gabarito para medir se a quarentena pegou exatamente o que foi injetado.

## Resultado com a configuração do repositório

Execução real (`seed = 20240101`), ~68 s em um notebook Windows:

| Dataset | Linhas | Dataset | Linhas |
|---|---:|---|---:|
| categories | 38 | orders | 100.000 |
| suppliers | 100 | order_items | 325.243 |
| stores | 30 | payments | 103.008 |
| products | 2.000 | shipments | 38.405 |
| customers | 50.237 (50.000 + 237 duplicatas) | inventory | 30.311 |
| | | inventory_movements | 355.723 |

| Anomalia | Registros |
|---|---:|
| null_value | 1.513 |
| duplicate_record | 547 |
| invalid_email | 538 |
| invalid_quantity | 526 |
| orphan_foreign_key | 452 |
| invalid_date | 299 |
| duplicate_payment | 284 |
| negative_price | 159 |

Sazonalidade observada: nov/2024 = 6.271 e dez/2024 = 6.676 pedidos contra 2.835 em
fev/2024; Black Friday 2024 = 773 pedidos contra 190 numa sexta comum (08/11).

## Modo incremental (CDC)

O modo full continua o mesmo (`generate_retail_dataset`; sem `--plan`). O incremental
(`retail_lakehouse.synthetic.incremental`) continua um lote full.

**Plano** `config/incremental.toml`:

- `base_config`: o lote full que os lotes continuam;
- `[[batches]]` em ordem, cada um com:
  - `batch_id`;
  - `as_of`, o último dia da janela; a janela começa no dia seguinte ao fim da anterior;
  - `customer_change_rate`, `product_change_rate`;
  - `new_customers`, `new_products`;
  - `orders`.

**Como o lote N é produzido:**

1. os dados mestres do lote full são reconstruídos da `base_config` (determinístico: mesmos IDs,
   nada é lido do disco);
2. as mudanças dos lotes 1..N−1 são reaplicadas, então o lote N parte do estado deixado pelos
   anteriores;
3. o lote N aplica as suas.

| Mudança | Como |
|---|---|
| clientes alterados | `customer_change_rate` dos clientes (sorteio sem reposição), nunca todos: cidade (outra cidade, peso por população; `state` acompanha), segmento (`standard` ↔ `premium`) ou ambos. Nome, e-mail, nascimento e cadastro não mudam |
| produtos alterados | `product_change_rate` dos produtos ativos: preço (±5–15%) ou categoria (outra folha do mesmo departamento); `updated_at` = data da mudança; SKU estável |
| novos clientes e produtos | numeração continua a do lote full e a dos lotes anteriores (`CUS-0000201`…, `PRD-000061`…); cadastro e criação dentro da janela |
| novas vendas | o ciclo de pedido do modo full (canal, loja, itens, pagamentos, entregas, movimentos de estoque) na janela, sobre os clientes cadastrados até a hora do pedido e os produtos ativos com o preço vigente |

**Saída (CDC):**

- só os clientes e produtos alterados ou novos e as transações da janela; os demais datasets
  saem vazios (com cabeçalho);
- `_changes_<lote>.csv`: um evento por atributo alterado ou entidade criada, com `event_id`
  determinístico, valor antigo e novo e **data real da mudança**;
- `_increment_<lote>.json`: `batch_fingerprint`, o SHA-256 da configuração base e dos lotes até
  este, que identifica o lote de forma reprodutível;
- o manifesto usual (`_manifest_<lote>.json`).

**Identificadores:** transações novas usam o lote como espaço de nomes
(`ORD-batch_002-000001`, `PAY-…`, `SHP-…`, `MOV-…`), então nunca colidem com o lote full nem com a
faixa de chaves órfãs (`9…`). Anomalias intencionais existem só no modo full.

**Data da mudança × versão SCD2:** o CRM exporta o cadastro sem data de alteração (contrato), então
a Silver abre a nova versão do cliente na ingestão do lote que revelou a mudança
([SILVER.md](SILVER.md)). A data real fica em `_changes_<lote>.csv`. A diferença entre as duas é o
atraso de exportação do CRM: uma venda entre a mudança real e a ingestão do lote fica com a versão
anterior. É o comportamento correto para os dados que a fonte entrega.

## Testes

| Arquivo (`tests/`) | Garante |
|---|---|
| `unit/test_generator_determinism.py` | mesma semente ⇒ linhas e arquivos idênticos; semente diferente ⇒ dados diferentes; streams independentes |
| `unit/test_source_contracts.py` | contratos consistentes e documentados neste diretório |
| `unit/test_generation_config.py` | TOML, volumes-alvo, validações, todos os tipos de anomalia habilitados |
| `unit/test_seasonality.py` | datas comerciais, curva de demanda, amostragem |
| `contract/test_generated_schema.py` | colunas/ordem/tipos/domínios dos dados limpos; arquivos CSV/JSONL fiéis |
| `contract/test_referential_expectations.py` | FKs, unicidade, reconciliação pagamento × itens + frete, estoque = Σ movimentos, coerência de datas — e só anomalias registradas violam |
| `data_quality/test_intentional_anomalies.py` | toda anomalia suportada é injetada, registrada, realmente inválida; taxas respeitadas; linhas não tocadas idênticas à geração limpa |
| `integration/test_full_scale_generation.py` | volumes-alvo, 24 meses, sazonalidade, 8 tipos de anomalia (≈ 40 s) |
| `unit/test_synthetic_incremental.py` | modo full intacto; incremental sem mudanças; 5% dos clientes alterados (só os atributos permitidos); mudanças em datas diferentes dentro da janela; novos produtos com numeração contínua; novas vendas com integridade referencial e sem colisão de IDs; mesmo lote duas vezes idêntico; outra semente, outra seleção; encadeamento entre lotes; `event_id` únicos e determinísticos; plano inconsistente recusado |
| `transformation/test_incremental_scd2.py` | pela Silver (`customer_versions`) e pela Gold (`build_dim_customer`, `build_fact_sales`): uma nova versão por cliente alterado, reprocessamento sem versão duplicada, uma versão corrente por cliente, versão anterior fechada onde a nova abre, sem sobreposição, venda associada à versão vigente na data de negócio |
| `transformation/test_json_key_check.py` | um arquivo JSON vazio (lote incremental sem pedidos) não é drift de schema |

## Limitações conhecidas

- O modo incremental começa cada lote com estoque fixo por loja e produto (sem saldo herdado do
  lote anterior) e não injeta anomalias.
- Reposição de estoque sob demanda (1 h antes da venda): mantém o estoque sempre ≥ 0,
  mas não modela ruptura nem lead time de fornecedor.
- Sazonalidade igual para todas as categorias (sem picos específicos, ex.: brinquedos em
  outubro).
- Sem CPF/CNPJ/telefone (ver `reference_data.py`): a demonstração de mascaramento de PII
  usa nome, e-mail e data de nascimento.
