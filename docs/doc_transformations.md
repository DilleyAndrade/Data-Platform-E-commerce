# Processo de transformação de dados

## 1. Visão geral

A pasta `src/transformation` implementa o fluxo:

```text
Raw -> Bronze -> Silver -> Gold
```

Cada camada possui uma finalidade:

- **Bronze:** tipa, normaliza e adiciona rastreabilidade, preservando registros
  válidos e inválidos;
- **Silver:** aplica regras de negócio e integridade, separa rejeições e
  quarentena e mantém uma visão consolidada por chave;
- **Gold:** constrói fatos, dimensões, agregações, visões 360 e KPIs.

## 2. Estrutura

```text
src/transformation/
|-- __init__.py
|-- transformation_raw_bronze.py
|-- transform_bronze_silver.py
`-- transform_silver_gold.py
```

`__init__.py` exporta as três funções públicas:

```python
transformation_raw_bronze(spark, run_id, execution_date, s3_client)
transform_bronze_silver(spark, run_id, execution_date)
transform_silver_gold(spark, run_id, execution_date)
```

## 3. Arquitetura e destinos

```text
s3://raw/<dataset>/ingestion_date_YYYYMMDD/
  -> s3a://bronze/<dataset>
  -> s3a://silver/<dataset>
  -> s3a://gold/<datamart>/<table>
```

Saídas auxiliares:

```text
s3a://rejected/bronze_to_silver/<dataset>
s3a://quarantine/silver/<dataset>
s3a://observability/transformation_log
s3a://observability/gold_validation_failures
```

Os contratos vêm de `BRONZE_DATASET_SCHEMAS`,
`DATASET_REQUIRED_FIELDS` e `DATASET_PRIMARY_KEYS`. Consulte
`docs/doc_schemas.md` para o catálogo campo a campo.

# Parte I — Raw para Bronze

## 4. Seleção e leitura

`transformation_raw_bronze` percorre os 13 datasets do catálogo. Para cada um,
consulta o prefixo Raw com Boto3 e `MaxKeys=1`:

```text
<dataset>/ingestion_date_YYYYMMDD/
```

Prefixos ausentes são ignorados sem evento. A função exige sessão Spark,
cliente S3 e `execution_date` do tipo `date`.

| Formato | Leitura |
| --- | --- |
| CSV | Cabeçalho e inferência de schema. |
| JSON | `multiLine=True`; JSON vazio recebe o schema esperado. |
| Parquet | Leitura Parquet padrão. |

Outros formatos falham.

## 5. Normalização e tipagem

Cada campo esperado percorre:

```text
valor -> string -> trim -> sentinelas para null -> tipo Spark final
```

As sentinelas `""`, `"null"` e `"n/a"`, sem diferença entre maiúsculas e
minúsculas, tornam-se nulas. Todas as colunas do contrato precisam existir;
colunas extras não são projetadas para a Bronze.

Uma falha de cast ocorre quando o original normalizado não é nulo, mas o cast
resulta em nulo.

## 6. Qualidade Bronze

`_record_status` recebe `INVALID` se houver:

- falha de cast;
- nulo em campo de `DATASET_REQUIRED_FIELDS`;
- outra linha idêntica em todas as colunas de negócio da carga.

Caso contrário, recebe `VALID`. Registros inválidos continuam na Bronze; serão
roteados pela Silver.

## 7. Metadados Bronze

| Campo | Conteúdo |
| --- | --- |
| `_source_file` | Arquivo físico lido pelo Spark. |
| `_record_status` | `VALID` ou `INVALID`. |
| `_bronze_run_id` | Execução atual. |
| `_source_name` | Origem lógica. |
| `_source_path` | Caminho Raw em `s3://`. |
| `_ingestion_date` | Data operacional. |
| `_bronze_processed_at` | Timestamp de processamento. |
| `_record_hash` | SHA-256 das colunas de negócio. |
| `_record_occurrence` | Ocorrência sequencial do mesmo hash. |

Para o hash, nulos viram `<NULL>` e valores são concatenados com `||`.
Ocorrências são ordenadas por arquivo e `monotonically_increasing_id()`.

## 8. Persistência Bronze

Uma tabela nova é gravada como Delta, sem partição física, com overwrite,
`mergeSchema=false` e `optimizeWrite` controlado por
`SPARK_DELTA_OPTIMIZE_WRITE`.

Tabelas existentes usam merge por:

```text
_ingestion_date + _record_hash + _record_occurrence
```

Correspondências são atualizadas e novas linhas inseridas. Tabelas legadas
fisicamente particionadas geram warning, mas continuam sendo processadas.

Reprocessar conteúdo idêntico na mesma data é idempotente nessas chaves.
Conteúdo alterado gera outro hash e pode coexistir com a versão anterior.

## 9. Evento Bronze

O evento usa `pipeline_name=raw_to_bronze` e `stage=bronze`.

- sucesso sem inválidos: qualidade `PASS`;
- sucesso com inválidos: `WARNING`;
- exceção: status `FAILED`, qualidade `FAIL` e entrada contabilizada como
  rejeitada.

Em sucesso, `records_rejected` permanece zero porque a Bronze preserva os
inválidos. `records_inserted` recebe todo o output, mesmo que o merge tenha
atualizado linhas; não é uma contagem exata de inserts Delta.

# Parte II — Bronze para Silver

## 10. Leitura incremental e ordem

A Silver filtra a Bronze por:

```text
_bronze_run_id = run_id AND _ingestion_date = execution_date
```

Tabela ausente ou filtro vazio é ignorado sem evento. Portanto, Bronze e
Silver precisam usar o mesmo run e data.

Ordem de processamento:

```text
customers, suppliers, products, inventory, coupons, orders,
order_items, delivery_tracking, payments, website_events,
customer_review, exchange_rates, marketing_campaigns
```

A ordem cria tabelas-pai antes das filhas.

## 11. Padronização

Todos os campos recebem novamente trim e cast ao tipo do contrato. Também são
convertidos para minúsculas:

| Dataset | Colunas |
| --- | --- |
| `customers` | `email`, `city`, `state`, `customer_type`, `status` |
| `suppliers` | `city`, `state` |
| `products` | `category`, `subcategory`, `brand` |
| `orders` | `status` |
| `delivery_tracking` | `status` |
| `payments` | `payment_method`, `payment_status` |
| `website_events` | `event`, `page` |
| `marketing_campaigns` | `channel`, `status` |

`_add_calculated_fields` é atualmente neutra: valores fornecidos pela origem
são preservados e apenas validados.

## 12. Roteamento

As regras acumulam motivos separados por `;`:

- `_reject_reason`: erro determinístico;
- `_quarantine_reason`: suspeita que requer análise.

| Condição | Destino |
| --- | --- |
| Bronze diferente de `VALID` | Rejected |
| Regra Silver de rejeição | Rejected |
| Sem rejeição, mas com alerta | Quarantine Silver |
| Sem motivos | Silver |

## 13. Regras de rejeição por dataset

Todas as chaves primárias rejeitam nulo; chaves string também rejeitam vazio.

| Dataset | Regras adicionais |
| --- | --- |
| `customer_review` | `rating` entre 1 e 5. |
| `coupons` | Desconto não negativo; percentual até 100; intervalo de datas válido; tipo `percentage` ou `fixed`. |
| `products` | `price` presente e não negativo. |
| `marketing_campaigns` | `budget` não negativo e término não anterior ao início. |
| `inventory` | Quantidades, ponto de reposição e estoque de segurança não negativos. |
| `order_items` | Quantidade positiva; preço não negativo; total da linha consistente. |
| `orders` | Total do pedido consistente. |
| `payments` | Valor não negativo; estorno entre zero e o valor pago; método permitido. |
| `customers` | Tipo `individual` ou `business`. |
| `exchange_rates` | Taxas USD/BRL e EUR/BRL positivas. |

Fórmulas, com tolerância de 0,01:

```text
line_total = quantity * unit_price - discount_amount + tax_amount
total_amount = subtotal_amount - discount_amount + shipping_amount + tax_amount
```

Métodos de pagamento aceitos: `pix`, `credit_card`, `debit_card`, `boleto`.

## 14. Domínios comuns

Status válidos:

| Dataset | Valores |
| --- | --- |
| `orders` | `pending`, `paid`, `shipped`, `delivered`, `cancelled`, `refunded` |
| `delivery_tracking` | `created`, `shipped`, `in_transit`, `out_for_delivery`, `delivered`, `failed`, `returned` |
| `payments` | `pending`, `authorized`, `approved`, `declined`, `partially_refunded`, `refunded`, `cancelled` |
| `customers` | `active`, `inactive`, `blocked` |

Moedas devem seguir `^[A-Z]{3}$`; códigos de país, `^[A-Z]{2}$`. Quando ambos
existem, `updated_at` não pode anteceder `created_at`.

## 15. Regras de quarentena

### Completude

O percentual usa somente campos obrigatórios. Nulos e strings vazias não
contam. Abaixo de 80% gera `LOW_COMPLETENESS`.

### Outliers

| Dataset/campo | Limite |
| --- | ---: |
| `payments.amount` | 1.000.000 |
| `products.price` | 100.000 |
| `inventory.quantity_available` | 1.000.000 |
| `order_items.quantity` | 10.000 |
| `order_items.unit_price` | 100.000 |
| `marketing_campaigns.budget` | 100.000.000 |

Somente valores estritamente maiores são quarantinados.

### Datas futuras

São avaliados `orders.order_date`, `delivery_tracking.occurred_at`,
`website_events.timestamp`, `customers.created_at`, `inventory.updated_at` e
`exchange_rates.date`. A condição é posterior a `execution_date + 1`; o dia
seguinte ainda é aceito.

## 16. Chaves estrangeiras

| Filho | Campo | Pai | Campo |
| --- | --- | --- | --- |
| `products` | `supplier_id` | `suppliers` | `supplier_id` |
| `inventory` | `product_id` | `products` | `product_id` |
| `orders` | `customer_id` | `customers` | `customer_id` |
| `orders` | `coupon_code` | `coupons` | `coupon` |
| `order_items` | `order_id` | `orders` | `order_id` |
| `order_items` | `product_id` | `products` | `product_id` |
| `delivery_tracking`, `payments` | `order_id` | `orders` | `order_id` |
| `website_events` | `customer_id`, `product_id`, `order_id` | respectivos pais | respectivos IDs |
| `customer_review` | `customer_id`, `product_id`, `order_id` | respectivos pais | respectivos IDs |

Se a tabela pai não existe, todo filho não nulo é rejeitado. Valores nulos só
são rejeitados se outra regra os tornar obrigatórios.

Para entregas, `updated_at < orders.order_date` gera
`DELIVERY_BEFORE_ORDER_DATE`.

## 17. CDC e merge Silver

Dentro da carga atual, vence uma linha por chave, ordenada de forma decrescente
por `updated_at`, `_bronze_processed_at` e `_record_occurrence`.

Metadados publicados:

```text
_source_file, _source_path, _ingestion_date,
_silver_record_hash, _silver_run_id, _silver_processed_at
```

Uma tabela nova usa overwrite. A existente usa merge pela chave primária. Uma
linha só é atualizada quando:

```text
source.updated_at >= target.updated_at
AND hashes Silver são diferentes
```

Chaves novas são inseridas. Não há delete CDC.

## 18. Rejected e Quarantine Silver

Rejected recebe inválidos Bronze e violações determinísticas em:

```text
s3a://rejected/bronze_to_silver/<dataset>
```

Adiciona hash, estágio, run, timestamp e data. Quarantine recebe alertas em:

```text
s3a://quarantine/silver/<dataset>
```

Ambos fazem merge por `run_id + hash`, evitando repetição dentro do mesmo run.
Um novo run pode registrar o problema novamente.

A correção automática do projeto atua na quarentena do Landing; não há etapa
automática que promova a quarentena Silver.

## 19. Evento Silver

`records_rejected` soma inválidos Bronze, rejeitados Silver e quarantinados.

| Resultado | Qualidade |
| --- | --- |
| Falha | `FAIL` |
| Sucesso com rejeição/quarentena | `WARNING` |
| Sucesso limpo | `PASS` |

As contagens de inserts e updates são estimadas antes do merge. Registros
existentes inalterados entram no output, mas não nessas duas contagens.

# Parte III — Silver para Gold

## 20. Estratégia Gold

A Gold lê o estado completo das tabelas Silver, sem filtrar run ou data.
Cada saída recebe `_gold_run_id`, `_gold_execution_date` e
`_gold_processed_at`.

A escrita usa Delta com overwrite e `overwriteSchema=true`. Depois, o destino
é relido e a quantidade com o run atual precisa coincidir com a calculada;
senão ocorre `GOLD_FRESHNESS_VOLUME_MISMATCH`.

São oito datamarts e 27 tabelas.

## 21. Catálogo de datamarts

### `master_data`

Fontes: clientes, produtos e fornecedores.

| Tabela | Grão | Conteúdo |
| --- | --- | --- |
| `dim_customers` | `customer_id` | Dados principais do cliente. |
| `dim_suppliers` | `supplier_id` | Nome e cidade do fornecedor. |
| `dim_products` | `product_id` | Produto, categoria, preço e fornecedor. |

### `sales`

Fontes: pedidos, itens, clientes, produtos e fornecedores.

| Tabela | Grão | Métricas |
| --- | --- | --- |
| `fact_sales` | `order_item_id` | Quantidade, preço e receita bruta. |
| `fact_orders` | `order_id` | Unidades, total e linhas do pedido. |
| `sales_daily` | `order_date` | Pedidos, clientes, unidades, receita e médias. |
| `sales_by_product` | `product_id` | Pedidos, clientes, unidades, receita e preço médio. |
| `sales_by_customer` | `customer_id` | Pedidos, unidades, lifetime value e datas. |
| `sales_by_category` | `category` | Pedidos, clientes, unidades e receita. |
| `sales_by_state` | `customer_state` | Pedidos, clientes e receita. |

Os fatos usam inner joins. `fact_orders` parte de todos os pedidos e usa left
join com totais dos itens, preenchendo zeros quando não há itens.

### `finance`

| Tabela | Grão | Conteúdo |
| --- | --- | --- |
| `fact_payments` | `payment_id` | Pagamento enriquecido com pedido e cliente. |
| `payments_daily` | data + método + status | Contagem, pedidos, soma e média. |

### `supply_chain`

| Tabela | Grão | Conteúdo |
| --- | --- | --- |
| `inventory_snapshot` | `product_id` | Estoque, preço, valor e ruptura. |
| `inventory_summary` | categoria + fornecedor | Produtos, unidades, valor e rupturas. |

`inventory_value = quantity_available * product.price` e ruptura significa
quantidade igual a zero.

### `logistics`

| Tabela | Grão | Conteúdo |
| --- | --- | --- |
| `fact_deliveries` | `tracking_id` | Entrega, pedido, cliente, status e horas desde pedido. |
| `delivery_performance` | status + estado | Entregas, pedidos e média de horas. |

### `customer_experience`

| Tabela | Grão | Conteúdo |
| --- | --- | --- |
| `fact_reviews` | `review_id` | Avaliação com cliente e produto. |
| `product_satisfaction` | `product_id` | Total, média, positivas e negativas. |
| `fact_website_events` | `event_id` | Evento web com estado do cliente. |
| `digital_engagement` | data + evento + página | Eventos e clientes únicos. |

Notas >= 4 são positivas; <= 2, negativas. Eventos sem cliente correspondente
somem no inner join.

### `commercial`

| Tabela | Grão | Conteúdo |
| --- | --- | --- |
| `coupon_portfolio` | `coupon` | Desconto, vigência e atividade. |
| `campaign_portfolio` | `campaign_id` | Campanha, canal e orçamento. |
| `marketing_budget_by_channel` | `channel` | Quantidade, total e média de orçamento. |
| `exchange_rates_daily` | `date` | USD/BRL e EUR/BRL. |

### `executive_analytics`

| Tabela | Grão | Conteúdo |
| --- | --- | --- |
| `customer_360` | `customer_id` | Vendas, pagamentos, reviews e engajamento. |
| `product_360` | `product_id` | Vendas, satisfação, estoque e fornecedor. |
| `executive_kpis` | Uma linha | Pedidos, receita, pagamentos, estoque e experiência. |

As visões 360 partem das entidades principais e usam left joins. Contagens
ausentes recebem zero; médias e valores sem informação podem continuar nulos.

## 22. Validações Gold

Todas as tabelas de um datamart são construídas e cacheadas antes da primeira
escrita. Havendo erro, as falhas são registradas, nenhuma tabela daquele
datamart é publicada nessa tentativa e surge um evento `FAILED` para
`datamart_build`.

### Estrutura

Os grãos cadastrados não podem ser nulos ou duplicados. Erros:

```text
NULL_GRAIN:<table>:<columns>
DUPLICATE_GRAIN:<table>:<columns>
```

`executive_kpis` não possui grão cadastrado.

### KPIs não negativos

São validadas métricas selecionadas de vendas, pedidos, pagamentos, estoque,
marketing e visões executivas. Uma violação gera `NEGATIVE_KPI:<table>`.

### Volumes

Devem igualar suas fontes Silver:

```text
dim_customers=customers; dim_suppliers=suppliers; dim_products=products;
fact_sales=order_items; fact_orders=orders; fact_payments=payments;
inventory_snapshot=inventory; fact_deliveries=delivery_tracking;
fact_reviews=customer_review; fact_website_events=website_events;
coupon_portfolio=coupons; campaign_portfolio=marketing_campaigns;
exchange_rates_daily=exchange_rates; customer_360=customers;
product_360=products
```

### Reconciliações

- receita do fato e agregações deve igualar `order_items.line_total`;
- todos os dias de pedidos devem aparecer em `sales_daily`;
- pagamentos Silver, fato e resumo diário devem reconciliar;
- nota média deve permanecer entre 1 e 5;
- rupturas não podem superar produtos;
- KPIs executivos de pedidos, cancelamentos e receita são recalculados.

Valores monetários são comparados por igualdade direta, sem tolerância.

## 23. Falhas e anomalias Gold

Falhas de validação são anexadas em:

```text
s3a://observability/gold_validation_failures
```

Cada evento contém run, datamart, tipo, mensagem, tabelas candidatas, status e
timestamps. Sucessos não geram linha nessa tabela.

Antes do overwrite, quatro métricas são comparadas com a Gold anterior:

| Tabela | Métrica |
| --- | --- |
| `sales_daily` | `gross_revenue` |
| `payments_daily` | `payment_amount` |
| `inventory_summary` | `inventory_value` |
| `executive_kpis` | `gross_revenue` |

Variação absoluta maior que 50% gera warning, mas não bloqueia publicação. Se
o valor anterior for zero, a checagem é ignorada. O evento fica `SUCCESS`, com
qualidade `WARNING` e a anomalia em `error_message`.

## 24. Ausência de Silver e atomicidade

Se uma fonte Silver obrigatória não existir, o datamart é pulado com warning,
sem evento `FAILED`.

A validação antecede a escrita, mas as tabelas são sobrescritas uma a uma. Se
uma escrita intermediária falhar, tabelas anteriores ficam na nova versão e
as seguintes na antiga. Não há transação de datamart.

## 25. Cache e contagens Gold

Cada fonte Silver é carregada e contada uma vez, sendo reutilizada entre
datamarts. Tabelas construídas também são cacheadas durante validação e escrita.

`records_input` é a soma das contagens completas das fontes exigidas e se
repete em cada evento do datamart; não representa necessariamente as linhas
que contribuíram para uma tabela depois de joins.

## 26. Observabilidade compartilhada

Todos os eventos vão para:

```text
s3a://observability/transformation_log
```

Eles registram pipeline, estágio, tabelas, caminhos, volumes, inserts,
updates, deletes, qualidade, duração, status e erro. Caminhos são apresentados
como `s3://`.

Na Gold há um evento por tabela publicada. Falha anterior às escritas gera
evento sintético `datamart_build`.

## 27. Tratamento de erros

- Bronze e Silver capturam exceções por dataset, registram `FAILED` e seguem;
- Gold captura exceções por datamart e segue para o próximo;
- ausência de fonte é normalmente tratada como skip, não falha;
- ao final do script CLI, `raise_for_failed_events` transforma qualquer evento
  `FAILED` em erro de processo;
- falha ao gravar o próprio log de transformação é propagada.

## 28. Pré-requisitos

- Python, Java, PySpark e Delta Lake configurados;
- MinIO/S3 acessível e buckets necessários existentes;
- credenciais com listagem, leitura e escrita;
- `PYTHONPATH` contendo `src` e a raiz.

```powershell
.\.venv\Scripts\Activate.ps1
$env:PYTHONPATH = "$PWD\src;$PWD"
$env:AWS_ENDPOINT_URL = "http://localhost:9000"
$env:AWS_ACCESS_KEY_ID = "<access-key>"
$env:AWS_SECRET_ACCESS_KEY = "<secret-key>"
```

Configurações úteis:

```dotenv
SPARK_MASTER=local[*]
SPARK_SHUFFLE_PARTITIONS=4
SPARK_DEFAULT_PARALLELISM=4
SPARK_DELTA_OPTIMIZE_WRITE=false
SPARK_DELTA_AUTO_COMPACT=false
```

## 29. Execução isolada

```powershell
python src/transformation/transformation_raw_bronze.py `
  --run-id "manual__2026-09-20" --date "2026-09-20"

python src/transformation/transform_bronze_silver.py `
  --run-id "manual__2026-09-20" --date "2026-09-20"

python src/transformation/transform_silver_gold.py `
  --run-id "manual__2026-09-20" --date "2026-09-20"
```

Bronze e Silver precisam compartilhar run e data. Gold lê a Silver inteira; o
run serve para metadados e logs.

## 30. Pipeline completo e Airflow

```powershell
python src/run_all_pipeline.py `
  --run-id "manual__pipeline_20260920" `
  --date "2026-09-20" `
  --watermark-until "2026-09-21T00:00:00Z"
```

Ordem completa:

```text
ingestões -> qualidade -> correção -> Bronze -> Silver -> Gold -> watermarks
```

No Airflow, as tarefas `transform_raw_to_bronze`,
`transform_bronze_to_silver` e `transform_silver_to_gold` são sequenciais e
recebem o mesmo run e data. Watermarks só são confirmados depois da Gold.

## 31. Consultas de diagnóstico

### Bronze por status

```python
spark.read.format("delta").load("s3a://bronze/orders") \
    .groupBy("_ingestion_date", "_record_status").count().show()
```

### Rejeições Silver

```python
spark.read.format("delta") \
    .load("s3a://rejected/bronze_to_silver/orders") \
    .groupBy("_reject_reason").count().show(truncate=False)
```

### Falhas Gold

```python
spark.read.format("delta") \
    .load("s3a://observability/gold_validation_failures") \
    .select("run_id", "datamart", "validation_type", "validation_error") \
    .show(truncate=False)
```

### Eventos de transformação

```python
spark.read.format("delta") \
    .load("s3a://observability/transformation_log") \
    .filter("run_id = 'manual__2026-09-20'") \
    .orderBy("processing_start_ts").show(truncate=False)
```

## 32. Limitações e cuidados

- ausência de Raw, Bronze ou Silver pode resultar em skip sem evento;
- a Bronze pode manter versões alteradas da mesma data;
- duplicidade Bronze considera a linha inteira, não apenas a chave;
- a Silver não processa exclusões CDC;
- rejected/quarantine Silver não têm retorno automático;
- joins internos Gold podem eliminar linhas e bloquear reconciliações;
- Gold sobrescreve schema e dados completos;
- a publicação Gold não é atômica entre tabelas;
- comparações financeiras Gold são exatas;
- `records_inserted` Bronze não distingue updates;
- uma exceção Silver pode manter cache até o encerramento da sessão.

## 33. Troubleshooting

### Nenhum evento Bronze

Confirme o prefixo Raw, a data `YYYYMMDD`, o bucket e a permissão de listagem.

### `Missing Raw columns`

Compare a origem com `BRONZE_DATASET_SCHEMAS`. Campos opcionais quanto ao valor
ainda precisam existir como coluna.

### Silver não encontrou registros

Confira `_bronze_run_id`, `_ingestion_date` e os argumentos usados nas duas
etapas.

### Filhos rejeitados por FK

Verifique se a tabela pai Silver existe, foi processada antes e contém chaves
compatíveis.

### Datamart ignorado

Procure `Silver table not found` no log e valide todas as fontes exigidas pelo
datamart.

### Datamart rejeitado

Consulte `gold_validation_failures` e o evento `datamart_build` para localizar
problemas de grão, volume, reconciliação ou KPI.

### `GOLD_FRESHNESS_VOLUME_MISMATCH`

Verifique escrita Delta, concorrência, `_gold_run_id` e acesso ao destino.

## 34. Recomendações

- gerar eventos explícitos para skips;
- implementar delete CDC;
- criar reprocessamento para rejeitados e quarentena Silver;
- detalhar razões de invalidade Bronze;
- publicar Gold via staging e troca atômica;
- versionar contratos e datamarts;
- parametrizar tolerâncias e limites;
- medir inserts/updates reais pelos resultados do Delta merge;
- garantir liberação de cache em `finally` por dataset;
- adicionar testes unitários de regras e integração Delta;
- monitorar compactação e retenção das tabelas.

## 35. Resumo operacional

Bronze preserva e classifica; Silver decide, relaciona e consolida; Gold
publica produtos analíticos validados. Para consistência, mantenha o mesmo run
e data entre Bronze e Silver, processe pais antes dos filhos e valide tanto os
objetos Delta quanto `transformation_log` e `gold_validation_failures`.
