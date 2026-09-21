# Catálogo de schemas da plataforma de dados

## 1. Objetivo

A pasta `src/schemas` concentra os contratos estruturais compartilhados pela
plataforma. Esses contratos definem:

- o formato das tabelas de observabilidade;
- o estado incremental da ingestão;
- as colunas e tipos dos treze datasets de negócio na Bronze;
- os campos que não podem ser nulos segundo as regras de qualidade;
- as chaves primárias lógicas usadas em validação, correção e Silver.

O arquivo é uma fonte central de verdade para várias etapas. Uma alteração de
campo ou tipo pode afetar simultaneamente Landing, Bronze, Silver,
observabilidade e correção. Por isso, mudanças devem ser tratadas como evolução
de contrato, não apenas como edição local de uma classe Python.

## 2. Estrutura da pasta

```text
src/schemas/
`-- schemas.py
```

O arquivo `schemas.py` possui quatro grupos de definições:

| Grupo | Objetos |
| --- | --- |
| Observabilidade | `ingestion_log_schema`, `landing_quality_log_schema`, `data_correction_log_schema`, `transformation_log_schema` |
| Controle | `gold_validation_failure_schema`, `ingestion_watermark_schema` |
| Negócio | `BRONZE_DATASET_SCHEMAS` |
| Regras lógicas | `DATASET_REQUIRED_FIELDS`, `DATASET_PRIMARY_KEYS` |

## 3. Tipos Spark usados

| Tipo no código | Representação | Uso típico |
| --- | --- | --- |
| `StringType` | Texto UTF-8 | Identificadores textuais, nomes, códigos e status. |
| `LongType` | Inteiro de 64 bits | IDs e contagens potencialmente grandes. |
| `IntegerType` | Inteiro de 32 bits | Quantidades menores, avaliações e dias. |
| `DoubleType` | Ponto flutuante de 64 bits | Durações e percentuais de observabilidade. |
| `DecimalType(p, s)` | Decimal exato | Valores monetários, taxas e medidas. |
| `BooleanType` | Booleano | Flags como ativo, opt-in e compra verificada. |
| `DateType` | Data sem horário | Datas civis e datas de referência. |
| `TimestampType` | Data e hora | Eventos, criação, atualização e processamento. |

Em `DecimalType(p, s)`, `p` é a quantidade total de dígitos e `s` é a
quantidade de casas decimais. Por exemplo, `decimal(16,2)` aceita até 14
dígitos inteiros e 2 fracionários.

## 4. Nullability do Spark versus obrigatoriedade de negócio

Cada `StructField` recebe um terceiro argumento:

```python
StructField("campo", StringType(), True)
```

Esse booleano é o `nullable` do Spark:

- `False`: o campo é não anulável no schema;
- `True`: o DataFrame aceita valor nulo estruturalmente.

Todos os campos dos schemas de negócio em `BRONZE_DATASET_SCHEMAS` são
declarados `nullable=True`. Isso não significa que todos sejam opcionais.
`DATASET_REQUIRED_FIELDS` define separadamente quais valores são obrigatórios
pelas regras da plataforma.

Essa separação permite carregar e identificar registros inválidos sem fazer a
leitura inteira falhar. Na Bronze, uma coluna obrigatória nula contribui para o
status `_record_status = INVALID`; na Silver, as regras voltam a avaliar esses
campos.

## 5. Como os contratos são consumidos

```text
schemas.py
  |-- schemas operacionais --> writers Delta de observabilidade
  |-- BRONZE_DATASET_SCHEMAS --> qualidade Landing e cast Raw -> Bronze
  |                              `--> padronização Bronze -> Silver
  |-- DATASET_REQUIRED_FIELDS --> qualidade, Bronze e Silver
  `-- DATASET_PRIMARY_KEYS ----> qualidade, correção e Silver
```

### 5.1 Qualidade do Landing

`dq_landing_raw.py` substitui as listas locais de colunas pelos campos do
schema Bronze. Com isso, cada dataset é validado quanto a:

- colunas ausentes;
- colunas inesperadas;
- valores nulos nos campos obrigatórios;
- duplicidades nas chaves primárias.

A validação de schema no Landing compara nomes de colunas. A conversão efetiva
dos tipos ocorre na transformação Raw para Bronze.

### 5.2 Raw para Bronze

`transformation_raw_bronze.py`:

1. exige a presença de todas as colunas do schema;
2. normaliza texto vazio, `null` e `n/a` para nulo;
3. converte cada coluna para o tipo Spark declarado;
4. identifica conversões que resultaram em nulo;
5. avalia campos obrigatórios e duplicidade;
6. adiciona metadados técnicos Bronze.

Colunas extras não são projetadas para a Bronze; o `select` final segue a
ordem definida no `StructType`.

### 5.3 Bronze para Silver

`transform_bronze_silver.py` reutiliza schemas, campos obrigatórios e chaves
para conversões, deduplicação, regras de validade e merges Delta. Regras como
chaves estrangeiras, domínios e limites de outlier ficam no módulo Silver, não
em `schemas.py`.

### 5.4 Correção

`data_correction.py` usa `DATASET_PRIMARY_KEYS` para deduplicar e corrigir
dados em quarentena.

## 6. Schemas operacionais

### 6.1 `ingestion_log_schema`

Destino:

```text
s3a://observability/ingestion_log
```

| Campo | Tipo | Nullable | Finalidade |
| --- | --- | --- | --- |
| `run_id` | string | Não | Identifica a execução. |
| `source_name` | string | Sim | Origem lógica. |
| `source_table` | string | Sim | Arquivo, tabela ou dataset. |
| `source_type` | string | Sim | Tipo da origem. |
| `file_name` | string | Sim | Nome físico associado. |
| `target_path` | string | Sim | Caminho no Landing. |
| `started_at` | timestamp | Sim | Início da extração. |
| `ended_at` | timestamp | Sim | Fim da extração. |
| `duration_seconds` | double | Sim | Duração em segundos. |
| `execution_status` | string | Sim | `SUCCESS` ou `FAILED`. |
| `error_message` | string | Sim | Mensagem de erro. |
| `execution_date` | date | Sim | Data do evento. |

### 6.2 `landing_quality_log_schema`

Destino:

```text
s3a://observability/landing_quality_log
```

| Campo | Tipo | Nullable | Finalidade |
| --- | --- | --- | --- |
| `run_id` | string | Não | Execução de qualidade. |
| `file_name` | string | Sim | Arquivo ou dataset validado. |
| `source_name` | string | Sim | Origem lógica. |
| `file_path` | string | Sim | Caminho avaliado. |
| `check_name` | string | Sim | Nome da verificação. |
| `check_type` | string | Sim | Categoria da verificação. |
| `records_total` | long | Sim | Total avaliado. |
| `records_valid` | long | Sim | Registros válidos. |
| `records_invalid` | long | Sim | Registros inválidos. |
| `invalid_percentage` | double | Sim | Percentual inválido. |
| `check_status` | string | Sim | `PASS` ou `FAIL`. |
| `error_message` | string | Sim | Detalhe da falha. |
| `execution_ts` | timestamp | Sim | Instante da validação. |

### 6.3 `data_correction_log_schema`

Destino:

```text
s3a://observability/data_correction_log
```

| Campo | Tipo | Nullable | Finalidade |
| --- | --- | --- | --- |
| `correction_run_id` | string | Não | Execução da correção. |
| `original_run_id` | string | Sim | Execução que gerou a quarentena. |
| `dataset_name` | string | Sim | Dataset corrigido. |
| `source_name` | string | Sim | Origem original. |
| `file_name` | string | Sim | Arquivo associado. |
| `original_path` | string | Sim | Caminho de entrada. |
| `target_path` | string | Sim | Caminho de saída. |
| `correction_type` | string | Sim | Estratégia aplicada. |
| `records_input` | long | Sim | Registros recebidos. |
| `records_corrected` | long | Sim | Registros corrigidos. |
| `records_still_invalid` | long | Sim | Registros ainda inválidos. |
| `records_discarded` | long | Sim | Registros descartados. |
| `correction_attempt` | integer | Sim | Número da tentativa. |
| `correction_status` | string | Sim | Resultado da correção. |
| `error_message` | string | Sim | Erro associado. |
| `correction_start_ts` | timestamp | Sim | Início da correção. |
| `correction_end_ts` | timestamp | Sim | Fim da correção. |
| `duration_seconds` | double | Sim | Duração. |
| `execution_date` | date | Sim | Data operacional. |

### 6.4 `transformation_log_schema`

Destino:

```text
s3a://observability/transformation_log
```

| Campo | Tipo | Nullable | Finalidade |
| --- | --- | --- | --- |
| `run_id` | string | Não | Execução da transformação. |
| `pipeline_name` | string | Sim | Processo lógico. |
| `stage` | string | Sim | Bronze, Silver ou Gold. |
| `source_table` | string | Sim | Tabela de origem. |
| `target_table` | string | Sim | Tabela de destino. |
| `source_path` | string | Sim | Caminho de leitura. |
| `target_path` | string | Sim | Caminho de escrita. |
| `records_input` | long | Sim | Quantidade recebida. |
| `records_output` | long | Sim | Quantidade produzida. |
| `records_rejected` | long | Sim | Quantidade rejeitada. |
| `records_inserted` | long | Sim | Registros inseridos. |
| `records_updated` | long | Sim | Registros atualizados. |
| `records_deleted` | long | Sim | Registros removidos. |
| `data_quality_status` | string | Sim | Estado de qualidade. |
| `processing_start_ts` | timestamp | Sim | Início do processamento. |
| `processing_end_ts` | timestamp | Sim | Fim do processamento. |
| `duration_seconds` | double | Sim | Duração. |
| `status` | string | Sim | Resultado da etapa. |
| `error_message` | string | Sim | Erro associado. |
| `execution_date` | date | Sim | Data operacional. |

### 6.5 `gold_validation_failure_schema`

Destino:

```text
s3a://observability/gold_validation_failures
```

| Campo | Tipo | Nullable | Finalidade |
| --- | --- | --- | --- |
| `run_id` | string | Não | Execução Gold. |
| `datamart` | string | Não | Datamart validado. |
| `validation_type` | string | Não | Categoria da validação. |
| `validation_error` | string | Não | Descrição do problema. |
| `candidate_tables` | string | Sim | Tabelas candidatas. |
| `validation_status` | string | Não | Valor `FAIL`. |
| `execution_ts` | timestamp | Não | Instante da validação. |
| `execution_date` | date | Não | Data operacional. |

Esse é o contrato operacional mais estrito: somente
`candidate_tables` aceita nulo.

### 6.6 `ingestion_watermark_schema`

Destino:

```text
s3a://observability/ingestion_watermarks
```

| Campo | Tipo | Nullable | Finalidade |
| --- | --- | --- | --- |
| `source_name` | string | Não | Origem incremental. |
| `dataset_name` | string | Não | Dataset controlado. |
| `watermark_ts` | timestamp | Não | Limite superior confirmado. |
| `last_successful_run_id` | string | Não | Pipeline que confirmou o estado. |
| `committed_at` | timestamp | Não | Instante do commit. |

A chave lógica do estado é `source_name + dataset_name`. O código de ingestão
faz `MERGE` usando essa combinação.

## 7. Visão geral dos datasets Bronze

| Dataset | Origem | Colunas | Chave primária |
| --- | --- | ---: | --- |
| `coupons` | Arquivo local | 14 | `coupon` |
| `delivery_tracking` | Arquivo local | 15 | `tracking_id` |
| `payments` | Arquivo local | 16 | `payment_id` |
| `website_events` | Arquivo local | 19 | `event_id` |
| `customer_review` | API | 13 | `review_id` |
| `exchange_rates` | API | 10 | `exchange_rate_id` |
| `marketing_campaigns` | API | 16 | `campaign_id` |
| `customers` | PostgreSQL | 17 | `customer_id` |
| `products` | PostgreSQL | 19 | `product_id` |
| `suppliers` | PostgreSQL | 18 | `supplier_id` |
| `inventory` | MySQL | 13 | `inventory_id` |
| `order_items` | MySQL | 14 | `order_item_id` |
| `orders` | MySQL | 24 | `order_id` |

Nas tabelas seguintes, “Obrigatório” representa a presença em
`DATASET_REQUIRED_FIELDS`, e não o atributo `nullable` do `StructField`.

## 8. Schema `coupons`

Chave primária: `coupon`.

| Campo | Tipo | Obrigatório | Descrição |
| --- | --- | --- | --- |
| `coupon` | string | Sim | Código do cupom. |
| `description` | string | Não | Descrição promocional. |
| `discount` | decimal(14,2) | Sim | Valor ou percentual do desconto. |
| `discount_type` | string | Sim | Modalidade do desconto. |
| `currency` | string | Sim | Moeda aplicável. |
| `minimum_order_amount` | decimal(16,2) | Sim | Pedido mínimo. |
| `maximum_discount_amount` | decimal(16,2) | Não | Teto do desconto. |
| `usage_limit` | integer | Não | Limite global de usos. |
| `usage_limit_per_customer` | integer | Não | Limite por cliente. |
| `is_active` | boolean | Sim | Indicador de atividade. |
| `start_date` | date | Sim | Início da vigência. |
| `end_date` | date | Sim | Fim da vigência. |
| `created_at` | timestamp | Sim | Criação. |
| `updated_at` | timestamp | Sim | Última atualização. |

## 9. Schema `delivery_tracking`

Chave primária: `tracking_id`.

| Campo | Tipo | Obrigatório | Descrição |
| --- | --- | --- | --- |
| `tracking_id` | long | Sim | Identificador do rastreio. |
| `order_id` | long | Sim | Pedido relacionado. |
| `shipment_id` | long | Sim | Identificador da remessa. |
| `tracking_code` | string | Sim | Código de rastreamento. |
| `carrier` | string | Sim | Transportadora. |
| `service_level` | string | Sim | Nível do serviço. |
| `status` | string | Sim | Estado da entrega. |
| `event_description` | string | Não | Descrição do evento. |
| `city` | string | Não | Cidade do evento. |
| `state` | string | Não | Estado ou região. |
| `country_code` | string | Sim | Código do país. |
| `estimated_delivery_at` | timestamp | Não | Previsão de entrega. |
| `occurred_at` | timestamp | Sim | Momento do evento. |
| `created_at` | timestamp | Sim | Criação. |
| `updated_at` | timestamp | Sim | Última atualização. |

## 10. Schema `payments`

Chave primária: `payment_id`.

| Campo | Tipo | Obrigatório | Descrição |
| --- | --- | --- | --- |
| `payment_id` | long | Sim | Identificador do pagamento. |
| `order_id` | long | Sim | Pedido relacionado. |
| `transaction_reference` | string | Sim | Referência no provedor. |
| `payment_method` | string | Sim | Meio de pagamento. |
| `payment_status` | string | Sim | Estado do pagamento. |
| `amount` | decimal(16,2) | Sim | Valor da transação. |
| `currency` | string | Sim | Moeda. |
| `installments` | integer | Sim | Número de parcelas. |
| `provider` | string | Sim | Provedor do pagamento. |
| `authorized_at` | timestamp | Não | Momento da autorização. |
| `paid_at` | timestamp | Não | Momento da liquidação. |
| `refunded_amount` | decimal(16,2) | Sim | Valor estornado. |
| `failure_code` | string | Não | Código de falha. |
| `failure_reason` | string | Não | Motivo da falha. |
| `created_at` | timestamp | Sim | Criação. |
| `updated_at` | timestamp | Sim | Última atualização. |

## 11. Schema `website_events`

Chave primária: `event_id`.

| Campo | Tipo | Obrigatório | Descrição |
| --- | --- | --- | --- |
| `event_id` | long | Sim | Identificador do evento. |
| `customer_id` | long | Não | Cliente reconhecido. |
| `anonymous_id` | string | Não | Visitante anônimo. |
| `session_id` | string | Sim | Sessão de navegação. |
| `event` | string | Sim | Tipo do evento. |
| `page` | string | Sim | Página associada. |
| `timestamp` | timestamp | Sim | Momento do evento. |
| `product_id` | long | Não | Produto relacionado. |
| `order_id` | long | Não | Pedido relacionado. |
| `device_type` | string | Não | Tipo de dispositivo. |
| `browser` | string | Não | Navegador. |
| `operating_system` | string | Não | Sistema operacional. |
| `traffic_source` | string | Não | Origem do tráfego. |
| `utm_source` | string | Não | Origem UTM. |
| `utm_medium` | string | Não | Mídia UTM. |
| `utm_campaign` | string | Não | Campanha UTM. |
| `country_code` | string | Não | Código do país. |
| `created_at` | timestamp | Sim | Criação. |
| `updated_at` | timestamp | Sim | Última atualização. |

## 12. Schema `customer_review`

Chave primária: `review_id`.

| Campo | Tipo | Obrigatório | Descrição |
| --- | --- | --- | --- |
| `review_id` | long | Sim | Identificador da avaliação. |
| `customer_id` | long | Sim | Cliente autor. |
| `product_id` | long | Sim | Produto avaliado. |
| `order_id` | long | Não | Pedido relacionado. |
| `rating` | integer | Sim | Nota. |
| `title` | string | Não | Título. |
| `comment` | string | Não | Comentário. |
| `verified_purchase` | boolean | Sim | Indica compra verificada. |
| `moderation_status` | string | Sim | Estado da moderação. |
| `helpful_votes` | integer | Não | Votos de utilidade. |
| `language` | string | Não | Idioma. |
| `created_at` | timestamp | Sim | Criação. |
| `updated_at` | timestamp | Sim | Última atualização. |

## 13. Schema `exchange_rates`

Chave primária: `exchange_rate_id`.

| Campo | Tipo | Obrigatório | Descrição |
| --- | --- | --- | --- |
| `exchange_rate_id` | long | Sim | Identificador da cotação. |
| `date` | date | Sim | Data de referência. |
| `base_currency` | string | Sim | Moeda-base. |
| `usd_brl` | decimal(12,6) | Sim | Cotação USD/BRL. |
| `eur_brl` | decimal(12,6) | Sim | Cotação EUR/BRL. |
| `provider` | string | Sim | Fonte da cotação. |
| `rate_type` | string | Não | Tipo da taxa. |
| `published_at` | timestamp | Sim | Momento da publicação. |
| `created_at` | timestamp | Sim | Criação. |
| `updated_at` | timestamp | Sim | Última atualização. |

## 14. Schema `marketing_campaigns`

Chave primária: `campaign_id`.

| Campo | Tipo | Obrigatório | Descrição |
| --- | --- | --- | --- |
| `campaign_id` | long | Sim | Identificador da campanha. |
| `campaign` | string | Sim | Nome da campanha. |
| `channel` | string | Sim | Canal de mídia. |
| `objective` | string | Não | Objetivo. |
| `status` | string | Sim | Estado da campanha. |
| `budget` | decimal(16,2) | Sim | Orçamento. |
| `actual_spend` | decimal(16,2) | Não | Gasto realizado. |
| `currency` | string | Sim | Moeda. |
| `start_date` | date | Sim | Início. |
| `end_date` | date | Sim | Término. |
| `target_audience` | string | Não | Público-alvo. |
| `utm_source` | string | Não | Origem UTM. |
| `utm_medium` | string | Não | Mídia UTM. |
| `utm_campaign` | string | Não | Campanha UTM. |
| `created_at` | timestamp | Sim | Criação. |
| `updated_at` | timestamp | Sim | Última atualização. |

## 15. Schema `customers`

Chave primária: `customer_id`.

| Campo | Tipo | Obrigatório | Descrição |
| --- | --- | --- | --- |
| `customer_id` | long | Sim | Identificador do cliente. |
| `name` | string | Sim | Nome. |
| `email` | string | Sim | E-mail. |
| `phone` | string | Não | Telefone. |
| `customer_type` | string | Sim | Tipo de cliente. |
| `tax_id` | string | Não | Documento fiscal. |
| `birth_date` | date | Não | Data de nascimento. |
| `address_line` | string | Não | Endereço. |
| `postal_code` | string | Não | CEP ou código postal. |
| `city` | string | Não | Cidade. |
| `state` | string | Não | Estado ou região. |
| `country_code` | string | Sim | Código do país. |
| `acquisition_channel` | string | Não | Canal de aquisição. |
| `marketing_opt_in` | boolean | Sim | Consentimento de marketing. |
| `status` | string | Sim | Estado cadastral. |
| `created_at` | timestamp | Sim | Criação. |
| `updated_at` | timestamp | Sim | Última atualização. |

## 16. Schema `products`

Chave primária: `product_id`.

| Campo | Tipo | Obrigatório | Descrição |
| --- | --- | --- | --- |
| `product_id` | long | Sim | Identificador do produto. |
| `sku` | string | Sim | SKU comercial. |
| `barcode` | string | Não | Código de barras. |
| `name` | string | Sim | Nome. |
| `description` | string | Não | Descrição. |
| `category` | string | Sim | Categoria. |
| `subcategory` | string | Não | Subcategoria. |
| `brand` | string | Não | Marca. |
| `supplier_id` | long | Sim | Fornecedor relacionado. |
| `price` | decimal(14,2) | Sim | Preço de venda. |
| `cost_price` | decimal(14,2) | Não | Custo. |
| `currency` | string | Sim | Moeda. |
| `weight_kg` | decimal(10,3) | Não | Peso em quilogramas. |
| `height_cm` | decimal(10,2) | Não | Altura em centímetros. |
| `width_cm` | decimal(10,2) | Não | Largura em centímetros. |
| `length_cm` | decimal(10,2) | Não | Comprimento em centímetros. |
| `is_active` | boolean | Sim | Indicador de atividade. |
| `created_at` | timestamp | Sim | Criação. |
| `updated_at` | timestamp | Sim | Última atualização. |

## 17. Schema `suppliers`

Chave primária: `supplier_id`.

| Campo | Tipo | Obrigatório | Descrição |
| --- | --- | --- | --- |
| `supplier_id` | long | Sim | Identificador do fornecedor. |
| `supplier_code` | string | Sim | Código do fornecedor. |
| `supplier_name` | string | Sim | Nome de exibição. |
| `legal_name` | string | Não | Razão social. |
| `tax_id` | string | Não | Documento fiscal. |
| `contact_name` | string | Não | Contato principal. |
| `email` | string | Não | E-mail. |
| `phone` | string | Não | Telefone. |
| `address_line` | string | Não | Endereço. |
| `postal_code` | string | Não | CEP ou código postal. |
| `city` | string | Não | Cidade. |
| `state` | string | Não | Estado ou região. |
| `country_code` | string | Sim | Código do país. |
| `payment_terms_days` | integer | Sim | Prazo de pagamento em dias. |
| `lead_time_days` | integer | Não | Prazo de fornecimento. |
| `is_active` | boolean | Sim | Indicador de atividade. |
| `created_at` | timestamp | Sim | Criação. |
| `updated_at` | timestamp | Sim | Última atualização. |

## 18. Schema `inventory`

Chave primária: `inventory_id`.

| Campo | Tipo | Obrigatório | Descrição |
| --- | --- | --- | --- |
| `inventory_id` | long | Sim | Identificador do estoque. |
| `product_id` | long | Sim | Produto relacionado. |
| `warehouse_code` | string | Sim | Código do armazém. |
| `bin_location` | string | Não | Posição física. |
| `quantity_available` | integer | Sim | Quantidade disponível. |
| `quantity_reserved` | integer | Sim | Quantidade reservada. |
| `quantity_damaged` | integer | Sim | Quantidade danificada. |
| `reorder_point` | integer | Sim | Ponto de reposição. |
| `safety_stock` | integer | Sim | Estoque de segurança. |
| `last_restocked_at` | timestamp | Não | Última reposição. |
| `last_counted_at` | timestamp | Não | Último inventário. |
| `created_at` | timestamp | Sim | Criação. |
| `updated_at` | timestamp | Sim | Última atualização. |

## 19. Schema `order_items`

Chave primária: `order_item_id`.

| Campo | Tipo | Obrigatório | Descrição |
| --- | --- | --- | --- |
| `order_item_id` | long | Sim | Identificador do item. |
| `order_id` | long | Sim | Pedido relacionado. |
| `line_number` | integer | Sim | Número da linha. |
| `product_id` | long | Sim | Produto relacionado. |
| `sku` | string | Sim | SKU registrado no pedido. |
| `product_name` | string | Sim | Nome registrado no pedido. |
| `quantity` | integer | Sim | Quantidade. |
| `unit_price` | decimal(14,2) | Sim | Preço unitário. |
| `discount_amount` | decimal(16,2) | Sim | Desconto da linha. |
| `tax_amount` | decimal(16,2) | Sim | Tributo da linha. |
| `line_total` | decimal(16,2) | Sim | Total da linha. |
| `currency` | string | Sim | Moeda. |
| `created_at` | timestamp | Sim | Criação. |
| `updated_at` | timestamp | Sim | Última atualização. |

## 20. Schema `orders`

Chave primária: `order_id`.

| Campo | Tipo | Obrigatório | Descrição |
| --- | --- | --- | --- |
| `order_id` | long | Sim | Identificador do pedido. |
| `order_number` | string | Sim | Número comercial. |
| `customer_id` | long | Sim | Cliente relacionado. |
| `order_date` | timestamp | Sim | Momento do pedido. |
| `status` | string | Sim | Estado do pedido. |
| `sales_channel` | string | Sim | Canal de venda. |
| `currency` | string | Sim | Moeda. |
| `subtotal_amount` | decimal(16,2) | Sim | Subtotal. |
| `discount_amount` | decimal(16,2) | Sim | Desconto. |
| `shipping_amount` | decimal(16,2) | Sim | Frete. |
| `tax_amount` | decimal(16,2) | Sim | Tributos. |
| `total_amount` | decimal(16,2) | Sim | Total. |
| `coupon_code` | string | Não | Cupom aplicado. |
| `shipping_recipient` | string | Sim | Destinatário. |
| `shipping_address_line` | string | Sim | Endereço de entrega. |
| `shipping_postal_code` | string | Sim | CEP ou código postal. |
| `shipping_city` | string | Sim | Cidade. |
| `shipping_state` | string | Sim | Estado ou região. |
| `shipping_country_code` | string | Sim | Código do país. |
| `delivered_at` | timestamp | Não | Momento da entrega. |
| `cancelled_at` | timestamp | Não | Momento do cancelamento. |
| `cancellation_reason` | string | Não | Motivo do cancelamento. |
| `created_at` | timestamp | Sim | Criação. |
| `updated_at` | timestamp | Sim | Última atualização. |

## 21. Catálogo consolidado de campos obrigatórios

| Dataset | Campos obrigatórios |
| --- | --- |
| `coupons` | `coupon`, `discount`, `discount_type`, `currency`, `minimum_order_amount`, `is_active`, `start_date`, `end_date`, `created_at`, `updated_at` |
| `delivery_tracking` | `tracking_id`, `order_id`, `shipment_id`, `tracking_code`, `carrier`, `service_level`, `status`, `country_code`, `occurred_at`, `created_at`, `updated_at` |
| `payments` | `payment_id`, `order_id`, `transaction_reference`, `payment_method`, `payment_status`, `amount`, `currency`, `installments`, `provider`, `refunded_amount`, `created_at`, `updated_at` |
| `website_events` | `event_id`, `session_id`, `event`, `page`, `timestamp`, `created_at`, `updated_at` |
| `customer_review` | `review_id`, `customer_id`, `product_id`, `rating`, `verified_purchase`, `moderation_status`, `created_at`, `updated_at` |
| `exchange_rates` | `exchange_rate_id`, `date`, `base_currency`, `usd_brl`, `eur_brl`, `provider`, `published_at`, `created_at`, `updated_at` |
| `marketing_campaigns` | `campaign_id`, `campaign`, `channel`, `status`, `budget`, `currency`, `start_date`, `end_date`, `created_at`, `updated_at` |
| `customers` | `customer_id`, `name`, `email`, `customer_type`, `country_code`, `marketing_opt_in`, `status`, `created_at`, `updated_at` |
| `products` | `product_id`, `sku`, `name`, `category`, `supplier_id`, `price`, `currency`, `is_active`, `created_at`, `updated_at` |
| `suppliers` | `supplier_id`, `supplier_code`, `supplier_name`, `country_code`, `payment_terms_days`, `is_active`, `created_at`, `updated_at` |
| `inventory` | `inventory_id`, `product_id`, `warehouse_code`, `quantity_available`, `quantity_reserved`, `quantity_damaged`, `reorder_point`, `safety_stock`, `created_at`, `updated_at` |
| `order_items` | `order_item_id`, `order_id`, `line_number`, `product_id`, `sku`, `product_name`, `quantity`, `unit_price`, `discount_amount`, `tax_amount`, `line_total`, `currency`, `created_at`, `updated_at` |
| `orders` | `order_id`, `order_number`, `customer_id`, `order_date`, `status`, `sales_channel`, `currency`, `subtotal_amount`, `discount_amount`, `shipping_amount`, `tax_amount`, `total_amount`, `shipping_recipient`, `shipping_address_line`, `shipping_postal_code`, `shipping_city`, `shipping_state`, `shipping_country_code`, `created_at`, `updated_at` |

## 22. Catálogo de chaves primárias

| Dataset | Chave lógica |
| --- | --- |
| `coupons` | `coupon` |
| `delivery_tracking` | `tracking_id` |
| `payments` | `payment_id` |
| `website_events` | `event_id` |
| `customer_review` | `review_id` |
| `exchange_rates` | `exchange_rate_id` |
| `marketing_campaigns` | `campaign_id` |
| `customers` | `customer_id` |
| `products` | `product_id` |
| `suppliers` | `supplier_id` |
| `inventory` | `inventory_id` |
| `order_items` | `order_item_id` |
| `orders` | `order_id` |

Atualmente todas as chaves são simples, embora o mapa aceite listas e possa
representar chaves compostas no futuro.

## 23. Relacionamentos lógicos entre datasets

As chaves estrangeiras não são declaradas em `schemas.py`, mas os nomes e
tipos permitem que a camada Silver valide os relacionamentos abaixo:

```text
suppliers.supplier_id
  `-- products.supplier_id
       `-- inventory.product_id
       `-- order_items.product_id
       `-- website_events.product_id
       `-- customer_review.product_id

customers.customer_id
  `-- orders.customer_id
  `-- website_events.customer_id
  `-- customer_review.customer_id

orders.order_id
  `-- order_items.order_id
  `-- delivery_tracking.order_id
  `-- payments.order_id
  `-- website_events.order_id
  `-- customer_review.order_id

coupons.coupon
  `-- orders.coupon_code
```

Os relacionamentos de `website_events` e `customer_review.order_id` são
opcionais porque esses campos não pertencem às respectivas listas de campos
obrigatórios.

## 24. Metadados adicionados na Bronze

Os metadados abaixo não aparecem em `BRONZE_DATASET_SCHEMAS`, pois são criados
durante a transformação:

| Campo | Finalidade |
| --- | --- |
| `_source_file` | Arquivo físico de origem lido pelo Spark. |
| `_record_status` | `VALID` ou `INVALID`. |
| `_bronze_run_id` | Execução responsável pelo registro. |
| `_source_name` | Origem lógica do dataset. |
| `_source_path` | Caminho Raw. |
| `_ingestion_date` | Data operacional. |
| `_bronze_processed_at` | Timestamp do processamento. |
| `_record_hash` | SHA-256 das colunas de negócio. |
| `_record_occurrence` | Ocorrência do mesmo hash. |

Eles não fazem parte do contrato de entrada. Ao adicionar um campo de negócio,
ele deve entrar no `StructType`; ao adicionar um metadado técnico, deve-se
avaliar se pertence ao processo de transformação.

## 25. Conversão de tipos na Bronze

Para cada campo, o pipeline:

```text
valor original
  -> cast para string
  -> trim
  -> "", "null" e "n/a" viram null
  -> cast para o tipo do schema
```

Uma conversão é considerada inválida quando o valor normalizado não é nulo,
mas o resultado do cast é nulo. O registro recebe `_record_status = INVALID`.

Consequências importantes:

- o schema não valida domínios de status;
- o schema não valida faixas numéricas;
- o schema não valida chaves estrangeiras;
- precisão decimal excessiva pode sofrer conversão incompatível;
- strings de data e timestamp precisam ser reconhecidas pelo Spark;
- valores sentinela são normalizados antes do cast.

Essas regras adicionais pertencem principalmente à Silver.

## 26. Como consultar os schemas em código

### Listar datasets

```python
from schemas.schemas import BRONZE_DATASET_SCHEMAS

print(sorted(BRONZE_DATASET_SCHEMAS))
```

### Inspecionar campos e tipos

```python
schema = BRONZE_DATASET_SCHEMAS["orders"]
for field in schema.fields:
    print(field.name, field.dataType.simpleString(), field.nullable)
```

### Obter obrigatórios e chaves

```python
from schemas.schemas import DATASET_PRIMARY_KEYS, DATASET_REQUIRED_FIELDS

required = DATASET_REQUIRED_FIELDS["orders"]
primary_key = DATASET_PRIMARY_KEYS["orders"]
```

### Exibir a representação Spark

```python
print(BRONZE_DATASET_SCHEMAS["products"].simpleString())
```

Para executar a partir da raiz do projeto no PowerShell:

```powershell
$env:PYTHONPATH = "$PWD\src;$PWD"
python -c "from schemas.schemas import BRONZE_DATASET_SCHEMAS; print(BRONZE_DATASET_SCHEMAS['orders'].simpleString())"
```

## 27. Como adicionar um novo dataset

Uma inclusão completa normalmente exige:

1. adicionar o `StructType` em `BRONZE_DATASET_SCHEMAS`;
2. adicionar todos os campos de negócio na ordem desejada;
3. cadastrar os campos obrigatórios em `DATASET_REQUIRED_FIELDS`;
4. cadastrar a chave em `DATASET_PRIMARY_KEYS`;
5. configurar origem, formato e arquivo em `LANDING_DATASETS`;
6. incluir o dataset na ingestão correspondente;
7. revisar correção e transformação Bronze;
8. incluir o dataset na ordem da Silver;
9. definir chaves estrangeiras, normalizações, domínios e limites Silver;
10. revisar datamarts Gold afetados;
11. atualizar geração de dados e API, quando aplicável;
12. criar testes de contrato e atualizar a documentação.

O nome usado nos três mapas precisa ser idêntico. Caso contrário, o import de
qualidade ou transformação pode gerar `KeyError` em runtime.

## 28. Como alterar uma coluna

### 28.1 Adição compatível

Mesmo um novo campo opcional precisa existir na origem, porque o fluxo atual
de qualidade exige todas as colunas presentes no schema, independentemente de
serem obrigatórias quanto ao valor.

Passos recomendados:

1. disponibilizar a coluna nas fontes;
2. atualizar geradores e contratos de API/bancos;
3. alterar `BRONZE_DATASET_SCHEMAS`;
4. adicionar à lista de obrigatórios somente se não puder ser nula;
5. planejar evolução das tabelas Delta existentes;
6. validar reprocessamento e consumidores downstream.

### 28.2 Alteração de tipo

Trocar tipo pode invalidar dados históricos ou ser incompatível com o schema
Delta já persistido. Antes da mudança:

- analise valores existentes;
- teste o cast com dados reais;
- avalie precisão e escala de decimais;
- planeje migração ou recriação controlada da tabela;
- revise joins e cálculos da Silver/Gold.

### 28.3 Renomeação ou remoção

São mudanças incompatíveis para o fluxo atual. Qualidade marcará schema
incorreto e a transformação Bronze exigirá o novo nome. Adote uma janela de
migração ou versão de contrato quando produtores e consumidores não puderem
mudar juntos.

## 29. Validações recomendadas do catálogo

O código atual depende de invariantes que podem ser testadas automaticamente:

- todos os datasets aparecem nos três mapas;
- toda chave primária existe no respectivo schema;
- todo campo obrigatório existe no respectivo schema;
- chaves primárias também são obrigatórias;
- não existem nomes de coluna repetidos;
- tipos das chaves estrangeiras são compatíveis;
- schemas operacionais preservam os campos esperados pelos produtores;
- precisão decimal suporta os valores gerados;
- todos os datasets incrementais possuem `updated_at`.

Exemplo de verificação básica:

```python
from schemas.schemas import (
    BRONZE_DATASET_SCHEMAS,
    DATASET_PRIMARY_KEYS,
    DATASET_REQUIRED_FIELDS,
)

assert set(BRONZE_DATASET_SCHEMAS) == set(DATASET_PRIMARY_KEYS)
assert set(BRONZE_DATASET_SCHEMAS) == set(DATASET_REQUIRED_FIELDS)

for dataset, schema in BRONZE_DATASET_SCHEMAS.items():
    names = [field.name for field in schema.fields]
    assert len(names) == len(set(names))
    assert set(DATASET_PRIMARY_KEYS[dataset]) <= set(names)
    assert set(DATASET_REQUIRED_FIELDS[dataset]) <= set(names)
    assert set(DATASET_PRIMARY_KEYS[dataset]) <= set(DATASET_REQUIRED_FIELDS[dataset])
```

## 30. Limitações e cuidados

### 30.1 Não existe versionamento explícito

Os contratos não possuem número de versão. O código e os dados persistidos
precisam evoluir coordenadamente.

### 30.2 Descrições não ficam no schema

O `StructType` armazena nome, tipo e nullability, mas não define metadados com
descrições, classificação ou proprietário do campo.

### 30.3 Chaves são lógicas

Spark e Delta não impõem `PRIMARY KEY` por esses mapas. A unicidade depende das
validações e merges implementados pelo pipeline.

### 30.4 Obrigatório não significa coluna opcional ausente

Um campo fora de `DATASET_REQUIRED_FIELDS` pode ter valor nulo, mas sua coluna
ainda é esperada porque a qualidade usa todas as colunas do schema como
`required_columns`.

### 30.5 Não há schema Silver separado

A Silver reutiliza o contrato Bronze e acrescenta regras no código. Mudanças
podem ter efeito indireto nas duas camadas.

### 30.6 Ordem das colunas é significativa no resultado

A Bronze projeta campos na ordem do `StructType`. Consumidores devem preferir
nomes a posições, mas alterações de ordem ainda podem afetar exportações e
ferramentas externas.

### 30.7 Datas e timestamps não declaram timezone

`TimestampType` não carrega timezone por campo. A interpretação depende da
configuração da sessão Spark e da normalização feita pelos produtores.

### 30.8 Decimais exigem planejamento

Valores que excedem precisão e escala podem virar nulos no cast ou perder a
representação esperada. Dinheiro usa majoritariamente duas casas; câmbio usa
seis e peso usa três.

## 31. Solução de problemas

### 31.1 `Missing Raw columns`

A origem não contém uma ou mais colunas do `StructType`. Compare os nomes
exatamente, incluindo singular/plural e underscore. Mesmo campos opcionais
quanto ao valor precisam estar presentes como coluna.

### 31.2 `Unexpected columns` na qualidade

A origem adicionou campos que ainda não existem no contrato. Decida se devem
ser incorporados oficialmente ou removidos pelo produtor.

### 31.3 Muitos registros `INVALID` na Bronze

Verifique:

- casts que produziram nulo;
- campos de `DATASET_REQUIRED_FIELDS` nulos;
- registros duplicados;
- formatos de data e timestamp;
- separador decimal e precisão.

### 31.4 `KeyError` pelo nome do dataset

Confirme que o mesmo nome existe em `BRONZE_DATASET_SCHEMAS`,
`DATASET_REQUIRED_FIELDS`, `DATASET_PRIMARY_KEYS` e `LANDING_DATASETS`.

### 31.5 Erro ao criar log de observabilidade

Compare o evento com o schema operacional correspondente. Campos marcados
`nullable=False` não podem ser omitidos ou enviados como `None`.

### 31.6 Incompatibilidade com tabela Delta existente

Uma tabela criada com schema anterior pode rejeitar a gravação. Inspecione o
schema persistido, compare com o atual e aplique uma migração controlada. Não
apague dados históricos sem aprovação e plano de recuperação.

### 31.7 Chave estrangeira rejeitada na Silver

Confirme que os campos relacionados têm o mesmo tipo, que a dimensão pai já
foi processada e que os valores realmente existem. O `StructType` garante tipo,
mas não integridade referencial.

## 32. Recomendações para evolução

- adicionar testes automáticos das invariantes entre os três mapas;
- versionar contratos de datasets;
- registrar descrições e classificação de dados como metadados;
- separar explicitamente schemas Bronze e Silver quando divergirem;
- definir uma política de evolução compatível para Delta Lake;
- documentar timezone esperado para timestamps;
- validar precisão decimal contra volumes e valores reais;
- gerar documentação técnica automaticamente a partir dos `StructType`;
- criar contratos de entrada independentes para produtores;
- monitorar schema drift antes do roteamento ao Raw;
- definir responsáveis e SLA para mudanças de cada dataset.

## 33. Checklist para revisão de schema

Antes de aprovar uma mudança:

- [ ] o produtor entrega a coluna no nome e tipo esperados;
- [ ] o campo foi classificado como obrigatório ou opcional;
- [ ] a chave primária continua válida;
- [ ] relacionamentos downstream foram revisados;
- [ ] casts Bronze foram testados com valores reais;
- [ ] regras Silver e Gold foram avaliadas;
- [ ] tabelas Delta existentes têm plano de migração;
- [ ] geradores, API e ingestão foram atualizados;
- [ ] testes de qualidade cobrem a alteração;
- [ ] documentação foi atualizada.

## 34. Resumo

`src/schemas/schemas.py` é o contrato estrutural central da plataforma. Os
`StructType` definem nomes e tipos; `DATASET_REQUIRED_FIELDS` define nulidade de
negócio; `DATASET_PRIMARY_KEYS` define identidade lógica. Esses três elementos
atuam em conjunto e devem permanecer consistentes.

Para interpretar corretamente o catálogo:

- `nullable=True` permite materializar e diagnosticar dados ruins;
- “opcional” permite valor nulo, mas não ausência da coluna;
- chaves não são constraints físicas do Spark;
- validações de domínio e relacionamento ficam fora do arquivo de schemas;
- qualquer mudança deve considerar todas as camadas e dados Delta existentes.
