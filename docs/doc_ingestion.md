# Processo de ingestão de dados

## 1. Objetivo

A pasta `src/ingestion` implementa a entrada de dados da plataforma. Sua
responsabilidade é extrair dados de quatro tipos de origem, aplicar uma janela
incremental baseada em `updated_at` e gravar o resultado no bucket `landing`
do S3 ou MinIO.

As origens atendidas são:

- arquivos CSV e JSON do diretório local;
- tabelas PostgreSQL;
- tabelas MySQL;
- endpoints HTTP da API Data Platform.

Além da extração, o módulo:

- registra o resultado de cada dataset em uma tabela Delta de observabilidade;
- calcula candidatos a watermark sem confirmá-los prematuramente;
- persiste os watermarks somente após a conclusão bem-sucedida do pipeline;
- padroniza os caminhos de saída por dataset e data de ingestão;
- permite execução isolada, pelo pipeline monolítico ou pelo Airflow.

Este documento descreve o comportamento existente no código atual. Ele não
substitui a documentação específica da API, da qualidade, da correção ou da
orquestração.

## 2. Estrutura da pasta

```text
src/ingestion/
|-- ingestion_local.py
|-- ingestion_postgres.py
|-- ingestion_mysql.py
|-- ingestion_api.py
|-- incremental_state.py
`-- commit_incremental_state.py
```

| Arquivo | Responsabilidade |
| --- | --- |
| `ingestion_local.py` | Descobre arquivos locais, filtra registros pela janela incremental e envia CSV/JSON ao Landing. |
| `ingestion_postgres.py` | Extrai tabelas PostgreSQL por JDBC/Spark e grava Parquet no Landing. |
| `ingestion_mysql.py` | Extrai tabelas MySQL por JDBC/Spark e grava Parquet no Landing. |
| `ingestion_api.py` | Consulta endpoints HTTP incrementalmente e transmite a resposta JSON ao Landing. |
| `incremental_state.py` | Lê watermarks confirmados, calcula janelas, anexa candidatos e realiza o commit Delta. |
| `commit_incremental_state.py` | Ponto de entrada usado pelo Airflow para confirmar todos os watermarks ao fim do fluxo. |

## 3. Posição no pipeline

O fluxo funcional é:

```text
Origens
  |-- arquivos locais
  |-- PostgreSQL
  |-- MySQL
  `-- API HTTP
          |
          v
      Ingestão
          |
          v
  s3://landing/<dataset>/ingestion_date_YYYYMMDD/
          |
          v
  Qualidade Landing -> Raw ou Quarantine
          |
          v
  Correção -> Bronze -> Silver -> Gold
          |
          v
  Commit dos watermarks
```

O bucket `landing` é uma área transitória. A etapa seguinte, implementada em
`src/data_quality/dq_landing_raw.py`, valida cada dataset e o encaminha para
`raw` ou `quarantine`.

## 4. Inventário de datasets

Ao todo, a ingestão controla 13 datasets.

| Origem lógica | Dataset | Fonte física | Formato no Landing | Campo incremental |
| --- | --- | --- | --- | --- |
| `local` | `coupons` | `local_data_source/coupons.csv` | CSV | `updated_at` |
| `local` | `delivery_tracking` | `local_data_source/delivery_tracking.csv` | CSV | `updated_at` |
| `local` | `payments` | `local_data_source/payments.csv` | CSV | `updated_at` |
| `local` | `website_events` | `local_data_source/website_events.json` | JSON | `updated_at` |
| `postgres` | `customers` | `public.customers` | Parquet | `updated_at` |
| `postgres` | `products` | `public.products` | Parquet | `updated_at` |
| `postgres` | `suppliers` | `public.suppliers` | Parquet | `updated_at` |
| `mysql` | `inventory` | `inventory` | Parquet | `updated_at` |
| `mysql` | `order_items` | `order_items` | Parquet | `updated_at` |
| `mysql` | `orders` | `orders` | Parquet | `updated_at` |
| `api` | `customer_review` | `/customer-reviews` | JSON | Parâmetros `updated_at_from` e `updated_at_until` |
| `api` | `exchange_rates` | `/exchange-rates` | JSON | Parâmetros `updated_at_from` e `updated_at_until` |
| `api` | `marketing_campaigns` | `/marketing-campaigns` | JSON | Parâmetros `updated_at_from` e `updated_at_until` |

Os nomes da origem e do dataset formam a identidade do watermark. Por
exemplo, `mysql.orders` e `postgres.customers` possuem estados independentes.

## 5. Convenção de armazenamento no Landing

Todos os destinos usam a data informada em `--date`, sem hífens:

```text
s3://landing/<dataset>/ingestion_date_YYYYMMDD/
```

Exemplo para uma execução com `--date 2026-09-20`:

```text
s3://landing/orders/ingestion_date_20260920/
```

### 5.1 Arquivos locais

O nome original é preservado:

```text
s3://landing/payments/ingestion_date_20260920/payments.csv
s3://landing/website_events/ingestion_date_20260920/website_events.json
```

### 5.2 API

Cada endpoint produz um único objeto JSON:

```text
s3://landing/customer_review/ingestion_date_20260920/customer_review.json
```

### 5.3 Bancos relacionais

O Spark grava um diretório Parquet, normalmente com arquivos `part-*` e
metadados de conclusão:

```text
s3a://landing/customers/ingestion_date_20260920/
|-- part-00000-....snappy.parquet
|-- part-00001-....snappy.parquet
`-- _SUCCESS
```

Nos logs, o caminho é apresentado com `s3://`; para leitura e escrita pelo
Spark, o código usa `s3a://`.

### 5.4 Comportamento em reexecuções

- PostgreSQL e MySQL escrevem com `mode("overwrite")`, substituindo o
  diretório da mesma tabela e data.
- Arquivos locais e API usam uma chave determinística; um novo upload na mesma
  data substitui o objeto correspondente.
- A data do caminho é uma partição operacional e não determina a janela de
  dados. A janela é controlada pelo watermark.

## 6. Ingestão incremental

### 6.1 Janela semiaberta

Cada extração trabalha com a janela:

```text
lower_bound <= updated_at < upper_bound
```

O limite inferior é inclusivo e o superior é exclusivo. Esse padrão evita que
um mesmo instante pertença a duas janelas consecutivas.

### 6.2 Cálculo do limite inferior

O módulo consulta a tabela Delta:

```text
s3a://observability/ingestion_watermarks
```

Se ainda não existir watermark para o dataset, o limite inicial vem de
`INCREMENTAL_INITIAL_WATERMARK`, cujo padrão é:

```text
1970-01-01T00:00:00Z
```

Se existir watermark confirmado, o código retrocede o intervalo configurado
em `INCREMENTAL_OVERLAP_MINUTES`:

```text
lower_bound = max(initial_watermark, committed_watermark - overlap)
```

O overlap padrão é de 10 minutos. Ele relê registros recentes para reduzir o
risco de perda por atrasos de atualização ou diferenças de relógio. Como isso
pode gerar duplicidade entre execuções, as camadas seguintes devem aplicar as
chaves e regras de consolidação previstas no pipeline.

Valores negativos de overlap são rejeitados.

### 6.3 Cálculo do limite superior

A precedência é:

1. valor recebido no argumento interno `watermark_upper_bound`;
2. variável `PIPELINE_WATERMARK_UNTIL`;
3. horário UTC obtido no início da chamada.

O pipeline monolítico calcula um único limite e o compartilha entre todas as
origens. No Airflow, a variável é preenchida com o fim do intervalo lógico:

```text
{{ data_interval_end.isoformat() }}
```

Isso garante que todos os datasets de uma execução usem o mesmo corte.

### 6.4 Candidato versus watermark confirmado

Uma ingestão bem-sucedida recebe internamente `_watermark_candidate` com:

- `source_name`;
- `dataset_name`;
- `watermark_ts`, igual ao limite superior da janela.

Esse campo não é persistido no log público de ingestão. Ele só serve para o
controle do pipeline.

O watermark não é confirmado ao final de cada extração. O commit ocorre
somente depois de qualidade, correção e todas as transformações concluírem sem
falha. Desse modo, uma falha posterior permite que a próxima execução releia a
mesma janela.

### 6.5 Commit Delta

`commit_watermarks` seleciona apenas eventos `SUCCESS`, mantém o maior
candidato por origem/dataset e grava:

| Campo | Significado |
| --- | --- |
| `source_name` | Origem lógica, como `api` ou `mysql`. |
| `dataset_name` | Nome do dataset. |
| `watermark_ts` | Limite superior confirmado. |
| `last_successful_run_id` | Execução responsável pela confirmação. |
| `committed_at` | Horário UTC do commit. |

Na primeira execução, a tabela Delta é criada com overwrite. Nas seguintes, é
feito um `MERGE` pela identidade `source_name + dataset_name`. Um registro só
é atualizado se o novo watermark for maior ou igual ao existente.

## 7. Ingestão de arquivos locais

### 7.1 Descoberta

`discover_local_files` lê apenas o primeiro nível do diretório configurado. A
busca não é recursiva e aceita extensões `.csv` e `.json`, sem diferenciar
maiúsculas e minúsculas. Os arquivos são ordenados pelo nome antes do
processamento.

O diretório padrão é `local_data_source`, definido em
`PATH_LOCAL_FILES`. Ele pode ser alterado com `LOCAL_DATA_PATH`.

Se o caminho não existir ou não for um diretório, a função lança erro. Se não
houver arquivos suportados, registra um aviso e retorna uma lista vazia.

### 7.2 Filtragem incremental

Para JSON, o arquivo inteiro é carregado, sendo aceito tanto um array quanto
um objeto único. Para CSV, o código usa `csv.DictReader`. Em ambos os casos:

- cada registro precisa conter `updated_at`;
- o valor precisa ser compatível com ISO 8601;
- o sufixo `Z` é interpretado como UTC;
- timestamps sem fuso são tratados como UTC;
- apenas registros dentro da janela são serializados.

O CSV mantém o cabeçalho mesmo quando a janela não seleciona nenhuma linha. O
JSON vazio é enviado como `[]`.

### 7.3 Upload

O conteúdo filtrado é mantido em memória e enviado com `upload_fileobj`. O
upload usa multipart a partir de 16 MiB, blocos de 16 MiB e até quatro threads.

Embora `upload_local_file` permita enviar o arquivo original quando não recebe
limites, o fluxo principal sempre fornece a janela incremental.

### 7.4 Tratamento de falhas

Cada arquivo é protegido por `try/except`. Uma falha produz evento `FAILED` e
o processamento segue para o próximo arquivo. Na execução por linha de
comando, qualquer evento com falha provoca código de saída não zero ao final.

## 8. Ingestão PostgreSQL

### 8.1 Tabelas permitidas

| Tabela | Coluna usada para particionamento JDBC |
| --- | --- |
| `public.customers` | `customer_id` |
| `public.products` | `product_id` |
| `public.suppliers` | `supplier_id` |

Qualquer tabela fora dessa lista é rejeitada por `read_postgres_table`.

### 8.2 Conexão

A URL construída é:

```text
jdbc:postgresql://<PG_HOST>:<PG_PORT>/<PG_DB>
```

O driver é `org.postgresql.Driver` e o fetch size é 10.000 registros. Todas as
cinco variáveis PostgreSQL são obrigatórias; a ausência de qualquer uma
interrompe a função antes do loop das tabelas.

### 8.3 Consulta incremental

O Spark recebe uma subconsulta equivalente a:

```sql
SELECT *
FROM public.<tabela>
WHERE updated_at >= TIMESTAMPTZ '<lower_bound>+00:00'
  AND updated_at <  TIMESTAMPTZ '<upper_bound>+00:00'
```

Antes da extração, outra consulta calcula `MIN`, `MAX` e `COUNT` da chave de
particionamento. O paralelismo efetivo é:

```text
min(num_partitions, max(1, ceil(record_count / 1.000.000)))
```

O padrão solicitado é de oito partições, mas datasets menores normalmente
usam menos partições. Se a consulta não retornar chave mínima, o DataFrame é
lido sem opções de particionamento.

`num_partitions` menor ou igual a zero é inválido.

## 9. Ingestão MySQL

### 9.1 Tabelas permitidas

| Tabela | Coluna usada para particionamento JDBC |
| --- | --- |
| `inventory` | `inventory_id` |
| `order_items` | `order_item_id` |
| `orders` | `order_id` |

### 9.2 Conexão

A URL construída é:

```text
jdbc:mysql://<MYSQL_HOST>:<MYSQL_PORT>/<MYSQL_DB>?useCursorFetch=true&useServerPrepStmts=true&serverTimezone=UTC
```

O driver é `com.mysql.cj.jdbc.Driver`. O uso de cursor e prepared statement
permite que o fetch size de 10.000 seja aplicado sem carregar toda a consulta
de uma vez no driver.

### 9.3 Consulta e particionamento

A subconsulta aplica:

```sql
WHERE updated_at >= '<lower_bound>'
  AND updated_at <  '<upper_bound>'
```

O cálculo de limites e do número efetivo de partições é igual ao PostgreSQL:
até oito partições e aproximadamente uma partição por milhão de registros.

As falhas são isoladas por tabela. Assim, uma tabela com erro não impede a
tentativa das demais, mas torna a tarefa malsucedida ao final da execução CLI.

## 10. Ingestão da API

### 10.1 Endpoints

A base padrão é `http://localhost:8000`, configurável por `API_BASE_URL`.

| Dataset | Endpoint |
| --- | --- |
| `customer_review` | `${API_BASE_URL}/customer-reviews` |
| `exchange_rates` | `${API_BASE_URL}/exchange-rates` |
| `marketing_campaigns` | `${API_BASE_URL}/marketing-campaigns` |

Consulte `docs/doc_api_data_platform.md` para iniciar e configurar a API.

### 10.2 Parâmetros incrementais

Cada requisição `GET` envia:

```text
updated_at_from=<lower_bound em UTC>
updated_at_until=<upper_bound em UTC>
```

Os timestamps usam precisão de microssegundos e sufixo `Z`.

### 10.3 Resiliência HTTP

A sessão HTTP possui:

- timeout de conexão de 10 segundos;
- timeout de leitura de 120 segundos;
- três tentativas de retry;
- backoff exponencial com fator 1;
- retry apenas para `GET`;
- retry nos status 429, 500, 502, 503 e 504;
- respeito ao cabeçalho `Retry-After`.

A resposta é transferida por streaming diretamente para o S3/MinIO, evitando
carregar todo o corpo em memória. O objeto recebe `Content-Type:
application/json`.

Se a própria função criou a sessão HTTP, ela também a fecha ao final. Uma
sessão injetada externamente permanece sob responsabilidade do chamador.

## 11. Observabilidade

Cada arquivo, tabela ou endpoint gera um evento. Ao final de cada módulo, os
eventos são anexados à tabela Delta:

```text
s3a://observability/ingestion_log
```

| Campo | Conteúdo |
| --- | --- |
| `run_id` | Identificador informado na execução. |
| `source_name` | `local`, `postgres`, `mysql` ou `api`. |
| `source_table` | Dataset processado. |
| `source_type` | `csv`, `json`, `table` ou `api`. |
| `file_name` | Nome do arquivo ou da tabela. |
| `target_path` | Diretório de destino no Landing. |
| `started_at` | Início do item. |
| `ended_at` | Fim do item. |
| `duration_seconds` | Duração calculada entre início e fim. |
| `execution_status` | `SUCCESS` ou `FAILED`. |
| `error_message` | Tipo e mensagem da exceção, ou vazio. |
| `execution_date` | Data civil da máquina no momento do log. |

O campo `execution_date` do log é obtido com `date.today()`. Portanto, ele
pode ser diferente da data operacional recebida em `--date`.

O log é gravado depois do processamento de todos os itens do módulo. Se a
escrita do próprio log Delta falhar, a função propaga a exceção.

## 12. Dependências e pré-requisitos

### 12.1 Software

- Python compatível com o ambiente do projeto;
- Java disponível para o PySpark;
- dependências de `requirements.txt` instaladas;
- acesso aos pacotes Maven usados pela sessão Spark na primeira resolução;
- S3 ou MinIO ativo e acessível;
- buckets `landing` e `observability` existentes;
- PostgreSQL, MySQL e API ativos para uma execução completa.

A sessão Spark inclui os seguintes artefatos:

- Delta Lake `3.1.0`;
- Hadoop AWS `3.3.4`;
- MySQL Connector/J `8.0.33`;
- PostgreSQL JDBC `42.7.3`.

### 12.2 Ambiente Python

Na raiz do projeto, no PowerShell:

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install -r requirements.txt
$env:PYTHONPATH = "$PWD\src;$PWD"
```

O `PYTHONPATH` permite resolver imports absolutos como `ingestion`,
`observability` e `utils` ao executar os arquivos diretamente.

### 12.3 Variáveis S3/MinIO

```dotenv
AWS_ENDPOINT_URL=http://localhost:9000
AWS_ACCESS_KEY_ID=access_key_minio
AWS_SECRET_ACCESS_KEY=secret_key_minio
```

O cliente Boto3 valida a conexão com `list_buckets()`. O Spark usa o mesmo
endpoint e credenciais por meio do conector S3A e de acesso path-style.

### 12.4 PostgreSQL

```dotenv
PG_HOST=localhost
PG_PORT=5432
PG_DB=data_platform
PG_USER=<usuario>
PG_PASSWORD=<senha>
```

### 12.5 MySQL

```dotenv
MYSQL_HOST=localhost
MYSQL_PORT=3306
MYSQL_DB=data_platform
MYSQL_USER=<usuario>
MYSQL_PASSWORD=<senha>
```

### 12.6 API e arquivos locais

```dotenv
API_BASE_URL=http://localhost:8000
LOCAL_DATA_PATH=local_data_source
```

Como constantes de caminho e URL são avaliadas durante a importação dos
módulos, prefira exportar essas variáveis no ambiente antes de iniciar o
Python.

### 12.7 Incremental e Spark

```dotenv
INCREMENTAL_INITIAL_WATERMARK=1970-01-01T00:00:00Z
INCREMENTAL_OVERLAP_MINUTES=10
SPARK_MASTER=local[*]
SPARK_SHUFFLE_PARTITIONS=4
SPARK_DEFAULT_PARALLELISM=4
```

`PIPELINE_WATERMARK_UNTIL` é normalmente definido pela orquestração para cada
execução, não como configuração permanente.

## 13. Execução isolada de cada origem

Os scripts exigem `--run-id` e `--date`. A data deve usar `YYYY-MM-DD`.

### 13.1 Local

```powershell
python src/ingestion/ingestion_local.py `
  --run-id "manual__2026-09-20_local" `
  --date "2026-09-20"
```

### 13.2 PostgreSQL

```powershell
python src/ingestion/ingestion_postgres.py `
  --run-id "manual__2026-09-20_postgres" `
  --date "2026-09-20"
```

### 13.3 MySQL

```powershell
python src/ingestion/ingestion_mysql.py `
  --run-id "manual__2026-09-20_mysql" `
  --date "2026-09-20"
```

### 13.4 API

```powershell
python src/ingestion/ingestion_api.py `
  --run-id "manual__2026-09-20_api" `
  --date "2026-09-20"
```

Para usar um corte superior determinístico nos scripts isolados, defina a
variável antes do comando:

```powershell
$env:PIPELINE_WATERMARK_UNTIL = "2026-10-01T09:00:00Z"
python src/ingestion/ingestion_api.py `
  --run-id "manual__api_20261001" `
  --date "2026-10-01"
```

Uma execução isolada cria candidatos internamente, mas não confirma o
watermark. Essa separação é intencional: o commit depende do sucesso do fluxo
completo.

## 14. Execução do pipeline completo

O orquestrador monolítico executa as quatro ingestões e todas as etapas
posteriores na mesma sessão Spark:

```powershell
python src/run_all_pipeline.py `
  --run-id "manual__pipeline_20260920" `
  --date "2026-09-20" `
  --watermark-until "2026-09-21T00:00:00Z"
```

Os três argumentos possuem comportamento distinto:

| Argumento | Obrigatório | Padrão |
| --- | --- | --- |
| `--run-id` | Não | `ingestion_<UUID>` |
| `--date` | Não | Data atual |
| `--watermark-until` | Não | Horário UTC capturado pelo pipeline |

O pipeline confirma os candidatos somente quando:

- não existe evento `FAILED` na ingestão, correção, Bronze, Silver ou Gold;
- não existe falha de roteamento na qualidade.

Ao finalizar, ele também tenta publicar métricas agregadas no Prometheus
Pushgateway conforme a configuração do projeto.

## 15. Execução pelo Airflow

A DAG `data_platform_monthly_pipeline`, em
`airflow/dags/data_platform_pipeline.py`, executa sequencialmente:

```text
ingest_local
  >> ingest_postgres
  >> ingest_mysql
  >> ingest_api
  >> validate_landing_and_route_raw
  >> correct_quarantined_data
  >> transform_raw_to_bronze
  >> transform_bronze_to_silver
  >> transform_silver_to_gold
  >> commit_incremental_watermarks
```

A agenda é `0 6 1 * *`: primeiro dia de cada mês, às 06:00 no fuso
`America/Sao_Paulo`. A DAG não faz catchup, aceita apenas uma execução ativa e
configura duas tentativas adicionais por tarefa.

O Airflow define:

```text
PIPELINE_RUN_ID=airflow__{{ run_id }}
PIPELINE_EXECUTION_DATE={{ data_interval_end.strftime('%Y-%m-%d') }}
PIPELINE_WATERMARK_UNTIL={{ data_interval_end.isoformat() }}
```

No container, o `PYTHONPATH` já inclui `/opt/airflow/data-platform/src` e a
raiz do projeto. Endereços de serviços executados no host usam normalmente
`host.docker.internal`, não `localhost`.

Consulte `docs/doc_airflow_orchestration.md` para instalação, inicialização e
operação detalhada do Airflow.

## 16. Commit manual de watermarks

O ponto de entrada de commit exige `PIPELINE_WATERMARK_UNTIL` e cria eventos de
sucesso para os 13 datasets cadastrados:

```powershell
$env:PIPELINE_WATERMARK_UNTIL = "2026-10-01T09:00:00Z"
python src/ingestion/commit_incremental_state.py `
  --run-id "manual__commit_20261001" `
  --date "2026-10-01"
```

Esse comando deve ser usado somente quando todas as etapas correspondentes
tiverem terminado com sucesso. Ele não inspeciona automaticamente os logs das
outras etapas: assume que a orquestração já garantiu essa condição.

O argumento `--date` é exigido pela interface comum, mas não participa do
cálculo do watermark nesse script.

## 17. Contrato de retorno das funções

As quatro funções principais retornam `list[dict]`, com um evento por item
processado:

```python
events = ingestion_api(
    spark=spark,
    run_id="manual__example",
    ingestion_date=date(2026, 9, 20),
    s3_client=s3_client,
)
```

Em sucesso, o dicionário em memória possui também `_watermark_candidate`. Ao
escrever o log Delta, `write_ingestion_log` seleciona apenas os campos do
schema público e remove esse campo técnico.

As funções de banco criam sua configuração internamente. As funções local e
API recebem um cliente S3, o que facilita testes e reutilização. A API também
aceita uma sessão HTTP injetada.

## 18. Tratamento de erros

### 18.1 Erros anteriores ao loop

Alguns problemas interrompem imediatamente a função:

- cliente S3 ausente nas ingestões local ou API;
- diretório local inexistente ou inválido;
- variável obrigatória de banco ausente;
- falha ao ler o estado Delta de watermark;
- janela incremental inválida;
- falha ao gravar a tabela de observabilidade.

### 18.2 Erros por dataset

Falhas de leitura, requisição ou escrita dentro do loop geram um evento
`FAILED`; o próximo item da mesma origem ainda é tentado.

O bloco `__main__` chama `raise_for_failed_events` depois de encerrar o Spark.
Se houver falhas, lança `RuntimeError` com os nomes dos itens e sinaliza erro
ao orquestrador.

## 19. Validação pós-execução

Depois de uma ingestão, confirme:

1. a presença do diretório da data em `landing`;
2. o formato esperado para cada origem;
3. o evento `SUCCESS` em `observability/ingestion_log`;
4. a ausência de mensagem em `error_message`;
5. o encaminhamento posterior para `raw` ou `quarantine`;
6. após o pipeline completo, a atualização de
   `observability/ingestion_watermarks`.

Exemplo de leitura dos logs em uma sessão Spark configurada:

```python
spark.read.format("delta") \
    .load("s3a://observability/ingestion_log") \
    .orderBy("started_at", ascending=False) \
    .show(truncate=False)
```

Exemplo para watermarks:

```python
spark.read.format("delta") \
    .load("s3a://observability/ingestion_watermarks") \
    .orderBy("source_name", "dataset_name") \
    .show(truncate=False)
```

## 20. Solução de problemas

### 20.1 `ModuleNotFoundError`

Defina o caminho de importação antes de executar:

```powershell
$env:PYTHONPATH = "$PWD\src;$PWD"
```

E confirme que o comando está sendo executado na raiz do projeto.

### 20.2 Falha de conexão com MinIO/S3

Verifique `AWS_ENDPOINT_URL`, credenciais, disponibilidade do serviço e
existência dos buckets. Para execução local, o endpoint costuma ser
`http://localhost:9000`; em um container Airflow, normalmente é
`http://host.docker.internal:9000`.

### 20.3 Driver JDBC não encontrado

Confirme acesso à rede para a resolução inicial dos pacotes Maven e as versões
definidas em `src/utils/spark_session.py`. Verifique também Java e o cache local
do Ivy.

### 20.4 Banco inacessível

Confira host, porta, banco, usuário, senha, firewall e permissão de `SELECT`.
De dentro de container, `localhost` representa o próprio container.

### 20.5 API retorna erro ou timeout

Confirme se a API está ativa na mesma base configurada em `API_BASE_URL` e se
os endpoints aceitam os parâmetros incrementais. Status elegíveis são
retentados automaticamente; outros erros HTTP falham sem retry por status.

### 20.6 `KeyError: updated_at` ou erro de timestamp local

Todos os registros de arquivos locais precisam ter `updated_at`. Confirme o
cabeçalho CSV ou a propriedade JSON e use valores ISO 8601 válidos.

### 20.7 Janela incremental inválida

O limite superior precisa ser posterior ao inferior. Revise
`PIPELINE_WATERMARK_UNTIL`, `INCREMENTAL_INITIAL_WATERMARK`, o watermark já
confirmado e o overlap.

### 20.8 Arquivos vazios no Landing

Isso pode ser legítimo quando nenhum registro pertence à janela. O local ainda
envia um CSV apenas com cabeçalho ou um JSON `[]`; bancos podem criar um
diretório Parquet vazio. A etapa de qualidade decide o tratamento seguinte.

### 20.9 Watermark não avançou

Verifique se o pipeline chegou à tarefa de commit. Executar somente um script
de ingestão não atualiza `ingestion_watermarks`. No pipeline completo, qualquer
falha relevante impede a confirmação.

## 21. Limitações e cuidados da implementação atual

### 21.1 Dependência obrigatória de `updated_at`

O incremental pressupõe que todas as fontes exponham `updated_at`. Não existe
fallback para carga completa nem estratégia específica para exclusões físicas.

### 21.2 Ausência de paginação própria na API

O cliente faz uma requisição por endpoint e transmite uma única resposta. Se
a API passar a paginar os resultados, a ingestão precisará percorrer as páginas
ou adotar outro protocolo de streaming.

### 21.3 Arquivos locais processados em memória

A filtragem de JSON carrega todo o documento, e a saída filtrada de CSV/JSON é
materializada em memória antes do upload. Arquivos grandes podem exigir uma
implementação realmente streaming.

### 21.4 Watermark avançado até o limite solicitado

O candidato usa o `upper_bound`, não o maior `updated_at` efetivamente lido.
Isso é correto quando a origem garante que todos os registros anteriores ao
corte já estão visíveis; fontes com atraso maior que o overlap podem exigir
outra estratégia.

### 21.5 Reexecução sobrescreve o Landing

O caminho depende apenas do dataset e da data. Duas execuções com a mesma data
não mantêm versões separadas por `run_id`.

### 21.6 Commit do Airflow baseado na orquestração

`commit_incremental_state.py` confirma os 13 datasets de uma vez usando o
limite do ambiente. A segurança depende de ele permanecer como última tarefa
de uma cadeia em que todas as anteriores precisam ter sucesso.

### 21.7 Observabilidade não é transacional com os dados

A gravação no Landing e a gravação do log Delta são operações distintas. Uma
falha no log pode ocorrer depois que os dados já foram persistidos.

### 21.8 Contagem de registros não é registrada

O schema de ingestão registra duração e status, mas não contém quantidade de
linhas extraídas. As contagens aparecem apenas em etapas posteriores do
pipeline.

## 22. Recomendações para evolução

- adicionar contagem de registros e tamanho em bytes ao log de ingestão;
- implementar streaming real para arquivos locais grandes;
- suportar paginação e validação de `Content-Type` na API;
- monitorar atraso da origem para dimensionar o overlap com segurança;
- registrar o limite inferior e superior usados em cada evento;
- criar uma estratégia explícita para deletes na origem;
- incluir `run_id` no layout se houver necessidade de preservar reexecuções;
- validar previamente a existência dos buckets e permissões necessárias;
- adicionar testes de integração para JDBC, MinIO e Delta Lake;
- confirmar watermarks a partir de um manifesto explícito de datasets bem-
  sucedidos, caso o fluxo deixe de ser estritamente sequencial.

## 23. Resumo operacional

Para uma execução manual completa e reproduzível:

1. ative o ambiente virtual e defina `PYTHONPATH`;
2. configure MinIO/S3, PostgreSQL, MySQL e a API;
3. confirme que os 13 datasets possuem `updated_at` válido;
4. escolha um `run_id`, uma data operacional e um limite superior UTC;
5. execute `src/run_all_pipeline.py`;
6. verifique Landing, logs de ingestão e etapas posteriores;
7. confirme que os watermarks avançaram somente após o sucesso integral.

O princípio central do módulo é separar **extração bem-sucedida** de
**confirmação do progresso**. Os dados podem ser ingeridos e avaliados sem que
o estado incremental avance antes de toda a cadeia estar concluída.
