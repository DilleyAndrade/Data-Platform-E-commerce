# Processo de ingestão

## Visão geral

O pipeline extrai dados de quatro origens e os grava no bucket S3/MinIO de
landing. Cada item é processado individualmente e recebe uma partição diária
no formato `ingestion_date_YYYYMMDD`.

A execução central está em `src/run_all_pipeline.py`, que chama os processos na
seguinte ordem:

1. Arquivos locais.
2. PostgreSQL.
3. MySQL.
4. API HTTP.

O destino padrão é o bucket `landing`, definido pela constante `BUCKET_LAN`.
Os arquivos são gravados no formato original quando a origem é local e em
Parquet quando a origem é um banco de dados ou uma API.

## Pré-requisitos

Instale as dependências na raiz do projeto:

```bash
pip install -r requirements.txt
```

Configure as variáveis de ambiente usadas pelo projeto. Um arquivo `.env` pode
ser utilizado porque os módulos carregam as variáveis com `python-dotenv`.

### S3 ou MinIO

| Variável | Uso |
| --- | --- |
| `AWS_ENDPOINT_URL` | Endpoint opcional do S3/MinIO. |
| `AWS_ACCESS_KEY_ID` | Chave de acesso. |
| `AWS_SECRET_ACCESS_KEY` | Segredo de acesso. |

O bucket `landing` precisa existir e a credencial precisa ter permissão para
listar buckets, fazer upload e gravar objetos. O cliente S3 valida a conexão
com `list_buckets()` antes de iniciar cada processo.

### PostgreSQL

| Variável | Uso |
| --- | --- |
| `PG_HOST` | Host do banco. |
| `PG_PORT` | Porta do banco. |
| `PG_DB` | Nome do banco. |
| `PG_USER` | Usuário. |
| `PG_PASSWORD` | Senha. |

### MySQL

| Variável | Uso |
| --- | --- |
| `MYSQL_HOST` | Host do banco. |
| `MYSQL_PORT` | Porta do banco. |
| `MYSQL_DB` | Nome do banco. |
| `MYSQL_USER` | Usuário. |
| `MYSQL_PASSWORD` | Senha. |

A API deve estar disponível em `http://localhost:8000`. A configuração dos
endpoints está em `src/path_constants/path_constants.py`; consulte a
documentação da API em [doc_api_data_platform.md](doc_api_data_platform.md)
para iniciar o servidor.

## Ingestão de arquivos locais

Implementada por `ingestion_local` em
`src/ingestion/ingestion_local.py`.

1. O processo lista os arquivos em `PATH_LOCAL_FILES`, atualmente
   `local_data_source/`.
2. Cada arquivo encontrado é enviado diretamente para o bucket `landing`, sem
   conversão de formato.
3. O nome do arquivo é dividido em nome e extensão para compor o registro de
   observabilidade.
4. O resultado é gravado no caminho:

```text
s3://landing/<nome_do_arquivo_sem_extensao>/ingestion_date_YYYYMMDD/<arquivo_original>
```

Os arquivos atualmente disponíveis são `coupons.csv`, `delivery_tracking.csv`,
`payments.csv` e `website_events.json`. Se o diretório estiver vazio, o
processo registra a situação no log e não cria uma entrada de ingestão.

## Ingestão do PostgreSQL

Implementada por `ingestion_postgres` em
`src/ingestion/ingestion_postgres.py`.

As tabelas processadas são:

- `public.customers`
- `public.products`
- `public.suppliers`

Para cada tabela, o processo:

1. Abre uma conexão SQLAlchemy usando o driver `psycopg`.
2. Executa `select * from public.<tabela>;`.
3. Converte o resultado em um `pandas.DataFrame`.
4. Serializa o DataFrame em Parquet na memória.
5. Envia o objeto para o bucket `landing`.

O layout de saída é:

```text
s3://landing/<tabela>/ingestion_date_YYYYMMDD/<tabela>.parquet
```

## Ingestão do MySQL

Implementada por `ingestion_mysql` em
`src/ingestion/ingestion_mysql.py`.

As tabelas processadas são:

- `inventory`
- `order_items`
- `orders`

Para cada tabela, o processo abre uma conexão SQLAlchemy usando o driver
`pymysql`, executa `select * from <tabela>;`, converte o resultado para
`pandas.DataFrame` e salva o conteúdo como Parquet no bucket `landing`.

O layout de saída é:

```text
s3://landing/<tabela>/ingestion_date_YYYYMMDD/<tabela>.parquet
```

## Ingestão da API

Implementada por `ingestion_api` em `src/ingestion/ingestion_api.py`.

Os endpoints e nomes lógicos processados são:

| Endpoint | Nome lógico | Arquivo de saída |
| --- | --- | --- |
| `/customer-reviews` | `customer_review` | `customer_review.parquet` |
| `/exchange-rates` | `exchange_rates` | `exchange_rates.parquet` |
| `/marketing-campaigns` | `marketing_campaigns` | `marketing_campaigns.parquet` |

Para cada endpoint, o processo:

1. Faz uma requisição HTTP `GET` com timeout de 10 segundos.
2. Valida a resposta HTTP e converte o corpo para JSON.
3. Normaliza uma resposta JSON do tipo objeto ou lista em um DataFrame.
4. Serializa o DataFrame em Parquet na memória.
5. Envia o objeto para o bucket `landing`.

O layout de saída é:

```text
s3://landing/<nome_logico>/ingestion_date_YYYYMMDD/<nome_logico>.parquet
```

Erros HTTP, de conexão, timeout e requisições inesperadas são registrados pelo
logger da aplicação.

## Observabilidade

Após cada item processado, o pipeline chama `create_ingestion_log_table` para
registrar uma linha na tabela Delta de observabilidade:

```text
s3a://observability/ingestion_log
```

O registro contém, entre outros campos:

- identificador da execução;
- origem e tabela ou arquivo de origem;
- tipo da origem;
- caminho de destino;
- horários de início e fim;
- duração em segundos;
- status `SUCCESS` ou `FAILED`;
- mensagem de erro, quando houver;
- data da execução.

A tabela é atualizada em modo append. Ao final do pipeline, o conteúdo é lido
com Spark e exibido no console.

## Execução

A partir da raiz do projeto, com os serviços de S3/MinIO, PostgreSQL, MySQL e a
API disponíveis, execute:

```bash
spark-submit --master 'local[*]' src/run_all_pipeline.py
```

O processo deve ser executado com o diretório `src` no caminho de importação,
ou com a configuração equivalente do ambiente Python, pois o orquestrador usa
imports como `ingestion.ingestion_api` e `utils.spark_session`.

Para validar uma execução, confira:

1. Os objetos criados no bucket `landing`.
2. Os logs da aplicação no console.
3. As entradas correspondentes em `s3a://observability/ingestion_log`.
4. O status e a mensagem de erro de cada origem.
