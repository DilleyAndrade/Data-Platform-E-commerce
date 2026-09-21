# Processo de observabilidade

## 1. Objetivo

A pasta `src/observability` centraliza dois mecanismos de acompanhamento da
plataforma de dados:

1. **observabilidade histórica em Delta Lake**, com eventos detalhados por
   arquivo, dataset, validação e transformação;
2. **observabilidade operacional em Prometheus**, com métricas agregadas da
   execução mais recente e publicação por meio do Pushgateway.

Esses mecanismos atendem necessidades diferentes. As tabelas Delta permitem
auditoria, investigação por `run_id` e análise detalhada dos eventos. As
métricas Prometheus são adequadas para painéis, alertas e acompanhamento rápido
do estado operacional.

Além do código dessa pasta, o processo depende de:

- schemas em `src/schemas/schemas.py`;
- bucket `observability`, definido em `src/path_constants/path_constants.py`;
- integração das etapas do pipeline com os writers;
- instrumentação HTTP da API Data Platform;
- configurações Prometheus, alertas e dashboard em `config/prometheus`.

## 2. Estrutura da pasta

```text
src/observability/
|-- obs_ingestion_log.py
|-- obs_landing_quality_log.py
|-- obs_data_correction_log.py
|-- obs_transformation_log.py
`-- prometheus_metrics.py
```

| Arquivo | Responsabilidade |
| --- | --- |
| `obs_ingestion_log.py` | Cria eventos de ingestão e os grava em Delta Lake. |
| `obs_landing_quality_log.py` | Persiste resultados das verificações de qualidade do Landing. |
| `obs_data_correction_log.py` | Persiste tentativas e resultados da correção de dados. |
| `obs_transformation_log.py` | Persiste métricas das transformações Raw, Bronze, Silver e Gold. |
| `prometheus_metrics.py` | Agrega eventos do pipeline e publica gauges no Pushgateway. |

## 3. Arquitetura geral

```text
Ingestão -----------------------> ingestion_log -----------+
Qualidade Landing --------------> landing_quality_log -----+
Correção -----------------------> data_correction_log ------+--> MinIO/S3
Transformações -----------------> transformation_log -------+    bucket observability
Validações Gold ----------------> gold_validation_failures -+
Estado incremental -------------> ingestion_watermarks ----+

API Data Platform -- /metrics ------------------------------+
                                                             v
Pipeline monolítico --> Pushgateway --> Prometheus --> Alertas/Grafana
```

O fluxo possui duas estratégias de coleta:

- **pull:** o Prometheus consulta periodicamente o endpoint `/metrics` da API;
- **push seguido de pull:** o pipeline batch envia métricas ao Pushgateway, e
  o Prometheus coleta o Pushgateway.

O Pushgateway é necessário porque o pipeline é um processo finito. Sem ele, o
Prometheus poderia não conseguir coletar o processo enquanto estivesse ativo.

## 4. Armazenamento no bucket de observabilidade

O bucket lógico é definido por:

```python
BUCKET_OBS = "observability"
```

As tabelas são acessadas pelo Spark com o esquema `s3a://`:

```text
s3a://observability/ingestion_log
s3a://observability/landing_quality_log
s3a://observability/data_correction_log
s3a://observability/transformation_log
s3a://observability/gold_validation_failures
s3a://observability/ingestion_watermarks
```

O diretório padrão do warehouse Spark também fica nesse bucket:

```text
s3a://observability/spark-warehouse/
```

As quatro tabelas escritas pelos módulos `obs_*` usam formato Delta e modo
`append`. Os eventos anteriores, portanto, são preservados.

## 5. Padrão dos writers Delta

Os writers seguem o mesmo fluxo:

1. recebem uma sessão Spark e uma lista de dicionários;
2. retornam `None` imediatamente se a lista estiver vazia;
3. criam um DataFrame com schema explícito;
4. gravam o DataFrame em formato Delta e modo `append`;
5. registram no logger o nome da tabela, quantidade de eventos e caminho;
6. retornam o DataFrame criado.

Exemplo conceitual:

```python
dataframe = spark.createDataFrame(events, schema=<schema>)
dataframe.write.format("delta").mode("append").save(target_path)
```

O schema explícito evita inferência inconsistente de tipos. Em contrapartida,
eventos incompatíveis com o contrato podem fazer a criação do DataFrame
falhar.

Não existe transação conjunta entre o processamento dos dados e a escrita do
evento. Uma etapa pode concluir sua escrita principal e falhar posteriormente
ao persistir a observabilidade.

## 6. Log de ingestão

### 6.1 Integração

As quatro origens chamam `create_ingestion_log` e `write_ingestion_log`:

- arquivos locais;
- PostgreSQL;
- MySQL;
- API HTTP.

O destino é:

```text
s3a://observability/ingestion_log
```

### 6.2 Criação do evento

`create_ingestion_log` recebe os identificadores, horários e resultado do item
e calcula:

```text
duration_seconds = ended_at - started_at
execution_date = date.today()
```

`execution_date` representa a data civil da máquina no momento do registro.
Ela pode ser diferente da data operacional informada em `--date`.

### 6.3 Schema

| Campo | Tipo Spark | Obrigatório | Descrição |
| --- | --- | --- | --- |
| `run_id` | string | Sim | Identificador da execução. |
| `source_name` | string | Não | Origem: `local`, `postgres`, `mysql` ou `api`. |
| `source_table` | string | Não | Dataset ou tabela processada. |
| `source_type` | string | Não | Tipo como `csv`, `json`, `table` ou `api`. |
| `file_name` | string | Não | Arquivo ou nome físico da fonte. |
| `target_path` | string | Não | Caminho gravado no Landing. |
| `started_at` | timestamp | Não | Início do item. |
| `ended_at` | timestamp | Não | Fim do item. |
| `duration_seconds` | double | Não | Duração em segundos. |
| `execution_status` | string | Não | `SUCCESS` ou `FAILED`. |
| `error_message` | string | Não | Exceção ou texto vazio. |
| `execution_date` | date | Não | Data em que o evento foi criado. |

### 6.4 Campo técnico de watermark

Eventos bem-sucedidos podem carregar `_watermark_candidate` em memória. Antes
da escrita, `write_ingestion_log` reconstrói cada dicionário usando apenas os
campos do `ingestion_log_schema`. Assim, o candidato não é persistido nessa
tabela; o estado confirmado fica em `ingestion_watermarks`.

## 7. Log de qualidade do Landing

### 7.1 Integração

`src/data_quality/dq_landing_raw.py` produz eventos para verificações de
existência, integridade, conteúdo, schema, nulos, duplicidade e roteamento.

O destino é:

```text
s3a://observability/landing_quality_log
```

### 7.2 Schema

| Campo | Tipo Spark | Obrigatório | Descrição |
| --- | --- | --- | --- |
| `run_id` | string | Sim | Execução da validação. |
| `file_name` | string | Não | Nome lógico ou físico validado. |
| `source_name` | string | Não | Origem do dataset. |
| `file_path` | string | Não | Caminho no Landing. |
| `check_name` | string | Não | Nome específico da verificação. |
| `check_type` | string | Não | Categoria da verificação. |
| `records_total` | long | Não | Registros avaliados. |
| `records_valid` | long | Não | Registros considerados válidos. |
| `records_invalid` | long | Não | Registros considerados inválidos. |
| `invalid_percentage` | double | Não | Percentual inválido. |
| `check_status` | string | Não | `PASS` ou `FAIL`. |
| `error_message` | string | Não | Detalhe da falha. |
| `execution_ts` | timestamp | Não | Instante da verificação. |

Os eventos de roteamento são especialmente relevantes para determinar se o
dataset foi movido a `raw` ou `quarantine`.

## 8. Log de correção

### 8.1 Integração

`src/data_correction/data_correction.py` registra cada tentativa de correção no
destino:

```text
s3a://observability/data_correction_log
```

Além de auditoria, essa tabela é consultada pelo próprio processo de correção
para determinar tentativas anteriores.

### 8.2 Schema

| Campo | Tipo Spark | Obrigatório | Descrição |
| --- | --- | --- | --- |
| `correction_run_id` | string | Sim | Identificador da execução de correção. |
| `original_run_id` | string | Não | Execução que originou a falha de qualidade. |
| `dataset_name` | string | Não | Dataset corrigido. |
| `source_name` | string | Não | Origem original. |
| `file_name` | string | Não | Arquivo associado. |
| `original_path` | string | Não | Caminho em quarentena. |
| `target_path` | string | Não | Caminho de saída. |
| `correction_type` | string | Não | Estratégia aplicada. |
| `records_input` | long | Não | Registros recebidos. |
| `records_corrected` | long | Não | Registros corrigidos. |
| `records_still_invalid` | long | Não | Registros ainda inválidos. |
| `records_discarded` | long | Não | Registros descartados. |
| `correction_attempt` | integer | Não | Número da tentativa. |
| `correction_status` | string | Não | Resultado da correção. |
| `error_message` | string | Não | Descrição de erro. |
| `correction_start_ts` | timestamp | Não | Início da tentativa. |
| `correction_end_ts` | timestamp | Não | Fim da tentativa. |
| `duration_seconds` | double | Não | Duração da correção. |
| `execution_date` | date | Não | Data operacional registrada. |

## 9. Log de transformação

### 9.1 Integração

O mesmo writer atende três módulos:

- Raw para Bronze;
- Bronze para Silver;
- Silver para Gold.

O destino compartilhado é:

```text
s3a://observability/transformation_log
```

Os campos `pipeline_name` e `stage` permitem separar as camadas durante as
consultas.

### 9.2 Schema

| Campo | Tipo Spark | Obrigatório | Descrição |
| --- | --- | --- | --- |
| `run_id` | string | Sim | Identificador da execução. |
| `pipeline_name` | string | Não | Nome lógico do processo. |
| `stage` | string | Não | Camada ou etapa executada. |
| `source_table` | string | Não | Tabela de origem. |
| `target_table` | string | Não | Tabela de destino. |
| `source_path` | string | Não | Caminho de leitura. |
| `target_path` | string | Não | Caminho de escrita. |
| `records_input` | long | Não | Quantidade lida. |
| `records_output` | long | Não | Quantidade produzida. |
| `records_rejected` | long | Não | Quantidade rejeitada. |
| `records_inserted` | long | Não | Registros inseridos. |
| `records_updated` | long | Não | Registros atualizados. |
| `records_deleted` | long | Não | Registros excluídos. |
| `data_quality_status` | string | Não | Resultado de qualidade associado. |
| `processing_start_ts` | timestamp | Não | Início do processamento. |
| `processing_end_ts` | timestamp | Não | Fim do processamento. |
| `duration_seconds` | double | Não | Duração do evento. |
| `status` | string | Não | `SUCCESS` ou `FAILED`. |
| `error_message` | string | Não | Detalhe da falha. |
| `execution_date` | date | Não | Data operacional. |

## 10. Outras tabelas de controle no mesmo bucket

Embora não possuam writer dentro de `src/observability`, duas tabelas fazem
parte do ecossistema de observabilidade.

### 10.1 Falhas de validação Gold

`transform_silver_gold.py` grava somente falhas em:

```text
s3a://observability/gold_validation_failures
```

| Campo | Descrição |
| --- | --- |
| `run_id` | Execução da transformação. |
| `datamart` | Datamart validado. |
| `validation_type` | Tipo extraído do erro. |
| `validation_error` | Mensagem completa. |
| `candidate_tables` | Tabelas participantes, separadas por vírgula. |
| `validation_status` | Valor `FAIL`. |
| `execution_ts` | Instante UTC. |
| `execution_date` | Data operacional. |

Como apenas falhas são persistidas, a ausência de linhas não prova por si só
que a etapa foi executada. Ela deve ser correlacionada com
`transformation_log`.

### 10.2 Watermarks de ingestão

`src/ingestion/incremental_state.py` mantém:

```text
s3a://observability/ingestion_watermarks
```

| Campo | Descrição |
| --- | --- |
| `source_name` | Origem incremental. |
| `dataset_name` | Dataset controlado. |
| `watermark_ts` | Limite superior confirmado. |
| `last_successful_run_id` | Último pipeline que confirmou o estado. |
| `committed_at` | Instante UTC do commit. |

Essa tabela representa estado operacional, não um histórico append-only. Após
a primeira criação, o código faz `MERGE` pela origem e dataset.

## 11. Métricas do pipeline

### 11.1 Ponto de publicação

`push_pipeline_metrics` é chamado no bloco `finally` de
`src/run_all_pipeline.py`. Isso permite publicar status de falha mesmo quando
ocorre uma exceção durante o processamento, desde que a execução alcance a
chamada de publicação.

A função recebe:

- `succeeded`: resultado geral;
- `duration_seconds`: duração de parede medida com relógio monotônico;
- `stage_events`: eventos agrupados por etapa;
- `quality_events`: verificações do Landing.

As etapas previstas são:

```text
ingestion, correction, bronze, silver, gold
```

### 11.2 Registry isolado

Cada publicação cria um novo `CollectorRegistry`. Isso evita misturar métricas
globais do processo Python e impede erros de registro duplicado quando a função
é chamada novamente no mesmo processo.

Todas as métricas próprias são `Gauge`, pois representam o último estado ou
um valor agregado da execução mais recente.

## 12. Catálogo de métricas Prometheus

### 12.1 Estado geral

| Métrica | Labels | Cálculo |
| --- | --- | --- |
| `data_pipeline_last_status` | Nenhum | `1` para sucesso; `0` para falha. |
| `data_pipeline_last_duration_seconds` | Nenhum | Duração completa do pipeline. |
| `data_pipeline_last_completion_timestamp_seconds` | Nenhum | Unix timestamp da publicação. |
| `data_pipeline_last_success_timestamp_seconds` | Nenhum | Publicada somente em execução bem-sucedida. |
| `data_pipeline_last_failure_timestamp_seconds` | Nenhum | Publicada somente em execução malsucedida. |

### 12.2 SLA

| Métrica | Labels | Cálculo |
| --- | --- | --- |
| `data_pipeline_duration_sla_seconds` | Nenhum | Limite configurado. |
| `data_pipeline_duration_sla_violation` | Nenhum | `1` se duração for estritamente maior que o SLA; senão `0`. |

O valor padrão é 3.600 segundos. `PIPELINE_SLA_SECONDS` inválido, não numérico,
zero ou negativo gera warning e retorna ao padrão.

### 12.3 Métricas por etapa

Todas usam o label `stage`.

| Métrica | Cálculo |
| --- | --- |
| `data_pipeline_stage_duration_seconds` | Soma de `duration_seconds`. |
| `data_pipeline_records_input` | Soma de `records_input`. |
| `data_pipeline_records_output` | Soma de `records_output`. |
| `data_pipeline_records_rejected` | Soma de `records_rejected`. |
| `data_pipeline_stage_failures` | Quantidade de eventos com status `FAILED`. |

Para determinar falha, o código procura primeiro `status` e, se ausente,
`execution_status`. Campos quantitativos ausentes, nulos ou vazios contam como
zero. Por isso, ingestão possui duração e falhas, mas normalmente apresenta
zero nas métricas de quantidade de registros.

### 12.4 Qualidade

| Métrica | Labels | Cálculo |
| --- | --- | --- |
| `data_pipeline_quality_checks_failed` | `check_type` | Número de eventos `FAIL` por categoria. |
| `data_pipeline_quality_ratio` | Nenhum | `(entrada Silver - rejeitados Silver) / entrada Silver`. |
| `data_pipeline_quality_records_invalid` | Nenhum | Soma de rejeitados na Silver. |

Se a entrada Silver for zero, o ratio fica em `0.0`. O cálculo limita o mínimo
a zero, mas não força máximo de um.

`data_pipeline_quality_checks_failed` só cria uma série para tipos de check que
aparecem em `quality_events`. Categorias ausentes na execução não são
explicitamente publicadas com zero.

## 13. Publicação no Pushgateway

### 13.1 Variáveis

```dotenv
PROMETHEUS_PUSHGATEWAY_URL=http://localhost:9091
PROMETHEUS_JOB_NAME=data_platform_pipeline
PIPELINE_SLA_SECONDS=3600
```

| Variável | Obrigatória | Padrão | Uso |
| --- | --- | --- | --- |
| `PROMETHEUS_PUSHGATEWAY_URL` | Para publicar | Nenhum | Endpoint do Pushgateway. |
| `PROMETHEUS_JOB_NAME` | Não | `data_platform_pipeline` | Grouping key `job`. |
| `PIPELINE_SLA_SECONDS` | Não | `3600` | Limite de duração. |

Se a URL não estiver configurada, a função registra warning e retorna sem
falhar o pipeline.

### 13.2 Operação de push

O cliente chama `pushadd_to_gateway` com timeout de 10 segundos. A operação
adiciona ou atualiza as famílias de métricas enviadas para o mesmo agrupamento
do job sem apagar famílias ausentes. Esse comportamento preserva, por exemplo,
o último timestamp de sucesso durante uma publicação de falha.

Qualquer exceção de rede ou resposta inválida é capturada e registrada como
warning com stack trace. A falha de telemetria não altera o resultado do
pipeline.

## 14. Métricas HTTP da API

`api_data_platform/main.py` usa
`prometheus-fastapi-instrumentator`:

```python
Instrumentator().instrument(app).expose(
    app,
    endpoint="/metrics",
    include_in_schema=False,
)
```

Consequências:

- `/metrics` não aparece na documentação OpenAPI;
- a rota continua publicamente acessível;
- o Prometheus coleta contadores e histogramas HTTP;
- o dashboard utiliza `http_requests_total` e
  `http_request_duration_seconds_*`;
- várias consultas excluem `handler="/metrics"` para o scrape não distorcer
  as métricas das rotas de negócio.

O nome exato de labels e séries depende da versão instalada do instrumentador.
O dashboard atual espera labels como `handler` e `status`.

## 15. Configuração do Prometheus

Existem duas configurações:

```text
config/prometheus/prometheus-local.yml
config/prometheus/prometheus-docker-runtime.yml
```

Ambas usam:

```text
scrape_interval: 15s
evaluation_interval: 15s
rule_files: prometheus-alerts.yml
```

### 15.1 Execução local

| Job | Target |
| --- | --- |
| `prometheus` | `127.0.0.1:9090` |
| `data-platform-api` | `127.0.0.1:8000`, caminho `/metrics` |
| `pushgateway` | `127.0.0.1:9091` |

### 15.2 Prometheus em container

| Job | Target |
| --- | --- |
| `prometheus` | `127.0.0.1:9090` |
| `data-platform-api` | `host.docker.internal:8000` |
| `pushgateway` | `data-platform-pushgateway:9091` |

Prometheus e Pushgateway precisam compartilhar a mesma rede para que o nome
`data-platform-pushgateway` resolva.

O job do Pushgateway usa `honor_labels: true`. Isso preserva o label
`job="data_platform_pipeline"` enviado no push. Sem essa opção, filtros dos
alertas e do dashboard poderiam encontrar `exported_job` em vez de `job`.

## 16. Regras de alerta

`config/prometheus/prometheus-alerts.yml` define cinco alertas.

| Alerta | Condição | Espera | Severidade |
| --- | --- | --- | --- |
| `DataPipelineLastRunFailed` | Último status igual a zero. | 1 minuto | critical |
| `DataPipelineFreshnessExceeded` | Sem conclusão há mais de 90.000 s (25 h). | 5 minutos | critical |
| `DataPipelineDurationSlaViolated` | Flag de violação igual a um. | 1 minuto | warning |
| `DataPipelineQualityBelowTarget` | Ratio de qualidade menor que 0,98. | 5 minutos | warning |
| `DataPipelineRejectedRateHigh` | Rejeitados sobre entradas acima de 5%. | 5 minutos | warning |

O Prometheus avalia e exibe esses alertas, mas o repositório não configura
Alertmanager. Portanto, as regras podem ficar `pending` ou `firing` sem enviar
e-mail, Slack ou outra notificação externa.

A regra de freshness pressupõe frequência próxima de diária, enquanto a DAG
principal documentada é mensal. Essa diferença pode fazer o alerta disparar de
forma permanente e deve ser ajustada ao SLA real de agendamento.

## 17. Dashboard Grafana

O arquivo importável é:

```text
config/prometheus/grafana-dashboard.json
```

O dashboard `Data Platform - Pipeline Overview` possui 26 painéis agrupando:

- status, quantidade e taxa de sucesso das execuções;
- duração total, duração por etapa e gargalo;
- SLA e tempo desde a última conclusão;
- entrada, saída e rejeição de registros;
- qualidade e checks com falha;
- produção da camada Gold;
- últimas execuções bem-sucedida e malsucedida;
- taxa, erros, latência e volume da API;
- quantidade de alertas ativos.

O datasource referenciado possui UID literal `prometheus`. Antes da importação,
o Grafana precisa ter um datasource Prometheus com exatamente esse UID ou o
JSON precisa ser ajustado.

O dashboard atualiza a cada 15 segundos e abre com janela padrão das últimas
24 horas.

## 18. Execução local da pilha de métricas

Os binários não são instalados automaticamente pela pasta
`src/observability`. Para usar a configuração local, inicie os componentes
separadamente.

### 18.1 API

Na raiz do projeto:

```powershell
$env:PYTHONPATH = "$PWD\src;$PWD"
python -m uvicorn api_data_platform.main:app `
  --host 127.0.0.1 `
  --port 8000
```

Valide:

```powershell
Invoke-WebRequest http://127.0.0.1:8000/metrics
```

### 18.2 Pushgateway

No diretório que contém o executável:

```powershell
.\pushgateway.exe --web.listen-address=:9091
```

Valide:

```powershell
Invoke-WebRequest http://127.0.0.1:9091/metrics
```

### 18.3 Prometheus

Para manter o caminho relativo do arquivo de regras, execute a partir de
`config/prometheus` ou forneça caminhos absolutos adequados:

```powershell
Set-Location config/prometheus
.\prometheus.exe --config.file=prometheus-local.yml
```

Se o executável estiver em outro diretório, use o caminho completo para ele.

Interfaces úteis:

| Recurso | Endereço |
| --- | --- |
| Prometheus | `http://127.0.0.1:9090` |
| Targets | `http://127.0.0.1:9090/targets` |
| Regras | `http://127.0.0.1:9090/rules` |
| Alertas | `http://127.0.0.1:9090/alerts` |
| Pushgateway | `http://127.0.0.1:9091` |
| API metrics | `http://127.0.0.1:8000/metrics` |

### 18.4 Grafana

Depois de iniciar o Grafana:

1. crie um datasource apontando para `http://127.0.0.1:9090`;
2. defina seu UID como `prometheus`;
3. importe `config/prometheus/grafana-dashboard.json`;
4. selecione um intervalo que contenha uma publicação do pipeline.

Se Grafana estiver em container, `127.0.0.1` aponta para o próprio container.
Use o endereço do Prometheus visto a partir da rede do Grafana.

## 19. Geração das métricas batch

Com Pushgateway configurado, execute o pipeline monolítico:

```powershell
$env:PROMETHEUS_PUSHGATEWAY_URL = "http://127.0.0.1:9091"
$env:PROMETHEUS_JOB_NAME = "data_platform_pipeline"
$env:PIPELINE_SLA_SECONDS = "3600"
$env:PYTHONPATH = "$PWD\src;$PWD"

python src/run_all_pipeline.py `
  --run-id "manual__observability_20260920" `
  --date "2026-09-20" `
  --watermark-until "2026-09-21T00:00:00Z"
```

Ao terminar, consulte diretamente o Pushgateway:

```powershell
Invoke-WebRequest http://127.0.0.1:9091/metrics
```

Ou consulte o Prometheus:

```powershell
$query = [uri]::EscapeDataString('data_pipeline_last_status{job="data_platform_pipeline"}')
Invoke-RestMethod "http://127.0.0.1:9090/api/v1/query?query=$query"
```

## 20. Consultas úteis nas tabelas Delta

Os exemplos pressupõem uma sessão Spark configurada para MinIO/S3 e Delta.

### 20.1 Últimas falhas de ingestão

```python
from pyspark.sql import functions as F

spark.read.format("delta") \
    .load("s3a://observability/ingestion_log") \
    .filter(F.col("execution_status") == "FAILED") \
    .orderBy(F.col("started_at").desc()) \
    .show(truncate=False)
```

### 20.2 Checks de qualidade com falha

```python
spark.read.format("delta") \
    .load("s3a://observability/landing_quality_log") \
    .filter("check_status = 'FAIL'") \
    .select("run_id", "source_name", "file_name", "check_type", "error_message") \
    .show(truncate=False)
```

### 20.3 Volume por estágio

```python
spark.read.format("delta") \
    .load("s3a://observability/transformation_log") \
    .groupBy("run_id", "stage") \
    .agg(
        F.sum("records_input").alias("input"),
        F.sum("records_output").alias("output"),
        F.sum("records_rejected").alias("rejected"),
    ) \
    .orderBy(F.col("run_id").desc(), "stage") \
    .show(truncate=False)
```

### 20.4 Tentativas de correção

```python
spark.read.format("delta") \
    .load("s3a://observability/data_correction_log") \
    .select(
        "dataset_name",
        "correction_attempt",
        "correction_status",
        "records_corrected",
        "records_still_invalid",
    ) \
    .orderBy(F.col("correction_end_ts").desc()) \
    .show(truncate=False)
```

## 21. Consultas PromQL úteis

### Estado atual

```promql
data_pipeline_last_status{job="data_platform_pipeline"}
```

### Tempo desde a última conclusão

```promql
time() - data_pipeline_last_completion_timestamp_seconds{job="data_platform_pipeline"}
```

### Etapa mais lenta

```promql
topk(1, data_pipeline_stage_duration_seconds{job="data_platform_pipeline"})
```

### Taxa global de rejeição

```promql
sum(data_pipeline_records_rejected{job="data_platform_pipeline"})
/
clamp_min(sum(data_pipeline_records_input{job="data_platform_pipeline"}), 1)
```

### Requisições da API por rota

```promql
sum by (handler) (
  rate(http_requests_total{job="data-platform-api",handler!="/metrics"}[5m])
)
```

## 22. Airflow e observabilidade

A DAG mensal atual executa cada módulo em um `BashOperator` separado. Cada
etapa continua gravando sua respectiva tabela Delta, pois essa lógica está
dentro dos módulos.

Entretanto, a DAG não chama `src/run_all_pipeline.py` e não possui uma tarefa
específica que invoque `push_pipeline_metrics`. Portanto:

- os logs Delta são produzidos normalmente;
- as métricas agregadas `data_pipeline_*` não são publicadas pela DAG atual;
- o Pushgateway pode continuar exibindo valores de uma execução monolítica
  anterior;
- alertas de freshness e status podem refletir informação antiga.

A variável `AIRFLOW_PROMETHEUS_PUSHGATEWAY_URL` é repassada ao container como
`PROMETHEUS_PUSHGATEWAY_URL`, mas isso não gera push sem uma chamada explícita
ao publicador.

Uma evolução possível é adicionar uma tarefa final de consolidação de métricas
e uma callback de falha. Essa mudança precisa preservar a publicação mesmo
quando uma tarefa intermediária falhar.

## 23. Logging da aplicação

Além das tabelas e métricas, os módulos usam o logger compartilhado
`utils.logger.log`, chamado `Extrator`.

O formato padrão é:

```text
YYYY-MM-DD HH:MM:SS [LEVEL]: mensagem
```

O nível global é `INFO`. Writers registram início e conclusão; falhas de envio
ao Pushgateway aparecem como `WARNING` com stack trace.

Esse logging é útil para acompanhamento imediato, mas não há no módulo atual
um handler dedicado a arquivo, JSON estruturado ou plataforma centralizada de
logs.

## 24. Tratamento de falhas

### 24.1 Falha ao escrever Delta

Os writers não capturam exceções de Spark ou S3. A falha é propagada ao
chamador e pode marcar a etapa como malsucedida. Isso favorece consistência da
auditoria, mas significa que indisponibilidade do bucket de observabilidade
pode interromper o processamento.

### 24.2 Falha ao publicar métricas

O publicador Prometheus captura qualquer exceção, registra warning e retorna.
Observabilidade de métricas é tratada como best effort e não derruba a carga.

### 24.3 Lista vazia de eventos

Os writers Delta retornam `None` e não criam tabela nem versão Delta. Um
consumidor deve distinguir “nenhum evento” de “tabela ainda inexistente”.

### 24.4 Falha antes de produzir eventos

O `run_all_pipeline.py` inicializa os grupos de estágio vazios. Se uma falha
ocorrer cedo, ainda pode publicar status zero e gauges de etapas com valores
zero. O detalhe da exceção não faz parte das métricas Prometheus; ele permanece
no log do processo ou em eventos já gravados.

## 25. Limitações e cuidados

### 25.1 Métricas representam o último estado

Gauges no Pushgateway não são um log de execuções. O histórico observado no
Prometheus depende da coleta contínua e da retenção configurada no servidor.
Para auditoria durável por execução, use as tabelas Delta.

### 25.2 Ausência de `run_id` nas métricas

As métricas agregadas não usam `run_id` como label. Isso evita cardinalidade
sem limite, mas impede filtrar diretamente uma execução específica no
Prometheus.

### 25.3 Séries condicionais e antigas

Como o push é aditivo e algumas séries são condicionais, valores como último
sucesso e última falha permanecem disponíveis entre execuções. Isso é útil
para histórico de estado, mas requer interpretação correta.

### 25.4 Categorias de qualidade ausentes

Um tipo de check não observado na execução não recebe necessariamente valor
zero novo. Dependendo do conteúdo mantido no Pushgateway, uma série antiga
pode continuar visível.

### 25.5 Métrica de qualidade limitada à Silver

`data_pipeline_quality_ratio` usa apenas entrada e rejeição dos eventos Silver.
Ela não é uma taxa consolidada de todos os checks do Landing, da correção e da
Gold.

### 25.6 Duração por estágio é soma de eventos

A soma das durações individuais pode ser diferente do tempo de parede se
eventos forem paralelos ou incluírem operações sobrepostas. No fluxo atual,
grande parte do processamento é sequencial, mas as duas medidas continuam com
semânticas distintas.

### 25.7 Datas e fusos

Nem todos os eventos usam a mesma origem temporal. Há datas operacionais,
`date.today()`, timestamps UTC e timestamps locais sem timezone. Consultas
comparativas devem normalizar esses campos.

### 25.8 Crescimento das tabelas Delta

Os logs usam append e não possuem política de retenção, compactação ou vacuum
específica no módulo. Com o tempo, podem exigir manutenção e otimização.

### 25.9 Segurança

As configurações fornecidas não habilitam TLS ou autenticação para Prometheus,
Pushgateway e endpoint `/metrics`. Não exponha diretamente as portas 8000,
9090, 9091 ou 3000 à internet.

## 26. Solução de problemas

### 26.1 Tabela Delta não existe

Confirme se a etapa produziu ao menos um evento e se o bucket
`observability` existe. Writers não criam a tabela para listas vazias.

### 26.2 Erro de schema no DataFrame

Compare o dicionário gerado com o schema correspondente em
`src/schemas/schemas.py`. Verifique campos obrigatórios, tipos numéricos,
timestamps e datas.

### 26.3 Pushgateway sem métricas do pipeline

Verifique:

1. se `PROMETHEUS_PUSHGATEWAY_URL` está definido no processo do pipeline;
2. se foi executado `run_all_pipeline.py`, e não apenas a DAG segmentada;
3. se a porta 9091 está acessível;
4. se o log contém confirmação ou warning de envio;
5. se o job consultado coincide com `PROMETHEUS_JOB_NAME`.

### 26.4 Target da API aparece `DOWN`

Abra `/metrics` diretamente a partir da máquina ou container onde o Prometheus
executa. Confirme host, porta, firewall e o arquivo de configuração escolhido.

### 26.5 Target do Pushgateway aparece `DOWN`

Em execução local, valide `127.0.0.1:9091`. Em Docker, confirme rede, nome DNS
`data-platform-pushgateway` e porta interna 9091.

### 26.6 Dashboard sem dados

Confira:

- datasource com UID `prometheus`;
- targets no estado `UP`;
- janela de tempo correta;
- existência das séries no Prometheus;
- label `job="data_platform_pipeline"`;
- execução prévia do pipeline monolítico.

### 26.7 Alertas ativos sem notificação

Isso é esperado sem Alertmanager. As regras atuais apenas avaliam o estado no
Prometheus.

### 26.8 Alerta de freshness permanente

Compare o limite de 25 horas com a periodicidade real. Para uma DAG mensal, a
regra deve usar uma janela coerente com o calendário e a tolerância esperada.

### 26.9 Prometheus não carrega as regras

`rule_files` usa caminho relativo. Execute Prometheus a partir do diretório
correto, monte os arquivos juntos no container ou ajuste o caminho. Valide com:

```powershell
promtool check config config/prometheus/prometheus-local.yml
promtool check rules config/prometheus/prometheus-alerts.yml
```

## 27. Recomendações para evolução

- criar uma tarefa Airflow específica para consolidar e publicar métricas;
- adicionar callback de falha para execuções interrompidas;
- alinhar o alerta de freshness à agenda mensal;
- configurar Alertmanager e rotas de notificação;
- incluir limites inferior e superior da janela nos logs de ingestão;
- registrar contagem de registros já na ingestão;
- definir retenção, compactação e `VACUUM` para as tabelas Delta;
- padronizar timestamps em UTC;
- publicar uma métrica de sucesso da própria entrega de telemetria;
- evitar séries antigas de checks ausentes com uma estratégia explícita de
  limpeza ou publicação de zeros;
- proteger endpoints e interfaces com autenticação, rede privada e TLS;
- adicionar testes de contrato dos schemas e das métricas;
- provisionar automaticamente datasource e dashboard do Grafana.

## 28. Checklist operacional

Antes da execução:

- bucket `observability` disponível;
- credenciais S3/MinIO válidas;
- Spark e Delta configurados;
- API expondo `/metrics`;
- Pushgateway acessível pelo pipeline;
- Prometheus usando o arquivo adequado ao ambiente;
- datasource Grafana com UID correto.

Depois da execução:

- eventos presentes nas tabelas Delta esperadas;
- status e mensagens coerentes com o resultado;
- métricas presentes no Pushgateway;
- targets Prometheus em `UP`;
- regras carregadas sem erro;
- dashboard dentro da janela temporal correta;
- alertas compatíveis com a frequência real do pipeline.

## 29. Resumo

A observabilidade da plataforma combina detalhe durável e monitoramento
imediato:

- Delta Lake registra o histórico granular do processamento;
- Prometheus acompanha o estado agregado e a API;
- Pushgateway mantém métricas do job batch disponíveis após seu término;
- Grafana apresenta os indicadores;
- regras Prometheus identificam falha, atraso, SLA e degradação de qualidade.

Para investigações, comece pelo `run_id` nas tabelas Delta. Para operação em
tempo real, use Prometheus, alertas e dashboard. A correlação entre os dois
mecanismos fornece a visão mais completa do pipeline.
