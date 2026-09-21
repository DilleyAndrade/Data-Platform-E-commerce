# Utilitários compartilhados da plataforma

## 1. Objetivo

A pasta `src/utils` reúne componentes reutilizados por ingestão, qualidade,
correção, transformações e observabilidade. Ela padroniza:

- argumentos dos jobs executáveis;
- criação e encerramento da sessão Spark;
- conexão Boto3 com S3 ou MinIO;
- uploads multipart;
- identificação de eventos malsucedidos;
- formato dos logs da aplicação.

Esses módulos são pequenos, mas ficam no caminho crítico de quase todo o
pipeline. Alterações neles podem afetar todos os estágios simultaneamente.

## 2. Estrutura

```text
src/utils/
|-- job.py
|-- logger.py
|-- s3_client.py
|-- s3_transfer.py
`-- spark_session.py
```

| Arquivo | Responsabilidade |
| --- | --- |
| `job.py` | Interface CLI e ciclo de vida comum dos jobs. |
| `logger.py` | Configuração básica de logging e logger compartilhado. |
| `s3_client.py` | Construção e validação do cliente Boto3. |
| `s3_transfer.py` | Política multipart para upload de objetos. |
| `spark_session.py` | Construção da sessão Spark com Delta, S3A e JDBC. |

## 3. Visão de integração

```text
Scripts de ingestão, qualidade, correção e transformação
                  |
                  v
              utils.job
             /         \
            v           v
  utils.spark_session  utils.s3_client
            |           |
            v           v
      Spark/Delta     Boto3/MinIO

Uploads diretos ------> utils.s3_transfer
Todos os módulos -----> utils.logger
```

## 4. `job.py`

`job.py` fornece quatro funções para padronizar os pontos de entrada
executáveis:

```python
job_arguments(description)
job_spark(app_name)
required_s3_client()
raise_for_failed_events(events, stage)
```

Ele é usado pelos scripts de ingestão, qualidade, correção, transformação e
commit de watermarks.

## 5. Argumentos comuns dos jobs

`job_arguments` cria um `ArgumentParser` com dois parâmetros obrigatórios:

| Argumento | Destino Python | Tipo | Finalidade |
| --- | --- | --- | --- |
| `--run-id` | `run_id` | string | Identificador da execução. |
| `--date` | `execution_date` | `datetime.date` | Data operacional. |

Exemplo:

```powershell
python src/ingestion/ingestion_api.py `
  --run-id "manual__2026-09-20" `
  --date "2026-09-20"
```

O parser usa `date.fromisoformat`. O formato recomendado é `YYYY-MM-DD`.
Argumentos ausentes ou datas inválidas fazem o `argparse` exibir ajuda e
encerrar com código diferente de zero antes da criação do Spark.

A descrição é definida por cada script, por exemplo:

```python
arguments = job_arguments("Transform Bronze data into Silver.")
```

O pipeline monolítico possui parser próprio porque seus argumentos têm padrões
e incluem `--watermark-until`.

## 6. Context manager Spark

`job_spark` encapsula a sessão em um context manager:

```python
with job_spark("bronze_to_silver") as spark:
    events = transform_bronze_silver(spark, run_id, execution_date)
```

O master é obtido de:

```text
SPARK_MASTER, padrão local[*]
```

O fluxo é:

1. chama `spark_session(app_name, master_mode)`;
2. entrega a sessão ao bloco `with`;
3. executa `spark.stop()` no `finally`.

Assim, o Spark é encerrado mesmo se o bloco lançar exceção. Se a própria
criação da sessão falhar antes do `yield`, não existe sessão para encerrar.

O context manager não captura a exceção do job; ela continua para o chamador
após a tentativa de `stop()`.

## 7. Cliente S3 obrigatório

`required_s3_client` chama `get_s3_client()` e transforma o retorno `None` em:

```text
ConnectionError: Could not create the S3/MinIO client.
```

Essa função é usada nos entry points que precisam fazer operações Boto3, como:

- ingestão local;
- ingestão da API;
- qualidade Landing;
- correção;
- Raw para Bronze.

Os módulos PostgreSQL e MySQL gravam pelo Spark/S3A e não recebem cliente Boto3
diretamente.

## 8. Detecção de eventos com falha

`raise_for_failed_events(events, stage)` procura eventos cujo status, sem
diferença entre maiúsculas e minúsculas, seja `FAILED`.

A seleção do campo é:

```python
event.get("status", event.get("execution_status", ""))
```

Isso atende dois contratos:

- ingestão usa `execution_status`;
- correção e transformações usam `status` ou estrutura equivalente.

Se houver falhas, o nome exibido segue a precedência:

```text
dataset_name -> source_table -> target_table -> file_name -> unknown
```

Exemplo de erro:

```text
RuntimeError: Bronze to Silver transformation failed for: orders, payments
```

A função não retorna uma lista de falhas e não registra o erro: retorna
normalmente quando tudo passou ou lança `RuntimeError` quando encontra falha.

Checks de qualidade usam `check_status`, não `FAILED` nesses campos. Por isso,
seus scripts precisam de lógica própria quando desejam converter um check
`FAIL` em falha de processo.

## 9. Padrão de entry point

Um script típico usa:

```python
if __name__ == "__main__":
    arguments = job_arguments("Descrição do job.")
    with job_spark("nome_da_aplicacao") as spark:
        events = process(
            spark,
            arguments.run_id,
            arguments.execution_date,
        )
    raise_for_failed_events(events, "Nome da etapa")
```

Quando Boto3 é necessário:

```python
events = process(
    spark,
    arguments.run_id,
    arguments.execution_date,
    required_s3_client(),
)
```

O check das falhas ocorre depois do bloco `with`, portanto após o encerramento
normal da sessão Spark.

## 10. `logger.py`

O módulo chama `logging.basicConfig` na importação:

```text
nível: INFO
formato: YYYY-MM-DD HH:MM:SS [LEVEL]: mensagem
```

O logger compartilhado é:

```python
log = logging.getLogger("Extrator")
```

Exemplos:

```python
log.info("Started API ingestion.")
log.warning("No supported files found in %s.", source_path)
log.exception("Failed to ingest dataset %s.", dataset)
```

`log.exception` deve ser usado dentro de um bloco de exceção; ele inclui stack
trace automaticamente.

### Comportamento de `basicConfig`

`basicConfig` configura o logging somente quando o processo ainda não possui
handlers configurados no root logger. Em ambientes como Airflow, testes ou
notebooks, o host pode ter configurado logging antes; nesse caso, formato e
nível efetivos podem ser diferentes.

O logger `Extrator` não define handler ou nível próprios. Ele herda a
configuração do root logger e propaga mensagens.

## 11. Boas práticas de logging

- use placeholders (`%s`) em vez de interpolação antecipada quando possível;
- inclua dataset, etapa e caminho para facilitar diagnóstico;
- não registre senhas, secrets ou corpos com dados pessoais;
- use `info` para ciclo normal, `warning` para degradação recuperável e
  `exception` para falhas capturadas;
- mantenha o `run_id` disponível no evento estruturado, pois o logger atual não
  o injeta automaticamente em cada mensagem.

O logging atual é textual. Não há handler JSON, rotação de arquivo, correlation
ID automático ou envio a uma plataforma centralizada.

## 12. `s3_client.py`

O módulo importa `load_dotenv()` no carregamento. Assim, tenta preencher o
ambiente a partir de `.env` antes de construir clientes.

`get_s3_client()` lê:

| Variável | Uso |
| --- | --- |
| `AWS_ENDPOINT_URL` | Endpoint customizado; se ausente, o Boto3 usa comportamento AWS padrão. |
| `AWS_ACCESS_KEY_ID` | Chave de acesso. |
| `AWS_SECRET_ACCESS_KEY` | Chave secreta. |

Configuração local típica:

```dotenv
AWS_ENDPOINT_URL=http://localhost:9000
AWS_ACCESS_KEY_ID=access_key_minio
AWS_SECRET_ACCESS_KEY=secret_key_minio
```

O endpoint é passado somente quando a variável possui valor. Não são definidos
explicitamente região, sessão, perfil, TLS, addressing style ou retries; essas
opções seguem o Boto3 e o ambiente.

## 13. Teste de conectividade S3

Depois de criar o cliente, a função chama:

```python
client.list_buckets()
```

Em sucesso, registra mensagem e retorna o cliente. Isso exige permissão global
de listagem de buckets, além das permissões específicas de objetos usadas pelo
pipeline.

Erros tratados:

| Exceção | Resultado |
| --- | --- |
| `EndpointConnectionError` | Log de serviço offline/inacessível e retorno implícito `None`. |
| `ClientError` | Log de credencial/permissão e retorno implícito `None`. |

Outras exceções, como configuração inválida fora dessas classes, não são
capturadas e podem propagar.

O log de erro de conexão inclui o endpoint, mas nunca deve incluir as
credenciais.

## 14. Boto3 versus S3A

Existem dois clientes independentes:

- Boto3, criado por `get_s3_client`, para upload, cópia, listagem e exclusão;
- conector S3A, configurado na sessão Spark, para DataFrames e Delta Lake.

As duas integrações usam as mesmas variáveis de endpoint e credenciais, mas
uma conexão Boto3 bem-sucedida não garante que o S3A esteja corretamente
configurado, e vice-versa.

## 15. `s3_transfer.py`

O módulo expõe uma configuração Boto3 reutilizável:

```python
MULTIPART_CHUNK_SIZE = 16 * 1024 * 1024

S3_TRANSFER_CONFIG = TransferConfig(
    multipart_threshold=MULTIPART_CHUNK_SIZE,
    multipart_chunksize=MULTIPART_CHUNK_SIZE,
    max_concurrency=4,
    use_threads=True,
)
```

| Opção | Valor | Efeito |
| --- | ---: | --- |
| Threshold multipart | 16 MiB | Objetos a partir desse limite usam multipart. |
| Tamanho da parte | 16 MiB | Cada parte possui esse tamanho aproximado. |
| Concorrência | 4 | Até quatro operações paralelas. |
| Threads | Habilitadas | Transferências paralelas usam threads. |

A configuração é usada por:

- upload dos arquivos locais;
- streaming da API para Landing;
- cópias feitas pela qualidade entre Landing, Raw e Quarantine.

Ela não controla gravações Spark/S3A.

## 16. Implicações do multipart

- arquivos pequenos usam upload simples;
- arquivos grandes podem ter maior throughput;
- maior concorrência aumenta conexões e consumo de memória/rede;
- falhas de upload multipart dependem da política interna do Boto3;
- o limite é constante de código, não variável de ambiente;
- o MinIO precisa aceitar operações multipart.

O uso de quatro threads é uma escolha conservadora para execução local. Cargas
maiores podem exigir medição antes de alterar partes ou concorrência.

## 17. `spark_session.py`

`spark_session(app_name, master_mode)` constrói a sessão central usada pelos
jobs. O nome e o master são recebidos do chamador.

O warehouse padrão é definido por `DEFAULT_SPARK_WAREHOUSE`:

```text
s3a://observability/spark-warehouse/
```

Pode ser substituído por `SPARK_WAREHOUSE_DIR`.

## 18. Pacotes Spark

`SPARK_PACKAGES` fixa quatro artefatos Maven:

| Pacote | Versão | Finalidade |
| --- | --- | --- |
| `io.delta:delta-spark_2.12` | 3.1.0 | Delta Lake. |
| `org.apache.hadoop:hadoop-aws` | 3.3.4 | Conector S3A. |
| `com.mysql:mysql-connector-j` | 8.0.33 | JDBC MySQL. |
| `org.postgresql:postgresql` | 42.7.3 | JDBC PostgreSQL. |

Na primeira execução, o Spark/Ivy pode precisar baixar esses pacotes. Rede
bloqueada ou cache Maven inconsistente pode impedir a criação da sessão.

As versões Python declaradas são `pyspark==3.5.1` e
`delta-spark==3.1.0`. O sufixo Scala dos jars é 2.12.

## 19. Delta Lake

A sessão configura:

```text
spark.sql.extensions = io.delta.sql.DeltaSparkSessionExtension
spark.sql.catalog.spark_catalog = org.apache.spark.sql.delta.catalog.DeltaCatalog
```

Sem essas opções, operações como `DeltaTable.forPath`, `MERGE` e escrita em
formato Delta podem falhar ou não ter o comportamento esperado.

O warehouse Spark é configurado em `spark.sql.warehouse.dir`.

## 20. Paralelismo Spark

| Variável | Config Spark | Padrão |
| --- | --- | --- |
| `SPARK_SHUFFLE_PARTITIONS` | `spark.sql.shuffle.partitions` | `4` |
| `SPARK_DEFAULT_PARALLELISM` | `spark.default.parallelism` | `4` |

Os padrões são adequados a desenvolvimento local e volumes modestos. Em
cluster ou grandes cargas, quatro partições podem limitar throughput; números
muito altos podem criar excesso de arquivos pequenos e overhead.

O master utilizado pelos scripts é definido por `SPARK_MASTER`, padrão
`local[*]`. O pipeline monolítico atual chama diretamente
`spark_session("ingestion_pipeline", "local[*]")` e não consulta essa variável.

## 21. Configuração S3A

| Configuração | Fonte/valor |
| --- | --- |
| `spark.hadoop.fs.s3a.endpoint` | `AWS_ENDPOINT_URL` |
| `spark.hadoop.fs.s3a.access.key` | `AWS_ACCESS_KEY_ID` |
| `spark.hadoop.fs.s3a.secret.key` | `AWS_SECRET_ACCESS_KEY` |
| `spark.hadoop.fs.s3a.path.style.access` | `true` |
| `spark.hadoop.fs.s3a.ssl.channel.mode` | `default_jsse` |
| `spark.hadoop.fs.s3a.impl` | `org.apache.hadoop.fs.s3a.S3AFileSystem` |

Path-style é especialmente importante para MinIO. O esquema usado nos paths
Spark é `s3a://`, enquanto logs e caminhos de apresentação podem usar
`s3://`.

`AWS_ENDPOINT_URL` precisa incluir o protocolo e a porta correta, por exemplo
`http://localhost:9000`.

## 22. Otimizações Delta

| Variável | Configuração | Padrão |
| --- | --- | --- |
| `SPARK_DELTA_OPTIMIZE_WRITE` | `spark.databricks.delta.optimizeWrite.enabled` | `false` |
| `SPARK_DELTA_AUTO_COMPACT` | `spark.databricks.delta.autoCompact.enabled` | `false` |

O tamanho máximo configurado para auto compact é 128 MiB:

```text
spark.databricks.delta.autoCompact.maxFileSize = 134217728
```

Os valores de ambiente são passados como texto. Habilitar otimizações deve ser
testado na distribuição Delta usada, pois suporte e comportamento podem variar.

## 23. Ajuste específico do Windows

Após criar a sessão, `_suppress_windows_temp_cleanup_warning` verifica
`os.name == "nt"`. No Windows, tenta ajustar o logger Java
`org.apache.spark.SparkEnv` para `ERROR`.

O objetivo é reduzir warnings de limpeza de arquivos temporários ao encerrar o
Spark. Qualquer falha nessa configuração é ignorada para não impedir o job.

Isso não desabilita todos os logs Spark nem corrige falhas reais de filesystem;
apenas altera o nível desse logger específico.

## 24. Variáveis de ambiente consolidadas

| Variável | Padrão | Consumidor |
| --- | --- | --- |
| `SPARK_MASTER` | `local[*]` | `job_spark` |
| `SPARK_WAREHOUSE_DIR` | `s3a://observability/spark-warehouse/` | Sessão Spark |
| `SPARK_SHUFFLE_PARTITIONS` | `4` | Sessão Spark |
| `SPARK_DEFAULT_PARALLELISM` | `4` | Sessão Spark |
| `SPARK_DELTA_OPTIMIZE_WRITE` | `false` | Sessão Spark e Bronze |
| `SPARK_DELTA_AUTO_COMPACT` | `false` | Sessão Spark |
| `AWS_ENDPOINT_URL` | Sem padrão | Boto3 e S3A |
| `AWS_ACCESS_KEY_ID` | Sem padrão | Boto3 e S3A |
| `AWS_SECRET_ACCESS_KEY` | Sem padrão | Boto3 e S3A |

`PYSPARK_PYTHON` e `PYSPARK_DRIVER_PYTHON`, embora presentes em
`.env.example`, não são lidos diretamente pelo código; o próprio PySpark usa
essas variáveis.

## 25. Execução de teste

Na raiz do projeto:

```powershell
.\.venv\Scripts\Activate.ps1
$env:PYTHONPATH = "$PWD\src;$PWD"
$env:AWS_ENDPOINT_URL = "http://localhost:9000"
$env:AWS_ACCESS_KEY_ID = "access_key_minio"
$env:AWS_SECRET_ACCESS_KEY = "secret_key_minio"
```

### Testar Boto3

```powershell
python -c "from utils.s3_client import get_s3_client; c=get_s3_client(); print([b['Name'] for b in c.list_buckets()['Buckets']] if c else 'sem conexão')"
```

### Testar Spark, Delta e S3A

```powershell
python -c "from utils.spark_session import spark_session; s=spark_session('utils_check','local[*]'); print(s.version); print(s.range(3).count()); s.stop()"
```

### Conferir configurações

```python
print(spark.sparkContext.master)
print(spark.conf.get("spark.sql.shuffle.partitions"))
print(spark.conf.get("spark.sql.warehouse.dir"))
print(spark.conf.get("spark.hadoop.fs.s3a.endpoint"))
```

Não imprima access key ou secret durante diagnósticos.

## 26. Uso em testes automatizados

As funções de negócio recebem dependências como `spark` e, em vários casos,
`s3_client`, o que permite substituir clientes por doubles ou mocks.

Cuidados:

- importar `s3_client.py` chama `load_dotenv`;
- `logger.py` pode configurar o root logger na primeira importação;
- `spark_session` sempre adiciona pacotes Maven;
- `required_s3_client` testa conexão real, portanto não é ideal em teste
  unitário sem patch;
- `job_arguments` lê `sys.argv` diretamente.

Para testes unitários, prefira testar funções de transformação com dependências
injetadas e reservar esses utilitários para testes de integração.

## 27. Tratamento de erros

| Componente | Estratégia |
| --- | --- |
| `job_arguments` | `argparse` encerra em uso inválido. |
| `job_spark` | Sempre tenta `spark.stop()` após o bloco. |
| `required_s3_client` | Converte retorno ausente em `ConnectionError`. |
| `raise_for_failed_events` | Converte eventos `FAILED` em `RuntimeError`. |
| `get_s3_client` | Captura conexão e `ClientError`; outros erros propagam. |
| `S3_TRANSFER_CONFIG` | Delega retries e falhas ao Boto3. |
| `spark_session` | Erros de Java, Maven, Delta ou S3A propagam. |
| ajuste Windows | Ignora falhas ao alterar nível do Log4j. |

## 28. Limitações e cuidados

### 28.1 Validação S3 exige `ListBuckets`

Uma credencial com acesso apenas a buckets específicos pode operar objetos,
mas falhar no teste global. Nesse caso, `get_s3_client` retorna `None` mesmo que
certas operações fossem autorizadas.

### 28.2 Retorno implícito de falha

`get_s3_client` não lança nos dois erros tratados; retorna implicitamente
`None`. Chamadores devem usar `required_s3_client` ou verificar o retorno.

### 28.3 Configuração global de logging

Importar o módulo pode configurar o root logger do processo. Em bibliotecas ou
hosts com logging próprio, isso pode causar acoplamento ou ser ignorado.

### 28.4 Segredos na configuração Spark

As credenciais ficam na configuração Hadoop da sessão. Interfaces de debug,
logs ou dumps de configuração precisam ser protegidos.

### 28.5 Pacotes fixos no código

Versões Maven são constantes e não podem ser alteradas por variável de
ambiente. Qualquer upgrade exige mudança de código e teste de compatibilidade.

### 28.6 Configuração voltada ao ambiente local

Paralelismo quatro, `local[*]`, path-style e MinIO são escolhas adequadas ao
projeto local, mas precisam ser revistas para cluster e AWS S3 real.

### 28.7 Sem timeout explícito na validação Boto3

O teste `list_buckets` usa timeouts e retries padrão do SDK. Um endpoint
inacessível pode demorar mais do que o desejado para falhar.

### 28.8 `raise_for_failed_events` depende de convenções

Eventos com outro campo ou valor, como `check_status=FAIL`, não são detectados.
Além disso, se a chave `status` existir com valor vazio, o fallback para
`execution_status` não é usado.

## 29. Troubleshooting

### `ModuleNotFoundError: utils`

Execute na raiz e configure:

```powershell
$env:PYTHONPATH = "$PWD\src;$PWD"
```

### Serviço S3/MinIO inacessível

Confirme protocolo, host, porta, processo ativo e firewall. Em container,
`localhost` aponta para o próprio container; use o endereço visto desse
ambiente, como `host.docker.internal` quando apropriado.

### Erro de credencial ou permissão

Valide access key, secret e permissão `ListAllMyBuckets`/equivalente, além de
listar, ler, gravar e excluir objetos nos buckets necessários.

### `ClassNotFoundException` JDBC ou S3A

Confira acesso ao Maven, cache Ivy, versões do Java/Spark/Scala e os artefatos
de `SPARK_PACKAGES`.

### Delta Lake não reconhecido

Confirme que a sessão veio de `utils.spark_session`, que extensões e catálogo
Delta foram aplicados e que `delta-spark` é compatível com o PySpark.

### Muitos arquivos pequenos

Revise paralelismo, número de partições, padrões de escrita e opções de
optimize write/auto compact. Faça medição antes de habilitar compactação.

### Spark não encerra

Use `job_spark` ou um `try/finally` explícito. Investigue também threads não
daemon e operações Java pendentes.

### Logs não usam o formato esperado

O processo host provavelmente configurou handlers antes do `basicConfig`.
Inspecione os handlers do root logger e a configuração do Airflow ou do teste.

### Upload multipart lento

Meça rede, CPU, tamanho dos objetos e limites do MinIO. Quatro threads e partes
de 16 MiB podem não ser ideais para todo ambiente.

## 30. Recomendações para evolução

- retornar erros S3 tipados ou oferecer modo estrito;
- validar um bucket específico em vez de exigir listagem global;
- parametrizar timeouts, retries, região e addressing style do Boto3;
- tornar tamanho e concorrência multipart configuráveis;
- mover versões Maven para configuração central validada;
- usar logging estruturado com `run_id` e estágio;
- evitar configurar o root logger dentro de módulo de biblioteca;
- mascarar secrets em qualquer dump de configuração;
- adicionar validação antecipada das variáveis Spark/S3;
- criar testes de compatibilidade entre PySpark, Delta, Hadoop AWS e JDBC;
- ampliar `raise_for_failed_events` para contratos explícitos ou tipos de
  evento;
- permitir configuração de sessão para testes sem resolução Maven.

## 31. Checklist operacional

Antes de iniciar um job:

- [ ] `PYTHONPATH` inclui `src` e a raiz;
- [ ] Java e Python usados pelo Spark estão disponíveis;
- [ ] endpoint e credenciais S3/MinIO estão corretos;
- [ ] a credencial consegue listar buckets;
- [ ] pacotes Maven estão acessíveis ou em cache;
- [ ] `SPARK_MASTER` é adequado ao ambiente;
- [ ] warehouse e buckets existem;
- [ ] `run_id` e data foram definidos.

Depois da execução:

- [ ] Spark foi encerrado;
- [ ] nenhum evento `FAILED` foi ignorado;
- [ ] logs não expuseram secrets;
- [ ] tabelas Delta e objetos esperados foram gravados;
- [ ] paralelismo e volume de arquivos estão adequados.

## 32. Resumo

`src/utils` estabelece a infraestrutura comum dos jobs. `job.py` padroniza a
interface e o ciclo de vida; `spark_session.py` integra Spark, Delta, JDBC e
S3A; `s3_client.py` fornece acesso Boto3 validado; `s3_transfer.py` controla
uploads multipart; e `logger.py` oferece logging textual compartilhado.

Ao diagnosticar um problema transversal, verifique primeiro esses utilitários:
falhas de imports, conexão, jars, sessão, credenciais e status de processo
normalmente passam por eles.
