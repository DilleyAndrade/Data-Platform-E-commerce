# API Data Platform

## 1. Objetivo

A pasta `api_data_platform` implementa uma API local que simula fontes externas de dados. Ela expõe arquivos JSON por HTTP para que o processo de ingestão da plataforma possa ser desenvolvido, executado e validado sem depender de uma API de terceiros.

O componente possui duas responsabilidades distintas:

1. **Leitura HTTP:** o FastAPI publica os datasets para os consumidores.
2. **Persistência local:** os geradores de dados usam o módulo `data_store` diretamente para validar e acrescentar registros aos arquivos JSON.

A API HTTP não possui endpoints de escrita. A inclusão de dados acontece no processo gerador, dentro do mesmo sistema de arquivos.

## 2. Estrutura da pasta

```text
api_data_platform/
├── main.py
├── data_store.py
├── data_loader.py
└── dataset/
    ├── api_customer_reviews.json
    ├── api_exchange_rates.json
    └── api_marketing_campaigns.json
```

### `main.py`

É o ponto de entrada da aplicação FastAPI. O arquivo:

- cria a instância `app`;
- registra os três endpoints de consulta;
- recebe os filtros incrementais de data e hora;
- delega a leitura ao método `data_store.read_dataset`;
- instrumenta automaticamente as requisições para o Prometheus;
- publica as métricas em `/metrics`.

### `data_store.py`

Centraliza a leitura, validação e gravação dos datasets. Suas operações públicas principais são:

- `read_dataset`: lê um dataset completo ou aplica uma janela incremental sobre `updated_at`;
- `append_batch`: valida e acrescenta um lote de registros aos arquivos JSON.

O módulo também mantém o mapeamento entre o nome lógico de cada dataset, seu arquivo e sua chave primária.

### `data_loader.py`

Contém funções auxiliares para resolver caminhos e carregar JSON com tratamento de erros HTTP. Esse módulo pertence à implementação anterior baseada na entrega direta de arquivos e não é utilizado pelo fluxo atual de `main.py`, que consulta os dados por meio de `data_store.py`.

### `dataset/`

Armazena os dados publicados pela API. Cada arquivo contém uma lista de objetos JSON:

| Dataset lógico | Arquivo | Chave primária |
| --- | --- | --- |
| `customer_reviews` | `api_customer_reviews.json` | `review_id` |
| `exchange_rates` | `api_exchange_rates.json` | `exchange_rate_id` |
| `marketing_campaigns` | `api_marketing_campaigns.json` | `campaign_id` |

## 3. Arquitetura e fluxo do processo

```mermaid
flowchart LR
    G[Geradores de dados] -->|append_batch| S[data_store.py]
    S -->|valida e grava| J[(Arquivos JSON)]
    C[Cliente ou ingestão] -->|GET com filtros opcionais| A[FastAPI - main.py]
    A -->|read_dataset| S
    S -->|lê e filtra| J
    A -->|lista JSON| C
    A -->|métricas HTTP| M[/metrics]
    C -->|pipeline de ingestão| L[S3 ou MinIO]
```

### 3.1 Atualização dos dados

Os módulos `data_generator/api_generator.py` e `data_generator/run_all_generator.py` importam `api_data_platform.data_store` diretamente. O fluxo de escrita é:

1. O gerador cria os registros em memória.
2. Os registros são organizados em um payload com `run_id` e `datasets`.
3. `append_batch` valida a estrutura geral do lote.
4. Cada dataset é validado contra o JSON Schema correspondente, definido em `data_generator/schema_definitions.py`.
5. Regras adicionais de consistência são verificadas, como `updated_at >= created_at`.
6. IDs sequenciais, `created_at` e `updated_at` são atribuídos pelo armazenamento.
7. Os documentos completos são preparados em memória e gravados em UTF-8.
8. `flush` e `fsync` são executados antes da conclusão da operação.

Para `exchange_rates`, a combinação de `date`, `provider` e `rate_type` é usada para evitar a inclusão duplicada da mesma cotação. O resultado de `append_batch` informa quantos registros foram inseridos e ignorados por dataset.

O lock existente protege somente threads do mesmo processo. Não há lock entre processos nem journal transacional em disco; por isso, apenas um gerador deve escrever nesses arquivos por vez.

### 3.2 Atendimento de uma requisição

O fluxo de leitura é:

1. O cliente chama um endpoint `GET`.
2. O FastAPI interpreta e valida os parâmetros de data e hora.
3. O endpoint chama `read_dataset` com o nome lógico do dataset.
4. `data_store` abre o arquivo usando `utf-8-sig`, formato compatível tanto com UTF-8 puro quanto com arquivos que possuam BOM.
5. O conteúdo é convertido para uma lista de objetos.
6. Se houver filtros, cada `updated_at` é normalizado para UTC e comparado com a janela solicitada.
7. A lista resultante é serializada pelo FastAPI e devolvida ao cliente.

Os arquivos são lidos em memória a cada requisição. A implementação não possui paginação; portanto, uma consulta sem filtros devolve o dataset completo.

## 4. Endpoints

| Método | Endpoint | Conteúdo |
| --- | --- | --- |
| `GET` | `/customer-reviews` | Avaliações de produtos feitas por clientes. |
| `GET` | `/exchange-rates` | Cotações de USD e EUR em relação ao BRL. |
| `GET` | `/marketing-campaigns` | Planejamento e execução de campanhas de marketing. |
| `GET` | `/metrics` | Métricas da aplicação no formato Prometheus. |
| `GET` | `/docs` | Documentação interativa Swagger UI. |
| `GET` | `/redoc` | Documentação interativa ReDoc. |
| `GET` | `/openapi.json` | Contrato OpenAPI da aplicação. |

O endpoint `/metrics` é criado pelo `prometheus-fastapi-instrumentator` e não aparece no contrato OpenAPI.

### 4.1 Filtros incrementais

Os três endpoints de dados aceitam os mesmos parâmetros opcionais:

| Parâmetro | Regra |
| --- | --- |
| `updated_at_from` | Limite inferior inclusivo: `updated_at >= updated_at_from`. |
| `updated_at_until` | Limite superior exclusivo: `updated_at < updated_at_until`. |

Os valores devem estar no formato ISO 8601. O uso de UTC com o sufixo `Z` é recomendado:

```text
2026-09-01T00:00:00Z
```

Datas sem fuso horário são interpretadas como UTC. O intervalo semiaberto `[from, until)` permite executar janelas consecutivas sem duplicar os registros da fronteira.

Exemplo de consulta incremental:

```http
GET /customer-reviews?updated_at_from=2026-09-01T00:00:00Z&updated_at_until=2026-10-01T00:00:00Z
```

Com PowerShell:

```powershell
Invoke-RestMethod "http://127.0.0.1:8000/customer-reviews?updated_at_from=2026-09-01T00:00:00Z&updated_at_until=2026-10-01T00:00:00Z"
```

Sem parâmetros, todos os registros do arquivo são retornados. Quando nenhum registro atende ao intervalo, a resposta é uma lista vazia.

## 5. Contratos dos datasets

### Avaliações de clientes

Campos principais:

- identificação: `review_id`, `customer_id`, `product_id` e `order_id`;
- conteúdo: `rating`, `title`, `comment` e `language`;
- controle: `verified_purchase`, `moderation_status` e `helpful_votes`;
- auditoria: `created_at` e `updated_at`.

### Cotações de moedas

Campos principais:

- identificação e referência: `exchange_rate_id`, `date` e `base_currency`;
- valores: `usd_brl` e `eur_brl`;
- origem: `provider`, `rate_type` e `published_at`;
- auditoria: `created_at` e `updated_at`.

### Campanhas de marketing

Campos principais:

- identificação: `campaign_id` e `campaign`;
- planejamento: `channel`, `objective`, `status`, `start_date` e `end_date`;
- valores: `budget`, `actual_spend` e `currency`;
- segmentação: `target_audience`;
- rastreamento: `utm_source`, `utm_medium` e `utm_campaign`;
- auditoria: `created_at` e `updated_at`.

Os contratos estruturais completos estão em `data_generator/schema_definitions.py`, na constante `JSON_SCHEMAS`.

## 6. Pré-requisitos

- Python compatível com o projeto;
- ambiente virtual `.venv` criado na raiz;
- dependências de `requirements.txt` instaladas;
- porta escolhida disponível no computador.

No Windows PowerShell, a instalação pode ser feita com:

```powershell
.\.venv\Scripts\python.exe -m pip install -r requirements.txt
```

Todos os comandos seguintes devem ser executados a partir da raiz do repositório. Isso é necessário para que o módulo `api_data_platform` seja encontrado corretamente.

## 7. Executar na porta padrão

Quando `--host` e `--port` são omitidos, o Uvicorn utiliza `127.0.0.1:8000`:

```powershell
.\.venv\Scripts\python.exe -m uvicorn api_data_platform.main:app
```

Endereços principais:

- API: `http://127.0.0.1:8000`;
- Swagger: `http://127.0.0.1:8000/docs`;
- ReDoc: `http://127.0.0.1:8000/redoc`;
- métricas: `http://127.0.0.1:8000/metrics`.

Para desenvolvimento, a recarga automática pode ser habilitada:

```powershell
.\.venv\Scripts\python.exe -m uvicorn api_data_platform.main:app --reload
```

`--reload` deve ser usado somente durante o desenvolvimento.

## 8. Executar em uma porta definida pelo projeto

A porta pode ser informada diretamente ao Uvicorn. Neste exemplo, a porta escolhida é `8081`:

```powershell
.\.venv\Scripts\python.exe -m uvicorn api_data_platform.main:app --host 127.0.0.1 --port 8081
```

Os endereços passam a usar a nova porta, por exemplo:

```text
http://127.0.0.1:8081/docs
http://127.0.0.1:8081/customer-reviews
```

Para centralizar o valor durante uma sessão do PowerShell, defina uma variável de ambiente:

```powershell
$env:API_PORT = "8081"
.\.venv\Scripts\python.exe -m uvicorn api_data_platform.main:app --host 127.0.0.1 --port $env:API_PORT
```

O Uvicorn não lê `API_PORT` automaticamente; a variável funciona porque seu valor é repassado explicitamente ao argumento `--port`.

### Ajustar os consumidores

Os processos de ingestão usam `API_BASE_URL`, cujo padrão é `http://localhost:8000`. Quando a API for iniciada em outra porta, configure os consumidores com o mesmo endereço antes de executar a ingestão:

```powershell
$env:API_BASE_URL = "http://127.0.0.1:8081"
```

No Airflow executado em container, a configuração equivalente é `AIRFLOW_API_BASE_URL`. Esse valor deve considerar a perspectiva de rede do container, por exemplo `http://host.docker.internal:8081`.

## 9. Exposição na rede

Por padrão, `127.0.0.1` aceita conexões somente da própria máquina. Para aceitar conexões em outras interfaces, é possível usar:

```powershell
.\.venv\Scripts\python.exe -m uvicorn api_data_platform.main:app --host 0.0.0.0 --port 8081
```

A API não implementa autenticação, autorização ou limitação de tráfego. A exposição em `0.0.0.0` deve ocorrer somente em ambiente controlado e com regras de firewall adequadas.

## 10. Verificação da execução

Com a porta padrão:

```powershell
Invoke-WebRequest http://127.0.0.1:8000/docs -UseBasicParsing
Invoke-RestMethod http://127.0.0.1:8000/exchange-rates
Invoke-WebRequest http://127.0.0.1:8000/metrics -UseBasicParsing
```

Os endpoints devem responder com status `200`. Um filtro de data inválido é rejeitado pelo FastAPI com status `422`.

Para encerrar o servidor iniciado no terminal, pressione `Ctrl+C`.

## 11. Observabilidade

Todas as rotas são instrumentadas pelo `prometheus-fastapi-instrumentator`. O endpoint `/metrics` expõe informações como quantidade de requisições, status HTTP e duração das chamadas.

O arquivo `config/prometheus/prometheus-local.yml` está preparado para coletar as métricas da API em `127.0.0.1:8000`. Se a porta da API for alterada, o target `data-platform-api` desse arquivo também deve ser atualizado para a porta escolhida.

## 12. Tratamento de erros e limitações

- **HTTP 422:** parâmetro de data e hora inválido.
- **HTTP 500:** falha não tratada durante a leitura, como JSON inválido no arquivo de origem.
- Um arquivo ausente é interpretado por `data_store` como um dataset vazio.
- Um JSON que não seja uma lista de objetos é rejeitado; por compatibilidade, um único objeto legado é convertido em uma lista de um item.
- Não há paginação, cache HTTP ou compressão configurada pela aplicação.
- A leitura e a escrita carregam o conteúdo dos datasets em memória.
- O lock de escrita não coordena processos diferentes.
- O componente foi projetado para desenvolvimento, testes e demonstrações locais, não como serviço público de produção.

## 13. Solução de problemas

### Porta já está em uso

Escolha outra porta com `--port` ou encerre o processo que já utiliza a porta desejada.

### `ModuleNotFoundError: api_data_platform`

Confirme que o comando está sendo executado na raiz do projeto e que está usando o Python do `.venv` correto.

### O pipeline não encontra a API

Verifique se `API_BASE_URL` aponta para o host e a porta usados pelo Uvicorn. Em containers, `localhost` aponta para o próprio container e não para o Windows host.

### Alterações no código não aparecem

Reinicie o processo da API ou use `--reload` durante o desenvolvimento.

### Alterações nos arquivos JSON

Não edite os arquivos enquanto um gerador estiver gravando. Antes de executar geração em paralelo, considere implementar lock entre processos e gravação atômica com arquivo temporário.
