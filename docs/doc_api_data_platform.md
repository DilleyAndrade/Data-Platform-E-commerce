# Data Platform API

## Visão geral

Esta API atua como uma fonte de dados simulada para a Data Platform. Seu
objetivo é disponibilizar arquivos JSON por meio de endpoints HTTP, reproduzindo
o comportamento de uma API externa que seria consumida em um processo de
ingestão.

Os dados retornados pelos endpoints devem ser consultados por um script de
ingestão e enviados para uma camada de armazenamento de objetos, como Amazon S3
ou MinIO. Dessa forma, o componente permite desenvolver, validar e demonstrar o
fluxo de extração de dados de uma API até o data lake sem dependência de uma
fonte externa real.

## Endpoints disponíveis

| Método | Endpoint | Dados disponibilizados |
| --- | --- | --- |
| `GET` | `/customer-reviews` | Avaliações de clientes. |
| `GET` | `/exchange-rates` | Taxas de câmbio. |
| `GET` | `/marketing-campaigns` | Campanhas de marketing. |

Cada endpoint retorna o respectivo arquivo de dados no formato JSON.

## Inicialização da API

Execute os comandos a partir do diretório raiz do projeto. Caso necessário,
instale primeiro as dependências:

```bash
pip install -r requirements.txt
```

### Execução na porta padrão

Inicie o servidor com:

```bash
uvicorn api_data_platform.main:app --reload
```

Por padrão, o Uvicorn inicia a API na porta `8000`. Os endpoints estarão
disponíveis em `http://127.0.0.1:8000`, por exemplo:

```text
http://127.0.0.1:8000/customer-reviews
```

A documentação interativa, gerada pelo FastAPI, pode ser acessada em:

```text
http://127.0.0.1:8000/docs
```

### Execução em uma porta específica

Para iniciar a API em uma porta diferente, utilize o parâmetro `--port`. Exemplo
na porta `8080`:

```bash
uvicorn api_data_platform.main:app --reload --port 8080
```

Nesse caso, os endpoints estarão disponíveis em `http://127.0.0.1:8080` e a
documentação interativa em `http://127.0.0.1:8080/docs`.
