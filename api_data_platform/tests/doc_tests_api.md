# Guia de testes da Data Platform API

## Objetivo

Os testes automatizados verificam se a API disponibiliza os arquivos JSON
esperados e se responde de forma previsível quando há problemas na leitura dos
dados. A suíte foi construída com `pytest` e utiliza o `TestClient` do FastAPI
para chamar a aplicação sem iniciar um servidor Uvicorn real.

## Organização e abordagem

Os testes estão organizados em `tests/api_data_platform` e separados por
responsabilidade:

| Arquivo | Abordagem utilizada | O que é validado |
| --- | --- | --- |
| `test_data_loader.py` | Cria arquivos temporários e substitui o diretório de dados durante cada teste. | Leitura e conversão de JSON válido; suporte a objeto, lista, texto, número e `null`; tratamento de arquivo ausente, JSON inválido, diretório no lugar de arquivo e tentativa de acesso fora do diretório de dados. |
| `test_main.py` | Usa `TestClient(app)` para enviar requisições HTTP diretamente para a aplicação FastAPI. Em cenários de erro, substitui temporariamente o carregador de dados com `unittest.mock.patch`. | Respostas dos três endpoints, conteúdo JSON retornado, código e cabeçalho HTTP, bloqueio de métodos não permitidos, propagação de erros `404` e `500`, rota inexistente, Swagger (`/docs`) e contrato OpenAPI (`/openapi.json`). |

Os testes dos endpoints leem os arquivos JSON de referência da própria pasta
`api_data_platform`. Assim, eles comparam a resposta da API com o conteúdo que
deve ser entregue ao processo de ingestão para S3 ou MinIO.

## Cenários cobertos

- Sucesso na leitura dos dados de avaliações, taxas de câmbio e campanhas de
  marketing.
- Compatibilidade do carregador com todos os tipos válidos na raiz de um JSON.
- Retorno `404` para arquivos não encontrados e para tentativas de acessar
  arquivos fora do diretório permitido.
- Retorno `500` para JSON malformado ou falha de leitura do arquivo.
- Retorno `405 Method Not Allowed` quando um endpoint é chamado com `POST`.
- Retorno `404 Not Found` para rotas que não existem.
- Disponibilidade da documentação interativa e metadados da API no OpenAPI.

## Como executar

Execute os comandos a partir da raiz do projeto.

### 1. Instalar dependências de desenvolvimento

```bash
pip install -r requirements-dev.txt
```

### 2. Executar somente os testes da API

```bash
python -m pytest tests/api_data_platform -v
```

O parâmetro `-v` apresenta o resultado individual de cada teste.

### 3. Executar um arquivo ou teste específico

Para executar somente os testes do carregador:

```bash
python -m pytest tests/api_data_platform/test_data_loader.py -v
```

Para executar apenas um cenário pelo nome:

```bash
python -m pytest tests/api_data_platform/test_main.py -k "unknown_route" -v
```

## Resultado esperado

Quando a API estiver consistente, o comando principal deve terminar com uma
mensagem semelhante a:

```text
24 passed
```
