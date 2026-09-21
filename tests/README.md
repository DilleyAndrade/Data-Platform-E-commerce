# Testes automatizados

A suíte está separada pelo processo testado:

```text
tests/
|-- api_data_platform/
|-- data_generator/
|-- ingestion/
|-- data_quality/
|-- data_correction/
|-- transformations/
|-- observability/
|-- schemas/
|-- utils/
`-- orchestration/
```

Os testes são unitários e de contrato. Serviços externos são substituídos por
mocks; não é necessário iniciar Docker, Airflow, MinIO, bancos ou Pushgateway.

Na raiz do projeto:

```powershell
.\.venv\Scripts\Activate.ps1
python -m pytest tests -q
```

Para executar um processo específico:

```powershell
python -m pytest tests/ingestion -q
python -m pytest tests/transformations -q
```

Use `-vv` para identificar cada cenário e `--maxfail=1` para interromper na
primeira falha.

