import json
from pathlib import Path
from unittest.mock import patch

import pytest
from fastapi import HTTPException
from fastapi.testclient import TestClient

from api_data_platform.main import app


DATA_DIRECTORY = Path(__file__).resolve().parents[2] / "api_data_platform"
client = TestClient(app)


ENDPOINTS = (
    ("/customer-reviews", "api_customer_reviews.json"),
    ("/exchange-rates", "api_exchange_rates.json"),
    ("/marketing-campaigns", "api_marketing_campaigns.json"),
)


@pytest.mark.parametrize(("endpoint", "filename"), ENDPOINTS)
def test_data_endpoints_return_expected_json(endpoint, filename):
    expected_content = json.loads((DATA_DIRECTORY / filename).read_text(encoding="utf-8"))

    response = client.get(endpoint)

    assert response.status_code == 200
    assert response.headers["content-type"].startswith("application/json")
    assert response.json() == expected_content


@pytest.mark.parametrize(("endpoint", "_"), ENDPOINTS)
def test_data_endpoints_allow_only_get_requests(endpoint, _):
    response = client.post(endpoint)

    assert response.status_code == 405
    assert response.headers["allow"] == "GET"


@pytest.mark.parametrize(("endpoint", "_"), ENDPOINTS)
def test_data_endpoints_propagate_not_found_errors(endpoint, _):
    with patch(
        "api_data_platform.main.load_json",
        side_effect=HTTPException(404, "Arquivo de dados não encontrado."),
    ):
        response = client.get(endpoint)

    assert response.status_code == 404
    assert response.json() == {"detail": "Arquivo de dados não encontrado."}


@pytest.mark.parametrize(("endpoint", "_"), ENDPOINTS)
def test_data_endpoints_propagate_invalid_file_errors(endpoint, _):
    with patch(
        "api_data_platform.main.load_json",
        side_effect=HTTPException(500, "Arquivo de dados inválido."),
    ):
        response = client.get(endpoint)

    assert response.status_code == 500
    assert response.json() == {"detail": "Arquivo de dados inválido."}


def test_unknown_route_returns_404():
    response = client.get("/unknown-endpoint")

    assert response.status_code == 404
    assert response.json() == {"detail": "Not Found"}


def test_documentation_endpoint_is_available():
    response = client.get("/docs")

    assert response.status_code == 200
    assert response.headers["content-type"].startswith("text/html")


def test_openapi_schema_describes_the_api_contract():
    response = client.get("/openapi.json")

    assert response.status_code == 200
    schema = response.json()
    assert schema["info"] == {
        "title": "Data Platform API",
        "description": "API para consulta dos dados JSON da plataforma.",
        "version": "1.0.0",
    }

    for endpoint, _ in ENDPOINTS:
        operation = schema["paths"][endpoint]["get"]
        assert operation["tags"] == ["Dados"]
        assert operation["responses"]["200"]["description"] == "Successful Response"
