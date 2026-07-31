import json

import pytest
from fastapi import HTTPException

from api_data_platform import data_loader


@pytest.fixture
def data_directory(tmp_path, monkeypatch):
    """Isola os arquivos de teste do diretório de dados da aplicação."""
    monkeypatch.setattr(data_loader, "DATA_DIRECTORY", tmp_path)
    return tmp_path


def test_load_json_returns_decoded_content(data_directory):
    expected_content = {
        "source": "avaliações",
        "items": [{"id": 1, "approved": True}, {"id": 2, "approved": False}],
    }
    (data_directory / "valid.json").write_text(
        json.dumps(expected_content, ensure_ascii=False), encoding="utf-8"
    )

    result = data_loader.load_json("valid.json")

    assert result == expected_content


@pytest.mark.parametrize("content", ["[1, 2, 3]", '"texto"', "42", "null"])
def test_load_json_supports_all_valid_json_root_types(data_directory, content):
    (data_directory / "valid.json").write_text(content, encoding="utf-8")

    assert data_loader.load_json("valid.json") == json.loads(content)


def test_load_json_returns_404_when_file_does_not_exist(data_directory):
    with pytest.raises(HTTPException) as exception_info:
        data_loader.load_json("missing.json")

    assert exception_info.value.status_code == 404
    assert exception_info.value.detail == "Arquivo de dados não encontrado."


def test_load_json_returns_500_when_json_is_invalid(data_directory):
    (data_directory / "invalid.json").write_text('{"id": }', encoding="utf-8")

    with pytest.raises(HTTPException) as exception_info:
        data_loader.load_json("invalid.json")

    assert exception_info.value.status_code == 500
    assert exception_info.value.detail == "Arquivo de dados inválido."


def test_load_json_returns_500_when_target_is_a_directory(data_directory):
    (data_directory / "directory.json").mkdir()

    with pytest.raises(HTTPException) as exception_info:
        data_loader.load_json("directory.json")

    assert exception_info.value.status_code == 500
    assert exception_info.value.detail == "Não foi possível ler o arquivo de dados."


def test_load_json_returns_404_for_path_outside_data_directory(data_directory, tmp_path):
    external_file = tmp_path.parent / "external.json"
    external_file.write_text('{"sensitive": true}', encoding="utf-8")

    with pytest.raises(HTTPException) as exception_info:
        data_loader.load_json("../external.json")

    assert exception_info.value.status_code == 404
    assert exception_info.value.detail == "Arquivo de dados não encontrado."
