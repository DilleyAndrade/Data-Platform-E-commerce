import json
from pathlib import Path
from fastapi import HTTPException

DATA_DIRECTORY = Path(__file__).parent


def resolve_data_file(filename: str) -> Path:
    data_directory = DATA_DIRECTORY.resolve()
    file_path = (data_directory / filename).resolve()

    if not file_path.is_relative_to(data_directory):
        raise HTTPException(status_code=404, detail="Arquivo de dados não encontrado.")

    if not file_path.exists():
        raise HTTPException(status_code=404, detail="Arquivo de dados não encontrado.")
    return file_path


def load_json(filename: str):
    file_path = resolve_data_file(filename)

    try:
        with file_path.open(encoding="utf-8") as json_file:
            return json.load(json_file)
    except FileNotFoundError as error:
        raise HTTPException(
            status_code=404, detail="Arquivo de dados não encontrado."
        ) from error
    except json.JSONDecodeError as error:
        raise HTTPException(
            status_code=500, detail="Arquivo de dados inválido."
        ) from error
    except OSError as error:
        raise HTTPException(
            status_code=500, detail="Não foi possível ler o arquivo de dados."
        ) from error
