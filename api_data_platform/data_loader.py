import json
from pathlib import Path

from fastapi import HTTPException


DATA_DIRECTORY = Path(__file__).parent


def load_json(filename: str):
   
    file_path = DATA_DIRECTORY / filename

    try:
        with file_path.open(encoding="utf-8") as json_file:
            return json.load(json_file)
    except FileNotFoundError as error:
        raise HTTPException(status_code=404, detail="Arquivo de dados não encontrado.") from error
    except json.JSONDecodeError as error:
        raise HTTPException(status_code=500, detail="Arquivo de dados inválido.") from error
