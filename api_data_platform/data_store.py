"""Leitura e grava??o direta dos tr?s JSONs de dados, sem arquivos auxiliares."""
from datetime import datetime, timezone
from decimal import Decimal
from functools import lru_cache
import json
from pathlib import Path
import os
import threading

from jsonschema import Draft202012Validator, FormatChecker
from data_generator.schema_definitions import JSON_SCHEMAS

ROOT = Path(__file__).resolve().parents[1]
DATA = ROOT / 'api_data_platform' / 'dataset'
THREAD_LOCK = threading.Lock()
DATASETS = {
    'customer_reviews': ('api_customer_reviews.json', 'review_id'),
    'exchange_rates': ('api_exchange_rates.json', 'exchange_rate_id'),
    'marketing_campaigns': ('api_marketing_campaigns.json', 'campaign_id'),
}


@lru_cache
def validator(dataset):
    schema = JSON_SCHEMAS[dataset]
    return Draft202012Validator(schema, format_checker=FormatChecker())


def legacy_rows(dataset):
    path = DATA / DATASETS[dataset][0]
    content = path.read_text(encoding='utf-8-sig').strip() if path.exists() else ''
    if not content:
        return []
    try:
        rows = json.loads(content)
    except json.JSONDecodeError as error:
        raise ValueError(f'JSON inv?lido em {path.name}: {error.msg}. O arquivo foi preservado.') from error
    # Tamb?m aceita o objeto ?nico do arquivo original de c?mbio.
    if isinstance(rows, dict):
        rows = [rows]
    if not isinstance(rows, list) or any(not isinstance(row, dict) for row in rows):
        raise ValueError(f'{path.name} deve conter uma lista de objetos JSON.')
    return rows


def _utc_datetime(value):
    if isinstance(value, datetime):
        parsed = value
    else:
        parsed = datetime.fromisoformat(str(value).replace('Z', '+00:00'))
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def read_dataset(dataset, updated_at_from=None, updated_at_until=None):
    with THREAD_LOCK:
        rows = legacy_rows(dataset)
    if updated_at_from is None and updated_at_until is None:
        return rows
    lower = _utc_datetime(updated_at_from) if updated_at_from is not None else None
    upper = _utc_datetime(updated_at_until) if updated_at_until is not None else None
    return [
        row
        for row in rows
        if (lower is None or _utc_datetime(row['updated_at']) >= lower)
        and (upper is None or _utc_datetime(row['updated_at']) < upper)
    ]



def append_batch(payload):
    run_id = payload.get('run_id')
    datasets = payload.get('datasets')
    if not isinstance(run_id, str) or not 1 <= len(run_id) <= 64:
        raise ValueError('run_id deve ser um texto de 1 a 64 caracteres.')
    if not isinstance(datasets, dict) or (not datasets or not set(datasets).issubset(DATASETS)):
        raise ValueError('Informe somente datasets conhecidos e pelo menos um dataset.')
    if any(not isinstance(rows, list) for rows in datasets.values()):
        raise ValueError('Cada dataset deve ser uma lista.')
    if not 1 <= sum(map(len, datasets.values())) <= 100000:
        raise ValueError('O lote deve ter de 1 a 100000 registros.')
    try:
        json.dumps(payload, ensure_ascii=False, allow_nan=False)
    except ValueError as error:
        raise ValueError('Números devem ser finitos.') from error
    for dataset, rows in datasets.items():
        # Decimal evita falsos erros de multipleOf para números como 19.90.
        decimal_rows = json.loads(json.dumps(rows), parse_float=Decimal)
        error = next(validator(dataset).iter_errors(decimal_rows), None)
        if error:
            raise ValueError(f'{dataset}, campo {list(error.path)}: {error.message}')
        for row in rows:
            if datetime.fromisoformat(row['updated_at']) < datetime.fromisoformat(row['created_at']):
                raise ValueError('updated_at deve ser >= created_at.')
            if dataset == 'marketing_campaigns' and (row['end_date'] < row['start_date'] or (row.get('actual_spend') or 0) > row['budget']):
                raise ValueError('Datas ou gastos da campanha inconsistentes.')
    with THREAD_LOCK:
        now = datetime.now(timezone.utc).isoformat(timespec='microseconds').replace('+00:00', 'Z')
        result = dict(run_id=run_id, inserted={}, skipped={})
        snapshots = {}
        for dataset, rows in datasets.items():
            id_field = DATASETS[dataset][1]
            existing = legacy_rows(dataset)
            next_id = max((r.get(id_field, 0) for r in existing), default=0) + 1
            result['inserted'][dataset] = 0
            result['skipped'][dataset] = 0
            keys = {(r.get('date'), r.get('provider'), r.get('rate_type')) for r in existing} if dataset == 'exchange_rates' else set()
            for source in rows:
                row = dict(source)
                if dataset == 'exchange_rates':
                    key = (row['date'], row['provider'], row.get('rate_type'))
                    if key in keys:
                        result['skipped'][dataset] += 1
                        continue
                    keys.add(key)
                row.update({id_field: next_id, 'created_at': now, 'updated_at': now})
                existing.append(row)
                next_id += 1
                result['inserted'][dataset] += 1
            snapshots[dataset] = existing
        result['total_inserted'] = sum(result['inserted'].values())
        # Prepara os tr?s documentos antes de abrir qualquer destino para escrita.
        documents = {dataset: json.dumps(rows, ensure_ascii=False, indent=2, allow_nan=False) + '\n'
                     for dataset, rows in snapshots.items()}
        DATA.mkdir(parents=True, exist_ok=True)
        # Sem lock em disco ou journal: execute somente um gerador por vez.
        for dataset, document in documents.items():
            with (DATA / DATASETS[dataset][0]).open('w', encoding='utf-8') as stream:
                stream.write(document)
                stream.flush()
                os.fsync(stream.fileno())
    return result
