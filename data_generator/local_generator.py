import argparse
import csv
from datetime import datetime, timedelta, timezone
from decimal import Decimal
import io
import json
from pathlib import Path
import random
import uuid

try:
    from data_generator.schema_definitions import CSV_HEADERS, JSON_SCHEMAS
    from data_generator.synthetic_profiles import device
    from data_generator.reference_data import load_references, validate_generated
except ModuleNotFoundError:
    from schema_definitions import CSV_HEADERS, JSON_SCHEMAS
    from synthetic_profiles import device
    from reference_data import load_references, validate_generated

ROOT = Path(__file__).resolve().parents[1]
DATA = ROOT / 'local_data_source'
CSV_NAMES = ('coupons', 'delivery_tracking', 'payments')


def stamp(value):
    if value.tzinfo is None:
        raise ValueError('Timestamp sem fuso horário não pode ser convertido com segurança.')
    return value.astimezone(timezone.utc).isoformat(timespec='microseconds').replace('+00:00', 'Z')


def read_sources():
    rows, headers = {}, {}
    for name in CSV_NAMES:
        headers[name] = list(CSV_HEADERS[name])
        path = DATA / f'{name}.csv'
        content = path.read_text(encoding='utf-8-sig') if path.exists() else ''
        reader = csv.DictReader(io.StringIO(content)) if content.strip() else None
        if reader and set(reader.fieldnames or []) - set(headers[name]):
            raise ValueError(f'{path.name}: colunas desconhecidas; arquivo preservado.')
        rows[name] = list(reader) if reader else []
        if any(None in row or any(value is None for value in row.values()) for row in rows[name]):
            raise ValueError(f'{path.name}: linha CSV com quantidade incorreta de campos.')
    path = DATA / 'website_events.json'
    content = path.read_text(encoding='utf-8-sig').strip() if path.exists() else ''
    events = json.loads(content) if content else []
    if isinstance(events, dict):
        events = [events]
    if not isinstance(events, list) or any(not isinstance(row, dict) for row in events):
        raise ValueError('website_events.json deve conter uma lista de objetos.')
    rows['website_events'] = events
    return rows, headers


def next_id(rows, field):
    return max((int(row[field]) for row in rows if row.get(field) not in (None, '')), default=0) + 1


def generate_payments(rng, count, existing, now, references):
    audit = dict(created_at=stamp(now), updated_at=stamp(now))
    run = uuid.uuid4().hex
    payments = []
    payment_id = next_id(existing, 'payment_id')
    already_paid = {int(r['order_id']) for r in existing if r.get('order_id')}
    candidates = [r for r in references['orders'] if r['order_id'] not in already_paid]
    selected_orders = rng.sample(candidates, min(count, len(candidates)))
    for i, order in enumerate(selected_orders):
        payment_status = {'pending': 'PENDING', 'cancelled': 'CANCELLED', 'refunded': 'REFUNDED'}.get(order['status'], 'APPROVED')
        paid = payment_status in ('APPROVED', 'REFUNDED')
        method = rng.choices(['PIX', 'CREDIT_CARD', 'DEBIT_CARD', 'BOLETO'], [40, 45, 5, 10])[0]
        moment = order['order_date']
        amount = Decimal(str(order['total_amount']))
        payments.append(dict(payment_id=payment_id + i, order_id=order['order_id'],
            transaction_reference=f'SIM-{run}-{i}', payment_method=method,
            payment_status=payment_status, amount=f'{amount:.2f}', currency=order['currency'],
            installments=rng.choice([1, 2, 3, 6]) if method == 'CREDIT_CARD' else 1,
            provider=rng.choice(['sim_gateway_aurora', 'sim_gateway_horizonte', 'sim_gateway_integra']), authorized_at=stamp(moment) if paid else '',
            paid_at=stamp(min(now, moment + timedelta(minutes=1))) if paid else '', refunded_amount=f'{amount:.2f}' if payment_status == 'REFUNDED' else '0.00',
            failure_code='PAYMENT_DECLINED' if payment_status == 'DECLINED' else '',
            failure_reason='Pagamento não autorizado na simulação' if payment_status == 'DECLINED' else '', **audit))
    return payments


def generate_delivery_tracking(rng, count, existing, now, references):
    audit = dict(created_at=stamp(now), updated_at=stamp(now))
    tracking = []
    tracking_id = next_id(existing, 'tracking_id')
    seen_tracking = {(int(r['order_id']), r['status']) for r in existing if r.get('order_id')}
    shippable = [r for r in references['orders'] if r['status'] in ('shipped', 'delivered') and (r['order_id'], r['status'].upper()) not in seen_tracking]
    for order in rng.sample(shippable, min(count, len(shippable))):
        tracking_status = order['status'].upper()
        occurred = order['delivered_at'] if tracking_status == 'DELIVERED' else order['updated_at']
        if occurred is None:
            raise ValueError('Pedido entregue sem delivered_at no MySQL.')
        tracking.append(dict(tracking_id=tracking_id + len(tracking),
            order_id=order['order_id'], shipment_id=order['order_id'], tracking_code=f"SIM{order['order_id']:012}",
            carrier='Transportes Simulados Brasil', service_level='standard', status=tracking_status,
            event_description='Entrega concluída' if tracking_status == 'DELIVERED' else 'Remessa despachada',
            city=order['shipping_city'], state=order['shipping_state'], country_code=order['shipping_country_code'],
            estimated_delivery_at=stamp(order['order_date'] + timedelta(days=5)), occurred_at=stamp(occurred), **audit))
    return tracking


def generate_coupons(rng, count, now):
    audit = dict(created_at=stamp(now), updated_at=stamp(now))
    run = uuid.uuid4().hex
    coupons = []
    for i in range(count):
        start = now.date()
        discount_type = rng.choice(['percentage', 'fixed'])
        coupons.append(dict(coupon=f'SIM{run[:24]}{i:05}', description='Campanha promocional sintética',
            discount=f'{rng.choice([5, 10, 15]) if discount_type == "percentage" else rng.choice([10, 20, 30]):.2f}', discount_type=discount_type, currency='BRL',
            minimum_order_amount=f'{rng.choice([100, 150, 200]):.2f}', maximum_discount_amount='50.00', usage_limit=rng.choice([100, 500, 1000, 5000]),
            usage_limit_per_customer=rng.choice([1, 1, 2]), is_active='true', start_date=start.isoformat(),
            end_date=(start + timedelta(days=rng.choice([7, 14, 30, 60]))).isoformat(), **audit))
    return coupons


def generate_website_events(rng, count, existing, now, references, campaigns=()):
    audit = dict(created_at=stamp(now), updated_at=stamp(now))
    events = []
    event_id = next_id(existing, 'event_id')

    while len(events) < count:
        session = f'sim-{uuid.uuid4().hex}'
        customer = rng.choice(references['customers'])['customer_id'] if rng.random() < 0.6 else None
        anonymous = None if customer else uuid.uuid4().hex
        product_row = rng.choice(references['products'])
        product = product_row['product_id']
        device_type, browser, system = device(rng)
        source = rng.choice(['organic', 'direct', 'paid_search'])
        active = [c for c in campaigns if c.get('status') == 'active'
                  and c['start_date'] <= now.date().isoformat() <= c['end_date']]
        campaign = rng.choice(active) if active and rng.random() < 0.35 else None
        if campaign:
            source = campaign['channel']
        moment = now - timedelta(seconds=rng.randint(600, 86400))
        created_dates = [product_row['created_at']]
        if customer is not None:
            created_dates += [r['created_at'] for r in references['customers'] if r['customer_id'] == customer]
        moment = max(moment, *created_dates)
        if campaign:
            moment = max(moment, datetime.fromisoformat(campaign['start_date']).replace(tzinfo=timezone.utc))
        steps = rng.choices([1, 2, 3], [65, 25, 10])[0]
        for step in range(min(steps, count - len(events))):
            event, page = [('page_view', f'/products/{product}'), ('add_to_cart', '/cart'), ('checkout_started', '/checkout')][step]
            events.append(dict(event_id=event_id + len(events),
                customer_id=customer, anonymous_id=anonymous, session_id=session, event=event, page=page,
                timestamp=stamp(min(now, moment + timedelta(seconds=step * 60))), product_id=product, order_id=None,
                device_type=device_type, browser=browser, operating_system=system,
                traffic_source=source, utm_source=campaign.get('utm_source') if campaign else None,
                utm_medium=campaign.get('utm_medium') if campaign else None, utm_campaign=campaign.get('utm_campaign') if campaign else None,
                country_code='BR', **audit))
    return events


def choose_volume(rng, average):
    anomaly = rng.random() < 0.1
    low, high = (0.1, 0.4) if anomaly else (0.9, 1.1)
    total = rng.randint(max(10, round(average * low)), max(10, round(average * high)))
    return anomaly, dict(total=total, payments=max(1, round(total*0.1)), coupons=max(1, round(total*0.002)))


def generate(rng, average, existing, now, references):
    anomaly, counts = choose_volume(rng, average)
    batch = dict(coupons=generate_coupons(rng, counts['coupons'], now),
                 payments=generate_payments(rng, counts['payments'], existing['payments'], now, references),
                 delivery_tracking=generate_delivery_tracking(rng, counts['payments'], existing['delivery_tracking'], now, references))
    batch['website_events'] = generate_website_events(rng, counts['total']-sum(map(len, batch.values())), existing['website_events'], now, references)
    return anomaly, batch


def save_table(name, existing, rows, headers):
    if name == 'website_events':
        from jsonschema import Draft202012Validator, FormatChecker
        schema = JSON_SCHEMAS['website_events']
        Draft202012Validator(schema, format_checker=FormatChecker()).validate(rows)
        filename = 'website_events.json'
        content = json.dumps(existing + rows, ensure_ascii=False, indent=2, allow_nan=False) + '\n'
    elif name in CSV_NAMES:
        stream = io.StringIO(newline='')
        writer = csv.DictWriter(stream, fieldnames=headers[name])
        writer.writeheader()
        writer.writerows(existing + rows)
        filename, content = f'{name}.csv', stream.getvalue()
    else:
        raise ValueError('Dataset local desconhecido.')
    DATA.mkdir(parents=True, exist_ok=True)
    with (DATA / filename).open('w', encoding='utf-8', newline='') as output:
        output.write(content)


def save(existing, batch, headers):
    from jsonschema import Draft202012Validator, FormatChecker
    schema = JSON_SCHEMAS['website_events']
    Draft202012Validator(schema, format_checker=FormatChecker()).validate(batch['website_events'])
    documents = {}
    for name in CSV_NAMES:
        stream = io.StringIO(newline='')
        writer = csv.DictWriter(stream, fieldnames=headers[name])
        writer.writeheader()
        writer.writerows(existing[name] + batch[name])
        documents[f'{name}.csv'] = stream.getvalue()
    documents['website_events.json'] = json.dumps(existing['website_events'] + batch['website_events'], ensure_ascii=False, indent=2, allow_nan=False) + '\n'

    DATA.mkdir(parents=True, exist_ok=True)
    for filename, content in documents.items():
        with (DATA / filename).open('w', encoding='utf-8', newline='') as stream:
            stream.write(content)


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument('--average-records', type=int, default=2000)
    parser.add_argument('--seed', type=int)
    parser.add_argument('--dry-run', action='store_true', help='Gera em memória, sem alterar os arquivos.')
    args = parser.parse_args()
    if not 100 <= args.average_records <= 100000:
        parser.error('--average-records deve estar entre 100 e 100000.')
    existing, headers = read_sources()
    references = load_references()
    anomaly, batch = generate(random.Random(args.seed), args.average_records, existing, datetime.now(timezone.utc), references)
    validate_generated(batch, references)
    if not args.dry_run:
        save(existing, batch, headers)
    print(json.dumps(dict(anomalous=anomaly, dry_run=args.dry_run,
        total_records=sum(map(len, batch.values())), records={name: len(rows) for name, rows in batch.items()}), indent=2))


if __name__ == '__main__':
    main()
