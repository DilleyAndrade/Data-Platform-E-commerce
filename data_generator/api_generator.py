import argparse
from datetime import datetime, timedelta, timezone
import json
from pathlib import Path
import random
import sys
import uuid

try:
    from data_generator.reference_data import load_references, validate_generated
except ModuleNotFoundError:
    from reference_data import load_references, validate_generated

ROOT = Path(__file__).resolve().parents[1]
COMMENTS = {
    1: ('Abaixo do esperado', 'A qualidade do produto ficou abaixo da minha expectativa.'),
    2: ('Pode melhorar', 'O acabamento poderia ser melhor para essa faixa de preço.'),
    3: ('Atende ao básico', 'Produto razoável, atende às necessidades básicas.'),
    4: ('Bom produto', 'Bom acabamento e funcionamento conforme a descrição.'),
    5: ('Muito satisfeito', 'Produto de boa qualidade, atendeu muito bem às expectativas.'),
}


def generate_customer_reviews(rng, count, now, references, existing_reviews=()):
    now = now.astimezone(timezone.utc)
    timestamp = now.isoformat(timespec='microseconds').replace('+00:00', 'Z')
    audit = dict(created_at=timestamp, updated_at=timestamp)
    orders = {r['order_id']: r for r in references['orders']}
    reviewed = {(r.get('order_id'), r['product_id']) for r in existing_reviews}
    purchases = list({(i['order_id'], i['product_id']): i for i in references['items']
                      if orders[i['order_id']]['status'] == 'delivered'
                      and (i['order_id'], i['product_id']) not in reviewed}.values())
    rng.shuffle(purchases)
    reviews = []
    for i in range(count):
        rating = rng.choices([1, 2, 3, 4, 5], [4, 6, 15, 35, 40])[0]
        title, comment = COMMENTS[rating]
        aspect = rng.choice(['acabamento', 'embalagem', 'material', 'custo-benefício', 'design', 'praticidade'])
        opinion = rng.choice({
            1: ['Não correspondeu à descrição.', 'Esperava uma qualidade melhor.', 'Não voltaria a escolher este modelo.'],
            2: ['Atendeu apenas parcialmente.', 'Há pontos que precisam melhorar.', 'A experiência deixou a desejar.'],
            3: ['É suficiente para uso ocasional.', 'Está dentro do esperado.', 'Tem pontos positivos e negativos.'],
            4: ['Uma boa escolha para o dia a dia.', 'O resultado foi satisfatório.', 'Recomendo para essa finalidade.'],
            5: ['Superou minhas expectativas.', 'Recomendo a outras pessoas.', 'Escolheria novamente.'],
        }[rating])
        comment = f'{comment} Sobre {aspect}: {opinion}'
        title = rng.choice([title, f'Minha experiência com o {aspect}', 'Impressões sobre o produto'])
        purchase = purchases.pop() if purchases else None
        customer_id = orders[purchase['order_id']]['customer_id'] if purchase else rng.choice(references['customers'])['customer_id']
        product_id = purchase['product_id'] if purchase else rng.choice(references['products'])['product_id']
        reviews.append(dict(review_id=i + 1, customer_id=customer_id, product_id=product_id,
            order_id=purchase['order_id'] if purchase else None, rating=rating, title=title, comment=comment, verified_purchase=purchase is not None,
            moderation_status='published', helpful_votes=0, language='pt-BR', **audit))
    return reviews


def generate_marketing_campaigns(rng, count, now):
    now = now.astimezone(timezone.utc)
    timestamp = now.isoformat(timespec='microseconds').replace('+00:00', 'Z')
    audit = dict(created_at=timestamp, updated_at=timestamp)
    campaigns = []
    for i in range(count):
        channel, source, medium = rng.choice([('paid_search', 'google', 'cpc'), ('social', 'instagram', 'paid_social'), ('email', 'newsletter', 'email')])
        status = rng.choices(['scheduled', 'active', 'paused', 'completed'], [25, 45, 10, 20])[0]
        start = now.date() + timedelta(days=rng.randint(1, 7)) if status == 'scheduled' else now.date() - timedelta(days=rng.randint(2, 30))
        end = now.date() - timedelta(days=1) if status == 'completed' else max(start, now.date()) + timedelta(days=rng.choice([7, 14, 21, 30]))
        budget = rng.randint(10, 100) * (50 if channel == 'email' else 250)
        spend = 0 if status == 'scheduled' else round(budget * rng.uniform(0.7, 1.0) if status == 'completed' else budget * rng.uniform(0.05, 0.65), 2)
        category = rng.choice(['moda', 'casa', 'eletronicos', 'esporte'])
        campaigns.append(dict(campaign_id=i + 1, campaign=f'{category.title()} - {start.isoformat()} - ação {i + 1}',
            channel=channel, objective=rng.choice(['conversion', 'customer_acquisition', 'retention'] if channel == 'email' else ['conversion', 'customer_acquisition']),
            status=status, budget=budget, actual_spend=spend,
            currency='BRL', start_date=start.isoformat(), end_date=end.isoformat(),
            target_audience=f'Interessados em {category}', utm_source=source, utm_medium=medium,
            utm_campaign=f'sim_{category}_{uuid.uuid4().hex[:16]}', **audit))
    return campaigns


def generate_exchange_rates(now):
    now = now.astimezone(timezone.utc)
    timestamp = now.isoformat(timespec='microseconds').replace('+00:00', 'Z')
    audit = dict(created_at=timestamp, updated_at=timestamp)

    rate_rng = random.Random(now.date().toordinal())
    usd = round(5.20 + rate_rng.uniform(-0.08, 0.08), 6)
    rates = [dict(exchange_rate_id=1, date=now.date().isoformat(), base_currency='BRL',
        usd_brl=usd, eur_brl=round(usd * rate_rng.uniform(1.07, 1.10), 6),
        provider='synthetic_reference', rate_type='daily_reference', published_at=timestamp, **audit)]
    return rates


def choose_volume(rng, average):
    anomaly = rng.random() < 0.10
    low, high = (0.10, 0.40) if anomaly else (0.90, 1.10)
    total = rng.randint(max(3, round(average * low)), max(3, round(average * high)))
    campaigns = max(1, round(total * 0.005))
    return anomaly, dict(customer_reviews=total-campaigns-1, marketing_campaigns=campaigns, exchange_rates=1)


def generate(rng, average, now, references, existing_reviews=()):
    anomaly, counts = choose_volume(rng, average)
    return anomaly, dict(customer_reviews=generate_customer_reviews(rng, counts['customer_reviews'], now, references, existing_reviews),
                         marketing_campaigns=generate_marketing_campaigns(rng, counts['marketing_campaigns'], now),
                         exchange_rates=generate_exchange_rates(now))


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument('--average-records', type=int, default=2000)
    parser.add_argument('--seed', type=int, help='Reproduz o sorteio de volume; omita na rotina diária.')
    parser.add_argument('--dry-run', action='store_true', help='Gera em memória sem alterar os arquivos.')
    args = parser.parse_args()
    if not 30 <= args.average_records <= 100000:
        parser.error('--average-records deve estar entre 30 e 100000.')
    references = load_references()
    if str(ROOT) not in sys.path:
        sys.path.insert(0, str(ROOT))
    from api_data_platform.data_store import read_dataset, append_batch
    existing_reviews = read_dataset('customer_reviews')
    anomaly, datasets = generate(random.Random(args.seed), args.average_records, datetime.now(timezone.utc), references, existing_reviews)
    validate_generated(datasets, references)
    payload = dict(run_id=uuid.uuid4().hex, datasets=datasets)
    summary = dict(run_id=payload['run_id'], anomalous=anomaly, dry_run=args.dry_run,
                   generated={name: len(rows) for name, rows in datasets.items()})
    if not args.dry_run:
        summary['persisted'] = append_batch(payload)
    print(json.dumps(summary, ensure_ascii=False, indent=2))


if __name__ == '__main__':
    main()
