from __future__ import annotations

import argparse
from datetime import datetime, timedelta, timezone
from decimal import Decimal, ROUND_HALF_UP
import json
from pathlib import Path
import random
import re
import uuid

try:
    from data_generator.synthetic_profiles import person, address
except ModuleNotFoundError:
    from synthetic_profiles import person, address

try:
    from data_generator.schema_definitions import POSTGRES_DDL
    from data_generator.database_setup import connect_postgres
except ModuleNotFoundError:
    from schema_definitions import POSTGRES_DDL
    from database_setup import connect_postgres

ROOT = Path(__file__).resolve().parents[1]

PRODUCTS = [
    ('Camiseta algodão', 'Moda', 'Camisetas', '59.90', '0.200', '3', '25', '30'),
    ('Calça jeans', 'Moda', 'Calças', '169.90', '0.650', '5', '30', '35'),
    ('Tênis casual', 'Moda', 'Calçados', '249.90', '0.900', '13', '22', '34'),
    ('Mochila urbana', 'Acessórios', 'Mochilas', '139.90', '0.600', '10', '32', '45'),
    ('Fone Bluetooth', 'Eletrônicos', 'Áudio', '189.90', '0.150', '5', '10', '12'),
    ('Teclado sem fio', 'Informática', 'Periféricos', '159.90', '0.600', '5', '18', '45'),
    ('Mouse óptico', 'Informática', 'Periféricos', '69.90', '0.160', '6', '10', '15'),
    ('Monitor 24 polegadas', 'Informática', 'Monitores', '899.90', '4.000', '42', '15', '62'),
    ('Cafeteira elétrica', 'Casa', 'Eletroportáteis', '219.90', '1.500', '32', '22', '25'),
    ('Liquidificador', 'Casa', 'Eletroportáteis', '179.90', '2.100', '40', '22', '25'),
    ('Jogo de toalhas', 'Casa', 'Banho', '99.90', '1.200', '12', '30', '40'),
    ('Jogo de cama casal', 'Casa', 'Cama', '199.90', '1.400', '10', '30', '40'),
    ('Garrafa térmica 500 ml', 'Casa', 'Cozinha', '79.90', '0.350', '25', '9', '9'),
    ('Tapete de yoga', 'Esporte', 'Fitness', '89.90', '0.800', '15', '15', '65'),
    ('Smartphone 128 GB', 'Eletrônicos', 'Celulares', '1499.90', '0.400', '6', '10', '18'),
    ('Panela antiaderente 24 cm', 'Casa', 'Cozinha', '119.90', '1.100', '12', '28', '42'),
    ('Luminária de mesa LED', 'Casa', 'Iluminação', '99.90', '0.700', '35', '15', '20'),
    ('Kit ferramentas 32 peças', 'Ferramentas', 'Kits', '189.90', '2.500', '10', '25', '35'),
    ('Coleira ajustável', 'Pet', 'Acessórios', '39.90', '0.100', '3', '10', '15'),
    ('Cama para cachorro tamanho M', 'Pet', 'Camas', '149.90', '1.500', '20', '50', '65'),
    ('Bola de futebol', 'Esporte', 'Futebol', '89.90', '0.450', '23', '23', '23'),
    ('Quebra-cabeça 1000 peças', 'Brinquedos', 'Jogos', '79.90', '0.800', '6', '25', '35'),
    ('Livro de ficção', 'Livros', 'Literatura', '44.90', '0.350', '3', '14', '21'),
    ('Caderno universitário', 'Papelaria', 'Cadernos', '29.90', '0.400', '2', '21', '28'),
    ('Creme hidratante 200 ml', 'Beleza', 'Cuidados pessoais', '34.90', '0.250', '18', '6', '6'),
    ('SSD 1 TB', 'Informática', 'Armazenamento', '399.90', '0.080', '2', '8', '12'),
    ('Caixa de som portátil', 'Eletrônicos', 'Áudio', '229.90', '0.650', '10', '10', '22'),
]


def money(value):
    return Decimal(str(value)).quantize(Decimal('0.01'), rounding=ROUND_HALF_UP)


def choose_volume(rng, average):
    anomaly = rng.random() < 0.10
    low, high = (0.10, 0.40) if anomaly else (0.90, 1.10)
    total = rng.randint(max(3, round(average * low)), max(3, round(average * high)))
    suppliers = max(1, round(total * 0.005))
    products = max(1, round(total * 0.095))
    return anomaly, dict(customers=total - suppliers - products, suppliers=suppliers, products=products)


def generate_suppliers(rng, count, run_id):
    suppliers = []
    for i in range(count):
        name = f'Distribuidora Simulada {rng.choice(["Horizonte", "Aurora", "Integra", "Nacional"])} {i + 1}'
        suppliers.append(dict(supplier_code=f'SIM-{run_id[:16]}-{i + 1:06}', supplier_name=name,
            legal_name=name + ' Ltda.', tax_id=None, contact_name=person(rng),
            email=f'supplier.{run_id}.{i + 1}@example.com', phone=None, **address(rng),
            payment_terms_days=rng.choice([15, 30, 30, 45, 60]), lead_time_days=rng.randint(2, 15),
            is_active=True))
    return suppliers


def generate_customers(rng, count, run_id, today):
    customers = []
    for i in range(count):
        business = rng.random() < 0.05
        age = min(80, max(18, round(rng.triangular(18, 80, 32))))
        birth_date = today - timedelta(days=round(age * 365.2425) + rng.randint(0, 364))
        customers.append(dict(name=f'Comércio Simulado {person(rng)} Ltda.' if business else person(rng),
            email=f'customer.{run_id}.{i + 1}@example.com', phone=None,
            customer_type='business' if business else 'individual', tax_id=None,
            birth_date=None if business else birth_date, **address(rng),
            acquisition_channel=rng.choices(['organic', 'paid_search', 'social', 'referral', 'marketplace'],
                                            [35, 25, 20, 15, 5])[0],
            marketing_opt_in=rng.random() < 0.30, status='active'))
    return customers


def generate_products(rng, count, run_id, supplier_ids):
    if not supplier_ids:
        raise ValueError("Fornecedores devem existir antes dos produtos.")
    products = []
    for i in range(count):
        name, category, subcategory, base_price, weight, height, width, length = rng.choice(PRODUCTS)
        price = money(Decimal(base_price) * Decimal(rng.randint(85, 125)) / 100)
        products.append(dict(sku=f'SIM-{run_id}-{i + 1:06}', barcode=None,
            name=f'{name} - coleção {run_id[:8]} modelo {i + 1}',
            description=f'{name}, linha de demonstração para e-commerce. Cadastro inteiramente sintético.',
            category=category, subcategory=subcategory, brand=rng.choice(['Marca Aurora', 'Marca Horizonte', 'Marca Essencial']),

            supplier_id=rng.choice(supplier_ids), price=price,
            cost_price=money(price * Decimal(rng.randint(45, 75)) / 100), currency='BRL',
            weight_kg=Decimal(weight), height_cm=Decimal(height), width_cm=Decimal(width),
            length_cm=Decimal(length), is_active=True))
    return products


def build_batch(rng, counts, run_id, today):
    return dict(suppliers=generate_suppliers(rng, counts['suppliers'], run_id),
                customers=generate_customers(rng, counts['customers'], run_id, today),
                products=generate_products(rng, counts['products'], run_id, list(range(counts['suppliers']))))


def write_table(connection, table, rows):
    if table not in ('suppliers', 'customers', 'products'):
        raise ValueError('Tabela PostgreSQL não permitida.')
    ids = []
    key = {'suppliers': 'supplier_id', 'customers': 'customer_id', 'products': 'product_id'}[table]
    with connection.transaction():
        with connection.cursor() as cursor:
            cursor.execute("SET LOCAL TIME ZONE 'UTC'")
            for row in rows:
                cursor.execute(insert_sql(table, row, key), tuple(row.values()))
                ids.append(cursor.fetchone()[0])
    return ids


def verify_schema(cursor):
    for table in ('suppliers', 'customers', 'products'):
        ddl = POSTGRES_DDL[table]
        expected = set(re.findall(r'^    (\w+) (?:BIGINT|INTEGER|VARCHAR|CHAR|TEXT|NUMERIC|BOOLEAN|DATE|TIMESTAMPTZ)\b', ddl, re.M))
        cursor.execute('SELECT column_name, is_identity FROM information_schema.columns WHERE table_schema = %s AND table_name = %s', ('public', table))
        columns = dict(cursor.fetchall())
        missing = expected - columns.keys()
        identity = {'suppliers': 'supplier_id', 'customers': 'customer_id', 'products': 'product_id'}[table]
        if missing or columns.get(identity) != 'YES':
            raise ValueError(f'public.{table} incompatível: colunas ausentes={sorted(missing)}; {identity} deve ser IDENTITY. Aplique o novo DDL antes de executar.')


def insert_sql(table, row, returning=None):

    fields = ', '.join(f'"{field}"' for field in row)
    placeholders = ', '.join(['%s'] * len(row))
    statement = f'INSERT INTO public."{table}" ({fields}) VALUES ({placeholders})'
    if returning:
        statement += f' RETURNING "{returning}"'
    return statement


def write_batch(connection, batch):


    with connection.transaction():
        with connection.cursor() as cursor:
            cursor.execute("SET LOCAL TIME ZONE 'UTC'")
            cursor.execute("SET LOCAL lock_timeout = '30s'")
            cursor.execute("SET LOCAL statement_timeout = '120s'")
            verify_schema(cursor)
            supplier_ids = []
            for supplier in batch['suppliers']:
                cursor.execute(insert_sql('suppliers', supplier, 'supplier_id'), tuple(supplier.values()))
                supplier_ids.append(cursor.fetchone()[0])
            for table in ('customers', 'products'):
                rows = []
                for source in batch[table]:
                    row = dict(source)
                    if table == 'products':
                        row['supplier_id'] = supplier_ids[row['supplier_id']]
                    rows.append(tuple(row.values()))
                cursor.executemany(insert_sql(table, batch[table][0]), rows)


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument('--average-records', type=int, default=2000, help='Média total de registros no cenário normal.')
    parser.add_argument('--seed', type=int, help='Reproduz sorteios; omita na rotina para variar a ocorrência de anomalias.')
    parser.add_argument('--dry-run', action='store_true', help='Gera em memória e resume, sem conectar ou inserir.')
    args = parser.parse_args()
    if not 30 <= args.average_records <= 100000:
        parser.error('--average-records deve estar entre 30 e 100000.')
    rng = random.Random(args.seed)
    anomalous, counts = choose_volume(rng, args.average_records)
    now = datetime.now(timezone.utc)
    run_id = uuid.uuid4().hex
    batch = build_batch(rng, counts, run_id, now.date())
    if not args.dry_run:
        with connect_postgres() as connection:
            write_batch(connection, batch)
    print(json.dumps(dict(run_id=run_id, execution_utc=now.isoformat(), anomalous=anomalous,
                         average_records=args.average_records, total_records=sum(counts.values()),
                         records=counts, dry_run=args.dry_run), ensure_ascii=False, indent=2))


if __name__ == '__main__':
    main()
