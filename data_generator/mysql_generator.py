from __future__ import annotations

import argparse
from collections import Counter
from datetime import datetime, timedelta, timezone
from decimal import Decimal, ROUND_HALF_UP
import json
from pathlib import Path
import random
import uuid

try:
    from data_generator.reference_data import load_catalog as read_postgres_catalog, load_references
except ModuleNotFoundError:
    from reference_data import load_catalog as read_postgres_catalog, load_references

try:
    from data_generator.schema_definitions import MYSQL_DDL
    from data_generator.database_setup import connect_mysql
except ModuleNotFoundError:
    from schema_definitions import MYSQL_DDL
    from database_setup import connect_mysql

ROOT = Path(__file__).resolve().parents[1]
CENT = Decimal('0.01')
WAREHOUSE = 'WH-SP-01'


def money(value):
    return Decimal(str(value)).quantize(CENT, rounding=ROUND_HALF_UP)


def choose_volume(rng, average=2000):
    anomalous = rng.random() < 0.10

    low, high = (0.10, 0.40) if anomalous else (0.90, 1.10)
    return anomalous, rng.randint(max(1, round(average * low)), max(1, round(average * high)))


def load_catalog():
    catalog = read_postgres_catalog()
    catalog['customers'] = [r for r in catalog['customers'] if r['status'] == 'active']
    catalog['products'] = [r for r in catalog['products'] if r['is_active'] and r['currency'] == 'BRL']
    for group, fields, key in [
        ('products', ('product_id', 'sku', 'name', 'price'), 'product_id'),
        ('customers', ('customer_id', 'name', 'address_line', 'postal_code', 'city', 'state', 'country_code'), 'customer_id'),
    ]:
        rows = catalog.get(group, [])
        if not rows or any(any(field not in row or row[field] is None for field in fields) for row in rows):
            raise ValueError(f'Catálogo: {group} precisa de registros com {fields}.')
        ids = [row[key] for row in rows]
        if any(type(value) is not int or value <= 0 for value in ids) or len(ids) != len(set(ids)):
            raise ValueError(f'Catálogo: IDs inválidos ou duplicados em {group}.')
    for product in catalog['products']:
        price = Decimal(str(product['price']))
        if not price.is_finite() or not 0 <= price < Decimal('1000000000000'):
            raise ValueError('Preço inválido no catálogo.')
        product['price'] = money(price)
    if len({p['sku'] for p in catalog['products']}) != len(catalog['products']):
        raise ValueError('SKUs duplicados no catálogo.')
    return catalog


def generate_orders(rng, catalog, count, now, run_id, coupons=(), coupon_usage=None):
    usage = dict(coupon_usage or {})
    batch = []
    for index in range(count):
        customer = rng.choice(catalog['customers'])
        status = rng.choices(['paid', 'pending', 'cancelled'], weights=[85, 12, 3])[0]
        order_date = now - timedelta(seconds=rng.randint(60, 24 * 3600))
        discount_rate = rng.choices([Decimal('0'), Decimal('0.05'), Decimal('0.10')], [75, 15, 10])[0]
        item_count = min(len(catalog['products']), rng.choices([1, 2, 3, 4, 5], [45, 30, 15, 7, 3])[0])
        items = []
        for line, product in enumerate(rng.sample(catalog['products'], item_count), 1):
            quantity = rng.choices([1, 2, 3], [85, 12, 3])[0]
            gross = product['price'] * quantity
            discount = money(gross * discount_rate)
            items.append(dict(line_number=line, product_id=product['product_id'], sku=product['sku'],
                              product_name=product['name'], quantity=quantity, unit_price=product['price'],
                              discount_amount=discount, tax_amount=Decimal('0.00'),
                              line_total=gross - discount, currency='BRL'))
        registered = [customer['created_at']] + [p['created_at'] for p in catalog['products'] if p['product_id'] in {i['product_id'] for i in items}]
        order_date = max(order_date, *(value.astimezone(timezone.utc).replace(tzinfo=None) for value in registered))
        subtotal = sum(i['quantity'] * i['unit_price'] for i in items)
        coupon_code = None
        eligible = [c for c in coupons if str(c.get('is_active')).lower() == 'true'
                    and c.get('currency') == 'BRL'
                    and c['start_date'] <= now.date().isoformat() <= c['end_date']
                    and subtotal >= Decimal(c.get('minimum_order_amount') or '0')
                    and (not c.get('usage_limit') or sum(n for (code, _), n in usage.items() if code == c['coupon']) < int(c['usage_limit']))
                    and (not c.get('usage_limit_per_customer') or usage.get((c['coupon'], customer['customer_id']), 0) < int(c['usage_limit_per_customer']))]
        if eligible and rng.random() < 0.25:
            coupon = rng.choice(eligible)
            discount = Decimal(coupon['discount'])
            discount = money(subtotal * discount / 100) if coupon['discount_type'] == 'percentage' else discount
            discount = min(subtotal, discount, Decimal(coupon['maximum_discount_amount']) if coupon.get('maximum_discount_amount') else subtotal)

            remaining = discount
            for position, item in enumerate(items):
                gross = item['quantity'] * item['unit_price']
                share = remaining if position == len(items)-1 else min(remaining, money(discount * gross / subtotal)) if subtotal else Decimal('0')
                item['discount_amount'] = share
                item['line_total'] = gross - share
                remaining -= share
            coupon_code = coupon['coupon']
            usage[(coupon_code, customer['customer_id'])] = usage.get((coupon_code, customer['customer_id']), 0) + 1
            order_date = max(order_date, datetime.fromisoformat(coupon['start_date']))
            if coupon.get('created_at'):
                order_date = max(order_date, datetime.fromisoformat(coupon['created_at']).astimezone(timezone.utc).replace(tzinfo=None))
        discounts = sum(i['discount_amount'] for i in items)
        shipping = Decimal('0.00') if subtotal - discounts >= 299 else rng.choice([Decimal('12.90'), Decimal('19.90'), Decimal('24.90'), Decimal('32.90')])
        order = dict(order_number=f'SIM-{run_id}-{index + 1:06}', customer_id=customer['customer_id'],
                     order_date=order_date, status=status, sales_channel=rng.choices(['web', 'app'], [55, 45])[0],
                     currency='BRL', subtotal_amount=subtotal, discount_amount=discounts,
                     shipping_amount=shipping, tax_amount=Decimal('0.00'),
                     total_amount=subtotal - discounts + shipping, coupon_code=coupon_code,
                     shipping_recipient=customer['name'], shipping_address_line=customer['address_line'],
                     shipping_postal_code=customer['postal_code'], shipping_city=customer['city'],
                     shipping_state=customer['state'], shipping_country_code=customer['country_code'],
                     delivered_at=None, cancelled_at=min(now, order_date + timedelta(seconds=30)) if status == 'cancelled' else None,
                     cancellation_reason=rng.choice(['Cancelamento solicitado pelo cliente', 'Pagamento não concluído', 'Endereço de entrega incorreto', 'Pedido duplicado pelo cliente']) if status == 'cancelled' else None)
        batch.append((order, items))
    return batch


def insert(cursor, table, row):
    columns = ', '.join(f'`{col}`' for col in row)
    placeholders = ', '.join(['%s'] * len(row))
    cursor.execute(f'INSERT INTO `{table}` ({columns}) VALUES ({placeholders})', tuple(row.values()))
    return cursor.lastrowid


def verify_schema(cursor):

    import re
    for table in ('orders', 'order_items', 'inventory'):
        ddl = MYSQL_DDL[table]
        expected = set(re.findall(r'^    (\w+) (?:BIGINT|INT|VARCHAR|CHAR|DECIMAL|DATETIME)\b', ddl, re.M))
        cursor.execute('SELECT COLUMN_NAME FROM information_schema.COLUMNS WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = %s', (table,))
        actual = {row[0] for row in cursor.fetchall()}
        if expected - actual:
            raise ValueError(f'Tabela {table} ausente/incompatível: faltam {sorted(expected - actual)}. Aplique os novos DDLs antes de executar.')
        cursor.execute('SELECT ENGINE FROM information_schema.TABLES WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = %s', (table,))
        if cursor.fetchone()[0] != 'InnoDB':
            raise ValueError(f'{table} precisa usar InnoDB para rollback seguro.')


def build_batch(rng, catalog, count, now, run_id):
    return generate_orders(rng, catalog, count, now, run_id)


def advance_orders(cursor):


    eligible = {r['order_id'] for r in load_references(allow_empty=True)['orders']}
    cursor.execute("SELECT order_id FROM orders WHERE order_number LIKE 'SIM-%' AND status = 'shipped' AND updated_at < UTC_TIMESTAMP(6) - INTERVAL 2 DAY FOR UPDATE")
    delivered = [(row[0],) for row in cursor.fetchall() if row[0] in eligible]
    if delivered:
        cursor.executemany("UPDATE orders SET status = 'delivered', delivered_at = UTC_TIMESTAMP(6) WHERE order_id = %s", delivered)
    cursor.execute("SELECT o.order_id, i.product_id, i.quantity FROM orders o JOIN order_items i ON i.order_id = o.order_id WHERE o.order_number LIKE 'SIM-%' AND o.status = 'paid' AND o.created_at < UTC_DATE() FOR UPDATE")
    grouped = {}
    for order_id, product_id, quantity in cursor.fetchall():
        if order_id in eligible:
            grouped.setdefault(order_id, Counter())[product_id] += quantity
    shipped = 0
    for order_id, demand in sorted(grouped.items()):
        available = {}
        for product_id in sorted(demand):
            cursor.execute('SELECT quantity_reserved FROM inventory WHERE product_id = %s AND warehouse_code = %s FOR UPDATE', (product_id, WAREHOUSE))
            row = cursor.fetchone()
            available[product_id] = row[0] if row else 0
        if any(available[key] < quantity for key, quantity in demand.items()):
            continue
        for product_id, quantity in demand.items():
            cursor.execute('UPDATE inventory SET quantity_reserved = quantity_reserved - %s WHERE product_id = %s AND warehouse_code = %s', (quantity, product_id, WAREHOUSE))
        cursor.execute("UPDATE orders SET status = 'shipped' WHERE order_id = %s", (order_id,))
        shipped += 1
    return dict(orders_delivered=len(delivered), orders_shipped=shipped)


def generate_inventory(cursor, batch, rng):

    counts = dict(inventory_inserted=0, inventory_updated=0)
    demand = Counter()
    used = set()
    for order, items in batch:
        for item in items:
            used.add(item['product_id'])
            if order['status'] == 'paid':
                demand[item['product_id']] += item['quantity']
    for product_id in sorted(used):
        cursor.execute('SELECT inventory_id, quantity_available FROM inventory WHERE product_id = %s AND warehouse_code = %s FOR UPDATE', (product_id, WAREHOUSE))
        current = cursor.fetchone()
        quantity = demand[product_id]
        if current is None:
            insert(cursor, 'inventory', dict(product_id=product_id, warehouse_code=WAREHOUSE,
                   bin_location=f'A-{product_id % 1000:03}', quantity_available=rng.randint(100, 300),
                   quantity_reserved=quantity, quantity_damaged=0, reorder_point=50, safety_stock=30,
                   last_restocked_at=datetime.now(timezone.utc).replace(tzinfo=None), last_counted_at=None))
            counts['inventory_inserted'] += 1
        else:
            restock = max(0, quantity + 100 - current[1]) if current[1] - quantity < 50 else 0
            cursor.execute('UPDATE inventory SET quantity_available = quantity_available + %s - %s, quantity_reserved = quantity_reserved + %s, last_restocked_at = CASE WHEN %s > 0 THEN UTC_TIMESTAMP(6) ELSE last_restocked_at END WHERE inventory_id = %s', (restock, quantity, quantity, restock, current[0]))
            counts['inventory_updated'] += 1
    return counts


def write_orders(cursor, batch):
    return [insert(cursor, 'orders', order) for order, _ in batch]


def generate_order_items(batch, order_ids):
    if len(batch) != len(order_ids):
        raise ValueError('Cada pedido precisa do ID retornado pelo banco.')
    return [dict(order_id=order_id, **item)
            for (_, items), order_id in zip(batch, order_ids) for item in items]


def write_order_items(cursor, rows):
    for row in rows:
        insert(cursor, 'order_items', row)
    return len(rows)


def write_batch(connection, batch, rng):
    with connection.cursor() as cursor:
        cursor.execute("SELECT GET_LOCK(CONCAT('sim_mysql_', LEFT(SHA2(DATABASE(), 256), 40)), 30)")
        if cursor.fetchone()[0] != 1:
            raise RuntimeError('Outra execução está ativa.')
        try:
            verify_schema(cursor)
            connection.begin()
            counts = advance_orders(cursor)
            counts.update(generate_inventory(cursor, batch, rng))
            ids = write_orders(cursor, batch)
            counts['orders'] = len(ids)
            counts['order_items'] = write_order_items(cursor, generate_order_items(batch, ids))
            connection.commit()
            return counts
        except BaseException:
            connection.rollback()
            raise
        finally:
            cursor.execute("SELECT RELEASE_LOCK(CONCAT('sim_mysql_', LEFT(SHA2(DATABASE(), 256), 40)))")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--average-orders', type=int, default=2000)
    parser.add_argument('--seed', type=int, help='Reproduz sorteios; omitido por padrão para variar cada execução.')
    parser.add_argument('--dry-run', action='store_true', help='Consulta o PostgreSQL e gera em memória, sem gravar.')
    args = parser.parse_args()
    if not 1 <= args.average_orders <= 100000:
        parser.error('--average-orders deve estar entre 1 e 100000.')
    rng = random.Random(args.seed)
    anomalous, count = choose_volume(rng, args.average_orders)
    catalog = load_catalog()
    run_id = uuid.uuid4().hex[:20]
    now = datetime.now(timezone.utc).replace(tzinfo=None)
    batch = build_batch(rng, catalog, count, now, run_id)
    summary = dict(run_id=run_id, execution_utc=now.isoformat() + 'Z', anomalous=anomalous,
                   average_orders=args.average_orders, orders=count, order_items=sum(len(items) for _, items in batch),
                   catalog='postgres', dry_run=args.dry_run)
    if not args.dry_run:
        connection = connect_mysql()
        try:
            summary.update(write_batch(connection, batch, rng))
        finally:
            connection.close()
    print(json.dumps(summary, ensure_ascii=False, indent=2))


if __name__ == '__main__':
    main()
