from contextlib import closing
from datetime import timezone
from decimal import Decimal
import os
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
try:
    from data_generator.schema_definitions import DATABASE_NAME
except ModuleNotFoundError:
    from schema_definitions import DATABASE_NAME


def settings(prefix):
    from dotenv import load_dotenv
    load_dotenv(ROOT / '.env')
    values = {key: os.getenv(f'{prefix}_{key}') for key in ('HOST', 'PORT', 'USER', 'PASSWORD')}
    missing = [f'{prefix}_{key}' for key, value in values.items() if not value]
    if missing:
        raise ValueError('Configurações ausentes: ' + ', '.join(missing))
    values['DB'] = DATABASE_NAME
    return values


def load_catalog(allow_empty=False):
    import psycopg
    from psycopg.rows import dict_row
    config = settings('PG')
    with psycopg.connect(host=config['HOST'], port=int(config['PORT']), dbname=config['DB'],
                        user=config['USER'], password=config['PASSWORD'], connect_timeout=10,
                        row_factory=dict_row) as connection:
        with connection.cursor() as cursor:
            cursor.execute('SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY')
            cursor.execute('SELECT customer_id, name, address_line, postal_code, city, state, country_code, status, created_at FROM public.customers')
            customers = cursor.fetchall()
            cursor.execute('SELECT p.product_id, p.sku, p.name, p.price, p.currency, p.supplier_id, p.is_active, p.created_at FROM public.products p JOIN public.suppliers s ON s.supplier_id = p.supplier_id')
            products = cursor.fetchall()
    if not allow_empty and (not customers or not products):
        raise ValueError('Execute postgres_generator.py primeiro: clientes e produtos são necessários.')
    return dict(customers=customers, products=products)


def load_references(allow_empty=False):

    import pymysql
    catalog = load_catalog(allow_empty=allow_empty)
    config = settings('MYSQL')
    with closing(pymysql.connect(host=config['HOST'], port=int(config['PORT']), database=config['DB'],
                    user=config['USER'], password=config['PASSWORD'], charset='utf8mb4',
                    cursorclass=pymysql.cursors.DictCursor, connect_timeout=10,
                    read_timeout=120, autocommit=False, init_command="SET time_zone = '+00:00'")) as connection:
        with connection.cursor() as cursor:
            cursor.execute('SET TRANSACTION ISOLATION LEVEL REPEATABLE READ')
            cursor.execute('START TRANSACTION READ ONLY')
            cursor.execute('SELECT order_id, customer_id, status, order_date, total_amount, currency, delivered_at, updated_at, shipping_city, shipping_state, shipping_country_code FROM orders')
            orders = cursor.fetchall()
            cursor.execute('SELECT order_id, product_id, sku, quantity, unit_price FROM order_items')
            items = cursor.fetchall()
    return select_valid_references(catalog, orders, items, allow_empty)


def select_valid_references(catalog, orders, items, allow_empty=False):
    customers = {r['customer_id'] for r in catalog['customers']}
    products = {r['product_id']: r for r in catalog['products']}
    order_map = {r['order_id']: dict(r) for r in orders if r['customer_id'] in customers}
    invalid_orders = set()
    with_items = set()
    for item in items:
        with_items.add(item['order_id'])
        product = products.get(item['product_id'])
        if product is None or item['sku'] != product['sku']:
            invalid_orders.add(item['order_id'])
    valid_orders = []
    for order_id, order in order_map.items():
        if order_id in invalid_orders or order_id not in with_items:
            continue
        if order['status'] not in ('pending', 'paid', 'shipped', 'delivered', 'cancelled', 'refunded'):
            continue
        if order.get('order_date') is None or order.get('updated_at') is None:
            continue
        if order['status'] == 'delivered' and order.get('delivered_at') is None:
            continue
        for key in ('order_date', 'updated_at', 'delivered_at'):
            if order.get(key) is not None:
                value = order[key]
                order[key] = value.replace(tzinfo=timezone.utc) if value.tzinfo is None else value.astimezone(timezone.utc)
        valid_orders.append(order)
    valid_ids = {r['order_id'] for r in valid_orders}
    valid_items = [r for r in items if r['order_id'] in valid_ids]
    if not allow_empty and (not valid_orders or not valid_items):
        raise ValueError('Nenhum pedido com refer?ncias v?lidas dispon?vel. Execute os geradores PostgreSQL e MySQL primeiro.')
    return dict(catalog, orders=valid_orders, items=valid_items)


def validate_generated(datasets, references):

    customers = {r['customer_id'] for r in references['customers']}
    products = {r['product_id'] for r in references['products']}
    orders = {r['order_id']: r for r in references['orders']}
    pairs = {(r['order_id'], r['product_id']) for r in references['items']}
    for name, rows in datasets.items():
        for index, row in enumerate(rows, 1):
            ids = {key: int(row[key]) for key in ('customer_id', 'product_id', 'order_id') if row.get(key) not in (None, '')}
            valid = (('customer_id' not in ids or ids['customer_id'] in customers)
                     and ('product_id' not in ids or ids['product_id'] in products)
                     and ('order_id' not in ids or ids['order_id'] in orders))
            if valid and 'order_id' in ids:
                order = orders[ids['order_id']]
                valid = ('customer_id' not in ids or ids['customer_id'] == order['customer_id'])
                valid = valid and ('product_id' not in ids or (ids['order_id'], ids['product_id']) in pairs)
                if name == 'payments':
                    valid = valid and Decimal(str(row['amount'])) == order['total_amount']
                    valid = valid and row.get('currency', order['currency']) == order['currency']
            if not valid:
                raise ValueError(f'{name}, registro novo {index}: referência ou valor incompatível com os bancos. Lote rejeitado antes da gravação.')
