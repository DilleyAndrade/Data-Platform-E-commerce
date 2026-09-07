import argparse
from contextlib import ExitStack, closing
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
import random
import sys
from time import monotonic
import uuid

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
from data_generator import postgres_generator as pg
from data_generator import mysql_generator as mysql
from data_generator import api_generator as api
from data_generator import local_generator as local
from data_generator.reference_data import load_references, validate_generated
from data_generator.database_setup import connect_postgres, connect_mysql

DEPENDENCIES = {
    'suppliers': (),
    'customers': (),
    'coupons': (),
    'marketing_campaigns': (),
    'exchange_rates': (),
    'products': ('suppliers',),
    'orders': ('customers', 'products', 'coupons'),
    'order_items': ('orders', 'products'),
    'inventory': ('products', 'order_items'),
    'payments': ('orders', 'order_items', 'inventory'),
    'delivery_tracking': ('orders', 'payments'),
    'customer_reviews': ('customers', 'products', 'order_items', 'delivery_tracking'),
    'website_events': ('customers', 'products', 'orders', 'marketing_campaigns', 'customer_reviews', 'payments', 'delivery_tracking'),
}


def dependency_order():
    remaining = dict(DEPENDENCIES)
    ordered = []
    while remaining:
        ready = [name for name, parents in remaining.items() if set(parents) <= set(ordered)]
        if not ready:
            raise ValueError('Dependência ausente ou ciclo na ordem de geração.')
        for name in ready:
            ordered.append(name)
            del remaining[name]
    return ordered


class Generation:
    def __init__(self, average, seed):
        self.run_id = uuid.uuid4().hex
        self.rng = {}
        for source in ('postgres', 'mysql', 'api', 'local'):
            derived = None if seed is None else int.from_bytes(hashlib.sha256(f'{seed}:{source}'.encode()).digest()[:8], 'big')
            self.rng[source] = random.Random(derived)
        self.pg_anomaly, self.pg_counts = pg.choose_volume(self.rng['postgres'], average)
        self.mysql_anomaly, self.mysql_count = mysql.choose_volume(self.rng['mysql'], average)
        self.api_anomaly, self.api_counts = api.choose_volume(self.rng['api'], average)
        self.local_anomaly, self.local_counts = local.choose_volume(self.rng['local'], average)
        self.local_inserted = 0
        self.mysql_pending = False
        self.references = None

    def open(self, stack):
        from api_data_platform import data_store
        self.store = data_store
        self.pg_connection = stack.enter_context(connect_postgres())
        self.mysql_connection = stack.enter_context(closing(connect_mysql()))
        self.cursor = stack.enter_context(self.mysql_connection.cursor())
        with self.pg_connection.cursor() as cursor:
            pg.verify_schema(cursor)
        mysql.verify_schema(self.cursor)
        self.mysql_connection.rollback()
        local.read_sources()
        for dataset in self.store.DATASETS:
            self.store.read_dataset(dataset)

    def api_save(self, name, rows):
        if name == 'customer_reviews':
            validate_generated({name: rows}, self.refs())
        result = self.store.append_batch(dict(run_id=self.run_id, datasets={name: rows}))
        return result['inserted'][name]

    def local_save(self, name, rows):
        if name != 'coupons':
            validate_generated({name: rows}, self.refs())
        existing, headers = local.read_sources()
        local.save_table(name, existing[name], rows, headers)
        self.local_inserted += len(rows)
        return len(rows)

    def refs(self):
        if self.references is None:
            self.references = load_references()
        return self.references

    def suppliers(self):
        rows = pg.generate_suppliers(self.rng['postgres'], self.pg_counts['suppliers'], self.run_id)
        return len(pg.write_table(self.pg_connection, 'suppliers', rows))

    def customers(self):
        rows = pg.generate_customers(self.rng['postgres'], self.pg_counts['customers'], self.run_id, datetime.now(timezone.utc).date())
        return len(pg.write_table(self.pg_connection, 'customers', rows))

    def coupons(self):
        rows = local.generate_coupons(self.rng['local'], self.local_counts['coupons'], datetime.now(timezone.utc))
        return self.local_save('coupons', rows)

    def marketing_campaigns(self):
        rows = api.generate_marketing_campaigns(self.rng['api'], self.api_counts['marketing_campaigns'], datetime.now(timezone.utc))
        return self.api_save('marketing_campaigns', rows)

    def exchange_rates(self):
        return self.api_save('exchange_rates', api.generate_exchange_rates(datetime.now(timezone.utc)))

    def products(self):
        with self.pg_connection.cursor() as cursor:
            cursor.execute('SELECT supplier_id FROM public.suppliers WHERE is_active = TRUE')
            ids = [row[0] for row in cursor.fetchall()]
        rows = pg.generate_products(self.rng['postgres'], self.pg_counts['products'], self.run_id, ids)
        return len(pg.write_table(self.pg_connection, 'products', rows))

    def orders(self):
        catalog = mysql.load_catalog()
        files, _ = local.read_sources()
        self.cursor.execute('SELECT coupon_code, customer_id, COUNT(*) FROM orders WHERE coupon_code IS NOT NULL GROUP BY coupon_code, customer_id')
        usage = {(code, customer): count for code, customer, count in self.cursor.fetchall()}
        self.order_plan = mysql.generate_orders(self.rng['mysql'], catalog, self.mysql_count,
            datetime.now(timezone.utc).replace(tzinfo=None), self.run_id[:20], files['coupons'], usage)
        self.cursor.execute("SELECT GET_LOCK(CONCAT('sim_mysql_', LEFT(SHA2(DATABASE(), 256), 40)), 30)")
        if self.cursor.fetchone()[0] != 1:
            raise RuntimeError('Outra geração MySQL está ativa.')
        self.mysql_connection.begin()
        self.mysql_pending = True
        self.lifecycle = mysql.advance_orders(self.cursor)
        self.order_ids = mysql.write_orders(self.cursor, self.order_plan)
        return len(self.order_ids)

    def order_items(self):
        rows = mysql.generate_order_items(self.order_plan, self.order_ids)
        return mysql.write_order_items(self.cursor, rows)

    def inventory(self):
        result = mysql.generate_inventory(self.cursor, self.order_plan, self.rng['mysql'])
        self.mysql_connection.commit()
        self.mysql_pending = False
        self.cursor.execute("SELECT RELEASE_LOCK(CONCAT('sim_mysql_', LEFT(SHA2(DATABASE(), 256), 40)))")
        return result

    def payments(self):
        existing, _ = local.read_sources()
        rows = local.generate_payments(self.rng['local'], self.local_counts['payments'], existing['payments'], datetime.now(timezone.utc), self.refs())
        return self.local_save('payments', rows)

    def delivery_tracking(self):
        existing, _ = local.read_sources()
        rows = local.generate_delivery_tracking(self.rng['local'], self.local_counts['payments'], existing['delivery_tracking'], datetime.now(timezone.utc), self.refs())
        return self.local_save('delivery_tracking', rows)

    def customer_reviews(self):
        existing = self.store.read_dataset('customer_reviews')
        rows = api.generate_customer_reviews(self.rng['api'], self.api_counts['customer_reviews'], datetime.now(timezone.utc), self.refs(), existing)
        return self.api_save('customer_reviews', rows)

    def website_events(self):
        existing, _ = local.read_sources()
        campaigns = self.store.read_dataset('marketing_campaigns')
        count = self.local_counts['total'] - self.local_inserted
        rows = local.generate_website_events(self.rng['local'], count, existing['website_events'], datetime.now(timezone.utc), self.refs(), campaigns)
        return self.local_save('website_events', rows)


def run_all(average=2000, seed=None, dry_run=False):
    order = dependency_order()
    generation = Generation(average, seed)
    print('Ordem: ' + ' -> '.join(order), flush=True)
    print(json.dumps(dict(anomalies=dict(postgres=generation.pg_anomaly, mysql=generation.mysql_anomaly,
        api=generation.api_anomaly, local=generation.local_anomaly)), indent=2), flush=True)
    if dry_run:
        print('Plano exibido. Nenhum banco consultado ou dado gravado.')
        return 0
    started = monotonic()
    completed = []
    current = 'preparação'
    with ExitStack() as stack:
        try:
            generation.open(stack)
            for current in order:
                print(f'[{len(completed)+1}/{len(order)}] {current}', flush=True)
                result = getattr(generation, current)()
                completed.append(current)
                print(json.dumps(dict(table=current, records=result), ensure_ascii=False), flush=True)
        except (Exception, KeyboardInterrupt) as error:
            if generation.mysql_pending:
                generation.mysql_connection.rollback()
                completed = [name for name in completed if name not in ('orders', 'order_items', 'inventory')]
            detail = str(error).strip() or 'Exceção sem mensagem.'
            print(f'Falha em {current}: {type(error).__name__}: {detail} Etapas confirmadas: {", ".join(completed) or "nenhuma"}.', file=sys.stderr)
            return 130 if isinstance(error, KeyboardInterrupt) else 1
    print(f'Geração concluída em {monotonic()-started:.1f}s.', flush=True)
    return 0


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument('--average-records', type=int, default=2000)
    parser.add_argument('--seed', type=int)
    parser.add_argument('--dry-run', action='store_true', help='Exibe ordem e sorteios sem conexão ou escrita.')
    args = parser.parse_args()
    if not 100 <= args.average_records <= 100000:
        parser.error('--average-records deve estar entre 100 e 100000.')
    return run_all(args.average_records, args.seed, args.dry_run)


if __name__ == '__main__':
    raise SystemExit(main())
