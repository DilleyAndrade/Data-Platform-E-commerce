from contextlib import closing
import os

try:
    from data_generator.schema_definitions import DATABASE_NAME, POSTGRES_DDL, MYSQL_DDL
    from data_generator.reference_data import settings
except ModuleNotFoundError:
    from schema_definitions import DATABASE_NAME, POSTGRES_DDL, MYSQL_DDL
    from reference_data import settings


def connect_postgres():
    import psycopg
    from psycopg import sql
    config = settings('PG')
    options = dict(host=config['HOST'], port=int(config['PORT']), user=config['USER'],
                   password=config['PASSWORD'], connect_timeout=10, autocommit=True)
    try:
        connection = psycopg.connect(dbname=DATABASE_NAME, **options)
    except psycopg.errors.InvalidCatalogName:
        with psycopg.connect(dbname=os.getenv('PG_MAINTENANCE_DB', 'postgres'), **options) as admin:
            try:
                admin.execute(sql.SQL('CREATE DATABASE {}').format(sql.Identifier(DATABASE_NAME)))
            except psycopg.errors.DuplicateDatabase:
                pass
        connection = psycopg.connect(dbname=DATABASE_NAME, **options)
    try:
        with connection.transaction():
            with connection.cursor() as cursor:
                cursor.execute('SELECT pg_advisory_xact_lock(746120031)')
                cursor.execute("SELECT 1 FROM pg_namespace WHERE nspname = 'public'")
                if cursor.fetchone() is None:
                    cursor.execute('CREATE SCHEMA public')
                for name, ddl in POSTGRES_DDL.items():
                    cursor.execute('SELECT to_regclass(%s)', (f'public.{name}',))
                    if cursor.fetchone()[0] is not None:
                        continue
                    for statement in ddl.split(';'):
                        if statement.strip():
                            cursor.execute(statement)
                    cursor.execute("SELECT to_regprocedure('public.set_record_updated_at()')")
                    if cursor.fetchone()[0] is None:
                        cursor.execute('''CREATE FUNCTION public.set_record_updated_at()
                            RETURNS TRIGGER LANGUAGE plpgsql AS $$
                            BEGIN
                                NEW.created_at = OLD.created_at;
                                NEW.updated_at = clock_timestamp();
                                RETURN NEW;
                            END; $$''')
                    cursor.execute(sql.SQL('CREATE TRIGGER {} BEFORE UPDATE ON public.{} '
                        'FOR EACH ROW EXECUTE FUNCTION public.set_record_updated_at()').format(
                            sql.Identifier(f'trg_{name}_updated'), sql.Identifier(name)))
        return connection
    except BaseException:
        connection.close()
        raise


def connect_mysql():
    import pymysql
    config = settings('MYSQL')
    options = dict(host=config['HOST'], port=int(config['PORT']), user=config['USER'],
                   password=config['PASSWORD'], charset='utf8mb4', connect_timeout=10,
                   read_timeout=120, write_timeout=120, init_command="SET time_zone = '+00:00'")
    try:
        connection = pymysql.connect(database=DATABASE_NAME, autocommit=False, **options)
    except pymysql.err.OperationalError as error:
        if error.args[0] != 1049:
            raise
        with closing(pymysql.connect(autocommit=True, **options)) as admin:
            with admin.cursor() as cursor:
                cursor.execute('CREATE DATABASE IF NOT EXISTS `data_platform` CHARACTER SET utf8mb4')
        connection = pymysql.connect(database=DATABASE_NAME, autocommit=False, **options)
    try:
        with connection.cursor() as cursor:
            for name, ddl in MYSQL_DDL.items():
                cursor.execute('SELECT 1 FROM information_schema.TABLES WHERE TABLE_SCHEMA = %s AND TABLE_NAME = %s', (DATABASE_NAME, name))
                if cursor.fetchone() is None:
                    cursor.execute(ddl.replace('CREATE TABLE ', 'CREATE TABLE IF NOT EXISTS ', 1))
        connection.commit()
        return connection
    except BaseException:
        connection.close()
        raise
