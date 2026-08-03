from ingestion.ingestion_api import ingestion_api
from ingestion.ingestion_mysql import ingestion_mysql
from ingestion.ingestion_postgres import ingestion_postgres

ingestion_api()
ingestion_postgres()
ingestion_mysql()