from ingestion.ingestion_api import ingestion_api
from ingestion.ingestion_local import ingestion_local
from ingestion.ingestion_mysql import ingestion_mysql
from ingestion.ingestion_postgres import ingestion_postgres
from utils.spark_session import spark_session

spark = spark_session("ingestion_pipeline", "local[*]")

ingestion_local(spark)
ingestion_postgres(spark)
ingestion_mysql(spark)
ingestion_api(spark)


s3_path = f"s3a://observability/ingestion_log"

df_log = spark.read.format("delta").load(s3_path)
df_log.show(100)

spark.stop()
