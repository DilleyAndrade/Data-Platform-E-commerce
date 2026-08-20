from ingestion.ingestion_local import ingestion_local
from utils.spark_session import spark_session

spark = spark_session("ingestion_pipeline", "local[*]")

ingestion_local(spark)

spark.stop()
