from path_constants.path_constants import BUCKET_OBS
from schemas.schemas import landing_quality_log_schema


def write_landing_quality_log(spark, events):
    if not events:
        return None
    dataframe = spark.createDataFrame(events, schema=landing_quality_log_schema)
    dataframe.write.format("delta").mode("append").save(f"s3a://{BUCKET_OBS}/landing_quality_log")
    return dataframe
