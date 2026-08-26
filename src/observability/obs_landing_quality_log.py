from path_constants.path_constants import BUCKET_OBS
from schemas.schemas import landing_quality_log_schema
from utils.logger import log


def write_landing_quality_log(spark, events):
    if not events:
        return None
    target_path = f"s3a://{BUCKET_OBS}/landing_quality_log"
    log.info(
        "Writing observability table: table=landing_quality_log events=%s path=%s.",
        len(events),
        target_path,
    )
    dataframe = spark.createDataFrame(events, schema=landing_quality_log_schema)
    dataframe.write.format("delta").mode("append").save(target_path)
    log.info(
        "Observability table written: table=landing_quality_log events=%s path=%s.",
        len(events),
        target_path,
    )
    return dataframe
