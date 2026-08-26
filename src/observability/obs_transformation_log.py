from path_constants.path_constants import BUCKET_OBS
from schemas.schemas import transformation_log_schema
from utils.logger import log


def write_transformation_log(spark, events):
    if not events:
        return None
    target_path = f"s3a://{BUCKET_OBS}/transformation_log"
    log.info(
        "Writing observability table: table=transformation_log events=%s path=%s.",
        len(events),
        target_path,
    )
    dataframe = spark.createDataFrame(events, schema=transformation_log_schema)
    dataframe.write.format("delta").mode("append").save(target_path)
    log.info(
        "Observability table written: table=transformation_log events=%s path=%s.",
        len(events),
        target_path,
    )
    return dataframe
