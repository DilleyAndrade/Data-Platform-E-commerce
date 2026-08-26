from path_constants.path_constants import BUCKET_OBS
from schemas.schemas import data_correction_log_schema
from utils.logger import log


def write_data_correction_log(spark, events):
    if not events:
        return None
    target_path = f"s3a://{BUCKET_OBS}/data_correction_log"
    log.info(
        "Writing observability table: table=data_correction_log events=%s path=%s.",
        len(events),
        target_path,
    )
    dataframe = spark.createDataFrame(events, schema=data_correction_log_schema)
    dataframe.write.format("delta").mode("append").save(target_path)
    log.info(
        "Observability table written: table=data_correction_log events=%s path=%s.",
        len(events),
        target_path,
    )
    return dataframe
