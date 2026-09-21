from ingestion.incremental_state import (
    commit_watermarks,
    configured_watermark_upper_bound,
    successful_events_for_all_datasets,
)
from utils.job import job_arguments, job_spark


def main():
    arguments = job_arguments("Commit incremental watermarks after pipeline success.")
    watermark_ts = configured_watermark_upper_bound()
    if watermark_ts is None:
        raise ValueError("PIPELINE_WATERMARK_UNTIL is required to commit watermarks.")
    events = successful_events_for_all_datasets(watermark_ts)
    with job_spark("commit_ingestion_watermarks") as spark:
        committed = commit_watermarks(spark, events, arguments.run_id)
    if committed != len(events):
        raise RuntimeError(
            f"Expected {len(events)} watermark commits, got {committed}."
        )


if __name__ == "__main__":
    main()
