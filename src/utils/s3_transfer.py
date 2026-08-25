from boto3.s3.transfer import TransferConfig

MULTIPART_CHUNK_SIZE = 16 * 1024 * 1024

S3_TRANSFER_CONFIG = TransferConfig(
    multipart_threshold=MULTIPART_CHUNK_SIZE,
    multipart_chunksize=MULTIPART_CHUNK_SIZE,
    max_concurrency=4,
    use_threads=True,
)
