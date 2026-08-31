import os
from pathlib import Path


API_BASE_URL = os.getenv("API_BASE_URL", "http://localhost:8000").rstrip("/")
URL_CUSTOMER_REVIEW = f"{API_BASE_URL}/customer-reviews"
URL_EXCHANGE_RATES = f"{API_BASE_URL}/exchange-rates"
URL_MARKETING_CAMPAIGNS = f"{API_BASE_URL}/marketing-campaigns"

PATH_LOCAL_FILES = Path(os.getenv("LOCAL_DATA_PATH", "local_data_source"))

BUCKET_LAN = "landing"
BUCKET_RAW = "raw"
BUCKET_BRO = "bronze"
BUCKET_SIL = "silver"
BUCKET_GOL = "gold"
BUCKET_OBS = "observability"
BUCKET_REJ = "rejected"
BUCKET_QUA = "quarantine"
