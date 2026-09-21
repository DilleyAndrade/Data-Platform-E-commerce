from datetime import datetime

from fastapi import FastAPI
from prometheus_fastapi_instrumentator import Instrumentator
from .data_store import read_dataset


app = FastAPI(
    title="Data Platform API",
    description="Mock API for querying the platform's JSON datasets.",
    version="1.0.0",
)

Instrumentator().instrument(app).expose(
    app,
    endpoint="/metrics",
    include_in_schema=False,
)


@app.get("/customer-reviews", tags=["Dados"])
def get_customer_reviews(
    updated_at_from: datetime | None = None,
    updated_at_until: datetime | None = None,
):
    return read_dataset('customer_reviews', updated_at_from, updated_at_until)


@app.get("/exchange-rates", tags=["Dados"])
def get_exchange_rates(
    updated_at_from: datetime | None = None,
    updated_at_until: datetime | None = None,
):
    return read_dataset('exchange_rates', updated_at_from, updated_at_until)


@app.get("/marketing-campaigns", tags=["Dados"])
def get_marketing_campaigns(
    updated_at_from: datetime | None = None,
    updated_at_until: datetime | None = None,
):
    return read_dataset('marketing_campaigns', updated_at_from, updated_at_until)
