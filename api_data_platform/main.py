from fastapi import FastAPI
from fastapi.responses import FileResponse
from prometheus_fastapi_instrumentator import Instrumentator
from .data_loader import resolve_data_file


app = FastAPI(
    title="Data Platform API",
    description="Mock API for querying the platform's JSON data.",
    version="1.0.0",
)

Instrumentator().instrument(app).expose(
    app,
    endpoint="/metrics",
    include_in_schema=False,
)


@app.get("/customer-reviews", tags=["Dados"])
def get_customer_reviews():
    return FileResponse(
        resolve_data_file("dataset/api_customer_reviews.json"),
        media_type="application/json",
    )


@app.get("/exchange-rates", tags=["Dados"])
def get_exchange_rates():
    return FileResponse(
        resolve_data_file("dataset/api_exchange_rates.json"),
        media_type="application/json",
    )


@app.get("/marketing-campaigns", tags=["Dados"])
def get_marketing_campaigns():
    return FileResponse(
        resolve_data_file("dataset/api_marketing_campaigns.json"),
        media_type="application/json",
    )
