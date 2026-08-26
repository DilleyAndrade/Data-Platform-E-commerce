from fastapi import FastAPI
from fastapi.responses import FileResponse
from .data_loader import resolve_data_file


app = FastAPI(
    title="Data Platform API",
    description="API smulada para consulta dos dados JSON da plataforma.",
    version="1.0.0",
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
