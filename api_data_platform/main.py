from fastapi import FastAPI

from .data_loader import load_json


app = FastAPI(
    title="Data Platform API",
    description="API para consulta dos dados JSON da plataforma.",
    version="1.0.0",
)


@app.get("/customer-reviews", tags=["Dados"])
def get_customer_reviews():
    """Retorna as avaliações de clientes."""
    return load_json("api_customer_reviews.json")


@app.get("/exchange-rates", tags=["Dados"])
def get_exchange_rates():
    """Retorna as taxas de câmbio."""
    return load_json("api_exchange_rates.json")


@app.get("/marketing-campaigns", tags=["Dados"])
def get_marketing_campaigns():
    """Retorna as campanhas de marketing."""
    return load_json("api_marketing_campaigns.json")
