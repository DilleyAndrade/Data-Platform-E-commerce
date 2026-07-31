from fastapi import FastAPI
from .data_loader import load_json


app = FastAPI(
    title="Data Platform API",
    description="API smulada para consulta dos dados JSON da plataforma.",
    version="1.0.0",
)


@app.get("/customer-reviews", tags=["Dados"])
def get_customer_reviews():
    #Return client reviews
    return load_json("dataset/api_customer_reviews.json")


@app.get("/exchange-rates", tags=["Dados"])
def get_exchange_rates():
    #Return exchange rates
    return load_json("dataset/api_exchange_rates.json")


@app.get("/marketing-campaigns", tags=["Dados"])
def get_marketing_campaigns():
    #Return marketing campaigns
    return load_json("dataset/api_marketing_campaigns.json")
