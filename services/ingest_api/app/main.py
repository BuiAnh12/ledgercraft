from fastapi import FastAPI
from .core.config import settings
from .routes.customers import router as customers_router
from .routes.accounts import router as accounts_router
from .routes.transactions import router as tx_router

app = FastAPI(title=settings.app_name)

@app.get("/healthz")
def health():
    return {"status": "ok"}

app.include_router(customers_router)
app.include_router(accounts_router)
app.include_router(tx_router)
