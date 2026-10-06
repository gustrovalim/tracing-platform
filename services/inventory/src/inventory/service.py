from fastapi import FastAPI, HTTPException, status
from inventory.seed import SEED_DATA
from inventory.model.error_message import ErrorMessage, ERROR_DETAIL

app = FastAPI()


@app.get(
    "/inventory/{sku}", responses={status.HTTP_404_NOT_FOUND: {"model": ErrorMessage}}
)
async def get_inventory(sku: str):
    if sku not in SEED_DATA:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=ERROR_DETAIL)
    return SEED_DATA[sku]


@app.get("/health")
async def health_check():
    return {"status": "healthy"}
