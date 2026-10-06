from pydantic import BaseModel, Field


class InventoryItem(BaseModel):
    sku: str = Field(description="Stock Keeping Unit")
    name: str
    quantity: int
