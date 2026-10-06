from pydantic import BaseModel

ERROR_DETAIL = "Item not found"


class ErrorMessage(BaseModel):
    detail: str = ERROR_DETAIL
