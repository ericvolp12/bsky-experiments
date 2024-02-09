from datetime import datetime
from typing import Optional, List
from pydantic import BaseModel


class ImageMeta(BaseModel):
    post_id: str
    actor_did: str
    cid: str
    mime_type: str
    created_at: datetime
    data: str

    def to_dict(self):
        # Format datetime to a RFC 3339 string
        created_at = self.created_at.isoformat()
        # Utilize the built-in `.dict()` method and customize it if necessary
        as_dict = self.dict(by_alias=True, exclude_none=True)
        as_dict["created_at"] = created_at
        return as_dict


class DetectionResult(BaseModel):
    label: str
    confidence: float
    box: List[float]


class ImageResult(BaseModel):
    meta: ImageMeta
    results: List[DetectionResult]

    def to_dict(self):
        # Utilize the built-in `.dict()` method and customize it if necessary
        return self.dict(by_alias=True, exclude_none=True)
