from datetime import datetime
from typing import Optional, List
from pydantic import BaseModel


class ImageMeta(BaseModel):
    post_id: str
    actor_did: str
    cid: str
    url: str
    mime_type: str
    created_at: datetime


class DetectionResult(BaseModel):
    label: str
    confidence: float


class ImageResult(BaseModel):
    meta: ImageMeta
    results: List[DetectionResult]
