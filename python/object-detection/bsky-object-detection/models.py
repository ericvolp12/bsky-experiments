from datetime import datetime
from typing import Optional, List
from pydantic import BaseModel


class ImageMeta(BaseModel):
    cid: str
    url: str
    mime_type: str
    created_at: datetime


class DetectionResult(BaseModel):
    label: str
    confidence: float
    box: List[float]


class ImageResult(BaseModel):
    meta: ImageMeta
    results: List[DetectionResult]
