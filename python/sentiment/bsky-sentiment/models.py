from datetime import datetime
from typing import Optional, List
from pydantic import BaseModel

class Decision(BaseModel):
    sentiment: str
    confidence_score: float

class Post(BaseModel):
    id: str
    text: str
    parent_post_id: Optional[str] = None
    root_post_id: Optional[str] = None
    author_did: str
    created_at: datetime
    has_embedded_media: bool
    parent_relationship: Optional[str] = None
    decision: Optional[Decision] = None

class PostList(BaseModel):
    posts: List[Post]
