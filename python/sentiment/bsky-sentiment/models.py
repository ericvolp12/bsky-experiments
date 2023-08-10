from datetime import datetime
from typing import Optional, List
from pydantic import BaseModel


class Decision(BaseModel):
    sentiment: str
    confidence_score: float


class Post(BaseModel):
    rkey: str
    actor_did: str
    text: str
    decision: Optional[Decision] = None


class PostList(BaseModel):
    posts: List[Post]
