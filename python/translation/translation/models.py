from datetime import datetime
from typing import Optional, List
from pydantic import BaseModel

class Post(BaseModel):
    rkey: str
    actor_did: str
    text: str
    src_lang: str
    dst_lang: str
    translated_text: Optional[str]


class PostList(BaseModel):
    posts: List[Post]
