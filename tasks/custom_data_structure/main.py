from typing import Optional
from pydantic import BaseModel


class ReplicaModel(BaseModel):
    id: str
    textOriginal: str
    authorDisplayName: str
    partentId: str  # id do comment
    publishedAt: str
    updatedAt: str
    likeCount: int


class CommentModel(BaseModel):
    id: str
    textOriginal: str
    authorDisplayName: str
    likeCount: int
    publishedAt: str
    updatedAt: str
    replies: Optional[list[ReplicaModel]] = []
