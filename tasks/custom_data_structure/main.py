from typing import List, Optional
from pydantic import BaseModel


class CommentSnippet(BaseModel):
    channelId: Optional[str] = None
    videoId: Optional[str] = None
    textDisplay: Optional[str] = None
    textOriginal: Optional[str] = None
    authorDisplayName: Optional[str] = None
    authorProfileImageUrl: Optional[str] = None
    authorChannelUrl: Optional[str] = None
    authorChannelId: Optional[dict] = {}
    canRate: Optional[bool] = None
    viewerRating: Optional[str] = None
    likeCount: Optional[int] = None
    publishedAt: Optional[str] = None
    updatedAt: Optional[str] = None


class Comment(BaseModel):
    kind: Optional[str] = None
    etag: Optional[str] = None
    id: Optional[str] = None
    snippet: Optional[CommentSnippet] = None


class Replies(BaseModel):
    comments: Optional[List[Comment]] = []


class CommentThreadSnippet(BaseModel):
    channelId: Optional[str] = None
    videoId: Optional[str] = None
    topLevelComment: Optional[Comment] = None
    canReply: Optional[bool] = None
    totalReplyCount: Optional[int] = None
    isPublic: Optional[bool] = None
    replies: Optional[Replies] = None


class CommentThread(BaseModel):
    kind: Optional[str] = None
    etag: Optional[str] = None
    id: Optional[str] = None
    snippet: Optional[CommentThreadSnippet] = None
