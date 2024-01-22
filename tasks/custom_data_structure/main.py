from typing import Optional
from pydantic import BaseModel


class ReplieCommentSnippet(BaseModel):
    id: str
    textDisplay: str
    parentId: str
    authorDisplayName: str
    authorChannelId: str
    likeCount: int
    publishedAt: str


class ReplieComments(BaseModel):
    comments: list[ReplieCommentSnippet]


class TopLevelCommentSnippet(BaseModel):
    id: str
    textDisplay: str
    authorDisplayName: str
    authorChannelId: str
    likeCount: int
    publishedAt: str


class CommentThread(BaseModel):
    id: str
    snippet: TopLevelCommentSnippet
    replies: Optional[ReplieComments] = None

    @property
    def totalReplyCount(self):
        return len(self.replies.comments) if self.replies else 0

    def __str__(self):
        return f"CommentThread(id={self.id}, snippet={self.snippet},replies={self.replies}, totalReplyCount={self.totalReplyCount})"


if __name__ == "__main__":
    from datetime import datetime

    snippet1 = TopLevelCommentSnippet(
        id="comment_id_1",
        textDisplay="This is a top-level comment",
        authorDisplayName="John Doe",
        authorChannelId="UC1234567890",
        likeCount=10,
        publishedAt=str(datetime.now()),
    )

    snippet2 = ReplieCommentSnippet(
        id="reply_id_1",
        textDisplay="This is a reply",
        parentId="comment_id_1",
        authorDisplayName="Jane Doe",
        authorChannelId="UC0987654321",
        likeCount=5,
        publishedAt=str(datetime.now()),
    )

    replies = ReplieComments(comments=[snippet2])

    comment_thread = CommentThread(id="thread_id_1", snippet=snippet1, replies=replies)

    print(snippet1)
    print("-" * 50)
    print(snippet2)
    print("-" * 50)
    print(replies)
    print("-" * 50)
    print(comment_thread)
