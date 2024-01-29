-- Tabela CommentSnippet
CREATE TABLE CommentSnippet (
  	id VARCHAR PRIMARY KEY,
    channelId VARCHAR,
    videoId VARCHAR,
    textDisplay VARCHAR,
    textOriginal VARCHAR,
    authorDisplayName VARCHAR,
    authorProfileImageUrl VARCHAR,
    authorChannelUrl VARCHAR,
    authorChannelId VARCHAR,
    canRate BOOLEAN,
    viewerRating VARCHAR,
    likeCount INTEGER,
    publishedAt VARCHAR,
    updatedAt VARCHAR
);

-- Tabela Comment
CREATE TABLE Comment (
  	id VARCHAR PRIMARY KEY,
    kind VARCHAR,
    etag VARCHAR,
    snippet_id VARCHAR REFERENCES CommentSnippet(id)
);

-- Tabela Replies
CREATE TABLE Replies (
  	id VARCHAR PRIMARY KEY,
    comment_id VARCHAR REFERENCES Comment(id)
);

-- Tabela CommentThreadSnippet
CREATE TABLE CommentThreadSnippet (
  	id VARCHAR PRIMARY KEY,
    channelId VARCHAR,
    videoId VARCHAR,
    topLevelComment_id VARCHAR REFERENCES Comment(id),
    canReply BOOLEAN,
    totalReplyCount INTEGER,
    isPublic BOOLEAN,
    replies_id VARCHAR REFERENCES Replies(id)
);

-- Tabela CommentThread
CREATE TABLE CommentThread (
  	id VARCHAR PRIMARY KEY,
    kind VARCHAR,
    etag VARCHAR,
    snippet_id VARCHAR REFERENCES CommentThreadSnippet(id)
);
