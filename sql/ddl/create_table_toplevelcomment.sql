create table if not exists toplevelcomment(
    id VARCHAR(30) PRIMARY KEY,
    author_name VARCHAR(100),
    text_original TEXT,
    published_date TEXT
);
