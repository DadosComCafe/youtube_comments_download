create table if not exists comment(
    id VARCHAR(30) PRIMARY KEY,
    author_name VARCHAR(20),
    text_original TEXT,
    published_date TEXT
);
