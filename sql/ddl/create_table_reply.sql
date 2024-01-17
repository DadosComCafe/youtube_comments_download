create table if not exists reply(
    id VARCHAR(30) PRIMARY KEY,
    parentid VARCHAR(30),
    author_name VARCHAR(20),
    text_original TEXT,
    published_date TEXT
);
-- need to be tested
