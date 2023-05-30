CREATE TABLE labels (
    id BIGSERIAL PRIMARY KEY,
    lookup_alias TEXT UNIQUE NOT NULL,
    name TEXT NOT NULL
);
