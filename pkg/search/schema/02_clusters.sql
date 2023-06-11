CREATE TABLE clusters (
    id SERIAL PRIMARY KEY,
    lookup_alias TEXT UNIQUE NOT NULL,
    name TEXT NOT NULL
);
