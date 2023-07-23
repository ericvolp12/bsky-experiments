-- The following Analyze commands are used to update the statistics of the tables
-- so that the query planner can make better decisions about the best way to execute
-- the query. This is especially important for the posts table, which is the largest
-- table in the database.
ANALYZE posts;
ANALYZE author_clusters;
ANALYZE clusters;
ANALYZE images;
ANALYZE authors;
