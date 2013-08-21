--- discard the following outputs
\o /dev/null 

-- run the queries in sequence to warm up the cache

-- each query enables and disables timing 
-- enable it here to disable their timings
\timing

\ir psql/query1.sql;
\ir psql/query3.sql;
\ir psql/query5.sql;
\ir psql/query6.sql;
\ir psql/query10.sql;
\ir psql/query12.sql;
\ir psql/query14.sql;
\ir psql/query19.sql;

-- disable timing to enable following ones
\timing

-- redirect output to the agiven file
\o /home/gokhan/psql_benchmark 

\ir psql/query1.sql;
\ir psql/query3.sql;
\ir psql/query5.sql;
\ir psql/query6.sql;
\ir psql/query10.sql;
\ir psql/query12.sql;
\ir psql/query14.sql;
\ir psql/query19.sql;

\ir psql/query1.sql;
\ir psql/query3.sql;
\ir psql/query5.sql;
\ir psql/query6.sql;
\ir psql/query10.sql;
\ir psql/query12.sql;
\ir psql/query14.sql;
\ir psql/query19.sql;

\ir psql/query1.sql;
\ir psql/query3.sql;
\ir psql/query5.sql;
\ir psql/query6.sql;
\ir psql/query10.sql;
\ir psql/query12.sql;
\ir psql/query14.sql;
\ir psql/query19.sql;

-- make the outputs appear on console again
\o 
