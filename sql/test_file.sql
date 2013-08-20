--- discard the following 3 runs' output
\o /dev/null 

-- run the queries in sequence to warm up the cache

\ir file/query1.sql;
\ir file/query3.sql;
\ir file/query5.sql;
\ir file/query6.sql;
\ir file/query10.sql;
\ir file/query12.sql;
\ir file/query14.sql;
\ir file/query19.sql;

-- redirect output to the given file
\o /home/gokhan/file_benchmark 

-- enable timing for the upcoming benchmarks
\timing 

\ir file/query1.sql;
\ir file/query3.sql;
\ir file/query5.sql;
\ir file/query6.sql;
\ir file/query10.sql;
\ir file/query12.sql;
\ir file/query14.sql;
\ir file/query19.sql;

\ir file/query1.sql;
\ir file/query3.sql;
\ir file/query5.sql;
\ir file/query6.sql;
\ir file/query10.sql;
\ir file/query12.sql;
\ir file/query14.sql;
\ir file/query19.sql;

\ir file/query1.sql;
\ir file/query3.sql;
\ir file/query5.sql;
\ir file/query6.sql;
\ir file/query10.sql;
\ir file/query12.sql;
\ir file/query14.sql;
\ir file/query19.sql;

-- disable timing
\timing 

-- make the outputs appear on console again
\o 