--- run the queries 3 times to make the benchmark ready

--- discard the following 3 runs' output
\o /dev/null 

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

-- redirect output to the given file
\o /home/gokhan/psql_benchmark 

-- enable timing for the upcoming benchmarks
\timing 

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

-- disable timing
\timing 

-- make the outputs appear on console again
\o 
