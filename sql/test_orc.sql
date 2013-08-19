-- run the queries 3 times to make the benchmark ready

-- discard the following 3 runs' output
\o /dev/null 

\ir orc/query1.sql;
\ir orc/query3.sql;
\ir orc/query5.sql;
\ir orc/query6.sql;
\ir orc/query10.sql;
\ir orc/query12.sql;
\ir orc/query14.sql;
\ir orc/query19.sql;

\ir orc/query1.sql;
\ir orc/query3.sql;
\ir orc/query5.sql;
\ir orc/query6.sql;
\ir orc/query10.sql;
\ir orc/query12.sql;
\ir orc/query14.sql;
\ir orc/query19.sql;

\ir orc/query1.sql;
\ir orc/query3.sql;
\ir orc/query5.sql;
\ir orc/query6.sql;
\ir orc/query10.sql;
\ir orc/query12.sql;
\ir orc/query14.sql;
\ir orc/query19.sql;

-- redirect output to the given file
\o /home/gokhan/orc_benchmark 

-- enable timing for the upcoming benchmarks
\timing 

\ir orc/query1.sql;
\ir orc/query3.sql;
\ir orc/query5.sql;
\ir orc/query6.sql;
\ir orc/query10.sql;
\ir orc/query12.sql;
\ir orc/query14.sql;
\ir orc/query19.sql;

\ir orc/query1.sql;
\ir orc/query3.sql;
\ir orc/query5.sql;
\ir orc/query6.sql;
\ir orc/query10.sql;
\ir orc/query12.sql;
\ir orc/query14.sql;
\ir orc/query19.sql;

\ir orc/query1.sql;
\ir orc/query3.sql;
\ir orc/query5.sql;
\ir orc/query6.sql;
\ir orc/query10.sql;
\ir orc/query12.sql;
\ir orc/query14.sql;
\ir orc/query19.sql;

-- disable timing
\timing 

-- make the outputs appear on console again
\o 
