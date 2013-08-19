--- run the queries 3 times to make the benchmark ready

--- discard the following 3 runs' output
\o /dev/null 

\ir cfile/query1.sql;
\ir cfile/query3.sql;
\ir cfile/query5.sql;
\ir cfile/query6.sql;
\ir cfile/query10.sql;
\ir cfile/query12.sql;
\ir cfile/query14.sql;
\ir cfile/query19.sql;

\ir cfile/query1.sql;
\ir cfile/query3.sql;
\ir cfile/query5.sql;
\ir cfile/query6.sql;
\ir cfile/query10.sql;
\ir cfile/query12.sql;
\ir cfile/query14.sql;
\ir cfile/query19.sql;

\ir cfile/query1.sql;
\ir cfile/query3.sql;
\ir cfile/query5.sql;
\ir cfile/query6.sql;
\ir cfile/query10.sql;
\ir cfile/query12.sql;
\ir cfile/query14.sql;
\ir cfile/query19.sql;

-- redirect output to the given cfile
\o /home/gokhan/cfile_benchmark 

-- enable timing for the upcoming benchmarks
\timing 

\ir cfile/query1.sql;
\ir cfile/query3.sql;
\ir cfile/query5.sql;
\ir cfile/query6.sql;
\ir cfile/query10.sql;
\ir cfile/query12.sql;
\ir cfile/query14.sql;
\ir cfile/query19.sql;

\ir cfile/query1.sql;
\ir cfile/query3.sql;
\ir cfile/query5.sql;
\ir cfile/query6.sql;
\ir cfile/query10.sql;
\ir cfile/query12.sql;
\ir cfile/query14.sql;
\ir cfile/query19.sql;

\ir cfile/query1.sql;
\ir cfile/query3.sql;
\ir cfile/query5.sql;
\ir cfile/query6.sql;
\ir cfile/query10.sql;
\ir cfile/query12.sql;
\ir cfile/query14.sql;
\ir cfile/query19.sql;

-- disable timing
\timing 

-- make the outputs appear on console again
\o 