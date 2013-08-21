-- analyze the foreign tables
analyze customer_orc;
analyze lineitem_orc;
analyze nation_orc;
analyze orders_orc;
analyze part_orc;
analyze partsupp_orc;
analyze region_orc;
analyze supplier_orc;

--- discard the following outputs
\o /dev/null 

-- run the queries in sequence to warm up the cache

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
