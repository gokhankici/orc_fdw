-- analyze the foreign tables
analyze customer_file;
analyze lineitem_file;
analyze nation_file;
analyze orders_file;
analyze part_file;
analyze partsupp_file;
analyze region_file;
analyze supplier_file;


--- discard the following 3 runs' output
\o /dev/null 

-- run the queries in sequence to warm up the cache

-- each query enables and disables timing 
-- enable it here to disable their timings
\timing

\ir file/query1.sql;
\ir file/query3.sql;
\ir file/query5.sql;
\ir file/query6.sql;
\ir file/query10.sql;
\ir file/query12.sql;
\ir file/query14.sql;
\ir file/query19.sql;

-- disable timing to enable following ones
\timing

-- redirect output to the given file
\o /home/gokhan/file_benchmark 

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

-- make the outputs appear on console again
\o 