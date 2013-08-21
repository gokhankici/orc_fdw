-- analyze the foreign tables
analyze customer_cfile;
analyze lineitem_cfile;
analyze nation_cfile;
analyze orders_cfile;
analyze part_cfile;
analyze partsupp_cfile;
analyze region_cfile;
analyze supplier_cfile;


-- discard the following 3 runs' output
\o /dev/null 

-- run the queries in sequence to warm up the cache

-- each query enables and disables timing 
-- enable it here to disable their timings
\timing

\ir cfile/query1.sql;
\ir cfile/query3.sql;
-- \ir cfile/query5.sql;
\ir cfile/query6.sql;
\ir cfile/query10.sql;
\ir cfile/query12.sql;
\ir cfile/query14.sql;
\ir cfile/query19.sql;

-- disable timing to enable following ones
\timing

-- redirect output to the given cfile
\o /home/gokhan/cfile_benchmark 

\ir cfile/query1.sql;
\ir cfile/query3.sql;
-- \ir cfile/query5.sql;
\ir cfile/query6.sql;
\ir cfile/query10.sql;
\ir cfile/query12.sql;
\ir cfile/query14.sql;
\ir cfile/query19.sql;

\ir cfile/query1.sql;
\ir cfile/query3.sql;
-- \ir cfile/query5.sql;
\ir cfile/query6.sql;
\ir cfile/query10.sql;
\ir cfile/query12.sql;
\ir cfile/query14.sql;
\ir cfile/query19.sql;

\ir cfile/query1.sql;
\ir cfile/query3.sql;
-- \ir cfile/query5.sql;
\ir cfile/query6.sql;
\ir cfile/query10.sql;
\ir cfile/query12.sql;
\ir cfile/query14.sql;
\ir cfile/query19.sql;

-- make the outputs appear on console again
\o 