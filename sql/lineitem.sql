create extension file_fdw;
create server file_server foreign data wrapper file_fdw;

drop foreign table if exists lineitem_file;
create foreign table lineitem_file(
    l_orderkey INT8,
    l_partkey INT8,
    l_suppkey INT8,
    l_linenumber INT,
    l_quantity FLOAT8,
    l_extendedprice FLOAT8,
    l_discount FLOAT8,
    l_tax FLOAT8,
    l_returnflag CHAR(1),
    l_linestatus CHAR(1),
    l_shipdate TIMESTAMP,
    l_commitdate TIMESTAMP,
    l_receiptdate TIMESTAMP,
    l_shipinstruct CHAR(25),
    l_shipmode CHAR(10),
    l_comment VARCHAR(44)
) server file_server
options(filename '/home/gokhan/Downloads/tpch_2_13_0/lineitem.tbl', delimiter '|');


create extension orc_fdw;
create server orc_server foreign data wrapper orc_fdw;

drop foreign table if exists lineitem_orc;
create foreign table lineitem_orc(
    l_orderkey INT8,
    l_partkey INT8,
    l_suppkey INT8,
    l_linenumber INT,
    l_quantity FLOAT8,
    l_extendedprice FLOAT8,
    l_discount FLOAT8,
    l_tax FLOAT8,
    l_returnflag CHAR(1),
    l_linestatus CHAR(1),
    l_shipdate TIMESTAMP,
    l_commitdate TIMESTAMP,
    l_receiptdate TIMESTAMP,
    l_shipinstruct CHAR(25),
    l_shipmode CHAR(10),
    l_comment VARCHAR(44)
) server orc_server
options(filename '/home/gokhan/orc-files/lineitem_gzip.orc');
