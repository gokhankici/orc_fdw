drop foreign table if exists lineitem;
create foreign table lineitem(
    l_orderkey INT8,
    l_partkey INT8,
    l_suppkey INT8,
    l_linenumber INT,
    l_quantity FLOAT8,
    l_extendedprice FLOAT8,
    l_discount FLOAT8,
    l_tax FLOAT8,
    l_returnflag VARCHAR,
    l_linestatus VARCHAR,
    l_shipdate TIMESTAMP,
    l_commitdate TIMESTAMP,
    l_receiptdate TIMESTAMP,
    l_shipinstruct VARCHAR,
    l_shipmode VARCHAR,
    l_comment VARCHAR
) server orc_server
options(filename '/home/gokhan/orc-files/lineitem_gzip.orc');
