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
    l_shipdate INT8,
    l_commitdate INT8,
    l_receiptdate INT8,
    l_shipinstruct VARCHAR,
    l_shipmode VARCHAR,
    l_comment VARCHAR
) server orc_server
options(filename '/home/gokhan/orc-files/output_gzip_lcomment.orc');
