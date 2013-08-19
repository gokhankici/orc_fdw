--- for file_fdw ---------------------------------------------------------------
create extension file_fdw;
create server file_server foreign data wrapper file_fdw;

drop foreign table if exists part_file;
drop foreign table if exists supplier_file;
drop foreign table if exists partsupp_file;
drop foreign table if exists customer_file;
drop foreign table if exists orders_file;
drop foreign table if exists nation_file;
drop foreign table if exists region_file;
drop foreign table if exists lineitem_file;

create foreign table part_file(
    P_PARTKEY INT8,
    P_NAME VARCHAR(55),
    P_MFGR CHAR(25),
    P_BRAND CHAR(10),
    P_TYPE VARCHAR(25),
    P_SIZE INTEGER,
    P_CONTAINER CHAR(10),
    P_RETAILPRICE FLOAT8,
    P_COMMENT VARCHAR(23)
) server file_server
options(filename '/home/gokhan/Downloads/tpch_2_13_0/part.tbl', delimiter '|');

create foreign table supplier_file(
    S_SUPPKEY INT8,
    S_NAME CHAR(25),
    S_ADDRESS VARCHAR(40),
    S_NATIONKEY INT8,
    S_PHONE CHAR(15),
    S_ACCTBAL FLOAT8,
    S_COMMENT VARCHAR(101)
) server file_server
options(filename '/home/gokhan/Downloads/tpch_2_13_0/supplier.tbl', delimiter '|');

create foreign table partsupp_file(
    PS_PARTKEY INT8,
    PS_SUPPKEY INT8,
    PS_AVAILQTY INTEGER,
    PS_SUPPLYCOST FLOAT8,
    PS_COMMENT VARCHAR(199)
) server file_server
options(filename '/home/gokhan/Downloads/tpch_2_13_0/partsupp.tbl', delimiter '|');

create foreign table customer_file(
    C_CUSTKEY INT8,
    C_NAME VARCHAR(25),
    C_ADDRESS VARCHAR(40),
    C_NATIONKEY INT8,
    C_PHONE CHAR(15),
    C_ACCTBAL FLOAT8,
    C_MKTSEGMENT CHAR(10),
    C_COMMENT VARCHAR(117)
) server file_server
options(filename '/home/gokhan/Downloads/tpch_2_13_0/customer.tbl', delimiter '|');

create foreign table orders_file(
    O_ORDERKEY INT8,
    O_CUSTKEY INT8,
    O_ORDERSTATUS CHAR(1),
    O_TOTALPRICE FLOAT8,
    O_ORDERDATE DATE,
    O_ORDERPRIORITY CHAR(15),
    O_CLERK CHAR(15),
    O_SHIPPRIORITY INTEGER,
    O_COMMENT VARCHAR(79)
) server file_server
options(filename '/home/gokhan/Downloads/tpch_2_13_0/orders.tbl', delimiter '|');

create foreign table nation_file(
    N_NATIONKEY INT8,
    N_NAME CHAR(25),
    N_REGIONKEY INT8,
    N_COMMENT VARCHAR(152)
) server file_server
options(filename '/home/gokhan/Downloads/tpch_2_13_0/nation.tbl', delimiter '|');

create foreign table region_file(
    R_REGIONKEY INT8,
    R_NAME CHAR(25),
    R_COMMENT VARCHAR
) server file_server
options(filename '/home/gokhan/Downloads/tpch_2_13_0/region.tbl', delimiter '|');

create foreign table lineitem_file(
    L_ORDERKEY INT8,
    L_PARTKEY INT8,
    L_SUPPKEY INT8,
    L_LINENUMBER INT,
    L_QUANTITY FLOAT8,
    L_EXTENDEDPRICE FLOAT8,
    L_DISCOUNT FLOAT8,
    L_TAX FLOAT8,
    L_RETURNFLAG CHAR(1),
    L_LINESTATUS CHAR(1),
    L_SHIPDATE DATE,
    L_COMMITDATE DATE,
    L_RECEIPTDATE DATE,
    L_SHIPINSTRUCT CHAR(25),
    L_SHIPMODE CHAR(10),
    L_COMMENT VARCHAR(44)
) server file_server
options(filename '/home/gokhan/Downloads/tpch_2_13_0/lineitem.tbl', delimiter '|');

--- for orc_fdw ----------------------------------------------------------------

create extension orc_fdw;
create server orc_server foreign data wrapper orc_fdw;

drop foreign table if exists part_orc;
drop foreign table if exists supplier_orc;
drop foreign table if exists partsupp_orc;
drop foreign table if exists customer_orc;
drop foreign table if exists orders_orc;
drop foreign table if exists nation_orc;
drop foreign table if exists region_orc;
drop foreign table if exists lineitem_orc;

create foreign table part_orc(
    P_PARTKEY INT8,
    P_NAME VARCHAR(55),
    P_MFGR CHAR(25),
    P_BRAND CHAR(10),
    P_TYPE VARCHAR(25),
    P_SIZE INTEGER,
    P_CONTAINER CHAR(10),
    P_RETAILPRICE FLOAT8,
    P_COMMENT VARCHAR(23)
) server orc_server
options(filename '/home/gokhan/orc-files/part_gzip.orc');

create foreign table supplier_orc(
    S_SUPPKEY INT8,
    S_NAME CHAR(25),
    S_ADDRESS VARCHAR(40),
    S_NATIONKEY INT8,
    S_PHONE CHAR(15),
    S_ACCTBAL FLOAT8,
    S_COMMENT VARCHAR(101)
) server orc_server
options(filename '/home/gokhan/orc-files/supplier_gzip.orc');

create foreign table partsupp_orc(
    PS_PARTKEY INT8,
    PS_SUPPKEY INT8,
    PS_AVAILQTY INTEGER,
    PS_SUPPLYCOST FLOAT8,
    PS_COMMENT VARCHAR(199)
) server orc_server
options(filename '/home/gokhan/orc-files/partsupp_gzip.orc');

create foreign table customer_orc(
    C_CUSTKEY INT8,
    C_NAME VARCHAR(25),
    C_ADDRESS VARCHAR(40),
    C_NATIONKEY INT8,
    C_PHONE CHAR(15),
    C_ACCTBAL FLOAT8,
    C_MKTSEGMENT CHAR(10),
    C_COMMENT VARCHAR(117)
) server orc_server
options(filename '/home/gokhan/orc-files/customer_gzip.orc');

create foreign table orders_orc(
    O_ORDERKEY INT8,
    O_CUSTKEY INT8,
    O_ORDERSTATUS CHAR(1),
    O_TOTALPRICE FLOAT8,
    O_ORDERDATE DATE,
    O_ORDERPRIORITY CHAR(15),
    O_CLERK CHAR(15),
    O_SHIPPRIORITY INTEGER,
    O_COMMENT VARCHAR(79)
) server orc_server
options(filename '/home/gokhan/orc-files/orders_gzip.orc');

create foreign table nation_orc(
    N_NATIONKEY INT8,
    N_NAME CHAR(25),
    N_REGIONKEY INT8,
    N_COMMENT VARCHAR(152)
) server orc_server
options(filename '/home/gokhan/orc-files/nation_gzip.orc');

create foreign table region_orc(
    R_REGIONKEY INT8,
    R_NAME CHAR(25),
    R_COMMENT VARCHAR
) server orc_server
options(filename '/home/gokhan/orc-files/region_gzip.orc');

create foreign table lineitem_orc(
    L_ORDERKEY INT8,
    L_PARTKEY INT8,
    L_SUPPKEY INT8,
    L_LINENUMBER INT,
    L_QUANTITY FLOAT8,
    L_EXTENDEDPRICE FLOAT8,
    L_DISCOUNT FLOAT8,
    L_TAX FLOAT8,
    L_RETURNFLAG CHAR(1),
    L_LINESTATUS CHAR(1),
    L_SHIPDATE DATE,
    L_COMMITDATE DATE,
    L_RECEIPTDATE DATE,
    L_SHIPINSTRUCT CHAR(25),
    L_SHIPMODE CHAR(10),
    L_COMMENT VARCHAR(44)
) server orc_server
options(filename '/home/gokhan/orc-files/lineitem_gzip.orc');
