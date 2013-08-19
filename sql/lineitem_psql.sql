drop table if exists part;
drop table if exists supplier;
drop table if exists partsupp;
drop table if exists customer;
drop table if exists orders;
drop table if exists nation;
drop table if exists region;
drop table if exists lineitem;

create table part(
	P_PARTKEY INT8,
	P_NAME VARCHAR(55),
	P_MFGR CHAR(25),
	P_BRAND CHAR(10),
	P_TYPE VARCHAR(25),
	P_SIZE INTEGER,
	P_CONTAINER CHAR(10),
	P_RETAILPRICE FLOAT8,
	P_COMMENT VARCHAR(23)
);

create table supplier(
	S_SUPPKEY INT8,
	S_NAME CHAR(25),
	S_ADDRESS VARCHAR(40),
	S_NATIONKEY INT8,
	S_PHONE CHAR(15),
	S_ACCTBAL FLOAT8,
	S_COMMENT VARCHAR(101)
);

create table partsupp(
	PS_PARTKEY INT8,
	PS_SUPPKEY INT8,
	PS_AVAILQTY INTEGER,
	PS_SUPPLYCOST FLOAT8,
	PS_COMMENT VARCHAR(199)
);

create table customer(
	C_CUSTKEY INT8,
	C_NAME VARCHAR(25),
	C_ADDRESS VARCHAR(40),
	C_NATIONKEY INT8,
	C_PHONE CHAR(15),
	C_ACCTBAL FLOAT8,
	C_MKTSEGMENT CHAR(10),
	C_COMMENT VARCHAR(117)
);

create table orders(
	O_ORDERKEY INT8,
	O_CUSTKEY INT8,
	O_ORDERSTATUS CHAR(1),
	O_TOTALPRICE FLOAT8,
	O_ORDERDATE DATE,
	O_ORDERPRIORITY CHAR(15),
	O_CLERK CHAR(15),
	O_SHIPPRIORITY INTEGER,
	O_COMMENT VARCHAR(79)
);

create table nation(
	N_NATIONKEY INT8,
	N_NAME CHAR(25),
	N_REGIONKEY INT8,
	N_COMMENT VARCHAR(152)
);

create table region(
	R_REGIONKEY INT8,
	R_NAME CHAR(25),
	R_COMMENT VARCHAR
);

create table lineitem(
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
);

COPY part 		FROM '/home/gokhan/Downloads/tpch_2_13_0/part.tbl' 		DELIMITER '|' CSV;
COPY supplier 	FROM '/home/gokhan/Downloads/tpch_2_13_0/supplier.tbl' 	DELIMITER '|' CSV;
COPY partsupp 	FROM '/home/gokhan/Downloads/tpch_2_13_0/partsupp.tbl' 	DELIMITER '|' CSV;
COPY customer 	FROM '/home/gokhan/Downloads/tpch_2_13_0/customer.tbl' 	DELIMITER '|' CSV;
COPY orders 	FROM '/home/gokhan/Downloads/tpch_2_13_0/orders.tbl' 	DELIMITER '|' CSV;
COPY nation 	FROM '/home/gokhan/Downloads/tpch_2_13_0/nation.tbl' 	DELIMITER '|' CSV;
COPY region 	FROM '/home/gokhan/Downloads/tpch_2_13_0/region.tbl' 	DELIMITER '|' CSV;
COPY lineitem 	FROM '/home/gokhan/Downloads/tpch_2_13_0/lineitem.tbl' 	DELIMITER '|' CSV;
