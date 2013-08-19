drop table if exists part;
drop table if exists supplier;
drop table if exists partsupp;
drop table if exists customer;
drop table if exists orders;
drop table if exists nation;
drop table if exists region;
drop table if exists lineitem;

create table part(
	p_partkey INT8,
	p_name VARCHAR(55),
	p_mfgr CHAR(25),
	p_brand CHAR(10),
	p_type VARCHAR(25),
	p_size INTEGER,
	p_container CHAR(10),
	p_retailprice FLOAT8,
	p_comment VARCHAR(23)
);

create table supplier(
	s_suppkey INT8,
	s_name CHAR(25),
	s_address VARCHAR(40),
	s_nationkey INT8,
	s_phone CHAR(15),
	s_acctbal FLOAT8,
	s_comment VARCHAR(101)
);

create table partsupp(
	ps_partkey INT8,
	ps_suppkey INT8,
	ps_availqty INTEGER,
	ps_supplycost FLOAT8,
	ps_comment VARCHAR(199)
);

create table customer(
	c_custkey INT8,
	c_name VARCHAR(25),
	c_address VARCHAR(40),
	c_nationkey INT8,
	c_phone CHAR(15),
	c_acctbal FLOAT8,
	c_mktsegment CHAR(10),
	c_comment VARCHAR(117)
);

create table orders(
	o_orderkey INT8,
	o_custkey INT8,
	o_orderstatus CHAR(1),
	o_totalprice FLOAT8,
	o_orderdate DATE,
	o_orderpriority CHAR(15),
	o_clerk CHAR(15),
	o_shippriority INTEGER,
	o_comment VARCHAR(79)
);

create table nation(
	n_nationkey INT8,
	n_name CHAR(25),
	n_regionkey INT8,
	n_comment VARCHAR(152)
);

create table region(
	r_regionkey INT8,
	r_name CHAR(25),
	r_comment VARCHAR(152)
);

create table lineitem(
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
	l_shipdate DATE,
	l_commitdate DATE,
	l_receiptdate DATE,
	l_shipinstruct CHAR(25),
	l_shipmode CHAR(10),
	l_comment VARCHAR(44)
);

COPY part 		FROM '/home/gokhan/Downloads/tpch_2_13_0/part.tbl' 		DELIMITER '|' CSV;
COPY supplier 	FROM '/home/gokhan/Downloads/tpch_2_13_0/supplier.tbl' 	DELIMITER '|' CSV;
COPY partsupp 	FROM '/home/gokhan/Downloads/tpch_2_13_0/partsupp.tbl' 	DELIMITER '|' CSV;
COPY customer 	FROM '/home/gokhan/Downloads/tpch_2_13_0/customer.tbl' 	DELIMITER '|' CSV;
COPY orders 	FROM '/home/gokhan/Downloads/tpch_2_13_0/orders.tbl' 	DELIMITER '|' CSV;
COPY nation 	FROM '/home/gokhan/Downloads/tpch_2_13_0/nation.tbl' 	DELIMITER '|' CSV;
COPY region 	FROM '/home/gokhan/Downloads/tpch_2_13_0/region.tbl' 	DELIMITER '|' CSV;
COPY lineitem 	FROM '/home/gokhan/Downloads/tpch_2_13_0/lineitem.tbl' 	DELIMITER '|' CSV;
