drop foreign table if exists bigrow;

create foreign table bigrow(
    boolean1 BOOLEAN,
    short1 INT2,
    integer1 INT,
    long1 INT8,
    list1 INT[],
    float1 FLOAT4,
    double1 FLOAT8,
    string1 VARCHAR,
    list2 VARCHAR[],
    date1 DATE,
    timestamp1 TIMESTAMP
) server orc_server
options(filename '/home/gokhan/orc-files/bigrow.orc');
