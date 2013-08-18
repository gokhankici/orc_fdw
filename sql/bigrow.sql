drop foreign table if exists bigrow;
create foreign table bigrow(
    boolean1 BOOLEAN,
    short1 INT2,
    integer1 INT,
    long1 INT8,
    float1 FLOAT,
    double1 FLOAT8,
    string1 VARCHAR,
    list1 VARCHAR[]
) server orc_server
options(filename '/home/gokhan/orc-files/bigrow.orc');
