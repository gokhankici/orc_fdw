drop foreign table if exists timestamp_test;
create foreign table timestamp_test(
    time timestamp
) server orc_server
options(filename '/home/gokhan/orc-files/timestamp.orc');

drop foreign table if exists date_test;
create foreign table date_test(
    time date
) server orc_server
options(filename '/home/gokhan/orc-files/timestamp.orc');
