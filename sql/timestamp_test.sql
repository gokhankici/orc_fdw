create foreign table timestamp_test(
    time timestamp
) server orc_server
options(filename '/home/gokhan/orc-files/timestamp.orc');
