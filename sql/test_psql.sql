DECLARE @count INT
SET @count = 0
WHILE (@count < 2)
BEGIN
   \i psql/query1.sql;
   \i psql/query3.sql;
   \i psql/query5.sql;
   \i psql/query6.sql;
   \i psql/query10.sql;
   \i psql/query12.sql;
   \i psql/query14.sql;
   \i psql/query19.sql;
   SET @count = (@count + 1)
END