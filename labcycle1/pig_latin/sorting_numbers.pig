data = LOAD '/user/hadoop/sorting_numbers.txt' USING PigStorage(',') AS (id:int, value:int);
sorted_data = ORDER data BY value ASC;
DUMP sorted_data;
