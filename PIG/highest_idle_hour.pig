REGISTER '/home/hduser/pig/udfs/user_data_udf.py' USING streaming_python AS udf;

load_file = LOAD '/home/hduser/pig/udfs/user_log.csv' USING PigStorage(',') AS (username:chararray,idle_time:chararray,working_hour:chararray,start_time:chararray,End_time:chararray);

early_user = FoREACH load_file GENERATE udf.parse_username(username),udf.parse_user_date(ilde_time);

order_early_user = ORDER BY early_user DESC;

limit_early_user = LIMIT order_early_user by 10;

DUMP limit_early_user;
