REGISTER '/home/hduser/pig/udfs/user_data_udf.py' USING streaming_python AS udf;

load_file = LOAD '/home/hduser/pig/udfs/user_log.csv' USING PigStorage(',') AS (username:chararray,idle_time:chararray,working_hour:chararray,start_time:chararray,End_time:chararray);

avr_working_hour = FOREACH load_file GENERATE udf.parse_user_working_hour(working_hour);

higher_then_avg_working_hour = cross  udf.parse_user_working_hour ,  avr_working_hour;

user_higher_then_avg_working_hour = foreach higher_then_avg_working_hour GENERATE udf.parse_user_working_hour>avr_working_hour;

DUMP user_higher_then_avg_working_hour;

user_lower_then_avg_working_hour = foreach higher_then_avg_working_hour GENERATE udf.parse_user_working_hour<avr_working_hour;

DUMP user_lower_then_avg_working_hour; 

DUMP avr_working_hour;

DUMP user_higher_then_avg_working_hour;

high_idle_time_user = FOREACH load_file GENERATE udf.parse_user_date(idle_time),udf.parse_username(username);

order_high_idle_time_user = ORDER BY high_idle_time_user DESC;

limit_high_idle_time_user = LIMIT order_high_idle_time_user by 10;

DUMP limit_high_idle_time_user;

early_user = FoREACH load_file GENERATE udf.parse_username(username),udf.parse_user_date(ilde_time);

order_early_user = ORDER BY early_user ASE;

limit_early_user = LIMIT order_early_user by 10;

DUMP limit_early_user;

