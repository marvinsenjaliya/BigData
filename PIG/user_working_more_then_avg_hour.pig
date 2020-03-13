REGISTER '/home/hduser/pig/udfs/user_data_udf.py' USING streaming_python AS udf;

load_file = LOAD '/home/hduser/pig/udfs/user_log.csv' USING PigStorage(',') AS (username:chararray,idle_time:chararray,working_hour:chararray,start_time:chararray,End_time:chararray);

avr_working_hour = FOREACH load_file GENERATE udf.parse_user_working_hour(working_hour);

higher_then_avg_working_hour = cross  udf.parse_user_working_hour ,  avr_working_hour;

user_higher_then_avg_working_hour = foreach higher_then_avg_working_hour GENERATE udf.parse_user_working_hour>avr_working_hour;

DUMP user_higher_then_avg_working_hour;



