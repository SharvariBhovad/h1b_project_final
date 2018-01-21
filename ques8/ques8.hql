use h1b_project;

set hiveconf:MY_VAR1;
set hiveconf:MY_VAR2;
set hiveconf:FOLDER_NAME;


INSERT OVERWRITE DIRECTORY '/h1b_project_output/${hiveconf:FOLDER_NAME}' 
ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' 
select year,job_title,AVG(prevailing_wage)
from h1b_final 
where (case_status='CERTIFIED' OR case_status='CERTIFIED-WITHDRAWN') AND 
full_time_position= '${hiveconf:MY_VAR2}' AND year= '${hiveconf:MY_VAR1}' 
group by year,job_title;

quit;

