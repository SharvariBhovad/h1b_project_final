
use h1b_project;

set hiveconf:VAR_YEAR;
set hiveconf:FOLDER_NAME;

INSERT OVERWRITE DIRECTORY '/h1b_project_output/${hiveconf:FOLDER_NAME}/' 
ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' 
SELECT year,job_title, count(*) as total_job_position
FROM h1b_partitioned
WHERE year='${hiveconf:VAR_YEAR}' AND case_status='CERTIFIED'
group by year,job_title
order by total_job_position desc
limit 10;
quit;


