--3)Which industry(SOC_NAME) has the most number of Data Scientist positions?[certified]

use h1b_project;

set hiveconf:MY_VAR;

INSERT OVERWRITE DIRECTORY '/h1b_project_output/${hiveconf:MY_VAR}/' 
ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' 
SELECT UPPER(soc_name), COUNT(*) AS no_job 
FROM h1b_partitioned
WHERE (UPPER(job_title) LIKE '%DATA SCIENTIST%')  AND case_status='CERTIFIED'
GROUP BY UPPER(soc_name)
ORDER BY no_job desc
limit 5;

quit;


