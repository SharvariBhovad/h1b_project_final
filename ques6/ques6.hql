
use h1b_project;
set hiveconf:MY_VAR1;

INSERT OVERWRITE DIRECTORY '/h1b_project_output/${hiveconf:MY_VAR1}/' 
ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' 
SELECT t1.year,t1.case_status,ROUND(count(t1.case_status),2),ROUND((ROUND(count(t1.case_status),2)/t2.total),2)
FROM h1b_final_external t1,(SELECT year,count(*) AS total FROM h1b_final_external GROUP BY year limit 1) t2
GROUP BY t1.year,t1.case_status,total
ORDER BY t1.year;

quit;


