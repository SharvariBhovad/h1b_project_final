use h1b_project;

INSERT OVERWRITE DIRECTORY '/h1b_project_output/ques_2a_data/' 
ROW FORMAT DELIMITED FIELDS TERMINATED BY ':' 
select year,worksite from h1b_final 
where case_status='CERTIFIED' 
AND (UPPER(job_title) LIKE '%DATA ENGINEER%') ;

quit;

