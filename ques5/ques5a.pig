ques5bag= load '/user/hive/warehouse/h1b_project.db/h1b_final' using PigStorage('\t')
	AS
(s_no:int, case_status:chararray, emp_name:chararray, soc_name:chararray, job_title:chararray, position:chararray, wage:double, year:chararray, worksite:chararray, logitute:double,latitute:double);
																																																																																																																																																	
filter5bag= FOREACH ques5bag GENERATE year, job_title;

group5bycol= group filter5bag by (year,job_title);

finalgroup5bag= FOREACH group5bycol GENERATE group as firstcol,COUNT(filter5bag.job_title)as empcount; 

group5year= group finalgroup5bag by firstcol.year;
	
top5job= foreach group5year {
sorted = order finalgroup5bag by $1 desc;
top5 = limit sorted 5;
generate flatten(top5);
};
																																																																									
--dump top5job;
store top5job into '/h1b_project_output/$filename/';
