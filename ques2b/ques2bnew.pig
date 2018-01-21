ques2bbag= load '/user/hive/warehouse/h1b_project.db/h1b_final' using PigStorage('\t')
	AS
(s_no:int, case_status:chararray, emp_name:chararray, soc_name:chararray, job_title:chararray, position:chararray, wage:double, year:chararray, worksite:chararray, logitute:double,latitute:double);

filtervalue= FOREACH ques2bbag GENERATE year, worksite;

groupby2col= group filtervalue by (year,worksite);

finalgroupbag= FOREACH groupby2col GENERATE group as firstcol,COUNT(filtervalue.worksite)as job; 

grouponyr= group finalgroupbag by firstcol.year;


top5val= foreach grouponyr {
sorted = order finalgroupbag by $1 desc;
top5 = limit sorted 5;
generate flatten(top5);
};

store top5val into '/h1b_project_output/ques_2b_output/';


