ques2bbag= LOAD '/user/hive/warehouse/h1b_project.db/h1b_final' USING PIGSTORAGE('\t')
	AS
(s_no:int, case_status:chararray, emp_name:chararray, soc_name:chararray, job_title:chararray, position:chararray, wage:double, year:chararray, worksite:chararray, logitute:double,latitute:double);

columnvalue= FOREACH ques2bbag GENERATE year, worksite,case_status;

filtervalue= FILTER columnvalue BY case_status== 'CERTIFIED'; 

groupby2col= GROUP filtervalue BY (year,worksite);

finalgroupbag= FOREACH groupby2col GENERATE group AS firstcol,COUNT(filtervalue.worksite) AS job; 

grouponyr= GROUP finalgroupbag BY firstcol.year;


top5val= FOREACH grouponyr {
sorted = ORDER finalgroupbag BY $1 desc;
top5 = LIMIT sorted 5;
GENERATE FLATTEN(top5);
};
--dump top5val;
STORE top5val into '/h1b_project_output/ques_2b_output/';


