ques6bag= load '/user/hive/warehouse/h1b_project.db/h1b_final' using PigStorage('\t')
	AS
(s_no:int, case_status:chararray, emp_name:chararray, soc_name:chararray, job_title:chararray, position:chararray, wage:double, year:chararray, worksite:chararray, logitute:double,latitute:double);


filterbag= foreach ques6bag generate year,case_status;

/*------------certified total------------------*/
Certified = FILTER filterbag BY case_status == 'CERTIFIED';
group_Certified = GROUP Certified BY year;
certified_value = FOREACH group_Certified GENERATE group, COUNT(Certified.case_status) AS Certified_total;


/*------------certified withdrawn total------------------*/
CertifiedWithdrawn = FILTER filterbag BY case_status == 'CERTIFIED-WITHDRAWN';
group_CertifiedWithdrawn = GROUP CertifiedWithdrawn BY year;
certified_withdrawn_value = FOREACH group_CertifiedWithdrawn GENERATE group, COUNT(CertifiedWithdrawn.case_status) AS CertifiedWithdrawn_total;


/*--------------withdrawn total------------------*/

Withdrawn = FILTER filterbag BY case_status == 'WITHDRAWN';
group_Withdrawn = GROUP Withdrawn BY year;
withdrawn_value= FOREACH group_Withdrawn GENERATE group, COUNT(Withdrawn.case_status) AS Withdrawn_total;

/*----------------denied total------------------*/

Denied = FILTER filterbag BY case_status == 'DENIED';
group_Denied = GROUP Denied BY year;
denied_value = FOREACH group_Denied GENERATE group, COUNT(Denied.case_status) AS Denied_total;


/*--------------total count---------------------*/

group_all_case = GROUP filterbag BY year; 

total_value = FOREACH group_all_case GENERATE group, COUNT(filterbag.year) AS total_count; 

/*-------------join all bag--------------------*/
joinbag = JOIN certified_value BY $0,
		 certified_withdrawn_value BY $0,
		withdrawn_value BY $0, 
		denied_value BY $0, 
		total_value BY $0;

column_wise = FOREACH joinbag GENERATE $0, $1, $3, $5, $7, $9;


/*--------------calculate percentage-----------*/
total_percentage = FOREACH column_wise GENERATE $0, $1, $2, $3, $4, $5, ROUND_TO(((DOUBLE)$1*100/$5),2) AS CERTIFIED_PERCENTAGE, 
ROUND_TO(((DOUBLE)$2*100/$5),2) AS CERTIFIED_WITHDRAWN_PERCENTAGE, ROUND_TO(((DOUBLE)$3*100/$5),2) AS WITHDRAWN_PERCENTAGE, ROUND_TO(((DOUBLE)$4*100/$5),2) AS DENIED_PERCENTAGE ;

--DUMP total_percentage;
store total_percentage into '/h1b_project_output/$filename/';
/*
(2011,307936,11596,10105,29130,358767,85.83,3.23,2.82,8.12)
(2012,352668,31118,10725,21096,415607,84.86,7.49,2.58,5.08)
(2013,382951,35432,11590,12141,442114,86.62,8.01,2.62,2.75)
(2014,455144,36350,16034,11899,519427,87.62,7.0,3.09,2.29)
(2015,547278,41071,19455,10923,618727,88.45,6.64,3.14,1.77)
(2016,569646,47092,21890,9175,647803,87.94,7.27,3.38,1.42)*/


