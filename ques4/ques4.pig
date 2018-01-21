ques4bag= load '/user/hive/warehouse/h1b_project.db/h1b_final' using PigStorage('\t')
	AS
(s_no:int, case_status:chararray, emp_name:chararray, soc_name:chararray, job_title:chararray, position:chararray, wage:double, year:chararray, worksite:chararray, logitute:double,latitute:double);


filter4bag= FOREACH ques4bag GENERATE year, emp_name;

groupbycol= group filter4bag by (year,emp_name);

finalgroup4bag= FOREACH groupbycol GENERATE group as firstcol,COUNT(filter4bag.emp_name)as empcount; 

groupyear= group finalgroup4bag by firstcol.year;

top5emp= foreach groupyear {
sorted = order finalgroup4bag by $1 desc;
top5 = limit sorted 5;
generate flatten(top5);
};

store top5emp into '/h1b_project_output/$filename/';

--((2011,TATA CONSULTANCY SERVICES LIMITED),5416)
--((2011,MICROSOFT CORPORATION),4253)
--((2011,DELOITTE CONSULTING LLP),3621)
--((2011,WIPRO LIMITED),3028)
--((2011,COGNIZANT TECHNOLOGY SOLUTIONS U.S. CORPORATION),2721)
--((2012,INFOSYS LIMITED),15818)
--((2012,WIPRO LIMITED),7182)
--((2012,TATA CONSULTANCY SERVICES LIMITED),6735)
--((2012,DELOITTE CONSULTING LLP),4727)
--((2012,IBM INDIA PRIVATE LIMITED),4074)
--((2013,INFOSYS LIMITED),32223)
--((2013,TATA CONSULTANCY SERVICES LIMITED),8790)
--((2013,WIPRO LIMITED),6734)
--((2013,DELOITTE CONSULTING LLP),6124)
--((2013,ACCENTURE LLP),4994)
--((2014,INFOSYS LIMITED),23759)
--((2014,TATA CONSULTANCY SERVICES LIMITED),14098)
--((2014,WIPRO LIMITED),8365)
--((2014,DELOITTE CONSULTING LLP),7017)
--((2014,ACCENTURE LLP),5498)
--((2015,INFOSYS LIMITED),33245)
--((2015,TATA CONSULTANCY SERVICES LIMITED),16553)
--((2015,WIPRO LIMITED),12201)
--((2015,IBM INDIA PRIVATE LIMITED),10693)
--((2015,ACCENTURE LLP),9605)
--((2016,INFOSYS LIMITED),25352)
--((2016,CAPGEMINI AMERICA INC),16725)
--((2016,TATA CONSULTANCY SERVICES LIMITED),13134)
--((2016,WIPRO LIMITED),10607)
--((2016,IBM INDIA PRIVATE LIMITED),9787)



