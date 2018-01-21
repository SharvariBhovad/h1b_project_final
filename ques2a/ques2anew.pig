ques4bag= load '/h1b_project_output/ques_2a_data/' using PigStorage(':')
	AS
(year:chararray, worksite:chararray);

filtervalue= FOREACH ques4bag GENERATE year, worksite;

groupbyyear= group filtervalue by (year,worksite);

finalbagnew= FOREACH groupbyyear GENERATE group as firstcol,COUNT(filtervalue.worksite) as job; 

groupAllValueNew= group finalbagnew by firstcol.$0;

maxValue= FOREACH groupAllValueNew GENERATE group,MAX(finalbagnew.job) as max_job;

--(2011,19)
--(2012,26)
--(2013,44)
--(2014,42)
--(2015,60)
--(2016,121)

newbag= JOIN finalbagnew by (job),maxValue by(max_job);

finalbag = order (FOREACH newbag GENERATE $0,$1) by $0;
--dump finalbag;

store finalbag into '/h1b_project_output/ques_2a_output/';

