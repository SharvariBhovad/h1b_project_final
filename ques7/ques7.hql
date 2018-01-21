--7) Create a bar graph to depict the number of applications for each year [All]

use h1b_project;

set hiveconf:MY_VAR1;

INSERT OVERWRITE DIRECTORY '/h1b_project_output/${hiveconf:MY_VAR1}' 
ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' 
SELECT year, count(*)
FROM h1b_final_external
GROUP BY year
ORDER BY year;

quit;


