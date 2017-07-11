select soc_name,count(soc_name) as cnt from hiveh1b.h1b_final where job_title LIKE '%DATA SCIENTIST%' group by soc_name order by cnt desc limit 1; 
