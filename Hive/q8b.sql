
select year,job_title,avg(prevailing_wage) as avr from hiveh1b.h1b_final where full_time_position="N" and prevailing_wage is not NULL and prevailing_wage>0 group by job_title,year order by avr desc;


