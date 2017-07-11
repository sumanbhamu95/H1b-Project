select year,employer_name,total,rank from(select year,employer_name,rank() over (partition by year order by total desc) as rank,total from hiveh1b.query4) ranked_table where ranked_table.rank <=5;

