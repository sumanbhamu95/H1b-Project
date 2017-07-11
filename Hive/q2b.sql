
select year,worksite,total,rank from (select year,worksite,rank() over (partition by year order by total desc) as rank,total from hiveh1b.query2e ) ranked_table where ranked_table.rank<=5;




