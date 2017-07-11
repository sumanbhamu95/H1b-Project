bag1 = load '/user/hive/warehouse/hiveh1b.db/h1b_final' using PigStorage() as (sno:int,casestatus:chararray,employername:chararray,socname:chararray,jobtitle:chararray,fulltime:chararray,wage:int,year:chararray,worksite:chararray,longt:double,lat:double);
bag3 = group bag1 by (year,jobtitle);
bag4 = foreach bag3 generate group,COUNT(bag1) as total;
bag5 = foreach bag4 generate FLATTEN(group.year),FLATTEN(group.jobtitle),FLATTEN(total);
bag6 = group bag5 by year;
top = foreach bag6 {
sorted = order bag5 by total desc;
top1 = limit sorted 10;
generate flatten(top1);
};
dump top;


