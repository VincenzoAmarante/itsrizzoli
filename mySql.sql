

LOAD DATA LOCAL INFILE '/home/maria_dev/forexSample.csv' INTO TABLE forex FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n';


select o._profit, count(*)
from(
	select 
	case when f.profit >0 then 1
	else 0 
	end _profit
	from forex f
	)o
group by o._profit;