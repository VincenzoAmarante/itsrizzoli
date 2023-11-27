ALTER TABLE exams SET TBLPROPERTIES ("skip.header.line.count"="2");
LOAD DATA LOCAL INPATH '/home/maria_dev/${hiveconf:file}' OVERWRITE INTO TABLE exams;

insert into working_exams
SELECT
row_number() OVER () AS sequenza
,CAST(regexp_replace(idesame, '\\.', '') AS INT) AS idesame
,date_format(FROM_UNIXTIME(UNIX_TIMESTAMP(dataesame, 'MM/dd/yyyy')), 'yyyy-MM-dd') AS dataesame
,paziente, tipoesame
,CAST(costo as DOUBLE) as costo
,CAST(compenso as DOUBLE) as compenso
,14 as perc
from exams;
