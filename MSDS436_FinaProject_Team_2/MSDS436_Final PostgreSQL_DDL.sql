CREATE EXTENSION aws_s3 CASCADE;





create table stuff
(date1 varchar(256),
_open varchar(256),
high varchar(256),
low varchar(256),
_close varchar(256),
adj_close varchar(256),
volume varchar(256),
ticker varchar(256),
date2 varchar(256),
id varchar(256),
date3 varchar(256),
probability varchar(256),
sentiment varchar(256),
PRIMARY KEY ( id )
)





SELECT aws_s3.table_import_from_s3(
   'stuff', '', '(format csv , header true)',
   'MSDS436group2-data',
'date/test20220228.csv',
'us-east-1',
'xxxxxxxxxxxxxxxxx', 'xxxxxxxxxxxxxxxxxxxxx'
)


