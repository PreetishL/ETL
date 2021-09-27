select * from data_staging
select * from gender
select * from ipadd
select * from person
select * from failed_records

truncate gender cascade;
truncate ipadd cascade;
truncate person cascade;

CALL insert_data()