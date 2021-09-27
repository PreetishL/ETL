-- query to find common values in staging and ipadd table inner join
-- query to find common values in staging and gender table inner join
--query to find common values in staging and person table

Select count(*) from data_staging limit 5;
Select * from data_staging limit 5;

select * from person

SELECT * from ipadd limit 4;

select * from gender

INSERT INTO ipadd(ipaddress)(
	SELECT 
		DISTINCT(ds.ip_address) as ipaddress 
	FROM
		data_staging ds
		LEFT JOIN ipadd ip
		ON ds.ip_address = ip.ipaddress
	WHERE ip.ipaddress is null
 )
	

INSERT INTO gender(gender)(
	SELECT 
		DISTINCT(ds.gender) as gender
	FROM
		data_staging ds
		LEFT JOIN gender gen
	ON ds.gender = gen.gender
	WHERE gen.gender is  null
)	

select * from failed_records
select * from data_staging
create table failed_records (
	fname varchar(50),
	lname varchar(50),
	email varchar(500),
	gender varchar(25),
	ipaddress varchar(1000),
	error varchar(500)
)

ALTER TABLE person ADD COLUMN email varchar(550);

INSERT INTO failed_records (fname, lname, email, gender, ipaddress, error) 
VALUES 
