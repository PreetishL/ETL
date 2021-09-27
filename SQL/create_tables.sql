create table if not exists gender (
	gender_id serial primary key not null,
	gender varchar(20)
);

create table if not exists ipadd (
	ipaddress_id serial primary key not null ,
	ipaddress varchar(100)
);

create table if not exists person (
	p_id serial primary key not null,
	f_name varchar(50),
	l_name varchar(50),
	email varchar(100),
	gender_id int references gender(gender_id),
	ipaddress_id int references ipadd(ipaddress_id)
);

create table if not exists failed_records (
	fname varchar(50),
	lname varchar(50),
	email varchar(500),
	gender varchar(25),
	ipaddress varchar(1000),
	error varchar(500)
);

create table if not exists data_staging (
	first_name varchar(50),
	last_name varchar(50),
	email varchar(500),
	gender varchar(25),
	ip_address varchar(1000)
)