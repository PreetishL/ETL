CREATE OR REPLACE PROCEDURE 
	insert_data()
LANGUAGE SQL
AS 
	$$
	-- insert values into ipadd table
		INSERT INTO ipadd(ipaddress)(
		SELECT 
			DISTINCT(ds.ip_address) as ipaddress 
		FROM
			data_staging ds
			LEFT JOIN ipadd ip
			ON ds.ip_address = ip.ipaddress
		WHERE ip.ipaddress is null
	 );
	 
	 -- add a query for update
	
	-- insert into gender table 
	INSERT INTO gender(gender)(
		SELECT 
			DISTINCT(ds.gender) as gender
		FROM
			data_staging ds
			LEFT JOIN gender gen
		ON ds.gender = gen.gender
		WHERE gen.gender is null
	);
	
	-- insert into persons table
		INSERT INTO person(f_name, l_name, email, gender_id, ipaddress_id)(
		SELECT 
			first_name, 
			last_name, 
			ds.email, 
			g.gender_id,
			ip.ipaddress_id
		FROM
			data_staging ds
			LEFT JOIN person ps
			ON ds.email = ps.email
			INNER JOIN gender g
			ON g.gender = ds.gender
			INNER JOIN ipadd ip
			ON ip.ipaddress = ds.ip_address
			WHERE ds.email NOT IN (
				SELECT 
					DISTINCT(email)
				FROM
					person
			)
		);
	
	-- TRUNCATE STAGING
	TRUNCATE data_staging;

$$;