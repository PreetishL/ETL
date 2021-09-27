class SqlStatements:

    CREATE_GENDER = '''create table if not exists gender (
                        gender_id serial primary key not null,
                        gender varchar(20)
                    );'''

    CREATE_IP = '''create table if not exists ipadd (
                ipaddress_id serial primary key not null ,
                ipaddress varchar(100)
                );'''

    CREATE_PERSON = '''create table if not exists person (
                        p_id serial primary key not null,
                        f_name varchar(50),
                        l_name varchar(50),
                        email varchar(100),
                        gender_id int references gender(gender_id),
                        ipaddress_id int references ipadd(ipaddress_id)
                    );'''

    CRETATE_FAILED_RECORDS = '''create table if not exists failed_records (
                                fname varchar(50),
                                lname varchar(50),
                                email varchar(500),
                                gender varchar(25),
                                ipaddress varchar(1000),
                                error varchar(500)
                            );'''

    CREATE_DATA_STATGING = '''create table if not exists data_staging (
                                first_name varchar(50),
                                last_name varchar(50),
                                email varchar(500),
                                gender varchar(25),
                                ip_address varchar(1000)
                            );'''

    CREATE_STORED_PROC = '''

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



    '''