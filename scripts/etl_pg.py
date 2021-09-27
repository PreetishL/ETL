#import libs
import pandas as pd
import psycopg2 as pg
from create import SqlStatements

class ProcesssData:

    'Class to process data'

    #function to get DB connections
    def get_db_objects():

        def get_connection():
            'Function to get db connection'
            conn = pg.connect("dbname =pits user=postgres")

            return conn
        conn = get_connection()
        
        def get_cursor(conn):
            'Function get the cursor'
            cursor = conn.cursor()

            return cursor
        cursor = get_cursor(conn)

        return conn, cursor

    CONN, CUR = get_db_objects()

    #function to close the connection
    def close_connection():
        'Function to close the connection'
        ProcesssData.CONN.commit()
        ProcesssData.CONN.close()

    #function to close the cursor()
    def close_cursor():
        ProcesssData.CUR.close()

    #function to execute query
    def execute_query(query, data, otype):
        ProcesssData.CUR.execute(query, data)

        if otype == 'insert':
            ProcesssData.CONN.commit()
            val = None
        else:
            val = ProcesssData.CUR.fetchall()
        
        return val

    #function to get the file
    def get_data():
        df = pd.read_csv('/Users/preetishlimaye/Downloads/etl/data/DATA.csv')

        return df

    def chk_record(email, ip_address):
        
        #function to check if elements exists
        def chk_ip():
            #chk ip address
            ip_sql = "SELECT COUNT(*) FROM ipadd WHERE ipaddress = (%s)"
            val = ProcesssData.execute_query(ip_sql, (ip_address, ), 'select')
            
            #check if val is 0
            return val[0][0]

        def chk_email():
            #chk email address
            email_sql = "SELECT COUNT(*) FROM person WHERE email = (%s)"
            val = ProcesssData.execute_query(email_sql, (email, ), 'select')
            
            #check if val is 0
            return val[0][0]

        #get the values
        #g = chk_gender()
        i = chk_ip()
        e = chk_email()

        return e, i

    def create_error_log(record):
        err_sql = """INSERT INTO failed_records (fname, lname, email, gender, ipaddress, error) VALUES (%s, %s, %s, %s, %s, %s)"""
        val = ProcesssData.execute_query(err_sql, record, 'insert')

        #return val

    def insert_into_staging(record):
        rec_st_sql = """INSERT INTO data_staging (first_name, last_name, email, gender, ip_address) VALUES (%s, %s, %s, %s, %s)"""
        val = ProcesssData.execute_query(rec_st_sql, record, 'insert')

    def insert_record(record):

        e, i = ProcesssData.chk_record(record[2], record[4])

        if e == 1 and i == 0:
            err = 'Duplicate email address'
            err_lst = list(record)
            err_lst.append(err)
            err_record = tuple(err_lst)
            #err_record = record + (err, )
            ProcesssData.create_error_log(err_record)
        elif i == 1 and e == 0:
            err = "Duplicate ip address"
            err_lst = list(record)
            err_lst.append(err)
            err_record = tuple(err_lst)
            ProcesssData.create_error_log(err_record)
        elif i == 1 and e == 1:
            err = "Duplicate ip address and email address"
            err_lst = list(record)
            err_lst.append(err)
            err_record = tuple(err_lst)
            ProcesssData.create_error_log(err_record)
        else:
            ProcesssData.insert_into_staging(record)

    def get_initial_count():


        sql_ip = "SELECT COUNT(*) FROM ipadd"
        sql_gender = "SELECT COUNT(*) FROM gender"
        sql_person = "SELECT COUNT(*) FROM person"

        ProcesssData.CUR.execute(sql_ip)
        ip = ProcesssData.CUR.fetchall()[0][0]
        ProcesssData.CUR.execute(sql_gender)
        gender = ProcesssData.CUR.fetchall()[0][0]
        ProcesssData.CUR.execute(sql_person)
        person = ProcesssData.CUR.fetchall()[0][0]

        if ip == 0 and gender == 0 and person == 0:
            return 'New'
        else:
            return 'tbd'

    def exec_stored_proc():
        ProcesssData.CUR.execute('CALL insert_data()')

    def checks(df):

        sql = "SELECT COUNT(*) FROM failed_records"
        ProcesssData.CUR.execute(sql)
        val = ProcesssData.CUR.fetchall()

        if val[0][0] == 0:
            print('All the records processed successfully!')
        else:
            print(str(len(df)) + ' records failed to be processed, please check failed_records table!')
        

    def create_db_objects():

        ProcesssData.CUR.execute(SqlStatements.CREATE_DATA_STATGING)
        ProcesssData.CUR.execute(SqlStatements.CREATE_GENDER)
        ProcesssData.CUR.execute(SqlStatements.CREATE_IP)
        ProcesssData.CUR.execute(SqlStatements.CREATE_PERSON)
        ProcesssData.CUR.execute(SqlStatements.CREATE_STORED_PROC)
        ProcesssData.CONN.commit()
    
    def execute_job():

        df = ProcesssData.get_data()
        df.pop('id')

        print('The records in the file are ' + str(len(df)))
        print('Processing records')

        records = df.to_records(index=False)
        init = ProcesssData.get_initial_count()
        ProcesssData.create_db_objects()
        #loop thru the records
        for r in records:
            if init == 'New':
                ProcesssData.insert_into_staging(r)
            else:
                ProcesssData.insert_record(r)
        #perform checks
        ProcesssData.checks(df)
        #call the function to execute stored proc
        ProcesssData.exec_stored_proc()
        #close the cursur()
        ProcesssData.close_cursor()
        #close the connection()
        ProcesssData.close_connection()