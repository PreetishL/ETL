import pandas as pd
import psycopg2 as pg
from pymongo import MongoClient

class ProcessDataMongo:

    def get_data():
        df = pd.read_csv('/Users/preetishlimaye/Downloads/etl/data/DATA.csv')

        return df

    def get_connection():

        myclient = MongoClient("mongodb://localhost:27017/")

        return myclient

    def get_collection(cname):

        conn = ProcessDataMongo.get_connection()
        db = conn['mydatabase']
        collection = db[cname]

        return collection
    
    def chk_duplicates(email, ip):
        
        def chk_email(email):

            coll = ProcessDataMongo.get_collection('Main')
            di = {'email': email}
            val = coll.find(di).count()

            return val

        def chk_ip(ip):

            coll = ProcessDataMongo.get_collection('Main')
            di = {'ip_address': ip}
            val = coll.find(di).count()

            return val

        e = chk_email(email)
        i = chk_ip(ip)

        return e, i 

    def insert_error_log(record, error):

        coll = ProcessDataMongo.get_collection('Error')
        record['error'] = error
        coll.insert_one(record)

    def insert_record(record):

        coll = ProcessDataMongo.get_collection('Main')
        coll.insert_one(record)

    def process_records(record):

        e, i = ProcessDataMongo.chk_duplicates(record['email'], record['ip_address'])
        
        if e == 0 & i == 1:
            error = 'Duplicate ip address.'
            ProcessDataMongo.insert_error_log(record, error)
        elif e == 1 & i == 0:
            error = 'Duplicate email address.'
            ProcessDataMongo.insert_error_log(record, error)
        elif e == 1 & i == 0:
            error = 'Duplicate ip and email address.'
            ProcessDataMongo.insert_error_log(record, error)
        else:
            ProcessDataMongo.insert_record(record)

    def execute_job():

        df = ProcessDataMongo.get_data()
        print('MongoDB: Trying to insert ' + str(len(df)))
        main = ProcessDataMongo.get_collection('Main')
    
        for r in df.to_dict('records'):
            ProcessDataMongo.process_records(r)

        next = ProcessDataMongo.get_collection('Main')

        if main.count() == next.count():
            print(str(len(df)) + ' number of records were not uploaded.')
        elif main.count() < next.count():
            val = next.count() -  main.count()
            print(str(val) + ' number of records were successfully uploaded!')

        
    