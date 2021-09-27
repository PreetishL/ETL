from etl_mongo import ProcessDataMongo
from etl_pg import ProcesssData

#if __name__ == "__main__":
ProcesssData.execute_job()
ProcessDataMongo.execute_job()