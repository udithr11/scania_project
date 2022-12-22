import pymongo
import pandas as pd
import json

from dotenv import load_dotenv
print(f"Loading environment variable from .env file")
load_dotenv()


# Provide the mongodb localhost url to connect python to mongodb.
client = pymongo.MongoClient("mongodb+srv://udithr:Ammazz123@cluster0.vqc1j4v.mongodb.net/?retryWrites=true&w=majority")

DATA_FILE_PATH="/config/workspace/aps_failure_training_set1.csv"
DATABASE_NAME="aps"
COLLECTION_NAME="sensor"

if __name__=="__main__":
    df=pd.read_csv(DATA_FILE_PATH)
    print(f"rows and columns:{df.shape}")


#convert dataframe into json so that we can dump the file into mongodb

df.reset_index(drop=True,inplace=True)

json_record=list(json.loads(df.T.to_json()).values())
print(json_record[0])


#inserting the converted data into mongodb
client[DATABASE_NAME][COLLECTION_NAME].insert_many(json_record)

