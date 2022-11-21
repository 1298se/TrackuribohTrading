import os

from dotenv import load_dotenv
from pymongo import MongoClient

load_dotenv()
mongo_client = MongoClient(os.environ.get("ATLAS_URI"))
db = mongo_client.get_database('YGOPricing')

collection_names = db.list_collection_names()

for name in collection_names:
    collection = db.get_collection(name)

    collection.delete_many({})
