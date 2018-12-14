import pymongo
import os


class NONESENSE:
    mongocli = pymongo.MongoClient(os.environ.get("JZDATAURI", "mongodb://127.0.0.1:27017"))


def DB() -> pymongo.MongoClient:
    return NONESENSE.mongocli


def connect_db(db_name="datacenter"):
    client = DB()
    return client['{}'.format(db_name)]


def connect_coll(coll_name, _db):
    return _db['{}'.format(coll_name)]


db = DB()

