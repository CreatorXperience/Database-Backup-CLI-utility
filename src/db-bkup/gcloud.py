#!/usr/bin/env python3
from google.cloud import storage
from mysql_operator import BackupOperator
import os
import typing
import click
import importlib
import re
import sys
import certifi
import pymongo
import json
import bson
import pymysql


os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "./database-service-account-key.json"


def create_bucket(bkt_name: str):
    client: typing.Any = storage.Client()
    new_bucket = client.bucket(bkt_name)
    new_bucket.storage_class("COLDLINE")
    bucket = client.create_bucket(new_bucket, location="us")
    print(f'''successfully created {bucket.name},{bucket.location}
          ,{bucket.storage_class}''')

    return bucket


# TODO #1 #Duration: 1 hour
# list all available bucket
# set a condition if a bucket already exist
# if it exist set document in that bucket #NOTE
# document must have a unique name
# if it doesn't create a new bucket

# TODO #2 #Duration:  2 hour
# connect to database if all credentials are valid
# create a command line using click for full backup

supported_db = ["postgresql", "mongodb", "mysql"]
config_path = os.path.expanduser("~")+"/.config"

current_user_db = dict({
        "db": None
        })

mongo_c = None


def validate_authentication_credentials(host, port, username, password, db):
    matched = re.search(r"(?P<ip>\d{1,3}.\d{1,3}.\d{1,3}.\d{1,3})", host)
    host_length = len(host.split("."))
    try:
        if matched and host_length == 4 and int(port) and (
                username and password and db
                ):
            return True
        else:
            return False
    except ValueError:
        print("Bad host")


def connect_to_mysql(host):
    hostname = os.environ.get("MYSQL_HOSTNAME")
    port = os.environ.get("PORT")
    username = os.environ.get("USER_NAME")
    password = os.environ.get("PASS_WORD")
    # backup_operator = BackupOperator(hostname=hostname, port=port, username=username, password=password)

    timeout = 10
    connection = pymysql.connect(
    charset="utf8mb4",
    connect_timeout=timeout,
    cursorclass=pymysql.cursors.DictCursor,
    db="defaultdb",
    host="mysql-82447bb-sqlone.j.aivencloud.com",
    password="AVNS_vo-by8bVADKM-Ywq2i0",
    read_timeout=timeout,
    port=15527,
    user="avnadmin",
    write_timeout=timeout,
    )

    try:
        cursor = connection.cursor()
        cursor.execute("CREATE TABLE mytest (id INTEGER PRIMARY KEY)")
        cursor.execute("INSERT INTO mytest (id) VALUES (1), (2)")
        cursor.execute("SELECT * FROM mytest")
        print(cursor.fetchall())
    finally:
        print("successfully connected to mysql")
        connection.close()


def connect_to_mongodb(host):
    global current_user_db
    try:
        if host == "127.0.0.1":
            uri = "mongodb://localhost:27017/"
            mongo_c = pymongo.MongoClient(uri, tlscafile=certifi.where())
            current_user_db["db"] = "mongodb"
            current_user_db["uri"] = host
            if os.path.exists(config_path+"/db_bkup"):
                with open(config_path+"/db_bkup/auth.json", "w") as auth_file:
                    json.dump(current_user_db, auth_file)
            else:
                os.makedirs(config_path+"/db_bkup")
                file = config_path+"/db_bkup/auth.json"
                with open(file, "w") as auth:
                    json.dump(current_user_db, auth)
        else:
            uri = host
            mongo_c = pymongo.MongoClient(uri, tlscafile=certifi.where())
            current_user_db["db"] = "mongodb"
            current_user_db["uri"] = uri
            with open(config_path+"/db_bkup/auth.json", "w") as auth_file:
                json.dump(current_user_db, auth_file)
        print(click.style("• connected to mongodb database successfully",
                          "green", bold=True, dim=True))

    except pymongo.errors.ServerSelectionTimeoutError:
        print(click.style(" x Timeout Error : Ip address whitelisted. Allow all origin from mongodb database access", "red",
                          dim=True, bold=True))


class JSONEncoder(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, bson.ObjectId):
            return str(o)
        return super().default(o)


def backup_mongodb(uri, db_name, *, coll: str | None):
    print(db_name, end="97")
    mongo_c = pymongo.MongoClient(uri, tlscafile=certifi.where())
    db = mongo_c[db_name]
    if coll is not None:
        print(coll, end="101")
        collection = db[coll]
        document = collection.find()
        mongodb_default_backup_dir = os.path.expanduser("~/.config/db_bkup/mongodb")
        backup_file = mongodb_default_backup_dir+"/{}".format(coll)
        if os.path.exists(mongodb_default_backup_dir):
            with open(backup_file, "w") as default_bk_file:
                for doc in document:
                    jsonDoc = {
                            '_id': doc["_id"],
                            **doc
                            }

                    json.dump(jsonDoc, default_bk_file, cls=JSONEncoder)
        else:
            os.mkdir(mongodb_default_backup_dir)
            with open(backup_file, "w") as default_bk_file:
                json.dump(document, default_bk_file)

    else:
        collections = db.list_collection_names()
        for c in collections:
            backup_mongodb(uri, db_name, coll=c)

    return


@click.group()
def cli():
    pass


@cli.command(help='''Connect to your prefered database.
            We currently support this databases 'MYSQL', 'POSTGRESQL', 'MONGODB'
             ''')
@click.argument("username")
@click.argument("password")
@click.option("--host", prompt=True)
@click.option("--port", prompt=True)
@click.option("--db", prompt=True,
              type=click.Choice(["mongodb", "postgresql", "mysql"]))
def sync(host, port, username, password, db):
    is_validated = validate_authentication_credentials(host, port, username, password, db)
    if is_validated:
        print("validated")
    else:
        print("bad payload, wrong data")
        sys.exit()
    match db:
        case "postgresql":
            print("postgresql is used")
            postgres = importlib.import_module("click")
            # replace click with the appropriate module for postgres
            print(postgres.command())
            return
        case "mongodb":
            connect_to_mongodb(host)
        case "mysql":
            connect_to_mysql()
            # replace click with the appropriate module for mysql

            print("mysql is used")


@cli.command(help="Backup entire database on local device or cloud. Note, we currently support google cloud ")
@click.argument("database_name")
@click.option("--table", help="Backup table in a SQL database")
@click.option("--collection", help="Backup collection for MongoDB database")
@click.option("--t", default="full", type=click.Choice(["full", "increment"]))
@click.option("--d", default=os.environ.get("HOME"))
def backup(database_name, table, collection, t, d):
    print(database_name)
    try:
        with open(config_path+"/db_bkup/auth.json", "r") as auth_file:
            auth_obj = json.load(auth_file)
            match auth_obj["db"]:
                case "mongodb":
                    print("the user is using mongodb database")
                    if collection:
                        backup_mongodb(auth_obj["uri"], database_name, coll=collection)
                    else:
                        backup_mongodb(auth_obj["uri"], database_name, coll=None)
                    return
                case "postgresql":
                    table = True
                    print("the user is using postgres")
                    return
                case "mysql":
                    table = True
                    return
            print(click.style(f" • successfully backup database {t} and {d}", "green", underline=True))
    except FileNotFoundError as FnFe:
        print(FnFe)
        print(click.style("Couldn't get  your database try sync with your db again", "red", bold=True, underline=True))


if __name__ == "__main__":
    cli()
