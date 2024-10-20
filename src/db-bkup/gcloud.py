#!/usr/bin/env python3
from google.cloud import storage
import os
import typing
import click
import importlib
import re
import sys

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


def validate_authentication_credentials(host, port, username, password, db):
    matched = re.search(r"(?P<ip>\d{1,3}.\d{1,3}.\d{1,3}.\d{1,3})", host)
    host_length = len(host.split("."))
    try:
        if matched and host_length == 4 and int(port) and username and password and db:
            return True
        else:
            return False
    except ValueError:
        print("Bad host")


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
            pymongo = importlib.import_module("pymongo")
            client = pymongo.MongoClient("mongodb://192.168.64.2:27017/")
            db = client.test
            print(db.my_collection)
            inserted_id = db.my_collection.insert_one({"x": 10}).inserted_id
            print(inserted_id)
            return
        case "mysql":
            mysql_connector = importlib.import_module("click")
            # replace click with the appropriate module for mysql

            print("mysql is used")


@cli.command(help="Backup database on local device or cloud. Note, we currently support google cloud ")
@click.option("--t", default="full", type=click.Choice(["full", "increment"]))
@click.option("--d", default=os.environ.get("HOME"))
def backup(t, d):
    print(click.style(" • successfully backup database", "green", underline=True))


if __name__ == "__main__":
    cli()
