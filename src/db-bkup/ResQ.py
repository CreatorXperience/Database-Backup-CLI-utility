#!/usr/bin/env python3
# from google.cloud import storage
from cryptography.fernet import Fernet
import os
import typing
import click
import importlib
import re
import certifi
import pymongo
import json
import pymysql
import datetime
import psycopg2
import bson
from decimal import Decimal
import certifi
from pymongo.server_api import ServerApi
import subprocess


os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "./database-service-account-key.json"


#def create_bucket(bkt_name: str):
#    client: typing.Any = storage.Client()
#    new_bucket = client.bucket(bkt_name)
#    new_bucket.storage_class("COLDLINE")
#   bucket = client.create_bucket(new_bucket, location="us")
#    print(f'''successfully created {bucket.name},{bucket.location}
#         ,{bucket.storage_class}''')

#   return bucket


supported_db = ["postgresql", "mongodb", "mysql"]
config_path = os.path.expanduser("~")+"/.config"

current_user_db = dict({
        "db": None
        })
warning = click.style("[!WARNING]", "yellow", bold=True, dim=True)
error = click.style("[ERROR]", "red", bold=True, dim=True)
success = click.style("[SUCCESS]", "green", bold=True, dim=True)
info = click.style("[INFO]", "green", bold=True, dim=True, underline=True)



def connect_to_mysql(hostname, port, username, password,db, db_type):
    connection = None
    try:
        timeout = 10
        if db_type == "mysql":
            connection = pymysql.connect(
            charset="utf8mb4",
            connect_timeout=timeout,
            cursorclass=pymysql.cursors.DictCursor,
            db=db,
            host=hostname,
            password=password,
            read_timeout=timeout,
            port=int(port),
            user=username,
            write_timeout=timeout,
            )
        else:
            connection = psycopg2.connect(
            host=hostname,
            database=db,
            user=username,
            password=password
            )

        
        print(click.style(f"{success} connected to mysql database", "green", bold=True, underline=True))
        if os.path.exists(config_path+"/db_bkup"):
            current_user_db["db"] = "mysql"
            current_user_db["port"] = port
            current_user_db["db_name"] = db
            current_user_db["username"] = username
            current_user_db["host"] = hostname
            key = Fernet.generate_key()
            f = Fernet(key)
            token = f.encrypt(password.encode())
            current_user_db["password"] = token.decode()
            current_user_db["key"] = key.decode()
            with open(config_path+"/db_bkup/auth.json", "w") as  auth_config:
                json.dump(current_user_db, auth_config)
    except pymysql.err.OperationalError as OE:
        print(click.style(f" x incorrect login credentials {OE} Try Again", "red" , bold=True, underline=True))
    finally:
        if connection is not None:
            connection.close()
        else:
            return


def connect_postgres(uri):
    try:
        if os.path.exists(config_path):
            if os.path.exists(config_path+"/db_bkup"):
                conn = psycopg2.connect(uri)
                cur = conn.cursor()
                if not cur:
                    click.echo(error+click.style(f"Couldn't resolve the uri {uri}","red", bold=True))
                else:
                    click.echo(success+click.style("connected to postgres db successfully", "green", bold=True))

                with open(config_path+"/db_bkup/auth.json", "w") as cnf_file:
                    connection_cnf = {
                                "uri": uri,
                                "db": "postgresql"
                                }
                    json.dump(connection_cnf, cnf_file)
            else:
                os.mkdir(config_path+"/db_bkup")
                connect_postgres(uri)
        else:
            os.makedirs(config_path+"/db_bkup")
            connect_postgres(uri)

    except psycopg2.OperationalError  or psycopg2.ProgrammingError:
        print(f"could not resolve the uri {uri}")


def connect_to_mongodb(uri):
    global current_user_db
    try:
        pymongo.MongoClient(uri, tlscafile=certifi.where(), server_api=ServerApi("1"))
        current_user_db["db"] = "mongodb"
        current_user_db["uri"] = uri
        if os.path.exists(config_path+"/db_bkup"):
            with open(config_path+"/db_bkup/auth.json", "w") as auth_file:
                json.dump(current_user_db, auth_file)
        else:
            os.makedirs(config_path+"/db_bkup")
            file = config_path+"/db_bkup/auth.json"
            with open(file, "w") as conf:
                json.dump(current_user_db, conf)
        click.echo(success + click.style(" Connected to mongodb database successfully",
                          "green", bold=True, dim=True))

    except pymongo.errors.ServerSelectionTimeoutError:
        print(click.style(" x Timeout Error : Ip address whitelisted. Allow all origin from mongodb database access", "red",
                          dim=True, bold=True))


class JSONEncoder(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, bson.ObjectId):
            return str(o)
        if isinstance(o, datetime.datetime):
            return o.isoformat()
        if isinstance(o, Decimal):
            return float(0)
        return super().default(o)



def deserializer(d):
    for key, value in d.items():
        if len(value) == 24 and value.isalnum() and isinstance(value, str):
            d[key] = bson.ObjectId(value)
            return d
        elif isinstance(value, str) and  value.endswith("Z") or "T" in value:
            d[key] = datetime.datetime.fromisoformat(value)
        elif isinstance(value, float):
            d[key] = Decimal(str(float))



def save_sql_data_on_local(data, filename: str, db: str):
    if os.path.exists(config_path+"/db_bkup"):
        if os.path.exists(config_path+f"/db_bkup/{db}"):
            with open(config_path+"/db_bkup/{}/{}".format(db,filename+".json"), "w") as sql_db_bkup:
                json.dump(data, sql_db_bkup, cls=JSONEncoder)
        else:
            os.mkdir(config_path+f"/db_bkup/{db}")
            save_sql_data_on_local(data, filename, db)
    else:
        os.makedirs(config_path+"/db_bkup/{db}")
        save_sql_data_on_local(data, filename, db)



def backup_mysql(table: None | str = None):

    with open(config_path+"/db_bkup/auth.json", "r") as auth_cnf:
        auth_cfg = json.load(auth_cnf)
        f = Fernet(auth_cfg["key"].encode())
        connectobj = pymysql.connect(
                host=auth_cfg["host"],
                password=f.decrypt(auth_cfg["password"].encode()),
                database=auth_cfg["db_name"],
                user=auth_cfg["username"],
                cursorclass=pymysql.cursors.DictCursor,
                read_timeout=10,
                connect_timeout=10,
                charset="utf8mb4",
                port=int(auth_cfg["port"])
                )

        cursor = connectobj.cursor()
        if table:
            cursor.execute("SELECT * FROM  {}".format(table))
            data = cursor.fetchall()
            save_sql_data_on_local(data, table, "mysql")
            return

        tables = cursor.execute("SHOW TABLES")
        tables = [table for table in cursor.fetchall()]
        table_map = {}

        click.echo(info+click.style("Select SQL Tables with digit  e.g 1 ", "red", bold=True))
        for idx, table in enumerate(tables):
            for t in table.values():
                table_map[f"{idx+1}"] = t
            print(click.style(f"{idx+1:<10}"+t,  "yellow", bold=True))

        selected_table = input("Which table do you want to backup:  ")

        if selected_table.lower() == "all":
            for t in table.values():
                backup_mysql(t)
                return

        try:
            click.echo(success+click.style("{} your selected database is {}".format(success, table_map[selected_table]), "green", bold=True))
            cursor.execute("SELECT * fROM {}".format(table_map[selected_table]))
            data = cursor.fetchall()
            save_sql_data_on_local(data, table_map[selected_table], "mysql")
        except KeyError:
            click.echo(error + click.style(" Please select with numbers instead."))

def backup_postgres(uri, table):
    try:
        conn = psycopg2.connect(uri)
        cur  = conn.cursor()
        cur.execute(f"SELECT * FROM {table}")
        dt = cur.fetchall()
        print(dt)
        save_sql_data_on_local(dt, table, "postgres")

    except psycopg2.OperationalError:
        click.echo(error+click.style("Couldn't reestablish the connection to your database, try connect again"))


     
def backup_mongodb(uri, db_name:str | None, *, coll: str | None):

    mongo_c = pymongo.MongoClient(uri, tlsCAfile=certifi.where(), server_api=ServerApi("1"))
    if db_name is not None:
        db = mongo_c[db_name]
    if coll is not None:
        collection = db[coll]
        document = collection.find()
        mongodb_default_backup_dir = os.path.expanduser("~/.config/db_bkup/mongodb")
        backup_file = mongodb_default_backup_dir+"/{}".format(coll)
        if os.path.exists(mongodb_default_backup_dir):
            data_struct  = []
            for doc in document:
                jsonDoc = {
                    '_id': doc["_id"],
                    **doc
                    }
                data_struct.append(jsonDoc)
            if len(data_struct) == 0:
                click.echo(warning + click.style(f" Collection {coll} contains no document", "yellow", bold=True))
                return
            with open(backup_file, "w") as default_bk_file:
                json.dump(data_struct, default_bk_file, cls=JSONEncoder)
        else:
            os.mkdir(mongodb_default_backup_dir)
            with open(backup_file, "w") as default_bk_file:
                json.dump(document, default_bk_file)

    else:
        collections = db.list_collection_names()
        if len(collections) == 0:
            click.echo(info+click.style(f"specified database {db_name} has zero collections. create one on your mongodb cluster",  "green", bold=True))
            return

        collection_map = {}
        print(click.style("Select MongoDB collection with digit e.g 1 or All for backing up entire database", "red", bold=True, dim=True))

        for idx, c in enumerate(collections):
            collection_map[f"{idx+1}"] = c
            print(click.style(f"{idx+1:^10}"+ c, "yellow", bold=True))
        selected_coll = input("select a collection to back up or backup entire database: ")
        if selected_coll.lower() == "all":
            for col in collections:
                backup_mongodb(uri, db_name, coll=col)
                click.echo(success + click.style(" Backup entire database successfully", "green"))
        else:
            try:
                backup_mongodb(uri, db_name, coll=collection_map[selected_coll])
            except KeyError:
                print(click.style(f"{error} Value error,{selected_coll} not among option", "red", bold=True))

    return


@click.group()
def cli():
    pass


@cli.command(help='''Connect to your prefered database.
            We currently support this databases 'MYSQL', 'POSTGRESQL', 'MONGODB'
             ''')
@click.option("--db", prompt=True,
              type=click.Choice(["mongodb", "postgresql", "mysql"]))
@click.option("--username", required=False)
@click.option("--password", required=False)
@click.option("--uri")
@click.option("--host", help="connect to database  via host ip address", required=False)
@click.option("--port", required=False)
@click.option("--sqldbname", help="Provide sql database name", required=False)
def sync(username, password, uri, host, port, db, sqldbname):
    match db:
        case "postgresql":
            connect_postgres(uri)
            return
        case "mongodb":
            if uri:
                connect_to_mongodb(uri)
            else:
                print(click.style("mongodb uri not provided. Run db_bkup sync --help", "red", bold=True, underline=True))
        case "mysql":
            if db == "mysql" and None in list([username, password, host, port, sqldbname]):
                print(click.style(f"Host not found. Run db_bkup sync --help", "red", bold=True, underline=True))
                raise click.ClickException("options to connect to your mysql database not provided or incomplete")
                return

            connect_to_mysql(host, port, username, password, sqldbname, "postgres")
            return


@cli.command(help="Backup entire database on local device or cloud. Note, we currently support google cloud ")
@click.option("--database_name")
@click.option("--table", help="Backup table in a SQL database")
@click.option("--collection", help="Backup collection for MongoDB database")
@click.option("--t", default="full", type=click.Choice(["full", "increment"]))
@click.option("--d", default=os.environ.get("HOME"))
def backup(database_name, table, collection, t, d):
    try:
        with open(config_path+"/db_bkup/auth.json", "r") as auth_file:
            auth_obj = json.load(auth_file)
            match auth_obj["db"]:
                case "mongodb":
                    if collection  and database_name:
                        backup_mongodb(auth_obj["uri"], database_name, coll=collection)
                    elif database_name and not collection:
                        backup_mongodb(auth_obj["uri"], database_name, coll=None)
                    else:
                        click.echo(error + " " + click.style("You did pass the options to backup a mongoDB  database. RUN db_bkup backup --help", "red", bold=True))
                    return
                case "postgresql":
                    if table:
                        backup_postgres(auth_obj["uri"], table)
                    else:
                        click.echo(error+ " " + click.style("table to backup not provided"))
                    return
                case "mysql":
                    if table and not collection:
                        backup_mysql(table)
                    else:
                        backup_mysql()
                    return
            print(click.style(f" • successfully backup database {t} and {d}", "green", underline=True))
    except FileNotFoundError as FnFe:
        print(FnFe)
        print(click.style("Couldn't get  your database try sync with your db again", "red", bold=True, underline=True))


@cli.command(help="restore a mongodb database")
@click.argument("file")
@click.argument("database")
@click.argument("collection")
def restore_mongodb(file, database, collection):
    try:
        uri = None
        coll = None
        with open(file,  "r") as json_file:
            auth_data = read_auth_file()
            uri = auth_data["uri"]
            client = pymongo.MongoClient(uri, tlscafile=certifi.where(), server_api=ServerApi("1"))
            db = client[database]
            coll = db[collection]

            loaded_data = json.load(json_file, object_hook=deserializer)
            print(loaded_data)
            if coll is not None  and  isinstance(loaded_data, list):
                return  coll.insert_many(loaded_data)
            else:
               return coll.insert_one(loade)
    except pymongo.errors.PyMongoError as pe:
        click.echo(error+" " + click.style(f"error occured while restoring database {pe}"))
                

@click.command(help="backup mysql database")
@click.argument("`file_path")
@click.argument("table")
def restore_mysql(file_path):
    try:
        data =  read_auth_file()
        conn = pymysql.connect(
                host=data["host"],
                password=f.decrypt(data["password"].encode()),
                database=data["db_name"],
                user=data["username"],
                cursorclass=pymysql.cursors.DictCursor,
                read_timeout=10,
                connect_timeout=10,
                charset="utf8mb4",
                port=int(data["port"])
                )
        curs = conn.cur()
        file_data = read_backup_file(file_path)
        if not  isinstance(file_data, list):
            raise ValueError("file  data is invalid or corrupted")
        
        columns = ", ".join(file[0].keys())
        placeholders = "".join(["%s"] * len(file[0].keys()))
        insert = "INSERT into  {table} "


def read_backup_file(file_path):
    with open(file_path,  "r") as f:
        loaded_file = json.load(f, object_hook=deserializer)
        return loaded_file




def read_auth_file():
    with open(config_path+"/db_bkup/auth.json", "r") as auth_config:
        auth_data = json.load(auth_config)
        return auth_data


if __name__ == "__main__":
    cli()
