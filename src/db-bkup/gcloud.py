#!/usr/bin/env python3
from google.cloud import storage
from mysql_operator import BackupOperator
from cryptography.fernet import Fernet
import os
import typing
import click
import importlib
import re
import certifi
import pymongo
import json
import bson
import pymysql
import datetime


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
warning = click.style("[!WARNING]", "yellow", bold=True, dim=True)
error = click.style("[ERROR]", "red", bold=True, dim=True)
success = click.style("[SUCCESS]", "green", bold=True, dim=True)
info = click.style("[INFO]", "green", bold=True, dim=True, underline=True)


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

def connect_to_mysql(hostname, port, username, password,db):
    # backup_operator = BackupOperator(hostname=hostname, port=port, username=username, password=password)
    connection = None
    try:
        timeout = 10
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

        #cursor = connection.cursor()
        #cursor.execute("INSERT INTO mytest (id) VALUES (1), (2)")
        #connection.commit()
        #cursor.execute("SELECT * FROM mytest")
        #print(cursor.fetchall())


        with connection.cursor() as cursor:
            create_table_query = """
        CREATE TABLE IF NOT EXISTS users (
            id INT AUTO_INCREMENT PRIMARY KEY,
            name VARCHAR(50) NOT NULL,
            email VARCHAR(100) NOT NULL,
            age INT
        );
        """

            cursor.execute(create_table_query)
            insert_data_query = "INSERT INTO users (name, email, age) VALUES (%s, %s, %s);"
            user_data = [
            ("John Doe", "john.doe@example.com", 30),
            ("Jane Smith", "jane.smith@example.com", 28),
            ("Alice Johnson", "alice.j@example.com", 25)
            ]

            cursor.executemany(insert_data_query, user_data)

        # Commit the transaction
            connection.commit()
            print("Data inserted successfully.")
            print("Table created successfully.")
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


def connect_to_mongodb(uri):
    global current_user_db
    try:
        pymongo.MongoClient(uri, tlscafile=certifi.where())
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
        if isinstance(o, datetime):
            return str(o)
        return super().default(o)


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
            print(data)
            # save_dir()
            return

        tables = cursor.execute("SHOW TABLES")
        tables = [table for table in cursor.fetchall()]
        table_map = {}

        print(click.style("Select SQL Tables with digit  e.g 1 ", "red", bold=True, dim=True))
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
            print(click.style("{} your selected database is {}".format(success, table_map[selected_table]), "green", bold=True))
            cursor.execute("SELECT * fROM {}".format(table_map[selected_table]))
            data = cursor.fetchall()
            print(data)
        except KeyError:
            click.echo(error + click.style(" Please select with numbers instead."))

#        save_data()

#        subprocess.run([f"echo {idx+1}"])
        # cursor.execute("SELECT * FROM {}".format(tables[0]["Tables_in_defaultfb"]))
#        data = cursor.fetchall()
        print()

def backup_mongodb(uri, db_name:str | None, *, coll: str | None):
    mongo_c = pymongo.MongoClient(uri, tlscafile=certifi.where())
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
   # is_validated = validate_authentication_credentials(host, port, username, password, db)
    #if is_validated:
     #   print("validated")
    #else:
     #   print("bad payload, wrong data")
      #  sys.exit()


    match db:
        case "postgresql":
            print("postgresql is used")
            postgres = importlib.import_module("click")
            # replace click with the appropriate module for postgres
            print(postgres.command())
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

            connect_to_mysql(host, port, username, password, sqldbname)
            return


@cli.command(help="Backup entire database on local device or cloud. Note, we currently support google cloud ")
@click.option("--database_name")
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
                    elif database_name:
                        backup_mongodb(auth_obj["uri"], database_name, coll=None)
                    else:
                        print(click.style("You did pass the options to backup a mongoDB  database. RUN db_bkup backup --help", "red", bold=True))
                    return
                case "postgresql":
                    table = True
                    print("the user is using postgres")
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


if __name__ == "__main__":
    cli()
