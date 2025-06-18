#!/usr/bin/env python3
# from google.cloud import storage
from cryptography.fernet import Fernet
import os
import click
import certifi
import pymongo
import json
import pymysql
import datetime
import psycopg2
import bson
from decimal import Decimal
from pymongo.server_api import ServerApi

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "./database-service-account-key.json"


supported_db = ["postgresql", "mongodb", "mysql"]
config_path = os.path.expanduser("~") + "/.config"

current_user_db = dict({"db": None})
warning = click.style("[!WARNING]", "yellow", bold=True, dim=True)
error = click.style("[ERROR]", "red", bold=True, dim=True)
success = click.style("[SUCCESS]", "green", bold=True, dim=True)
info = click.style("[INFO]", "green", bold=True, dim=True, underline=True)


@click.group()
def cli():
    """
    cli group object
    """


@cli.command()
@click.argument("hostname")
@click.argument("port")
@click.argument("username")
@click.argument("password")
@click.argument("db")
def connect_to_mysql(hostname, port, username, password, db, db_type="mysql"):
    """connect to Mysql Database"""
    connection = None
    print("working§")
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
            print("workers")
        else:
            connection = psycopg2.connect(
                host=hostname, database=db, user=username, password=password
            )

        print(
            click.style(
                f"{success} connected to mysql database",
                "green",
                bold=True,
                underline=True,
            )
        )
        if os.path.exists(config_path + "/db_bkup"):
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
            with open(
                config_path + "/db_bkup/auth.json", "w", encoding="utf-8"
            ) as auth_config:
                json.dump(current_user_db, auth_config)
    except pymysql.err.OperationalError as operation_e:
        print(
            click.style(
                f" x incorrect login credentials {operation_e} Try Again",
                "red",
                bold=True,
                underline=True,
            )
        )
    finally:
        if connection is not None:
            connection.close()


@cli.command()
@click.argument("uri")
def connect_postgres(uri):
    """
    connect PostgresQl

    """
    try:
        if os.path.exists(config_path):
            if os.path.exists(config_path + "/db_bkup"):
                conn = psycopg2.connect(uri)
                cur = conn.cursor()
                if not cur:
                    click.echo(
                        error
                        + click.style(
                            f"Couldn't resolve the uri {uri}", "red", bold=True
                        )
                    )
                else:
                    click.echo(
                        success
                        + click.style(
                            "connected to postgres db successfully", "green", bold=True
                        )
                    )

                with open(
                    config_path + "/db_bkup/auth.json", "w", encoding="utf-8"
                ) as cnf_file:
                    connection_cnf = {"uri": uri, "db": "postgresql"}
                    json.dump(connection_cnf, cnf_file)
            else:
                os.mkdir(config_path + "/db_bkup")
                connect_postgres(uri)
        else:
            os.makedirs(config_path + "/db_bkup")
            connect_postgres(uri)

    except psycopg2.OperationalError or psycopg2.ProgrammingError:
        print(f"could not resolve the uri {uri}")


@cli.command()
@click.argument("uri")
def connect_to_mongodb(uri):
    """
    connect mongodb database

    """
    global current_user_db
    try:
        pymongo.MongoClient(uri, tlscafile=certifi.where(), server_api=ServerApi("1"))
        current_user_db["db"] = "mongodb"
        current_user_db["uri"] = uri
        if os.path.exists(config_path + "/db_bkup"):
            with open(
                config_path + "/db_bkup/auth.json", "w", encoding="utf-8"
            ) as auth_file:
                json.dump(current_user_db, auth_file)
        else:
            os.makedirs(config_path + "/db_bkup")
            file = config_path + "/db_bkup/auth.json"
            with open(file, "w", encoding="uft-8") as conf:
                json.dump(current_user_db, conf)
        click.echo(
            success
            + click.style(
                " Connected to mongodb database successfully",
                "green",
                bold=True,
                dim=True,
            )
        )

    except pymongo.errors.ServerSelectionTimeoutError:
        print(
            click.style(
                " x Timeout Error : Ip address whitelisted. Allow all origin from mongodb database access",
                "red",
                dim=True,
                bold=True,
            )
        )


class JSONEncoder(json.JSONEncoder):
    """
    custom JSON encoder inherit native JSONEncoder and overrides
    it default using polymorphism

    """

    def default(self, o):
        if isinstance(o, bson.ObjectId):
            return str(o)
        if isinstance(o, datetime.datetime):
            return o.isoformat()
        if isinstance(o, Decimal):
            return float(0)
        return super().default(o)


def deserializer(d):
    """
    convert python data structure into  non-serializable object

    """
    for key, value in d.items():
        if isinstance(value, str) and len(str(value)) == 24 and value.isalnum():
            d[key] = bson.ObjectId(value)
            return d
        elif (
            isinstance(value, str)
            and value.endswith("Z")
            or isinstance(value, str)
            and "T" in value
        ):
            d[key] = datetime.datetime.fromisoformat(value)
            return d
        elif isinstance(value, float):
            d[key] = Decimal(str(float))
            return d
        else:
            return d


def save_sql_data_on_local(file_path, data, filename: str, db: str):
    """
    saves SQL data in local storage

    """
    if os.path.exists(file_path + "/db_bkup"):
        if os.path.exists(file_path + f"/db_bkup/{db}"):
            with open(
                file_path + "/db_bkup/{}/{}".format(db, filename + ".json"),
                "w",
                encoding="utf-8",
            ) as sql_db_bkup:
                json.dump(data, sql_db_bkup, cls=JSONEncoder)
        else:
            os.mkdir(file_path + f"/db_bkup/{db}")
            save_sql_data_on_local(file_path, data, filename, db)
    else:
        os.makedirs(file_path + "/db_bkup/{db}")
        save_sql_data_on_local(file_path, data, filename, db)


@cli.command()
@click.option("--path", help="path to restore database")
@click.option("--table", help="Provide Table name")
def backup_mysql(path, table: None | str = None):
    """
    backup mysql function

    """
    with open(config_path + "/db_bkup/auth.json", "r", encoding="utf-8") as auth_cnf:
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
            port=int(auth_cfg["port"]),
        )

        cursor = connectobj.cursor()
        if table:
            cursor.execute("SELECT * FROM  {}".format(table))
            data = cursor.fetchall()
            if path:
                save_sql_data_on_local(path, data, table, "mysql")
            else:
                save_sql_data_on_local(config_path, data, table, "mysql")
            return

        tables = cursor.execute("SHOW TABLES")
        tables = [table for table in cursor.fetchall()]
        table_map = {}

        click.echo(
            info + click.style("Select SQL Tables with digit  e.g 1 ", "red", bold=True)
        )
        for idx, table in enumerate(tables):
            for t in table.values():
                table_map[f"{idx+1}"] = t
            print(click.style(f"{idx+1:<10}" + t, "yellow", bold=True))

        selected_table = input("Which table do you want to backup:  ")

        if selected_table.lower() == "all":
            for t in table.values():
                backup_mysql(t)
                return

        try:
            click.echo(
                success
                + click.style(
                    "{} your selected database is {}".format(
                        success, table_map[selected_table]
                    ),
                    "green",
                    bold=True,
                )
            )
            cursor.execute("SELECT * fROM {}".format(table_map[selected_table]))
            data = cursor.fetchall()
            if path:
                save_sql_data_on_local(path, data, table_map[selected_table], "mysql")
            else:
                save_sql_data_on_local(
                    config_path, data, table_map[selected_table], "mysql"
                )
        except KeyError:
            click.echo(error + click.style(" Please select with numbers instead."))


@cli.command()
@click.argument("table")
@click.option("--path")
def backup_postgres(table, path):
    """
    backup postgres database

    """
    auth_file = read_auth_file()

    try:
        conn = psycopg2.connect(auth_file["uri"])
        cur = conn.cursor()
        cur.execute(f"SELECT * FROM {table}")
        dt = cur.fetchall()
        print(dt)
        file_path = os.path.expanduser(path or "~/.config")
        save_sql_data_on_local(file_path, dt, table, "postgres")

    except psycopg2.OperationalError:
        click.echo(
            error
            + click.style(
                "Couldn't reestablish the connection to your database, try connect again"
            )
        )


def save_mongodb_doc(file: str, document, coll):
    """
    save mongodb document to local file

    """
    data_struct = []
    for doc in document:
        jsonDoc = {"_id": doc["_id"], **doc}
        data_struct.append(jsonDoc)
        if len(data_struct) == 0:
            click.echo(
                warning
                + click.style(
                    f" Collection {coll} contains no document", "yellow", bold=True
                )
            )
            return
        with open(file, "w", encoding="utf-8") as default_bk_file:
            json.dump(data_struct, default_bk_file, cls=JSONEncoder)


def get_collection_and_save_doc(db, coll, path: str):
    """
    get collection and save doc in local file

    """
    collection = db[coll]
    document = collection.find()
    file_path = path or "~/.config"
    mongodb_default_backup_dir = os.path.expanduser(file_path + "/db_bkup/mongodb")
    backup_file = mongodb_default_backup_dir + "/{}".format(coll)
    if os.path.exists(mongodb_default_backup_dir):
        save_mongodb_doc(backup_file, document, collection)
    else:
        os.makedirs(mongodb_default_backup_dir)
        save_mongodb_doc(backup_file, document, collection)


@cli.command()
@click.option("--file_path")
@click.option("--db_name")
@click.option("--coll")
def backup_mongodb(file_path, db_name: str | None, coll: str | None):
    """
    backup mongodb database

    """
    auth_file = read_auth_file()
    mongo_c = pymongo.MongoClient(
        auth_file["uri"], tlsCAfile=certifi.where(), server_api=ServerApi("1")
    )
    db = mongo_c[db_name]
    if coll is not None:
        get_collection_and_save_doc(db, coll, file_path)
    else:
        collections = db.list_collection_names()
        if len(collections) == 0:
            click.echo(
                info
                + click.style(
                    f"specified database {db_name} has zero collections. create one on your mongodb cluster",
                    "green",
                    bold=True,
                )
            )
            return

        collection_map = {}
        print(
            click.style(
                "Select MongoDB collection with digit e.g 1 or All for backing up entire database",
                "red",
                bold=True,
                dim=True,
            )
        )

        for idx, c in enumerate(collections):
            collection_map[f"{idx+1}"] = c
            print(click.style(f"{idx+1:^10}" + c, "yellow", bold=True))
        selected_coll = input(
            "select a collection to back up or backup entire database: "
        )
        if selected_coll.lower() == "all":
            for col in collections:
                # backup_mongodb(db_name, col)
                get_collection_and_save_doc(db, col, file_path)
                click.echo(
                    success
                    + click.style(" Backup entire database successfully", "green")
                )
        else:
            try:
                get_collection_and_save_doc(
                    db, collection_map[selected_coll], file_path
                )
            except KeyError:
                print(
                    click.style(
                        f"{error} Value error,{selected_coll} not among option",
                        "red",
                        bold=True,
                    )
                )

    return


@cli.command(help="restore a mongodb database")
@click.argument("file")
@click.argument("database")
@click.argument("collection")
def restore_mongodb(file, database, collection):
    """restore mongodb"""
    try:
        uri = None
        coll = None
        with open(file, "r", encoding="utf-8") as json_file:
            auth_data = read_auth_file()
            uri = auth_data["uri"]
            client = pymongo.MongoClient(
                uri, tlscafile=certifi.where(), server_api=ServerApi("1")
            )
            db = client[database]
            coll = db[collection]

            loaded_data = json.load(json_file, object_hook=deserializer)
            print(loaded_data)
            if coll is not None and isinstance(loaded_data, list):
                return coll.insert_many(loaded_data)
            else:
                return coll.insert_one(loaded_data)
    except pymongo.errors.PyMongoError as pe:
        click.echo(
            error + " " + click.style(f"error occured while restoring database {pe}")
        )


@cli.command(help="backup mysql database")
@click.argument("file_path")
@click.argument("table")
def restore_mysql(file_path, table):
    """restore mongodb database"""
    try:
        data = read_auth_file()
        f = Fernet(data["key"])
        conn = pymysql.connect(
            host=data["host"],
            password=f.decrypt(data["password"].encode()),
            db=data["db_name"],
            user=data["username"],
            cursorclass=pymysql.cursors.DictCursor,
            read_timeout=10,
            connect_timeout=10,
            write_timeout=10,
            charset="utf8mb4",
            port=int(data["port"]),
        )
        curs = conn.cursor()
        file_data = read_backup_file(file_path)
        print(file_data)
        if not isinstance(file_data, list):
            raise ValueError("file  data is invalid or corrupted")

        columns = ", ".join(file_data[0].keys())
        placeholders = ", ".join(["%s"] * len(file_data[0]))
        print(columns, placeholders)
        q = f"INSERT into  {table} ({columns}) VALUES ({placeholders})"

        for data_record in file_data:
            try:
                curs.execute(q, list(data_record.values()))
            except pymysql.MySQLError as my_e:
                click.echo(
                    error
                    + click.style(f"error occured while restoring to database, {my_e}")
                )
        conn.commit()
        curs.close()
        conn.close()
    except pymongo.errors.PyMongoError as pe:
        click.echo(
            error + " " + click.style(f"error occured while restoring database {pe}")
        )


@cli.command()
@click.argument("filepath")
@click.argument("table")
def restore_postgres(filepath, table):
    """restore postgres"""
    try:
        auth_file = read_auth_file()
        conn = psycopg2.connect(auth_file["uri"])
        cur = conn.cursor()
        cur.execute(f"SELECT * FROM {table}")
        file_data = read_backup_file(filepath)
        delete_table_query = """ DROP TABLE IF EXISTS users """
        cur.execute(delete_table_query)
        create_table_query = """
    CREATE TABLE IF NOT EXISTS users (
    id SERIAL PRIMARY KEY,
    username VARCHAR(50) NOT NULL,
    email VARCHAR(100) NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    """

        cur.execute(create_table_query)

        insert_query = """
        INSERT INTO users (username, email)
        VALUES (%s, %s)
        RETURNING id;
        """

        datas = []
        for datum in file_data:
            ex_data = (datum[1], datum[2])
            datas.append(ex_data)
        print(datas)
        cur.executemany(insert_query, datas)
        print("successfully add data to postgres database", datas)
        conn.commit()
    except pymongo.errors.PyMongoError as pe:
        click.echo(
            error + " " + click.style(f"error occured while restoring database {pe}")
        )


def read_backup_file(file_path):
    """read backup file helper function"""
    with open(file_path, "r", encoding="utf-8") as f:
        loaded_file = json.load(f, object_hook=deserializer)
        return loaded_file


def read_auth_file():
    with open(config_path + "/db_bkup/auth.json", "r", encoding="utf-8") as auth_config:
        auth_data = json.load(auth_config)
        return auth_data


if __name__ == "__main__":
    cli()
