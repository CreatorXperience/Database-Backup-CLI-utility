#!/usr/bin/env python3
from google.cloud import storage
import os
import typing
import click

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


@click.command()
@click.argument("username")
@click.argument("password")
@click.option("--host", prompt=True)
@click.option("--port", prompt=True)
@click.option("--db", prompt=True,
              type=click.Choice(["mongodb", "postgresql", "mysql"]))
def connect_to_db(host, port, username, password, db):
    click.echo(host)


if __name__ == "__main__":
    connect_to_db()
