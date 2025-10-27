#!/usr/bin/env python3

from google.cloud.sql.connector import Connector
import pymysql
import sqlalchemy as db

from sqlalchemy.engine import Connection, Engine


def get_gbc_connection(test: bool = False, readonly: bool = True, sqluser: str = "gbcreader", sqlpass: str = None) -> tuple[Connector, Engine, Connection]:
    """Get a connection to the GBC Google Cloud SQL instance.

    Args:
        test (bool): Whether to connect to the test database. Defaults to False.
        readonly (bool): Whether to connect in read-only mode. Defaults to True.
        sqluser (str): The SQL username to connect with. Defaults to "gbcreader".
        sqlpass (str): The SQL password to connect with. Required if readonly is False.

    Returns:
        tuple[Connector, Engine, Connection]: A tuple containing the Google Cloud SQL Connector, SQLAlchemy Engine, and SQLAlchemy Connection objects.
    """

    if not readonly and not sqlpass:
        raise ValueError("You must provide a SQL user credentials if not in readonly mode.")

    database = "gbc-publication-analysis:europe-west2:gbc-sql/gbc-publication-analysis"
    database += "-test" if test else ""
    instance, db_name = database.split('/')

    gcp_connector = Connector()
    def getcloudconn() -> pymysql.connections.Connection:
        conn: pymysql.connections.Connection = gcp_connector.connect(
            instance, "pymysql",
            user=sqluser,
            password=sqlpass,
            db=db_name
        )
        return conn

    cloud_engine = db.create_engine("mysql+pymysql://", creator=getcloudconn, pool_recycle=60 * 5, pool_pre_ping=True)
    cloud_engine.execution_options(isolation_level="READ COMMITTED")
    return (gcp_connector, cloud_engine, cloud_engine.connect())