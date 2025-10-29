from __future__ import annotations

from dataclasses import dataclass
from .utils_db import insert_into_table, delete_from_table
from .utils_fetch import fetch_version

from typing import Optional
from sqlalchemy.engine import Connection, Engine
from datetime import datetime, date

@dataclass
class Version:
    """
    Class to represent Version information

    Attributes:
        id (int): Database ID for Version
        name (str): Name of version/pipeline/type
        date (date): Date of run
        user (str): User who ran pipeline
        additional_metadata (dict): Additional data in JSON format
    """
    id:int
    name:str
    date:date
    user:str
    additional_metadata:dict

    def __init__(self, p):
        self.id = p.get('id')
        self.name = p.get('version_name') or p.get('name')
        self.date = p.get('version_date') or p.get('date')
        self.user = p.get('version_user') or p.get('user')
        self.additional_metadata = p.get('additional_version_metadata') or p.get('additional_metadata')

        self.date = datetime.strptime(self.date, "%Y-%m-%d").date() if self.date and type(self.date) is str else self.date

    def __str__(self):
        version_str = f"Version(id={self.id}, name={self.name}, date={self.date}, user={self.user}, additional_metadata={self.additional_metadata})"
        return version_str

    def write(self, conn: Optional[Connection] = None, engine: Optional[Engine] = None, debug: bool = False) -> int:
        """Write Version to database.

        Args:
            conn (Optional[Connection], optional): SQLAlchemy Connection object.
            engine (Optional[Engine], optional): SQLAlchemy Engine object.
            debug (bool, optional): If `True`, print debug information.

        Returns:
            The ID of the Version written to the database.
        """
        new_version_id = insert_into_table('version', self.__dict__, conn=conn, engine=engine, debug=debug)
        self.id = new_version_id
        return self.id

    def delete(self, conn: Optional[Connection] = None, engine: Optional[Engine] = None, debug: bool = False) -> int:
        """Delete Version from database.

        Args:
            conn (Optional[Connection], optional): SQLAlchemy Connection object.
            engine (Optional[Engine], optional): SQLAlchemy Engine object.
            debug (bool, optional): If `True`, print debug information.

        Returns:
            Number of rows deleted.
        """
        if conn is None:
            conn = engine.connect()

        if not self.id:
            raise ValueError("Version object must have an ID to delete.")

        del_result = delete_from_table('version', {'id':self.id}, conn=conn, engine=engine, debug=debug)
        return del_result

    def fetch_by_id(version_id: int, conn: Optional[Connection] = None, engine: Optional[Engine] = None, debug: bool = False) -> Version:
        """Fetch Version from database by ID.

        Args:
            version_id (int): ID of the Version to fetch.
            conn (Optional[Connection], optional): SQLAlchemy Connection object.
            engine (Optional[Engine], optional): SQLAlchemy Engine object.
            debug (bool, optional): If `True`, print debug information.

        Returns:
            The fetched Version object.
        """
        return fetch_version({'id': version_id}, conn=conn, engine=engine, debug=debug)