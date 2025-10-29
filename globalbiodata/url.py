from __future__ import annotations

from dataclasses import dataclass
from .utils_db import insert_into_table, delete_from_table
from .utils_fetch import fetch_url, fetch_connection_status

from typing import Optional
from sqlalchemy.engine import Connection, Engine
from datetime import datetime
import sqlalchemy as db

@dataclass
class URL:
    """
    Class to represent URL objects and associated information

    Attributes:
        id (int): Database ID for URL
        url (str): URL string
        url_country (str): Country of URL
        url_coordinates (str): Coordinates of URL
        status (list[ConnectionStatus]): ConnectionStatus object(s)
        wayback_url (str): URL for Wayback Machine
    """
    id:int
    url:str
    url_country:str
    url_coordinates:str
    url_status:str
    status:list[ConnectionStatus]
    wayback_url:str

    def __init__(self, u):
        self.id = u.get('id')
        self.url = u.get('url')
        self.url_country = u.get('url_country')
        self.url_coordinates = u.get('url_coordinates')
        self.wayback_url = u.get('wayback_url')

        # ConnectionStatus can either come as a list of dicts, a list of ConnectionStatus objects
        # or status fields directly in the url object.
        # End result should be a list of ConnectionStatus objs.
        if (not u.get('status') or len(str(u.get('status'))) == 0) and not u.get('url_status'):
            cs = []

        if u.get('status') and type(u.get('status')) is not list:
            u['status'] = [u['status']]

        if type(u.get('status')) is list:
            if type(u.get('status')[0]) is dict:
                cs = [ConnectionStatus(s) for s in u.get('status')]
            elif type(u.get('status')[0]) is ConnectionStatus:
                cs = u.get('status')
        elif u.get('url_status'):
            cs = [ConnectionStatus({'url_id':self.id, 'status':u.get('url_status'), 'date':u.get('connection_date')})]
        else:
            cs = []
        self.status = cs

    def __str__(self):
        url_str = ', '.join([
            f"id={self.id}", f"url={self.url}", f"url_country={self.url_country}",
            f"url_coordinates={self.url_coordinates}", f"wayback_url={self.wayback_url}",
            f"status=[{', '.join([s.__str__() for s in self.status])}]" if self.status else "status=[]"
        ])
        return f"URL({url_str})"

    def write(self, conn: Optional[Connection] = None, engine: Optional[Engine] = None, debug: bool = False) -> int:
        """Write URL to database along with associated ConnectionStatus data.

        Args:
            conn (Optional[Connection], optional): SQLAlchemy Connection object.
            engine (Optional[Engine], optional): SQLAlchemy Engine object.
            debug (bool, optional): If `True`, print debug information.

        Returns:
            The ID of the URL written to the database.
        """
        conn_statuses = self.status
        d = self.__dict__
        del d['status']
        new_url_id = insert_into_table('url', d, conn=conn, engine=engine, debug=debug)
        self.id = new_url_id
        for c in conn_statuses:
            c.url_id = self.id
            c.write(conn=conn, engine=engine, debug=debug)
        self.status = conn_statuses
        return self.id

    def delete(self, conn: Optional[Connection] = None, engine: Optional[Engine] = None, debug: bool = False) -> int:
        """Delete URL from database along with associated ConnectionStatus data.

        Args:
            conn (Optional[Connection], optional): SQLAlchemy Connection object.
            engine (Optional[Engine], optional): SQLAlchemy Engine object.
            debug (bool, optional): If `True`, print debug information.

        Returns:
            The number of rows deleted.
        """
        if conn is None:
            conn = engine.connect()

        if not self.id:
            raise ValueError("URL object must have an ID to delete.")

        delete_from_table('connection_status', {'url_id':self.id}, conn=conn, engine=engine, debug=debug)
        del_result = delete_from_table('url', {'id':self.id}, conn=conn, engine=engine, debug=debug)
        return del_result

    def fetch_by_id(url_id: int, conn: Optional[Connection] = None, engine: Optional[Engine] = None, debug: bool = False) -> URL:
        """Fetch URL from database by ID.

        Args:
            url_id (int): ID of the URL to fetch.
            conn (Optional[Connection], optional): SQLAlchemy Connection object.
            engine (Optional[Engine], optional): SQLAlchemy Engine object.
            debug (bool, optional): If `True`, print debug information.

        Returns:
            The fetched URL object.
        """
        return fetch_url({'id': url_id}, conn=conn, engine=engine, debug=debug)

    def fetch_by_url(url: str, conn: Optional[Connection] = None, engine: Optional[Engine] = None, debug: bool = False) -> Optional[URL]:
        """Fetch URL from database by URL string.

        Args:
            url (str): URL string to fetch.
            conn (Optional[Connection], optional): SQLAlchemy Connection object.
            engine (Optional[Engine], optional): SQLAlchemy Engine object.
            debug (bool, optional): If `True`, print debug information.

        Returns:
            The fetched URL object if one result, else list of URLs, else `None`.
        """
        return fetch_url({'url': url}, conn=conn, engine=engine, debug=debug)


    def latest_connection_status(self) -> ConnectionStatus:
        """Get the latest ConnectionStatus object for this URL.

        Returns:
            The latest ConnectionStatus object or `None` if not found.
        """
        if not self.status or len(self.status) == 0:
            return None
        latest_status = self.status[0]
        if latest_status.is_latest != 1:
            for s in self.status:
                if s.is_latest == 1:
                    latest_status = s
                    break
        return latest_status

    def is_online(self) -> bool:
        """Return boolean describing whether URL is online based on latest ConnectionStatus.

        Returns:
            `True` if URL is online, `False` otherwise.
        """
        latest_status = self.latest_connection_status()
        return True if (latest_status and latest_status.is_online) else False

@dataclass
class ConnectionStatus:
    """
    Class to represent status of a ping of a URL

    Attributes:
        url_id (int): Database ID for URL
        status (str): Code returned from connection
        date (datetime): Date of connection
        is_online (bool): boolean describing whether return code indicates resource is online
        is_latest (bool): boolean value describing whether this is the most recent connection attempt
    """
    url_id:int
    status:str
    date:datetime
    is_online:bool
    is_latest:bool

    def __init__(self, c):
        self.url_id = c.get('url_id')
        self.status = str(c.get('status'))
        self.date = c.get('connection_date') or c.get('date')
        self.is_latest = c.get('is_latest', 0)

        if not self.date:
            self.date = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            self.is_latest = 1
        else:
            self.date = datetime.strptime(self.date, "%Y-%m-%d %H:%M:%S") if type(self.date) is str else self.date

        if c.get('is_online') is None:
            self.is_online = self.status[:18] not in ['404', '500', 'HTTPConnectionPool']
        else:
            self.is_online = c.get('is_online')

    def __str__(self):
        status_str = f"ConnectionStatus(url_id={self.url_id}, status={self.status}, date={self.date}, is_online={self.is_online}, is_latest={self.is_latest})"
        return status_str

    def write(self, conn: Optional[Connection] = None, engine: Optional[Engine] = None, debug: bool = False) -> int:
        """Write ConnectionStatus to database.

        Args:
            conn (Optional[Connection], optional): SQLAlchemy Connection object.
            engine (Optional[Engine], optional): SQLAlchemy Engine object.
            debug (bool, optional): If `True`, print debug information.

        Returns:
            The ID of the ConnectionStatus written to the database.
        """
        # update is_latest to 0 for other connection statuses
        if self.is_latest:
            lconn = engine.connect()
            lconn.execute(db.text(f"UPDATE connection_status SET is_latest = 0 WHERE url_id = {self.url_id}"))
            lconn.commit()

        insert_into_table('connection_status', self.__dict__, conn=conn, engine=engine, debug=debug)

    def delete(self, conn: Optional[Connection] = None, engine: Optional[Engine] = None, debug: bool = False) -> int:
        """Delete ConnectionStatus from database.

        Args:
            conn (Optional[Connection], optional): SQLAlchemy Connection object.
            engine (Optional[Engine], optional): SQLAlchemy Engine object.
            debug (bool, optional): If `True`, print debug information.

        Returns:
            Number of rows deleted.
        """
        if conn is None:
            conn = engine.connect()

        if not self.url_id:
            raise ValueError("ConnectionStatus object must have a URL ID to delete.")

        del_result = delete_from_table('connection_status', {'url_id':self.url_id, 'date':self.date}, conn=conn, engine=engine, debug=debug)
        return del_result

    def fetch_by_url_id(url_id: int, conn: Optional[Connection] = None, engine: Optional[Engine] = None, debug: bool = False) -> list:
        """Fetch ConnectionStatus from database by URL ID.

        Args:
            url_id (int): URL ID of the ConnectionStatus to fetch.
            conn (Optional[Connection], optional): SQLAlchemy Connection object.
            engine (Optional[Engine], optional): SQLAlchemy Engine object.
            debug (bool, optional): If `True`, print debug information.

        Returns:
            The list of fetched ConnectionStatus objects.
        """
        return fetch_connection_status({'url_id': url_id}, conn=conn, engine=engine, debug=debug)
