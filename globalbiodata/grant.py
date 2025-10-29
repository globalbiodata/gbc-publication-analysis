from __future__ import annotations

from dataclasses import dataclass

from .utils_db import insert_into_table, delete_from_table
from .utils_fetch import fetch_grant, fetch_grant_agency

from typing import Optional
from sqlalchemy.engine import Connection, Engine

@dataclass
class Grant:
    """ Class representing a Grant with associated metadata and linked GrantAgency.

    Attributes:
        id (int): Database ID for Grant
        ext_grant_id (str): External grant ID
        grant_agency (GrantAgency): GrantAgency object
    """
    id:int
    ext_grant_id:str
    grant_agency:GrantAgency

    def __init__(self, g):
        self.id = g.get('id')
        self.ext_grant_id = g.get('ext_grant_id')

        if type(g.get('grant_agency')) is str:
            self.grant_agency = GrantAgency({'name': g.get('grant_agency')})
        elif type(g.get('grant_agency')) is GrantAgency:
            self.grant_agency = g.get('grant_agency')
        else:
            raise ValueError(f"Grant Agency must be a string or GrantAgency object. Got: {g.get('grant_agency')} (type:{type(g.get('grant_agency'))}).")

    def __str__(self):
        grant_str = f"Grant(id={self.id}, ext_grant_id={self.ext_grant_id}, grant_agency={self.grant_agency.__str__()})"
        return grant_str

    def write(self, conn: Optional[Connection] = None, engine: Optional[Engine] = None, debug: bool = False, force: bool = False) -> int:
        """Write Grant to database along with associated GrantAgency data.

        Args:
            conn (Optional[Connection], optional): SQLAlchemy Connection object.
            engine (Optional[Engine], optional): SQLAlchemy Engine object.
            debug (bool, optional): If `True`, print debug information.
            force (bool, optional): If `True`, force writing of associated objects even if they have IDs.

        Returns:
            The ID of the Grant written to the database.
        """
        if not self.grant_agency.id or force:
            new_ga_id = self.grant_agency.write(conn=conn, engine=engine, debug=debug)
            self.grant_agency.id = new_ga_id

        g_cols = {'id':self.id, 'ext_grant_id':self.ext_grant_id, 'grant_agency_id':self.grant_agency.id}
        new_g_id = insert_into_table('grant', g_cols, conn=conn, engine=engine, debug=debug)
        self.id = new_g_id
        return self.id

    def delete(self, conn: Optional[Connection] = None, engine: Optional[Engine] = None, debug: bool = False) -> int:
        """Delete Grant from database along with associated links to resources.

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
            raise ValueError("Grant object must have an ID to delete.")

        delete_from_table('resource_grant', {'grant_id':self.id}, conn=conn, engine=engine, debug=debug)
        del_result = delete_from_table('grant', {'id':self.id}, conn=conn, engine=engine, debug=debug)
        return del_result

    def fetch_by_ext_id(ext_id: str, conn: Optional[Connection] = None, engine: Optional[Engine] = None, debug: bool = False) -> Grant:
        """Fetch Grant from database by external ID.

        Args:
            ext_id (str): External ID of the Grant to fetch.
            conn (Optional[Connection], optional): SQLAlchemy Connection object.
            engine (Optional[Engine], optional): SQLAlchemy Engine object.
            debug (bool, optional): If `True`, print debug information.

        Returns:
            The Grant object.
        """
        return fetch_grant({'ext_grant_id': ext_id}, conn=conn, engine=engine, debug=debug)

    def fetch_by_grant_agency_id(grant_agency_id: int, conn: Optional[Connection] = None, engine: Optional[Engine] = None, debug: bool = False) -> list:
        """Fetch Grant from database by GrantAgency ID.

        Args:
            grant_agency_id (int): GrantAgency ID of the Grant to fetch.
            conn (Optional[Connection], optional): SQLAlchemy Connection object.
            engine (Optional[Engine], optional): SQLAlchemy Engine object.
            debug (bool, optional): If `True`, print debug information.

        Returns:
            The list of fetched Grant objects.
        """
        return fetch_grant({'grant_agency_id': grant_agency_id}, conn=conn, engine=engine, debug=debug)

@dataclass
class GrantAgency:
    """
    id: Database ID for GrantAgency
    name: Name of grant agency
    parent_agency: Parent agency (to show hierarchy of agencies)
    representative_agency: Representative agency (to show grouping of agencies)
    """
    id:int
    name:str
    country:str
    parent_agency: 'GrantAgency'
    representative_agency: 'GrantAgency'


    def __init__(self, ga):
        self.id = ga.get('id')
        self.name = ga.get('name')
        self.country = ga.get('country')

        if ga.get('parent_agency') or ga.get('parent_agency_id'):
            self.parent_agency = ga.get('parent_agency') or GrantAgency({'id':ga.get('parent_agency_id')})
        else:
            self.parent_agency = None

        if ga.get('representative_agency') or ga.get('representative_agency_id'):
            self.representative_agency = ga.get('representative_agency') or GrantAgency({'id':ga.get('representative_agency_id')})
        else:
            self.representative_agency = None

    def __str__(self):
        grant_agency_str = f"GrantAgency(id={self.id}, name={self.name}, country={self.country}, "
        grant_agency_str += f"parent_agency_id={self.parent_agency.id if self.parent_agency else ''}"
        grant_agency_str += f"representative_agency_id={self.representative_agency.id if self.representative_agency else ''})"
        return grant_agency_str

    def write(self, conn: Optional[Connection] = None, engine: Optional[Engine] = None, debug: bool = False) -> int:
        """Write GrantAgency to database.

        Args:
            conn (Optional[Connection], optional): SQLAlchemy Connection object.
            engine (Optional[Engine], optional): SQLAlchemy Engine object.
            debug (bool, optional): If `True`, print debug information.

        Returns:
            The ID of the GrantAgency written to the database.
        """
        writable = {'id':self.id, 'name':self.name, 'country':self.country}
        writable['parent_agency_id'] = self.parent_agency.id if self.parent_agency else None
        writable['representative_agency_id'] = self.representative_agency.id if self.representative_agency else None

        new_ga_id = insert_into_table('grant_agency', writable, conn=conn, engine=engine, debug=debug, filter_long_data=True)
        self.id = new_ga_id
        return self.id

    def delete(self, conn: Optional[Connection] = None, engine: Optional[Engine] = None, debug: bool = False) -> int:
        """Delete GrantAgency from database.

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
            raise ValueError("GrantAgency object must have an ID to delete.")

        del_result = delete_from_table('grant_agency', {'id':self.id}, conn=conn, engine=engine, debug=debug)
        return del_result

    def fetch_by_name(name: str, conn: Optional[Connection] = None, engine: Optional[Engine] = None, debug: bool = False) -> GrantAgency:
        """Fetch GrantAgency from database by name.

        Args:
            name (str): Name of the GrantAgency to fetch.
            conn (Optional[Connection], optional): SQLAlchemy Connection object.
            engine (Optional[Engine], optional): SQLAlchemy Engine object.
            debug (bool, optional): If `True`, print debug information.

        Returns:
            The list of GrantAgency objects if more than one is found, else a single GrantAgency object, else `None`.
        """
        return fetch_grant_agency({'name': name}, conn=conn, engine=engine, debug=debug)
