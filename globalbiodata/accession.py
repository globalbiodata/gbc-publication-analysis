from __future__ import annotations

from dataclasses import dataclass
from .resource import Resource
from .version import Version
from .publication import Publication

from .utils import extract_fields_by_type
from .utils_db import insert_into_table, delete_from_table
from .utils_fetch import fetch_accession

from typing import Optional
from sqlalchemy.engine import Connection, Engine

@dataclass
class Accession:
    """ Class representing an Accession - denoting a data citation of a Resource in a Publication, with associated metadata.

    Attributes:
        accession (str): Accession ID
        resource (Resource): Resource object
        publications (list[Publication]): Publication object(s)
        version (Version): Version object
        url (str): URL string
        additional_metadata (str): Additional version metadata in JSON format
    """
    accession:str
    resource:Resource
    publications:list[Publication]
    version:Version
    url:str
    additional_metadata:str

    def __init__(self, a):
        self.accession = a.get('accession')
        self.resource = a.get('resource') or Resource(extract_fields_by_type(a, 'resource'))
        self.publications = a.get('publications') or [Publication(extract_fields_by_type(a, 'publication'))]
        self.version = a.get('version') or Version(extract_fields_by_type(a, 'version'))
        self.url = a.get('url')
        self.additional_metadata = a.get('additional_metadata')

    def __str__(self):
        accession_str = ', '.join([
            f"accession={self.accession}", f"resource={self.resource.__str__()}",
            f"version={self.version.__str__()}", f"additional_metadata={self.additional_metadata}",
            f"publications=[{', '.join([p.__str__() for p in self.publications])}]" if self.publications else "publications=[]",
            f"url={self.url}", f"additional_metadata={self.additional_metadata}"
        ])
        return f"Accession({accession_str})"

    def write(self, conn: Optional[Connection] = None, engine: Optional[Engine] = None, debug: bool = False, force: bool = False) -> int:
        """Write Accession to database along with associated Resource, Version, and Publication data.

        Args:
            conn (Optional[Connection], optional): SQLAlchemy Connection object.
            engine (Optional[Engine], optional): SQLAlchemy Engine object.
            debug (bool, optional): If `True`, print debug information.
            force (bool, optional): If `True`, force writing of associated objects even if they have IDs.

        Returns:
            The ID of the Accession written to the database.
        """
        if not self.resource.id or force:
            resource_id = self.resource.write(conn=conn, engine=engine, debug=debug)
            self.resource.id = resource_id

        if not self.version.id or force:
            version_id = self.version.write(conn=conn, engine=engine, debug=debug)
            self.version.id = version_id

        accession_cols = {
            'accession':self.accession, 'resource_id':self.resource.id, 'version_id':self.version.id,
            'url':self.url, 'additional_metadata':self.additional_metadata
        }
        insert_into_table('accession', accession_cols, conn=conn, engine=engine, debug=debug)


        if self.publications:
            for p in self.publications:
                if not p.id or force:
                    new_pub_id = p.write(conn=conn, engine=engine, debug=debug)
                    p.id = new_pub_id

                # create links between resource and publication tables
                insert_into_table('accession_publication', {'accession':self.accession, 'publication_id':p.id}, conn=conn, engine=engine, debug=debug)

    def delete(self, conn: Optional[Connection] = None, engine: Optional[Engine] = None, debug: bool = False) -> int:
        """Delete Accession from database along with associated links to publications.

        Args:
            conn (Optional[Connection], optional): SQLAlchemy Connection object.
            engine (Optional[Engine], optional): SQLAlchemy Engine object.
            debug (bool, optional): If `True`, print debug information.

        Returns:
            Number of rows deleted.
        """
        if conn is None:
            conn = engine.connect()

        if not self.accession:
            raise ValueError("Accession object must have an accession field to perform delete.")

        delete_from_table('accession_publication', {'accession':self.accession}, conn=conn, engine=engine, debug=debug)
        delete_from_table('accession', {'accession':self.accession}, conn=conn, engine=engine, debug=debug)

    def fetch_by_accession(accession: str, conn: Optional[Connection] = None, engine: Optional[Engine] = None, debug: bool = False) -> Accession:
        """Fetch Accession from database by accession ID.

        Args:
            accession (str): Accession ID of the Accession to fetch.
            conn (Optional[Connection], optional): SQLAlchemy Connection object.
            engine (Optional[Engine], optional): SQLAlchemy Engine object.
            debug (bool, optional): If `True`, print debug information.

        Returns:
            The Accession object.
        """
        return fetch_accession({'accession': accession}, conn=conn, engine=engine, debug=debug)

    def fetch_by_resource(resource_id: int, conn: Optional[Connection] = None, engine: Optional[Engine] = None, debug: bool = False) -> list:
        """Fetch Accession from database by Resource ID.

        Args:
            resource_id (int): Resource ID of the Accession to fetch.
            conn (Optional[Connection], optional): SQLAlchemy Connection object.
            engine (Optional[Engine], optional): SQLAlchemy Engine object.
            debug (bool, optional): If `True`, print debug information.

        Returns:
            The list of Accession objects.
        """
        return fetch_accession({'resource_id': resource_id}, conn=conn, engine=engine, debug=debug)

    def fetch_by_publication(self, publication_id: int, conn: Optional[Connection] = None, engine: Optional[Engine] = None, debug: bool = False) -> list:
        """Fetch Accession from database by Publication ID.

        Args:
            publication_id (int): Publication ID of the Accession to fetch.
            conn (Optional[Connection], optional): SQLAlchemy Connection object.
            engine (Optional[Engine], optional): SQLAlchemy Engine object.
            debug (bool, optional): If `True`, print debug information.

        Returns:
            The list of Accession objects.
        """
        return fetch_accession({'publication_id': publication_id}, conn=conn, engine=engine, debug=debug)