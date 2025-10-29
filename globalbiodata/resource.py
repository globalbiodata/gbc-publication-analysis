from __future__ import annotations

from dataclasses import dataclass
from .url import URL
from .version import Version
from .publication import Publication
from .grant import Grant

from .utils_db import insert_into_table, delete_from_table
from .utils_fetch import fetch_resource, fetch_accession, fetch_resource_mention

from typing import Optional, TYPE_CHECKING
import sqlalchemy as db

if TYPE_CHECKING: # only import on type checking to avoid circular imports
    from sqlalchemy.engine import Connection, Engine

    from .accession import Accession
    from .resource_mention import ResourceMention

@dataclass
class Resource:
    """Class representing a Biodata Resource.

    Attributes:
        id (int): Database ID for the resource.
        short_name (str): Short name of the resource.
        common_name (str): Common name of the resource.
        full_name (str): Full name of the resource.
        url (URL): URL object.
        version (Version): Version object.
        prediction_metadata (str): Additional prediction metadata (JSON).
        publications (list[Publication]): Associated publications.
        grants (list[Grant]): Associated grants.
        is_gcbr (bool): Core Biodata Resource status.
        is_latest (bool): Whether this is the most current version.
    """
    id: int
    short_name: str
    common_name: str
    full_name: str
    url: URL
    version: Version
    prediction_metadata: str
    publications: list[Publication]
    grants: list[Grant]
    is_gcbr: bool
    is_latest: bool
    __conn__: Connection
    __engine__: Engine

    def __init__(self, r):
        self.id = r.get('id')
        self.short_name = r.get('short_name')
        self.common_name = r.get('common_name')
        self.full_name = r.get('full_name')

        r2 = {k:r[k] for k in r.keys() if k!='id'} # copy input and remove id to avoid propagating it to other objects
        self.url = URL(r2) if type(r.get('url')) is str else r.get('url')
        self.version = r.get('version') or Version(r2)
        self.prediction_metadata = r.get('resource_prediction_metadata') or r.get('prediction_metadata')
        self.is_gcbr = r.get('is_gcbr')
        self.is_latest = r.get('is_latest')

        self.__conn__ = r.get('__conn__')
        self.__engine__ = r.get('__engine__')

        if r.get('grants'):
            self.grants = [Grant(r2)] if type(r.get('grants')[0]) is str else r.get('grants')
        elif r.get('ext_grant_ids') and r.get('grant_agencies'):
            self.grants = [Grant({'ext_grant_id':g, 'grant_agency':ga}) for g, ga in zip([x.strip() for x in r.get('ext_grant_ids').split(',')], [x.strip() for x in r.get('grant_agencies').split(',')])]
        else:
            self.grants = []

        if not r.get('publications') and r.get('title') and r.get('pubmed_id') and r.get('authors'):
            self.publications = [Publication(r2)]
        else:
            self.publications = r.get('publications')

    def __str__(self):
        resource_str = ', '.join([
            f"id={self.id}", f"short_name={self.short_name}", f"common_name={self.common_name}", f"full_name={self.full_name}",
            f"is_gcbr={self.is_gcbr}", f"is_latest={self.is_latest}", f"url={self.url.__str__()}",
            f"version={self.version.__str__()}", f"prediction_metadata={self.prediction_metadata}",
            f"publications=[{', '.join([p.__str__() for p in self.publications])}]" if self.publications else "publications=[]",
            f"grants=[{', '.join(g.__str__() for g in self.grants)}]" if self.grants else "grants=[]"
        ])
        return f"Resource({resource_str})"

    def write(self, conn: Optional[Connection] = None, engine: Optional[Engine] = None, debug: bool = False, force: bool = False) -> int:
        """Write Resource to database along with associated URL, Version, Publication, and Grant data.

        Args:
            conn (Optional[Connection], optional): SQLAlchemy Connection object.
            engine (Optional[Engine], optional): SQLAlchemy Engine object.
            debug (bool, optional): If `True`, print debug information.
            force (bool, optional): If `True`, force writing of associated objects even if they have IDs.

        Returns:
            The ID of the resource written to the database.
        """
        conn = conn or self.__conn__
        engine = engine or self.__engine__

        if not self.url.id or force:
            url_id = self.url.write(conn=conn, engine=engine, debug=debug)
            self.url.id = url_id

        if not self.version.id or force:
            version_id = self.version.write(conn=conn, engine=engine, debug=debug)
            self.version.id = version_id

        # set is_latest to 0 for other versions of this resource
        if self.is_latest:
            lconn = engine.connect()
            lconn.execute(db.text(f"UPDATE resource SET is_latest = 0 WHERE short_name = '{self.short_name}'"))
            lconn.commit()

        resource_cols = {
            'id':self.id, 'short_name':self.short_name, 'common_name':self.common_name, 'full_name':self.full_name,
            'url_id':self.url.id, 'version_id':self.version.id, 'prediction_metadata':self.prediction_metadata,
            'is_gcbr':self.is_gcbr, 'is_latest':self.is_latest
        }
        new_resource_id = insert_into_table('resource', resource_cols, conn=conn, engine=engine, debug=debug)
        self.id = new_resource_id

        if self.publications:
            # delete_from_table('resource_publication', {'resource_id':new_resource_id}, conn=conn, engine=engine, debug=debug) # delete existing links
            for p in self.publications:
                if not p.id or force:
                    new_pub_id = p.write(conn=conn, engine=engine, debug=debug)
                    p.id = new_pub_id
                # create links between resource and publication tables
                insert_into_table('resource_publication', {'resource_id':new_resource_id, 'publication_id':p.id}, conn=conn, engine=engine, debug=debug)

        if self.grants:
            # delete_from_table('resource_grant', {'resource_id':new_resource_id}, conn=conn, engine=engine, debug=debug) # delete existing links
            for g in self.grants:
                if not g.id or force:
                    new_grant_id = g.write(conn=conn, engine=engine, debug=debug)
                    g.id = new_grant_id
                # create links between resource and grant tables
                insert_into_table('resource_grant', {'resource_id':new_resource_id, 'grant_id':g.id}, conn=conn, engine=engine, debug=debug)

        return self.id

    def delete(self, conn: Optional[Connection] = None, engine: Optional[Engine] = None, debug: bool = False) -> int:
        """Delete Resource from database along with associated links to publications and grants.

        Args:
            conn (Optional[Connection], optional): SQLAlchemy Connection object.
            engine (Optional[Engine], optional): SQLAlchemy Engine object.
            debug (bool, optional): If `True`, print debug information.

        Returns:
            Number of rows deleted.
        """

        conn = conn or self.__conn__
        engine = engine or self.__engine__

        if conn is None:
            conn = engine.connect()

        if not self.id:
            raise ValueError("Resource object must have an ID to delete.")

        delete_from_table('resource_publication', {'resource_id':self.id}, conn=conn, engine=engine, debug=debug)
        delete_from_table('resource_grant', {'resource_id':self.id}, conn=conn, engine=engine, debug=debug)
        self.url.delete(conn=conn, engine=engine, debug=debug)
        r_result = delete_from_table('resource', {'id':self.id}, conn=conn, engine=engine, debug=debug)

        return r_result

    def fetch_by_id(resource_id: int, expanded: bool = True, conn: Optional[Connection] = None, engine: Optional[Engine] = None, debug: bool = False) -> Resource:
        """Fetch Resource from database by ID.

        Args:
            resource_id (int): ID of the Resource to fetch.
            conn (Optional[Connection], optional): SQLAlchemy Connection object.
            engine (Optional[Engine], optional): SQLAlchemy Engine object.
            debug (bool, optional): If `True`, print debug information.

        Returns:
            The fetched Resource object.
        """
        return fetch_resource({'id': resource_id}, expanded=expanded, conn=conn, engine=engine, debug=debug)

    def fetch_by_name(name: str, expanded: bool = True, conn: Optional[Connection] = None, engine: Optional[Engine] = None, debug: bool = False) -> Optional[list]:
        """Fetch Resource from database by name. This will search short_name, common_name, and full_name fields.

        Args:
            name (str): Name of the Resource to fetch.
            conn (Optional[Connection], optional): SQLAlchemy Connection object.
            engine (Optional[Engine], optional): SQLAlchemy Engine object.
            debug (bool, optional): If `True`, print debug information.

        Returns:
            The list of fetched Resource objects.
        """
        sn_results = fetch_resource({'short_name': name, 'is_latest': 1}, expanded=expanded, conn=conn, engine=engine, debug=debug)
        cn_results = fetch_resource({'common_name': name, 'is_latest': 1}, expanded=expanded, conn=conn, engine=engine, debug=debug)
        fn_results = fetch_resource({'full_name': name, 'is_latest': 1}, expanded=expanded, conn=conn, engine=engine, debug=debug)

        sn_results = sn_results if type(sn_results) is list else [sn_results] if sn_results else []
        cn_results = cn_results if type(cn_results) is list else [cn_results] if cn_results else []
        fn_results = fn_results if type(fn_results) is list else [fn_results] if fn_results else []

        combined_results = {r.id: r for r in (sn_results + cn_results + fn_results)}
        if len(combined_results) == 1:
            return list(combined_results.values())[0]
        elif len(combined_results) > 1:
            return list(combined_results.values())
        else:
            return None

    def is_online(self) -> bool:
        """Return boolean describing whether resource URL is online.

        Returns:
            `True` if resource URL is online, `False` otherwise.
        """
        return self.url.is_online()

    def accessions(self) -> list[Accession]:
        """Return list of Accession objects associated with this Resource.

        Returns:
            List of Accession objects.
        """
        return fetch_accession({'resource_id': self.id}, conn=self.__conn__, engine=self.__engine__, debug=False)

    def mentions(self) -> list[ResourceMention]:
        """Return list of ResourceMention objects that mention this Resource.

        Returns:
            List of ResourceMention objects.
        """
        return fetch_resource_mention({'resource_id': self.id}, conn=self.__conn__, engine=self.__engine__, debug=False)

    def referenced_by(self) -> list[Publication]:
        """Return list of Publication objects that reference this Resource either via an Accession or a ResourceMention.

        Returns:
            List of Publication objects.
        """
        refs = {}
        for acc in self.accessions():
            for pub in acc.publications:
                refs[pub.id] = pub

        for mention in self.mentions():
            refs[mention.publication.id] = mention.publication
        return list(refs.values())
