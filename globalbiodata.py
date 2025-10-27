from __future__ import annotations

import sys
import re
from datetime import datetime

import sqlalchemy as db
from sqlalchemy.dialects.mysql import insert # for on_duplicate_key_update

from typing import Optional
from sqlalchemy.engine import Connection, Engine

import time
import random
from sqlalchemy.exc import OperationalError

import locationtagger
import googlemaps

# ---------------------------------------------------------------------------- #
# Classes for Global Biodata Core Resource data                                #
# ---------------------------------------------------------------------------- #

def extract_fields_by_type(data: dict, type_prefix: str) -> dict:
    """Extract fields from a dictionary that start with a given prefix.

    Args:
        data (dict): Input dictionary.
        type_prefix (str): Prefix to filter keys.

    Returns:
        dict: Dictionary with extracted fields.
    """
    extracted = {}
    for k, v in data.items():
        if k.startswith(f"{type_prefix}_"):
            extracted[re.sub(f"^{type_prefix}_", "", k)] = v
    return extracted

class URL:
    """
    URL and associated information

    `id`: Database ID for URL
    `url`: URL
    `url_country`: Country of URL
    `url_coordinates`: Coordinates of URL
    `status`: ConnectionStatus object(s)
    `wayback_url`: URL for Wayback Machine
    """
    id:int
    url:str
    url_country:str
    url_coordinates:str
    url_status:str
    status:list
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
            conn (Optional[Connection], optional): SQLAlchemy Connection object. Defaults to None.
            engine (Optional[Engine], optional): SQLAlchemy Engine object. Defaults to None.
            debug (bool, optional): If True, print debug information. Defaults to False.

        Returns:
            int: The ID of the URL written to the database.
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
            conn (Optional[Connection], optional): SQLAlchemy Connection object. Defaults to None.
            engine (Optional[Engine], optional): SQLAlchemy Engine object. Defaults to None.
            debug (bool, optional): If True, print debug information. Defaults to False.

        Returns:
            int: The number of rows deleted.
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
            conn (Optional[Connection], optional): SQLAlchemy Connection object. Defaults to None.
            engine (Optional[Engine], optional): SQLAlchemy Engine object. Defaults to None.
            debug (bool, optional): If True, print debug information. Defaults to False.

        Returns:
            URL: The fetched URL object.
        """
        return fetch_url({'id': url_id}, conn=conn, engine=engine, debug=debug)

    def fetch_by_url(url: str, conn: Optional[Connection] = None, engine: Optional[Engine] = None, debug: bool = False) -> Optional[URL]:
        """Fetch URL from database by URL string.

        Args:
            url (str): URL string to fetch.
            conn (Optional[Connection], optional): SQLAlchemy Connection object. Defaults to None.
            engine (Optional[Engine], optional): SQLAlchemy Engine object. Defaults to None.
            debug (bool, optional): If True, print debug information. Defaults to False.

        Returns:
            URL: The fetched URL object if one result, else list of URLs, else None.
        """
        return fetch_url({'url': url}, conn=conn, engine=engine, debug=debug)


    def latest_connection_status(self) -> ConnectionStatus:
        """Get the latest ConnectionStatus object for this URL.

        Returns:
            ConnectionStatus: The latest ConnectionStatus object or None if not found.
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
            bool: True if URL is online, False otherwise.
        """
        latest_status = self.latest_connection_status()
        return True if (latest_status and latest_status.is_online) else False

class Version:
    """
    Version information

    `id` : Database ID for Version
    `name` : Name of version/pipeline/type
    `date` : Date of run
    `user` : User who ran pipeline
    `additional_metadata` : Additional data in JSON format
    """
    id:int
    name:str
    date:str
    user:str
    additional_metadata:dict

    def __init__(self, p):
        self.id = p.get('id')
        self.name = p.get('version_name') or p.get('name')
        self.date = p.get('version_date') or p.get('date')
        self.user = p.get('version_user') or p.get('user')
        self.additional_metadata = p.get('additional_version_metadata') or p.get('additional_metadata')

    def __str__(self):
        version_str = f"Version(id={self.id}, name={self.name}, date={self.date}, user={self.user}, additional_metadata={self.additional_metadata})"
        return version_str

    def write(self, conn: Optional[Connection] = None, engine: Optional[Engine] = None, debug: bool = False) -> int:
        """Write Version to database.

        Args:
            conn (Optional[Connection], optional): SQLAlchemy Connection object. Defaults to None.
            engine (Optional[Engine], optional): SQLAlchemy Engine object. Defaults to None.
            debug (bool, optional): If True, print debug information. Defaults to False.

        Returns:
            int: The ID of the Version written to the database.
        """
        new_version_id = insert_into_table('version', self.__dict__, conn=conn, engine=engine, debug=debug)
        self.id = new_version_id
        return self.id

    def delete(self, conn: Optional[Connection] = None, engine: Optional[Engine] = None, debug: bool = False) -> int:
        """Delete Version from database.

        Args:
            conn (Optional[Connection], optional): SQLAlchemy Connection object. Defaults to None.
            engine (Optional[Engine], optional): SQLAlchemy Engine object. Defaults to None.
            debug (bool, optional): If True, print debug information. Defaults to False.

        Returns:
            int: Number of rows deleted.
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
            conn (Optional[Connection], optional): SQLAlchemy Connection object. Defaults to None.
            engine (Optional[Engine], optional): SQLAlchemy Engine object. Defaults to None.
            debug (bool, optional): If True, print debug information. Defaults to False.

        Returns:
            Version: The fetched Version object.
        """
        return fetch_version({'id': version_id}, conn=conn, engine=engine, debug=debug)

class Resource:
    """
    Biodata Resource description

    `id`: Database ID for resource
    `short_name`: Short name of resource
    `common_name`: Common name of resource
    `full_name`: Full name of resource
    `url`: URL object
    `version`: Version object
    `prediction_metadata`: Additional prediction metadata in JSON format
    `publications`: Publication object(s)
    `grants`: Grant object(s)
    `is_gcbr`: boolean value describing Core Biodata Resource status
    `is_latest`: boolean value describing whether this is the most current version of the resource
    """
    id:int
    short_name:str
    common_name:str
    full_name:str
    url:URL
    version:Version
    prediction_metadata:str
    publications:list
    grants:list
    is_gcbr:bool
    is_latest:bool
    __conn__:object
    __engine__:object

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
            conn (Optional[Connection], optional): SQLAlchemy Connection object. Defaults to None.
            engine (Optional[Engine], optional): SQLAlchemy Engine object. Defaults to None.
            debug (bool, optional): If True, print debug information. Defaults to False.
            force (bool, optional): If True, force writing of associated objects even if they have IDs. Defaults to False.

        Returns:
            int: The ID of the resource written to the database.
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
            conn (Optional[Connection], optional): SQLAlchemy Connection object. Defaults to None.
            engine (Optional[Engine], optional): SQLAlchemy Engine object. Defaults to None.
            debug (bool, optional): If True, print debug information. Defaults to False.

        Returns:
            int: Number of rows deleted.
        """

        conn = conn or self.__conn__
        engine = engine or self.__engine__

        if conn is None:
            conn = engine.connect()

        if not self.id:
            raise ValueError("Resource object must have an ID to delete.")

        rp_del = delete_from_table('resource_publication', {'resource_id':self.id}, conn=conn, engine=engine, debug=debug)
        rg_del = delete_from_table('resource_grant', {'resource_id':self.id}, conn=conn, engine=engine, debug=debug)
        u_del = self.url.delete(conn=conn, engine=engine, debug=debug)
        r_result = delete_from_table('resource', {'id':self.id}, conn=conn, engine=engine, debug=debug)

        return r_result

    def fetch_by_id(resource_id: int, expanded: bool = False, conn: Optional[Connection] = None, engine: Optional[Engine] = None, debug: bool = False) -> Resource:
        """Fetch Resource from database by ID.

        Args:
            resource_id (int): ID of the Resource to fetch.
            conn (Optional[Connection], optional): SQLAlchemy Connection object. Defaults to None.
            engine (Optional[Engine], optional): SQLAlchemy Engine object. Defaults to None.
            debug (bool, optional): If True, print debug information. Defaults to False.

        Returns:
            Resource: The fetched Resource object.
        """
        return fetch_resource({'id': resource_id}, expanded=expanded, conn=conn, engine=engine, debug=debug)

    def fetch_by_name(name: str, expanded: bool = False, conn: Optional[Connection] = None, engine: Optional[Engine] = None, debug: bool = False) -> Optional[list]:
        """Fetch Resource from database by name. This will search short_name, common_name, and full_name fields.

        Args:
            name (str): Name of the Resource to fetch.
            conn (Optional[Connection], optional): SQLAlchemy Connection object. Defaults to None.
            engine (Optional[Engine], optional): SQLAlchemy Engine object. Defaults to None.
            debug (bool, optional): If True, print debug information. Defaults to False.

        Returns:
            list: The list of fetched Resource objects.
        """
        sn_results = fetch_resource({'short_name': name}, expanded=expanded, conn=conn, engine=engine, debug=debug)
        cn_results = fetch_resource({'common_name': name}, expanded=expanded, conn=conn, engine=engine, debug=debug)
        fn_results = fetch_resource({'full_name': name}, expanded=expanded, conn=conn, engine=engine, debug=debug)

        sn_results = sn_results if type(sn_results) is list else [sn_results] if sn_results else []
        cn_results = cn_results if type(cn_results) is list else [cn_results] if cn_results else []
        fn_results = fn_results if type(fn_results) is list else [fn_results] if fn_results else []

        return sn_results + cn_results + fn_results

    def is_online(self) -> bool:
        """Return boolean describing whether resource URL is online.

        Returns:
            bool: True if resource URL is online, False otherwise.
        """
        return self.url.is_online()

    def accessions(self) -> list[Accession]:
        """Return list of Accession objects associated with this Resource.

        Returns:
            list[Accession]: List of Accession objects.
        """
        return fetch_accession({'resource_id': self.id}, conn=self.__conn__, engine=self.__engine__, debug=False)

    def mentions(self) -> list[ResourceMention]:
        """Return list of ResourceMention objects that mention this Resource.

        Returns:
            list[ResourceMention]: List of ResourceMention objects.
        """
        return fetch_resource_mention({'resource_id': self.id}, conn=self.__conn__, engine=self.__engine__, debug=False)

    def referenced_by(self) -> list[Publication]:
        """Return list of Publication objects that reference this Resource either via an Accession or a ResourceMention.

        Returns:
            list[Publication]: List of Publication objects.
        """
        refs = {}
        for acc in self.accessions():
            for pub in acc.publications:
                refs[pub.id] = pub

        for mention in self.mentions():
            refs[mention.publication.id] = mention.publication
        return list(refs.values())


class ConnectionStatus:
    """

    `url_id`: Database ID for URL
    `status`: Code returned from connection
    `date`: Date of connection
    `is_online`: boolean describing whether return code indicates resource is online
    `is_latest`: boolean value describing whether this is the most recent connection attempt
    """
    url_id:int
    status:str
    date:str
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
            conn (Optional[Connection], optional): SQLAlchemy Connection object. Defaults to None.
            engine (Optional[Engine], optional): SQLAlchemy Engine object. Defaults to None.
            debug (bool, optional): If True, print debug information. Defaults to False.

        Returns:
            int: The ID of the ConnectionStatus written to the database.
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
            conn (Optional[Connection], optional): SQLAlchemy Connection object. Defaults to None.
            engine (Optional[Engine], optional): SQLAlchemy Engine object. Defaults to None.
            debug (bool, optional): If True, print debug information. Defaults to False.

        Returns:
            int: Number of rows deleted.
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
            conn (Optional[Connection], optional): SQLAlchemy Connection object. Defaults to None.
            engine (Optional[Engine], optional): SQLAlchemy Engine object. Defaults to None.
            debug (bool, optional): If True, print debug information. Defaults to False.

        Returns:
            list: The list of fetched ConnectionStatus objects.
        """
        return fetch_connection_status({'url_id': url_id}, conn=conn, engine=engine, debug=debug)

class Publication:
    """

    `id`: Database ID for Publication
    `title`: Title of publication
    `pubmed_id`: PubMed ID
    `pmc_id`: PubMed Central ID
    `publication_date`: Date of publication
    `authors`: Authors of publication
    `affiliation`: Affiliation of authors
    `affiliation_countries`: Countries of affiliation
    `citation_count`: Number of citations
    `keywords`: Keywords/Mesh terms
    """
    id:int
    title:str
    pubmed_id:int
    pmc_id:str
    publication_date:str
    authors:str
    affiliation:str
    affiliation_countries:str
    grants:list
    citation_count:int
    keywords:str
    __conn__:object
    __engine__:object

    def __init__(self, p):
        self.id = p.get('id')
        self.title = p.get('publication_title') or p.get('title')
        self.pubmed_id = None if p.get('pubmed_id', '') == '' else p.get('pubmed_id')
        self.pmc_id = None if p.get('pmc_id', '') == '' else p.get('pmc_id')
        self.publication_date = p.get('publication_date')
        self.authors = p.get('authors') if type(p.get('authors')) is not list else '; '.join(p.get('authors'))
        self.affiliation = p.get('affiliation') if type(p.get('affiliation')) is not list else '; '.join(p.get('affiliation'))
        self.affiliation_countries = p.get('affiliation_countries') if type(p.get('affiliation_countries')) is not list else '; '.join(p.get('affiliation_countries'))
        self.citation_count = p.get('citation_count')
        self.keywords = p.get('keywords') if type(p.get('keywords')) is not list else '; '.join(p.get('keywords'))

        self.__conn__ = p.get('__conn__')
        self.__engine__ = p.get('__engine__')

        if p.get('grants'):
            self.grants = [Grant(p)] if type(p.get('grants')[0]) is str else p.get('grants')
        elif p.get('ext_grant_ids') and p.get('grant_agencies'):
            self.grants = [Grant({'ext_grant_id':g, 'grant_agency':ga}) for g, ga in zip([e.strip() for e in p.get('ext_grant_ids').split(',')], [e.strip() for e in p.get('grant_agencies').split(',')])]
        else:
            self.grants = None

    def __str__(self):
        pub_str = ', '.join([
            f"id={self.id}", f"title={self.title}", f"pubmed_id={self.pubmed_id}", f"pmc_id={self.pmc_id}",
            f"publication_date={self.publication_date}", f"authors={self.authors}", f"affiliation={self.affiliation}",
            f"affiliation_countries={self.affiliation_countries}",f"citation_count={self.citation_count}",
            f"keywords={self.keywords}", f"grants=[{', '.join(g.__str__() for g in self.grants)}]",
            f"grants=[{', '.join(g.__str__() for g in self.grants)}]" if self.grants else "grants=[]"
        ])
        return f"Publication({pub_str})"

    def write(self, conn: Optional[Connection] = None, engine: Optional[Engine] = None, debug: bool = False, force: bool = False) -> int:
        """Write Publication to database along with associated Grant data.

        Args:
            conn (Optional[Connection], optional): SQLAlchemy Connection object. Defaults to None.
            engine (Optional[Engine], optional): SQLAlchemy Engine object. Defaults to None.
            debug (bool, optional): If True, print debug information. Defaults to False.
            force (bool, optional): If True, force writing of associated objects even if they have IDs. Defaults to False.

        Returns:
            int: The ID of the Publication written to the database.
        """
        conn = conn or self.__conn__
        engine = engine or self.__engine__

        self_dict = self.__dict__.copy()
        pub_grants = self_dict.pop('grants')

        if not self.title or not self.authors or self.citation_count is None:
            raise ValueError("Publication must have a title, authors, and citation count to write to the database.")

        new_pub_id = insert_into_table('publication', self_dict, conn=conn, engine=engine, debug=debug, filter_long_data=True)
        self.id = new_pub_id

        if pub_grants:
            # delete_from_table('publication_grant', {'publication_id':new_pub_id}, conn=conn, engine=engine, debug=debug) # delete existing links
            for g in pub_grants:
                if not g.id or force:
                    new_grant_id = g.write(conn=conn, engine=engine, debug=debug)
                    g.id = new_grant_id

                # create links between publication and grant tables
                insert_into_table('publication_grant', {'publication_id':new_pub_id, 'grant_id':g.id}, conn=conn, engine=engine, debug=debug)

        return self.id

    def delete(self, conn: Optional[Connection] = None, engine: Optional[Engine] = None, debug: bool = False) -> int:
        """Delete Publication from database along with associated links to grants.

        Args:
            conn (Optional[Connection], optional): SQLAlchemy Connection object. Defaults to None.
            engine (Optional[Engine], optional): SQLAlchemy Engine object. Defaults to None.
            debug (bool, optional): If True, print debug information. Defaults to False.

        Returns:
            int: Number of rows deleted.
        """
        conn = conn or self.__conn__
        engine = engine or self.__engine__

        if conn is None:
            conn = engine.connect()

        if not self.id:
            raise ValueError("Publication object must have an ID to delete.")

        del_result = delete_from_table('publication', {'id':self.id}, conn=conn, engine=engine, debug=debug)
        return del_result

    def fetch_by_id(id: int, expanded: bool = False, conn: Optional[Connection] = None, engine: Optional[Engine] = None, debug: bool = False) -> Publication:
        """Fetch Publication from database by ID.

        Args:
            id (int): ID of the Publication to fetch.
            conn (Optional[Connection], optional): SQLAlchemy Connection object. Defaults to None.
            engine (Optional[Engine], optional): SQLAlchemy Engine object. Defaults to None.
            debug (bool, optional): If True, print debug information. Defaults to False.

        Returns:
            list: The Publication object.
        """
        return fetch_publication({'id': id}, expanded=expanded, conn=conn, engine=engine, debug=debug)

    def fetch_by_pubmed_id(pubmed_id: int, expanded: bool = False, conn: Optional[Connection] = None, engine: Optional[Engine] = None, debug: bool = False) -> Publication:
        """Fetch Publication from database by PubMed ID.

        Args:
            pubmed_id (int): PubMed ID of the Publication to fetch.
            conn (Optional[Connection], optional): SQLAlchemy Connection object. Defaults to None.
            engine (Optional[Engine], optional): SQLAlchemy Engine object. Defaults to None.
            debug (bool, optional): If True, print debug information. Defaults to False.

        Returns:
            Publication: The Publication object.
        """
        return fetch_publication({'pubmed_id': pubmed_id}, expanded=expanded, conn=conn, engine=engine, debug=debug)

    def fetch_by_pmc_id(pmc_id: str, expanded: bool = False, conn: Optional[Connection] = None, engine: Optional[Engine] = None, debug: bool = False) -> Publication:
        """Fetch Publication from database by PubMed Central ID.

        Args:
            pmc_id (str): PubMed Central ID of the Publication to fetch.
            conn (Optional[Connection], optional): SQLAlchemy Connection object. Defaults to None.
            engine (Optional[Engine], optional): SQLAlchemy Engine object. Defaults to None.
            debug (bool, optional): If True, print debug information. Defaults to False.

        Returns:
            Publication: The Publication object.
        """
        return fetch_publication({'pmc_id': pmc_id}, expanded=expanded, conn=conn, engine=engine, debug=debug)

    def accessions(self) -> list[Accession]:
        """Return list of Accession objects associated with this Publication.

        Returns:
            list[Accession]: List of Accession objects.
        """
        return fetch_accession({'publication_id': self.id}, conn=self.__conn__, engine=self.__engine__, debug=False)

    def mentions(self) -> list[ResourceMention]:
        """Return list of ResourceMention objects that mention this Publication.

        Returns:
            list[ResourceMention]: List of ResourceMention objects.
        """
        return fetch_resource_mention({'publication_id': self.id}, conn=self.__conn__, engine=self.__engine__, debug=False)

    def references_resources(self) -> list[Resource]:
        """Return list of Resource objects that are referenced by this Publication either via an Accession or a ResourceMention.

        Returns:
            list[Resource]: List of Resource objects.
        """
        resources = {}
        accessions = self.accessions()
        for acc in accessions:
            resources[acc.resource.id] = acc.resource

        mentions = self.mentions()
        for mention in mentions:
            resources[mention.resource.id] = mention.resource

        # print(f"[SET] resources: {', '.join([str(r.id) for r in resources.values()])}")
        # print(f"[LIST] resources: {','.join([str(r.id) for r in list(resources.values())])}")
        return list(resources.values())

class Grant:
    """
    `id`: Database ID for Grant
    `ext_grant_id`: External grant ID
    `grant_agencies`: GrantAgency object(s)
    """
    id:int
    ext_grant_id:str
    grant_agency:str

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
            conn (Optional[Connection], optional): SQLAlchemy Connection object. Defaults to None.
            engine (Optional[Engine], optional): SQLAlchemy Engine object. Defaults to None.
            debug (bool, optional): If True, print debug information. Defaults to False.
            force (bool, optional): If True, force writing of associated objects even if they have IDs. Defaults to False.

        Returns:
            int: The ID of the Grant written to the database.
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
            conn (Optional[Connection], optional): SQLAlchemy Connection object. Defaults to None.
            engine (Optional[Engine], optional): SQLAlchemy Engine object. Defaults to None.
            debug (bool, optional): If True, print debug information. Defaults to False.

        Returns:
            int: Number of rows deleted.
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
            conn (Optional[Connection], optional): SQLAlchemy Connection object. Defaults to None.
            engine (Optional[Engine], optional): SQLAlchemy Engine object. Defaults to None.
            debug (bool, optional): If True, print debug information. Defaults to False.

        Returns:
            Grant: The Grant object.
        """
        return fetch_grant({'ext_grant_id': ext_id}, conn=conn, engine=engine, debug=debug)

    def fetch_by_grant_agency_id(grant_agency_id: int, conn: Optional[Connection] = None, engine: Optional[Engine] = None, debug: bool = False) -> list:
        """Fetch Grant from database by GrantAgency ID.

        Args:
            grant_agency_id (int): GrantAgency ID of the Grant to fetch.
            conn (Optional[Connection], optional): SQLAlchemy Connection object. Defaults to None.
            engine (Optional[Engine], optional): SQLAlchemy Engine object. Defaults to None.
            debug (bool, optional): If True, print debug information. Defaults to False.

        Returns:
            list: The list of fetched Grant objects.
        """
        return fetch_grant({'grant_agency_id': grant_agency_id}, conn=conn, engine=engine, debug=debug)

class GrantAgency:
    """
    `id`: Database ID for GrantAgency
    `name`: Name of grant agency
    `parent_agency`: Parent agency (to show hierarchy of agencies)
    `representative_agency`: Representative agency (to show grouping of agencies)
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
            conn (Optional[Connection], optional): SQLAlchemy Connection object. Defaults to None.
            engine (Optional[Engine], optional): SQLAlchemy Engine object. Defaults to None.
            debug (bool, optional): If True, print debug information. Defaults to False.

        Returns:
            int: The ID of the GrantAgency written to the database.
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
            conn (Optional[Connection], optional): SQLAlchemy Connection object. Defaults to None.
            engine (Optional[Engine], optional): SQLAlchemy Engine object. Defaults to None.
            debug (bool, optional): If True, print debug information. Defaults to False.

        Returns:
            int: Number of rows deleted.
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
            conn (Optional[Connection], optional): SQLAlchemy Connection object. Defaults to None.
            engine (Optional[Engine], optional): SQLAlchemy Engine object. Defaults to None.
            debug (bool, optional): If True, print debug information. Defaults to False.

        Returns:
            GrantAgency: The list of GrantAgency objects if more than one is found, else a single GrantAgency object, else None.
        """
        return fetch_grant_agency({'name': name}, conn=conn, engine=engine, debug=debug)

class Accession:
    """
    `accession`: Accession ID
    `resource`: Resource object
    `publications`: Publication object(s)
    `version`: Version object
    `url`: URL string
    `additional_metadata`: Additional version metadata in JSON format
    """
    accession:str
    resource:Resource
    publications:list
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
            conn (Optional[Connection], optional): SQLAlchemy Connection object. Defaults to None.
            engine (Optional[Engine], optional): SQLAlchemy Engine object. Defaults to None.
            debug (bool, optional): If True, print debug information. Defaults to False.
            force (bool, optional): If True, force writing of associated objects even if they have IDs. Defaults to False.

        Returns:
            int: The ID of the Accession written to the database.
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
            conn (Optional[Connection], optional): SQLAlchemy Connection object. Defaults to None.
            engine (Optional[Engine], optional): SQLAlchemy Engine object. Defaults to None.
            debug (bool, optional): If True, print debug information. Defaults to False.

        Returns:
            int: Number of rows deleted.
        """
        if conn is None:
            conn = engine.connect()

        if not self.accession:
            raise ValueError("Accession object must have an accession field to perform delete.")

        ap_del = delete_from_table('accession_publication', {'accession':self.accession}, conn=conn, engine=engine, debug=debug)
        ac_del = delete_from_table('accession', {'accession':self.accession}, conn=conn, engine=engine, debug=debug)

    def fetch_by_accession(accession: str, conn: Optional[Connection] = None, engine: Optional[Engine] = None, debug: bool = False) -> Accession:
        """Fetch Accession from database by accession ID.

        Args:
            accession (str): Accession ID of the Accession to fetch.
            conn (Optional[Connection], optional): SQLAlchemy Connection object. Defaults to None.
            engine (Optional[Engine], optional): SQLAlchemy Engine object. Defaults to None.
            debug (bool, optional): If True, print debug information. Defaults to False.

        Returns:
            Accession: The Accession object.
        """
        return fetch_accession({'accession': accession}, conn=conn, engine=engine, debug=debug)

    def fetch_by_resource(resource_id: int, conn: Optional[Connection] = None, engine: Optional[Engine] = None, debug: bool = False) -> list:
        """Fetch Accession from database by Resource ID.

        Args:
            resource_id (int): Resource ID of the Accession to fetch.
            conn (Optional[Connection], optional): SQLAlchemy Connection object. Defaults to None.
            engine (Optional[Engine], optional): SQLAlchemy Engine object. Defaults to None.
            debug (bool, optional): If True, print debug information. Defaults to False.

        Returns:
            list: The list of Accession objects.
        """
        return fetch_accession({'resource_id': resource_id}, conn=conn, engine=engine, debug=debug)

    def fetch_by_publication(self, publication_id: int, conn: Optional[Connection] = None, engine: Optional[Engine] = None, debug: bool = False) -> list:
        """Fetch Accession from database by Publication ID.

        Args:
            publication_id (int): Publication ID of the Accession to fetch.
            conn (Optional[Connection], optional): SQLAlchemy Connection object. Defaults to None.
            engine (Optional[Engine], optional): SQLAlchemy Engine object. Defaults to None.
            debug (bool, optional): If True, print debug information. Defaults to False.

        Returns:
            list: The list of Accession objects.
        """
        return fetch_accession({'publication_id': publication_id}, conn=conn, engine=engine, debug=debug)

class ResourceMention:
    """
    Link between a publication and a resource, with match info.

    `publication`: Publication object
    `resource`: Resource object
    `version`: Version object
    `matched_alias`: alias string that matched
    `match_count`: number of matches
    `mean_confidence`: mean confidence score
    """
    publication: Publication
    resource: Resource
    version: Version
    matched_aliases: list
    match_count: int
    mean_confidence: float

    def __init__(self, m):
        self.publication = m.get('publication') or Publication(extract_fields_by_type(m, 'publication'))
        self.resource = m.get('resource') or Resource(extract_fields_by_type(m, 'resource'))
        self.version = m.get('version') or Version(extract_fields_by_type(m, 'version'))
        self.matched_aliases = m.get('matched_aliases', []) if type(m.get('matched_aliases')) is list else [m.get('matched_aliases')]
        self.match_count = m.get('match_count', 0)
        self.mean_confidence = m.get('mean_confidence', 0.0)

    def __str__(self):
        return (
            f"ResourceMention(pub={self.publication.__str__()}, res={self.resource.__str__()}, "
            f"ver={self.version.__str__()}, alias='{self.matched_alias}', "
            f"count={self.match_count}, mean_conf={self.mean_confidence})"
        )

    def write(self, conn: Optional[Connection] = None, engine: Optional[Engine] = None, debug: bool = False, force: bool = False) -> int:
        """Write ResourceMention to database along with associated Publication, Resource, and Version data.

        Args:
            conn (Optional[Connection], optional): SQLAlchemy Connection object. Defaults to None.
            engine (Optional[Engine], optional): SQLAlchemy Engine object. Defaults to None.
            debug (bool, optional): If True, print debug information. Defaults to False.
            force (bool, optional): If True, force writing of associated objects even if they have IDs. Defaults to False.

        Returns:
            int: The ID of the ResourceMention written to the database.
        """
        if not self.publication.id or force:
            pub_id = self.publication.write(conn=conn, engine=engine, debug=debug)
            self.publication.id = pub_id

        if not self.resource.id or force:
            res_id = self.resource.write(conn=conn, engine=engine, debug=debug)
            self.resource.id = res_id

        if not self.version.id or force:
            ver_id = self.version.write(conn=conn, engine=engine, debug=debug)
            self.version.id = ver_id

        for matched_alias in self.matched_aliases:
            mention_cols = {
                'publication_id': self.publication.id,
                'resource_id': self.resource.id,
                'version_id': self.version.id,
                'matched_alias': matched_alias['matched_alias'],
                'match_count': matched_alias['match_count'],
                'mean_confidence': matched_alias['mean_confidence'],
            }
            insert_into_table('resource_mention', mention_cols, conn=conn, engine=engine, debug=debug)

    def delete(self, conn=None, engine=None, debug=False):
        if conn is None:
            conn = engine.connect()
        if not self.publication_id or not self.resource_id:
            raise ValueError("ResourceMention requires publication_id and resource_id to delete.")

        for matched_alias in self.matched_aliases:
            del_result = delete_from_table(
                'resource_mention',
                {'publication_id': self.publication.id, 'resource_id': self.resource.id, 'matched_alias': matched_alias['matched_alias']},
                conn=conn,
                engine=engine,
            debug=debug
        )
        return del_result

    def fetch_by_publication_id(publication_id: int, conn: Optional[Connection] = None, engine: Optional[Engine] = None, debug: bool = False) -> list:
        """Fetch ResourceMention from database by Publication ID.

        Args:
            publication_id (int): Publication ID of the ResourceMention to fetch.
            conn (Optional[Connection], optional): SQLAlchemy Connection object. Defaults to None.
            engine (Optional[Engine], optional): SQLAlchemy Engine object. Defaults to None.
            debug (bool, optional): If True, print debug information. Defaults to False.

        Returns:
            list: The list of ResourceMention objects.
        """
        return fetch_resource_mention({'publication_id': publication_id}, conn=conn, engine=engine, debug=debug)

    def fetch_by_resource_id(resource_id: int, conn: Optional[Connection] = None, engine: Optional[Engine] = None, debug: bool = False) -> list:
        """Fetch ResourceMention from database by Resource ID.

        Args:
            resource_id (int): Resource ID of the ResourceMention to fetch.
            conn (Optional[Connection], optional): SQLAlchemy Connection object. Defaults to None.
            engine (Optional[Engine], optional): SQLAlchemy Engine object. Defaults to None.
            debug (bool, optional): If True, print debug information. Defaults to False.

        Returns:
            list: The list of ResourceMention objects.
        """
        return fetch_resource_mention({'resource_id': resource_id}, conn=conn, engine=engine, debug=debug)

# ---------------------------------------------------------------------------- #
# Database helper methods                                                      #
# ---------------------------------------------------------------------------- #

# manually defined primary and unique keys for tables
table_keys = {
    'resource': {'pk': ['id'], 'uk': ['short_name', 'url_id', 'version_id']},
    'url': {'pk': ['id'], 'uk': ['url']},
    'connection_status': {'pk': ['url_id', 'date'], 'uk': []},
    'version': {'pk': ['id'], 'uk': ['name', 'date']},
    'publication': {'pk': ['id'], 'uk': ['pubmed_id', 'pmc_id']},
    'grant': {'pk': ['id'], 'uk': ['ext_grant_id', 'grant_agency_id']},
    'grant_agency': {'pk': ['id'], 'uk': ['name_hash']},
    'resource_publication': {'pk': ['resource_id', 'publication_id'], 'uk': []},
    'resource_grant': {'pk': ['resource_id', 'grant_id'], 'uk': []},
    'publication_grant': {'pk': ['publication_id', 'grant_id'], 'uk': []},
    'accession': {'pk': ['accession'], 'uk': []},
    'accession_publication': {'pk': ['accession', 'publication_id'], 'uk': []},
    'resource_mention': {'pk': ['publication_id', 'resource_id', 'matched_alias'], 'uk': []},
}

def get_primary_keys(table, conn):
    cached_pks = table_keys.get(table.name, {}).get('pk', None)
    if cached_pks is not None:
        return cached_pks
    return db.inspect(conn).get_pk_constraint(table.name)['constrained_columns']

def get_unique_keys(table, conn):
    cached_uks = table_keys.get(table.name, {}).get('uk', None)
    if cached_uks is not None:
        return cached_uks
    insp_uks = db.inspect(conn).get_unique_constraints(table.name)
    try:
        return insp_uks[0]["column_names"]
    except IndexError:
        return []

def get_all_keys(table, conn):
    return get_primary_keys(table, conn) + get_unique_keys(table, conn)

def fetch_id_from_unique_keys(table, data, conn, debug=False):
    # detect unique keys for table and use to query id
    uniq_col_names = get_unique_keys(table, conn)
    if not uniq_col_names:
        return 0

    wheres = [table.columns.get(ucn) == data[ucn] for ucn in uniq_col_names]
    if debug:
        print(f"--> finding ids with cols: {', '.join(uniq_col_names)}")

    select = db.select(table.c.id).where(db.and_(*wheres))
    result = conn.execute(select).fetchone()
    if result is None:
        raise ValueError(f"Entity not found in table {table.name} with unique keys: ", {k:v for k, v in data.items() if k in uniq_col_names})
    return result[0]

def remove_key_fields(table, conn, data): # also remove empty values
    key_names = get_all_keys(table, conn)
    return {k:v for k, v in data.items() if (k not in key_names and v is not None)}

def stringify_data(data):
    for k, v in data.items():
        if type(v) is list:
            data[k] = '; '.join(v)
    return data

def _get_db_name(engine):
    db_name = engine.url.database
    if db_name is None:
        # grab from the raw connection
        conn = engine.connect()
        raw_conn = conn.connection.connection
        db_name = raw_conn.db.decode()

    return db_name

_max_lens = {}
def _get_max_len(col_name, table_name, conn=None, engine=None):
    """Return CHARACTER_MAXIMUM_LENGTH for a column using an existing connection if provided.
    Falls back to engine.connect() only when necessary.
    """
    global _max_lens
    col_key = f"{table_name}.{col_name}"
    if col_key not in _max_lens:
        close_after = False
        if conn is None:
            if engine is None:
                raise ValueError("_get_max_len requires either an engine or an open connection")
            conn = engine.connect()
            close_after = True

        # Determine current database name (schema) in a driver-agnostic way
        try:
            db_name = conn.execute(db.text("SELECT DATABASE()")).scalar()
        except Exception:
            # Fallback: try the engine helper if available
            if engine is not None:
                db_name = _get_db_name(engine)
            else:
                raise

        max_len_sql = db.text(
            """
            SELECT CHARACTER_MAXIMUM_LENGTH
            FROM INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_SCHEMA = :db AND TABLE_NAME = :table AND COLUMN_NAME = :col
            """
        )
        rows = conn.execute(max_len_sql, {"db": db_name, "table": table_name, "col": col_name}).fetchall()
        for result in rows:
            _max_lens[col_key] = result[0]

        if close_after:
            conn.close()

    return _max_lens.get(col_key, None)

def insert_into_table(
    table_name: str,
    data: dict,
    conn: Optional[Connection] = None,
    engine: Optional[Engine] = None,
    debug: bool = False,
    filter_long_data: bool = False,
    retry_on_deadlock: bool = True,
    max_retries: int = 5,
    base_delay: float = 0.2,
) -> int:
    """Insert-or-update a row and return its id.

    Args:
        table_name (str): Name of the table to insert into.
        data (dict): Dictionary of column names and values to insert.
        conn (Optional[Connection], optional): SQLAlchemy Connection object. Defaults to None.
        engine (Optional[Engine], optional): SQLAlchemy Engine object. Defaults to None.
        debug (bool, optional): If True, print debug information. Defaults to False.
        filter_long_data (bool, optional): If True, move overlength strings to long_text table. Defaults to False.
        retry_on_deadlock (bool, optional): If True, retry on deadlock errors. Defaults to True.
        max_retries (int, optional): Maximum number of retries on deadlock. Defaults to 5.
        base_delay (float, optional): Base delay in seconds for exponential backoff. Defaults to 0.2.

    Returns:
        int: The ID/PK of the inserted or updated row.
    """
    metadata_obj = db.MetaData()

    conn_created = False
    if conn is None:
        if engine is None:
            raise ValueError("insert_into_table requires either an engine or an open connection")
        conn = engine.connect()
        conn_created = True

    # Reflect the table using the ENGINE (separate pooled connection) to avoid
    # starting an implicit transaction on our working connection.
    table = db.Table(table_name, metadata_obj, autoload_with=conn)
    pk_cols = get_primary_keys(table, conn)
    data = stringify_data(data)

    if debug:
        print(f"\n--> Inserting into table: {table_name}")
        print(data)
        print(f"Columns: {', '.join(table.columns.keys())}")
        print(f"Primary keys: {', '.join(pk_cols)}")

    # Optionally move overlength strings to long_text and replace with reference token
    if filter_long_data:
        for k, v in list(data.items()):
            this_max_len = _get_max_len(k, table_name, conn=conn, engine=engine)
            if v and this_max_len and isinstance(v, str) and len(v) > this_max_len:
                longtext_id = insert_into_table('long_text', {'text': v}, conn=conn, engine=engine, debug=debug)
                data[k] = f"long_text({longtext_id})"

    # We "own" the transaction if we opened this connection in this function.
    # Reflection above may have triggered an implicit txn elsewhere; don't let that
    # disable retries. We manage our own explicit txn when we created the conn.
    own_txn = conn_created
    # print(f"conn_created={conn_created}, own_txn={own_txn}, conn.in_transaction()={conn.in_transaction()}")

    # Retry loop only if we control our own transaction
    attempts = 0
    while True:
        attempts += 1
        # If we own the connection, start an explicit transaction only if one isn't already active
        implicit_txn = conn.in_transaction()
        trans = conn.begin() if (own_txn and not implicit_txn) else None
        try:
            insert_stmt = insert(table).values(data)
            data_no_pks = remove_key_fields(table, conn, data)

            if data_no_pks:  # typical path: upsert (ON DUPLICATE KEY UPDATE)
                if pk_cols and len(pk_cols) == 1:
                    # single primary key: use LAST_INSERT_ID() hack to get the id of existing row if no insert occurred
                    pk_name = pk_cols[0]
                    data_no_pks[pk_name] = db.func.last_insert_id(table.c[pk_name])

                do_update_stmt = insert_stmt.on_duplicate_key_update(**data_no_pks)
                if debug:
                    print(f"Updating {table_name} with data: {data_no_pks}")
                result = conn.execute(do_update_stmt)
            else:  # tables that are pure key rows
                result = conn.execute(insert_stmt.prefix_with('IGNORE'))

            if trans is not None:
                trans.commit()
            elif own_txn and implicit_txn and conn.in_transaction():
                # We own the connection and were already in an implicit txn (likely started by prior metadata queries)
                conn.commit()

            inserted_pk = result.inserted_primary_key[0] if result.inserted_primary_key else None
            affected_rows = result.rowcount

            # Determine the resulting id to return
            if (not inserted_pk) or (inserted_pk and affected_rows == 0):
                # entity existed and was not updated
                existing_id = conn.execute(db.text("SELECT LAST_INSERT_ID()")).scalar() if len(pk_cols) == 1 else fetch_id_from_unique_keys(table, data, conn, debug=debug)
                if debug:
                    print(f"Entity already exists. Fetched id: {existing_id}")
                this_id = existing_id
            elif (inserted_pk and affected_rows > 1):
                # entity existed and was updated (MySQL reports >1 affected rows on upsert-update)
                this_id = inserted_pk
                if debug:
                    print(f"Entity already exists. Updated id: {this_id}")
            else:
                this_id = inserted_pk
                if debug:
                    print(f"New entity added. Inserted id: {this_id}")

            # Success; break retry loop
            break

        except OperationalError as e:
            # MySQL deadlock / lock wait timeout codes
            code = None
            try:
                code = e.orig.args[0]
            except Exception:
                pass

            if trans is not None:
                try:
                    trans.rollback()
                except Exception:
                    pass
            elif own_txn and conn.in_transaction():
                try:
                    conn.rollback()
                except Exception:
                    pass

            # If we don't own the transaction or retries disabled, re-raise immediately
            if not own_txn or not retry_on_deadlock or code not in (1213, 1205):
                raise

            # Backoff and retry (we own the txn)
            if attempts > max_retries:
                raise
            # Exponential backoff with jitter
            delay = (base_delay * (2 ** (attempts - 1))) * (1 + 0.25 * random.random())
            if debug:
                sys.stderr.write(f"[retry] {table_name}: OperationalError {code}; attempt {attempts}/{max_retries}; sleeping {delay:.2f}s\n")
            time.sleep(delay)
            continue

        except Exception as e:
            if trans is not None:
                try:
                    trans.rollback()
                except Exception:
                    pass
            elif own_txn and conn.in_transaction():
                try:
                    conn.rollback()
                except Exception:
                    pass
            sys.stderr.write(f"Transaction rolled back due to: {e}\n")
            raise
        finally:
            if conn_created and own_txn and not conn.closed:
                # keep connection open for caller if they provided it; otherwise close at end
                pass

    if conn_created:
        conn.close()

    return this_id

def delete_from_table(
    table_name: str,
    data: dict,
    conn: Optional[Connection] = None,
    engine: Optional[Engine] = None,
    debug: bool = False
) -> int:
    """Delete rows from a table matching the provided data.

    Args:
        table_name (str): Name of the table to delete from.
        data (dict): Dictionary of column names and values to match for deletion.
        conn (Optional[Connection], optional): SQLAlchemy Connection object. Defaults to None.
        engine (Optional[Engine], optional): SQLAlchemy Engine object. Defaults to None.
        debug (bool, optional): If True, print debug information. Defaults to False.

    Returns:
        int: Number of rows deleted.
    """
    metadata_obj = db.MetaData()

    conn_created = False
    if conn is None:
        if engine is None:
            raise ValueError("select_from_table requires either an engine or an open connection")
        conn = engine.connect()
        conn_created = True

    # Reflect the table using the active connection
    table = db.Table(table_name, metadata_obj, autoload_with=conn)
    data = stringify_data(data)

    if debug:
        print(f"\n--> Deleting from table: {table_name} WHERE:")
        print(' AND '.join([f"{k} == {data[k]}" for k in data.keys()]))

    trans = conn.begin() if not conn.in_transaction() else None  # Begin a transaction if we're not already in one
    try:
        wheres = [table.columns.get(c) == data[c] for c in data.keys()]
        del_result = conn.execute(db.delete(table).where(db.and_(*wheres)))
        if debug:
            print(f"Deleted {del_result.rowcount} rows.")
        trans.commit()  # Commit the transaction
    except Exception as e:
        if trans:
            trans.rollback()  # Rollback the transaction if an error occurs
            sys.stderr.write(f"Transaction rolled back due to: {e}\n")
        raise
    finally:
        if conn_created:
            conn.close()  # Close the connection

    return del_result.rowcount

def select_from_table(
    table_name: str,
    data: dict = {},
    join_table: str = None,
    order_by: list = None,
    conn: Optional[Connection] = None,
    engine: Optional[Engine] = None,
    debug: bool = False
) -> list:
    """Select rows from a table matching the provided data.

    Args:
        table_name (str): Name of the table to select from.
        data (dict, optional): Dictionary of column names and values to match for selection. Defaults to {}.
        join_table (str, optional): Name of a table to join with. Defaults to None.
        order_by (list, optional): Column name(s) to order the results by. Defaults to None.
        conn (Optional[Connection], optional): SQLAlchemy Connection object. Defaults to None.
        engine (Optional[Engine], optional): SQLAlchemy Engine object. Defaults to None.
        debug (bool, optional): If True, print debug information. Defaults to False.

    Returns:
        list: List of dictionaries representing the selected rows.
    """
    metadata_obj = db.MetaData()

    # Ensure we have a live connection before reflecting
    conn_created = False
    if conn is None:
        if engine is None:
            raise ValueError("select_from_table requires either an engine or an open connection")
        conn = engine.connect()
        conn_created = True

    # Reflect using the active connection (works in SA 1.4/2.0)
    table = db.Table(table_name, metadata_obj, autoload_with=conn)
    if join_table:
        join_tbl = db.Table(join_table, metadata_obj, autoload_with=conn)
        table = table.join(join_tbl)
        # print("JOINED TABLE COLUMNS:", table.columns.keys())

    if debug:
        print(f"\n--> Selecting from table: {table_name} WHERE:")
        print('AND '.join([f"{k} == '{data[k]}'" for k in data.keys()]))

    wheres = [
        table.columns.get(c).in_(data[c]) if isinstance(data[c], list)
        else table.columns.get(c) == data[c]
        for c in data
    ]

    # construct select statement with correct options
    stmt = db.select(table)
    if wheres:
        stmt = stmt.where(db.and_(*wheres))
    if order_by:
        if isinstance(order_by, list):
            order_cols = [table.columns.get(col) for col in order_by if table.columns.get(col) is not None]
            if order_cols:
                stmt = stmt.order_by(*order_cols)
        else:
            order_col = table.columns.get(order_by)
            if order_col is not None:
                stmt = stmt.order_by(order_col)
    result = conn.execute(stmt).fetchall()

    # convert result to list of dicts
    d_result = [dict(zip(table.columns.keys(), list(r))) for r in result]

    if conn_created:
        conn.close()
    return d_result

# ----------------------------------------------------------------------- #
# Fetcher methods for Global Biodata Resource data                        #
# ----------------------------------------------------------------------- #

def fetch_resource(query: dict, order_by: str = 'id', expanded: bool = True, conn: Optional[Connection] = None, engine: Optional[Engine] = None, debug: bool = False) -> Optional[Resource]:
    """Fetch Resource(s) from the database matching the provided query.

    Args:
        query (dict): Dictionary of column names and values to match for selection.
        order_by (str, optional): Column name(s) to order the results by. Defaults to 'id'.
        expanded (bool, optional): If True, fetch associated publications and grants. Defaults to True.
        conn (Optional[Connection], optional): SQLAlchemy Connection object. Defaults to None.
        engine (Optional[Engine], optional): SQLAlchemy Engine object. Defaults to None.
        debug (bool, optional): If True, print debug information. Defaults to False.

    Returns:
        Optional[Resource]: single Resource object (where single result if found), or list of Resource objects if found, else None.
    """
    resource_raw = select_from_table('resource', query, order_by=order_by, conn=conn, engine=engine, debug=debug)
    if len(resource_raw) == 0:
        return None

    resources = []
    for r in resource_raw:
        r['url'] = fetch_url({'id':r['url_id']}, conn=conn, engine=engine, debug=debug)
        r['version'] = fetch_version({'id':r['version_id']}, conn=conn, engine=engine, debug=debug)

        if expanded:
            pub_ids = select_from_table('resource_publication', {'resource_id':r['id']}, conn=conn, engine=engine, debug=debug)
            r['publications'] = fetch_publication({'id':[p['publication_id'] for p in pub_ids]}, conn=conn, engine=engine, debug=debug)
            r['publications'] = [r['publications']] if type(r['publications']) is not list else r['publications']

            grant_ids = select_from_table('resource_grant', {'resource_id':r['id']}, conn=conn, engine=engine, debug=debug)
            r['grants'] = fetch_grant({'id':[g['grant_id'] for g in grant_ids]}, conn=conn, engine=engine, debug=debug)
            r['grants'] = [r['grants']] if (r['grants'] is not None and type(r['grants']) is not list) else r['grants']

        r['__conn__'] = conn
        r['__engine__'] = engine

        resources.append(Resource(r))

    return resources if len(resources) > 1 else resources[0]

def fetch_all_resources(order_by: str = 'id', expanded: bool = True, conn: Optional[Connection] = None, engine: Optional[Engine] = None, debug: bool = False) -> list:
    """Fetch all Resources from the database.

    Args:
        order_by (str, optional): Column name(s) to order the results by. Defaults to 'id'.
        expanded (bool, optional): If True, fetch associated publications and grants. Defaults to True.
        conn (Optional[Connection], optional): SQLAlchemy Connection object. Defaults to None.
        engine (Optional[Engine], optional): SQLAlchemy Engine object. Defaults to None.
        debug (bool, optional): If True, print debug information. Defaults to False.

    Returns:
        list: List of Resource objects.
    """
    return fetch_resource({}, order_by=order_by, expanded=expanded, conn=conn, engine=engine, debug=debug)

def fetch_all_online_resources(order_by: str = 'id', expanded: bool = True, conn: Optional[Connection] = None, engine: Optional[Engine] = None, debug: bool = False) -> list:
    """Fetch all Resources from the database where online status is true.

    Args:
        order_by (str, optional): Column name(s) to order the results by. Defaults to 'id'.
        expanded (bool, optional): If True, fetch associated publications and grants. Defaults to True.
        conn (Optional[Connection], optional): SQLAlchemy Connection object. Defaults to None.
        engine (Optional[Engine], optional): SQLAlchemy Engine object. Defaults to None.
        debug (bool, optional): If True, print debug information. Defaults to False.

    Returns:
        list: List of online Resource objects.
    """
    full_list = fetch_all_resources(order_by=order_by, expanded=expanded, conn=conn, engine=engine, debug=debug)
    return [r for r in full_list if r.is_online()]

def fetch_url(query: dict, order_by: str = 'id', expanded: bool = True, conn: Optional[Connection] = None, engine: Optional[Engine] = None, debug: bool = False) -> Optional[URL]:
    """Fetch URL(s) from the database matching the provided query.

    Args:
        query (dict): Dictionary of column names and values to match for selection.
        order_by (str, optional): Column name(s) to order the results by. Defaults to 'id'.
        expanded (bool, optional): If True, fetch associated connection status. Defaults to True.
        conn (Optional[Connection], optional): SQLAlchemy Connection object. Defaults to None.
        engine (Optional[Engine], optional): SQLAlchemy Engine object. Defaults to None.
        debug (bool, optional): If True, print debug information. Defaults to False.

    Returns:
        Optional[URL]: single URL object (where single result if found), or list of URL objects if found, else None.
    """
    url_raw = select_from_table('url', query, order_by=order_by, conn=conn, engine=engine, debug=debug)
    if len(url_raw) == 0:
        return None

    urls = []
    for u in url_raw:
        if expanded:
            u['status'] = fetch_connection_status({'url_id':u['id']}, order_by=['is_latest', 'date'], conn=conn, engine=engine, debug=debug)
            u['status'] = [u['status']] if (u['status'] is not None and type(u['status']) is not list) else u['status']
            u['status'] = u['status'][::-1] # reverse order to have latest first

        urls.append(URL(u))

    return urls if len(urls) > 1 else urls[0]

def fetch_all_urls(order_by: str = 'id', expanded: bool = True, conn: Optional[Connection] = None, engine: Optional[Engine] = None, debug: bool = False) -> list:
    """Fetch all URLs from the database.

    Args:
        order_by (str, optional): Column name(s) to order the results by. Defaults to 'id'.
        expanded (bool, optional): If True, fetch associated connection status. Defaults to True.
        conn (Optional[Connection], optional): SQLAlchemy Connection object. Defaults to None.
        engine (Optional[Engine], optional): SQLAlchemy Engine object. Defaults to None.
        debug (bool, optional): If True, print debug information. Defaults to False.

    Returns:
        list: List of URL objects.
    """
    return fetch_url({}, order_by=order_by, expanded=expanded, conn=conn, engine=engine, debug=debug)

def fetch_connection_status(query: dict, order_by: str = 'url_id', conn: Optional[Connection] = None, engine: Optional[Engine] = None, debug: bool = False) -> Optional[ConnectionStatus]:
    """Fetch ConnectionStatus(es) from the database matching the provided query.

    Args:
        query (dict): Dictionary of column names and values to match for selection.
        order_by (str, optional): Column name(s) to order the results by. Defaults to 'url_id'.
        conn (Optional[Connection], optional): SQLAlchemy Connection object. Defaults to None.
        engine (Optional[Engine], optional): SQLAlchemy Engine object. Defaults to None.
        debug (bool, optional): If True, print debug information. Defaults to False.

    Returns:
        Optional[ConnectionStatus]: single ConnectionStatus object (where single result if found), or list of ConnectionStatus objects if found, else None.
    """
    status_raw = select_from_table('connection_status', query, order_by=order_by, conn=conn, engine=engine, debug=debug)
    if len(status_raw) == 0:
        return None

    conn_stats = [ConnectionStatus(cs) for cs in status_raw]

    return conn_stats if len(conn_stats) > 1 else conn_stats[0]

def fetch_all_connection_statuses(order_by: str = 'url_id', conn: Optional[Connection] = None, engine: Optional[Engine] = None, debug: bool = False) -> list:
    """Fetch all ConnectionStatuses from the database.

    Args:
        order_by (str, optional): Column name(s) to order the results by. Defaults to 'url_id'.
        conn (Optional[Connection], optional): SQLAlchemy Connection object. Defaults to None.
        engine (Optional[Engine], optional): SQLAlchemy Engine object. Defaults to None.
        debug (bool, optional): If True, print debug information. Defaults to False.

    Returns:
        list: List of ConnectionStatus objects.
    """
    return fetch_connection_status({}, order_by=order_by, conn=conn, engine=engine, debug=debug)

def fetch_version(query: dict, order_by: str = 'id', conn: Optional[Connection] = None, engine: Optional[Engine] = None, debug: bool = False) -> Optional[Version]:
    """Fetch Version(s) from the database matching the provided query.

    Args:
        query (dict): Dictionary of column names and values to match for selection.
        order_by (str, optional): Column name(s) to order the results by. Defaults to 'id'.
        conn (Optional[Connection], optional): SQLAlchemy Connection object. Defaults to None.
        engine (Optional[Engine], optional): SQLAlchemy Engine object. Defaults to None.
        debug (bool, optional): If True, print debug information. Defaults to False.

    Returns:
        Optional[Version]: single Version object (where single result if found), or list of Version objects if found, else None.
    """
    version_raw = select_from_table('version', query, order_by=order_by, conn=conn, engine=engine, debug=debug)
    if len(version_raw) == 0:
        return None

    versions = [Version(p) for p in version_raw]

    return versions if len(versions) > 1 else versions[0]

def fetch_all_versions(order_by: str = 'id', conn: Optional[Connection] = None, engine: Optional[Engine] = None, debug: bool = False) -> list:
    """Fetch all Versions from the database.

    Args:
        order_by (str, optional): Column name(s) to order the results by. Defaults to 'id'.
        conn (Optional[Connection], optional): SQLAlchemy Connection object. Defaults to None.
        engine (Optional[Engine], optional): SQLAlchemy Engine object. Defaults to None.
        debug (bool, optional): If True, print debug information. Defaults to False.

    Returns:
        list: List of Version objects.
    """
    return fetch_version({}, order_by=order_by, conn=conn, engine=engine, debug=debug)

def fetch_publication(query: dict, order_by: str = 'id', expanded: bool = True, conn: Optional[Connection] = None, engine: Optional[Engine] = None, debug: bool = False) -> Optional[Publication]:
    """Fetch Publication(s) from the database matching the provided query.

    Args:
        query (dict): Dictionary of column names and values to match for selection.
        order_by (str, optional): Column name(s) to order the results by. Defaults to 'id'.
        expanded (bool, optional): If True, fetch associated grants. Defaults to True.
        conn (Optional[Connection], optional): SQLAlchemy Connection object. Defaults to None.
        engine (Optional[Engine], optional): SQLAlchemy Engine object. Defaults to None.
        debug (bool, optional): If True, print debug information. Defaults to False.

    Returns:
        Optional[Publication]: single Publication object (where single result if found), or list of Publication objects if found, else None.
    """
    publication_raw = select_from_table('publication', query, order_by=order_by, conn=conn, engine=engine, debug=debug)
    if len(publication_raw) == 0:
        return None

    publications = []
    for p in publication_raw:
        if expanded:
            grant_ids = select_from_table('publication_grant', {'publication_id':p['id']}, conn=conn, engine=engine, debug=debug)
            p['grants'] = fetch_grant({'id':[g['grant_id'] for g in grant_ids]}, conn=conn, engine=engine, debug=debug)
            p['grants'] = [p['grants']] if (p['grants'] is not None and type(p['grants']) is not list) else p['grants']
        else:
            p['grants'] = None

        p['__conn__'] = conn
        p['__engine__'] = engine

        publications.append(Publication(p))

    return publications if len(publications) > 1 else publications[0]

def fetch_all_publications(order_by: str = 'id', expanded: bool = True, conn: Optional[Connection] = None, engine: Optional[Engine] = None, debug: bool = False) -> list:
    """Fetch all Publications from the database.

    Args:
        order_by (str, optional): Column name(s) to order the results by. Defaults to 'id'.
        expanded (bool, optional): If True, fetch associated grants. Defaults to True.
        conn (Optional[Connection], optional): SQLAlchemy Connection object. Defaults to None.
        engine (Optional[Engine], optional): SQLAlchemy Engine object. Defaults to None.
        debug (bool, optional): If True, print debug information. Defaults to False.

    Returns:
        list: List of Publication objects.
    """
    return fetch_publication({}, order_by=order_by, expanded=expanded, conn=conn, engine=engine, debug=debug)

def fetch_grant(query: dict, order_by: str = 'id', conn: Optional[Connection] = None, engine: Optional[Engine] = None, debug: bool = False) -> Optional[Grant]:
    """Fetch Grant(s) from the database matching the provided query.

    Args:
        query (dict): Dictionary of column names and values to match for selection.
        order_by (str, optional): Column name(s) to order the results by. Defaults to 'id'.
        conn (Optional[Connection], optional): SQLAlchemy Connection object. Defaults to None.
        engine (Optional[Engine], optional): SQLAlchemy Engine object. Defaults to None.
        debug (bool, optional): If True, print debug information. Defaults to False.

    Returns:
        Optional[Grant]: single Grant object (where single result if found), or list of Grant objects if found, else None.
    """
    grant_raw = select_from_table('grant', query, order_by=order_by, conn=conn, engine=engine, debug=debug)
    if len(grant_raw) == 0:
        return None

    grants = []
    for g in grant_raw:
        g['grant_agency'] = fetch_grant_agency({'id':g['grant_agency_id']}, conn=conn, engine=engine, debug=debug)
        grants.append(Grant(g))

    return grants if len(grants) > 1 else grants[0]

def fetch_all_grants(order_by: str = 'id', conn: Optional[Connection] = None, engine: Optional[Engine] = None, debug: bool = False) -> list:
    """Fetch all Grants from the database.

    Args:
        order_by (str, optional): Column name(s) to order the results by. Defaults to 'id'.
        conn (Optional[Connection], optional): SQLAlchemy Connection object. Defaults to None.
        engine (Optional[Engine], optional): SQLAlchemy Engine object. Defaults to None.
        debug (bool, optional): If True, print debug information. Defaults to False.

    Returns:
        list: List of Grant objects.
    """
    return fetch_grant({}, order_by=order_by, conn=conn, engine=engine, debug=debug)

def fetch_grant_agency(query: dict, order_by: str = 'id', conn: Optional[Connection] = None, engine: Optional[Engine] = None, debug: bool = False) -> Optional[GrantAgency]:
    """Fetch GrantAgency(s) from the database matching the provided query.

    Args:
        query (dict): Dictionary of column names and values to match for selection.
        order_by (str, optional): Column name(s) to order the results by. Defaults to 'id'.
        conn (Optional[Connection], optional): SQLAlchemy Connection object. Defaults to None.
        engine (Optional[Engine], optional): SQLAlchemy Engine object. Defaults to None.
        debug (bool, optional): If True, print debug information. Defaults to False.

    Returns:
        Optional[GrantAgency]: single GrantAgency object (where single result if found), or list of GrantAgency objects if found, else None.
    """
    grant_agency_raw = select_from_table('grant_agency', query, order_by=order_by, conn=conn, engine=engine, debug=debug)
    if len(grant_agency_raw) == 0:
        return None

    grant_agencies = [GrantAgency(ga) for ga in grant_agency_raw]

    return grant_agencies if len(grant_agencies) > 1 else grant_agencies[0]

def fetch_all_grant_agencies(order_by: str = 'id', conn: Optional[Connection] = None, engine: Optional[Engine] = None, debug: bool = False) -> list:
    """Fetch all GrantAgencies from the database.

    Args:
        order_by (str, optional): Column name(s) to order the results by. Defaults to 'id'.
        conn (Optional[Connection], optional): SQLAlchemy Connection object. Defaults to None.
        engine (Optional[Engine], optional): SQLAlchemy Engine object. Defaults to None.
        debug (bool, optional): If True, print debug information. Defaults to False.

    Returns:
        list: List of GrantAgency objects.
    """
    return fetch_grant_agency({}, order_by=order_by, conn=conn, engine=engine, debug=debug)

def fetch_accession(query: dict, expanded: bool = False, conn: Optional[Connection] = None, engine: Optional[Engine] = None, debug: bool = False) -> Optional[list]:
    """Fetch Accession(es) from the database matching the provided query.

    Args:
        query (dict): Dictionary of column names and values to match for selection.
        expanded (bool, optional): If True, fetch associated resource, version, and publications. Defaults to False.
        conn (Optional[Connection], optional): SQLAlchemy Connection object. Defaults to None.
        engine (Optional[Engine], optional): SQLAlchemy Engine object. Defaults to None.
        debug (bool, optional): If True, print debug information. Defaults to False.

    Returns:
        Optional[Accession]: list of Accession objects if found, else None.
    """
    # join accession and accession_publication tables to get publication IDs
    order_by = ["accession_resource_id", "accession_accession"]
    formatted_query = {f"accession_publication_{k}" if k =='publication_id' else f"accession_{k}": v for k, v in query.items() if v is not None}
    accession_raw = select_from_table('accession', formatted_query, join_table='accession_publication', order_by=order_by, conn=conn, engine=engine, debug=debug)

    # format column names to remove table prefixes added by sqlalchemy join
    accession_results = []
    for a in accession_raw:
        af = {re.sub('^accession_publication_', '', k): v for k, v in a.items()}
        af = {re.sub('^accession_', '', k): v for k, v in af.items()}
        accession_results.append(af)

    if len(accession_results) == 0:
        return None

    # group by accession to combine multiple publications
    grouped_accessions = {}
    for a in accession_results:
        if a['accession'] not in grouped_accessions:
            grouped_accessions[a['accession']] = {
                'version_id': a['version_id'],
                'resource_id': a['resource_id'],
                'publications': set(),
                'url': a['url'],
                'additional_metadata': a['prediction_metadata']
            }
        grouped_accessions[a['accession']]['publications'].add(a['publication_id'])

    sorted_accessions = sorted(grouped_accessions.keys()) # sort for consistent order (important for testing)

    # build component objects
    accessions = []
    for a in sorted_accessions:
        a_obj = { 'accession': a, 'publications': [] }
        a_obj['resource'] = fetch_resource({'id':grouped_accessions[a]['resource_id']}, expanded=expanded, conn=conn, engine=engine, debug=debug)
        a_obj['version'] = fetch_version({'id':grouped_accessions[a]['version_id']}, conn=conn, engine=engine, debug=debug)
        a_obj['publications'] = fetch_publication({'id':list(grouped_accessions[a]['publications'])}, expanded=expanded, conn=conn, engine=engine, debug=debug)
        a_obj['publications'] = [a_obj['publications']] if type(a_obj['publications']) is not list else a_obj['publications']

        accessions.append(Accession(a_obj))

    return accessions

def fetch_resource_mention(query: dict, expanded: bool = True, conn: Optional[Connection] = None, engine: Optional[Engine] = None, debug: bool = False) -> Optional[list]:
    """Fetch ResourceMention(s) from the database matching the provided query.

    Args:
        query (dict): Dictionary of column names and values to match for selection.
        expanded (bool, optional): If True, fetch associated resource, version, and publication. Defaults to True.
        conn (Optional[Connection], optional): SQLAlchemy Connection object. Defaults to None.
        engine (Optional[Engine], optional): SQLAlchemy Engine object. Defaults to None.
        debug (bool, optional): If True, print debug information. Defaults to False.

    Returns:
        Optional[list]: list of ResourceMention objects if found, else None.
    """
    order_by = ['publication_id', 'resource_id', 'match_count']
    mention_raw = select_from_table('resource_mention', query, order_by=order_by, conn=conn, engine=engine, debug=debug)
    if len(mention_raw) == 0:
        return None

    # group by publication_id, resource_id, version_id to aggregate matched_aliases
    mentions_grouped = {}
    group_order = set()
    for m in mention_raw:
        m['mean_confidence'] = float(m['mean_confidence'])
        m['match_count'] = int(m['match_count'])

        key = (m['publication_id'], m['resource_id'], m['version_id'])
        group_order.add(key)
        if key not in mentions_grouped:
            mentions_grouped[key] = {
                'publication_id': m['publication_id'],
                'resource_id': m['resource_id'],
                'version_id': m['version_id'],
                'matched_aliases': [],
            }
        mentions_grouped[key]['matched_aliases'].append({
            'matched_alias': m['matched_alias'],
            'match_count': m['match_count'],
            'mean_confidence': m['mean_confidence']
        })
    for k in mentions_grouped:
        this_group_match_count, this_group_conf_sum, this_group_conf_n = 0, 0.0, 0
        for ma in mentions_grouped[k]['matched_aliases']:
            this_group_match_count += ma['match_count']
            this_group_conf_sum += ma['mean_confidence']
            this_group_conf_n += 1
        mentions_grouped[k]['match_count'] = this_group_match_count
        mentions_grouped[k]['mean_confidence'] = (this_group_conf_sum / this_group_conf_n) if this_group_conf_n > 0 else 0.0

    # build component objects
    mentions = []
    cache = {}
    for group_key in group_order:
        m = mentions_grouped[group_key]
        m_obj = {}
        if f"pub:{m['publication_id']}" in cache:
            m_obj['publication'] = cache[f"pub:{m['publication_id']}"]
        else:
            m_obj['publication'] = fetch_publication({'id':m['publication_id']}, expanded=expanded, conn=conn, engine=engine, debug=debug)
            cache[f"pub:{m['publication_id']}"] = m_obj['publication']

        if f"res:{m['resource_id']}" in cache:
            m_obj['resource'] = cache[f"res:{m['resource_id']}"]
        else:
            m_obj['resource'] = fetch_resource({'id':m['resource_id']}, expanded=expanded, conn=conn, engine=engine, debug=debug)
            cache[f"res:{m['resource_id']}"] = m_obj['resource']

        if f"ver:{m['version_id']}" in cache:
            m_obj['version'] = cache[f"ver:{m['version_id']}"]
        else:
            m_obj['version'] = fetch_version({'id':m['version_id']}, conn=conn, engine=engine, debug=debug)
            cache[f"ver:{m['version_id']}"] = m_obj['version']

        m_obj['matched_aliases'] = m['matched_aliases'][::-1] # reverse order to have highest count first
        m_obj['match_count'] = m['match_count']
        m_obj['mean_confidence'] = m['mean_confidence']

        mentions.append(ResourceMention(m_obj))

    return mentions

# ----------------------------------------------------------------------- #
# Other helper methods                                                    #
# ----------------------------------------------------------------------- #
def new_publication_from_EuropePMC_result(epmc_result: dict, google_maps_api_key: str = None) -> Publication:
    """Create a new Publication object from an EuropePMC search result, including additional geographic metadata enrichment.

    Args:
        epmc_result (dict): EuropePMC search result metadata.
        google_maps_api_key (str, optional): Google Maps API key for advanced geolocation.

    Returns:
        Publication: New Publication object.
    """
    print(f"Creating new Publication from EuropePMC result: {epmc_result.get('title', '')} (PMID:{epmc_result.get('pmid', '')})")
    print("Searching for author affiliations and countries...")
    affiliations, countries = _extract_affiliations(epmc_result, google_maps_api_key=google_maps_api_key)
    print(f"  Found countries: {', '.join(countries) if countries else 'None found'}")
    new_publication = Publication({
        'publication_title': epmc_result.get('title', ''), 'pubmed_id': epmc_result.get('pmid', None), 'pmc_id': epmc_result.get('pmcid', ''),
        'publication_date': epmc_result.get('journalInfo', {}).get('printPublicationDate') or epmc_result.get('firstPublicationDate'),
        'grants': _extract_grants(epmc_result),'keywords': '; '.join(_extract_keywords(epmc_result)),
        'citation_count': epmc_result.get('citedByCount', 0), 'authors': epmc_result.get('authorString', ''), 'affiliation': affiliations,
        'affiliation_countries': countries
    })
    return new_publication

def _extract_grants(metadata):
    # extract grant list
    try:
        grant_list = metadata['grantsList']['grant']
    except KeyError:
        return []

    grants = []
    for g in grant_list:
        ga = g.get('agency', '')
        grants.append(Grant({'ext_grant_id':g.get('grantId', ''), 'grant_agency':GrantAgency({'name':ga})}))

    return grants

def _extract_keywords(metadata):
    keywords = []

    # first, MeSH terms
    these_mesh_terms = metadata.get('meshHeadingList', {}).get('meshHeading', [])
    for m in these_mesh_terms:
        keywords.append(f"'{m['descriptorName']}'")
        if m.get('meshQualifierList'):
            for q in m.get('meshQualifierList', {}).get('meshQualifier', []):
                keywords.append(f"'{q['qualifierName']}'")

    # then, other keywords
    these_keywords = metadata.get('keywordList', {}).get('keyword', [])
    for k in these_keywords:
        keywords.append(f"'{k}'")

    return keywords

def _clean_affiliation(s):
    # format and replace common abbreviations
    s = re.sub(r'\s*[\w\.]+@[\w\.]+\s*', '', s) # remove email addresses & surrounding whitespace
    s = re.sub(r'\s?[A-Z]{1,2}[0-9R][0-9A-Z]? [0-9][A-Z]{2}', '', s) # remove UK postal codes
    s = re.sub(r';', ',', s) # unify delimiters
    s = ', '.join([x.strip() for x in s.split(',') if x.strip()]) # remove excess whitespace around commas
    s = re.sub(r'USA[\.]?$', 'United States', s)
    s = re.sub(r'UK[\.]?$', 'United Kingdom', s)
    return s

# pull author affiliations and identify countries
def _extract_affiliations(metadata, google_maps_api_key=None):
    # extract author affiliations & countries
    affiliations, countries = [], []
    affiliation_dict, countries_dict = {}, {}
    custom_country_mappings = {
        "People's Republic of China": "China", "Macao": "China",
        "United States of America": "United States", 'Russian Federation': 'Russia',
        'Kingdom of Saudi Arabia': 'Saudi Arabia', 'Republic of Singapore': 'Singapore'
    }

    try:
        author_list = metadata['authorList']['author']
        for author in author_list:
            affiliation_list = author.get('authorAffiliationDetailsList', {}).get('authorAffiliation', [])
            for a in affiliation_list:
                clean_a = _clean_affiliation(a['affiliation'])
                if not clean_a or clean_a in affiliation_dict:
                    continue
                affiliation_dict[clean_a] = 1
                a_countries = _find_country(clean_a, google_maps_api_key=google_maps_api_key)
                countries_dict.update({(custom_country_mappings.get(x) or x):1 for x in a_countries[0]})
        affiliations = list(affiliation_dict.keys())
        countries = sorted(list(countries_dict.keys()))
    except KeyError:
        pass

    return affiliations, countries

def _find_country(s, google_maps_api_key=None):
    # print(f"Searching for countries in '{s}'")
    if not s:
        return ([], '')

    # location search
    place_entity = locationtagger.find_locations(text = s)
    if place_entity.countries:
        return (place_entity.countries, 'locationtagger')
    elif place_entity.country_regions:
        if len(place_entity.country_regions) == 1: # no ambiguity
            return (list(place_entity.country_regions.keys()), 'locationtagger')
        elif google_maps_api_key:
            return (_advanced_geo_lookup(s, api_key=google_maps_api_key), 'GoogleMaps')
        else:
            return ([], '')
    else:
        if len(place_entity.country_cities) == 1: # no ambiguity
            return (list(place_entity.country_cities.keys()), 'locationtagger')
        elif google_maps_api_key:
            return (_advanced_geo_lookup(s, api_key=google_maps_api_key), 'GoogleMaps')
        else:
            return ([], '')

def _advanced_geo_lookup(address, api_key=None):
    gmaps = googlemaps.Client(key=api_key)
    place_search = gmaps.find_place(address, "textquery", fields=["formatted_address", "place_id"])
    try:
        place_entity = locationtagger.find_locations(text = place_search['candidates'][0]['formatted_address'])
        return place_entity.countries
    except Exception as e:
        print(f"[ERROR] Failed to find location for '{address}': {e}")
        return []