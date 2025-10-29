from __future__ import annotations

from dataclasses import dataclass
from .grant import Grant

from .utils_db import insert_into_table, delete_from_table
from .utils_fetch import fetch_publication, fetch_accession, fetch_resource_mention
from .utils import new_publication_from_EuropePMC_result

from typing import Optional, TYPE_CHECKING
from datetime import datetime, date

if TYPE_CHECKING: # only import on type checking to avoid circular imports
    from sqlalchemy.engine import Connection, Engine

    from .accession import Accession
    from .resource_mention import ResourceMention
    from .resource import Resource

@dataclass
class Publication:
    """Class representing a Publication with associated metadata and linked Grants.

    Attributes:
        id (int): Database ID for Publication
        title (str): Title of publication
        pubmed_id (int): PubMed ID
        pmc_id (str): PubMed Central ID
        publication_date (date): Date of publication
        authors (str): Authors of publication (; separated)
        affiliation (str): Affiliation of authors (; separated)
        affiliation_countries (str): Countries of affiliation (; separated)
        citation_count (int): Number of citations
        grants (list[Grant]): Associated Grant objects
        keywords (str): Keywords/Mesh terms (; separated)
    """
    id:int
    title:str
    pubmed_id:int
    pmc_id:str
    publication_date:date
    authors:str
    affiliation:str
    affiliation_countries:str
    grants:list[Grant]
    citation_count:int
    keywords:str
    __conn__:Connection
    __engine__:Engine

    def __init__(self, p):
        self.id = p.get('id')
        self.title = p.get('publication_title') or p.get('title')
        self.pubmed_id = None if p.get('pubmed_id', '') == '' else p.get('pubmed_id')
        self.pmc_id = None if p.get('pmc_id', '') == '' else p.get('pmc_id')
        self.publication_date = datetime.strptime(p.get('publication_date'), "%Y-%m-%d").date() if p.get('publication_date') and type(p.get('publication_date')) is str else p.get('publication_date')
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
            f"keywords={self.keywords}",
            f"grants=[{', '.join(g.__str__() for g in self.grants)}]" if self.grants else "grants=[]"
        ])
        return f"Publication({pub_str})"

    def write(self, conn: Optional[Connection] = None, engine: Optional[Engine] = None, debug: bool = False, force: bool = False) -> int:
        """Write Publication to database along with associated Grant data.

        Args:
            conn (Optional[Connection], optional): SQLAlchemy Connection object.
            engine (Optional[Engine], optional): SQLAlchemy Engine object.
            debug (bool, optional): If True, print debug information.
            force (bool, optional): If True, force writing of associated objects even if they have IDs.

        Returns:
            The ID of the Publication written to the database.
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
            conn (Optional[Connection], optional): SQLAlchemy Connection object.
            engine (Optional[Engine], optional): SQLAlchemy Engine object.
            debug (bool, optional): If True, print debug information.

        Returns:
            Number of rows deleted.
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
            conn (Optional[Connection], optional): SQLAlchemy Connection object.
            engine (Optional[Engine], optional): SQLAlchemy Engine object.
            debug (bool, optional): If True, print debug information.

        Returns:
            The Publication object.
        """
        return fetch_publication({'id': id}, expanded=expanded, conn=conn, engine=engine, debug=debug)

    def fetch_by_pubmed_id(pubmed_id: int, expanded: bool = False, conn: Optional[Connection] = None, engine: Optional[Engine] = None, debug: bool = False) -> Publication:
        """Fetch Publication from database by PubMed ID.

        Args:
            pubmed_id (int): PubMed ID of the Publication to fetch.
            conn (Optional[Connection], optional): SQLAlchemy Connection object.
            engine (Optional[Engine], optional): SQLAlchemy Engine object.
            debug (bool, optional): If True, print debug information.

        Returns:
            The Publication object.
        """
        return fetch_publication({'pubmed_id': pubmed_id}, expanded=expanded, conn=conn, engine=engine, debug=debug)

    def fetch_by_pmc_id(pmc_id: str, expanded: bool = False, conn: Optional[Connection] = None, engine: Optional[Engine] = None, debug: bool = False) -> Publication:
        """Fetch Publication from database by PubMed Central ID.

        Args:
            pmc_id (str): PubMed Central ID of the Publication to fetch.
            conn (Optional[Connection], optional): SQLAlchemy Connection object.
            engine (Optional[Engine], optional): SQLAlchemy Engine object.
            debug (bool, optional): If True, print debug information.

        Returns:
            The Publication object.
        """
        return fetch_publication({'pmc_id': pmc_id}, expanded=expanded, conn=conn, engine=engine, debug=debug)

    def accessions(self) -> list[Accession]:
        """Return list of Accession objects associated with this Publication.

        Returns:
            List of Accession objects.
        """
        return fetch_accession({'publication_id': self.id}, conn=self.__conn__, engine=self.__engine__, debug=False)

    def mentions(self) -> list[ResourceMention]:
        """Return list of ResourceMention objects that mention this Publication.

        Returns:
            List of ResourceMention objects.
        """
        return fetch_resource_mention({'publication_id': self.id}, conn=self.__conn__, engine=self.__engine__, debug=False)

    def references_resources(self) -> list[Resource]:
        """Return list of Resource objects that are referenced by this Publication either via an Accession or a ResourceMention.

        Returns:
            List of Resource objects.
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

    def from_EuropePMC_search(result: dict, conn: Optional[Connection] = None, engine: Optional[Engine] = None) -> Publication:
        """Create Publication object from EuropePMC search API result dictionary.

        Args:
            result (dict): EuropePMC search API result dictionary.
            conn (Optional[Connection], optional): SQLAlchemy Connection object.
            engine (Optional[Engine], optional): SQLAlchemy Engine object.

        Returns:
            The Publication object.
        """
        return new_publication_from_EuropePMC_result(result, conn=conn, engine=engine)