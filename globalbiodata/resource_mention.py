from __future__ import annotations

from dataclasses import dataclass
from .publication import Publication
from .resource import Resource
from .version import Version

from .utils import extract_fields_by_type
from .utils_db import insert_into_table, delete_from_table
from .utils_fetch import fetch_resource_mention

from typing import Optional
from sqlalchemy.engine import Connection, Engine
from statistics import mean

@dataclass
class ResourceMention:
    """
    Object to represent a link between a Publication and a Resource, with match info.

    Attributes:
        publication (Publication): Publication object
        resource (Resource): Resource object
        version (Version): Version object
        matched_aliases (list[MatchedAlias]): list of MatchedAlias objects, each representing an alias that matched
        match_count (int): number of matches
        mean_confidence (float): mean confidence score
    """
    publication: Publication
    resource: Resource
    version: Version
    matched_aliases: list[MatchedAlias]
    match_count: int
    mean_confidence: float

    def __init__(self, m):
        self.publication = m.get('publication') or Publication(extract_fields_by_type(m, 'publication'))
        self.resource = m.get('resource') or Resource(extract_fields_by_type(m, 'resource'))
        self.version = m.get('version') or Version(extract_fields_by_type(m, 'version'))

        if m.get('matched_alias') and type(m.get('matched_alias')) is str:
            this_ma = MatchedAlias({'matched_alias': m.get('matched_alias'), 'match_count': m.get('match_count', 0), 'mean_confidence': m.get('mean_confidence', 0.0)})
            self.matched_aliases = [this_ma]
        elif type(m.get('matched_aliases')) is list:
            if type(m.get('matched_aliases')[0]) is str:
                raise ValueError("matched_aliases cannot be a list of strings; must be a list of dicts with matched_alias, match_count, and mean_confidence.")
            if type(m.get('matched_aliases')[0]) is MatchedAlias:
                self.matched_aliases = m.get('matched_aliases', [])
            elif type(m.get('matched_aliases')[0]) is dict:
                self.matched_aliases = [MatchedAlias(ma) for ma in m.get('matched_aliases', [])]
        elif type(m.get('matched_aliases')) is MatchedAlias:
            self.matched_aliases = [m.get('matched_aliases')]

        self.match_count = sum([ma.match_count for ma in self.matched_aliases])
        self.mean_confidence = mean([ma.mean_confidence for ma in self.matched_aliases]) if self.matched_aliases else 0.0

    def __str__(self):
        return (
            f"ResourceMention(pub={self.publication.__str__()}, res={self.resource.__str__()}, "
            f"ver={self.version.__str__()}, aliases='{self.matched_aliases}', "
            f"count={self.match_count}, mean_conf={self.mean_confidence})"
        )

    def write(self, conn: Optional[Connection] = None, engine: Optional[Engine] = None, debug: bool = False, force: bool = False) -> int:
        """Write ResourceMention to database along with associated Publication, Resource, and Version data.

        Args:
            conn (Optional[Connection], optional): SQLAlchemy Connection object.
            engine (Optional[Engine], optional): SQLAlchemy Engine object.
            debug (bool, optional): If `True`, print debug information.
            force (bool, optional): If `True`, force writing of associated objects even if they have IDs.

        Returns:
            The ID of the ResourceMention written to the database.
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
                'matched_alias': matched_alias.matched_alias,
                'match_count': matched_alias.match_count,
                'mean_confidence': matched_alias.mean_confidence,
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
            conn (Optional[Connection], optional): SQLAlchemy Connection object.
            engine (Optional[Engine], optional): SQLAlchemy Engine object.
            debug (bool, optional): If `True`, print debug information.

        Returns:
            The list of ResourceMention objects.
        """
        return fetch_resource_mention({'publication_id': publication_id}, conn=conn, engine=engine, debug=debug)

    def fetch_by_resource_id(resource_id: int, conn: Optional[Connection] = None, engine: Optional[Engine] = None, debug: bool = False) -> list:
        """Fetch ResourceMention from database by Resource ID.

        Args:
            resource_id (int): Resource ID of the ResourceMention to fetch.
            conn (Optional[Connection], optional): SQLAlchemy Connection object.
            engine (Optional[Engine], optional): SQLAlchemy Engine object.
            debug (bool, optional): If `True`, print debug information.

        Returns:
            The list of ResourceMention objects.
        """
        return fetch_resource_mention({'resource_id': resource_id}, conn=conn, engine=engine, debug=debug)

@dataclass
class MatchedAlias:
    """ Object to represent a matched alias with match count and confidence.

    Attributes:
        matched_alias (str): alias string that matched
        match_count (int): number of matches
        mean_confidence (float): mean confidence score
    """
    matched_alias: str
    match_count: int
    mean_confidence: float

    def __init__(self, ma):
        self.matched_alias = ma.get('matched_alias')
        self.match_count = ma.get('match_count', 0)
        self.mean_confidence = ma.get('mean_confidence', 0.0)
