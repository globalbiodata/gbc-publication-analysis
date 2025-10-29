from __future__ import annotations

import re
from .utils_db import select_from_table

from typing import Optional, TYPE_CHECKING
from sqlalchemy.engine import Connection, Engine

if TYPE_CHECKING: # to avoid circular imports
    from .resource import Resource
    from .url import URL, ConnectionStatus
    from .version import Version
    from .publication import Publication
    from .grant import Grant, GrantAgency


# ----------------------------------------------------------------------- #
# Fetcher methods for Global Biodata Resource data                        #
# ----------------------------------------------------------------------- #

def fetch_resource(query: dict, order_by: str = 'id', expanded: bool = True, conn: Optional[Connection] = None, engine: Optional[Engine] = None, debug: bool = False) -> Optional[Resource]:
    """Fetch Resource(s) from the database matching the provided query.

    Args:
        query (dict): Dictionary of column names and values to match for selection.
        order_by (str, optional): Column name(s) to order the results by.
        expanded (bool, optional): If `True`, fetch associated publications and grants.
        conn (Optional[Connection], optional): SQLAlchemy Connection object.
        engine (Optional[Engine], optional): SQLAlchemy Engine object.
        debug (bool, optional): If `True`, print debug information.

    Returns:
        single Resource object (where single result if found), or list of Resource objects if found, else `None`.
    """
    from .resource import Resource

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
        order_by (str, optional): Column name(s) to order the results by.
        expanded (bool, optional): If `True`, fetch associated publications and grants.
        conn (Optional[Connection], optional): SQLAlchemy Connection object.
        engine (Optional[Engine], optional): SQLAlchemy Engine object.
        debug (bool, optional): If `True`, print debug information.

    Returns:
        List of Resource objects.
    """
    return fetch_resource({}, order_by=order_by, expanded=expanded, conn=conn, engine=engine, debug=debug)

def fetch_all_online_resources(order_by: str = 'id', expanded: bool = True, conn: Optional[Connection] = None, engine: Optional[Engine] = None, debug: bool = False) -> list:
    """Fetch all Resources from the database where online status is true.

    Args:
        order_by (str, optional): Column name(s) to order the results by.
        expanded (bool, optional): If `True`, fetch associated publications and grants.
        conn (Optional[Connection], optional): SQLAlchemy Connection object.
        engine (Optional[Engine], optional): SQLAlchemy Engine object.
        debug (bool, optional): If `True`, print debug information.

    Returns:
        List of online Resource objects.
    """
    full_list = fetch_all_resources(order_by=order_by, expanded=expanded, conn=conn, engine=engine, debug=debug)
    return [r for r in full_list if r.is_online()]

def fetch_url(query: dict, order_by: str = 'id', expanded: bool = True, conn: Optional[Connection] = None, engine: Optional[Engine] = None, debug: bool = False) -> Optional[URL]:
    """Fetch URL(s) from the database matching the provided query.

    Args:
        query (dict): Dictionary of column names and values to match for selection.
        order_by (str, optional): Column name(s) to order the results by.
        expanded (bool, optional): If `True`, fetch associated connection status.
        conn (Optional[Connection], optional): SQLAlchemy Connection object.
        engine (Optional[Engine], optional): SQLAlchemy Engine object.
        debug (bool, optional): If `True`, print debug information.

    Returns:
        single URL object (where single result if found), or list of URL objects if found, else `None`.
    """
    from .url import URL

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
        order_by (str, optional): Column name(s) to order the results by.
        expanded (bool, optional): If `True`, fetch associated connection status.
        conn (Optional[Connection], optional): SQLAlchemy Connection object.
        engine (Optional[Engine], optional): SQLAlchemy Engine object.
        debug (bool, optional): If `True`, print debug information.

    Returns:
        List of URL objects.
    """
    return fetch_url({}, order_by=order_by, expanded=expanded, conn=conn, engine=engine, debug=debug)

def fetch_connection_status(query: dict, order_by: str = 'url_id', conn: Optional[Connection] = None, engine: Optional[Engine] = None, debug: bool = False) -> Optional[ConnectionStatus]:
    """Fetch ConnectionStatus(es) from the database matching the provided query.

    Args:
        query (dict): Dictionary of column names and values to match for selection.
        order_by (str, optional): Column name(s) to order the results by.
        conn (Optional[Connection], optional): SQLAlchemy Connection object.
        engine (Optional[Engine], optional): SQLAlchemy Engine object.
        debug (bool, optional): If `True`, print debug information.

    Returns:
        single ConnectionStatus object (where single result if found), or list of ConnectionStatus objects if found, else `None`.
    """
    from .url import ConnectionStatus

    status_raw = select_from_table('connection_status', query, order_by=order_by, conn=conn, engine=engine, debug=debug)
    if len(status_raw) == 0:
        return None

    conn_stats = [ConnectionStatus(cs) for cs in status_raw]

    return conn_stats if len(conn_stats) > 1 else conn_stats[0]

def fetch_all_connection_statuses(order_by: str = 'url_id', conn: Optional[Connection] = None, engine: Optional[Engine] = None, debug: bool = False) -> list:
    """Fetch all ConnectionStatuses from the database.

    Args:
        order_by (str, optional): Column name(s) to order the results by.
        conn (Optional[Connection], optional): SQLAlchemy Connection object.
        engine (Optional[Engine], optional): SQLAlchemy Engine object.
        debug (bool, optional): If `True`, print debug information.

    Returns:
        List of ConnectionStatus objects.
    """
    return fetch_connection_status({}, order_by=order_by, conn=conn, engine=engine, debug=debug)

def fetch_version(query: dict, order_by: str = 'id', conn: Optional[Connection] = None, engine: Optional[Engine] = None, debug: bool = False) -> Optional[Version]:
    """Fetch Version(s) from the database matching the provided query.

    Args:
        query (dict): Dictionary of column names and values to match for selection.
        order_by (str, optional): Column name(s) to order the results by.
        conn (Optional[Connection], optional): SQLAlchemy Connection object.
        engine (Optional[Engine], optional): SQLAlchemy Engine object.
        debug (bool, optional): If `True`, print debug information.

    Returns:
		single Version object (where single result if found), or list of Version objects if found, else `None`.
    """
    from .version import Version

    version_raw = select_from_table('version', query, order_by=order_by, conn=conn, engine=engine, debug=debug)
    if len(version_raw) == 0:
        return None

    versions = [Version(p) for p in version_raw]

    return versions if len(versions) > 1 else versions[0]

def fetch_all_versions(order_by: str = 'id', conn: Optional[Connection] = None, engine: Optional[Engine] = None, debug: bool = False) -> list:
    """Fetch all Versions from the database.

    Args:
        order_by (str, optional): Column name(s) to order the results by.
        conn (Optional[Connection], optional): SQLAlchemy Connection object.
        engine (Optional[Engine], optional): SQLAlchemy Engine object.
        debug (bool, optional): If `True`, print debug information.

    Returns:
		List of Version objects.
    """
    return fetch_version({}, order_by=order_by, conn=conn, engine=engine, debug=debug)

def fetch_publication(query: dict, order_by: str = 'id', expanded: bool = True, conn: Optional[Connection] = None, engine: Optional[Engine] = None, debug: bool = False) -> Optional[Publication]:
    """Fetch Publication(s) from the database matching the provided query.

    Args:
        query (dict): Dictionary of column names and values to match for selection.
        order_by (str, optional): Column name(s) to order the results by.
        expanded (bool, optional): If `True`, fetch associated grants.
        conn (Optional[Connection], optional): SQLAlchemy Connection object.
        engine (Optional[Engine], optional): SQLAlchemy Engine object.
        debug (bool, optional): If `True`, print debug information.

    Returns:
		single Publication object (where single result if found), or list of Publication objects if found, else `None`.
    """
    from .publication import Publication

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
        order_by (str, optional): Column name(s) to order the results by.
        expanded (bool, optional): If `True`, fetch associated grants.
        conn (Optional[Connection], optional): SQLAlchemy Connection object.
        engine (Optional[Engine], optional): SQLAlchemy Engine object.
        debug (bool, optional): If `True`, print debug information.

    Returns:
		List of Publication objects.
    """
    return fetch_publication({}, order_by=order_by, expanded=expanded, conn=conn, engine=engine, debug=debug)

def fetch_grant(query: dict, order_by: str = 'id', conn: Optional[Connection] = None, engine: Optional[Engine] = None, debug: bool = False) -> Optional[Grant]:
    """Fetch Grant(s) from the database matching the provided query.

    Args:
        query (dict): Dictionary of column names and values to match for selection.
        order_by (str, optional): Column name(s) to order the results by.
        conn (Optional[Connection], optional): SQLAlchemy Connection object.
        engine (Optional[Engine], optional): SQLAlchemy Engine object.
        debug (bool, optional): If `True`, print debug information.

    Returns:
		single Grant object (where single result if found), or list of Grant objects if found, else `None`.
    """
    from .grant import Grant

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
        order_by (str, optional): Column name(s) to order the results by.
        conn (Optional[Connection], optional): SQLAlchemy Connection object.
        engine (Optional[Engine], optional): SQLAlchemy Engine object.
        debug (bool, optional): If `True`, print debug information.

    Returns:
		List of Grant objects.
    """
    return fetch_grant({}, order_by=order_by, conn=conn, engine=engine, debug=debug)

def fetch_grant_agency(query: dict, order_by: str = 'id', conn: Optional[Connection] = None, engine: Optional[Engine] = None, debug: bool = False) -> Optional[GrantAgency]:
    """Fetch GrantAgency(s) from the database matching the provided query.

    Args:
        query (dict): Dictionary of column names and values to match for selection.
        order_by (str, optional): Column name(s) to order the results by.
        conn (Optional[Connection], optional): SQLAlchemy Connection object.
        engine (Optional[Engine], optional): SQLAlchemy Engine object.
        debug (bool, optional): If `True`, print debug information.

    Returns:
		single GrantAgency object (where single result if found), or list of GrantAgency objects if found, else `None`.
    """
    from .grant import GrantAgency

    grant_agency_raw = select_from_table('grant_agency', query, order_by=order_by, conn=conn, engine=engine, debug=debug)
    if len(grant_agency_raw) == 0:
        return None

    grant_agencies = [GrantAgency(ga) for ga in grant_agency_raw]

    return grant_agencies if len(grant_agencies) > 1 else grant_agencies[0]

def fetch_all_grant_agencies(order_by: str = 'id', conn: Optional[Connection] = None, engine: Optional[Engine] = None, debug: bool = False) -> list:
    """Fetch all GrantAgencies from the database.

    Args:
        order_by (str, optional): Column name(s) to order the results by.
        conn (Optional[Connection], optional): SQLAlchemy Connection object.
        engine (Optional[Engine], optional): SQLAlchemy Engine object.
        debug (bool, optional): If `True`, print debug information.

    Returns:
		List of GrantAgency objects.
    """
    return fetch_grant_agency({}, order_by=order_by, conn=conn, engine=engine, debug=debug)

def fetch_accession(query: dict, expanded: bool = False, conn: Optional[Connection] = None, engine: Optional[Engine] = None, debug: bool = False) -> Optional[list]:
    """Fetch Accession(es) from the database matching the provided query.

    Args:
        query (dict): Dictionary of column names and values to match for selection.
        expanded (bool, optional): If `True`, fetch associated resource, version, and publications.
        conn (Optional[Connection], optional): SQLAlchemy Connection object.
        engine (Optional[Engine], optional): SQLAlchemy Engine object.
        debug (bool, optional): If `True`, print debug information.

    Returns:
		list of Accession objects if found, else `None`.
    """
    from .accession import Accession

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
        expanded (bool, optional): If `True`, fetch associated resource, version, and publication.
        conn (Optional[Connection], optional): SQLAlchemy Connection object.
        engine (Optional[Engine], optional): SQLAlchemy Engine object.
        debug (bool, optional): If `True`, print debug information.

    Returns:
		list of ResourceMention objects if found, else `None`.
    """
    from .resource_mention import ResourceMention, MatchedAlias

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
        mentions_grouped[key]['matched_aliases'].append(MatchedAlias({
            'matched_alias': m['matched_alias'],
            'match_count': m['match_count'],
            'mean_confidence': m['mean_confidence']
        }))
    for k in mentions_grouped:
        this_group_match_count, this_group_conf_sum, this_group_conf_n = 0, 0.0, 0
        for ma in mentions_grouped[k]['matched_aliases']:
            this_group_match_count += ma.match_count
            this_group_conf_sum += ma.mean_confidence
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