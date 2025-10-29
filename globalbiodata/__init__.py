from __future__ import annotations

from .accession import Accession
from .grant import Grant, GrantAgency
from .publication import Publication
from .resource import Resource
from .resource_mention import ResourceMention, MatchedAlias
from .url import URL, ConnectionStatus
from .version import Version

from .utils_db import insert_into_table, delete_from_table, select_from_table
from .utils_fetch import fetch_accession, fetch_grant, fetch_grant_agency, fetch_publication, fetch_resource, fetch_resource_mention, fetch_url, fetch_connection_status, fetch_version
from .utils_fetch import fetch_all_resources, fetch_all_grant_agencies, fetch_all_grants, fetch_all_publications, fetch_all_urls, fetch_all_connection_statuses, fetch_all_versions, fetch_all_online_resources
from .utils import extract_fields_by_type, new_publication_from_EuropePMC_result

__all__ = [
    # classes
    'Accession',
    'Grant',
    'GrantAgency',
    'Publication',
    'Resource',
    'ResourceMention',
    'MatchedAlias',
    'URL',
    'ConnectionStatus',
    'Version',

    # db utils
    'insert_into_table',
    'delete_from_table',
    'select_from_table',

    # fetch utils
    'fetch_accession',
    'fetch_grant',
    'fetch_grant_agency',
    'fetch_publication',
    'fetch_resource',
    'fetch_resource_mention',
    'fetch_url',
    'fetch_connection_status',
    'fetch_version',
    'fetch_all_resources',
    'fetch_all_grant_agencies',
    'fetch_all_grants',
    'fetch_all_publications',
    'fetch_all_urls',
    'fetch_all_connection_statuses',
    'fetch_all_versions',
    'fetch_all_online_resources',

    # other utils
    'extract_fields_by_type',
    'new_publication_from_EuropePMC_result'
]