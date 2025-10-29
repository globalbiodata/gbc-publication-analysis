from __future__ import annotations

import re
import locationtagger
import googlemaps

from .publication import Publication
from .grant import Grant, GrantAgency

# from .utils_db import select_from_table, insert_into_table, delete_from_table
# from .utils_fetch import fetch_resource, fetch_url, fetch_version, fetch_publication, fetch_grant, fetch_grant_agency, fetch_accession, fetch_resource_mention


def extract_fields_by_type(data: dict, type_prefix: str) -> dict:
    """Extract fields from a dictionary that start with a given prefix.

    Args:
        data (dict): Input dictionary.
        type_prefix (str): Prefix to filter keys.

    Returns:
        Dictionary with extracted fields.
    """
    extracted = {}
    for k, v in data.items():
        if k.startswith(f"{type_prefix}_"):
            extracted[re.sub(f"^{type_prefix}_", "", k)] = v
    return extracted

def new_publication_from_EuropePMC_result(epmc_result: dict, google_maps_api_key: str = None) -> Publication:
    """Create a new Publication object from an EuropePMC search result, including additional geographic metadata enrichment.

    Args:
        epmc_result (dict): EuropePMC search result metadata.
        google_maps_api_key (str, optional): Google Maps API key for advanced geolocation.

    Returns:
        New Publication object.
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