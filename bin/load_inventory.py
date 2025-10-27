#!/usr/bin/env python3

import json
import os
import sys
import argparse

import pandas as pd
from collections import OrderedDict

import globalbiodata as gbc
from gbcutils.gbc_db import get_gbc_connection
from gbcutils.europepmc import epmc_search


def build_parser():
    parser = argparse.ArgumentParser()
    parser.add_argument('--csv', type=str, required=True, help='Inventory CSV file')
    parser.add_argument('--version-file', type=str, default='version.json', help='JSON file describing version info')
    parser.add_argument('--resume-from', type=str, help='Short name of resource to resume processing from')
    parser.add_argument('--test', action='store_true', help='Use test database')
    return parser


def uniq_with_order(s, to_remove=[]):
    spl = [x.strip() for x in str(s).split(',')]
    sl = list(OrderedDict.fromkeys(spl))
    for r in to_remove:
        if r in sl:
            sl.remove(r)
    return '; '.join(sl)

# def find_country(s):
#     # print(f"Searching for countries in '{s}'")

#     # location search
#     place_entity = locationtagger.find_locations(text = s)
#     if place_entity.countries:
#         return place_entity.countries, 'locationtagger'
#     elif place_entity.country_regions:
#         if len(place_entity.country_regions) == 1: # no ambiguity
#             return list(place_entity.country_regions.keys()), 'locationtagger'
#         else:
#             return advanced_geo_lookup(s), 'GoogleMaps'
#     else:
#         if len(place_entity.country_cities) == 1: # no ambiguity
#             return list(place_entity.country_cities.keys()), 'locationtagger'
#         else:
#             return advanced_geo_lookup(s), 'GoogleMaps'

# def advanced_geo_lookup(address):
#     place_search = gmaps.find_place(address, "textquery", fields=["formatted_address", "place_id"])
#     try:
#         place_entity = locationtagger.find_locations(text = place_search['candidates'][0]['formatted_address'])
#         return place_entity.countries
#     except:
#         return []


def explode_record(record):
    ids = [x.strip() for x in record['pubmed_id'].split(',')]
    exploded_records = []
    for i in ids:
        new_record = record.copy()
        new_record['pubmed_id'] = i
        exploded_records.append(new_record)
    return exploded_records


# def query_europepmc(pubmed_id, retry_count=0):
#     url = f"https://www.ebi.ac.uk/europepmc/webservices/rest/search?query=EXT_ID:{pubmed_id}&resultType=core&format=json"
#     response = requests.get(url)
#     if response.status_code == 200:
#         data = response.json()

#     if not data.get('hitCount'):
#         # response is malformed - retry up to 3 times
#         sys.stderr.write(f"Error: No data found for {pubmed_id}\n")
#         if retry_count < 3:
#             return query_europepmc(pubmed_id, retry_count=retry_count+1)
#         else:
#             sys.exit(f"Error: No data found for {pubmed_id} after 3 retries\n")

#     if data['hitCount'] == 0:
#         sys.stderr.write(f"Error: No data found for {pubmed_id}\n")
#         return {}

#     return data['resultList']['result'][0]

# def extract_grants(metadata):
#     # extract grant list
#     try:
#         grant_list = metadata['grantsList']['grant']
#     except KeyError:
#         return {}

#     grants = []
#     for g in grant_list:
#         grants.append([g.get('grantId', ''), g.get('agency', '')])

#     return grants

# def extract_keywords(metadata):
#     # extract MeSH terms
#     keywords = []

#     these_mesh_terms = metadata.get('meshHeadingList', {}).get('meshHeading', [])
#     for m in these_mesh_terms:
#         keywords.append(f"'{m['descriptorName']}'")
#         if m.get('meshQualifierList'):
#             for q in m.get('meshQualifierList', {}).get('meshQualifier', []):
#                 keywords.append(f"'{q['qualifierName']}'")

#     these_keywords = metadata.get('keywordList', {}).get('keyword', [])
#     for k in these_keywords:
#         keywords.append(f"'{k}'")

#     return keywords


# def expand_metadata(record):
#     metadata = query_europepmc(record['pubmed_id'])

#     record['publication_title'] = metadata.get('title', '')
#     record['publication_date'] = metadata.get('journalInfo', {}).get('printPublicationDate')
#     record['pmc_id'] = metadata.get('pmcid')
#     record['grants'] = extract_grants(metadata)
#     record.pop('ext_grant_ids')
#     record.pop('grant_agencies')

#     record['keywords'] = extract_keywords(metadata)
#     record['authors'] = metadata.get('authorString')
#     record['affiliation'] = gbc._extract_affiliations(metadata)

#     # improved country search
#     if (not record['affiliation_countries'] or record['affiliation_countries'] == 'None') and record['affiliation']:
#         # print(f"\n\n\n\n'{'; '.join(record['affiliation'])}' has no countries - searching for countries")
#         country_dict = {}
#         affiliation_dict = {} # avoid querying the same affiliation multiple times
#         for a in record['affiliation']:
#             countries, source = find_country(a)
#             for x in countries:
#                 country_dict[x] = source
#             affiliation_dict[a] = 1

#         record['affiliation_countries'] = list(country_dict.keys())
#         record['affiliation_country_sources'] = list(country_dict.values())

#     return record


def split_record_data(record):
    # check if last ping status was good
    url_status = record['url_status']
    record['online'] = (str(url_status)[:18] not in ['404', '500', 'HTTPConnectionPool'])

    # remove duplicate info from metadata lists
    record['affiliation_countries'] = uniq_with_order(record['affiliation_countries'], ['nan'])
    record['authors'] = uniq_with_order(record['authors'])

    # move prediction probability scores to a new column
    prediction_metadata = {}
    prediction_metadata['short_name_prob'] = record.pop('short_name_prob')
    prediction_metadata['common_name_prob'] = record.pop('common_name_prob')
    prediction_metadata['full_name_prob'] = record.pop('full_name_prob')
    record['resource_prediction_metadata'] = prediction_metadata

    return record



column_rename = {
    "ID":"pubmed_id", "best_name":"short_name", "best_name_prob":"short_name_prob",
    "best_common":"common_name", "best_common_prob":"common_name_prob",
    "best_full":"full_name", "best_full_prob":"full_name_prob",
    'num_citations':'citation_count', 'grant_ids':'ext_grant_ids',
    'extracted_url':'url', 'extracted_url_status':'url_status', 'extracted_url_country':'url_country',
    'extracted_url_coordinates':'url_coordinates'
}

def main(argv=None):
    parser = build_parser()
    args = parser.parse_args(argv)

    resume_from = args.resume_from

    gcp_connector, cloud_engine, cloud_conn = get_gbc_connection(
        test=args.test, readonly=False,
        sqluser=os.environ.get('GBC_SQL_USER'),
        sqlpass=os.environ.get('GBC_SQL_PASS')
    )

    gmaps_api_key = os.environ.get('GOOGLE_MAPS_API_KEY')
    if not gmaps_api_key:
        sys.stderr.write("[WARN] Google Maps API key not found in environment variable GOOGLE_MAPS_API_KEY : using basic location detection only\n")


    # read input CSV file
    df = pd.read_csv(args.csv, dtype='str').rename(columns=column_rename).replace({float('nan'): None})

    # set version info from JSON file
    version_info = json.load(open(args.version_file, 'r'))
    df['version_name'] = version_info['name']
    df['version_date'] = version_info['date']
    df['version_user'] = version_info['user']
    df['version_data'] = version_info['data']
    df['connection_date'] = version_info['connection_date'] or version_info['date']
    df['is_latest'] = version_info['is_latest'] or 1

    # process each record & generate GBC Resource objects
    record_total = df.shape[0]
    record_count = 0
    for r1 in df.to_dict('records'):
        record_count += 1
        if resume_from:
            if r1['short_name'] == resume_from:
                print(f"Skipped {record_count} records. Resuming from {resume_from}")
                resume_from = None
            else:
                continue

        print(f"Processing record {r1['short_name']} ({record_count} of {record_total} records)")
        exploded_records = explode_record(r1)
        subrecord_total = len(exploded_records)
        subrecord_count = 1
        for r2 in exploded_records:
            if subrecord_total > 1:
                print(f"\tprocessing subrecord PubMedID {r2['pubmed_id']} ({subrecord_count} of {subrecord_total} subrecords)")
            # orig_r = r2.copy()

            split_record = split_record_data(r2)

            # add publication info
            epmc_metadata = epmc_search(f"EXT_ID:{split_record['pubmed_id']}")[0]
            resource_publication = gbc.new_publication_from_EuropePMC_result(epmc_metadata, google_maps_api_key=gmaps_api_key)
            resource = gbc.Resource(split_record)
            resource.publications = [resource_publication]
            resource.write(engine=cloud_engine, debug=True)

            subrecord_count += 1

    return 0

if __name__ == '__main__':
    raise SystemExit(main())