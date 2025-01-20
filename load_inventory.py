#!/usr/bin/env python3

import os
import re
import sys
from pprint import pprint
import argparse

import pandas as pd
from collections import OrderedDict
import requests

import sqlalchemy as db
from google.cloud.sql.connector import Connector
import pymysql
import globalbiodata as gbc

import locationtagger
import googlemaps
gmaps = googlemaps.Client(key='AIzaSyB0uGcyCjumIOjBpUHS1GQh1SRiPerHjn0')


parser = argparse.ArgumentParser()
parser.add_argument('--resume-from', type=str, help='Short name of resource to resume processing from')
args = parser.parse_args()
resume_from = args.resume_from if args.resume_from else None


gcp_connector = Connector()
def getcloudconn() -> pymysql.connections.Connection:
    conn: pymysql.connections.Connection = gcp_connector.connect(
        "gbc-publication-analysis:europe-west2:gbc-sql",
        "pymysql",
        user=os.environ.get("CLOUD_SQL_USER"),
        password=os.environ.get("CLOUD_SQL_PASSWORD"),
        db="gbc-publication-analysis"
        # db="gbc-publication-analysis-test"
    )
    return conn

def uniq_with_order(s, to_remove=[]):
    spl = [x.strip() for x in str(s).split(',')]
    l = list(OrderedDict.fromkeys(spl))
    for r in to_remove:
        if r in l: l.remove(r)
    return '; '.join(l)

def clean_affilation(s):
    # format and replace common abbreviations
    s = re.sub(r'\s*[\w\.]+@[\w\.]+\s*', '', s) # remove email addresses & surrounding whitespace
    s = re.sub(r'\s?[A-Z]{1,2}[0-9R][0-9A-Z]? [0-9][A-Z]{2}', '', s) # remove UK postal codes
    s = s.strip()
    s = re.sub(r'USA[\.]?$', 'United States', s)
    s = re.sub(r'UK[\.]?$', 'United Kingdom', s)
    return s

def find_country(s):
    # print(f"Searching for countries in '{s}'")

    # location search
    place_entity = locationtagger.find_locations(text = s)
    if place_entity.countries:
        return place_entity.countries, 'locationtagger'
    elif place_entity.country_regions:
        if len(place_entity.country_regions) == 1: # no ambiguity
            return list(place_entity.country_regions.keys()), 'locationtagger'
        else:
            return advanced_geo_lookup(s), 'GoogleMaps'
    else: 
        if len(place_entity.country_cities) == 1: # no ambiguity
            return list(place_entity.country_cities.keys()), 'locationtagger'
        else:
            return advanced_geo_lookup(s), 'GoogleMaps'

def advanced_geo_lookup(address):
    place_search = gmaps.find_place(address, "textquery", fields=["formatted_address", "place_id"])
    try:
        place_entity = locationtagger.find_locations(text = place_search['candidates'][0]['formatted_address'])
        return place_entity.countries
    except:
        return []


def explode_record(record):
    ids = [x.strip() for x in record['pubmed_id'].split(',')]
    exploded_records = []
    for i in ids:
        new_record = record.copy()
        new_record['pubmed_id'] = i
        exploded_records.append(new_record)
    return exploded_records


def query_europepmc(pubmed_id, retry_count=0):
    url = f"https://www.ebi.ac.uk/europepmc/webservices/rest/search?query=EXT_ID:{pubmed_id}&resultType=core&format=json"
    response = requests.get(url)
    if response.status_code == 200:
        data = response.json()

    if not data.get('hitCount'):
        # response is malformed - retry up to 3 times
        sys.stderr.write(f"Error: No data found for {pubmed_id}\n")
        if retry_count < 3:
            return query_europepmc(pubmed_id, retry_count=retry_count+1)
        else:
            sys.exit(f"Error: No data found for {pubmed_id} after 3 retries\n")
        
    if data['hitCount'] == 0:
        sys.stderr.write(f"Error: No data found for {pubmed_id}\n")
        return {}


    # try:
    #     data['resultList']['result'][0]
    # except IndexError:
    #     sys.stderr.write(f"Error: No data found for {pubmed_id}\n")
    #     return {}
    # except KeyError:
    #     sys.stderr.write(f"Error: No data found for {pubmed_id}\n")
    #     print(data)
    #     return {}

    return data['resultList']['result'][0]

def extract_grants(metadata):
    # extract grant list
    try:
        grant_list = metadata['grantsList']['grant']
    except KeyError:
        return {}
    
    grants = {}
    for g in grant_list:
        grants[g.get('grantId', '')] = g.get('agency', '')
    
    return grants

def extract_keywords(metadata):
    # extract MeSH terms
    keywords = []
    try:
        these_mesh_terms = metadata['meshHeadingList']['meshHeading']
        for m in these_mesh_terms:
            keywords.append(f"'{m['descriptorName']}'")
            if m.get('meshQualifierList'):
                for q in m['meshQualifierList']['meshQualifier']:
                    keywords.append(f"'{q['qualifierName']}'")
    except KeyError:
        keywords = []

    return keywords

def extract_affiliations(metadata):
    # extract author affiliations & countries
    affiliations = []
    affiliation_dict = {}
    try:
        author_list = metadata['authorList']['author']
        for author in author_list:
            affiliation_list = author.get('authorAffiliationDetailsList', {}).get('authorAffiliation', [])
            for a in affiliation_list:
                clean_a = clean_affilation(a['affiliation'])
                affiliation_dict[clean_a] = 1
        affiliations = list(affiliation_dict.keys())
    except KeyError:
        affiliations = []

    return affiliations

def expand_metadata(record):
    metadata = query_europepmc(record['pubmed_id'])

    record['publication_title'] = metadata.get('title', '')
    record['publication_date'] = metadata.get('journalInfo', {}).get('printPublicationDate')
    record['pmc_id'] = metadata.get('pmcid')
    record['grants'] = extract_grants(metadata)
    record.pop('ext_grant_ids')
    record.pop('grant_agencies')

    record['keywords'] = extract_keywords(metadata)
    record['authors'] = metadata.get('authorString')
    record['affiliation'] = extract_affiliations(metadata)

    # improved country search
    if (not record['affiliation_countries'] or record['affiliation_countries'] == 'None') and record['affiliation']:
        # print(f"\n\n\n\n'{'; '.join(record['affiliation'])}' has no countries - searching for countries")
        country_dict = {}
        affiliation_dict = {} # avoid querying the same affiliation multiple times
        for a in record['affiliation']:
            countries, source = find_country(a)
            for x in countries:
                country_dict[x] = source
            affiliation_dict[a] = 1

        record['affiliation_countries'] = list(country_dict.keys())
        record['affiliation_country_sources'] = list(country_dict.values())

    return record
    

def clean_data(record):
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
# df22 = pd.read_csv('running_inventory_2022.csv').rename(columns=column_rename).replace({float('nan'): None})
# df22['prediction_name'] = 'inventory pipeline 1'
# df22['prediction_date'] = '2022-07-12'
# df22['prediction_user'] = 'kschackart'
# df22['prediction_data'] = '{}'
# df22['connection_date'] = '2022-07-12'
# df22['is_latest'] = 0

df24 = pd.read_csv('running_inventory_2024.csv').rename(columns=column_rename).replace({float('nan'): None})
df24['prediction_name'] = 'inventory pipeline 2'
df24['prediction_date'] = '2024-07-12'
df24['prediction_user'] = 'kschackart'
df24['prediction_data'] = '{}'
df24['connection_date'] = '2024-07-12'
df24['is_latest'] = 1

# df = pd.concat([df22, df24], ignore_index=True)
df = df24

# test_df = pd.read_csv('test_predictions_2.csv', dtype='str').rename(columns=column_rename).replace({float('nan'): None})
# test_df['prediction_name'] = 'api test'
# test_df['prediction_date'] = '2025-01-08'
# test_df['prediction_user'] = 'carlac'
# test_df['connection_date'] = '2025-01-15'
# test_df['is_latest'] = 1
# print(test_df)

# df = test_df

cloud_engine = db.create_engine("mysql+pymysql://", creator=getcloudconn, pool_recycle=60 * 5, pool_pre_ping=True)
cloud_conn = cloud_engine.connect()

comp_outfile = open('metadata_comparison.tsv', 'a')
comp_headers = [
    'pubmed_id', 'old_affiliation', 'new_affiliation', 'old_countries', 'new_countries', 'country_source',
    'old_grant_ids', 'new_grant_ids', 'old_grant_agencies', 'new_grant_agencies',
]
if not os.path.exists('metadata_comparison.tsv'):
    comp_outfile.write(f"{'\t'.join(comp_headers)}\n")

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
        orig_r = r2.copy()
        
        clean_record = clean_data(r2)
        expanded_record = expand_metadata(clean_record)

        # record comparison of original and expanded records
        comp_l = [
            orig_r['pubmed_id'], orig_r['affiliation'], expanded_record['affiliation'], orig_r['affiliation_countries'], expanded_record['affiliation_countries'], expanded_record.get('affiliation_country_sources', 'original'),
            orig_r['ext_grant_ids'], list(expanded_record['grants'].keys()), orig_r['grant_agencies'], uniq_with_order(','.join(expanded_record['grants'].values()))
        ]
        comp_s = ''
        for c in comp_l:
            if type(c) == list:
                comp_s += f"{'; '.join(c)}\t"
            else:
                comp_s += f"{c}\t"
        comp_outfile.write(f"{comp_s}\n")
            

        # create GBC API objects and load to DB
        grants = []
        for g, ga in expanded_record['grants'].items():
            grant_data = {'ext_grant_id': g, 'grant_agency': ga}
            grant = gbc.Grant(grant_data)
            grants.append(grant)
        expanded_record['grants'] = grants
        resource = gbc.Resource(expanded_record)
        resource.write(engine=cloud_engine)
        
        subrecord_count += 1

comp_outfile.close()