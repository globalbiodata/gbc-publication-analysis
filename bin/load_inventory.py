#!/usr/bin/env python3

import json
import os
import sys
import argparse

import pandas as pd
from collections import OrderedDict

import globalbiodata as gbc
from gbcutils.db import get_gbc_connection
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

def explode_record(record):
    ids = [x.strip() for x in record['pubmed_id'].split(',')]
    exploded_records = []
    for i in ids:
        new_record = record.copy()
        new_record['pubmed_id'] = i
        exploded_records.append(new_record)
    return exploded_records


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