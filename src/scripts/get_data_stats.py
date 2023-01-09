"""
Displays common statistics about the curated dataset
"""

import os
import glob
# import argparse
import collections

from src import utils

def print_as_table(data):
    index_len = max(len(index) for index in data)
    rows = tuple(data.items())

    print('| ', ' '*index_len, end=' |', sep='')
    for column in rows[0][1]:
        print(' ', column.replace('_', ' ').title(), end=' |', sep='')
    print()

    print('|-', '-'*index_len, end='-|', sep='')
    for column in rows[0][1]:
        print('-', '-'*len(column), end='-|', sep='')
    print()

    for index, row_data in rows:
        print('| ', f"{index:>{index_len}}", end=' |', sep='')
        for key, value in row_data.items():
            print(' ', f"{value:^{len(key)}}", end=' |', sep='')
        print()
    print()

if __name__ == "__main__":
    # parser = argparse.ArgumentParser(
    #     description="display some statistics about the curated dataset"
    # )

    # args = parser.parse_args()

    while not os.path.exists("data"):
        os.chdir("..")

    total_data     = collections.defaultdict(int)

    for court_dir in os.listdir(os.path.join("data", "json")):
        court = court_dir.replace('Judgments', '').rstrip()
        court_dir_path = os.path.join("data", "json", court_dir)
        if not os.path.isdir(court_dir_path): continue

        per_query_data = collections.defaultdict(lambda: collections.defaultdict(int))
        per_file_data  = collections.defaultdict(lambda: collections.defaultdict(int))

        for json_file in glob.glob("*.json", root_dir=court_dir_path):
            data = utils.fs.read_json(os.path.join(court_dir_path, json_file))
            key   = f"{data['meta']['request']['query']} page {data['meta']['request']['page']}"
            query = f"{data['meta']['request']['query']}"

            per_query_data[query]['pages'] += 1
            per_query_data[query]['judgments'] += 0
            per_query_data[query]['with_file'] += 0
            per_query_data[query]['paragraphs'] += 0
            per_query_data[query]['max_paragraphs'] = max(per_query_data[query]['max_paragraphs'], 0)

            per_file_data[key]['judgments'] += 0
            per_file_data[key]['with_file'] += 0
            per_file_data[key]['paragraphs'] += 0
            per_file_data[key]['max_paragraphs'] = max(per_file_data[key]['max_paragraphs'], 0)

            for judgment in data['data']:
                paras = judgment.get('paragraphs', None)
                para_count = len(paras['adobe_api']) if paras else 0

                per_file_data[key]['judgments']        += 1
                per_file_data[key]['with_file'] += (0 if judgment['document_path'] is None else 1)
                per_file_data[key]['paragraphs']   += para_count
                per_file_data[key]['max_paragraphs'] = max(per_file_data[key]['max_paragraphs'], para_count)

                per_query_data[query]['judgments']        += 1
                per_query_data[query]['with_file'] += (0 if judgment['document_path'] is None else 1)
                per_query_data[query]['paragraphs']   += para_count
                per_query_data[query]['max_paragraphs'] = max(per_query_data[query]['max_paragraphs'], para_count)

            per_file_data[key]['avg_paragraphs']    = per_file_data[key]['paragraphs']
            per_file_data[key]['avg_paragraphs']    /= (per_file_data[key]['with_file'] or 1)
            per_file_data[key]['avg_paragraphs']    = round(per_file_data[key]['avg_paragraphs'])
            per_query_data[query]['avg_paragraphs'] = per_query_data[query]['paragraphs']
            per_query_data[query]['avg_paragraphs'] /= (per_query_data[query]['with_file'] or 1)
            per_query_data[query]['avg_paragraphs'] = round(per_query_data[query]['avg_paragraphs'], 3)

        for query_data in per_query_data.values():
            for key, value in query_data.items():
                if 'max' in key:
                    total_data[key] = max(total_data[key], value)
                else:
                    total_data[key] += value

        print("[>] data per page for query requests to", court, "website:")
        print_as_table(per_file_data)

        print("[>] aggregate data over query requests to", court, "website:")
        print_as_table(per_query_data)

    total_data['avg_paragraphs'] = total_data['paragraphs']
    total_data['avg_paragraphs'] /= (total_data['with_file'] or 1)
    total_data['avg_paragraphs'] = round(total_data['avg_paragraphs'], 3)

    print("[>] aggregate data over courts:")
    print_as_table({ 'total': total_data })