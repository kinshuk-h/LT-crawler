"""
Test suite for the processing stage of the pipeline.
"""

import os
import glob
import argparse
import collections

import pytest

from src import utils
from src.pipeline import preprocess, process

@pytest.fixture(scope="session")
# pylint: disable-next=redefined-outer-name,missing-function-docstring
def prog():
    return "test"
@pytest.fixture(scope="session")
# pylint: disable-next=redefined-outer-name,missing-function-docstring
def args():
    return argparse.Namespace(
        courts=[ "SC2" ], extractors=[ "generic1", "generic2", "generic3" ],
        output_dir=os.path.join("tests", "data"),
        document_dir="judgments", debug=False
    )

@pytest.fixture(scope="session")
# pylint: disable-next=redefined-outer-name,missing-function-docstring
def data_indexes(prog, args):
    return preprocess.load_indexes(prog, args)

@pytest.fixture(scope="session")
# pylint: disable-next=redefined-outer-name,missing-function-docstring
def file_index(data_indexes):
    return data_indexes[0]

path_1 = os.path.join(
            "tests", "data", "json", "SC2 Judgments",
            "judgments SC2 sample page 1.json"
        )
path_2 = os.path.join(
            "tests", "data", "json", "SC2 Judgments",
            "judgments SC2 sample page 2.json"
        )
dedup_content_test_data = [
    (
        path_2,
        {
            'generic1': {
                'judgment_indexes': [0,2,3,4,5,6],
                'indexes': [0,2,3],
                'merger_requests': {
                    path_1: [{
                        'src_index': 1,
                        'index': 0
                    }]
                }
            },
            'generic2': {
                'judgment_indexes': [0,2,4,5,6],
                'indexes': [0,3],
                'merger_requests': {
                    path_2: [{
                        'src_index': 3,
                        'index': 0
                    }]
                }
            },
            'generic3': {
                'judgment_indexes': [2,4,5,6],
                'indexes': [3,],
                'merger_requests': {
                    path_1: [{
                        'src_index': 0,
                        'index': 0
                    }]
                }
            }
        }
    ),
]

# pylint: disable-next=redefined-outer-name,missing-function-docstring
def make_batch(args, json_file, court):
    output_dir = os.path.join(args.output_dir, args.document_dir, f"{court} Judgments")
    data = utils.fs.read_json(json_file)
    judgment_files = [
        os.path.join(output_dir, os.path.basename(judgment['document_path']))
        for judgment in data['data'] if judgment['document_path'] is not None
    ]
    extraction_results = {}
    for extractor in args.extractors:
        extract_dir = os.path.join(output_dir, f"extracted_{extractor}")
        extraction_results[extractor] = [
            [
                os.path.join(extract_dir, efile)
                for efile in glob.glob(f"{os.path.basename(file)[:-4]}*", root_dir=extract_dir)
            ]
            for file in judgment_files
        ]

    return {
        'json': json_file,
        'params': data['meta']['request'],
        'judgments': judgment_files,
        'extractions': extraction_results,
        'merger_requests': collections.defaultdict(list)
    }, data['data']

@pytest.mark.parametrize("json_file,results", dedup_content_test_data)
# pylint: disable-next=redefined-outer-name,missing-function-docstring
def test_deduplicate_by_content(args, file_index, json_file, results):
    for court in args.courts:
        judgment_indexes = None
        batch, judgments = make_batch(args, json_file, court)
        for extractor in args.extractors:
            unique_judgment_indexes, batch['indexes'], merger_requests = process.deduplicate_by_content(
                file_index, extractor, batch, judgments, judgment_indexes
            )
            result = results[extractor]
            assert unique_judgment_indexes == result['judgment_indexes']
            assert batch['indexes'] == result['indexes']
            result['merger_requests'] = {
                file: [
                    {
                        'index': entry['index'],
                        'data': judgments[entry['src_index']]
                    } for entry in entries
                ]
                for file, entries in result['merger_requests'].items()
            }
            assert result['merger_requests'] == merger_requests
            batch['merger_requests'] = utils.merge_dicts(batch['merger_requests'], merger_requests)
            judgment_indexes = unique_judgment_indexes
