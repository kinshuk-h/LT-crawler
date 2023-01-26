"""
Test suite for preprocessing stage of the pipeline.
"""

import os
import argparse

import pytest

from src.pipeline import preprocess

@pytest.fixture(scope="session")
# pylint: disable-next=redefined-outer-name,missing-function-docstring
def prog():
    return "test"
@pytest.fixture(scope="session")
# pylint: disable-next=redefined-outer-name,missing-function-docstring
def args():
    return argparse.Namespace(
        courts=[ "SC" ], extractors=[ "generic" ],
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
@pytest.fixture(scope="session")
# pylint: disable-next=redefined-outer-name,missing-function-docstring
def judgment_index(data_indexes):
    return data_indexes[1]

judgment_index_test_data = [
    (
        ({
            'case_number': 'SC 188/2022',
            # pylint: disable-next=line-too-long
            'document_href': 'http://dhcappl.nic.in/FreeText/download.do?FILENAME=dhc/PMS/judgement/12-10-2022//PMS10102022SC1882022_124159.pdf&ID=1921559389_1'
        }, "SC"), {
            'json': os.path.join("tests", "data", "json", "SC Judgments", "judgments SC sample page 1.json"),
            'index': 0
        }
    ),
    (
        ({
            'case_number': 'SC 316/2021',
            # pylint: disable-next=line-too-long
            'document_href': 'http://dhcappl.nic.in/FreeText/download.do?FILENAME=dhc/PMS/judgement/04-10-2022//PMS27092022SC3162021_150037.pdf&ID=1921559389_5'
        }, "SC"), {
            'json': os.path.join("tests", "data", "json", "SC Judgments", "judgments SC sample page 1.json"),
            'index': 1
        }
    ),
    (
        ({
            'case_number': 'SC 420/1969',
            # pylint: disable-next=line-too-long
            'document_href': 'http://dhcappl.nic.in/FreeText/download.do?FILENAME=dhc/PMS/judgement/04-10-2022//PMS27092022SC3162021_156937.pdf&ID=1921559389_5'
        }, "SC"), None
    )
]

@pytest.mark.parametrize("judgment_key,meta", judgment_index_test_data)
# pylint: disable-next=redefined-outer-name,missing-function-docstring
def test_judgment_index(judgment_index, judgment_key, meta):
    assert len(judgment_index.data['SC']['data']) == 2

    judgment, group = judgment_key
    for search_key in judgment:
        _judgment = { search_key: judgment[search_key] }
        _result, _meta = judgment_index.get(_judgment, group)
        assert _result is None or _result[search_key] == judgment[search_key]
        assert _meta == meta

file_index_test_data = [
    (
        (
            os.path.join("tests", "data", "judgments", "SC Judgments", "1921559389_1.pdf"),
            "SC Judgments"
        ),
        {
            'json': os.path.join("tests", "data", "json", "SC Judgments", "judgments SC sample page 1.json"),
            'index': 0
        }
    ),
    (
        (
            os.path.join(
                "tests", "data", "judgments", "SC Judgments",
                "extracted_generic", "1921559389_1.txt"
            ),
            "extracted_generic"
        ),
        {
            'json': os.path.join("tests", "data", "json", "SC Judgments", "judgments SC sample page 1.json"),
            'index': 0
        }
    ),
    (
        (
            os.path.join("tests", "data", "judgments", "SC Judgments", "1921559389_5.pdf"),
            "SC Judgments"
        ),
        {
            'json': os.path.join("tests", "data", "json", "SC Judgments", "judgments SC sample page 1.json"),
            'index': 1
        }
    ),
    (
        (
            os.path.join(
                "tests", "data", "judgments", "SC Judgments",
                "extracted_generic", "1921559389_5.txt"
            ),
            "extracted_generic"
        ),
        {
            'json': os.path.join("tests", "data", "json", "SC Judgments", "judgments SC sample page 1.json"),
            'index': 1
        }
    ),
    (
        (
            os.path.join("tests", "data", "judgments", "SC Judgments", "1921559389_6.pdf"),
            "SC Judgments"
        ),
        None
    ),
    (
        (
            os.path.join(
                "tests", "data", "judgments", "SC Judgments",
                "extracted_generic", "1921559389_6.txt"
            ),
            "extracted_generic"
        ),
        None
    ),
]

@pytest.mark.parametrize("file_key,meta", file_index_test_data)
# pylint: disable-next=redefined-outer-name,missing-function-docstring
def test_file_index(file_index, file_key, meta):
    assert len(file_index.data['SC Judgments']['data']) == 2
    assert len(file_index.data['extracted_generic']['data']) == 2

    filepath, group = file_key
    _meta = file_index.get(filepath, group)
    assert _meta == meta
