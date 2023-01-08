"""
Test suite for extraction stage of the pipeline.
"""

import os
import argparse

import pytest

from src.pipeline import extract

init_extractors_test_data = [
    (
        [ 'pdfminer_text', 'parsr', 'parsr_custom', 'adobe_api' ],
        {
            'parsr_custom_config_path'  : 'test.json',
            'adobe_api_credentials_file': os.path.join('config', 'pdfservices-api-credentials.json'),
        },
        { 'pdfminer_text', 'parsr', 'parsr_custom', 'adobe_api' }
    ),
    (
        [ 'pdfminer_text', 'parsr' ],
        {
            'parsr_custom_config_path'  : 'test.json',
            'adobe_api_credentials_file': os.path.join('config', 'pdfservices-api-credentials.json'),
        },
        { 'pdfminer_text', 'parsr' }
    ),
    ( [ 'pdfminer_text', 'parsr' ], {}, { 'pdfminer_text', 'parsr' } ),
    (
        [ 'parsr_custom', 'adobe_api' ],
        { 'adobe_api_credentials_file': os.path.join('config', 'pdfservices-api-credentials.json') },
        { 'adobe_api' }
    ),
    ( [ 'parsr_custom' ], dict(), set() ),
]

@pytest.mark.parametrize("extractors,options,initialized_extractors", init_extractors_test_data)
# pylint: disable-next=redefined-outer-name,missing-function-docstring
def test_init_extractors(extractors, options, initialized_extractors):
    args = argparse.Namespace(extractors=extractors, **options)
    _initialized_extractors = extract.initialize_extractors(args)
    assert set(_initialized_extractors.keys()) == initialized_extractors
    for extractor in _initialized_extractors.values():
        assert extractor is not None
