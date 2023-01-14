"""
Test suite for utility functions.
"""

import collections

import pytest

from src import utils

constrain_test_data = [
    ( "hello", 10, "hello     " ),
    ( "hello world this is a somewhat long string", 10, "hell...ing" )
]

@pytest.mark.parametrize("string, length, final_string", constrain_test_data)
def test_constrain(string, length, final_string):
    new_string = utils.constrain(string, length)
    assert len(new_string) == length
    assert new_string == final_string


# pylint: disable-next=redefined-outer-name,missing-function-docstring
def test_merge_dicts():
    dict_a = collections.defaultdict(list)
    dict_b = collections.defaultdict(list)

    dict_a.update({ 'a': [1,2], 'b': [3] })
    dict_b.update({ 'a': [3], 'c': [1,2,3] })

    dict_c = utils.merge_dicts(dict_a, dict_b)
    assert isinstance(dict_c, collections.defaultdict)
    assert dict_c == { 'a': [ 1,2,3 ], 'b': [ 3 ], 'c': [ 1,2,3 ] }

# pylint: disable-next=redefined-outer-name,missing-function-docstring
def preprocess_1():
    return []
# pylint: disable-next=redefined-outer-name,missing-function-docstring
def preprocess_2():
    return {}
# pylint: disable-next=redefined-outer-name,missing-function-docstring
def phase_1(_list, _dict, **_):
    _list.append(1)
    _dict['p1'] = 1
    return [ len(_list) ]
# pylint: disable-next=redefined-outer-name,missing-function-docstring
def phase_2(_lens, _list, _dict, **_):
    _list.append(2)
    _dict['p2'] = 2
    return [ *_lens, len(_list) ]
# pylint: disable-next=redefined-outer-name,missing-function-docstring
def phase_3(_lens, _list, _dict, **_):
    _list.append(3)
    _dict['p3'] = 3
    return [ *_lens, len(_list) ]
# pylint: disable-next=redefined-outer-name,missing-function-docstring
def postprocess_1(_lens, _list, _dict, **__):
    return len(_lens), len(_list)
# pylint: disable-next=redefined-outer-name,missing-function-docstring
def postprocess_2(_lens, _list, _dict, **__):
    return len(_lens), list(_dict.keys())

# pylint: disable-next=redefined-outer-name,missing-function-docstring
def test_pipeline():
    pipeline = utils.Pipeline(
        preprocessing={ '_list': preprocess_1, '_dict': preprocess_2 },
        phases=[ phase_1, phase_2, phase_3 ],
        postprocessing={ '_post1': postprocess_1, '_post2': postprocess_2  }
    )

    # pylint: disable-next=unbalanced-tuple-unpacking
    pre_res, res, post_res = pipeline.execute(with_preprocessing_results=True)

    assert pre_res  == {
        '_list': [ 1, 2, 3 ],
        '_dict': { 'p1': 1, 'p2': 2, 'p3': 3 }
    }
    assert res      == [ 1, 2, 3 ]
    assert post_res == {
        '_post1': (3, 3),
        '_post2': (3, [ 'p1', 'p2', 'p3' ])
    }
