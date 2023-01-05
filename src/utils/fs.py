"""
    Provides filesystem and IO-specific functions.
"""

import re
import json

def read_json(file_path):
    """ Loads data from a JSON file. """
    with open(file_path, 'r+', encoding='utf-8') as file:
        return json.load(file)

def write_json(file_path, data):
    """ Dumps data to a JSON file. """
    with open(file_path, 'w+', encoding='utf-8') as file:
        json.dump(data, file, indent=4, ensure_ascii=False)

def pathsafe(filename):
    """ Returns a santized, path-safe version of a filename. """
    return re.sub(r'[:/\\|*]', '-', re.sub(r'[?\"<>]', '', filename))
