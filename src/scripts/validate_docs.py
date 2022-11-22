import os
import glob
import json
import argparse
import threading
import collections

from src import retrievers, utils

avl_retrievers = {
    'SC' : retrievers.SCJudgmentRetriever,
    'DHC': retrievers.DHCJudgmentRetriever
}

def constrain(string, width=30):
    """ Constrain the length of a given string to the specified width. """
    if len(string) > width:
        half_len = len(string) >> 1
        oth_half_len = len(string) - half_len
        string = string[:half_len-1] + "..." + string[oth_half_len-2:]
    return f"{string:{width}}"

def show_progress(limit):
    bar = utils.ProgressBar(limit=limit, size=20)
    lock = threading.Lock()
    def show_progress_impl(file_name):
        with lock:
            bar.advance()
            print(f"\r    {constrain(file_name, width=20)}{bar} ", end='')
    return show_progress_impl

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="validate downloaded judgment files, and reports missing or unused files"
    )
    parser.add_argument("--fix-missing", action="store_true", help="downloads missing files, if any.")
    parser.add_argument("--remove-unused", action="store_true", help="removes unused files, if any.")

    args = parser.parse_args()

    while not os.path.exists("data"):
        os.chdir("..")

    non_existing = set()
    valid        = set()
    non_referent = set()
    non_existing_ref = collections.defaultdict(list)

    for court_dir in os.listdir(os.path.join("data", "json")):
        court = court_dir.replace('Judgments', '').rstrip()
        court_dir_path = os.path.join("data", "json", court_dir)
        if not os.path.isdir(court_dir_path): continue
        for json_file in glob.glob("*.json", root_dir=court_dir_path):
            with open(os.path.join(court_dir_path, json_file), "r", encoding="utf-8") as file:
                print("> examining", json_file, "... ", end='')
                data = json.load(file)
                for judgment in data['data']:
                    doc_path = judgment['document_path']
                    if doc_path is not None:
                        if os.path.exists(doc_path):
                            valid.add(doc_path)
                        else:
                            non_existing.add(doc_path)
                            non_existing_ref[court].append({
                                'document_href': judgment['document_href'],
                                'path': doc_path
                            })
                print("done")

    for court_dir in os.listdir(os.path.join("data", "judgments")):
        court_dir_path = os.path.join("data", "judgments", court_dir)
        if not os.path.isdir(court_dir_path): continue
        print("> examining", court_dir, "... ", end='')
        for file in glob.glob("*.pdf", root_dir=court_dir_path):
            file_path = os.path.join(court_dir_path, file)
            if file_path not in valid:
                non_referent.add(file_path)
        print("done")

    root_path = os.path.join("data", "judgments")

    print()
    print("> Valid files:", len(valid), '/', len(valid) + len(non_existing))
    print()

    print("> Missing files:", len(non_existing))
    for path in non_existing:
        print("  ", os.path.relpath(path, root_path), end='', flush=True)
    print()

    if args.fix_missing:
        for court, docs in non_existing_ref.items():
            print("> Downloading missing judgments from", court, ":")
            avl_retrievers[court].save_documents(
                docs, os.path.join("data", "judgments", court), show_progress(len(docs))
            )
            for doc in docs:
                if doc['document_path'] is not None:
                    os.rename(doc['document_path'], doc['path'])


    print("> Non-referent files:", len(non_referent))
    for path in non_referent:
        print("  ", os.path.relpath(path, root_path), end='', flush=True)
        if args.remove_unused:
            os.remove(path)
            print(" (removed)", end='')
        print()
