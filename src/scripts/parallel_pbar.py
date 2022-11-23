import os
import re
import sys
import time
import tempfile
import threading
import multiprocessing
import concurrent.futures
from urllib.parse import urlparse

import requests

from src.utils import ProgressBar, ProgressBarManager

__SIZE_UNITS__    = ("", "Ki", "Mi", "Gi", "Ti", "Pi", "Ei", "Zi", "Yi")
__SIZE_UNITS_SI__ = ("", "k" , "M" , "G" , "T" , "P" , "E" , "Z" , "Y" )

def unitize_size(size_in_bytes, use_si = False, ratio = 0.7):
    unit_index = 0; factor = 1000 if use_si else 1024
    while size_in_bytes > factor * ratio: size_in_bytes /= factor; unit_index += 1
    unit = __SIZE_UNITS_SI__[unit_index] if use_si else __SIZE_UNITS__[unit_index]
    return f"{size_in_bytes:.3f} {unit}B"

def task(args):
    global_dict, url = args
    manager = global_dict['manager']

    response = requests.get(url, stream=True)
    response.raise_for_status()
    if "image" not in response.headers['Content-Type']:
      raise ValueError("get_image(): invalid URL, referring to non-image content")
    if "Content-Disposition" in response.headers.keys():
      fname = re.findall("filename=(.+)", response.headers["Content-Disposition"])[0]
    else:
      fname = os.path.basename(urlparse(response.url).path)
    path = fname
    total_size = int(response.headers['Content-Length'])
    total_size_str = unitize_size(total_size)
    index = manager.add(limit=total_size)
    w = len(total_size_str)
    with open(os.path.join(tempfile.gettempdir(), path), "wb+") as file:
        current_size = 0
        for chunk in response.iter_content(chunk_size=1024):
            file.write(chunk)
            current_size += len(chunk)
            manager.update(index, increment=len(chunk), prefix=f"{fname[:50]:50}", suffix=f"{unitize_size(current_size):>11} / {total_size_str:>11}")
    return path

def main():
    # with multiprocessing.Manager() as process_manager:
    #     global_dict = process_manager.dict()
    #     global_dict['manager'] = ProgressBarManager(10, process_manager)
    #     with concurrent.futures.ProcessPoolExecutor() as executor:
    #         print([
    #             *executor.map(task, [ (global_dict, limit) for limit in [ 100, 200, 50 ] ])
    #         ])

    global_dict = {
        'manager': ProgressBarManager(10)
    }
    # limits = [ 100, 50, 90, 70, 80, 90, 60, 10, 100 ]
    test_images = [
        "https://www.humanesociety.org/sites/default/files/styles/1240x698/public/2020-07/kitten-510651.jpg?h=f54c7448&itok=ZhplzyJ9",
        "https://hips.hearstapps.com/hmg-prod.s3.amazonaws.com/images/close-up-of-cat-wearing-sunglasses-while-sitting-royalty-free-image-1571755145.jpg",
        "https://pbs.twimg.com/media/EAmr-PAWsAEoiWR.jpg",
        "https://i.ytimg.com/vi/QH2-TGUlwu4/sddefault.jpg",
        "https://images.unsplash.com/photo-1587402092301-725e37c70fd8?ixlib=rb-1.2.1&ixid=MnwxMjA3fDB8MHxzZWFyY2h8MXx8cHVwcHklMjBkb2d8ZW58MHx8MHx8&w=1000&q=80",
        "https://techcrunch.com/wp-content/uploads/2020/01/MarsCat-6.jpg"
    ]
    with concurrent.futures.ThreadPoolExecutor() as executor:
        [
            *executor.map(task, [ (global_dict, url) for url in test_images ])
        ]

    # manager.add(limit=100)
    # manager.add(limit=200)
    # manager.add(limit=100)

    # manager.print(prefix=f"{'':9}")

    # iteration_number = 0

    # # time.sleep(5)
    # manager.advance(0, 1)
    # manager.print(prefix=f"tick={iteration_number:4}")

    # time.sleep(10)
    # for _ in range(max(bar.limit for bar in (manager.pbars))):
    #     time.sleep(0.05)
    #     manager.advance(0, 1)
    #     manager.print(prefix=f"tick={iteration_number:4}")
    #     iteration_number += 1
    #     time.sleep(0.05)
    #     manager.advance(1, 1)
    #     manager.print(prefix=f"tick={iteration_number:4}")
    #     iteration_number += 1
    #     time.sleep(0.05)
    #     manager.advance(2, 1)
    #     manager.print(prefix=f"tick={iteration_number:4}")
    #     iteration_number += 1

    print()

    bar = ProgressBar(limit=100, size=10)
    for _ in range(bar.limit + 10):
        bar.advance()
        print('\r', f"{' ':50} {bar}", end='')
    print()

if __name__ == "__main__":
    main()