import re
import os
import timeit
import asyncio
import cProfile
import tempfile
import itertools
from urllib.parse import urlparse

import aiohttp
import requests

import sys
sys.path.append("..")

from src.retrievers import DHCJudgmentRetriever

UNITS = [ '', 'm', 'u', 'n', 'p' ]
def time_unit(time_in_secs):
    index = 0
    while time_in_secs < 1:
        time_in_secs *= 1e3
        index += 1
    return f"{time_in_secs:.3f} {UNITS[index]}s"

SIZE_UNITS = [ '', 'Ki', 'Mi', 'Gi', 'Ti' ]
def size_unit(size_in_bytes):
    index = 0
    while size_in_bytes > 1024:
        size_in_bytes /= 1024
        index += 1
    if size_in_bytes > 512:
        size_in_bytes /= 1024
        index += 1
    return f"{size_in_bytes:.3f} {SIZE_UNITS[index]}B"

def benchmark(function, *args, number=timeit.default_number, profile=False, **kwargs):
    total_exec_time = timeit.timeit("function(*args, **kwargs)", globals=locals(), number=number)
    print(function.__name__, ':', number, "executions in", time_unit(total_exec_time))
    print(function.__name__, ':', time_unit(total_exec_time / number), "per operation")
    if profile:
        cProfile.runctx("function(*args, **kwargs)", globals=globals(), locals=locals())

async def generate_download_urls(judgments):
    async with aiohttp.ClientSession(connector=aiohttp.TCPConnector(ssl=False)) as session:
        return await asyncio.gather(*(
            DHCJudgmentRetriever.preprocess_document_url(judgment['document_href'], session)
            for judgment in judgments
        ))
def get_judgment_urls(query, count):
    documents = []; page = 1
    while True:
        judgments, metadata = DHCJudgmentRetriever.get_judgments(query, page=page)
        if len(judgments) == 0: break
        for judgment in judgments:
            documents.append(judgment)
            if len(documents) == count: break
        if len(documents) == count: break
        if metadata['entry_total'] - len(documents) <= 0: break
        page += 1
    return asyncio.run(generate_download_urls(documents))

def download_file_sync(url):
    response = requests.get(url, stream=True)
    try:
        response.raise_for_status()
    except:
        return None
    if "Content-Disposition" in response.headers:
        file_name = re.findall("filename=(.+)", response.headers["Content-Disposition"])[1]
    else:
        file_name = os.path.basename(urlparse(response.url).path)
    with tempfile.TemporaryFile() as file:
        for chunk in response.iter_content(chunk_size=4096):
            file.write(chunk)
    return file_name
def download_judgments_sync(urls):
    return [ download_file_sync(url) for url in urls ]

async def download_file_async(url, session):
    async with session.get(url) as response:
        try:
            response.raise_for_status()
        except:
            return None
        file_name = None
        if "Content-Disposition" in response.headers:
            file_name = re.findall("filename=(.+)", response.headers["Content-Disposition"])[0]
        else:
            file_name = os.path.basename(response.url.path)
        # with open(os.path.join(".", "dump", file_name), "wb+") as file:
        with tempfile.TemporaryFile() as file:
            async for chunk in response.content.iter_chunked(n=4096):
                file.write(chunk)
        # print("downloaded", response.url, "to", file_name)
        return file_name
async def download_judgments_async_impl(urls):
    async with aiohttp.ClientSession(connector=aiohttp.TCPConnector(ssl=False)) as session:
        return await asyncio.gather(*(
            download_file_async(url, session=session)
            for url in urls
        ))
def download_judgments_async(urls):
    return asyncio.run(download_judgments_async_impl(urls))

async def get_download_size(url, session: aiohttp.ClientSession):
    try:
        async with session.head(url) as response:
            response.raise_for_status()
            return int(response.headers['Content-Length'])
    except:
        return None
async def get_download_size_stats_impl(urls):
    async with aiohttp.ClientSession(connector=aiohttp.TCPConnector(ssl=False)) as session:
        sizes = [ size for size in await asyncio.gather(*(
            get_download_size(url, session=session) for url in urls
        )) if size is not None ]
        print("total download size:", size_unit(sum(sizes)))
        print("average size:       ", size_unit(sum(sizes) / len(sizes)))
def get_download_size_stats(urls):
    asyncio.run(get_download_size_stats_impl(urls))

if __name__ == "__main__":
    queries = [ "trade marks" ]
    counts  = [ 10, 25, 100, 150 ]
    numbers = [ 10, 10, 5, 5 ]

    for query, (count, number) in itertools.product(queries, zip(counts, numbers)):
        urls = get_judgment_urls(query, count)
        print({ 'query': query, 'count': count, 'num_urls': len(urls) })
        get_download_size_stats(urls)
        benchmark(download_judgments_sync, urls, number=number)
        benchmark(download_judgments_async, urls, number=number)
        print()