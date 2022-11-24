import os
import re
import time
import asyncio
from urllib.parse import urlparse

import aiohttp
import requests

from src import utils
from src.retrievers import DHCJudgmentRetriever
from src.extractors import AdobeAPIExtractor

async def generate_download_urls(urls):
    async with aiohttp.ClientSession(connector=aiohttp.TCPConnector(ssl=False)) as session:
        return await asyncio.gather(*(
            DHCJudgmentRetriever.preprocess_document_url(url, session)
            for url in urls
        ))

def download_file_sync(url):
    response = requests.get(url, stream=True)
    try:
        response.raise_for_status()
    except requests.RequestException:
        return None
    if "Content-Disposition" in response.headers:
        file_name = re.findall("filename=(.+)", response.headers["Content-Disposition"])[1]
    else:
        file_name = os.path.basename(urlparse(response.url).path)
    with open(file_name, "wb") as file:
        for chunk in response.iter_content(chunk_size=4096):
            file.write(chunk)
    return file_name

def main():
    file_index = utils.FileIndexStore()

    # urls = [
    #     "http://dhcappl.nic.in:8080/FreeText/download.do?FILENAME=dhc/VIB/judgement/01-06-2022//VIB01062022CW82842022_202139.pdf&ID=1072369180_4",
    #     "http://dhcappl.nic.in:8080/FreeText/download.do?FILENAME=dhc/VIB/judgement/01-06-2022//VIB01062022CW83452022_202255.pdf&ID=1072369180_5",
    #     "http://dhcappl.nic.in:8080/FreeText/download.do?FILENAME=dhc/VIB/judgement/01-06-2022//VIB01062022CW85702022_203847.pdf&ID=1072369180_6",
    #     "http://dhcappl.nic.in:8080/FreeText/download.do?FILENAME=dhc/VIB/judgement/01-06-2022//VIB01062022CW85512022_202913.pdf&ID=1072369180_7"
    # ]
    urls = [
        "http://dhcappl.nic.in:8080/FreeText/download.do?FILENAME=dhc/NAC/judgement/12-10-2022//NAC12102022WO762021_183759.pdf&ID=1403414511_0",
        "http://dhcappl.nic.in:8080/FreeText/download.do?FILENAME=dhc/NAC/judgement/12-10-2022//NAC12102022SC6752019_183633.pdf&ID=1403414511_1",
        "http://dhcappl.nic.in:8080/FreeText/download.do?FILENAME=dhc/NAC/judgement/12-10-2022//NAC12102022WO552021_183727.pdf&ID=1403414511_2",
        "http://dhcappl.nic.in:8080/FreeText/download.do?FILENAME=dhc/PMS/judgement/12-10-2022//PMS11102022SC4782021_171710.pdf&ID=1403414511_3",
        "http://dhcappl.nic.in:8080/FreeText/download.do?FILENAME=dhc/PMS/judgement/12-10-2022//PMS10102022SC4232022_124311.pdf&ID=1403414511_4",
        "http://dhcappl.nic.in:8080/FreeText/download.do?FILENAME=dhc/PMS/judgement/12-10-2022//PMS10102022SC4742022_124501.pdf&ID=1403414511_5",
        "http://dhcappl.nic.in:8080/FreeText/download.do?FILENAME=dhc/PMS/judgement/12-10-2022//PMS07102022SC1792018_124618.pdf&ID=1403414511_6",
        "http://dhcappl.nic.in:8080/FreeText/download.do?FILENAME=dhc/595/judgement/08-10-2022//59507102022FAC942020_191733.pdf&ID=1403414511_7",
        "http://dhcappl.nic.in:8080/FreeText/download.do?FILENAME=dhc/595/judgement/06-10-2022//59506102022RFC62019_172809.pdf&ID=1403414511_8",
        "http://dhcappl.nic.in:8080/FreeText/download.do?FILENAME=dhc/SVN/judgement/06-10-2022//SVN06102022SC3082021_182926.pdf&ID=1403414511_9"
    ]

    resolved = asyncio.run(generate_download_urls(urls))
    downloaded = []

    for url in resolved:
        print("[>] downloading from", url, "... ", end='', flush=True)
        file = download_file_sync(url)
        downloaded.append(file)
        print("done")

        if file_index.has(file, "pdf"):
            print("[!]", file, "was already downloaded under a different name")
        else:
            print("[>]", file, "is unique, adding to index ... ", end='', flush=True)
            file_index.load(file, "pdf")
            print("done")

    print()
    print("> Unique files (PDF):", len(file_index.data['pdf']['hash']), '/', len(resolved))
    print()

    extractor = AdobeAPIExtractor(credentials_file=os.path.join("config", "pdfservices-api-credentials.json"))
    for i, file in enumerate(downloaded):
        if not file.endswith("pdf"):
            continue
        if i>0:
            time.sleep(2)
        print("[>] extracting text from", file, "... ", end='', flush=True)
        results = extractor.extract_to_file(file)
        print("done")
        for result in results:
            if result.endswith('processed.txt'):
                if file_index.has(result, "txt"):
                    print("[!]", result, "was already present under a different name")
                else:
                    print("[>]", result, "is unique, adding to index ... ", end='', flush=True)
                    file_index.load(result, "txt")
                    print("done")
            downloaded.append(result)

    print()
    print("> Unique files (Extracted Content):", len(file_index.data['txt']['hash']), '/', len(resolved))

    # Cleanup
    for file in downloaded:
        os.remove(file)

if __name__ == "__main__":
    main()
