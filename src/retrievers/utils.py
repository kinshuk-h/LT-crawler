import os
import re

import aiohttp

from . import logger as root_logger

logger = root_logger.getChild(__name__.rsplit('.', maxsplit=1)[-1])

async def download_file(
    url: str, session: aiohttp.ClientSession,
    output_dir: str = ".", skip_existing=False,
    suppress_exc=False, chunk_size=4096, callback=None
) -> (str | None):
    """ Downloads a file referred by a given URL into a specified output directory.

    Args:
        url (str): The URL to download the file from.
        session (aiohttp.ClientSession): Asynchronous session object to use for concurrent requests.
        output_dir (str, optional): Directory to save the downloaded file. Defaults to ".".
        skip_existing (bool, optional): If True, skips downloading of files which already exist. Defaults to False.
        suppress_exc (bool, optional): If True, raised exceptions are suppressed and None is returned instead.
            Defaults to False.
        chunk_size (int, optional): Chunk size to retrieve at a a time, in bytes. Defaults to 4096 (bytes).
        callback ((str) -> None, optional): Optional callback to register file save events.

    Returns:
        str: Path to the downloaded file.

    Raises:
        aiohttp.ClientError: Failure in completion of the request due to a client error.
    """
    try:
        async with session.get(url) as response:
            response.raise_for_status()
            logger.debug("GET %s: HTTP %s", str(response.url), response.status)
            if "Content-Disposition" in response.headers:
                file_name = re.findall("filename=(.+)", response.headers["Content-Disposition"])[0]
            else:
                file_name = os.path.basename(response.url.path)
            file_path = os.path.join(output_dir, file_name)
            if skip_existing and os.path.exists(file_path):
                if os.stat(file_path).st_size == int(response.headers['Content-Length']):
                    return file_path
                else:
                    count = 1
                    base, ext = os.path.splitext(file_path)
                    while True:
                        if not os.path.exists(f"{base}_({count}){ext}"):
                            file_path = f"{base}_({count}){ext}"
                            break
                        count += 1
            with open(file_path, 'wb') as file:
                async for chunk in response.content.iter_chunked(n=chunk_size):
                    file.write(chunk)
            logger.debug("Saved '%s' to '%s'", file_name, output_dir)
            if callback is not None:
                callback(file_name)
            return file_path
    except aiohttp.ClientError as exc:
        if suppress_exc:
            logger.exception("GET %s failed", url)
            return None
        else: raise exc