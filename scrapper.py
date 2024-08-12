import asyncio
import aiohttp
import json
import logging
from typing import List, Set, Optional, Dict, Any
import argparse
from pathlib import Path
from datetime import datetime, timedelta
from functools import wraps
import time
from tqdm import tqdm

# Constants
BASE_URL = "http://www.saij.gob.ar/busqueda?o=0&p={}&f=Total|Fecha/{}[20,1]|Estado de Vigencia[5,1]|Tema[5,1]|Organismo[5,1]|Autor[5,1]|Jurisdicción|Tribunal[5,1]|Publicación[5,1]|Colección temática[5,1]|Tipo de Documento/Jurisprudencia&s=fecha-rango|DESC&v=colapsada"
DATA_URL = "http://www.saij.gob.ar/view-document?guid={}"

class RateLimiter:
    def __init__(self, calls: int, period: float, backoff_factor: float = 2.0):
        self.calls = calls
        self.period = period
        self.backoff_factor = backoff_factor
        self.timestamps = []
        self.backoff = 0

    async def wait(self):
        now = datetime.now()
        backoff = 1  # Initial backoff multiplier
        while True:
            if len(self.timestamps) < self.calls:
                self.timestamps.append(now)
                break
            elif now - self.timestamps[0] > timedelta(seconds=self.period):
                self.timestamps.pop(0)
                backoff = max(1, backoff // 2)  # Reset backoff if within period
            else:
                await asyncio.sleep(self.period * (self.backoff_factor ** backoff))
                backoff *= self.backoff_factor  # Increase backoff on retries

def retry(max_retries: int = 3, delay: float = 1.0):
    def decorator(func):
        @wraps(func)
        async def wrapper(*args, **kwargs):
            for attempt in range(max_retries):
                try:
                    return await func(*args, **kwargs)
                except (aiohttp.ClientError, json.JSONDecodeError, Exception) as e:
                    if attempt == max_retries - 1:
                        raise
                    logging.warning(f"Attempt {attempt + 1} failed: {e}. Retrying...")
                    await asyncio.sleep(delay * (2 ** attempt))  # Exponential backoff
        return wrapper
    return decorator

@retry(max_retries=3)
async def get_urls(session: aiohttp.ClientSession, base_url: str, offset: int, year: int, rate_limiter: RateLimiter) -> Optional[List[str]]:
    """Fetch URLs from the specified offset and year."""
    await rate_limiter.wait()
    url = base_url.replace("o=0", f"o={offset}").format(args.amount, year)
    try:
        async with session.get(url) as response:
            if response.status == 500:
                logging.info(f"No more URLs for year {year}")
                return []
            if response.status != 200:
                logging.error(f"Failed to fetch URLs: HTTP {response.status}")
                return None
            data = await response.json()
            
            if not isinstance(data, dict):
                logging.error(f"Unexpected response type: {type(data)}")
                return None
            
            search_results = data.get("searchResults", {})
            document_list = search_results.get("documentResultList", [])
            
            urls = []
            for item in document_list:
                try:
                    result = json.loads(item["documentAbstract"])
                    friendly_url = result["document"]["metadata"]["friendly-url"]["description"]
                    uuid = result["document"]["metadata"]["uuid"]
                    urls.append(f"{friendly_url}/{uuid}")
                except (json.JSONDecodeError, KeyError) as e:
                    logging.warning(f"Error parsing item: {e}")
                    continue
            
            return urls
    except aiohttp.ClientError as e:
        logging.error(f"Network error while fetching URLs: {e}")
        return None
    except json.JSONDecodeError as e:
        logging.error(f"JSON decoding error: {e}")
        return None
    except Exception as e:
        logging.error(f"Unexpected error in get_urls: {e}")
        return None

@retry(max_retries=3, delay=1.0)
async def scrape_data(session: aiohttp.ClientSession, url: str, rate_limiter: RateLimiter) -> Optional[dict]:
    """Scrape data for a given URL."""
    await rate_limiter.wait()
    guid = url.split("/")[-1]
    data_url = DATA_URL.format(guid)
    try:
        async with session.get(data_url) as response:
            if response.status == 403:
                logging.warning(f"403 Forbidden error for {guid}, retrying...")
                raise aiohttp.ClientError("403 Forbidden")
            if response.status != 200:
                logging.error(f"Failed to fetch data for {guid}: HTTP {response.status}")
                return None
            data = await response.json()
            content = json.loads(data['data'])['document']['content']
            content['guid'] = guid
            return content
    except aiohttp.ClientError as e:
        logging.error(f"Network error while scraping data for {guid}: {e}")
        raise e
    except json.JSONDecodeError as e:
        logging.error(f"JSON decoding error for {guid}: {e}")
        raise e
    except Exception as e:
        logging.error(f"Unexpected error in scrape_data for {guid}: {e}")
        raise e

def load_existing_data(file_path: Path, key: str) -> Set[str]:
    """Load existing data from a file."""
    existing_data = set()
    try:
        with file_path.open('r') as f:
            if key == 'url':
                # For URL file, each line is a URL
                existing_data = set(line.strip() for line in f if line.strip())
            else:
                # For dataset file, each line is a JSON object
                for line in f:
                    try:
                        data = json.loads(line)
                        if key in data:
                            existing_data.add(data[key])
                    except json.JSONDecodeError:
                        logging.warning(f"Skipping invalid JSON in dataset: {line.strip()}")
        logging.info(f"Loaded {len(existing_data)} existing entries from {file_path}")
    except FileNotFoundError:
        logging.info(f"No existing data found at {file_path}")
    return existing_data

def validate_data(content: Dict[str, Any]) -> bool:
    """Validate scraped data."""
    required_fields = ['guid']  # Add more fields as needed
    return all(field in content for field in required_fields)

async def main(args):
    urls_file = Path(args.urls_output)
    dataset_file = Path(args.dataset_output)
    
    existing_urls = load_existing_data(urls_file, 'url')
    existing_guids = load_existing_data(dataset_file, 'guid')

    rate_limiter = RateLimiter(calls=1000000, period=100)

    async with aiohttp.ClientSession() as session:
        if not args.data:
            all_urls = set()
            pbar = tqdm(desc="Collecting URLs")
            # Scrape the latest URLs and data
            year = datetime.now().year
            if args.update:
                # Scrape URLs from current year backwards until we find an existing one
                current_year = datetime.now().year
                offset = 0
                while True:
                    urls = await get_urls(session, BASE_URL, offset, current_year, rate_limiter)
                    if not urls:
                        logging.info(f"No more URLs found for year {current_year}")
                        break
                    new_urls = set(urls) - existing_urls
                    if not new_urls:
                        logging.info(f"No new URLs found for year {current_year}")
                        current_year -= 1
                        offset = 0
                        continue
                    all_urls.update(new_urls)
                    pbar.update(len(new_urls))
                    logging.info(f"Collected {len(new_urls)} new URLs from year {current_year}, offset {offset}")
                    offset += args.amount
            else:
                # Scrape URLs from oldest to newest
                for year in range(1799, datetime.now().year + 1, 1):
                    logging.info(f"Attempting to collect URLs for year {year}")
                    offset = 0
                    while True:
                        urls = await get_urls(session, BASE_URL, offset, year, rate_limiter)
                        if not urls:
                            logging.info(f"No more URLs in year {year}")
                            break
                        new_urls = set(urls) - existing_urls
                        all_urls.update(new_urls)
                        offset += args.amount
                        pbar.update(len(new_urls))

            pbar.close()

            with urls_file.open('a') as f:
                for url in all_urls:
                    f.write(f"{url}\n")
            logging.info(f"Saved {len(all_urls)} new URLs to {urls_file}")

        urls_to_scrape = [url.strip() for url in urls_file.open('r')]
        pbar = tqdm(total=len(urls_to_scrape), desc="Scraping data")
        for url in urls_to_scrape:
            guid = url.split("/")[-1]
            if guid in existing_guids:
                logging.info(f"Skipping {guid} - already in dataset")
                pbar.update(1)
                continue
            content = await scrape_data(session, url, rate_limiter)
            if content and validate_data(content):
                with dataset_file.open('a') as f:
                    json.dump(content, f)
                    f.write('\n')
                logging.info(f"Added {guid} to dataset")
            else:
                logging.warning(f"Invalid or missing data for {guid}")
            pbar.update(1)
        pbar.close()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Scraping dataset from URLs')
    parser.add_argument('--urls-output', help='Output file for URLs', default='urls.txt')
    parser.add_argument('--dataset-output', help='Output file for dataset', default='dataset.jsonl')
    parser.add_argument('--update', help='Only scrape the latest content', action='store_true')
    parser.add_argument('--data', help='Only scrape content data', action='store_true')
    parser.add_argument('--amount', type=int, help='Maximum amount of URLs to scrape at a time', default=4000)
    parser.add_argument('--log-level', help='Logging level', default='INFO', choices=['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL'])
    args = parser.parse_args()

    logging.basicConfig(level=getattr(logging, args.log_level), 
                        format='%(asctime)s - %(levelname)s - %(message)s')

    asyncio.run(main(args))