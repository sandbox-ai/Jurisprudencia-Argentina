import asyncio
import aiohttp
import json, sys,random
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

class AdaptiveRateLimiter:
    def __init__(self, initial_delay: float = 0.1, max_delay: float = 5.0, backoff_factor: float = 1.5):
        self.delay = initial_delay
        self.max_delay = max_delay
        self.backoff_factor = backoff_factor
        self.success_streak = 0
        self.failure_streak = 0

    async def wait(self):
        await asyncio.sleep(self.delay)

    def success(self):
        self.success_streak += 1
        self.failure_streak = 0
        if self.success_streak >= 10:
            self.delay = max(self.delay / self.backoff_factor, 0.1)
            self.success_streak = 0

    def failure(self):
        self.failure_streak += 1
        self.success_streak = 0
        self.delay = min(self.delay * self.backoff_factor, self.max_delay)

class RateLimiter:
    def __init__(self, calls: int = 1, period: float = 1.0, backoff_factor: float = 2.0, jitter: float = 1):
        self.calls = calls
        self.period = period
        self.backoff_factor = backoff_factor
        self.jitter = jitter
        self.timestamps = []
        self.successful_requests = 0
        self.error_count = 0
        self.adaptive_limiter = AdaptiveRateLimiter()

    async def wait(self):
        now = datetime.now()
        backoff = 1  # Initial backoff multiplier
        while True:
            if len(self.timestamps) < self.calls:
                self.timestamps.append(now)
                self.successful_requests += 1
                if self.successful_requests >= 11:
                    self.calls += 1
                    self.successful_requests = 0
                await self.adaptive_limiter.wait()
                self.adaptive_limiter.success()
                break
            elif now - self.timestamps[0] > timedelta(seconds=self.period):
                self.timestamps.pop(0)
                self.successful_requests += 1
            else:
                jitter_delay = self.period * (self.backoff_factor ** backoff) + random.uniform(-self.jitter, self.jitter)
                await asyncio.sleep(jitter_delay)
                backoff *= self.backoff_factor
                self.adaptive_limiter.failure()

    def reset_on_error(self):
        self.calls = max(1, self.calls - 1)
        self.backoff_factor *= 2
        self.successful_requests = 0
        self.error_count += 1
        self.adaptive_limiter.failure()
        if self.error_count >= 5:
            self.calls = max(1, self.calls - 1)
            self.error_count = 0
                    
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
    await rate_limiter.wait()
    url = base_url.replace("o=0", f"o={offset}").format(args.amount, year)
    try:
        async with session.get(url) as response:
            if response.status == 500:
                tqdm.write(f"No more URLs for year {year}", file=sys.stdout)
                return []
            if response.status != 200:
                logging.error(f"Failed to fetch URLs: HTTP {response.status}")
                rate_limiter.reset_on_error()
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
        rate_limiter.reset_on_error()
        return None
    except json.JSONDecodeError as e:
        logging.error(f"JSON decoding error: {e}")
        rate_limiter.reset_on_error()
        return None
    except Exception as e:
        logging.error(f"Unexpected error in get_urls: {e}")
        rate_limiter.reset_on_error()
        return None

@retry(max_retries=10, delay=1)
async def scrape_data(session: aiohttp.ClientSession, url: str, rate_limiter: RateLimiter) -> Optional[dict]:
    await rate_limiter.wait()
    guid = url.split("/")[-1]
    data_url = DATA_URL.format(guid)
    try:
        async with session.get(data_url) as response:
            if response.status == 403:
                tqdm.write(f"403 Forbidden error for {guid}, retrying...", file=sys.stdout)
                rate_limiter.reset_on_error()
                raise aiohttp.ClientError("403 Forbidden")
            if response.status != 200:
                logging.error(f"Failed to fetch data for {guid}: HTTP {response.status}")
                rate_limiter.reset_on_error()
                return None
            data = await response.json()
            content = json.loads(data['data'])['document']['content']
            content['guid'] = guid
            return content
    except aiohttp.ClientError as e:
        logging.error(f"Network error while scraping data for {guid}: {e}")
        rate_limiter.reset_on_error()
        raise e
    except json.JSONDecodeError as e:
        logging.error(f"JSON decoding error for {guid}: {e}")
        rate_limiter.reset_on_error()
        raise e
    except Exception as e:
        logging.error(f"Unexpected error in scrape_data for {guid}: {e}")
        rate_limiter.reset_on_error()
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
        tqdm.write(f"Loaded {len(existing_data)} existing entries from {file_path}", file=sys.stdout)
    except FileNotFoundError:
        tqdm.write(f"No existing data found at {file_path}", file=sys.stdout)
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
                        tqdm.write(f"No more URLs found for year {current_year}", file=sys.stdout)
                        break
                    new_urls = set(urls) - existing_urls
                    if not new_urls:
                        tqdm.write(f"No new URLs found for year {current_year}", file=sys.stdout)
                        current_year -= 1
                        offset = 0
                        continue
                    all_urls.update(new_urls)
                    pbar.update(len(new_urls))
                    tqdm.write(f"Collected {len(new_urls)} new URLs from year {current_year}, offset {offset}", file=sys.stdout)
                    offset += args.amount
            else:
                # Scrape URLs from oldest to newest
                for year in range(1799, datetime.now().year + 1, 1):
                    tqdm.write(f"Attempting to collect URLs for year {year}", file=sys.stdout)
                    offset = 0
                    while True:
                        urls = await get_urls(session, BASE_URL, offset, year, rate_limiter)
                        if not urls:
                            tqdm.write(f"No more URLs in year {year}", file=sys.stdout)
                            break
                        new_urls = set(urls) - existing_urls
                        all_urls.update(new_urls)
                        offset += args.amount
                        pbar.update(len(new_urls))

            pbar.close()

            with urls_file.open('a') as f:
                for url in all_urls:
                    f.write(f"{url}\n")
            tqdm.write(f"Saved {len(all_urls)} new URLs to {urls_file}", file=sys.stdout)

        urls_to_scrape = [url.strip() for url in urls_file.open('r')]
        pbar = tqdm(total=len(urls_to_scrape), desc="Scraping data", position=1, leave=True)
        for url in urls_to_scrape:
            guid = url.split("/")[-1]
            if guid in existing_guids:
                tqdm.write(f"Skipping {guid} - already in dataset", file=sys.stdout)
                pbar.update(1)
                continue
            content = await scrape_data(session, url, rate_limiter)
            if content and validate_data(content):
                with dataset_file.open('a') as f:
                    json.dump(content, f)
                    f.write('\n')
                tqdm.write(f"Added {guid} to dataset", file=sys.stdout)
            else:
                tqdm.write(f"Invalid or missing data for {guid}", file=sys.stdout)
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
    parser.add_argument('--initial-delay', type=float, default=0.1, help='Initial delay for adaptive rate limiting')
    parser.add_argument('--max-delay', type=float, default=5.0, help='Maximum delay for adaptive rate limiting')
    args = parser.parse_args()

    logging.basicConfig(level=getattr(logging, args.log_level), 
                        format='%(asctime)s - %(levelname)s - %(message)s', handlers=[logging.StreamHandler(sys.stdout)])

    asyncio.run(main(args))