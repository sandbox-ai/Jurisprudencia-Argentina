import asyncio
import json, sys, random
import logging
import subprocess
import signal
import os
from typing import List, Set, Optional, Dict, Any
import argparse
from pathlib import Path
from datetime import datetime, timedelta
from functools import wraps
from tqdm import tqdm

# Constants
# resultados.jsp is an HTML shell; the actual JSON payload is returned by /busqueda.
# We keep pagination via `o` (offset) and `p` (page size) and filter to Jurisprudencia.
BUSQUEDA_URL = "https://www.saij.gob.ar/busqueda"
DEFAULT_FACETS = (
    "Total|Tipo de Documento/Jurisprudencia|Fecha|Organismo|Publicación|"
    "Tema|Estado de Vigencia|Autor|Jurisdicción"
)
DEFAULT_VIEW = "colapsada"

DATA_URL = "https://www.saij.gob.ar/view-document?guid={}"

# Obscura CDP config
OBSCURA_PORT = 9222


class ObscuraClient:
    """Async client for obscura headless browser via CDP websocket."""

    def __init__(self, port: int = OBSCURA_PORT):
        self.port = port
        self.binary = os.environ.get("OBSCURA_BINARY", "./obscura")
        self.ws = None
        self.session_id = None
        self.process = None
        self._req_id = 0

    async def _next_id(self):
        self._req_id += 1
        return self._req_id

    async def _send_recv(self, method: str, params: dict = None, timeout: float = 30) -> dict:
        """Send a CDP command and wait for the response."""
        req_id = await self._next_id()
        msg = {'id': req_id, 'method': method, 'params': params or {}}
        if self.session_id:
            msg['sessionId'] = self.session_id
        await self.ws.send(json.dumps(msg))

        while True:
            raw = await asyncio.wait_for(self.ws.recv(), timeout=timeout)
            resp = json.loads(raw)
            if 'id' in resp and resp['id'] == req_id:
                return resp

    async def start(self):
        """Start obscura serve process and connect via websocket."""
        import websockets

        # Kill any existing obscura on this port
        try:
            result = subprocess.run(
                ['lsof', '-ti', f':{self.port}'],
                capture_output=True, text=True, timeout=5
            )
            for pid in result.stdout.strip().split('\n'):
                if pid.isdigit():
                    try:
                        os.kill(int(pid), signal.SIGTERM)
                    except ProcessLookupError:
                        pass
            await asyncio.sleep(2)
        except Exception:
            pass

        # Start obscura serve
        tqdm.write(f"Starting obscura on port {self.port}...", file=sys.stdout)
        self.process = subprocess.Popen(
            [self.binary, 'serve', '--port', str(self.port)],
            stdout=subprocess.PIPE, stderr=subprocess.PIPE
        )

        # Wait for obscura to be ready
        for attempt in range(30):
            await asyncio.sleep(1)
            try:
                self.ws = await asyncio.wait_for(
                    websockets.connect(
                        f'ws://127.0.0.1:{self.port}/devtools/browser',
                        max_size=10 * 1024 * 1024  # 10MB for large JSON responses
                    ),
                    timeout=5
                )
                tqdm.write("Obscura connected!", file=sys.stdout)
                break
            except Exception:
                if attempt == 29:
                    tqdm.write("Failed to connect to obscura!", file=sys.stdout)
                    raise
        else:
            raise RuntimeError("Failed to connect to obscura")

        # Create a blank target (custom headers interfere with cookie-setting)
        await self.ws.send(json.dumps({
            'id': 0, 'method': 'Target.createTarget',
            'params': {'url': 'about:blank'}
        }))
        for _ in range(5):
            raw = await asyncio.wait_for(self.ws.recv(), timeout=5)
            msg = json.loads(raw)
            if msg.get('method') == 'Target.attachedToTarget':
                self.session_id = msg['params']['sessionId']
                break

        await self._send_recv('Network.enable')
        await self._send_recv('Page.enable')

        # Visit main page first to establish cookies/session (JSESSIONID is required)
        tqdm.write("Initializing session with saij.gob.ar...", file=sys.stdout)
        # Send navigate and wait for load event properly
        req_id = await self._next_id()
        await self.ws.send(json.dumps({
            'id': req_id, 'method': 'Page.navigate',
            'params': {'url': 'https://www.saij.gob.ar/'},
            'sessionId': self.session_id
        }))
        end = asyncio.get_event_loop().time() + 15
        while asyncio.get_event_loop().time() < end:
            try:
                raw = await asyncio.wait_for(self.ws.recv(), timeout=5)
                msg = json.loads(raw)
                if msg.get('method') == 'Page.loadEventFired':
                    break
            except asyncio.TimeoutError:
                pass

    async def navigate_and_get_body(self, url: str, timeout: float = 30) -> str:
        """Navigate to a URL and return the page body text."""
        await self._send_recv('Page.navigate', {'url': url}, timeout=timeout)
        resp = await self._send_recv('Runtime.evaluate', {
            'expression': 'document.body.innerText'
        }, timeout=timeout)
        return resp.get('result', {}).get('result', {}).get('value', '')

    async def navigate_fetch(self, url: str, timeout: float = 30) -> str:
        """Navigate to a URL and return the response body as text.
        Works for both HTML pages and JSON endpoints (SAIJ returns JSON in <body>).
        Uses Page.navigate + document.body.innerText which works reliably with obscura."""
        # Send the navigate command and wait for BOTH the cmd response AND load event
        req_id = await self._next_id()
        msg = {'id': req_id, 'method': 'Page.navigate', 'params': {'url': url}, 'sessionId': self.session_id}
        await self.ws.send(json.dumps(msg))

        loaded = False
        replied = False
        end = asyncio.get_event_loop().time() + timeout
        while asyncio.get_event_loop().time() < end:
            remaining = max(0.5, end - asyncio.get_event_loop().time())
            try:
                raw = await asyncio.wait_for(self.ws.recv(), timeout=remaining)
                resp = json.loads(raw)
                if 'id' in resp and resp['id'] == req_id:
                    replied = True
                    if resp.get('result', {}).get('errorText'):
                        raise RuntimeError(f"Navigation failed: {resp['result']['errorText']}")
                if resp.get('method') == 'Page.loadEventFired':
                    loaded = True
                if replied and loaded:
                    break
            except asyncio.TimeoutError:
                pass

        if not replied:
            raise RuntimeError(f"Navigation timed out for {url}")

        await asyncio.sleep(1)  # brief settle for any post-load JS
        resp = await self._send_recv('Runtime.evaluate', {
            'expression': 'document.body.innerText',
            'returnByValue': True,
        }, timeout=timeout)
        result = resp.get('result', {})
        if result.get('subtype') == 'error':
            raise RuntimeError(result.get('description', 'Unknown evaluate error'))
        value = result.get('result', {}).get('value', '')
        if not value:
            raise RuntimeError(f"Empty response body from {url}")
        return value

    async def close(self):
        """Close the websocket and stop the obscura process."""
        try:
            if self.ws:
                await self.ws.close()
        except Exception:
            pass
        if self.process:
            self.process.terminate()
            try:
                self.process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                self.process.kill()


class RateLimiter:
    def __init__(self, calls: int = 1, period: float = 1.0, backoff_factor: float = 1.4, jitter: float = 1):
        self.calls = calls
        self.period = period
        self.backoff_factor = backoff_factor
        self.jitter = jitter
        self.timestamps = []
        self.successful_requests = 0
        self.error_count = 0

    async def wait(self):
        now = datetime.now()
        backoff = 1
        while True:
            if len(self.timestamps) < self.calls:
                self.timestamps.append(now)
                self.successful_requests += 1
                if self.successful_requests >= 11:
                    self.calls += 1
                    self.successful_requests = 0
                break
            elif now - self.timestamps[0] > timedelta(seconds=self.period):
                self.timestamps.pop(0)
                self.successful_requests += 1
            else:
                jitter_delay = self.period * (self.backoff_factor ** backoff) + random.uniform(-self.jitter, self.jitter)
                await asyncio.sleep(max(0, jitter_delay))
                backoff *= self.backoff_factor

    def reset_on_error(self):
        self.calls = max(1, self.calls - 1)
        self.backoff_factor *= 2
        self.successful_requests = 0
        self.error_count += 1
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
                except Exception as e:
                    if attempt == max_retries - 1:
                        raise
                    logging.warning(f"Attempt {attempt + 1} failed: {e}. Retrying...")
                    await asyncio.sleep(delay * (2 ** attempt))
        return wrapper
    return decorator


@retry(max_retries=3)
async def get_urls_for_month(client: ObscuraClient, year: int, month: int,
                            rate_limiter: RateLimiter,
                            page_size: int = 200) -> List[str]:
    """Fetch all jurisprudencia URLs for a specific year/month using month-filtered facets."""
    await rate_limiter.wait()
    ym = f"{year}/{month:02d}"
    facets = f"Total|Tipo de Documento/Jurisprudencia|Fecha/{ym}|Organismo|Publicación|Tema|Estado de Vigencia|Autor|Jurisdicción"
    from urllib.parse import urlencode

    all_urls = []
    offset = 0
    while True:
        params = {"o": offset, "p": page_size, "f": facets, "v": DEFAULT_VIEW}
        url = f"{BUSQUEDA_URL}?{urlencode(params, doseq=True)}"

        try:
            body = await client.navigate_fetch(url, timeout=45)
        except Exception as e:
            if offset == 0:
                raise
            # Subsequent pages failing means we hit the end
            break

        try:
            data = json.loads(body)
        except json.JSONDecodeError:
            logging.error(f"JSON decode error for {ym} offset={offset}")
            break

        if not isinstance(data, dict):
            break

        search_results = data.get("searchResults", {})
        document_list = search_results.get("documentResultList", [])

        if not document_list:
            break

        for item in document_list:
            try:
                result = json.loads(item["documentAbstract"])
                friendly_url = result["document"]["metadata"]["friendly-url"]["description"]
                uuid = result["document"]["metadata"]["uuid"]
                all_urls.append(f"{friendly_url}/{uuid}")
            except (json.JSONDecodeError, KeyError):
                continue

        # If we got fewer than requested, we've hit the last page
        if len(document_list) < page_size:
            break

        offset += page_size

    return all_urls

@retry(max_retries=10, delay=1)
async def scrape_data(client: ObscuraClient, url: str, rate_limiter: RateLimiter) -> Optional[dict]:
    await rate_limiter.wait()
    guid = url.split("/")[-1]
    data_url = DATA_URL.format(guid)

    try:
        body = await client.navigate_fetch(data_url, timeout=30)

        # Parse JSON response
        data = json.loads(body)
        content = json.loads(data['data'])['document']['content']
        content = enforce_schema(content)
        content['guid'] = guid
        return content

    except Exception as e:
        logging.error(f"Error in scrape_data for {guid}: {e}")
        rate_limiter.reset_on_error()
        raise e


correct_schema = {
    "descriptores": {
        "descriptor": [
            {
                "elegido": {
                    "termino": "Término elegido para describir al caso"
                },
                "preferido": {
                    "termino": "Término preferido para describir al caso"
                },
                "sinonimos": {
                    "termino": ["Lista de sinónimos"]
                }
            }
        ],
        "suggest": {
            "termino": ["Lista de términos sugeridos"]
        }
    }
}


def enforce_schema(data):
    if not isinstance(data, dict):
        return correct_schema

    descriptores = data.get('descriptores', {})
    if not isinstance(descriptores, dict):
        tqdm.write(f"Wrong schema for descriptores: {descriptores}", file=sys.stdout)
        return correct_schema

    descriptor = descriptores.get('descriptor', [])
    if not isinstance(descriptor, list) or not all(isinstance(d, dict) for d in descriptor):
        return correct_schema

    for d in descriptor:
        if 'elegido' not in d or not isinstance(d['elegido'], dict):
            tqdm.write(f"Wrong schema for elegido: {d}", file=sys.stdout)
            return correct_schema
        if 'preferido' not in d or not isinstance(d['preferido'], dict):
            tqdm.write(f"Wrong schema for preferido: {d}", file=sys.stdout)
            return correct_schema
        if 'sinonimos' not in d or not isinstance(d['sinonimos'], dict):
            d['sinonimos'] = {'termino': []}
        if not isinstance(d['sinonimos']['termino'], list):
            d['sinonimos']['termino'] = [d['sinonimos']['termino']]

    suggest = descriptores.get('suggest', {})
    if not isinstance(suggest, dict):
        tqdm.write(f"Wrong schema for suggest: {suggest}", file=sys.stdout)
        return correct_schema
    if 'termino' not in suggest or not isinstance(suggest['termino'], list):
        suggest['termino'] = []

    return data


def load_existing_data(file_path: Path, key: str) -> Set[str]:
    """Load existing data from a file."""
    existing_data = set()
    try:
        with file_path.open('r') as f:
            if key == 'url':
                existing_data = {line.strip() for line in f if line.strip()}
            else:
                existing_data = {json.loads(line).get(key) for line in f if line.strip() and key in json.loads(line)}
    except FileNotFoundError:
        tqdm.write(f"No existing data found at {file_path}", file=sys.stdout)
    except json.JSONDecodeError as e:
        logging.warning(f"Skipping invalid JSON in dataset: {e}")
    else:
        tqdm.write(f"Loaded {len(existing_data)} existing entries from {file_path}", file=sys.stdout)
    return existing_data


def load_existing_data_reverse(file_path, key):
    existing_guids = set()
    with open(file_path, 'r') as f:
        for line in reversed(list(f)):
            data = json.loads(line)
            existing_guids.add(data[key])
    return existing_guids


def read_lines_reverse(file_path):
    with open(file_path, 'r') as f:
        return list(reversed([line.strip() for line in f]))


def validate_data(content: Dict[str, Any]) -> bool:
    """Validate scraped data."""
    required_fields = ['guid']
    return all(field in content for field in required_fields)


async def main(args):
    urls_file = Path(args.urls_output)
    dataset_file = Path(args.dataset_output)

    rate_limiter = RateLimiter(calls=1000000, period=100)

    # Start obscura headless browser
    client = ObscuraClient(port=args.obscura_port)
    try:
        await client.start()
    except Exception as e:
        tqdm.write(f"Failed to start obscura: {e}", file=sys.stdout)
        sys.exit(1)

    try:
        if not args.data:
            tqdm.write("Loading existing URL list...")
            existing_urls = load_existing_data(urls_file, 'url')
            all_new_urls = set()

            if args.update:
                # Scrape the last N months for new entries.
                # Month-filtered facets are the only reliable way SAIJ returns
                # results sorted by recency.
                now = datetime.now()
                current_year, current_month = now.year, now.month
                for i in range(args.months):
                    # Walk backwards by months
                    m = current_month - i
                    y = current_year
                    while m <= 0:
                        m += 12
                        y -= 1
                    ym = f"{y}/{m:02d}"
                    tqdm.write(f"Checking {ym}...", file=sys.stdout)
                    urls = await get_urls_for_month(client, y, m, rate_limiter, page_size=args.amount)
                    new_urls = set(urls) - existing_urls
                    all_new_urls.update(new_urls)
                    tqdm.write(f"  {ym}: {len(urls)} total, {len(new_urls)} new", file=sys.stdout)
            else:
                # Full scrape: iterate all months from now back to 1970
                for year in range(datetime.now().year, 1969, -1):
                    for month in range(12, 0, -1):
                        y, m = (year, month)
                        # Skip future months
                        now = datetime.now()
                        if y > now.year or (y == now.year and m > now.month):
                            continue
                        urls = await get_urls_for_month(client, y, m, rate_limiter, page_size=args.amount)
                        if not urls:
                            # If a recent month returns nothing, keep going
                            # (could just be an empty month)
                            tqdm.write(f"{y}/{m:02d}: 0 entries", file=sys.stdout)
                            continue
                        new_urls = set(urls) - existing_urls
                        all_new_urls.update(new_urls)
                        tqdm.write(f"{y}/{m:02d}: {len(urls)} total, {len(new_urls)} new", file=sys.stdout)

            with urls_file.open('a') as f:
                for url in all_new_urls:
                    f.write(f"{url}\n")
            tqdm.write(f"Saved {len(all_new_urls)} new URLs to {urls_file}", file=sys.stdout)

        # Load existing GUIDs
        tqdm.write("Loading existing dataset...")
        if args.update:
            existing_guids = load_existing_data_reverse(dataset_file, 'guid')
        else:
            existing_guids = load_existing_data(dataset_file, 'guid')

        # Load URLs to scrape
        tqdm.write("Loading URLs to scrape...")
        if args.update:
            urls_to_scrape = read_lines_reverse(urls_file)
        else:
            with urls_file.open('r') as f:
                urls_to_scrape = [url.strip() for url in f]

        pbar = tqdm(total=len(urls_to_scrape), desc="Scraping data", position=1, leave=True)
        for url in urls_to_scrape:
            guid = url.split("/")[-1]
            if guid in existing_guids:
                tqdm.write(f"Skipping {guid} - already in dataset", file=sys.stdout)
                if args.update:
                    break
                else:
                    pbar.update(1)
                    continue

            content = await scrape_data(client, url, rate_limiter)
            if content and validate_data(content):
                with open(dataset_file, 'a') as f:
                    json.dump(content, f)
                    f.write('\n')
                tqdm.write(f"Added {guid} to dataset", file=sys.stdout)
            else:
                tqdm.write(f"Invalid or missing data for {guid}", file=sys.stdout)
            pbar.update(1)
        pbar.close()

    finally:
        await client.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Scraping dataset from URLs')
    parser.add_argument('--urls-output', help='Output file for URLs', default='urls.txt')
    parser.add_argument('--dataset-output', help='Output file for dataset', default='dataset.jsonl')
    parser.add_argument('--update', help='Only scrape the latest content', action='store_true')
    parser.add_argument('--data', help='Only scrape content data', action='store_true')
    parser.add_argument('--amount', type=int, help='Maximum amount of URLs to scrape at a time', default=200)
    parser.add_argument('--months', type=int, help='Number of past months to check for updates (--update mode)', default=3)
    parser.add_argument('--log-level', help='Logging level', default='INFO',
                        choices=['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL'])
    parser.add_argument('--obscura-port', type=int, default=OBSCURA_PORT, help='Port for obscura CDP')
    args = parser.parse_args()

    logging.basicConfig(
        level=getattr(logging, args.log_level),
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[logging.StreamHandler(sys.stdout)]
    )

    asyncio.run(main(args))
