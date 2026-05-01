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
    "Total|Fecha[20,1]|Estado de Vigencia[5,1]|Tema[5,1]|Organismo[5,1]|Autor[5,1]|"
    "Jurisdicción|Tribunal[5,1]|Publicación[5,1]|Colección temática[5,1]|"
    "Tipo de Documento/Jurisprudencia"
)
DEFAULT_SORT = "fecha-rango|DESC"
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
                    websockets.connect(f'ws://127.0.0.1:{self.port}/devtools/browser'),
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

        # Create target on saij.gob.ar to avoid CORS issues with fetch()
        await self.ws.send(json.dumps({
            'id': 0, 'method': 'Target.createTarget',
            'params': {'url': 'https://www.saij.gob.ar/'}
        }))
        for _ in range(5):
            raw = await asyncio.wait_for(self.ws.recv(), timeout=5)
            msg = json.loads(raw)
            if msg.get('method') == 'Target.attachedToTarget':
                self.session_id = msg['params']['sessionId']
                break

        # Enable Network and set browser-like headers
        await self._send_recv('Network.enable')
        await self._send_recv('Network.setExtraHTTPHeaders', {
            'headers': {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36',
                'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
                'Accept-Language': 'es-AR,es;q=0.9,en;q=0.8',
                'Accept-Encoding': 'gzip, deflate, br',
                'Sec-Fetch-Dest': 'document',
                'Sec-Fetch-Mode': 'navigate',
                'Sec-Fetch-Site': 'none',
                'Sec-Fetch-User': '?1',
                'Upgrade-Insecure-Requests': '1',
            }
        })

        # Visit main page first to establish cookies/session
        tqdm.write("Initializing session with saij.gob.ar...", file=sys.stdout)
        await self._send_recv('Page.navigate', {'url': 'https://www.saij.gob.ar/'})
        await asyncio.sleep(2)

    async def navigate_and_get_body(self, url: str, timeout: float = 30) -> str:
        """Navigate to a URL and return the page body text."""
        await self._send_recv('Page.navigate', {'url': url}, timeout=timeout)
        resp = await self._send_recv('Runtime.evaluate', {
            'expression': 'document.body.innerText'
        }, timeout=timeout)
        return resp.get('result', {}).get('result', {}).get('value', '')

    async def fetch_json(self, url: str, timeout: float = 30) -> str:
        """Fetch a URL using browser's fetch API and return the response text.
        This bypasses Content-Type rendering issues and works for JSON endpoints."""
        # Escape backslashes and quotes for safe JS string interpolation
        safe_url = url.replace('\\', '\\\\').replace('"', '\\"')
        resp = await self._send_recv('Runtime.evaluate', {
            'expression': f'''
                (async () => {{
                    const response = await fetch("{safe_url}");
                    if (!response.ok) throw new Error(`HTTP {{response.status}}: {{response.statusText}}`);
                    return await response.text();
                }})()
            '''.strip(),
            'awaitPromise': True,
        }, timeout=timeout)

        result = resp.get('result', {})
        if result.get('subtype') == 'error':
            raise RuntimeError(result.get('value', 'Unknown fetch error'))
        return result.get('value', '')

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
async def get_urls(client: ObscuraClient, offset: int, page_size: int, rate_limiter: RateLimiter) -> List[str]:
    await rate_limiter.wait()
    params = {
        "o": offset,
        "p": page_size,
        "f": DEFAULT_FACETS,
        "s": DEFAULT_SORT,
        "v": DEFAULT_VIEW,
    }
    # Build URL with query params
    from urllib.parse import urlencode
    url = f"{BUSQUEDA_URL}?{urlencode(params, doseq=True)}"

    try:
        body = await client.fetch_json(url, timeout=30)

        # Parse JSON response
        try:
            data = json.loads(body)
        except json.JSONDecodeError as e:
            logging.error(f"JSON decode error: {body[:200]!r}")
            rate_limiter.reset_on_error()
            raise

        if not isinstance(data, dict):
            raise ValueError(f"Unexpected response type: {type(data)}")

        if not data.get('success', True) and 'errors' in data:
            logging.error(f"SAIJ API error: {data['errors']}")
            rate_limiter.reset_on_error()
            raise ValueError(f"SAIJ API error: {data['errors']}")

        search_results = data.get("searchResults", {})
        document_list = search_results.get("documentResultList", [])

        if not document_list:
            return []

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

    except Exception as e:
        logging.error(f"Error in get_urls: {e}")
        rate_limiter.reset_on_error()
        raise


@retry(max_retries=10, delay=1)
async def scrape_data(client: ObscuraClient, url: str, rate_limiter: RateLimiter) -> Optional[dict]:
    await rate_limiter.wait()
    guid = url.split("/")[-1]
    data_url = DATA_URL.format(guid)

    try:
        body = await client.fetch_json(data_url, timeout=30)

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
            all_urls = set()
            pbar = tqdm(desc="Collecting URLs")

            if args.update:
                offset = 0
                while True:
                    urls = await get_urls(client, offset, args.amount, rate_limiter)
                    new_urls = set(urls) - existing_urls
                    if not new_urls:
                        tqdm.write(f"No more new URLs", file=sys.stdout)
                        break
                    all_urls.update(new_urls)
                    pbar.update(len(new_urls))
                    tqdm.write(f"Collected {len(new_urls)} new URLs at offset {offset}", file=sys.stdout)
                    offset += args.amount
            else:
                offset = 0
                while True:
                    urls = await get_urls(client, offset, args.amount, rate_limiter)
                    if not urls:
                        tqdm.write("No more URLs.", file=sys.stdout)
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
    parser.add_argument('--amount', type=int, help='Maximum amount of URLs to scrape at a time', default=4000)
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
