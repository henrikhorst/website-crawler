import requests
from bs4 import BeautifulSoup
from collections import deque
from urllib.parse import urljoin, urlparse
import os
import threading
import logging
import re
import time

# Set up logging to help with debugging and tracking the crawler's activity
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)

# Constants to configure the crawler
USER_AGENT = "Your custom user agent"
DOMAIN = "exampledomain.com"
ROOT_URL = "https://www.exampledomain.com/"
MAX_CRAWL_LIMIT = 1000
TIMEOUT = 50


class WebCrawler:
    MAX_RETRIES = 3  # Maximum retries for a failed request
    RETRY_DELAY = 5  # Seconds to wait between retries

    def __init__(self, domain, root_url, max_limit):
        # Initial setup for the crawler
        self.domain = domain.replace("www.", "")  # Normalizing the domain name
        self.root_url = root_url
        self.max_limit = max_limit  # Max number of pages to crawl
        self.crawled_urls = set([root_url])  # Tracks URLs already crawled
        self.to_crawl = deque([root_url])  # URLs to be crawled
        self.session = requests.Session()  # Manages HTTP connections
        self.session.headers.update(
            {"User-Agent": USER_AGENT}
        )  # Set a user-agent header
        self.lock = (
            threading.Lock()
        )  # Prevents multiple threads from accessing shared resources simultaneously
        self.last_request_time = None
        self.last_request_time = (
            time.time()
        )  # Initialize last_request_time to the current time
        self.request_interval = 1 / 100  # Limit requests to 100 per second

    def crawl(self):
        # Fetch first the main page and get an initial url list
        url = self.to_crawl.popleft()  # Take the next URL to crawl
        self.fetch_page(url)
        # Start the crawling process using threads
        threads = []
        for _ in range(5):
            t = threading.Thread(target=self.crawl_thread)
            t.start()
            threads.append(t)

        for t in threads:
            t.join()  # Wait for all threads to finish

    def crawl_thread(self):
        # Method executed by each thread
        logging.info(f"{threading.current_thread().name} started.")
        while self.to_crawl and len(self.crawled_urls) < self.max_limit:
            self.lock.acquire()
            url = self.to_crawl.popleft()  # Take the next URL to crawl
            self.lock.release()
            self.fetch_page(url)
        logging.info(f"{threading.current_thread().name} finished.")

    def fetch_page(self, url):
        # Attempts to fetch a page and handle failures
        retries = 0
        while retries < self.MAX_RETRIES:
            try:
                # Throttle requests
                time.sleep(
                    max(
                        0,
                        self.request_interval - (time.time() - self.last_request_time),
                    )
                )
                response = self.session.get(url, timeout=TIMEOUT)
                self.last_request_time = time.time()
                if response.status_code != 200:
                    logging.warning(
                        f"Non-OK status code {response.status_code} for {url}"
                    )
                    return

                if "text/html" in response.headers.get("Content-Type", ""):
                    self.process_page(url, response.text)
                    self.crawled_urls.add(url)
                    return

            except requests.Timeout:
                logging.warning(
                    f"Timeout for {url}, retry {retries + 1}/{self.MAX_RETRIES}"
                )
            except requests.ConnectionError:
                logging.warning(
                    f"Connection error for {url}, retry {retries + 1}/{self.MAX_RETRIES}"
                )
            except Exception as e:
                logging.error(f"Unexpected error for {url}: {e}")
                break

            retries += 1
            if retries < self.MAX_RETRIES:
                time.sleep(self.RETRY_DELAY)

        if retries == self.MAX_RETRIES:
            logging.error(f"Failed to fetch {url} after {self.MAX_RETRIES} retries")

    def process_page(self, url, html_content):
        # Processes fetched HTML content
        soup = BeautifulSoup(html_content, "html.parser")
        self.save_html(url, soup.prettify())
        for link in self.extract_links(url, soup):
            link = self.remove_fragment(link)
            with self.lock:
                if link not in self.crawled_urls and link not in self.to_crawl:
                    self.to_crawl.append(link)

    def remove_fragment(self, url):
        # Removes URL fragments (e.g., #section)
        parsed = urlparse(url)
        return parsed.scheme + "://" + parsed.netloc + parsed.path

    def extract_links(self, url, soup):
        # Extracts all internal links on the page and filters out asset links
        links = []
        for a_tag in soup.find_all("a", href=True):
            href = a_tag.get("href")
            if not href.startswith(("http://", "https://")):
                href = urljoin(
                    self.root_url, href
                )  # Using self.root_url instead of ROOT_URL to keep it flexible
            if self.is_valid_url(href) and not self.is_asset(href):
                links.append(href)
        return links

    def is_asset(self, url):
        # Checks if the URL points to a common asset file based on its extension
        asset_extensions = [
            ".png",
            ".jpg",
            ".jpeg",
            ".gif",
            ".css",
            ".js",
            ".svg",
            ".webp",
            ".ico",
            ".pdf",
        ]
        return any(url.lower().endswith(ext) for ext in asset_extensions)

    def is_valid_url(self, url):
        # Checks if a URL belongs to the same domain
        parsed_url_domain = urlparse(url).netloc
        normalized_parsed_url_domain = parsed_url_domain.replace("www.", "")
        return normalized_parsed_url_domain == self.domain.replace("www.", "")

    def save_html(self, url, html):
        # Saves fetched HTML to the disk
        parsed_url = urlparse(url)
        domain = parsed_url.netloc
        path = parsed_url.path
        path_parts = [part for part in path.split("/") if part]
        directory_structure = os.path.join(domain, *path_parts)
        sanitized_filename = self.sanitize_url(
            path_parts[-1] if path_parts else "index"
        )
        full_file_path = os.path.join(
            f"newhtml/{directory_structure}", f"{sanitized_filename}.html"
        )
        os.makedirs(os.path.dirname(full_file_path), exist_ok=True)
        with open(full_file_path, "w", encoding="utf-8") as file:
            file.write(html)
            logging.info(f"Saved html for {url}")

    def sanitize_url(self, url_path):
        # Sanitizes strings to be safe for use as filenames
        return re.sub(r'[\\/*?:"<>|]', "_", url_path)


if __name__ == "__main__":
    crawler = WebCrawler(DOMAIN, ROOT_URL, MAX_CRAWL_LIMIT)
    crawler.crawl()
