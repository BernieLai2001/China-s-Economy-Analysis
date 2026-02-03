import requests
from bs4 import BeautifulSoup
import time
import re
import logging
from urllib.parse import urljoin
from pathlib import Path

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")


class CSRCDownloader:
    def __init__(self, base_url, download_dir="csrc_downloads"):
        self.base_url = base_url
        self.download_dir = Path(download_dir)
        self.download_dir.mkdir(exist_ok=True)

        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "Mozilla/5.0",
            "Referer": "http://www.csrc.gov.cn"
        })

        self.downloaded = set()
        self._load_downloaded()

    def _load_downloaded(self):
        f = self.download_dir / "downloaded_files.txt"
        if f.exists():
            self.downloaded = set(f.read_text(encoding="utf-8").splitlines())

    def _save_downloaded(self, url):
        f = self.download_dir / "downloaded_files.txt"
        with f.open("a", encoding="utf-8") as fp:
            fp.write(url + "\n")
        self.downloaded.add(url)

    def fetch(self, url):
        r = self.session.get(url, timeout=30)
        r.raise_for_status()
        r.encoding = "utf-8"
        return BeautifulSoup(r.text, "lxml")

    def extract_links(self, soup, page_url):
        files, contents = [], []
        for a in soup.find_all("a", href=True):
            href = a["href"]
            text = a.get_text(strip=True)
            full = urljoin(page_url, href)

            if re.search(r"\.(csv|xls|xlsx)$", href, re.I):
                name = re.sub(r"[\\/:*?\"<>|]", "_", text or full.split("/")[-1])
                files.append((full, name))
            elif "common_detail" in href or href.endswith(".shtml"):
                contents.append(full)

        return files, list(set(contents))

    def download(self, url, name):
        valid_names = any(name.endswith(ext) for ext in ['csv', 'xls', 'xlsx'])
        if url in self.downloaded and valid_names:
            return
        r = self.session.get(url, stream=True, timeout=60)
        r.raise_for_status()
        path = self.download_dir / name
        with open(path, "wb") as f:
            for c in r.iter_content(8192):
                f.write(c)
        self._save_downloaded(url)
        logger.info(f"Downloaded: {name}")

    # ---------- 智能翻页核心 ----------

    def detect_pagination_mode(self, soup, current_url):
        """
        Detect which pagination mechanism is available.
        Returns: 'button' if Next Page button exists, 'url' if URL pattern detected, None otherwise
        """
        # Priority 1: Check for Next Page button (multiple possible texts)
        next_texts = ["下一页", "下页", "next", "Next", ">", "»"]
        for a in soup.find_all("a", href=True):
            text = a.get_text(strip=True)
            # Check if it's a next page button (not disabled)
            if any(next_text in text for next_text in next_texts):
                # Check if button is disabled (common patterns)
                if a.get("class") and any("disabled" in str(c).lower() for c in a.get("class")):
                    continue
                if "disabled" in str(a.get("onclick", "")).lower():
                    continue
                return "button"
        
        # Priority 2: Check for URL-based pagination patterns in current URL
        if self._has_url_pagination_pattern(current_url):
            # Verify that next URL pattern would work
            next_url = self.generate_next_url(current_url)
            if next_url and self.url_exists(next_url):
                return "url"
        
        # Priority 3: Check for pagination links on the page (numbered pages)
        pagination_links = self.find_all_pagination_links(soup, current_url)
        if pagination_links:
            # If we found multiple pagination links, it's likely URL-based pagination
            # Try to detect the pattern from these links
            for link in pagination_links[:5]:  # Check first 5 links
                if self._has_url_pagination_pattern(link):
                    return "url"
        
        return None

    def _has_url_pagination_pattern(self, url):
        """Check if URL matches common pagination patterns"""
        patterns = [
            r"common_list(_\d+)?\.shtml",
            r"page[_\s]*(\d+)",
            r"p[_\s]*(\d+)",
            r"index[_\s]*(\d+)",
        ]
        return any(re.search(pattern, url, re.I) for pattern in patterns)

    def find_next_button(self, soup, current_url):
        """Find the Next Page button and return its URL"""
        next_texts = ["下一页", "下页", "next", "Next", ">", "»"]
        for a in soup.find_all("a", href=True):
            text = a.get_text(strip=True)
            if any(next_text in text for next_text in next_texts):
                # Skip if disabled
                if a.get("class") and any("disabled" in str(c).lower() for c in a.get("class")):
                    continue
                if "disabled" in str(a.get("onclick", "")).lower():
                    continue
                href = a.get("href")
                if href:
                    return urljoin(current_url, href)
        return None

    def find_all_pagination_links(self, soup, current_url):
        """Extract all pagination links from the page to understand the pattern"""
        pagination_links = []
        
        # Look for pagination containers (common class names)
        pagination_selectors = [
            {"class": re.compile(r"page", re.I)},
            {"class": re.compile(r"pagination", re.I)},
            {"id": re.compile(r"page", re.I)},
        ]
        
        for selector in pagination_selectors:
            containers = soup.find_all(attrs=selector)
            for container in containers:
                for a in container.find_all("a", href=True):
                    href = a.get("href")
                    if href:
                        full_url = urljoin(current_url, href)
                        pagination_links.append(full_url)
        
        # Also check for numbered links in the page
        for a in soup.find_all("a", href=True):
            href = a.get("href")
            text = a.get_text(strip=True)
            # If link text is a number and href matches pagination pattern
            if text.isdigit() and self._has_url_pagination_pattern(href):
                logger.info(f"Found numbered pagination link: {href}")
                logger.info(f"Current URL: {current_url}")
                logger.info(f"Full URL: {urljoin(current_url, href)}")
                full_url = urljoin(current_url, href)
                if full_url not in pagination_links:
                    pagination_links.append(full_url)
        
        return list(set(pagination_links))

    def generate_next_url(self, current_url):
        """Generate the next URL based on common pagination patterns"""
        # Pattern 1: common_list.shtml -> common_list_2.shtml (skip common_list_1)
        if current_url.endswith("common_list.shtml"):
            return current_url.replace("common_list.shtml", "common_list_2.shtml")
        
        # Pattern 2: common_list_N.shtml -> common_list_N+1.shtml
        m = re.search(r"common_list_(\d+)\.shtml", current_url)
        if m:
            n = int(m.group(1)) + 1
            return current_url.replace(m.group(0), f"common_list_{n}.shtml")
        
        # Pattern 3: page_N or p_N
        m = re.search(r"(page[_\s]*|p[_\s]*)(\d+)", current_url, re.I)
        if m:
            prefix = m.group(1)
            n = int(m.group(2)) + 1
            return re.sub(rf"{re.escape(prefix)}\d+", f"{prefix}{n}", current_url, flags=re.I)
        
        # Pattern 4: index_N
        m = re.search(r"index[_\s]*(\d+)", current_url, re.I)
        if m:
            n = int(m.group(1)) + 1
            return re.sub(r"index[_\s]*\d+", f"index_{n}", current_url, flags=re.I)
        
        return None

    def url_exists(self, url):
        """Check if URL exists and returns valid content"""
        try:
            response = self.session.head(url, timeout=10, allow_redirects=True)
            return response.status_code == 200
        except:
            try:
                # Sometimes HEAD is not allowed, try GET with stream
                response = self.session.get(url, timeout=10, stream=True)
                return response.status_code == 200
            except:
                return False

    def is_last_page(self, soup, current_url, pagination_mode):
        """Detect if current page is the last page"""
        if pagination_mode == "button":
            # Check if Next Page button is disabled or missing
            next_texts = ["下一页", "下页", "next", "Next", ">", "»"]
            has_next = False
            for a in soup.find_all("a", href=True):
                text = a.get_text(strip=True)
                if any(next_text in text for next_text in next_texts):
                    # If found and not disabled, it's not the last page
                    if not (a.get("class") and any("disabled" in str(c).lower() for c in a.get("class"))):
                        if "disabled" not in str(a.get("onclick", "")).lower():
                            has_next = True
                            break
            
            # Also check for "最后一页" or "末页" indicators
            last_page_texts = ["最后一页", "末页", "last", "Last"]
            for a in soup.find_all("a", href=True):
                text = a.get_text(strip=True)
                if any(last_text in text for last_text in last_page_texts):
                    # If current page matches last page link, we're on last page
                    href = a.get("href")
                    if href:
                        full_url = urljoin(current_url, href)
                        if full_url == current_url or self._urls_match(full_url, current_url):
                            return True
            
            return not has_next
        
        elif pagination_mode == "url":
            # For URL-based pagination, check if next URL exists
            next_url = self.generate_next_url(current_url)
            if next_url:
                return not self.url_exists(next_url)
            return True
        
        return True

    def _urls_match(self, url1, url2):
        """Check if two URLs point to the same page (normalized)"""
        # Remove query parameters and fragments for comparison
        from urllib.parse import urlparse
        p1 = urlparse(url1)
        p2 = urlparse(url2)
        return p1.path == p2.path

    def smart_next(self, soup, current_url, pagination_mode):
        """
        Get next page URL based on detected pagination mode.
        Returns None if no next page exists.
        """
        if pagination_mode == "button":
            next_url = self.find_next_button(soup, current_url)
            if next_url:
                logger.info(f"Using Next Page button: {next_url}")
                return next_url
            else:
                logger.info("No Next Page button found (reached last page)")
                return None
        
        elif pagination_mode == "url":
            next_url = self.generate_next_url(current_url)
            if next_url and self.url_exists(next_url):
                logger.info(f"Using URL pagination: {next_url}")
                return next_url
            else:
                logger.info(f"Next URL does not exist (reached last page): {next_url}")
                return None
        
        return None

    # ---------- 主流程 ----------

    def run(self, max_pages=None):
        current = self.base_url
        visited = set()
        page = 1
        pagination_mode = None  # Will be detected on first page

        logger.info(f"Starting download from: {current}")

        while current:
            if current in visited:
                logger.info(f"Already visited {current}, stopping to avoid loop")
                break
            if max_pages and page > max_pages:
                logger.info(f"Reached max_pages limit ({max_pages})")
                break

            visited.add(current)
            logger.info(f"Processing page {page}: {current}")

            try:
                soup = self.fetch(current)
                
                # Detect pagination mode on first page
                if pagination_mode is None:
                    pagination_mode = self.detect_pagination_mode(soup, current)
                    if pagination_mode:
                        logger.info(f"Detected pagination mode: {pagination_mode}")
                    else:
                        logger.warning("No pagination mechanism detected. Will process single page only.")
                
                # Extract and download files
                files, contents = self.extract_links(soup, current)

                for url, name in files:
                    self.download(url, name)
                    time.sleep(1)

                for c in contents:
                    try:
                        s = self.fetch(c)
                        fs, _ = self.extract_links(s, c)
                        for u, n in fs:
                            self.download(u, n)
                            time.sleep(1)
                    except Exception as e:
                        logger.warning(f"Error processing content page {c}: {e}")
                        pass

                # Check if this is the last page
                if pagination_mode and self.is_last_page(soup, current, pagination_mode):
                    logger.info(f"Reached last page (page {page})")
                    break

                # Get next page URL
                if pagination_mode:
                    current = self.smart_next(soup, current, pagination_mode)
                else:
                    # No pagination detected, only process this page
                    logger.info("No pagination available, processing single page only")
                    break

                page += 1
                time.sleep(2)  # Be polite to the server

            except Exception as e:
                logger.error(f"Error processing page {current}: {e}")
                # Try to continue with next page if possible
                if pagination_mode == "url":
                    current = self.generate_next_url(current)
                    if current and current not in visited:
                        page += 1
                        continue
                break

        logger.info(f"All pages processed. Total pages: {page}")
