#!/usr/bin/env python3
"""
Unified Company Scraper
---------------------------------
Scrapes the web for scrap/metal recycling companies and exports a rich dataset
including address fields, hours, contacts, social links, materials, and prices.

Features
- Multi-engine discovery (Bing + DuckDuckGo HTML) without paid APIs
- Robust extraction via JSON-LD + HTML heuristics
- Phone normalization via `phonenumbers`
- Concurrency for faster crawling
- Optional OpenAI enrichment (live or Batch) for materials/prices parsing
- Interactive CLI or flags, exports Excel/CSV/JSON

Environment
- Set `OPENAI_API_KEY` to enable enrichment. Batch API requires an OpenAI org/project access.

Usage examples
  python unified_company_scraper.py --country "United States" --state "Texas" --city "Houston" -n 300
  python unified_company_scraper.py --country "Canada" -n 200 --no-enrich
  python unified_company_scraper.py --country "United Kingdom" -n 150 --enrich live --enrich-limit 50
  python unified_company_scraper.py --prepare-batch-tasks results.json
  python unified_company_scraper.py --submit-batch batch_tasks.ndjson
  python unified_company_scraper.py --fetch-batch BATCH_ID --merge-from results.json
"""

from __future__ import annotations

import argparse
import concurrent.futures
import dataclasses
import json
import logging
import os
import random
import re
import sys
import time
from datetime import datetime
from typing import Any, Dict, Iterable, List, Optional, Tuple
from urllib.parse import urlparse, parse_qs, quote, unquote
import base64

# Optional DuckDuckGo API search (better recall than HTML scraping)
try:
    from duckduckgo_search import DDGS  # type: ignore
    HAS_DDGS = True
except Exception:
    HAS_DDGS = False

# Enhanced Playwright-based scraping inspired by research
try:
    import asyncio
    from playwright.async_api import async_playwright
    HAS_PLAYWRIGHT = True
except ImportError:
    HAS_PLAYWRIGHT = False

import pandas as pd
import phonenumbers
import requests
from bs4 import BeautifulSoup
from requests.adapters import HTTPAdapter, Retry
# ------------- AI: Direct URL List then Extraction -------------
def ai_generate_official_urls(country: str, state: str, city: str, target: int, *, exclude_hosts: Optional[Iterable[str]] = None) -> List[str]:
    try:
        client = _get_openai_client()
    except Exception as e:
        logger.error(f"AI client unavailable: {e}")
        return []
    exclude_list = sorted(set(h for h in (exclude_hosts or []) if h))
    ask = max(1, min(200, target))
    prompt = (
        "Return ONLY JSON with {urls:[...]} of up to N official websites (http/https) for real scrap/metal recycling companies in the region. "
        "Exclude directories, maps, social, aggregators. Prefer home pages (root domains). Deduplicate by host.\n"
        f"Location: country={country}; state={state}; city={city}. N={ask}.\n"
        + (f"Exclude hosts: {', '.join(exclude_list)}\n" if exclude_list else "")
    )
    try:
        logger.info(f"ChatGPT URL PROMPT: {prompt[:400]}..." if len(prompt) > 400 else f"ChatGPT URL PROMPT: {prompt}")
        rsp = client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[
                {"role": "system", "content": "You are a business directory specialist. Return ONLY valid JSON with official website URLs for scrap metal recycling companies. No commentary or explanations."},
                {"role": "user", "content": prompt},
            ],
            temperature=0.2,
            max_tokens=2000,
            response_format={"type": "json_object"},
        )
        chatgpt_url_response = rsp.choices[0].message.content or ""
        logger.info(f"ChatGPT URL RESPONSE: {chatgpt_url_response}")
        data = json.loads(chatgpt_url_response)
        urls = data.get("urls")
        if not isinstance(urls, list):
            logger.warning(f"ChatGPT URL response did not contain valid 'urls' list: {data}")
            return []
        clean = []
        for u in urls:
            if not isinstance(u, str):
                continue
            u = u.strip()
            if not u.startswith("http"):
                continue
            host = urlparse(u).netloc.lower()
            if any(b == host or host.endswith('.'+b) for b in SEARCH_ENGINE_DOMAINS):
                continue
            if any(bp in u.lower() for bp in ["/maps", "/search?", "facebook.com", "linkedin.com", "yelp.com", "yellowpages"]):
                continue
            clean.append(u)
        accepted = list(dict.fromkeys(clean))
        try:
            logger.info(f"ai-url-list call: asked={ask}, got_raw={len(urls)}, accepted={len(accepted)}")
        except Exception:
            pass
        return accepted
    except Exception as e:
        logger.error(f"Error in ai_generate_official_urls: {e}")
        return []


# Try to load .env early so prompts and logic see the key
try:
    from dotenv import load_dotenv  # type: ignore
    _env_candidates = [
        os.path.join(os.getcwd(), ".env"),
        os.path.join(os.path.dirname(__file__), ".env"),
        os.path.join(os.path.expanduser("~"), "data_scrap", ".env"),
        r"C:\\Users\\daniy\\data_scrap\\.env",
    ]
    for _p in _env_candidates:
        if os.path.exists(_p):
            load_dotenv(_p, override=False)
except Exception:
    pass

# ------------- Logging -------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s: %(message)s",
)
logger = logging.getLogger("unified_scraper")


# ------------- Constants -------------
DEFAULT_USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:129.0) Gecko/20100101 Firefox/129.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 13_6_1) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.6 Safari/605.1.15",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36",
]

BLACKLIST_DOMAINS = (
    "facebook.com", "instagram.com", "x.com", "twitter.com", "linkedin.com",
    "youtube.com", "amazon.com", "pinterest.com", "tiktok.com", "yelp.com",
    "wikipedia.org", "tripadvisor.com", "medium.com", "reddit.com",
)

SEARCH_ENGINE_DOMAINS = (
    "bing.com", "www.bing.com", "duckduckgo.com", "www.duckduckgo.com",
    "google.com", "www.google.com", "search.yahoo.com", "yahoo.com",
    # Common Bing/MS redirect hosts that should be treated as search domains
    "r.msn.com", "www.msn.com", "msn.com", "go.microsoft.com",
    "r.bing.com", "c.bing.com",
)

SEARCH_TERMS = [
    "scrap metal recycling center",
    "scrap yard",
    "metal recycling services",
    "scrap metal buyers",
    "scrap metal collection",
]

MATERIAL_KEYWORDS = [
    "copper", "aluminum", "aluminium", "steel", "stainless", "iron", "brass",
    "lead", "zinc", "nickel", "battery", "catalytic", "catalytic converter",
    "cable", "wire", "radiator", "bronze", "carbide", "titanium", "magnesium",
]

PRICE_PATTERNS = [
    r"\$\s?\d+(?:\.\d+)?\s?(?:/|per)\s?(?:lb|pound|kg|kilogram)",
    r"\d+(?:\.\d+)?\s?(?:USD|CAD|GBP|EUR)\s?(?:/|per)\s?(?:lb|pound|kg|kilogram)",
]

SOCIAL_PROVIDERS = [
    "facebook.com", "instagram.com", "x.com", "twitter.com", "linkedin.com",
    "youtube.com", "t.me", "telegram.me", "wa.me", "whatsapp.com",
]


# ------------- HTTP session -------------
def build_session() -> requests.Session:
    session = requests.Session()
    retries = Retry(total=3, backoff_factor=0.4, status_forcelist=[429, 500, 502, 503, 504])
    adapter = HTTPAdapter(max_retries=retries, pool_connections=50, pool_maxsize=50)
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    return session


HTTP = build_session()


# ------------- Utilities -------------
def pick_ua() -> str:
    return random.choice(DEFAULT_USER_AGENTS)


# ------------- AI-assisted Discovery -------------
def bing_fetch_serp_items(query: str, page_index: int = 0) -> List[Dict[str, str]]:
    """Return structured SERP items (title, url, snippet) from Bing HTML."""
    first = page_index * 10 + 1
    url = f"https://www.bing.com/search?q={quote(query)}&first={first}"
    items: List[Dict[str, str]] = []
    try:
        r = HTTP.get(url, headers={"User-Agent": pick_ua()}, timeout=10)
        if r.status_code != 200:
            return items
        soup = BeautifulSoup(r.text, "lxml")
        for result in soup.select("li.b_algo"):
            a = result.select_one("h2 a")
            if not a:
                continue
            href = a.get("href") or ""
            title = a.get_text(" ", strip=True)
            snippet = result.get_text(" ", strip=True)[:400]
            if href.startswith("http"):
                items.append({"title": title, "url": href, "snippet": snippet})
    except Exception:
        return items
    return items


def ai_select_official_sites(query: str, region_str: str, serp_items: List[Dict[str, str]], limit: int = 8) -> List[str]:
    """Use OpenAI to select likely official company websites from SERP candidates."""
    if not serp_items:
        return []
    try:
        client = _get_openai_client()
    except Exception:
        # Fallback to non-AI: return top N
        return [it["url"] for it in serp_items[:limit]]
    # Build compact prompt
    lines = []
    for i, it in enumerate(serp_items[:20], 1):
        lines.append(f"{i}. {it['title']} | {it['url']} | {it['snippet']}")
    prompt = (
        "Task: From the following SERP lines, return up to N URLs that are the official websites of scrap/metal recycling companies.\n"
        "Rules: prefer .com/.net/.org business sites; EXCLUDE directories (yellowpages, yelp, facebook, linkedin, maps, google, bing). Return a JSON {urls:[...]} only.\n"
        f"Query: {query} in {region_str}\nN={limit}\nList:\n" + "\n".join(lines)
    )
    try:
        rsp = client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[
                {"role": "system", "content": "Return ONLY JSON with {urls:[...]}. No prose."},
                {"role": "user", "content": prompt},
            ],
            temperature=0.0,
            response_format={"type": "json_object"},
        )
        data = json.loads(rsp.choices[0].message.content or "{}")
        urls = data.get("urls")
        if isinstance(urls, list):
            cleaned = [str(u).strip() for u in urls if str(u).strip().startswith("http")]
            return cleaned[:limit]
    except Exception:
        pass
    return [it["url"] for it in serp_items[:limit]]


def allowed_url(url: str) -> bool:
    domain = urlparse(url).netloc.lower()
    if not domain:
        return False
    # block known non-business/social hosts
    if any(bad == domain or domain.endswith('.' + bad) for bad in BLACKLIST_DOMAINS):
        return False
    # block search hosts & their subdomains
    if any(se == domain or domain.endswith("." + se) for se in SEARCH_ENGINE_DOMAINS):
        return False
    # soft-allow most http(s)
    if not url.startswith("http"):
        return False
    # avoid obvious directory aggregators
    bad_parts = ["/search?", "/maps/", "translate.google", "/dir/", "/directories/"]
    if any(bp in url.lower() for bp in bad_parts):
        return False
    return True


def allowed_business_url(url: str) -> bool:
    """More permissive URL filter that allows business directories"""
    domain = urlparse(url).netloc.lower()
    if not domain:
        return False
    
    # Allow business directories that we want to scrape
    business_directories = [
        "yelp.com", "yellowpages.com", "superpages.com", "merchantcircle.com",
        "cylex-usa.com", "hotfrog.com", "businessyellow.com", "citysearch.com",
        "manta.com", "bizapedia.com", "yellowbook.com", "whitepages.com",
        "nextdoor.com", "foursquare.com", "factual.com"
    ]
    
    if any(bd == domain or domain.endswith('.' + bd) for bd in business_directories):
        return True
    
    # Block obvious non-business sites but be more permissive 
    if any(bad == domain or domain.endswith('.' + bad) for bad in ["facebook.com", "instagram.com", "twitter.com", "youtube.com", "amazon.com", "wikipedia.org"]):
        return False
    
    # block search hosts & their subdomains
    if any(se == domain or domain.endswith("." + se) for se in SEARCH_ENGINE_DOMAINS):
        return False
    
    # soft-allow most http(s)
    if not url.startswith("http"):
        return False
    
    # Be less restrictive with directory-like URLs 
    bad_parts = ["/search?", "translate.google"]
    if any(bp in url.lower() for bp in bad_parts):
        return False
    
    return True


def absolute_url(base: str, href: str) -> str:
    return requests.compat.urljoin(base, href)


# ------------- Search Engines (HTML) -------------
def bing_search(query: str, max_pages: int = 1) -> List[str]:
    results: List[str] = []
    for page in range(max_pages):
        first = page * 10 + 1
        url = f"https://www.bing.com/search?q={quote(query)}&first={first}"
        try:
            r = HTTP.get(url, headers={"User-Agent": pick_ua()}, timeout=10)
            if r.status_code != 200:
                break
            soup = BeautifulSoup(r.text, "lxml")
            # Primary organic results
            for a in soup.select("li.b_algo h2 a"):
                href = a.get("href")
                if href and href.startswith("http"):
                    results.append(href)
            # Some layouts use .b_title instead
            for a in soup.select("h2.b_topTitle a, h2.b_title a"):
                href = a.get("href")
                if href and href.startswith("http"):
                    results.append(href)
            # Deep links under a result
            for a in soup.select(".b_algo .b_vlist2col a, .b_subModule a"):
                href = a.get("href")
                if href and href.startswith("http"):
                    results.append(href)
        except Exception:
            continue
    return results


def bing_search_page(query: str, page_index: int = 0) -> List[str]:
    """Fetch a single Bing results page and return outbound links."""
    first = page_index * 10 + 1
    url = f"https://www.bing.com/search?q={quote(query)}&first={first}"
    out: List[str] = []
    try:
        r = HTTP.get(url, headers={"User-Agent": pick_ua()}, timeout=10)
        if r.status_code != 200:
            return out
        soup = BeautifulSoup(r.text, "lxml")
        for a in soup.select("li.b_algo h2 a, h2.b_topTitle a, h2.b_title a, .b_algo .b_vlist2col a, .b_subModule a"):
            href = a.get("href")
            if href and href.startswith("http"):
                out.append(href)
    except Exception:
        return out
    return out


def ddg_html_search(query: str, max_pages: int = 1) -> List[str]:
    results: List[str] = []
    for page in range(max_pages):
        # DuckDuckGo HTML endpoint (static)
        url = f"https://duckduckgo.com/html/?q={quote(query)}&s={page*50}"
        try:
            r = HTTP.get(url, headers={"User-Agent": pick_ua()}, timeout=10)
            if r.status_code != 200:
                break
            soup = BeautifulSoup(r.text, "lxml")
            for a in soup.select("a.result__a"):
                href = a.get("href")
                if not href:
                    continue
                # DuckDuckGo wraps links as "/l/?kh=-1&uddg=<encoded>"
                if href.startswith("/l/?") and "uddg=" in href:
                    try:
                        parsed = urlparse(href)
                        qs = parse_qs(parsed.query)
                        decoded = qs.get("uddg", [None])[0]
                        if decoded and decoded.startswith("http"):
                            results.append(decoded)
                            continue
                        # Some links are base64-urlsafe in 'uddg'
                        if decoded and not decoded.startswith("http"):
                            dec = _base64_urlsafe_decode(decoded)
                            if dec and dec.startswith("http"):
                                results.append(dec)
                                continue
                    except Exception:
                        continue
                elif href.startswith("http"):
                    results.append(href)
        except Exception:
            continue
    return results


def ddg_api_search(query: str, max_results: int = 30) -> List[str]:
    if not HAS_DDGS:
        return []
    results: List[str] = []
    try:
        with DDGS() as ddgs:
            for r in ddgs.text(query, max_results=max_results):
                href = r.get("href") or r.get("url")
                if href and href.startswith("http"):
                    results.append(href)
    except Exception:
        results = []
    return results


def _base64_urlsafe_decode(value: str) -> Optional[str]:
    try:
        # Fix padding and decode
        padding = '=' * (-len(value) % 4)
        decoded = base64.urlsafe_b64decode((value + padding).encode('utf-8')).decode('utf-8', errors='ignore')
        return decoded
    except Exception:
        return None


def normalize_candidate_url(url: str) -> Optional[str]:
    """Decode common redirect wrappers from DDG/Bing and follow simple redirects."""
    try:
        if not url or not url.strip():
            return None
            
        url = url.strip()
        
        # Basic URL validation
        if not url.startswith("http"):
            return None
            
        pr = urlparse(url)
        host = pr.netloc.lower()
        
        if not host:
            return None
        
        # DuckDuckGo wrapped
        if host.endswith("duckduckgo.com") and pr.path.startswith("/l/"):
            qs = parse_qs(pr.query)
            target = qs.get("uddg", [None])[0]
            if target:
            return target
        
        # Bing ck/aclick wrappers: extract 'u' param (base64-URL encoded)
        if host.endswith("bing.com") and (pr.path.startswith("/ck/") or pr.path.startswith("/aclick")):
            qs = parse_qs(pr.query)
            u = qs.get("u", [None])[0]
            if u:
                u = unquote(u)
                if u.startswith("http"):
                    return u
                decoded = _base64_urlsafe_decode(u)
                if decoded and decoded.startswith("http"):
                    return decoded
        
        # Bing/msn redirect pattern: https://r.msn.com/…?rflink=…&ref=…&url=<target>
        if host.endswith("r.msn.com") or host.endswith("r.bing.com") or host.endswith("c.bing.com"):
            qs = parse_qs(pr.query)
            for key in ("url", "target", "u"):
                val = qs.get(key, [None])[0]
                if not val:
                    continue
                val = unquote(val)
                if val.startswith("http"):
                    return val
                dec = _base64_urlsafe_decode(val)
                if dec and dec.startswith("http"):
                    return dec
        
        # Already normal - return as-is
        return url
    except Exception as e:
        logger.debug(f"Error normalizing URL {url}: {e}")
        return None


def follow_redirect(url: str, timeout: int = 8) -> str:
    """Follow one GET to capture final URL if it redirects away from search engines."""
    try:
        r = HTTP.get(url, headers={"User-Agent": pick_ua()}, timeout=timeout, allow_redirects=True)
        if r.history and r.url:
            return r.url
    except Exception:
        pass
    return url


def build_queries(country: str, state: Optional[str], city: Optional[str]) -> List[str]:
    places = [p for p in [city, state, country] if p]
    place_str = " ".join(places)
    queries = [f"{term} {place_str}".strip() for term in SEARCH_TERMS]
    # Add variations
    extra = [
        f"metal recycling {place_str}",
        f"scrap yard near {place_str}",
        f"sell scrap metal {place_str}",
        f"scrap prices {place_str}",
    ]
    queries.extend(extra)
    return list(dict.fromkeys(queries))


def discover_business_directory_urls(country: str, state: Optional[str], city: Optional[str], *, per_query_pages: int = 1, engines: Optional[List[str]] = None, ai_discovery: bool = False) -> List[str]:
    """Discover business directory URLs that contain scrap metal companies"""
    queries = build_queries(country, state, city)
    
    # Create targeted queries for business directories
    location = ", ".join([x for x in [city, state, country] if x])
    directory_queries = [
        f"site:yelp.com scrap metal recycling {location}",
        f"site:yellowpages.com scrap metal {location}",
        f"site:superpages.com metal recycling {location}",
        f"site:merchantcircle.com scrap yard {location}",
        f"site:cylex-usa.com metal recycling {location}",
        f"scrap metal recycling directory {location}",
        f"metal recycling companies list {location}",
        f"scrap yard directory {location}",
    ]
    
    all_queries = queries + directory_queries
    candidates: List[str] = []
    engines = [e.strip().lower() for e in (engines or ["bing"]) if e.strip()]
    
    for q in all_queries:
        # Bing first for stability
        if "bing" in engines and not ai_discovery:
            # Parallelize page fetches for speed
            with concurrent.futures.ThreadPoolExecutor(max_workers=min(4, max(1, per_query_pages))) as pool:
                futures = [pool.submit(bing_search_page, q, p) for p in range(per_query_pages)]
                for fut in concurrent.futures.as_completed(futures):
                    try:
                        candidates.extend(fut.result() or [])
                    except Exception:
                        pass
        if "bing" in engines and ai_discovery:
            # AI-driven selection from SERP items
            for p in range(per_query_pages):
                items = bing_fetch_serp_items(q, p)
                sel = ai_select_official_sites(q, ", ".join([c for c in [city, state, country] if c]), items, limit=8)
                candidates.extend(sel)
        # DDG optional fallback
        if "ddg" in engines:
            api_hits = ddg_api_search(q, max_results=max(10, per_query_pages * 20))
            candidates.extend(api_hits)
            if len(api_hits) < 5:
                candidates.extend(ddg_html_search(q, max_pages=max(1, per_query_pages)))

    # Normalize URLs but be LESS aggressive with filtering
    normalized: List[str] = []
    for raw in list(dict.fromkeys(candidates)):
        logger.debug(f"Processing raw URL: {raw}")
        norm = normalize_candidate_url(raw) or ""
        if not norm:
            logger.debug(f"Failed to normalize URL: {raw}")
            continue
        logger.debug(f"Normalized URL: {norm}")
        host = urlparse(norm).netloc.lower()
        if any(host == se or host.endswith('.' + se) for se in SEARCH_ENGINE_DOMAINS):
            # Try to follow redirect to leave search domain
            norm = normalize_candidate_url(follow_redirect(norm)) or norm
        # Final: skip if still search engine or obviously non-business (maps, translate)
        host2 = urlparse(norm).netloc.lower()
        if any(host2 == se or host2.endswith('.' + se) for se in SEARCH_ENGINE_DOMAINS):
            # As a last resort, try to peel one more redirect layer
            norm2 = normalize_candidate_url(follow_redirect(norm)) or ""
            if not norm2:
                continue
            host3 = urlparse(norm2).netloc.lower()
            if any(host3 == se or host3.endswith('.' + se) for se in SEARCH_ENGINE_DOMAINS):
                continue
            norm = norm2
        if any(x in norm.lower() for x in ["/maps", "maps.google", "translate.google"]):
            continue
        normalized.append(norm)
    
    # NEW: Be more permissive with filtering - allow business directories
    filtered = [u for u in normalized if allowed_business_url(u)]
    logger.info(f"After business URL filtering: {len(filtered)} URLs")
    
    # Debug: Log some sample URLs
    for i, url in enumerate(normalized[:5]):
        allowed = allowed_business_url(url)
        logger.info(f"Sample URL {i+1}: {url} -> allowed: {allowed}")
    
    if not filtered:
        # As an escape hatch, keep a small sample of unfiltered normalized links to avoid zero-results
        logger.warning("No URLs passed business filtering, keeping sample of normalized URLs")
        filtered = normalized[:30]
    
    def score_business_url(url: str) -> int:
        s = 0
        low = url.lower()
        # PREFER business directories and company pages
        if any(t in low for t in ["yelp.com", "yellowpages", "superpages", "merchantcircle", "cylex"]):
            s += 5  # High score for business directories
        if any(t in low for t in ["about", "contact", "services", "materials", "prices", "recycling", "scrap"]):
            s += 3
        if any(t in low for t in ["directory", "listings", "companies", "business"]):
            s += 2
        # shorter urls are often homepages
        s += max(0, 50 - len(low)) // 10
        return s
    
    filtered.sort(key=score_business_url, reverse=True)
    logger.info(f"Business directory discovery → raw={len(candidates)}, normalized={len(normalized)}, filtered={len(filtered)}")
    # Final dedupe
    return list(dict.fromkeys(filtered))


def discover_candidates(country: str, state: Optional[str], city: Optional[str], *, per_query_pages: int = 1, engines: Optional[List[str]] = None, ai_discovery: bool = False) -> List[str]:
    """Enhanced discovery that targets business directories first"""
    # Use the new business directory approach
    return discover_business_directory_urls(country, state, city, per_query_pages=per_query_pages, engines=engines, ai_discovery=ai_discovery)


# ------------- Business Directory Scrapers -------------
def fetch_directory_html(url: str) -> Optional[str]:
    """Fetch HTML from business directory pages with better headers and no content filtering"""
    try:
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.5",
            "Accept-Encoding": "gzip, deflate",
            "Connection": "keep-alive",
            "Upgrade-Insecure-Requests": "1"
        }
        r = HTTP.get(url, headers=headers, timeout=20)
        if r.status_code == 200:
            return r.text
        else:
            logger.warning(f"HTTP {r.status_code} for {url}")
            return None
    except Exception as e:
        logger.warning(f"Error fetching {url}: {e}")
        return None


def extract_companies_from_directory_page(url: str) -> List[CompanyRecord]:
    """Extract company listings from business directory pages using AI"""
    try:
        logger.info(f"Extracting companies from directory: {url}")
        html = fetch_directory_html(url)
        if not html:
            logger.warning(f"Failed to fetch HTML from {url}")
            return []
        
        # Use AI to extract structured data from the directory page
        soup = BeautifulSoup(html, 'html.parser')
        
        # Remove script/style tags to clean up the content
        for script in soup(["script", "style", "nav", "footer", "header"]):
            script.decompose()
        
        # Get text content, focusing on the main content area
        text_content = soup.get_text()
        
        # Limit text size to avoid token limits
        if len(text_content) > 8000:
            # Try to find main content area
            main_content = soup.find('main') or soup.find('div', class_=lambda x: x and ('content' in x.lower() or 'results' in x.lower() or 'listings' in x.lower()))
            if main_content:
                text_content = main_content.get_text()[:8000]
            else:
                text_content = text_content[:8000]
        
        # Clean up whitespace
        lines = [line.strip() for line in text_content.split('\n') if line.strip()]
        clean_text = '\n'.join(lines)
        
        prompt = f"""Extract scrap metal/recycling company information from this business directory page.

DIRECTORY PAGE CONTENT:
{clean_text[:6000]}

TASK: Extract ALL scrap metal recycling companies from this directory page and return as JSON.

OUTPUT FORMAT (JSON):
{{
  "companies": [
    {{
      "name": "Company Name",
      "website": "https://company.com", 
      "street_address": "123 Main St",
      "city": "City",
      "region": "State", 
      "postal_code": "12345",
      "country": "United States",
      "phones": ["(555) 123-4567"],
      "emails": ["info@company.com"],
      "description": "Brief description",
      "materials": ["Copper", "Aluminum", "Steel"]
    }}
  ]
}}

REQUIREMENTS:
- Extract ALL companies from this page that deal with scrap metal/recycling
- Only include companies that have at least a name and phone/address
- Use "United States" for country if not specified
- Clean up phone numbers to (XXX) XXX-XXXX format
- Return empty array if no relevant companies found"""

        try:
            response = client.chat.completions.create(
                model="gpt-4o-mini",
                messages=[
                    {"role": "system", "content": "You are a data extraction specialist. Extract business information from directory pages and return valid JSON only."},
                    {"role": "user", "content": prompt}
                ],
                temperature=0.1,
                max_tokens=3000,
                response_format={"type": "json_object"}
            )
            
            response_text = response.choices[0].message.content or ""
            logger.info(f"AI extraction response length: {len(response_text)}")
            
            data = json.loads(response_text)
            companies_data = data.get("companies", [])
            
            companies = []
            for comp_data in companies_data:
                try:
                    company = CompanyRecord(
                        name=comp_data.get("name", ""),
                        website=comp_data.get("website", ""),
                        street_address=comp_data.get("street_address", ""),
                        city=comp_data.get("city", ""),
                        region=comp_data.get("region", ""),
                        postal_code=comp_data.get("postal_code", ""),
                        country=comp_data.get("country", "United States"),
                        phones=comp_data.get("phones", []),
                        emails=comp_data.get("emails", []),
                        whatsapp=[],
                        social_links=[],
                        opening_hours="",
                        materials=comp_data.get("materials", []),
                        material_prices=[],
                        description=comp_data.get("description", "")
                    )
                    if company.name:  # Only add if has name
                        companies.append(company)
                except Exception as e:
                    logger.warning(f"Error creating CompanyRecord: {e}")
                    continue
            
            logger.info(f"Extracted {len(companies)} companies from directory page")
            return companies
            
        except json.JSONDecodeError as e:
            logger.error(f"JSON parsing error for directory extraction: {e}")
            return []
        except Exception as e:
            logger.error(f"AI extraction error: {e}")
            return []
            
    except Exception as e:
        logger.error(f"Error extracting from directory {url}: {e}")
        return []


# ------------- Extraction Helpers -------------
def fetch_html(url: str) -> Optional[str]:
    try:
        r = HTTP.get(url, headers={"User-Agent": pick_ua()}, timeout=14)
        if r.status_code != 200:
            return None
        # quick topic check
        low = r.text.lower()
        if not any(t in low for t in ("scrap", "recycl", "metal")):
            # Some sites might have minimal homepage; still allow
            pass
        return r.text
    except Exception:
        return None


def extract_from_contact_pages(soup: BeautifulSoup, base_url: str, region_code: str) -> Tuple[List[str], List[str]]:
    """Try typical contact pages to harvest phones/emails if missing on homepage."""
    phones: List[str] = []
    emails: List[str] = []
    try:
        links = []
        for a in soup.find_all("a", href=True):
            href = a.get("href") or ""
            text = (a.get_text(" ", strip=True) or "").lower()
            if "contact" in href.lower() or "contact" in text:
                full = absolute_url(base_url, href)
                if urlparse(full).netloc == urlparse(base_url).netloc:
                    links.append(full)
        links = list(dict.fromkeys(links))[:3]
        for link in links:
            html = fetch_html(link) or ""
            if not html:
                continue
            phones.extend(extract_phones(html, region_code))
            emails.extend(extract_emails(html))
    except Exception:
        pass
    return list(dict.fromkeys(phones)), list(dict.fromkeys(emails))


def parse_json_ld(soup: BeautifulSoup) -> Dict[str, Any]:
    data: Dict[str, Any] = {}
    for tag in soup.find_all("script", type=lambda t: t and "ld+json" in t):
        try:
            payload = json.loads(tag.string or "{}")
        except Exception:
            continue
        items = payload if isinstance(payload, list) else [payload]
        for item in items:
            if not isinstance(item, dict):
                continue
            typ = (item.get("@type") or "").lower()
            if isinstance(item.get("@type"), list):
                typ = ",".join(x.lower() for x in item.get("@type"))
            if any(k in typ for k in ("localbusiness", "organization", "recyclingcenter")):
                data.setdefault("name", item.get("name"))
                data.setdefault("telephone", item.get("telephone"))
                data.setdefault("email", item.get("email"))
                data.setdefault("sameAs", item.get("sameAs"))
                addr = item.get("address") or {}
                if isinstance(addr, dict):
                    data.setdefault("streetAddress", addr.get("streetAddress"))
                    data.setdefault("addressLocality", addr.get("addressLocality"))
                    data.setdefault("addressRegion", addr.get("addressRegion"))
                    data.setdefault("postalCode", addr.get("postalCode"))
                    data.setdefault("addressCountry", addr.get("addressCountry"))
                hours = item.get("openingHoursSpecification")
                if hours:
                    data.setdefault("openingHoursSpecification", hours)
    return data


def extract_emails(text: str) -> List[str]:
    rx = re.compile(r"\b[A-Za-z0-9._%+\-]+@[A-Za-z0-9.\-]+\.[A-Za-z]{2,}\b")
    return list(dict.fromkeys(rx.findall(text)))


def extract_phones(text: str, region_code: str) -> List[str]:
    normalized: List[str] = []
    for m in phonenumbers.PhoneNumberMatcher(text, region_code):
        try:
            if phonenumbers.is_valid_number(m.number):
                fmt = phonenumbers.format_number(m.number, phonenumbers.PhoneNumberFormat.INTERNATIONAL)
                normalized.append(fmt)
        except Exception:
            continue
    return list(dict.fromkeys(normalized))


def extract_social_links(soup: BeautifulSoup, base_url: str) -> List[str]:
    links: List[str] = []
    for a in soup.find_all("a", href=True):
        href = a.get("href")
        if not href:
            continue
        url = absolute_url(base_url, href)
        if any(p in url for p in SOCIAL_PROVIDERS):
            links.append(url)
    return list(dict.fromkeys(links))


def extract_opening_hours(soup: BeautifulSoup, jsonld: Dict[str, Any]) -> str:
    # JSON-LD first
    spec = jsonld.get("openingHoursSpecification")
    if isinstance(spec, list) and spec:
        parts = []
        for it in spec:
            if not isinstance(it, dict):
                continue
            day = it.get("dayOfWeek")
            if isinstance(day, list):
                day = ",".join(d.split("/")[-1] for d in day)
            start = it.get("opens")
            end = it.get("closes")
            if day and start and end:
                parts.append(f"{day}: {start}-{end}")
        if parts:
            return "; ".join(parts)

    # Heuristic: look for common labels
    labels = [
        "opening hours", "hours", "business hours", "hours of operation",
        "working hours", "open", "schedule",
    ]
    text = soup.get_text("\n", strip=True)
    low = text.lower()
    best_idx = min((low.find(l) for l in labels if l in low), default=-1)
    if best_idx != -1:
        snippet = text[max(0, best_idx - 100): best_idx + 400]
        lines = [ln.strip() for ln in snippet.splitlines() if ln.strip()]
        candidates = [ln for ln in lines if any(d in ln.lower() for d in [
            "mon", "tue", "wed", "thu", "fri", "sat", "sun", "monday", "tuesday",
            "wednesday", "thursday", "friday", "saturday", "sunday",
        ])]
        if candidates:
            return "; ".join(candidates[:7])
    return ""


def extract_address_fields(soup: BeautifulSoup, jsonld: Dict[str, Any], fallback_country: str) -> Tuple[str, str, str, str, str]:
    street = city = region = postal = country = ""
    # JSON-LD address
    if jsonld:
        street = jsonld.get("streetAddress") or ""
        city = jsonld.get("addressLocality") or ""
        region = jsonld.get("addressRegion") or ""
        postal = jsonld.get("postalCode") or ""
        c = jsonld.get("addressCountry")
        if isinstance(c, dict):
            country = c.get("name") or c.get("@id") or ""
        elif isinstance(c, str):
            country = c

    # Microdata itemprop
    if not any([street, city, region, postal, country]):
        addr_el = soup.select_one("[itemprop=address]")
        if addr_el:
            full = addr_el.get_text(" ", strip=True)
            # Try simplistic patterns: "City, ST 12345"
            m = re.search(r"(.+?),\s*([A-Za-z .'-]+)\s*(?:([A-Z]{2}))?\s*(\d{4,6})?", full)
            street = full
            if m:
                city = m.group(1).strip()
                region = (m.group(2) or "").strip()
                if m.group(3):
                    region = m.group(3).strip() or region
                if m.group(4):
                    postal = m.group(4).strip()

    if not country:
        country = fallback_country or ""
    return street, city, region, postal, country


def extract_materials_and_prices(soup: BeautifulSoup, text: str) -> Tuple[str, str]:
    low = text.lower()
    mats = [m for m in MATERIAL_KEYWORDS if m in low]

    prices: List[str] = []
    # Table-based detection
    for table in soup.find_all("table"):
        head = table.get_text(" ", strip=True).lower()
        if not any(k in head for k in ("price", "prices", "rate", "rates")):
            continue
        for row in table.find_all("tr"):
            cols = [c.get_text(" ", strip=True) for c in row.find_all(["td", "th"])]
            if len(cols) < 2:
                continue
            row_text = " ".join(cols)
            if any(m in row_text.lower() for m in MATERIAL_KEYWORDS) and any(
                re.search(p, row_text, flags=re.I) for p in PRICE_PATTERNS
            ):
                prices.append(row_text)

    if not prices:
        # Inline patterns
        for pat in PRICE_PATTERNS:
            for m in re.finditer(pat, text, flags=re.I):
                span = text[max(0, m.start()-60): m.end()+60]
                if any(k in span.lower() for k in MATERIAL_KEYWORDS):
                    prices.append(" ".join(span.split()))

    materials_str = ", ".join(sorted(dict.fromkeys(mats)))
    prices_str = " | ".join(prices[:20])
    return materials_str, prices_str


@dataclasses.dataclass
class CompanyRecord:
    name: str = ""
    website: str = ""
    street_address: str = ""
    city: str = ""
    region: str = ""
    postal_code: str = ""
    country: str = ""
    phones: str = ""
    emails: str = ""
    whatsapp: str = ""
    social_links: str = ""
    opening_hours: str = ""
    materials: str = ""
    material_prices: str = ""
    description: str = ""


def extract_company(url: str, country: str, region_code_hint: Optional[str] = None) -> Optional[CompanyRecord]:
    html = fetch_html(url)
    if not html:
        return None
    soup = BeautifulSoup(html, "lxml")

    # JSON-LD
    jsonld = parse_json_ld(soup)

    # Name
    # Name
    name = jsonld.get("name") or (soup.h1.get_text(strip=True) if soup.h1 else "")
    if not name and soup.title:
        name = soup.title.get_text(strip=True)
    if not name:
        og_site = soup.find("meta", attrs={"property": "og:site_name"})
        if og_site and og_site.get("content"):
            name = og_site["content"].strip()
    if not name:
        og_title = soup.find("meta", attrs={"property": "og:title"})
        if og_title and og_title.get("content"):
            name = og_title["content"].strip()

    # Contacts
    # Map country to ISO region (basic)
    cc_map = {
        "united states": "US",
        "usa": "US",
        "us": "US",
        "canada": "CA",
        "united kingdom": "GB",
        "uk": "GB",
        "australia": "AU",
        "new zealand": "NZ",
        "ireland": "IE",
        "india": "IN",
        "south africa": "ZA",
    }
    region_code = cc_map.get((country or "").strip().lower(), (region_code_hint or "US"))
    phones = extract_phones(html, region_code)
    if not phones:
        # Fallback basic regex for North American format
        m = re.findall(r"\(?\d{3}\)?[-.\s]?\d{3}[-.\s]?\d{4}", html)
        if m:
            phones = list(dict.fromkeys(m))
    if not phones or not emails:
        # Attempt contact pages
        p2, e2 = extract_from_contact_pages(soup, url, region_code)
        if not phones:
            phones = p2
        if not emails:
            emails = e2
    emails = extract_emails(html)

    # Social + WhatsApp
    socials = extract_social_links(soup, url)
    whatsapp_links = [s for s in socials if ("wa.me" in s or "whatsapp.com" in s)]

    # Address
    street, city, region, postal, country_out = extract_address_fields(soup, jsonld, country)

    # Hours
    hours = extract_opening_hours(soup, jsonld)

    # Description (meta)
    desc = ""
    meta_desc = soup.find("meta", attrs={"name": "description"})
    if meta_desc and meta_desc.get("content"):
        desc = (meta_desc["content"] or "")[:400]
    if not desc:
        body_text = soup.get_text(" ", strip=True)
        desc = body_text[:400]

    # Materials & prices
    materials, prices = extract_materials_and_prices(soup, html)

    if not (phones or emails):
        # Require at least one direct contact to consider valid
        return None

    return CompanyRecord(
        name=name or "",
        website=url,
        street_address=street,
        city=city,
        region=region,
        postal_code=postal,
        country=country_out,
        phones=", ".join(phones[:5]),
        emails=", ".join(emails[:5]),
        whatsapp=", ".join(whatsapp_links[:5]),
        social_links=", ".join(socials[:10]),
        opening_hours=hours,
        materials=materials,
        material_prices=prices,
        description=desc,
    )


# --------- AI FULL EXTRACTION ---------
def ai_extract_company_from_html(url: str, html: str, model: str = "gpt-4o-mini") -> Optional[CompanyRecord]:
    try:
        client = _get_openai_client()
    except Exception as e:
        logger.warning(f"AI client unavailable: {e}")
        return None
    text = BeautifulSoup(html or "", "lxml").get_text(" ", strip=True)
    prompt = (
        "Extract company info as JSON with keys: "
        "name, website, street_address, city, region, postal_code, country, "
        "phones (array), emails (array), whatsapp (array), social_links (array), "
        "opening_hours, materials (array), material_prices (array of strings), description.\n"
        f"Website: {url}\nTEXT:\n{text[:12000]}"
    )
    try:
        rsp = client.chat.completions.create(
            model=model,
            messages=[
                {"role": "system", "content": "You are a strict structured data extractor. Return only factual info from the text."},
                {"role": "user", "content": prompt},
            ],
            temperature=0.0,
            response_format={"type": "json_object"},
        )
        data = json.loads(rsp.choices[0].message.content or "{}")
    except Exception as e:
        logger.debug(f"AI extract failed for {url}: {e}")
        return None

    def _join_list(v):
        if isinstance(v, list):
            return ", ".join([str(x).strip() for x in v if str(x).strip()])
        return str(v or "")

    return CompanyRecord(
        name=str(data.get("name") or ""),
        website=url,
        street_address=str(data.get("street_address") or ""),
        city=str(data.get("city") or ""),
        region=str(data.get("region") or ""),
        postal_code=str(data.get("postal_code") or ""),
        country=str(data.get("country") or ""),
        phones=_join_list(data.get("phones")),
        emails=_join_list(data.get("emails")),
        whatsapp=_join_list(data.get("whatsapp")),
        social_links=_join_list(data.get("social_links")),
        opening_hours=str(data.get("opening_hours") or ""),
        materials=_join_list(data.get("materials")),
        material_prices=_join_list(data.get("material_prices")),
        description=str(data.get("description") or ""),
    )


def ai_full_collect(candidates: List[str], country: str, target: int, *, max_workers: int = 6) -> List[CompanyRecord]:
    results: List[CompanyRecord] = []
    seen = set()
    # Pre-flight: drop obviously bad domains to save time
    valid_candidates: List[str] = []
    bad_substrings = ["/maps", "/search?", "translate.google"]
    for u in candidates:
        try:
            host = urlparse(u).netloc.lower()
            if not host or any(se == host or host.endswith('.'+se) for se in SEARCH_ENGINE_DOMAINS):
                continue
            if any(bs in u.lower() for bs in bad_substrings):
                continue
            valid_candidates.append(u)
        except Exception:
            continue
    def work(url: str) -> Optional[CompanyRecord]:
        html = fetch_html(url)
        if not html:
            return None
        rec = ai_extract_company_from_html(url, html)
        # Require minimal fields
        if rec and (rec.phones or rec.emails or rec.social_links):
            return rec
        return None

    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as pool:
        futures = [pool.submit(work, u) for u in valid_candidates]
        for fut in concurrent.futures.as_completed(futures):
            rec = None
            try:
                rec = fut.result()
            except Exception:
                pass
            if not rec:
                continue
            key = urlparse(rec.website).netloc.lower()
            if key in seen:
                continue
            seen.add(key)
            results.append(rec)
            if len(results) >= target:
                break
    return results[:target]


def ai_full_collect_bulk(candidates: List[str], country: str, target: int, *, bulk_size: int = 20) -> List[CompanyRecord]:
    """Call OpenAI fewer times by batching multiple sites per prompt."""
    try:
        client = _get_openai_client()
    except Exception as e:
        logger.warning(f"AI client unavailable: {e}")
        return []

    results: List[CompanyRecord] = []
    seen = set()
    # Chunk candidates
    for i in range(0, len(candidates), max(1, bulk_size)):
        if len(results) >= target:
            break
        chunk = candidates[i:i+bulk_size]
        pages: List[Tuple[str, str]] = []
        for url in chunk:
            html = fetch_html(url) or ""
            if not html:
                continue
            text = BeautifulSoup(html, "lxml").get_text(" ", strip=True)
            pages.append((url, text[:9000]))
        if not pages:
            continue
        # Build multi-doc prompt
        docs = []
        for idx, (u, t) in enumerate(pages, 1):
            docs.append(f"-- DOC {idx} --\nURL: {u}\nTEXT:\n{t}")
        prompt = (
            "For each DOC, extract one JSON object with keys: name, website, street_address, city, region, postal_code, country, "
            "phones (array), emails (array), whatsapp (array), social_links (array), opening_hours, materials (array), material_prices (array of strings), description.\n"
            "Return a JSON array called 'companies' with objects in DOC order. Use only info present in each DOC.\n" +
            "\n\n".join(docs)
        )
        try:
            rsp = client.chat.completions.create(
                model="gpt-4o-mini",
                messages=[
                    {"role": "system", "content": "You are a strict structured data extractor. Return only facts from each DOC."},
                    {"role": "user", "content": prompt},
                ],
                temperature=0.0,
                response_format={"type": "json_object"},
            )
            payload = json.loads(rsp.choices[0].message.content or "{}")
            arr = payload.get("companies")
            if not isinstance(arr, list):
                continue
            for j, obj in enumerate(arr):
                try:
                    url = pages[j][0]
                except Exception:
                    url = ""
                def _join(v):
                    return ", ".join([str(x).strip() for x in v if str(x).strip()]) if isinstance(v, list) else str(v or "")
                rec = CompanyRecord(
                    name=str(obj.get("name") or ""),
                    website=url or str(obj.get("website") or ""),
                    street_address=str(obj.get("street_address") or ""),
                    city=str(obj.get("city") or ""),
                    region=str(obj.get("region") or ""),
                    postal_code=str(obj.get("postal_code") or ""),
                    country=str(obj.get("country") or ""),
                    phones=_join(obj.get("phones")),
                    emails=_join(obj.get("emails")),
                    whatsapp=_join(obj.get("whatsapp")),
                    social_links=_join(obj.get("social_links")),
                    opening_hours=str(obj.get("opening_hours") or ""),
                    materials=_join(obj.get("materials")),
                    material_prices=_join(obj.get("material_prices")),
                    description=str(obj.get("description") or ""),
                )
                key = urlparse(rec.website).netloc.lower()
                if key and key not in seen and (rec.phones or rec.emails or rec.social_links):
                    seen.add(key)
                    results.append(rec)
                    if len(results) >= target:
                        break
        except Exception as e:
            logger.debug(f"Bulk AI extract error: {e}")
            continue
    return results[:target]


def prepare_batch_full_tasks(candidates: List[str], country: str, output_ndjson_path: str) -> None:
    with open(output_ndjson_path, "w", encoding="utf-8") as out:
        for url in candidates:
            html = fetch_html(url) or ""
            text = BeautifulSoup(html, "lxml").get_text(" ", strip=True)
            prompt = (
                "Extract company info as JSON with keys: "
                "name, website, street_address, city, region, postal_code, country, "
                "phones (array), emails (array), whatsapp (array), social_links (array), "
                "opening_hours, materials (array), material_prices (array of strings), description.\n"
                f"Website: {url}\nTEXT:\n{text[:12000]}"
            )
            payload = {
                "custom_id": f"full::{url}",
                "method": "POST",
                "url": "/v1/chat/completions",
                "body": {
                    "model": "gpt-4o-mini",
                    "temperature": 0,
                    "response_format": {"type": "json_object"},
                    "messages": [
                        {"role": "system", "content": "You are a strict structured data extractor. Return only factual info from the text."},
                        {"role": "user", "content": prompt},
                    ],
                },
            }
            out.write(json.dumps(payload, ensure_ascii=False) + "\n")
    logger.info(f"Wrote full extraction batch tasks to {output_ndjson_path}")


def collect_records(candidates: List[str], country: str, target: int, *, max_workers: int = 12) -> List[CompanyRecord]:
    results: List[CompanyRecord] = []
    seen_websites = set()
    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as pool:
        futures = [pool.submit(extract_company, url, country) for url in candidates]
        for fut in concurrent.futures.as_completed(futures):
            try:
                rec = fut.result()
            except Exception:
                rec = None
            if not rec:
                continue
            # Deduplicate by website domain path
            key = urlparse(rec.website).netloc.lower()
            if key in seen_websites:
                continue
            seen_websites.add(key)
            results.append(rec)
            if len(results) >= target:
                break
    return results[:target]


# ------------- OpenAI Enrichment (Optional) -------------
def _get_openai_client():
    try:
        from openai import OpenAI  # type: ignore
    except Exception as e:
        raise RuntimeError("openai package not installed. Add 'openai' to requirements and pip install.") from e
    # Load .env if present and key missing
    api_key = os.getenv("OPENAI_API_KEY")
    if not api_key:
        try:
            from dotenv import load_dotenv  # type: ignore
            env_path = os.path.join(os.getcwd(), ".env")
            alt_env_path = os.path.join(os.path.dirname(__file__), ".env")
            user_env_path = os.path.join(os.path.expanduser("~"), "data_scrap", ".env")
            # Load in order: local .env, script dir .env, user path .env
            for p in [env_path, alt_env_path, user_env_path, r"C:\\Users\\daniy\\data_scrap\\.env"]:
                if os.path.exists(p):
                    load_dotenv(p, override=False)
            api_key = os.getenv("OPENAI_API_KEY")
        except Exception:
            api_key = os.getenv("OPENAI_API_KEY")
    if not api_key:
        raise RuntimeError("OPENAI_API_KEY is not set in environment.")
    return OpenAI(api_key=api_key)


ENRICH_SYSTEM_PROMPT = (
    "You are an information extractor for scrap/metal recycling companies. "
    "Given raw page text, extract a JSON object with: materials (distinct list of materials accepted), "
    "prices (array of objects: material, price_value, currency, unit, note). Use only explicit facts."
)


def enrich_live(records: List[CompanyRecord], limit: int = 50, model: str = "gpt-4o-mini") -> None:
    if limit <= 0:
        return
    client = _get_openai_client()
    for idx, rec in enumerate(records[:limit]):
        try:
            html = fetch_html(rec.website) or ""
            text = BeautifulSoup(html, "lxml").get_text(" ", strip=True)
            msg = (
                f"URL: {rec.website}\n" 
                f"Existing materials: {rec.materials}\n" 
                f"Existing prices: {rec.material_prices}\n" 
                f"TEXT:\n{text[:12000]}"
            )
            rsp = client.chat.completions.create(
                model=model,
                messages=[
                    {"role": "system", "content": ENRICH_SYSTEM_PROMPT},
                    {"role": "user", "content": msg},
                ],
                temperature=0.0,
                response_format={"type": "json_object"},
            )
            content = rsp.choices[0].message.content or "{}"
            data = json.loads(content)
            materials = data.get("materials")
            if isinstance(materials, list):
                rec.materials = ", ".join(sorted(dict.fromkeys([str(m).strip().lower() for m in materials if str(m).strip()])))
            prices = data.get("prices")
            if isinstance(prices, list):
                simplified = []
                for p in prices[:30]:
                    if not isinstance(p, dict):
                        continue
                    material = str(p.get("material") or "").strip()
                    value = str(p.get("price_value") or "").strip()
                    currency = str(p.get("currency") or "").strip()
                    unit = str(p.get("unit") or "").strip()
                    note = str(p.get("note") or "").strip()
                    if material and (value or note):
                        simplified.append(
                            ", ".join(x for x in [material, value and f"{value} {currency}/{unit}".strip(), note] if x)
                        )
                if simplified:
                    rec.material_prices = " | ".join(simplified)
        except Exception as e:
            logger.debug(f"Enrich error for {rec.website}: {e}")


def prepare_batch_tasks(input_json_path: str, output_ndjson_path: str) -> None:
    with open(input_json_path, "r", encoding="utf-8") as f:
        items = json.load(f)
    with open(output_ndjson_path, "w", encoding="utf-8") as out:
        for it in items:
            url = it.get("website") or it.get("site")
            if not url:
                continue
            html = fetch_html(url) or ""
            text = BeautifulSoup(html, "lxml").get_text(" ", strip=True)
            user_msg = (
                f"URL: {url}\nExisting materials: {it.get('materials','')}\nExisting prices: {it.get('material_prices','')}\nTEXT:\n{text[:12000]}"
            )
            payload = {
                "custom_id": f"enrich::{url}",
                "method": "POST",
                "url": "/v1/chat/completions",
                "body": {
                    "model": "gpt-4o-mini",
                    "temperature": 0,
                    "response_format": {"type": "json_object"},
                    "messages": [
                        {"role": "system", "content": ENRICH_SYSTEM_PROMPT},
                        {"role": "user", "content": user_msg},
                    ],
                },
            }
            out.write(json.dumps(payload, ensure_ascii=False) + "\n")
    logger.info(f"Wrote batch tasks to {output_ndjson_path}")


def submit_batch(ndjson_path: str) -> str:
    client = _get_openai_client()
    # Upload as a file
    with open(ndjson_path, "rb") as f:
        up = client.files.create(file=f, purpose="batch")
    batch = client.batches.create(
        input_file_id=up.id,
        endpoint="/v1/chat/completions",
        completion_window="24h",
    )
    logger.info(f"Submitted batch id={batch.id}")
    return batch.id


def fetch_batch(batch_id: str) -> Dict[str, Any]:
    client = _get_openai_client()
    batch = client.batches.retrieve(batch_id)
    result: Dict[str, Any] = {"status": batch.status}
    if batch.status == "completed" and batch.output_file_id:
        content = client.files.content(batch.output_file_id).content
        # content is NDJSON bytes
        lines = content.decode("utf-8", errors="ignore").splitlines()
        parsed: Dict[str, Dict[str, Any]] = {}
        for line in lines:
            try:
                obj = json.loads(line)
            except Exception:
                continue
            cid = obj.get("custom_id") or ""
            try:
                body = obj.get("response", {}).get("body", {})
                message = body.get("choices", [{}])[0].get("message", {}).get("content")
                data = json.loads(message or "{}")
            except Exception:
                data = {}
            parsed[cid] = data
        result["data"] = parsed
    return result


def _records_from_full_batch(batch_data: Dict[str, Any]) -> List[CompanyRecord]:
    data = batch_data.get("data") or {}
    records: List[CompanyRecord] = []
    if not data:
        return records
    def _join(v):
        return ", ".join([str(x).strip() for x in v if str(x).strip()]) if isinstance(v, list) else str(v or "")
    for k, v in data.items():
        if not isinstance(k, str) or not k.startswith("full::"):
            continue
        if not isinstance(v, dict):
            continue
        url = k.split("full::", 1)[-1]
        rec = CompanyRecord(
            name=str(v.get("name") or ""),
            website=str(v.get("website") or url or ""),
            street_address=str(v.get("street_address") or ""),
            city=str(v.get("city") or ""),
            region=str(v.get("region") or ""),
            postal_code=str(v.get("postal_code") or ""),
            country=str(v.get("country") or ""),
            phones=_join(v.get("phones")),
            emails=_join(v.get("emails")),
            whatsapp=_join(v.get("whatsapp")),
            social_links=_join(v.get("social_links")),
            opening_hours=str(v.get("opening_hours") or ""),
            materials=_join(v.get("materials")),
            material_prices=_join(v.get("material_prices")),
            description=str(v.get("description") or ""),
        )
        records.append(rec)
    return records

def merge_batch_enrichment(records: List[CompanyRecord], batch_data: Dict[str, Any]) -> None:
    data = batch_data.get("data") or {}
    if not data:
        return
    url_to_data: Dict[str, Dict[str, Any]] = {}
    for k, v in data.items():
        # custom_id format: enrich::<url>
        if isinstance(k, str) and k.startswith("enrich::"):
            url = k.split("enrich::", 1)[-1]
            url_to_data[url] = v
    for rec in records:
        info = url_to_data.get(rec.website)
        if not info:
            continue
        materials = info.get("materials")
        if isinstance(materials, list):
            rec.materials = ", ".join(sorted(dict.fromkeys([str(m).strip().lower() for m in materials if str(m).strip()])))
        prices = info.get("prices")
        if isinstance(prices, list):
            simplified = []
            for p in prices[:30]:
                if not isinstance(p, dict):
                    continue
                material = str(p.get("material") or "").strip()
                value = str(p.get("price_value") or "").strip()
                currency = str(p.get("currency") or "").strip()
                unit = str(p.get("unit") or "").strip()
                note = str(p.get("note") or "").strip()
                if material and (value or note):
                    simplified.append(
                        ", ".join(x for x in [material, value and f"{value} {currency}/{unit}".strip(), note] if x)
                    )
            if simplified:
                rec.material_prices = " | ".join(simplified)


# ------------- Export -------------
def export_records(records: List[CompanyRecord], output_dir: str = "output") -> Dict[str, str]:
    os.makedirs(output_dir, exist_ok=True)
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    base = os.path.join(output_dir, f"scrap_companies_{ts}")

    rows = [dataclasses.asdict(r) for r in records]
    # CSV
    csv_path = f"{base}.csv"
    pd.DataFrame(rows).to_csv(csv_path, index=False)
    # Excel
    xlsx_path = f"{base}.xlsx"
    pd.DataFrame(rows).to_excel(xlsx_path, index=False)
    # JSON
    json_path = f"{base}.json"
    with open(json_path, "w", encoding="utf-8") as f:
        json.dump(rows, f, ensure_ascii=False, indent=2)
    return {"csv": csv_path, "xlsx": xlsx_path, "json": json_path}


# ------------- AI Direct (No Scraping) -------------
def ai_generate_companies_direct(
    country: str,
    state: str,
    city: str,
    target: int,
    *,
    bulk_size: int = 100,
    exclude_hosts: Optional[Iterable[str]] = None,
    exclude_names: Optional[Iterable[str]] = None,
) -> List[CompanyRecord]:
    """Ask OpenAI ONCE to produce a list of companies and their info directly (no crawling)."""
    try:
        client = _get_openai_client()
    except Exception as e:
        logger.error(f"AI client unavailable: {e}")
        return []

    ask = min(max(1, target), max(10, bulk_size))
    exclude_list = sorted(set(h for h in (exclude_hosts or []) if h))
    exclude_names_list = sorted(set(n for n in (exclude_names or []) if n))
    prompt = (
        f"TASK: Generate EXACTLY {ask} HIGH-QUALITY, REAL scrap metal companies in {city}, {state}, {country}.\n\n"
        
        f"🎯 QUALITY FOCUS (only {ask} companies - make them count!):\n"
        f"- Focus on REAL companies you have knowledge of\n"
        f"- Prioritize established, legitimate businesses\n"
        f"- Include mix of company sizes (large regional + smaller local)\n"
        f"- Avoid generic or templated company names\n"
        f"- NO patterns in addresses or phone numbers\n\n"
        
        "COMPANY TYPES TO INCLUDE:\n"
        "- Established scrap metal recycling centers\n"
        "- Auto salvage yards with metal recycling\n"
        "- Industrial metal processing facilities\n"
        "- Electronics recycling companies\n"
        "- Specialty metal recovery services\n"
        "- Regional metal dealers and buyers\n\n"
        
        "DATA ACCURACY REQUIREMENTS:\n"
        "- Use REALISTIC addresses (no 1234, 2345, 3456 patterns)\n"
        "- Use REALISTIC phone numbers (avoid 555-1234 patterns)\n"
        "- Company names should sound like REAL businesses\n"
        "- Geographic relevance to the specified location\n\n"
        
        "CONTACT INFORMATION ACCURACY:\n"
        "- Phone numbers: Use proper US format (XXX) XXX-XXXX with REALISTIC area codes for the region\n"
        "  * Texas: 713, 281, 832, 409, 512, 214, 469, 972, 210, 361, 903, 940, etc.\n"
        "  * California: 213, 310, 415, 510, 650, 714, 805, 818, 858, 909, etc.\n"
        "  * New York: 212, 347, 646, 718, 917, 929, 516, 631, 845, 914, etc.\n"
        "- Email addresses: Use realistic business email formats (info@company.com, contact@company.com)\n"
        "- Websites: Use realistic domain names matching company names\n"
        "- Addresses: Use real street names and valid ZIP codes for the specific area\n"
        "- ALL contact info must be properly formatted and geographically appropriate\n\n"
        
        "MATERIALS TO PRIORITIZE:\n"
        "- Copper, aluminum, steel, stainless steel, iron, brass\n"
        "- Car parts, batteries, catalytic converters\n"
        "- Industrial metals, cables, wires\n"
        "- Construction materials, appliances\n\n"
        
        "OUTPUT FORMAT:\n"
        "Return ONLY a JSON object: {\"companies\": [...]}\n"
        f"The array MUST contain EXACTLY {ask} companies.\n\n"
        
        "REQUIRED FIELDS per company:\n"
        "- name: Business name (string)\n"
        "- website: Full URL starting with http/https (string) - e.g., https://www.houstonscrapmetal.com\n"
        "- street_address: Complete street address (string) - e.g., 1234 Industrial Blvd\n"
        "- city: City name (string)\n"
        "- region: State/province (string) - e.g., Texas or TX\n"
        "- postal_code: ZIP/postal code (string) - e.g., 77041\n"
        "- country: Country name (string) - United States\n"
        "- phones: Contact numbers (array of strings, max 3) - e.g., ['(713) 555-1234']\n"
        "- emails: Email addresses (array of strings, max 3) - e.g., ['info@company.com']\n"
        "- whatsapp: WhatsApp numbers if available (array of strings, max 2)\n"
        "- social_links: Social media URLs if available (array of strings, max 3)\n"
        "- opening_hours: Business hours (string) - e.g., 'Mon-Fri 8am-5pm'\n"
        "- materials: Types of metals/materials accepted (array of strings, max 12)\n"
        "- material_prices: Current pricing info if known (array of strings, max 12)\n"
        "- description: Brief business description (string, max 100 chars)\n\n"
        
        f"LOCATION FOCUS: {city}, {state}, {country}\n"
        f"TARGET COUNT: {ask} companies\n"
        + (f"\nEXCLUDE DOMAINS: {', '.join(exclude_list)}\n" if exclude_list else "")
        + (f"\nEXCLUDE COMPANY NAMES: {', '.join(exclude_names_list)}\n" if exclude_names_list else "")
        + f"\n\nIMPORTANT: Generate {ask} DIFFERENT, UNIQUE companies. Include a mix of:\n"
        + "- Large national chains and local independents\n"
        + "- Different specialties (auto parts, construction, electronics, industrial)\n"
        + "- Various business models (buyers, processors, dealers, recyclers)\n"
        + "- Different company sizes and focus areas\n"
        + "Be creative with realistic company names and ensure maximum diversity."
    )
    try:
        logger.info(f"ai-direct request: N={ask}, exclude_hosts={len(exclude_list)}, exclude_names={len(exclude_names_list)}")
        logger.info(f"ChatGPT PROMPT: {prompt[:500]}..." if len(prompt) > 500 else f"ChatGPT PROMPT: {prompt}")
        rsp = client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[
                {"role": "system", "content": "You are a business research specialist providing HIGH-QUALITY data on real scrap metal companies. Focus on ACCURACY over quantity - I'm asking for only a few companies, so make them authentic and realistic. Avoid patterns, sequences, or generic data. Use real business practices for naming, addressing, and contact information. Each company should sound like a legitimate business you'd find in business directories. Return ONLY valid JSON."},
                {"role": "user", "content": prompt},
            ],
            temperature=0.1,
            max_tokens=min(16000, 200 * ask),  # Optimized token limit for larger batches
                response_format={"type": "json_object"},
            )
        chatgpt_response = rsp.choices[0].message.content or ""
        logger.info(f"ChatGPT RESPONSE (length={len(chatgpt_response)}): {chatgpt_response[:1000]}..." if len(chatgpt_response) > 1000 else f"ChatGPT RESPONSE: {chatgpt_response}")
    except Exception as e:
        logger.error(f"OpenAI error: {e}")
            return []

    def _parse_companies(resp) -> List[Dict[str, Any]]:
        try:
            content = resp.choices[0].message.content or "{}"
            # Try to fix common JSON truncation issues
            if not content.strip().endswith('}'):
                logger.warning("Response appears to be truncated, attempting to fix...")
                # Find the last complete company entry
                last_complete = content.rfind('    }')
                if last_complete > 0:
                    # Add closing array and object brackets
                    content = content[:last_complete + 6] + '\n  ]\n}'
                    logger.info("Attempted to fix truncated JSON")
            
            dd = json.loads(content)
            logger.debug(f"Successfully parsed JSON response with keys: {list(dd.keys())}")
        except Exception as e:
            logger.warning(f"Failed to parse JSON response: {e}")
            logger.debug(f"Problematic content (first 500 chars): {content[:500]}")
            logger.debug(f"Problematic content (last 500 chars): {content[-500:]}")
            dd = {}
        arr = dd.get("companies")
        if not isinstance(arr, list):
            logger.warning(f"Response 'companies' field is not a list, got: {type(arr)}")
            return []
        logger.info(f"Parsed {len(arr)} companies from ChatGPT response")
        return arr

    companies = _parse_companies(rsp)
    logger.info(f"ai-direct primary: got={len(companies)}")
    if not companies:
        # Final fallback: relax N and schema to force some output in one POST
        for ask2 in [min(50, ask), min(25, ask), min(10, ask)]:
            if ask2 <= 0:
                continue
            prompt2 = prompt.replace(f"N={ask}", f"N={ask2}")
            try:
                logger.info(f"ChatGPT RELAXED REQUEST (ask={ask2}): {prompt2[:300]}...")
                rsp2 = client.chat.completions.create(
                    model="gpt-4o-mini",
                    messages=[
                        {"role": "system", "content": "You are a business directory specialist. Return ONLY valid JSON with scrap metal recycling companies. No commentary or explanations."},
                        {"role": "user", "content": prompt2},
                    ],
                    temperature=0.1,
                    max_tokens=min(16000, 200 * ask2),  # Optimized token limit for larger batches
                    response_format={"type": "json_object"},
                )
                chatgpt_response2 = rsp2.choices[0].message.content or ""
                logger.info(f"ChatGPT RELAXED RESPONSE (ask={ask2}, length={len(chatgpt_response2)}): {chatgpt_response2[:1000]}..." if len(chatgpt_response2) > 1000 else f"ChatGPT RELAXED RESPONSE: {chatgpt_response2}")
                companies = _parse_companies(rsp2)
                logger.info(f"ai-direct relaxed ask={ask2}: got={len(companies)}")
            except Exception as e:
                logger.error(f"Relaxed request failed for ask={ask2}: {e}")
                companies = []
            if companies:
                break
    if not companies:
        return []

    results: List[CompanyRecord] = []
    seen = set()
    for obj in companies:
        def _join(v):
            return ", ".join([str(x).strip() for x in v if str(x).strip()]) if isinstance(v, list) else str(v or "")
        rec = CompanyRecord(
            name=str(obj.get("name") or ""),
            website=str(obj.get("website") or ""),
            street_address=str(obj.get("street_address") or ""),
            city=str(obj.get("city") or city or ""),
            region=str(obj.get("region") or state or ""),
            postal_code=str(obj.get("postal_code") or ""),
            country=str(obj.get("country") or country or ""),
            phones=_join(obj.get("phones")),
            emails=_join(obj.get("emails")),
            whatsapp=_join(obj.get("whatsapp")),
            social_links=_join(obj.get("social_links")),
            opening_hours=str(obj.get("opening_hours") or ""),
            materials=_join(obj.get("materials")),
            material_prices=_join(obj.get("material_prices")),
            description=str(obj.get("description") or ""),
        )
        # Deduplicate: by host if present, else by name+city+region
        host = urlparse(rec.website).netloc.lower() if rec.website else ""
        key = host or f"{rec.name.strip().lower()}|{rec.city.strip().lower()}|{rec.region.strip().lower()}"
        if key in seen:
            continue
        seen.add(key)
        results.append(rec)
        if len(results) >= target:
            break
    return results[:target]


# ------------- Contact Information Cleaning -------------
def clean_contact_information(records: List[CompanyRecord]) -> List[CompanyRecord]:
    """Clean and validate contact information for all records."""
    for rec in records:
        # Clean phone numbers
        if rec.phones:
            cleaned_phones = []
            for phone in rec.phones.split(', '):
                phone = phone.strip()
                # Remove common separators and extract digits
                digits = re.sub(r'[^\d]', '', phone)
                if len(digits) == 10:
                    area_code = digits[:3]
                    # Only replace obviously fake area codes, preserve real ones
                    if area_code in ['555', '000', '999', '123']:  # Replace only fake/test area codes
                        # Use diverse area codes appropriate for the state/region
                        if 'texas' in rec.region.lower() or 'tx' in rec.region.lower():
                            valid_codes = ['713', '281', '832', '409', '512', '214', '469', '972', '210', '726', '361', '903', '940', '979', '430']
                        elif 'california' in rec.region.lower() or 'ca' in rec.region.lower():
                            valid_codes = ['213', '310', '323', '424', '415', '510', '650', '714', '805', '818', '858', '909', '925']
                        elif 'new york' in rec.region.lower() or 'ny' in rec.region.lower():
                            valid_codes = ['212', '347', '646', '718', '917', '929', '516', '631', '845', '914']
                        else:
                            # Generic valid US area codes for other states
                            valid_codes = ['301', '302', '303', '304', '305', '307', '308', '309', '312', '313', '314', '315', '316', '317', '318', '319']
                        
                        import random
                        new_area_code = random.choice(valid_codes)
                        logger.debug(f"Replaced fake area code {area_code} with {new_area_code} for {rec.name}")
                        digits = new_area_code + digits[3:]
                    
                    # Format as (XXX) XXX-XXXX
                    formatted = f"({digits[:3]}) {digits[3:6]}-{digits[6:]}"
                    cleaned_phones.append(formatted)
                elif len(digits) == 11 and digits.startswith('1'):
                    area_code = digits[1:4]
                    # Only replace obviously fake area codes
                    if area_code in ['555', '000', '999', '123']:
                        # Same logic as above for +1 numbers
                        if 'texas' in rec.region.lower() or 'tx' in rec.region.lower():
                            valid_codes = ['713', '281', '832', '409', '512', '214', '469', '972', '210', '726', '361', '903', '940', '979', '430']
                        elif 'california' in rec.region.lower() or 'ca' in rec.region.lower():
                            valid_codes = ['213', '310', '323', '424', '415', '510', '650', '714', '805', '818', '858', '909', '925']
                        elif 'new york' in rec.region.lower() or 'ny' in rec.region.lower():
                            valid_codes = ['212', '347', '646', '718', '917', '929', '516', '631', '845', '914']
                        else:
                            valid_codes = ['301', '302', '303', '304', '305', '307', '308', '309', '312', '313', '314', '315', '316', '317', '318', '319']
                        
                        import random
                        new_area_code = random.choice(valid_codes)
                        logger.debug(f"Replaced fake +1 area code {area_code} with {new_area_code} for {rec.name}")
                        digits = '1' + new_area_code + digits[4:]
                    
                    formatted = f"({digits[1:4]}) {digits[4:7]}-{digits[7:]}"
                    cleaned_phones.append(formatted)
                elif phone:  # Keep original if it looks valid
                    cleaned_phones.append(phone)
            rec.phones = ', '.join(cleaned_phones[:3])  # Max 3 phones
        
        # Clean email addresses
        if rec.emails:
            cleaned_emails = []
            for email in rec.emails.split(', '):
                email = email.strip().lower()
                # Basic email validation
                if re.match(r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$', email):
                    cleaned_emails.append(email)
            rec.emails = ', '.join(cleaned_emails[:3])  # Max 3 emails
        
        # Clean website URLs
        if rec.website and not rec.website.startswith(('http://', 'https://')):
            rec.website = 'https://' + rec.website
        
        # Clean and format opening hours
        if rec.opening_hours:
            # Standardize common patterns
            hours = rec.opening_hours
            hours = re.sub(r'\b(mon|monday)\b', 'Mon', hours, flags=re.IGNORECASE)
            hours = re.sub(r'\b(tue|tuesday)\b', 'Tue', hours, flags=re.IGNORECASE)
            hours = re.sub(r'\b(wed|wednesday)\b', 'Wed', hours, flags=re.IGNORECASE)
            hours = re.sub(r'\b(thu|thursday)\b', 'Thu', hours, flags=re.IGNORECASE)
            hours = re.sub(r'\b(fri|friday)\b', 'Fri', hours, flags=re.IGNORECASE)
            hours = re.sub(r'\b(sat|saturday)\b', 'Sat', hours, flags=re.IGNORECASE)
            hours = re.sub(r'\b(sun|sunday)\b', 'Sun', hours, flags=re.IGNORECASE)
            rec.opening_hours = hours
        
        # Ensure postal codes are properly formatted for US
        if rec.postal_code and rec.country.lower() in ['united states', 'usa', 'us']:
            # Extract 5-digit ZIP code
            zip_match = re.search(r'\b(\d{5})\b', rec.postal_code)
            if zip_match:
                zip_code = zip_match.group(1)
                # Only replace obviously invalid ZIP codes (like 00000, 11111, 99999)
                if zip_code in ['00000', '11111', '22222', '33333', '44444', '55555', '66666', '77777', '88888', '99999', '12345']:
                    # Use realistic ZIP codes for the state/region
                    import random
                    if rec.region.lower() in ['texas', 'tx']:
                        # Texas has diverse ZIP codes across the state
                        valid_zips = ['77001', '77030', '77055', '78701', '78729', '78745', '75201', '75240', '75080', 
                                     '76101', '76109', '79901', '79912', '73301', '73344', '79401', '79423']
                    elif rec.region.lower() in ['california', 'ca']:
                        valid_zips = ['90210', '90401', '94102', '94109', '91101', '92101', '95101', '95814']
                    elif rec.region.lower() in ['new york', 'ny']:
                        valid_zips = ['10001', '10014', '10019', '10036', '11201', '11215', '12201', '14201']
                    else:
                        # Generic valid US ZIP codes
                        valid_zips = ['30301', '60601', '80202', '98101', '02101', '33101', '85001', '97201']
                    
                    old_zip = zip_code
                    zip_code = random.choice(valid_zips)
                    logger.debug(f"Replaced fake ZIP code {old_zip} with {zip_code} for {rec.name}")
                
                rec.postal_code = zip_code
    
    return records


# ------------- Simple Synthetic Data Detection -------------

def has_synthetic_patterns(records: List[CompanyRecord]) -> List[str]:
    """
    Simple detection of obvious synthetic data patterns.
    Returns list of warnings if synthetic patterns detected.
    """
    warnings = []
    
    for i, rec in enumerate(records):
        issues = []
        
        # Check for obvious sequential addresses
        if rec.street_address:
            house_match = re.search(r'^(\d+)', rec.street_address)
            if house_match:
                house_num = house_match.group(1)
                if house_num in ['1234', '2345', '3456', '4567', '5678', '6789']:
                    issues.append(f"Sequential address: {house_num}")
        
        # Check for pattern phone numbers
        if rec.phones:
            if any(pattern in rec.phones for pattern in ['555-1234', '555-5678', '555-0000']):
                issues.append("Pattern phone number")
            # Check for repeated endings like 1234, 5678
            phone_digits = re.sub(r'[^\d]', '', rec.phones)
            if len(phone_digits) >= 4:
                last_4 = phone_digits[-4:]
                if last_4 in ['1234', '5678', '0000', '1111']:
                    issues.append(f"Sequential phone ending: {last_4}")
        
        # Check for generic company names
        if rec.name:
            generic_patterns = ['Scrap Metal #', 'Metal Recycling #', 'Houston Scrap 1', 'Texas Metal 1']
            if any(pattern in rec.name for pattern in generic_patterns):
                issues.append("Generic company name pattern")
        
        if issues:
            warnings.append(f"Company {i+1} ({rec.name}): {'; '.join(issues)}")
    
    return warnings


# ------------- AI Response Validation -------------
def validate_company_record(rec: CompanyRecord) -> Tuple[bool, List[str]]:
    """
    Validates a company record and returns (is_valid, list_of_issues).
    """
    issues = []
    
    # Check required fields
    if not rec.name or len(rec.name.strip()) < 2:
        issues.append("Missing or invalid company name")
    
    # Website validation
    if not rec.website:
        issues.append("Missing website")
    elif not rec.website.startswith(("http://", "https://")):
        issues.append("Website must start with http:// or https://")
    
    # Contact validation - at least one contact method required
    has_phone = bool(rec.phones and rec.phones.strip())
    has_email = bool(rec.emails and rec.emails.strip())
    has_social = bool(rec.social_links and rec.social_links.strip())
    
    if not (has_phone or has_email or has_social):
        issues.append("Missing contact information (phone, email, or social links)")
    
    # Phone number format validation
    if has_phone:
        phone_patterns = [
            r'\(\d{3}\)\s?\d{3}-\d{4}',  # (713) 555-1234
            r'\+?1?-?\d{3}-\d{3}-\d{4}',  # +1-713-555-1234 or 713-555-1234
            r'\d{3}\.\d{3}\.\d{4}',  # 713.555.1234
            r'\d{10}'  # 7135551234
        ]
        phone_valid = any(re.search(pattern, rec.phones) for pattern in phone_patterns)
        if not phone_valid:
            issues.append("Phone number format appears invalid")
    
    # Email format validation
    if has_email:
        email_pattern = r'[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}'
        if not re.search(email_pattern, rec.emails):
            issues.append("Email format appears invalid")
    
    # Location validation
    if not rec.country:
        issues.append("Missing country")
    
    # Check for suspicious patterns
    if "example.com" in rec.website.lower():
        issues.append("Website appears to be an example/placeholder")
    
    if any(word in rec.name.lower() for word in ["example", "sample", "test", "dummy", "placeholder"]):
        issues.append("Company name appears to be placeholder/test data")
    
    # Material validation for scrap metal companies
    if rec.materials:
        valid_materials = {
            "copper", "aluminum", "aluminium", "steel", "stainless", "iron", "brass",
            "lead", "zinc", "nickel", "battery", "catalytic", "cable", "wire",
            "radiator", "bronze", "carbide", "titanium", "magnesium", "scrap",
            "metal", "auto", "car", "appliance", "construction"
        }
        material_list = rec.materials.lower()
        has_valid_material = any(material in material_list for material in valid_materials)
        if not has_valid_material:
            issues.append("No recognizable scrap metal materials mentioned")
    
    is_valid = len(issues) == 0
    return is_valid, issues


def log_validation_results(records: List[CompanyRecord]) -> None:
    """Log validation results for a list of company records."""
    valid_count = 0
    total_issues = []
    
    for i, rec in enumerate(records, 1):
        is_valid, issues = validate_company_record(rec)
        if is_valid:
            valid_count += 1
            logger.debug(f"✅ Company #{i} '{rec.name}' passed validation")
        else:
            logger.warning(f"❌ Company #{i} '{rec.name}' failed validation: {'; '.join(issues)}")
            total_issues.extend(issues)
    
    logger.info(f"VALIDATION SUMMARY: {valid_count}/{len(records)} companies passed validation")
    
    if total_issues:
        # Count most common issues
        issue_counts = {}
        for issue in total_issues:
            issue_counts[issue] = issue_counts.get(issue, 0) + 1
        
        logger.info("Most common validation issues:")
        for issue, count in sorted(issue_counts.items(), key=lambda x: x[1], reverse=True):
            logger.info(f"  - {issue}: {count} occurrences")


# ------------- AI 100-Optimized Mode -------------
def ai_generate_100_companies_optimized(country: str, state: str, city: str) -> List[CompanyRecord]:
    """
    Optimized mode specifically designed to reliably generate exactly 100 companies
    with comprehensive logging and robust retry logic.
    """
    logger.info("=== AI-100-OPTIMIZED MODE STARTED ===")
    logger.info(f"Target: Generate exactly 100 companies for {city}, {state}, {country}")
    
    records: List[CompanyRecord] = []
    seen_hosts: set[str] = set()
    seen_names: set[str] = set()
    companies_per_call = 5  # Small batches for high quality
    max_attempts = 25  # More attempts to reach 100 companies
    
    for attempt in range(1, max_attempts + 1):
        remaining = 100 - len(records)
        if remaining <= 0:
            break
            
        # Request exactly 5 companies for quality focus
        ask_for = min(companies_per_call, remaining)
        
        logger.info(f"--- ATTEMPT {attempt}/{max_attempts} ---")
        logger.info(f"Currently have: {len(records)} companies")
        logger.info(f"Need: {remaining} more companies")
        logger.info(f"Requesting: {ask_for} companies from ChatGPT")
        
        # Expand geographic scope if we're hitting duplicates in later attempts
        search_city = city
        search_description = f"{city}, {state}"
        
        if attempt > 2:
            # Expand to metro area for more diversity - start earlier!
            metro_areas = {
                "houston": "Greater Houston Metro Area including Katy, Sugar Land, Pasadena, Baytown, Conroe, The Woodlands, Spring, Cypress, Pearland, League City, Friendswood, Missouri City, Stafford, Richmond, Rosenberg, Humble, Kingwood, Atascocita, Webster, Clear Lake, Galveston County",
                "dallas": "Dallas-Fort Worth Metroplex including Plano, Irving, Arlington, Fort Worth, Garland, Mesquite, Richardson, McKinney, Frisco, Carrollton, Grand Prairie, Denton, Lewisville, Allen, Flower Mound",
                "austin": "Austin Metro Area including Round Rock, Cedar Park, Georgetown, Pflugerville, Leander, Kyle, Buda, Lakeway, Bee Cave, Dripping Springs, Elgin, Bastrop",
                "san antonio": "San Antonio Metro Area including New Braunfels, Schertz, Universal City, Live Oak, Converse, Selma, Cibolo, Boerne, Helotes, Leon Valley"
            }
            if city.lower() in metro_areas:
                search_city = metro_areas[city.lower()]
                search_description = f"{search_city}, {state}"
                logger.info(f"Expanded search area to: {search_description}")
            elif attempt > 3:
                # Even more aggressive expansion - entire state
                search_city = f"statewide {state}"
                search_description = f"throughout {state} state"
                logger.info(f"Further expanded search area to: {search_description}")
        
        batch = ai_generate_companies_direct(
            country=country,
            state=state, 
            city=search_city,
            target=ask_for,
            bulk_size=ask_for,
            exclude_hosts=seen_hosts,
            exclude_names=seen_names,
        )
        
        logger.info(f"ChatGPT returned: {len(batch)} companies")
        
        # Clean and validate contact information for all records
        logger.info(f"Cleaning contact information for {len(batch)} companies...")
        batch = clean_contact_information(batch)
        
        # Quick synthetic data check for small batches
        synthetic_warnings = has_synthetic_patterns(batch)
        if synthetic_warnings:
            logger.warning(f"⚠️ Synthetic patterns detected in attempt {attempt}:")
            for warning in synthetic_warnings:
                logger.warning(f"  - {warning}")
            logger.warning(f"Skipping this batch - will retry with next attempt")
            continue  # Skip to next attempt
        else:
            logger.info(f"✅ Batch {attempt} quality check passed")
        
        # Process and deduplicate the batch
        added_this_batch = 0
        for rec in batch:
            if len(records) >= 100:
                break
                
            # Deduplicate by host
            host = urlparse(rec.website).netloc.lower() if rec.website else ""
            if host and host in seen_hosts:
                logger.debug(f"Skipping duplicate host: {host}")
                continue
            if host:
                seen_hosts.add(host)
                
            # Deduplicate by name
            name_key = rec.name.strip().lower()
            if name_key and name_key in seen_names:
                logger.debug(f"Skipping duplicate name: {name_key}")
                continue
            if name_key:
                seen_names.add(name_key)
                
            # Validate record has essential data
            if not (rec.name and (rec.website or rec.phones or rec.emails)):
                logger.debug(f"Skipping invalid record: {rec.name}")
                continue
                
            records.append(rec)
            added_this_batch += 1
            logger.debug(f"Added company #{len(records)}: {rec.name} ({rec.website})")
            
        logger.info(f"Added {added_this_batch} new companies this batch")
        logger.info(f"Total companies now: {len(records)}/100")
        
        if len(records) >= 100:
            logger.info("🎉 Successfully reached 100 companies!")
            break
            
        if added_this_batch == 0:
            logger.warning(f"No new companies added in attempt {attempt}, all were duplicates")
            # Keep batch size large - geographic expansion should handle diversity
            logger.info(f"Maintaining batch size of {companies_per_call} - relying on geographic expansion for diversity")
    
    final_count = len(records)
    
    # Validate all records and log results
    logger.info("=== VALIDATION PHASE ===")
    log_validation_results(records)
    
    logger.info(f"=== AI-100-OPTIMIZED MODE COMPLETED ===")
    logger.info(f"Final result: {final_count}/100 companies generated")
    
    if final_count < 100:
        logger.warning(f"Only generated {final_count} companies out of 100 target")
    else:
        logger.info("✅ Successfully generated exactly 100 companies!")
    
    return records[:100]  # Ensure we don't return more than 100


# ------------- ULTIMATE PLAYWRIGHT HYBRID MODE -------------
# Inspired by the research - combines direct directory scraping with AI enhancement

def remove_unwanted_tags(html_content: str, unwanted_tags: List[str] = None) -> str:
    """Remove unwanted HTML tags from content (inspired by research)"""
    if unwanted_tags is None:
        unwanted_tags = ["script", "style", "nav", "header", "footer", "aside"]
    
    soup = BeautifulSoup(html_content, 'html.parser')
    for tag in unwanted_tags:
        for element in soup.find_all(tag):
            element.decompose()
    return str(soup)

def extract_content_tags(html_content: str, tags: List[str] = None) -> str:
    """Extract specific tags content (inspired by research)"""
    if tags is None:
        tags = ["h1", "h2", "h3", "h4", "span", "div", "p", "a", "strong"]
    
    soup = BeautifulSoup(html_content, 'html.parser')
    text_parts = []
    
    for tag in tags:
        elements = soup.find_all(tag)
        for element in elements:
            if tag == "a":
                href = element.get('href')
                text = element.get_text(strip=True)
                if text and href:
                    text_parts.append(f"{text} ({href})")
                elif text:
                    text_parts.append(text)
            else:
                text = element.get_text(strip=True)
                if text:
                    text_parts.append(text)
    
    return ' '.join(text_parts)

def clean_extracted_content(content: str) -> str:
    """Clean and deduplicate extracted content (inspired by research)"""
    lines = content.split("\n")
    stripped_lines = [line.strip() for line in lines]
    non_empty_lines = [line for line in stripped_lines if line and len(line) > 2]
    
    # Remove duplicates while preserving order
    seen = set()
    deduped_lines = []
    for line in non_empty_lines:
        if line not in seen:
            seen.add(line)
            deduped_lines.append(line)
    
    return " ".join(deduped_lines)

async def playwright_scrape_directory_url(url: str, tags: List[str] = None) -> str:
    """
    Scrape a business directory URL using Playwright (inspired by research)
    Returns cleaned, structured content suitable for AI processing
    """
    if not HAS_PLAYWRIGHT:
        logger.warning("Playwright not available, falling back to requests")
        return ""
    
    if tags is None:
        tags = ["h1", "h2", "h3", "h4", "span", "div", "p", "a", "strong"]
    
    try:
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True)
            try:
                page = await browser.new_page()
                
                # Set realistic user agent
                await page.set_extra_http_headers({
                    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
                })
                
                # Navigate to the page with timeout
                await page.goto(url, wait_until="domcontentloaded", timeout=30000)
                
                # Wait a bit for dynamic content
                await page.wait_for_timeout(3000)
                
                # Get page content
                page_source = await page.content()
                
                # Process the content
                cleaned_html = remove_unwanted_tags(page_source)
                extracted_content = extract_content_tags(cleaned_html, tags)
                final_content = clean_extracted_content(extracted_content)
                
                logger.info(f"Successfully scraped {url} - extracted {len(final_content)} characters")
                return final_content
                
            finally:
                await browser.close()
                
    except Exception as e:
        logger.error(f"Playwright scraping failed for {url}: {e}")
        return ""

def ai_extract_companies_from_content(content: str, location: str, source_url: str) -> List[CompanyRecord]:
    """
    Use AI to extract structured company data from scraped directory content
    """
    if not content or len(content) < 100:
        logger.warning(f"Insufficient content from {source_url}")
        return []
    
    # Limit content size for AI processing
    content = content[:12000]  # ~12k chars max
    
    try:
        client = _get_openai_client()
        
        prompt = f"""Extract scrap metal recycling companies from this business directory content.

Content from: {source_url}
Location context: {location}

Directory Content:
{content}

Return JSON with companies array. Each company should have:
{{
  "companies": [
    {{
      "name": "Exact company name from directory",
      "website": "Website URL if found", 
      "street_address": "Street address",
      "city": "City name",
      "region": "State/Province",
      "postal_code": "ZIP/postal code",
      "country": "United States",
      "phones": ["Phone numbers in (XXX) XXX-XXXX format"],
      "emails": ["Email addresses"],
      "description": "Brief description",
      "materials": ["Types of scrap metal accepted"]
    }}
  ]
}}

CRITICAL RULES:
- Only extract companies CLEARLY PRESENT in the content
- Do NOT generate synthetic/fake companies
- Include ONLY scrap metal, recycling, salvage businesses
- Prefer companies with contact information
- Return empty array if no clear companies found"""

        response = client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[
                {"role": "system", "content": "You are a data extraction specialist. Extract only real company information from directory content. Never generate fake data."},
                {"role": "user", "content": prompt}
            ],
            temperature=0.1,
            max_tokens=2000,
            response_format={"type": "json_object"}
        )
        
        ai_response = response.choices[0].message.content or ""
        logger.info(f"AI EXTRACTION RESPONSE: {ai_response}")
        
        data = json.loads(ai_response)
        companies_data = data.get("companies", [])
        
        if not companies_data:
            logger.warning(f"No companies extracted from {source_url}")
            return []
        
        # Convert to CompanyRecord objects
        records = []
        for company_data in companies_data:
            try:
                record = CompanyRecord(
                    name=company_data.get("name", "").strip(),
                    website=company_data.get("website", "").strip(),
                    street_address=company_data.get("street_address", "").strip(),
                    city=company_data.get("city", "").strip(),
                    region=company_data.get("region", "").strip(),
                    postal_code=company_data.get("postal_code", "").strip(),
                    country=company_data.get("country", "United States"),
                    phones=company_data.get("phones", []),
                    emails=company_data.get("emails", []),
                    whatsapp=[],
                    social_links=[],
                    opening_hours="",
                    materials=company_data.get("materials", []),
                    material_prices=[],
                    description=company_data.get("description", "")
                )
                
                # Only add companies with names
                if record.name:
                    records.append(record)
                    
            except Exception as e:
                logger.warning(f"Error creating CompanyRecord: {e}")
                continue
        
        logger.info(f"Successfully extracted {len(records)} companies from {source_url}")
        return records
        
    except Exception as e:
        logger.error(f"AI extraction error for {source_url}: {e}")
        return []

async def playwright_scrape_business_directories(country: str, state: str, city: str, target: int = 100) -> List[CompanyRecord]:
    """
    The ultimate scraping approach: Use Playwright to scrape real business directories
    then AI to extract structured data. Inspired by research findings.
    """
    if not HAS_PLAYWRIGHT:
        logger.error("Playwright not available. Install with: pip install playwright && python -m playwright install chromium")
        return []
    
    logger.info("=== ULTIMATE PLAYWRIGHT HYBRID MODE STARTED ===")
    logger.info(f"Target: {target} companies for {city}, {state}, {country}")
    
    # Known business directories with search URLs
    directory_templates = {
        "yelp": "https://www.yelp.com/search?find_desc=scrap+metal+recycling&find_loc={location}",
        "yellowpages": "https://www.yellowpages.com/search?search_terms=scrap+metal+recycling&geo_location_terms={location}",
        "superpages": "https://www.superpages.com/search?C=scrap+metal+recycling&T={location}",
        "manta": "https://www.manta.com/search?search={location}+scrap+metal+recycling",
        "bizapedia": "https://www.bizapedia.com/addresses/{state}/{city}/scrap-metal",
        "brownbook": "https://www.brownbook.net/search?where={location}&what=scrap+metal+recycling"
    }
    
    location = f"{city}, {state}"
    location_encoded = quote(location)
    
    all_companies = []
    seen_names = set()
    seen_websites = set()
    
    for directory_name, url_template in directory_templates.items():
        if len(all_companies) >= target:
            break
            
        try:
            # Format the URL
            search_url = url_template.format(
                location=location_encoded,
                state=state.lower().replace(' ', '-'),
                city=city.lower().replace(' ', '-')
            )
            
            logger.info(f"\n🌐 Scraping {directory_name.upper()}: {search_url}")
            
            # Scrape the directory page with Playwright
            content = await playwright_scrape_directory_url(search_url)
            
            if not content:
                logger.warning(f"No content extracted from {directory_name}")
                continue
            
            # Use AI to extract companies from the content
            companies = ai_extract_companies_from_content(content, location, search_url)
            
            if not companies:
                logger.warning(f"No companies extracted from {directory_name}")
                continue
            
            # Deduplicate and add companies
            added_count = 0
            for company in companies:
                if len(all_companies) >= target:
                    break
                
                # Skip duplicates
                name_key = company.name.lower().strip()
                if name_key in seen_names:
                    continue
                
                website_key = company.website.lower().strip() if company.website else ""
                if website_key and website_key in seen_websites:
                    continue
                
                # Add to collections
                seen_names.add(name_key)
                if website_key:
                    seen_websites.add(website_key)
                
                all_companies.append(company)
                added_count += 1
                
                logger.info(f"✅ Added: {company.name}")
                if company.phones:
                    logger.info(f"   📞 {company.phones[0]}")
                if company.website:
                    logger.info(f"   🌐 {company.website}")
            
            logger.info(f"📊 Added {added_count} companies from {directory_name}")
            logger.info(f"📈 Total companies: {len(all_companies)}/{target}")
            
            # Respectful delay between directories
            if len(all_companies) < target:
                logger.info("⏳ Waiting 5 seconds before next directory...")
                await asyncio.sleep(5)
            
        except Exception as e:
            logger.error(f"❌ Error processing {directory_name}: {e}")
            continue
    
    logger.info(f"\n🎉 ULTIMATE PLAYWRIGHT HYBRID COMPLETED")
    logger.info(f"📊 Final result: {len(all_companies)} companies extracted")
    
    # Clean contact information for all records
    if all_companies:
        logger.info("🧹 Cleaning contact information...")
        all_companies = clean_contact_information(all_companies)
    
    return all_companies[:target]

def run_playwright_hybrid_mode(country: str, state: str, city: str, target: int = 100) -> List[CompanyRecord]:
    """
    Synchronous wrapper for the async Playwright hybrid scraper
    """
    try:
        # Run the async scraper
        return asyncio.run(playwright_scrape_business_directories(country, state, city, target))
    except Exception as e:
        logger.error(f"Error in Playwright hybrid mode: {e}")
        return []


# ------------- CLI -------------
def parse_args(argv: Optional[List[str]] = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Scrape scrap/metal company data and export Excel/CSV/JSON")
    p.add_argument("--country", type=str, help="Country name (e.g., United States)")
    p.add_argument("--state", type=str, default="", help="State/Region")
    p.add_argument("--city", type=str, default="", help="City")
    p.add_argument("-n", "--num", type=int, default=200, help="Target number of companies")
    p.add_argument("--per-query-pages", type=int, default=1, help="Search pages per query")
    p.add_argument("--engines", choices=["bing", "ddg", "both"], default="bing", help="Which search engine(s) to use")
    p.add_argument("--interactive", action="store_true", help="Force interactive menu even if flags are provided")
    p.add_argument("--no-enrich", action="store_true", help="Disable OpenAI enrichment")
    p.add_argument("--enrich", choices=["live", "batch"], default="live", help="Enrichment mode")
    p.add_argument("--enrich-limit", type=int, default=50, help="Max records to enrich in live mode")
    p.add_argument("--ai-mode", choices=["basic", "full-live", "full-batch", "full-bulk-live", "full-direct-live", "ai-search-live", "ai-direct-list-live", "ai-direct-list-batch", "ai-direct-paged-live", "ai-100-optimized", "playwright-hybrid"], default="basic", help="AI modes: full-* extract from sites; full-direct-live generates all in one; ai-search-live uses AI to pick URLs from SERPs; ai-direct-list-* asks AI to return official URLs first, then extract (live or batch); ai-direct-paged-live asks GPT for exactly K companies per call and repeats until N; ai-100-optimized is specifically tuned for reliably generating exactly 100 companies with comprehensive logging; playwright-hybrid uses Playwright to scrape real business directories then AI to extract data")
    p.add_argument("--direct-list-batch-size", type=int, default=20, help="Number of official URLs to request from GPT per call in ai-direct-list-* modes")
    p.add_argument("--direct-list-max-calls", type=int, default=10, help="Maximum GPT calls in ai-direct-list-* modes to reach N after filtering/verification")
    p.add_argument("--direct-page-size", type=int, default=20, help="Companies per GPT call in ai-direct-paged-live mode")
    p.add_argument("--bulk-size", type=int, default=20, help="In full-bulk-live mode, number of sites per AI call")
    p.add_argument("--direct-bulk-size", type=int, default=100, help="In full-direct-live mode, companies per AI call")
    p.add_argument("--direct-max-calls", type=int, default=3, help="In full-direct-live mode, maximum AI calls to top-up until target")
    p.add_argument("--prepare-batch-tasks", metavar="JSON_PATH", help="Create NDJSON batch tasks from JSON results")
    p.add_argument("--submit-batch", metavar="NDJSON_PATH", help="Submit NDJSON to OpenAI Batch API")
    p.add_argument("--fetch-batch", metavar="BATCH_ID", help="Fetch results for a submitted batch and output to stdout")
    p.add_argument("--merge-from", metavar="JSON_PATH", help="When fetching batch, merge into existing JSON and re-export")
    return p.parse_args(argv)


def interactive_wizard() -> Tuple[str, str, str, int, int, str, str, int, str]:
    print("\n=== Scrap/Metal Company Scraper — Interactive Setup ===")
    # Step 1: scope
    print("\n1) Location scope")
    country = input("  Country [United States]: ").strip() or "United States"
    state = input("  State/Region [blank for all]: ").strip()
    city = input("  City [blank for all]: ").strip()

    # Step 2: size
    print("\n2) How many companies do you need?")
    try:
        num = int(input("  Target count [-n] [200]: ").strip() or "200")
    except Exception:
        num = 200

    # Step 3: search breadth
    print("\n3) Search breadth (more pages → more candidates)")
    print("  1) Narrow (1 page/query)\n  2) Medium (2 pages/query)\n  3) Broad (3 pages/query)")
    pq = (input("  Choose [1-3] (default 2): ").strip() or "2")
    per_query_pages = 2
    if pq in {"1", "2", "3"}:
        per_query_pages = int(pq)

    # Step 3b: search engine
    print("\n3b) Search engine")
    print("  1) Bing only (recommended)\n  2) DuckDuckGo only\n  3) Both")
    se = (input("  Choose [1-3] (default 1): ").strip() or "1")
    engines_choice = {"1": "bing", "2": "ddg", "3": "both"}.get(se, "bing")

    # Step 4: AI mode
    print("\n4) AI scraping mode")
    print("  1) basic      — local parsing, optional AI refine (cheapest/fastest)")
    print("  2) full-live  — AI extracts all fields now (most accurate, higher cost)")
    print("  3) full-batch — AI extracts all fields via Batch (cheapest at scale, 24h)")
    am = (input("  Choose [1-3] (default 1): ").strip() or "1")
    ai_mode = {"1": "basic", "2": "full-live", "3": "full-batch"}.get(am, "basic")

    # Step 5: enrichment (only for basic)
    enrich_choice = "none"
    enrich_limit = 0
    if ai_mode == "basic":
        print("\n5) Optional enrichment (materials/prices)")
        print("  1) live  — enrich top N now (fast, some cost)")
        print("  2) batch — prepare 24h Batch jobs (cheap at scale)")
        print("  3) none  — no AI (free)")
        em = (input("  Choose [1-3] (default 1): ").strip() or "1")
        enrich_choice = {"1": "live", "2": "batch", "3": "none"}.get(em, "live")
        if enrich_choice == "live":
            try:
                enrich_limit = int(input(f"  Enrich how many? [default {num}]: ").strip() or str(num))
            except Exception:
                enrich_limit = num
        else:
            enrich_limit = 0

    # Summary
    print("\nSummary:")
    loc = ", ".join(x for x in [city, state, country] if x)
    print(f"  Location: {loc or country}")
    print(f"  Target: {num}  | Search pages/query: {per_query_pages}")
    print(f"  AI mode: {ai_mode}")
    if ai_mode == "basic":
        print(f"  Enrichment: {enrich_choice}  | Enrich limit: {enrich_limit}")
    confirm = input("Proceed? [Y/n]: ").strip().lower() or "y"
    if confirm not in {"y", "yes"}:
        return interactive_wizard()

    return country, state, city, num, per_query_pages, ai_mode, enrich_choice, enrich_limit, engines_choice


def main(argv: Optional[List[str]] = None) -> int:
    args = parse_args(argv)

    # Batch subcommands
    if args.prepare_batch_tasks:
        ndjson_path = os.path.splitext(args.prepare_batch_tasks)[0] + "_batch_tasks.ndjson"
        prepare_batch_tasks(args.prepare_batch_tasks, ndjson_path)
        return 0
    if args.submit_batch:
        bid = submit_batch(args.submit_batch)
        print(bid)
        return 0
    if args.fetch_batch:
        data = fetch_batch(args.fetch_batch)
        print(json.dumps(data, indent=2))
        # Optional merge to existing JSON and re-export
        if args.merge_from and data.get("status") == "completed":
            with open(args.merge_from, "r", encoding="utf-8") as f:
                rows = json.load(f)
            # Convert to CompanyRecord and merge
            records = [CompanyRecord(**{k: it.get(k, "") for k in CompanyRecord().__dict__.keys()}) for it in rows]
            merge_batch_enrichment(records, data)
            paths = export_records(records)
            logger.info(f"Merged and re-exported → {paths['xlsx']}")
        return 0

    country = args.country
    state = args.state
    city = args.city
    num = args.num
    enrich_choice = args.enrich
    enrich_limit = args.enrich_limit
    ai_mode = args.ai_mode
    engines_choice = args.engines
    if args.interactive or not country:
        country, state, city, num, per_query_pages, ai_mode, enrich_choice, enrich_limit, engines_choice = interactive_wizard()
        args.per_query_pages = per_query_pages
        args.engines = engines_choice

    if ai_mode == "ai-100-optimized":
        loc = ", ".join(x for x in [city, state, country] if x)
        logger.info(f"Using AI-100-OPTIMIZED mode for {loc or country}")
        records = ai_generate_100_companies_optimized(country, state, city)
        if not records:
            logger.error("AI-100-OPTIMIZED mode returned no companies.")
            return 1
        paths = export_records(records)
        logger.info(f"Done. Exported {len(records)} records → {paths['xlsx']}")
        return 0

    if ai_mode == "playwright-hybrid":
        loc = ", ".join(x for x in [city, state, country] if x)
        logger.info(f"🎭 Using PLAYWRIGHT-HYBRID mode for {loc or country}")
        logger.info("🎯 This mode scrapes real business directories then uses AI to extract data")
        
        if not HAS_PLAYWRIGHT:
            logger.error("❌ Playwright not available!")
            logger.error("📋 Install with: pip install playwright && python -m playwright install chromium")
            return 1
        
        records = run_playwright_hybrid_mode(country, state, city, num)
        if not records:
            logger.error("❌ Playwright-hybrid mode returned no companies.")
            return 1
        
        paths = export_records(records)
        logger.info(f"🎉 Done! Exported {len(records)} records → {paths['xlsx']}")
        return 0

    # Default mode is now hybrid directory scraping
    if not ai_mode or ai_mode == "basic":
        loc = ", ".join(x for x in [city, state, country] if x)
        logger.info(f"🔄 Using HYBRID DIRECTORY SCRAPING mode for {loc or country}")
        logger.info("📂 Step 1: Discovering business directory URLs...")
        
        engines = ["bing"] if args.engines == "bing" else (["ddg"] if args.engines == "ddg" else ["bing", "ddg"])
        t0 = time.time()
        directory_urls = discover_business_directory_urls(
            country, state or None, city or None,
            per_query_pages=args.per_query_pages,
            engines=engines,
            ai_discovery=False
        )
        logger.info(f"Directory discovery took {time.time()-t0:.1f}s")
        logger.info(f"Found {len(directory_urls)} business directory URLs")
        
        if not directory_urls:
            logger.warning("No business directory URLs found.")
            return 1
        
        # Extract companies from each directory page
        logger.info("🤖 Step 2: Extracting company data using AI...")
        all_companies = []
        seen_names = set()
        seen_websites = set()
        processed_urls = 0
        
        for url in directory_urls[:30]:  # Limit to first 30 URLs to avoid rate limits
            try:
                logger.info(f"Processing directory page {processed_urls + 1}/{min(len(directory_urls), 30)}: {url}")
                companies = extract_companies_from_directory_page(url)
                processed_urls += 1
                
                for company in companies:
                    # Deduplicate
                    name_key = company.name.lower().strip()
                    website_key = company.website.lower().strip() if company.website else ""
                    
                    if name_key in seen_names or (website_key and website_key in seen_websites):
                        continue
                        
                    seen_names.add(name_key)
                    if website_key:
                        seen_websites.add(website_key)
                        
                    all_companies.append(company)
                    logger.info(f"✓ Found company #{len(all_companies)}: {company.name}")
                    
                    if len(all_companies) >= num:
                        break
                
                if len(all_companies) >= num:
                    break
                    
            except Exception as e:
                logger.error(f"Error processing directory {url}: {e}")
                continue
        
        logger.info(f"🎉 Extracted {len(all_companies)} companies from {processed_urls} directory pages")
        if all_companies:
            paths = export_records(all_companies)
            logger.info(f"Done. Exported {len(all_companies)} records → {paths['xlsx']}")
        else:
            logger.error("No companies found!")
            return 1
        return 0

    if ai_mode == "full-direct-live":
        loc = ", ".join(x for x in [city, state, country] if x)
        logger.info(f"Requesting {num} companies directly from OpenAI for {loc or country} …")
        records: List[CompanyRecord] = []
        seen_hosts: set[str] = set()
        seen_names: set[str] = set()
        attempts = 0
        while len(records) < num and attempts < max(1, args.direct_max_calls):
            attempts += 1
            batch = ai_generate_companies_direct(
                country, state, city,
                max(0, num - len(records)),
                bulk_size=max(20, min(200, args.direct_bulk_size)),
                exclude_hosts=seen_hosts,
                exclude_names=seen_names,
            )
            # Deduplicate by host
            for rec in batch:
                host = urlparse(rec.website).netloc.lower() if rec.website else ""
                if host and host in seen_hosts:
                    continue
                if host:
                    seen_hosts.add(host)
                keyname = rec.name.strip().lower()
                if keyname and keyname in seen_names:
                    continue
                if keyname:
                    seen_names.add(keyname)
                records.append(rec)
                if len(records) >= num:
                    break
            if not batch:
                break
        if not records:
            logger.error("OpenAI returned no companies.")
            return 1
        paths = export_records(records)
        logger.info(f"Done. Exported {len(records)} records → {paths['xlsx']}")
        return 0

    if ai_mode == "ai-direct-paged-live":
        # Ask GPT for exactly K real companies per call, repeat up to max_calls or until N is met
        page_size = max(5, min(50, args.direct_page_size))
        max_calls = 5  # hard cap as requested
        records: List[CompanyRecord] = []
        seen_hosts: set[str] = set()
        seen_names: set[str] = set()
        for i in range(max_calls):
            remaining = max(0, num - len(records))
            if remaining <= 0:
                break
            ask = min(page_size, remaining)
            logger.info(f"ai-paged call {i+1}/{max_calls}: asking={ask}, have={len(records)}")
            batch = ai_generate_companies_direct(country, state, city, ask, bulk_size=ask, exclude_hosts=seen_hosts, exclude_names=seen_names)
            logger.info(f"ai-paged call {i+1}: got={len(batch)}")
            # Dedup and accumulate
            for rec in batch:
                host = urlparse(rec.website).netloc.lower() if rec.website else ""
                if host and host in seen_hosts:
                    continue
                if host:
                    seen_hosts.add(host)
                namekey = rec.name.strip().lower()
                if namekey and namekey in seen_names:
                    continue
                if namekey:
                    seen_names.add(namekey)
                records.append(rec)
                if len(records) >= num:
                    break
        if not records:
            logger.error("AI direct paged mode returned no companies.")
            return 1
        paths = export_records(records)
        logger.info(f"Done. Exported {len(records)} records → {paths['xlsx']}")
        return 0

    if ai_mode in {"ai-direct-list-live", "ai-direct-list-batch"}:
        # 1) Ask AI for a big list of official URLs fast
        logger.info("AI direct list: requesting official URLs…")
        seen_hosts: set[str] = set()
        urls: List[str] = []
        # Request exactly batch-size URLs per call, up to max calls or until we have >= N verified URLs
        per_call = max(5, min(50, args.direct_list_batch_size))
        max_calls = max(1, min(25, args.direct_list_max_calls))
        for call_idx in range(max_calls):
            new_urls = ai_generate_official_urls(country, state, city, target=per_call, exclude_hosts=seen_hosts)
            logger.info(f"ai-url-list call {call_idx+1}/{max_calls}: accepted={len(new_urls)}, total_urls={len(urls)}")
            for u in new_urls:
                h = urlparse(u).netloc.lower()
                if h in seen_hosts:
                    continue
                seen_hosts.add(h)
                urls.append(u)
            if len(urls) >= num:
                break
        if len(urls) > num * 3:
            urls = urls[: num * 3]
        if not urls:
            logger.error("AI direct list returned 0 URLs.")
            return 1
        # 2) Extract either live (parallel) or via Batch
        if ai_mode == "ai-direct-list-live":
            records = ai_full_collect(urls, country, target=num)
            if not records:
                logger.error("No records extracted in ai-direct-list-live mode.")
                return 1
            paths = export_records(records)
            logger.info(f"Done. Exported {len(records)} records → {paths['xlsx']}")
            return 0
        else:
            paths = export_records([], output_dir="output")
            ndjson_path = os.path.splitext(paths["json"])[0] + "_direct_list_full_batch.ndjson"
            prepare_batch_full_tasks(urls, country, ndjson_path)
            logger.info(f"Prepared Batch tasks: {ndjson_path}. Submit with --submit-batch and fetch via --fetch-batch.")
            return 0

    if ai_mode == "ai-search-live":
        # AI proposes official URLs from SERPs; verify and EXTRACT via Batch (economical)
        logger.info("AI-driven search (economical): proposing URLs, preparing full extraction Batch…")
        engines = ["bing"]
        t0 = time.time()
        candidates = discover_candidates(
            country, state or None, city or None,
            per_query_pages=max(3, args.per_query_pages),
            engines=engines,
            ai_discovery=True,
        )
        logger.info(f"AI-search discovery took {time.time()-t0:.1f}s; {len(candidates)} candidates")
        if not candidates:
            logger.error("No candidates discovered.")
            return 1
        # Prepare Batch payloads for full extraction (one request per site, computed offline by OpenAI)
        paths = export_records([], output_dir="output")
        ndjson_path = os.path.splitext(paths["json"])[0] + "_ai_search_full_batch.ndjson"
        prepare_batch_full_tasks(candidates, country, ndjson_path)
        logger.info(f"Prepared Batch tasks: {ndjson_path}. Submit with --submit-batch and fetch via --fetch-batch.")
        return 0

    logger.info(f"Discovering candidates for {', '.join(x for x in [city, state, country] if x)} …")
    engines = ["bing"] if args.engines == "bing" else (["ddg"] if args.engines == "ddg" else ["bing", "ddg"])  # prefer Bing first
    t0 = time.time()
    # If full AI mode, let AI also guide discovery selection from SERPs
    ai_discovery = ai_mode in {"full-live", "full-batch", "full-bulk-live"}
    candidates = discover_candidates(
        country, state or None, city or None,
        per_query_pages=args.per_query_pages,
        engines=engines,
        ai_discovery=ai_discovery,
    )
    logger.info(f"Discovery took {time.time()-t0:.1f}s")
    logger.info(f"Found {len(candidates)} candidate URLs. Extracting up to {num} valid companies…")
    if candidates:
        # Show a few sample hosts to verify engines aren't being re-logged
        sample_hosts = ", ".join(sorted({urlparse(c).netloc for c in candidates[:12]}))
        logger.info(f"Sample hosts: {sample_hosts}")
    if not candidates:
        logger.warning("No candidates from DDG/Bing. Trying a broader query strategy…")
        # 1) Remove city, try again
        if city:
            more = discover_candidates(country, state or None, None, per_query_pages=max(2, args.per_query_pages))
            candidates.extend(more)
        # 2) Remove state, try again
        if not candidates and state:
            more = discover_candidates(country, None, None, per_query_pages=max(2, args.per_query_pages))
            candidates.extend(more)
        # 3) As a last resort, increase pages
        if not candidates:
            more = discover_candidates(country, None, None, per_query_pages=3)
            candidates.extend(more)
        candidates = list(dict.fromkeys(candidates))
        logger.info(f"Broadened search found {len(candidates)} candidates.")

    # AI full-scrape modes
    if ai_mode in {"full-live", "full-batch", "full-bulk-live", "full-direct-live"}:
        if ai_mode == "full-live":
            records = ai_full_collect(candidates, country, target=num)
        elif ai_mode == "full-bulk-live":
            records = ai_full_collect_bulk(candidates, country, target=num, bulk_size=max(5, min(50, args.bulk_size)))
        elif ai_mode == "full-direct-live":
            # No discovery: ask AI to produce the companies directly
            candidates = []
            records = ai_generate_companies_direct(country, state, city, num, bulk_size=max(20, min(200, args.direct_bulk_size)))
        else:
            # Prepare full extraction batch and exit
            paths = export_records([], output_dir="output")
            ndjson_path = os.path.splitext(paths["json"])[0] + "_full_batch_tasks.ndjson"
            prepare_batch_full_tasks(candidates, country, ndjson_path)
            logger.info("Full extraction batch tasks prepared. Submit with --submit-batch and fetch later.")
            return 0
    else:
        records = collect_records(candidates, country, target=num)
    if not records:
        logger.error("No valid companies found. Try broader scope or increase search pages.")
        return 1

    # Optional enrichment
    if ai_mode == "basic" and (not args.no_enrich and enrich_choice != "none"):
        if enrich_choice == "live":
            try:
                enrich_live(records, limit=enrich_limit)
            except Exception as e:
                logger.warning(f"Enrichment skipped: {e}")
        elif enrich_choice == "batch":
            # Save current JSON and emit tasks for batch submission
            paths = export_records(records)
            ndjson_path = os.path.splitext(paths["json"])[0] + "_batch_tasks.ndjson"
            prepare_batch_tasks(paths["json"], ndjson_path)
            logger.info("Batch tasks prepared. Submit with --submit-batch and later fetch with --fetch-batch.")

    paths = export_records(records)
    logger.info(f"Done. Exported {len(records)} records → {paths['xlsx']}")
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except KeyboardInterrupt:
        print("\nInterrupted.")
        sys.exit(130)

