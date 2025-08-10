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

import pandas as pd
import phonenumbers
import requests
from bs4 import BeautifulSoup
from requests.adapters import HTTPAdapter, Retry


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
        "You are selecting official websites of scrap/metal recycling companies. "
        "From the list, return ONLY up to N URLs that are likely the company's own site (exclude social, directories, maps).\n"
        f"Query: {query} in {region_str}\nN={limit}\nList:\n" + "\n".join(lines)
    )
    try:
        rsp = client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[
                {"role": "system", "content": "Return a JSON with {urls: [..]}."},
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
        pr = urlparse(url)
        host = pr.netloc.lower()
        # DuckDuckGo wrapped
        if host.endswith("duckduckgo.com") and pr.path.startswith("/l/"):
            qs = parse_qs(pr.query)
            target = qs.get("uddg", [None])[0]
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
        # Already normal
        return url
    except Exception:
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


def discover_candidates(country: str, state: Optional[str], city: Optional[str], *, per_query_pages: int = 1, engines: Optional[List[str]] = None, ai_discovery: bool = False) -> List[str]:
    queries = build_queries(country, state, city)
    candidates: List[str] = []
    engines = [e.strip().lower() for e in (engines or ["bing"]) if e.strip()]
    for q in queries:
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
    # Deduplicate and filter
    normalized: List[str] = []
    for raw in list(dict.fromkeys(candidates)):
        norm = normalize_candidate_url(raw) or ""
        if not norm:
            continue
        host = urlparse(norm).netloc.lower()
        if any(host == se or host.endswith('.' + se) for se in SEARCH_ENGINE_DOMAINS):
            # Try to follow redirect to leave search domain
            norm = normalize_candidate_url(follow_redirect(norm)) or norm
        # Final: skip if still search domain or obviously non-business (maps, translate)
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
    # Filter out search engine or blacklisted links, and prefer likely company pages
    filtered = [u for u in normalized if allowed_url(u)]
    if not filtered:
        # As an escape hatch, keep a small sample of unfiltered normalized links to avoid zero-results
        filtered = normalized[:30]
    def score(url: str) -> int:
        s = 0
        low = url.lower()
        # prefer .com/.net/.org and non-directory listings
        if any(t in low for t in ["about", "contact", "services", "materials", "prices", "recycling", "scrap"]):
            s += 2
        if not any(t in low for t in ["yelp.com", "yellowpages", "directory", "listings", "tripadvisor", "facebook.com"]):
            s += 1
        # shorter urls are often homepages
        s += max(0, 50 - len(low)) // 10
        return s
    filtered.sort(key=score, reverse=True)
    logger.info(f"Discovery summary → raw={len(candidates)}, normalized={len(normalized)}, filtered={len(filtered)}")
    # Final dedupe
    return list(dict.fromkeys(filtered))


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
        futures = [pool.submit(work, u) for u in candidates]
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
def ai_generate_companies_direct(country: str, state: str, city: str, target: int, *, bulk_size: int = 100) -> List[CompanyRecord]:
    """Ask OpenAI ONCE to produce a list of companies and their info directly (no crawling)."""
    try:
        client = _get_openai_client()
    except Exception as e:
        logger.error(f"AI client unavailable: {e}")
        return []

    ask = min(max(1, target), max(10, bulk_size))
    prompt = (
        "Produce a JSON array 'companies' of up to N scrap/metal recycling companies with fields: "
        "name, website, street_address, city, region, postal_code, country, phones (array), emails (array), whatsapp (array), "
        "social_links (array), opening_hours, materials (array), material_prices (array of strings), description.\n"
        "Return only companies that are verifiable and include at least one contact (phone/email/social). If unsure, omit the company.\n"
        "If fewer than N are confidently available, return as many as you can in ONE response.\n"
        f"Location: country={country}; state={state}; city={city}; N={ask}"
    )
    try:
        rsp = client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[
                {"role": "system", "content": "Return a single JSON object with {companies: [...]} and do not include any text outside JSON."},
                {"role": "user", "content": prompt},
            ],
            temperature=0.1,
            response_format={"type": "json_object"},
        )
    except Exception as e:
        logger.error(f"OpenAI error: {e}")
        return []

    try:
        data = json.loads(rsp.choices[0].message.content or "{}")
    except Exception:
        data = {}
    companies = data.get("companies")
    if not isinstance(companies, list):
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
        # Keep only entries with at least one contact and a plausible website
        host = urlparse(rec.website).netloc.lower() if rec.website else ""
        if (rec.phones or rec.emails or rec.social_links):
            if host and host in seen:
                continue
            if host:
                seen.add(host)
            results.append(rec)
            if len(results) >= target:
                break
    return results[:target]


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
    p.add_argument("--ai-mode", choices=["basic", "full-live", "full-batch", "full-bulk-live", "full-direct-live"], default="basic", help="'basic' uses HTML parsing + optional enrich; 'full-*' lets AI extract all fields; 'full-direct-live' asks AI to generate companies directly (no scraping)")
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

    if ai_mode == "full-direct-live":
        loc = ", ".join(x for x in [city, state, country] if x)
        logger.info(f"Requesting {num} companies directly from OpenAI for {loc or country} …")
        records: List[CompanyRecord] = []
        seen_hosts: set[str] = set()
        attempts = 0
        while len(records) < num and attempts < max(1, args.direct_max_calls):
            attempts += 1
            batch = ai_generate_companies_direct(
                country, state, city,
                max(0, num - len(records)),
                bulk_size=max(20, min(200, args.direct_bulk_size)),
            )
            # Deduplicate by host
            for rec in batch:
                host = urlparse(rec.website).netloc.lower() if rec.website else ""
                if host and host in seen_hosts:
                    continue
                if host:
                    seen_hosts.add(host)
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

