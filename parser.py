#!/usr/bin/env python3
import os
import sys
import json
import random
import re
import logging
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor
from urllib.parse import quote_plus, urlparse

from ddgs import DDGS
import requests
from bs4 import BeautifulSoup
import phonenumbers
import pandas as pd
from requests.adapters import HTTPAdapter, Retry

# ─────────────── Logging Setup ───────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s: %(message)s"
)
logger = logging.getLogger("scrap")

# ─────────────── Fields & Keywords ───────────────
FIELDS = [
    'name','phone','email','website',
    'address','city','state','country',
    'description','materials','services'
]

MATS = ['copper','aluminum','steel','iron','brass','battery','wire','cable']
SVCS = ['pickup','container','demolition','processing','sorting','weighing']

phone_rx = re.compile(r'\(?\d{3}\)?[-.\s]?\d{3}[-.\s]?\d{4}')
email_rx = re.compile(r'\b[A-Za-z0-9._%+\-]+@[A-Za-z0-9.\-]+\.[A-Za-z]{2,}\b')

# ─────────────── URL Filtering ───────────────
BLACKLIST = (
    "youtube.com","amazon.com","quora.com","yelp.com",
    "wikipedia.org","facebook.com","twitter.com",
    "medium.com","dailymotion.com"
)
def allowed_url(u: str) -> bool:
    nl = urlparse(u).netloc.lower()
    if any(domain in nl for domain in BLACKLIST):
        return False
    return any(tok in u.lower() for tok in ("scrap","metal","recycl"))

# ─────────────── DuckDuckGo Search ───────────────
def ddg_search(query: str, region: str, maxr: int) -> list[str]:
    links = []
    with DDGS() as ddgs:
        for r in ddgs.text(f"{query} {region}", max_results=maxr):
            href = r.get("href")
            if href and href.startswith("http"):
                links.append(href)
    return links

# ─────────────── Bing Search ───────────────
session = requests.Session()
session.mount("https://", HTTPAdapter(max_retries=Retry(3, backoff_factor=0.3)))

def bing_search(query: str, region: str, pages: int = 1) -> list[str]:
    term = quote_plus(f"{query} {region}")
    found = []
    for p in range(pages):
        first = p * 10 + 1
        url = f"https://www.bing.com/search?q={term}&first={first}"
        r = session.get(url, headers={"User-Agent": random.choice([
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7)"
        ])}, timeout=10)
        if r.status_code != 200:
            break
        soup = BeautifulSoup(r.text, "html.parser")
        for a in soup.select("li.b_algo h2 a"):
            href = a.get("href")
            if href and href.startswith("http"):
                found.append(href)
    return found

# ─────────────── Search Terms & States ───────────────
SEARCH_TERMS = [
    "scrap metal recycling center",
    "scrap yard",
    "metal recycling services",
    "scrap metal buyers",
    "scrap metal collection"
]

US_STATES = [
    "Alabama","Alaska","Arizona","Arkansas","California","Colorado","Connecticut",
    "Delaware","Florida","Georgia","Hawaii","Idaho","Illinois","Indiana","Iowa",
    "Kansas","Kentucky","Louisiana","Maine","Maryland","Massachusetts","Michigan",
    "Minnesota","Mississippi","Missouri","Montana","Nebraska","Nevada",
    "New Hampshire","New Jersey","New Mexico","New York","North Carolina",
    "North Dakota","Ohio","Oklahoma","Oregon","Pennsylvania","Rhode Island",
    "South Carolina","South Dakota","Tennessee","Texas","Utah","Vermont",
    "Virginia","Washington","West Virginia","Wisconsin","Wyoming"
]

# ─────────────── Combined Multi-Search ───────────────
def multi_search(country: str, target: int) -> list[str]:
    candidates: list[str] = []
    # how many results per term/region
    per_bucket = max(target // (len(SEARCH_TERMS) * (51 if country.lower()=="united states" else 1)), 5)

    # 1) If USA, also iterate each state
    if country.lower() == "united states":
        for state in US_STATES:
            for term in SEARCH_TERMS:
                candidates += ddg_search(term, state, per_bucket)
                candidates += bing_search(term, state, pages=1)
    # 2) Always do an overall country search
    for term in SEARCH_TERMS:
        candidates += ddg_search(term, country, per_bucket)
        candidates += bing_search(term, country, pages=1)

    # dedupe + filter
    unique = list(dict.fromkeys(candidates))
    return [u for u in unique if allowed_url(u)]

# ─────────────── Page Extractor ───────────────
def extract(url: str, country: str) -> dict[str,str] | None:
    rec = dict.fromkeys(FIELDS, "")
    rec['website'] = url
    try:
        r = requests.get(url, timeout=10, headers={
            "User-Agent": random.choice([
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
                "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7)"
            ])
        })
        if r.status_code != 200:
            return None

        txt = r.text.lower()
        if not any(k in txt for k in ("scrap","recycl","metal")):
            return None

        soup = BeautifulSoup(r.text, "lxml")

        # Name
        if soup.h1:
            rec['name'] = soup.h1.get_text(strip=True)
        elif soup.title:
            rec['name'] = soup.title.get_text(strip=True)

        # Phone
        region_code = country[:2].upper()
        for m in phonenumbers.PhoneNumberMatcher(r.text, region_code):
            if phonenumbers.is_valid_number(m.number):
                rec['phone'] = phonenumbers.format_number(
                    m.number, phonenumbers.PhoneNumberFormat.NATIONAL
                )
                break
        if not rec['phone']:
            m = phone_rx.search(r.text)
            if m:
                rec['phone'] = m.group(0)

        # Email
        m = email_rx.search(r.text)
        if m:
            rec['email'] = m.group(0)

        # Address / City / State
        addr = soup.select_one("[itemprop=address]")
        if addr:
            full = addr.get_text(" ", strip=True)
            rec['address'] = full
            mm = re.search(r'(.+),\s*([A-Z]{2})', full)
            if mm:
                rec['city'], rec['state'] = mm.group(1).strip(), mm.group(2)

        # Description
        meta = soup.find("meta", {"name":"description"})
        if meta and meta.get("content"):
            rec['description'] = meta["content"][:300]
        else:
            rec['description'] = soup.get_text()[:300]

        # Materials & Services
        rec['materials'] = ", ".join(k for k in MATS if k in txt)
        rec['services']  = ", ".join(k for k in SVCS if k in txt)

        rec['country'] = country
        return rec

    except Exception:
        return None

# ─────────────── Collector ───────────────
def collect_valid(candidates: list[str], country: str, target: int) -> list[dict]:
    results: list[dict] = []
    seen_urls = set(candidates)
    idx = 0

    def refill_if_needed():
        # if we run out of candidates, grab more
        extra = multi_search(country, target * 2)
        for u in extra:
            if u not in seen_urls:
                seen_urls.add(u)
                candidates.append(u)

    while len(results) < target:
        if idx >= len(candidates):
            refill_if_needed()
            if idx >= len(candidates):
                break
        url = candidates[idx]
        idx += 1
        rec = extract(url, country)
        if rec and (rec['phone'] or rec['email']):
            results.append(rec)

    return results[:target]

# ─────────────── Main ───────────────
if __name__ == "__main__":
    country = input("Country? [United States] ").strip() or "United States"
    tgt     = input("How many companies? [200] ").strip()
    try:
        target = int(tgt)
    except ValueError:
        target = 200

    logger.info(f"➤ Gathering candidate URLs for '{country}'…")
    candidates = multi_search(country, target)
    logger.info(f"→ {len(candidates)} candidates fetched. Parsing up to {target} valid entries…")

    results = collect_valid(candidates, country, target)
    if not results:
        logger.error("❌ No valid scrap-metal entries found.")
        sys.exit(1)

    df = pd.DataFrame(results, columns=FIELDS)
    od = "output"
    os.makedirs(od, exist_ok=True)
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")

    csv_path  = os.path.join(od, f"scrap_{ts}.csv")
    xlsx_path = os.path.join(od, f"scrap_{ts}.xlsx")
    json_path = os.path.join(od, f"scrap_{ts}.json")

    df.to_csv(csv_path, index=False)
    df.to_excel(xlsx_path, index=False)
    with open(json_path, "w", encoding="utf-8") as f:
        json.dump(results, f, indent=2)

    logger.info(f"✔ Done! Exported {len(results)} records to `{od}/`")
