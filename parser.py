#!/usr/bin/env python3
import os
import json
import random
import re
import logging
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor
from ddgs import DDGS
import requests
from bs4 import BeautifulSoup
import phonenumbers
import pandas as pd

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

# ─────────────── 1) DuckDuckGo Search ───────────────
def duck_search(query, country="United States", maxr=100):
    links = []
    with DDGS() as ddgs:
        # pass the combined query as the first positional argument:
        for r in ddgs.text(f"{query} {country}", max_results=maxr):
            href = r.get("href")
            if href and href.startswith("http"):
                links.append(href)
    # dedupe while preserving order
    return list(dict.fromkeys(links))

# ─────────────── 2) Page Extraction ───────────────
def extract(url):
    rec = dict.fromkeys(FIELDS, "")
    rec['website'] = url
    try:
        r = requests.get(
            url, timeout=10,
            headers={"User-Agent": random.choice([
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
                "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7)"
            ])}
        )
        if r.status_code != 200:
            return None

        text_lower = r.text.lower()
        # skip non-scrap sites
        if not any(k in text_lower for k in ["scrap","recycl","metal"]):
            return None

        soup = BeautifulSoup(r.text, "lxml")

        # ─── Name ───
        if soup.h1:
            rec['name'] = soup.h1.get_text(strip=True)
        elif soup.title:
            rec['name'] = soup.title.get_text(strip=True)

        # ─── Phone ───
        for m in phonenumbers.PhoneNumberMatcher(r.text, "US"):
            if phonenumbers.is_valid_number(m.number):
                rec['phone'] = phonenumbers.format_number(
                    m.number,
                    phonenumbers.PhoneNumberFormat.NATIONAL
                )
                break
        if not rec['phone']:
            m = phone_rx.search(r.text)
            rec['phone'] = m.group(0) if m else ""

        # ─── Email ───
        m = email_rx.search(r.text)
        rec['email'] = m.group(0) if m else ""

        # ─── Address / City / State ───
        addr = soup.select_one("[itemprop=address]")
        if addr:
            full = addr.get_text(" ", strip=True)
            rec['address'] = full
            m = re.search(r'(.+),\s*([A-Z]{2})', full)
            if m:
                rec['city'], rec['state'] = m.group(1).strip(), m.group(2)

        # ─── Description ───
        meta = soup.find("meta", {"name":"description"})
        rec['description'] = (
            meta["content"] if meta and meta.get("content") else soup.get_text()
        )[:300]

        # ─── Materials & Services ───
        rec['materials'] = ", ".join(
            [k for k in MATS if k in text_lower]
        )
        rec['services'] = ", ".join(
            [k for k in SVCS if k in text_lower]
        )

        rec['country'] = "United States"
        return rec

    except Exception:
        return None

# ─────────────── 3) Main ───────────────
if __name__ == "__main__":
    # 1) search
    urls = duck_search("scrap metal recycling center", maxr=100)
    logger.info(f"Found {len(urls)} candidate links, now parsing…")

    # 2) parse in parallel
    results = []
    with ThreadPoolExecutor(max_workers=8) as pool:
        for rec in pool.map(extract, urls):
            if rec and (rec['phone'] or rec['email']):
                results.append(rec)

    if not results:
        logger.warning("No valid scrap-metal entries found.")
        sys.exit(1)

    # 3) export
    df = pd.DataFrame(results)[FIELDS]
    od = "output"
    os.makedirs(od, exist_ok=True)
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")

    csv_path = f"{od}/scrap_{ts}.csv"
    xlsx_path = f"{od}/scrap_{ts}.xlsx"
    json_path = f"{od}/scrap_{ts}.json"

    df.to_csv(csv_path, index=False)
    df.to_excel(xlsx_path, index=False)
    with open(json_path, "w", encoding="utf-8") as f:
        json.dump(results, f, indent=2)

    logger.info(f"✔ Done! Exported {len(results)} records to `{od}/`")
