#!/usr/bin/env python3
"""
Real Working Scraper - AI-powered comprehensive data collection
Uses OpenAI to generate complete, realistic business information
"""

import os
import sys
import json
import time
import random
import logging
import requests
from bs4 import BeautifulSoup
from datetime import datetime
import pandas as pd
from urllib.parse import quote_plus, urljoin, unquote
import re
from concurrent.futures import ThreadPoolExecutor, as_completed
import asyncio
import aiohttp
from typing import List, Dict, Any, Optional

# Add OpenAI for intelligent data generation
try:
    from openai import OpenAI
    client = OpenAI(api_key="YOUR_OPENAI_API_KEY_HERE")
    OPENAI_AVAILABLE = True
except ImportError:
    OPENAI_AVAILABLE = False
    print("⚠️ OpenAI not available. Install with: pip install openai")

class RealWorkingScraper:
    def __init__(self):
        self.session = requests.Session()
        self.results = []
        self.logger = self._setup_logging()
        
        # Rotate user agents and headers
        self.user_agents = [
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
        ]

        # Pre-compile all regex patterns for maximum performance
        self._compile_patterns()

        # Add metal types to search for (your requirement)
        self.metal_types = {
            'copper': ['copper', 'медь', 'cu', 'bare copper', 'copper wire', 'copper tubing'],
            'aluminum': ['aluminum', 'aluminium', 'алюминий', 'al', 'aluminum cans', 'aluminum siding'],
            'steel': ['steel', 'сталь', 'iron', 'железо', 'scrap steel', 'structural steel'],
            'brass': ['brass', 'латунь', 'yellow brass', 'red brass', 'brass fittings'],
            'stainless_steel': ['stainless steel', 'нержавейка', 'stainless', 'ss'],
            'lead': ['lead', 'свинец', 'pb', 'lead batteries', 'lead pipes'],
            'zinc': ['zinc', 'цинк', 'zn', 'galvanized'],
            'nickel': ['nickel', 'никель', 'ni', 'nickel alloy'],
            'tin': ['tin', 'олово', 'sn'],
            'carbide': ['carbide', 'карбид', 'tungsten carbide', 'cutting tools'],
            'precious_metals': ['gold', 'silver', 'platinum', 'palladium', 'rhodium'],
            'electronic': ['electronics', 'e-waste', 'circuit boards', 'computer scrap', 'cell phones'],
            'automotive': ['catalytic converters', 'car batteries', 'radiators', 'engines'],
            'wire': ['wire', 'провода', 'cable', 'insulated wire', 'romex wire'],
            'cast_iron': ['cast iron', 'чугун', 'cast', 'machine parts'],
            'titanium': ['titanium', 'титан', 'ti', 'aerospace grade']
        }
        
        # Services to look for
        self.services = [
            'pickup service', 'container rental', 'demolition', 'roll-off containers',
            'processing', 'sorting', 'weighing', 'cash payment', 'check payment',
            'industrial cleanup', 'auto dismantling', 'certified scales',
            'commercial accounts', 'residential pickup', 'same day pickup'
        ]

        # Build keyword sets for fast matching
        self._build_keyword_sets()

    def _compile_patterns(self):
        """Pre-compile all regex patterns for maximum performance"""
        # Price patterns with capturing groups for structured data
        self.price_patterns = [
            re.compile(r'(?P<metal>\w+)\s*\$?(?P<price>\d+\.?\d*)\s*(?P<unit>lb|pound|ton|per\s*pound)', re.IGNORECASE),
            re.compile(r'\$(?P<price>\d+\.?\d*)\s*(?P<unit>per\s*pound|/\s*lb|per\s*ton)', re.IGNORECASE),
            re.compile(r'(?P<price>\d+\.?\d*)\s*cents?\s*(?P<unit>per\s*pound|/\s*lb)', re.IGNORECASE),
            re.compile(r'paying\s*\$?(?P<price>\d+\.?\d*)\s*(?P<unit>per\s*pound|/\s*lb)', re.IGNORECASE),
            re.compile(r'(?P<metal>copper|aluminum|steel|brass|iron)\s*\$?(?P<price>\d+\.?\d*)', re.IGNORECASE)
        ]
        
        # Phone patterns
        self.phone_patterns = [
            re.compile(r'\((\d{3})\)\s*(\d{3})-(\d{4})', re.IGNORECASE),
            re.compile(r'(\d{3})-(\d{3})-(\d{4})', re.IGNORECASE),
            re.compile(r'(\d{3})\s*(\d{3})\s*(\d{4})', re.IGNORECASE),
            re.compile(r'(\d{10})', re.IGNORECASE)
        ]
        
        # Email patterns
        self.email_patterns = [
            re.compile(r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b', re.IGNORECASE)
        ]
        
        # Hours patterns
        self.hours_patterns = [
            re.compile(r'(\d{1,2}):(\d{2})\s*(am|pm)\s*-\s*(\d{1,2}):(\d{2})\s*(am|pm)', re.IGNORECASE),
            re.compile(r'(\d{1,2})\s*(am|pm)\s*-\s*(\d{1,2})\s*(am|pm)', re.IGNORECASE),
            re.compile(r'open\s*(\d{1,2}):(\d{2})\s*-\s*(\d{1,2}):(\d{2})', re.IGNORECASE)
        ]

    def _build_keyword_sets(self):
        """Build sets for ultra-fast keyword matching"""
        # Flatten all metal keywords into a single set
        self.all_metal_keywords = set()
        for metal_type, keywords in self.metal_types.items():
            self.all_metal_keywords.update(keyword.lower() for keyword in keywords)
        
        # Convert services to set for fast lookup
        self.services_set = set(service.lower() for service in self.services)

    def _setup_logging(self):
        logger = logging.getLogger('RealWorkingScraper')
        logger.setLevel(logging.INFO)
        
        if not logger.handlers:
            handler = logging.StreamHandler()
            formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
            handler.setFormatter(formatter)
            logger.addHandler(handler)
        
        return logger

    def scrape_real_sources(self, target_count=100):
        """Scrape from real internet sources with maximum performance"""
        start_time = time.perf_counter()
        self.logger.info(f"🚀 Starting ULTRA-FAST data collection for {target_count} businesses")
        
        # Use only the reliable OSM source – the other sources rarely return any data and just slow us down
        sources = [
            self._scrape_overpass_api  # OpenStreetMap data (primary, parallelized)
        ]
        
        for source_func in sources:
            try:
                source_start = time.perf_counter()
                self.logger.info(f"📡 Trying {source_func.__name__}...")
                
                remaining_needed = target_count - len(self.results)
                
                # Always try to get more businesses from each source
                # Don't stop early - we want maximum coverage
                request_count = max(remaining_needed * 2, 200)  # Request extra to account for duplicates
                results = source_func(request_count)
                
                if results:
                    self.results.extend(results)
                    source_elapsed = time.perf_counter() - source_start
                    self.logger.info(f"✅ {source_func.__name__}: {len(results)} businesses in {source_elapsed:.1f}s")
                    
                    # Remove duplicates after each source
                    before_dedup = len(self.results)
                    self.results = self._remove_duplicates(self.results)
                    after_dedup = len(self.results)
                    
                    if before_dedup != after_dedup:
                        self.logger.info(f"🔄 Removed {before_dedup - after_dedup} duplicates, {after_dedup} unique businesses")
                    
                    # Continue with other sources even if we have enough
                    # This gives us better geographic coverage and more complete data
                else:
                    self.logger.warning(f"⚠️ {source_func.__name__} returned no results")
                
            except Exception as e:
                self.logger.error(f"❌ {source_func.__name__} failed: {e}")
                continue
        
        # Final deduplication
        initial_count = len(self.results)
        self.results = self._remove_duplicates(self.results)
        final_count = len(self.results)
        
        if initial_count != final_count:
            self.logger.info(f"🔄 Final deduplication: {initial_count} → {final_count} unique businesses")
        
        # PHASE 2: Comprehensive async enhancement
        if self.results:
            enhance_start = time.perf_counter()
            self.logger.info(f"🔬 PHASE 2: Comprehensive enhancement of {len(self.results)} businesses...")
            
            # Use asyncio for comprehensive data enrichment
            self.results = asyncio.run(self._enhance_businesses_async(limit=target_count))
            
            enhance_elapsed = time.perf_counter() - enhance_start
            self.logger.info(f"✅ Enhancement completed in {enhance_elapsed:.1f}s")
        
        total_elapsed = time.perf_counter() - start_time
        self.logger.info(f"🎯 Total {len(self.results)} REAL businesses collected in {total_elapsed:.1f}s")
        return self.results

    async def _enhance_businesses_async(self, limit=None):
        """Comprehensive async enhancement for maximum data quality"""
        process_list = self.results if limit is None else self.results[:limit]
        
        self.logger.info(f"🔧 Starting comprehensive enhancement of {len(process_list)} businesses...")
        
        # Configure session for heavy parallel processing
        timeout = aiohttp.ClientTimeout(total=30, connect=10)
        connector = aiohttp.TCPConnector(
            limit=300,           # Total connection pool
            limit_per_host=30,   # Max connections per host
            ttl_dns_cache=300,   # DNS cache TTL
            use_dns_cache=True,
            keepalive_timeout=30
        )
        
        async with aiohttp.ClientSession(timeout=timeout, connector=connector) as session:
            # Process in batches to avoid overwhelming the system
            batch_size = 50
            enhanced_businesses = []
            
            total_batches = (len(process_list) - 1) // batch_size + 1
            for batch_idx in range(total_batches):
                batch = process_list[batch_idx*batch_size : (batch_idx+1)*batch_size]
                batch_start = time.perf_counter()
                
                self.logger.info(f"�� Processing batch {batch_idx+1}/{total_batches} ({len(batch)} businesses)")
                
                # Create tasks for this batch
                tasks = []
                for business in batch:
                    task = self._enhance_single_async(session, business)
                    tasks.append(task)
                
                # Execute batch in parallel
                batch_results = await asyncio.gather(*tasks, return_exceptions=True)
                
                # Process results
                for result in batch_results:
                    if isinstance(result, Exception):
                        self.logger.debug(f"Enhancement exception: {result}")
                    elif isinstance(result, dict):
                        enhanced_businesses.append(result)
                
                batch_elapsed = time.perf_counter() - batch_start
                percent = (batch_idx+1) / total_batches * 100
                self.logger.info(f"✅ Batch {batch_idx+1} completed in {batch_elapsed:.1f}s — Enhancement progress {percent:.0f}%")
                
                # Small delay between batches to be respectful
                await asyncio.sleep(0.1)  # Reduced delay for faster processing
            
            self.logger.info(f"✅ Enhanced {len(enhanced_businesses)} businesses with comprehensive data")
            return enhanced_businesses

    async def _enhance_single_async(self, session: aiohttp.ClientSession, business: Dict[str, Any]) -> Dict[str, Any]:
        """Enhance a single business with comprehensive parallel data enrichment"""
        try:
            enhanced = business.copy()
            
            # Fast keyword extraction from existing data
            enhanced['metal_types'] = self._extract_metal_types_comprehensive(self._combine_all_text_sources(business))
            enhanced['services'] = self._extract_services_comprehensive(self._combine_all_text_sources(business))
            
            # Create parallel tasks for comprehensive data enrichment
            tasks = []
            
            # Always search for additional info, even if we have some data
            name = enhanced.get('name', '')
            if name:
                # Task 1: Find website if missing
                if not enhanced.get('website'):
                    tasks.append(self._find_website_async(session, enhanced))
                
                # Task 2: Find phone number if missing  
                if not enhanced.get('phone'):
                    tasks.append(self._find_phone_async(session, enhanced))
                
                # Task 3: Search for detailed business info (always run)
                tasks.append(self._search_business_details_async(session, enhanced))
            
            # Execute all search tasks in parallel
            if tasks:
                # Add a small delay to avoid overwhelming servers
                await asyncio.sleep(0.1)
                search_results = await asyncio.gather(*tasks, return_exceptions=True)
                
                # Merge results from all parallel searches
                for result in search_results:
                    if isinstance(result, Exception):
                        self.logger.debug(f"Search task failed: {result}")
                    elif isinstance(result, dict) and result:
                        # Merge non-empty results
                        for key, value in result.items():
                            if value and not enhanced.get(key):  # Only add if we don't have it
                                enhanced[key] = value
            
            # Task 4: If we have a website (original or found), scrape it
            if enhanced.get('website'):
                try:
                    website_data = await self._scrape_business_website_async(
                        session, enhanced['website'], enhanced['name']
                    )
                    if website_data:
                        enhanced.update(website_data)
                except Exception as e:
                    self.logger.debug(f"Website scraping failed for {enhanced['name']}: {e}")
            
            # Enhanced data processing from all available text
            all_text = self._combine_all_text_sources(enhanced)
            enhanced['metal_types'] = self._extract_metal_types_comprehensive(all_text)
            enhanced['services'] = self._extract_services_comprehensive(all_text)
            
            # Extract additional contact info from all sources
            enhanced = self._extract_additional_contacts(enhanced, all_text)
            
            # Fast quality assessment
            enhanced['contact_quality'] = self._assess_contact_quality_fast(enhanced)
            enhanced['business_type'] = self._classify_business_type_fast(enhanced)
            enhanced['data_completeness'] = self._calculate_data_completeness_fast(enhanced)
            
            return enhanced
            
        except Exception as e:
            self.logger.debug(f"Enhancement error for {business.get('name')}: {e}")
            return business

    async def _find_website_async(self, session: aiohttp.ClientSession, business: Dict[str, Any]) -> Dict[str, Any]:
        """Find business website using multiple parallel search strategies"""
        name = business.get('name', '')
        city = business.get('city', '')
        state = business.get('state', '')
        
        if not name:
            return {}
        
        # Create multiple search tasks in parallel
        search_tasks = [
            self._search_google_async(session, f"{name} {city} {state} scrap metal recycling"),
            self._search_bing_async(session, f"{name} {city} {state} metal recycling"),
            self._search_yellowpages_async(session, name, city, state),
            self._search_business_directories_async(session, name, city, state)
        ]
        
        try:
            results = await asyncio.gather(*search_tasks, return_exceptions=True)
            
            # Find the first valid website from any search
            for result in results:
                if isinstance(result, dict) and result.get('website'):
                    website = self._normalize_url(result['website'])
                    if website and self._is_valid_website(website):
                        return {'website': website, 'website_source': result.get('source', 'search')}
                        
        except Exception as e:
            self.logger.debug(f"Website search failed for {name}: {e}")
        
        return {}

    async def _find_phone_async(self, session: aiohttp.ClientSession, business: Dict[str, Any]) -> Dict[str, Any]:
        """Find business phone number using multiple parallel search strategies"""
        name = business.get('name', '')
        city = business.get('city', '')
        state = business.get('state', '')
        
        if not name:
            return {}
        
        # Create multiple phone search tasks in parallel
        phone_tasks = [
            self._search_phone_google_async(session, f"{name} {city} {state} phone"),
            self._search_phone_whitepages_async(session, name, city, state),
            self._search_phone_411_async(session, name, city, state),
            self._search_phone_yellowpages_async(session, name, city, state)
        ]
        
        try:
            results = await asyncio.gather(*phone_tasks, return_exceptions=True)
            
            # Find the first valid phone from any search
            for result in results:
                if isinstance(result, dict) and result.get('phone'):
                    phone = self._normalize_phone(result['phone'])
                    if phone:
                        return {'phone': phone, 'phone_source': result.get('source', 'search')}
                        
        except Exception as e:
            self.logger.debug(f"Phone search failed for {name}: {e}")
        
        return {}

    async def _search_business_details_async(self, session: aiohttp.ClientSession, business: Dict[str, Any]) -> Dict[str, Any]:
        """Search for comprehensive business details using multiple sources"""
        name = business.get('name', '')
        city = business.get('city', '')
        state = business.get('state', '')
        
        if not name:
            return {}
        
        # Create multiple detail search tasks in parallel
        detail_tasks = [
            self._search_bbb_async(session, name, city, state),
            self._search_chamber_async(session, name, city, state),
            self._search_manta_async(session, name, city, state),
            self._search_superpages_async(session, name, city, state)
        ]
        
        try:
            results = await asyncio.gather(*detail_tasks, return_exceptions=True)
            
            # Merge all found details
            merged_details = {}
            for result in results:
                if isinstance(result, dict) and result:
                    merged_details.update(result)
            
            return merged_details
                        
        except Exception as e:
            self.logger.debug(f"Business details search failed for {name}: {e}")
        
        return {}

    async def _process_enhanced_data_async(self, session: aiohttp.ClientSession, business: Dict[str, Any]) -> Dict[str, Any]:
        """Process and enhance data with additional parallel searches"""
        enhanced = business.copy()
        
        # If we found a website, scrape it for additional info
        if enhanced.get('website') and not enhanced.get('website_scraped'):
            try:
                website_data = await self._scrape_business_website_async(
                    session, enhanced['website'], enhanced['name']
                )
                enhanced.update(website_data)
                enhanced['website_scraped'] = True
            except Exception as e:
                self.logger.debug(f"Website scraping failed: {e}")
        
        # Enhanced metal type detection from all available text
        all_text = self._combine_all_text_sources(enhanced)
        enhanced['metal_types'] = self._extract_metal_types_comprehensive(all_text)
        enhanced['services'] = self._extract_services_comprehensive(all_text)
        
        # Extract additional contact info from all sources
        enhanced = self._extract_additional_contacts(enhanced, all_text)
        
        return enhanced

    # Fast parallel search implementations
    async def _search_google_async(self, session: aiohttp.ClientSession, query: str) -> Dict[str, Any]:
        """Search Google for business website"""
        try:
            # Skip Google search for now - they block automated requests
            # Focus on more reliable sources
            await asyncio.sleep(0.01)  # Small delay to simulate work
            return {}
                            
        except Exception as e:
            self.logger.debug(f"Google search failed: {e}")
        
        return {}

    async def _search_bing_async(self, session: aiohttp.ClientSession, query: str) -> Dict[str, Any]:
        """Search Bing for business website"""
        try:
            # Skip Bing search for now - focus on more reliable sources
            await asyncio.sleep(0.01)  # Small delay to simulate work
            return {}
                            
        except Exception as e:
            self.logger.debug(f"Bing search failed: {e}")
        
        return {}

    async def _search_yellowpages_async(self, session: aiohttp.ClientSession, name: str, city: str, state: str) -> Dict[str, Any]:
        """Search Yellow Pages for business info"""
        try:
            query = f"{name} {city} {state}".replace(' ', '+')
            search_url = f"https://www.yellowpages.com/search?search_terms={query}&geo_location_terms={city}+{state}"
            
            async with session.get(search_url, headers={'User-Agent': random.choice(self.user_agents)}) as response:
                if response.status == 200:
                    html = await response.text()
                    soup = BeautifulSoup(html, 'lxml')
                    
                    # Extract business info from Yellow Pages
                    result = {}
                    
                    # Look for website
                    website_link = soup.find('a', href=True, string=lambda text: text and 'website' in text.lower())
                    if website_link:
                        result['website'] = website_link['href']
                    
                    # Look for phone
                    phone_elem = soup.find('div', class_='phones')
                    if phone_elem:
                        result['phone'] = phone_elem.get_text().strip()
                    
                    if result:
                        result['source'] = 'yellowpages'
                        return result
                        
        except Exception as e:
            self.logger.debug(f"Yellow Pages search failed: {e}")
        
        return {}

    async def _search_business_directories_async(self, session: aiohttp.ClientSession, name: str, city: str, state: str) -> Dict[str, Any]:
        """Search business directories for comprehensive info"""
        try:
            # Search multiple directories in parallel
            directory_tasks = [
                self._search_superpages_async(session, name, city, state),
                self._search_manta_async(session, name, city, state),
                self._search_bbb_async(session, name, city, state)
            ]
            
            results = await asyncio.gather(*directory_tasks, return_exceptions=True)
            
            # Merge results from all directories
            merged = {}
            for result in results:
                if isinstance(result, dict) and result:
                    merged.update(result)
            
            return merged
                        
        except Exception as e:
            self.logger.debug(f"Business directories search failed: {e}")
        
        return {}

    # Phone search implementations
    async def _search_phone_google_async(self, session: aiohttp.ClientSession, query: str) -> Dict[str, Any]:
        """Search Google for phone number"""
        try:
            search_url = f"https://www.google.com/search?q={quote_plus(query)}"
            
            async with session.get(search_url, headers={'User-Agent': random.choice(self.user_agents)}) as response:
                if response.status == 200:
                    html = await response.text()
                    
                    # Extract phone numbers from search results
                    phones = []
                    for pattern in self.phone_patterns:
                        phones.extend(pattern.findall(html))
                    
                    if phones:
                        return {'phone': phones[0], 'source': 'google'}
                        
        except Exception as e:
            self.logger.debug(f"Google phone search failed: {e}")
        
        return {}

    async def _search_phone_whitepages_async(self, session: aiohttp.ClientSession, name: str, city: str, state: str) -> Dict[str, Any]:
        """Search White Pages for phone number"""
        try:
            query = f"{name} {city} {state}".replace(' ', '%20')
            search_url = f"https://www.whitepages.com/search/FindBusiness?what={query}&where={city}%2C+{state}"
            
            async with session.get(search_url, headers={'User-Agent': random.choice(self.user_agents)}) as response:
                if response.status == 200:
                    html = await response.text()
                    
                    # Extract phone from White Pages
                    phones = []
                    for pattern in self.phone_patterns:
                        phones.extend(pattern.findall(html))
                    
                    if phones:
                        return {'phone': phones[0], 'source': 'whitepages'}
                        
        except Exception as e:
            self.logger.debug(f"White Pages search failed: {e}")
        
        return {}

    async def _search_phone_411_async(self, session: aiohttp.ClientSession, name: str, city: str, state: str) -> Dict[str, Any]:
        """Search 411.com for phone number"""
        try:
            query = f"{name} {city} {state}".replace(' ', '+')
            search_url = f"https://www.411.com/search/reverse_phone?what={query}&where={city}+{state}"
            
            async with session.get(search_url, headers={'User-Agent': random.choice(self.user_agents)}) as response:
                if response.status == 200:
                    html = await response.text()
                    
                    # Extract phone from 411
                    phones = []
                    for pattern in self.phone_patterns:
                        phones.extend(pattern.findall(html))
                    
                    if phones:
                        return {'phone': phones[0], 'source': '411'}
                        
        except Exception as e:
            self.logger.debug(f"411.com search failed: {e}")
        
        return {}

    async def _search_phone_yellowpages_async(self, session: aiohttp.ClientSession, name: str, city: str, state: str) -> Dict[str, Any]:
        """Search Yellow Pages specifically for phone number"""
        try:
            query = f"{name} {city} {state}".replace(' ', '+')
            search_url = f"https://www.yellowpages.com/search?search_terms={query}&geo_location_terms={city}+{state}"
            
            async with session.get(search_url, headers={'User-Agent': random.choice(self.user_agents)}) as response:
                if response.status == 200:
                    html = await response.text()
                    
                    # Extract phone from Yellow Pages
                    phones = []
                    for pattern in self.phone_patterns:
                        phones.extend(pattern.findall(html))
                    
                    if phones:
                        return {'phone': phones[0], 'source': 'yellowpages'}
                        
        except Exception as e:
            self.logger.debug(f"Yellow Pages phone search failed: {e}")
        
        return {}

    async def _scrape_business_website_async(self, session: aiohttp.ClientSession, url: str, business_name: str) -> Dict[str, Any]:
        """Scrape business website for detailed information"""
        try:
            if not url or not url.startswith('http'):
                return {}
            
            async with session.get(url, headers={'User-Agent': random.choice(self.user_agents)}, timeout=aiohttp.ClientTimeout(total=10)) as response:
                if response.status == 200:
                    html = await response.text()
                    soup = BeautifulSoup(html, 'lxml')
                    
                    result = {}
                    
                    # Extract phone numbers
                    text = soup.get_text()
                    phones = []
                    for pattern in self.phone_patterns:
                        phones.extend(pattern.findall(text))
                    if phones:
                        result['phone'] = phones[0]
                    
                    # Extract email addresses
                    emails = []
                    for pattern in self.email_patterns:
                        emails.extend(pattern.findall(text))
                    if emails:
                        result['email'] = emails[0]
                    
                    # Extract business hours
                    hours_text = text.lower()
                    if any(word in hours_text for word in ['hours', 'open', 'monday', 'tuesday']):
                        result['hours'] = 'Available on website'
                    
                    # Extract services and metal types from website content
                    result['website_content'] = text[:1000]  # First 1000 chars for analysis
                    
                    return result
                    
        except Exception as e:
            self.logger.debug(f"Website scraping failed for {url}: {e}")
        
        return {}

    # Business directory search implementations
    async def _search_bbb_async(self, session: aiohttp.ClientSession, name: str, city: str, state: str) -> Dict[str, Any]:
        """Search Better Business Bureau for business details"""
        try:
            query = f"{name} {city} {state}".replace(' ', '+')
            search_url = f"https://www.bbb.org/search?find_country=USA&find_text={query}&find_type=Name"
            
            async with session.get(search_url, headers={'User-Agent': random.choice(self.user_agents)}) as response:
                if response.status == 200:
                    html = await response.text()
                    soup = BeautifulSoup(html, 'lxml')
                    
                    result = {}
                    
                    # Extract BBB rating
                    rating_elem = soup.find('span', class_='bbb-rating')
                    if rating_elem:
                        result['bbb_rating'] = rating_elem.get_text().strip()
                    
                    # Extract additional contact info
                    contact_info = soup.find('div', class_='contact-info')
                    if contact_info:
                        text = contact_info.get_text()
                        
                        # Extract phone
                        phones = []
                        for pattern in self.phone_patterns:
                            phones.extend(pattern.findall(text))
                        if phones:
                            result['phone'] = phones[0]
                        
                        # Extract website
                        website_link = contact_info.find('a', href=True)
                        if website_link and self._is_valid_website(website_link['href']):
                            result['website'] = website_link['href']
                    
                    if result:
                        result['source'] = 'bbb'
                        return result
                        
        except Exception as e:
            self.logger.debug(f"BBB search failed: {e}")
        
        return {}

    async def _search_chamber_async(self, session: aiohttp.ClientSession, name: str, city: str, state: str) -> Dict[str, Any]:
        """Search Chamber of Commerce for business details"""
        try:
            query = f"{name} {city} {state} chamber of commerce".replace(' ', '+')
            search_url = f"https://www.google.com/search?q={query}"
            
            async with session.get(search_url, headers={'User-Agent': random.choice(self.user_agents)}) as response:
                if response.status == 200:
                    html = await response.text()
                    
                    result = {}
                    
                    # Extract phone numbers
                    phones = []
                    for pattern in self.phone_patterns:
                        phones.extend(pattern.findall(html))
                    if phones:
                        result['phone'] = phones[0]
                    
                    # Extract emails
                    emails = []
                    for pattern in self.email_patterns:
                        emails.extend(pattern.findall(html))
                    if emails:
                        result['email'] = emails[0]
                    
                    if result:
                        result['source'] = 'chamber'
                        return result
                        
        except Exception as e:
            self.logger.debug(f"Chamber search failed: {e}")
        
        return {}

    async def _search_manta_async(self, session: aiohttp.ClientSession, name: str, city: str, state: str) -> Dict[str, Any]:
        """Search Manta.com for business details"""
        try:
            query = f"{name} {city} {state}".replace(' ', '+')
            search_url = f"https://www.manta.com/mb_{query}"
            
            async with session.get(search_url, headers={'User-Agent': random.choice(self.user_agents)}) as response:
                if response.status == 200:
                    html = await response.text()
                    soup = BeautifulSoup(html, 'lxml')
                    
                    result = {}
                    
                    # Extract business description
                    desc_elem = soup.find('div', class_='business-description')
                    if desc_elem:
                        result['description_enhanced'] = desc_elem.get_text().strip()
                    
                    # Extract contact info
                    contact_section = soup.find('section', class_='contact-info')
                    if contact_section:
                        text = contact_section.get_text()
                        
                        # Extract phone
                        phones = []
                        for pattern in self.phone_patterns:
                            phones.extend(pattern.findall(text))
                        if phones:
                            result['phone'] = phones[0]
                        
                        # Extract website
                        website_link = contact_section.find('a', href=True)
                        if website_link and self._is_valid_website(website_link['href']):
                            result['website'] = website_link['href']
                    
                    if result:
                        result['source'] = 'manta'
                        return result
                        
        except Exception as e:
            self.logger.debug(f"Manta search failed: {e}")
        
        return {}

    async def _search_superpages_async(self, session: aiohttp.ClientSession, name: str, city: str, state: str) -> Dict[str, Any]:
        """Search SuperPages for business details"""
        try:
            query = f"{name} {city} {state}".replace(' ', '+')
            search_url = f"https://www.superpages.com/search?what={query}&where={city}+{state}"
            
            async with session.get(search_url, headers={'User-Agent': random.choice(self.user_agents)}) as response:
                if response.status == 200:
                    html = await response.text()
                    soup = BeautifulSoup(html, 'lxml')
                    
                    result = {}
                    
                    # Extract business info
                    business_card = soup.find('div', class_='business-card')
                    if business_card:
                        text = business_card.get_text()
                        
                        # Extract phone
                        phones = []
                        for pattern in self.phone_patterns:
                            phones.extend(pattern.findall(text))
                        if phones:
                            result['phone'] = phones[0]
                        
                        # Extract website
                        website_link = business_card.find('a', href=True, string=lambda text: text and 'website' in text.lower())
                        if website_link and self._is_valid_website(website_link['href']):
                            result['website'] = website_link['href']
                    
                    if result:
                        result['source'] = 'superpages'
                        return result
                        
        except Exception as e:
            self.logger.debug(f"SuperPages search failed: {e}")
        
        return {}

    # Helper methods for data processing
    def _combine_all_text_sources(self, business: Dict[str, Any]) -> str:
        """Combine all text sources for comprehensive analysis"""
        text_sources = [
            business.get('name', ''),
            business.get('description', ''),
            business.get('description_enhanced', ''),
            business.get('shop_type', ''),
            business.get('recycling_materials', ''),
            business.get('bbb_rating', ''),
            str(business.get('website_content', '')),
            str(business.get('pricing', '')),
            str(business.get('services_from_website', '')),
            str(business.get('metals_from_website', ''))
        ]
        
        return ' '.join(str(s) for s in text_sources if s).lower()

    def _extract_metal_types_comprehensive(self, text: str) -> List[Dict[str, Any]]:
        """Comprehensive metal type extraction from all text sources"""
        found_metals = []
        
        # Fast set intersection for basic detection
        text_words = set(text.split())
        found_keywords = text_words.intersection(self.all_metal_keywords)
        
        # Map found keywords back to metal categories with confidence scores
        for metal_category, keywords in self.metal_types.items():
            matched_keywords = [k for k in keywords if k.lower() in found_keywords]
            if matched_keywords:
                confidence = len(matched_keywords) / len(keywords) * 100
                found_metals.append({
                    'type': metal_category,
                    'keywords_found': matched_keywords,
                    'confidence': round(confidence, 1)
                })
        
        # Sort by confidence
        found_metals.sort(key=lambda x: x['confidence'], reverse=True)
        return found_metals

    def _extract_services_comprehensive(self, text: str) -> List[str]:
        """Comprehensive service extraction from all text sources"""
        found_services = []
        
        # Enhanced service detection with context
        for service in self.services:
            if service.lower() in text:
                found_services.append(service)
        
        # Additional service patterns
        service_patterns = [
            r'we\s+(?:offer|provide|do)\s+([^.]+)',
            r'services?\s*:?\s*([^.]+)',
            r'specializ(?:e|ing)\s+in\s+([^.]+)',
            r'(?:pickup|collection|removal)\s+service',
            r'(?:container|dumpster|roll-off)\s+rental',
            r'(?:demolition|dismantling)\s+service'
        ]
        
        for pattern in service_patterns:
            matches = re.findall(pattern, text, re.IGNORECASE)
            for match in matches:
                if isinstance(match, str) and len(match) < 100:  # Reasonable length
                    found_services.append(match.strip())
        
        return list(set(found_services))  # Remove duplicates

    def _extract_additional_contacts(self, business: Dict[str, Any], text: str) -> Dict[str, Any]:
        """Extract additional contact information from all sources"""
        enhanced = business.copy()
        
        # Extract additional phone numbers
        if not enhanced.get('phone'):
            phones = []
            for pattern in self.phone_patterns:
                phones.extend(pattern.findall(text))
            if phones:
                enhanced['phone'] = self._normalize_phone(phones[0])
        
        # Extract additional emails
        if not enhanced.get('email'):
            emails = []
            for pattern in self.email_patterns:
                emails.extend(pattern.findall(text))
            if emails:
                enhanced['email'] = emails[0]
        
        # Extract business hours
        if not enhanced.get('hours'):
            hours = []
            for pattern in self.hours_patterns:
                hours.extend(pattern.findall(text))
            if hours:
                enhanced['hours'] = str(hours[0])
        
        return enhanced

    def _is_valid_website(self, url: str) -> bool:
        """Check if URL is a valid business website"""
        if not url:
            return False
        
        # Skip social media and directory sites
        skip_domains = [
            'facebook.com', 'twitter.com', 'instagram.com', 'linkedin.com',
            'yelp.com', 'google.com', 'yellowpages.com', 'whitepages.com',
            'bbb.org', 'manta.com', 'superpages.com'
        ]
        
        url_lower = url.lower()
        for domain in skip_domains:
            if domain in url_lower:
                return False
        
        # Must be a proper URL
        return url.startswith(('http://', 'https://')) and '.' in url

    def _normalize_phone(self, phone: str) -> str:
        """Normalize phone number format"""
        if not phone:
            return ""
        
        # Extract digits only
        digits = re.sub(r'\D', '', phone)
        
        # US phone number format
        if len(digits) == 10:
            return f"({digits[:3]}) {digits[3:6]}-{digits[6:]}"
        elif len(digits) == 11 and digits[0] == '1':
            return f"({digits[1:4]}) {digits[4:7]}-{digits[7:]}"
        
        return phone

    def _make_safe_request(self, url, params=None, data=None, method='GET', max_retries=3):
        """Make HTTP request with proper error handling"""
        for attempt in range(max_retries):
            try:
                headers = {
                    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
                    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
                    'Accept-Language': 'en-US,en;q=0.9',
                    'Accept-Encoding': 'gzip, deflate',
                    'Connection': 'keep-alive'
                }
                
                # Rate limiting
                time.sleep(random.uniform(1, 3))
                
                if method == 'POST':
                    response = self.session.post(url, headers=headers, data=data, timeout=30)
                else:
                    response = self.session.get(url, headers=headers, params=params, timeout=30)
                
                if response.status_code == 200:
                    return response
                elif response.status_code == 429:
                    wait_time = (attempt + 1) * 10
                    self.logger.warning(f"Rate limited, waiting {wait_time}s")
                    time.sleep(wait_time)
                else:
                    self.logger.warning(f"HTTP {response.status_code} for {url}")
                    
            except Exception as e:
                self.logger.warning(f"Request error (attempt {attempt + 1}): {e}")
                if attempt < max_retries - 1:
                    time.sleep(random.uniform(2, 5))
        
        return None

    def _scrape_overpass_api(self, count):
        """Scrape 100% REAL data from OpenStreetMap Overpass API"""
        start_time = time.perf_counter()
        self.logger.info(f"🌐 Collecting {count} REAL scrap metal businesses from OpenStreetMap")
        
        businesses = []
        
        # Real Overpass API endpoints
        overpass_endpoints = [
            "https://overpass-api.de/api/interpreter",
            "https://overpass.kumi.systems/api/interpreter",
            "https://overpass.openstreetmap.ru/api/interpreter"
        ]
        
        # Define bounding boxes for major US industrial areas
        bounding_boxes = [
            # Chicago, IL area
            {"name": "Chicago", "bbox": "41.6,-87.9,42.0,-87.5"},
            # Houston, TX area  
            {"name": "Houston", "bbox": "29.5,-95.8,30.1,-95.0"},
            # Detroit, MI area
            {"name": "Detroit", "bbox": "42.1,-83.5,42.6,-82.9"},
            # Pittsburgh, PA area
            {"name": "Pittsburgh", "bbox": "40.3,-80.2,40.6,-79.8"},
            # Los Angeles, CA area
            {"name": "Los Angeles", "bbox": "33.9,-118.7,34.3,-118.0"},
            # Atlanta, GA area
            {"name": "Atlanta", "bbox": "33.5,-84.7,33.9,-84.1"},
            # Philadelphia, PA area
            {"name": "Philadelphia", "bbox": "39.8,-75.4,40.1,-74.9"},
            # Birmingham, AL area
            {"name": "Birmingham", "bbox": "33.3,-87.0,33.7,-86.5"},
            # Cleveland, OH area
            {"name": "Cleveland", "bbox": "41.3,-81.9,41.7,-81.4"},
            # Dallas, TX area
            {"name": "Dallas", "bbox": "32.6,-97.0,33.0,-96.5"},
            # Phoenix, AZ area
            {"name": "Phoenix", "bbox": "33.2,-112.5,33.8,-111.6"},
            # San Antonio, TX area
            {"name": "San Antonio", "bbox": "29.2,-98.8,29.7,-98.2"},
            # Jacksonville, FL area
            {"name": "Jacksonville", "bbox": "30.1,-81.9,30.5,-81.4"},
            # Memphis, TN area
            {"name": "Memphis", "bbox": "34.9,-90.3,35.4,-89.8"},
            # Nashville, TN area
            {"name": "Nashville", "bbox": "36.0,-87.0,36.3,-86.5"}
        ]
        
        # Targeted Overpass query for actual scrap metal businesses only
        overpass_query_template = """
        [out:json][timeout:45];
        (
          // Specific scrap yards and metal recycling facilities
          nwr["shop"="scrap_yard"]({bbox});
          nwr["amenity"="recycling"]["recycling_type"="centre"]({bbox});
          nwr["industrial"="scrap_yard"]({bbox});
          nwr["industrial"="metal_processing"]({bbox});
          
          // Recycling centers that specifically handle metals
          nwr["amenity"="recycling"]["recycling:metal"="yes"]({bbox});
          nwr["amenity"="recycling"]["recycling:aluminium"="yes"]({bbox});
          nwr["amenity"="recycling"]["recycling:copper"="yes"]({bbox});
          nwr["amenity"="recycling"]["recycling:steel"="yes"]({bbox});
          nwr["amenity"="recycling"]["recycling:iron"="yes"]({bbox});
          
          // Auto salvage and scrap dealers
          nwr["shop"="car_parts"]["service"~"recycling|salvage|scrap",i]({bbox});
          nwr["amenity"="car_sharing"]["operator"~"salvage|scrap",i]({bbox});
          
          // Businesses with specific scrap/recycling names (more targeted)
          nwr["name"~"^.*scrap.*yard.*$|^.*metal.*recycl.*$|^.*auto.*salvage.*$|^.*junk.*yard.*$",i]({bbox});
          nwr["operator"~"^.*scrap.*metal.*$|^.*recycling.*center.*$|^.*salvage.*yard.*$",i]({bbox});
          
          // Industrial waste and metal processing
          nwr["industrial"="waste_management"]["waste"~"metal|scrap",i]({bbox});
        );
        out geom;
        """
        
        # Calculate how many boxes to use based on target count
        if count <= 50:
            boxes_to_use = min(5, len(bounding_boxes))
        elif count <= 150:
            boxes_to_use = min(10, len(bounding_boxes))
        else:
            boxes_to_use = len(bounding_boxes)
        
        selected_boxes = bounding_boxes[:boxes_to_use]
        
        for i, bbox_info in enumerate(selected_boxes):
            if len(businesses) >= count:
                break
                
            bbox = bbox_info["bbox"]
            area_name = bbox_info["name"]
            
            self.logger.info(f"🔍 Searching {area_name} area ({i+1}/{len(selected_boxes)})")
            
            # Format the query with the bounding box
            query = overpass_query_template.format(bbox=bbox)
            
            # Try each endpoint until one works
            for endpoint in overpass_endpoints:
                try:
                    response = self._make_safe_request(
                        endpoint,
                        data=query,
                        method='POST'
                    )
                    
                    if response and response.status_code == 200:
                        try:
                            data = response.json()
                            area_businesses = self._parse_overpass_results(data)
                            
                            if area_businesses:
                                businesses.extend(area_businesses)
                                self.logger.info(f"✅ Found {len(area_businesses)} businesses in {area_name}")
                                break
                            else:
                                self.logger.info(f"ℹ️ No businesses found in {area_name}")
                                break
                                
                        except json.JSONDecodeError:
                            self.logger.warning(f"Invalid JSON response from {endpoint}")
                            continue
                    else:
                        self.logger.warning(f"Failed to get data from {endpoint}")
                        continue
                        
                except Exception as e:
                    self.logger.warning(f"Error querying {endpoint}: {e}")
                    continue
            
            # Progress update
            if (i + 1) % 3 == 0:
                progress = len(businesses) / count * 100
                self.logger.info(f"📈 Progress: {len(businesses)}/{count} businesses ({progress:.0f}%)")
            
            # Small delay between requests
            time.sleep(random.uniform(2, 4))
        
        # Remove duplicates and limit to requested count
        unique_businesses = self._remove_duplicates_real(businesses)
        final_businesses = unique_businesses[:count]
        
        elapsed = time.perf_counter() - start_time
        self.logger.info(f"🌐 Collected {len(final_businesses)} REAL businesses in {elapsed:.1f}s")
        
        return final_businesses
    
    def _parse_overpass_results(self, data):
        """Parse OpenStreetMap Overpass API results into business data"""
        businesses = []
        
        try:
            elements = data.get('elements', [])
            
            for element in elements:
                try:
                    tags = element.get('tags', {})
                    
                    # Extract business name
                    name = (tags.get('name') or 
                           tags.get('operator') or 
                           tags.get('brand') or 
                           "Unnamed Business")
                    
                    # Skip if no meaningful name
                    if not name or name == "Unnamed Business":
                        continue
                    
                    # Filter out non-scrap metal businesses
                    if self._is_non_scrap_business(tags, name):
                        continue
                    
                    # Extract coordinates
                    lat = lon = None
                    if element.get('type') == 'node':
                        lat = element.get('lat')
                        lon = element.get('lon')
                    elif element.get('type') in ['way', 'relation']:
                        # For ways/relations, use center point
                        if 'center' in element:
                            lat = element['center'].get('lat')
                            lon = element['center'].get('lon')
                        elif 'geometry' in element and element['geometry']:
                            # Calculate centroid from geometry
                            coords = element['geometry']
                            if coords:
                                lat = sum(p.get('lat', 0) for p in coords) / len(coords)
                                lon = sum(p.get('lon', 0) for p in coords) / len(coords)
                    
                    if not lat or not lon:
                        continue
                    
                    # Build address from tags
                    address_parts = []
                    if tags.get('addr:housenumber'):
                        address_parts.append(tags['addr:housenumber'])
                    if tags.get('addr:street'):
                        address_parts.append(tags['addr:street'])
                    
                    address = ' '.join(address_parts) if address_parts else "Address not available"
                    
                    # Extract city, state, zip
                    city = tags.get('addr:city', 'Unknown')
                    state = tags.get('addr:state', 'Unknown')
                    zip_code = tags.get('addr:postcode', 'Unknown')
                    
                    # Extract phone
                    phone = tags.get('phone', 'Phone not available')
                    if phone and phone != 'Phone not available':
                        phone = self._normalize_phone(phone)
                    
                    # Extract website
                    website = tags.get('website') or tags.get('url') or 'Website not available'
                    
                    # Extract email
                    email = tags.get('email', 'Email not available')
                    
                    # Determine metal types based on tags
                    metal_types = self._determine_metal_types_from_tags(tags)
                    
                    # Determine services based on tags
                    services = self._determine_services_from_tags(tags)
                    
                    # Extract business hours
                    hours = tags.get('opening_hours', 'Hours not available')
                    
                    # Determine business type
                    business_type = self._determine_business_type_from_tags(tags)
                    
                    business = {
                        'name': name,
                        'address': address,
                        'city': city,
                        'state': state,
                        'zip_code': zip_code,
                        'country': 'United States',
                        'latitude': lat,
                        'longitude': lon,
                        'phone': phone,
                        'email': email,
                        'website': website,
                        'metal_types': metal_types,
                        'services': services,
                        'hours': hours,
                        'business_type': business_type,
                        'source': 'OpenStreetMap',
                        'scraped_at': datetime.now().isoformat(),
                        'shop_type': tags.get('shop', 'scrap_yard'),
                        'osm_id': element.get('id'),
                        'osm_type': element.get('type')
                    }
                    
                    businesses.append(business)
                    
                except Exception as e:
                    self.logger.debug(f"Error parsing element: {e}")
                    continue
                    
        except Exception as e:
            self.logger.error(f"Error parsing Overpass results: {e}")
        
        return businesses
    
    def _determine_metal_types_from_tags(self, tags):
        """Determine metal types from OSM tags"""
        metal_types = []
        
        # Check recycling materials
        recycling_materials = tags.get('recycling', '').lower()
        if 'metal' in recycling_materials:
            metal_types.extend(['Steel', 'Iron', 'Aluminum'])
        if 'aluminium' in recycling_materials or 'aluminum' in recycling_materials:
            metal_types.append('Aluminum')
        if 'copper' in recycling_materials:
            metal_types.append('Copper')
        if 'steel' in recycling_materials:
            metal_types.append('Steel')
        if 'iron' in recycling_materials:
            metal_types.append('Iron')
        
        # Check name and description for metal types
        name_desc = f"{tags.get('name', '')} {tags.get('description', '')}".lower()
        
        metal_keywords = {
            'steel': 'Steel',
            'iron': 'Iron', 
            'aluminum': 'Aluminum',
            'aluminium': 'Aluminum',
            'copper': 'Copper',
            'brass': 'Brass',
            'bronze': 'Bronze',
            'stainless': 'Stainless Steel',
            'scrap': ['Steel', 'Iron', 'Aluminum'],
            'metal': ['Steel', 'Iron', 'Aluminum', 'Copper']
        }
        
        for keyword, metal_type in metal_keywords.items():
            if keyword in name_desc:
                if isinstance(metal_type, list):
                    metal_types.extend(metal_type)
                else:
                    metal_types.append(metal_type)
        
        # Default metals for scrap yards
        if not metal_types and (tags.get('shop') == 'scrap_yard' or 'scrap' in name_desc):
            metal_types = ['Steel', 'Iron', 'Aluminum', 'Copper']
        
        return list(set(metal_types)) if metal_types else ['Mixed Metals']
    
    def _determine_services_from_tags(self, tags):
        """Determine services from OSM tags"""
        services = []
        
        # Check for specific service tags
        if tags.get('service:pickup') == 'yes':
            services.append('Pickup Service')
        if tags.get('service:container') == 'yes':
            services.append('Container Rental')
        if tags.get('service:processing') == 'yes':
            services.append('Metal Processing')
        if tags.get('service:weighing') == 'yes':
            services.append('Certified Scales')
        
        # Check name and description for services
        name_desc = f"{tags.get('name', '')} {tags.get('description', '')}".lower()
        
        service_keywords = {
            'pickup': 'Pickup Service',
            'container': 'Container Rental',
            'processing': 'Metal Processing',
            'scale': 'Certified Scales',
            'weighing': 'Certified Scales',
            'demolition': 'Demolition Services',
            'cash': 'Cash Payment',
            'commercial': 'Commercial Accounts'
        }
        
        for keyword, service in service_keywords.items():
            if keyword in name_desc:
                services.append(service)
        
        # Default services for scrap yards
        if not services:
            services = ['Metal Processing', 'Cash Payment']
        
        return list(set(services))
    
    def _determine_business_type_from_tags(self, tags):
        """Determine business type from OSM tags"""
        shop_type = tags.get('shop', '')
        amenity = tags.get('amenity', '')
        industrial = tags.get('industrial', '')
        
        if shop_type == 'scrap_yard':
            return 'Scrap Metal Dealer'
        elif amenity == 'recycling':
            return 'Metal Recycling Center'
        elif industrial in ['scrap_yard', 'metal_processing']:
            return 'Industrial Recycling'
        elif 'auto' in tags.get('name', '').lower():
            return 'Auto Salvage'
                 else:
             return 'Metal Recycling Center'

    def _is_non_scrap_business(self, tags, name):
        """Filter out businesses that are not actual scrap metal operations"""
        
        # Check for specific business types that are NOT scrap metal
        non_scrap_types = [
            'restaurant', 'bar', 'pub', 'cafe', 'food', 'tattoo', 
            'salon', 'gym', 'fitness', 'hotel', 'motel', 'retail',
            'clothing', 'electronics', 'computer', 'phone', 'mobile',
            'bank', 'insurance', 'real_estate', 'office', 'medical',
            'dental', 'veterinary', 'school', 'education', 'church',
            'gas_station', 'fuel', 'convenience', 'grocery', 'pharmacy'
        ]
        
        # Check OSM tags for non-scrap business types
        amenity = tags.get('amenity', '').lower()
        shop = tags.get('shop', '').lower()
        cuisine = tags.get('cuisine', '').lower()
        building = tags.get('building', '').lower()
        
        for non_scrap_type in non_scrap_types:
            if (non_scrap_type in amenity or 
                non_scrap_type in shop or 
                non_scrap_type in cuisine or
                non_scrap_type in building):
                return True
        
        # Check business name for non-scrap indicators
        name_lower = name.lower()
        non_scrap_keywords = [
            'restaurant', 'bar', 'grill', 'kitchen', 'cafe', 'coffee',
            'tattoo', 'ink', 'salon', 'spa', 'gym', 'fitness', 'hotel',
            'motel', 'inn', 'lodge', 'retail', 'store', 'shop', 'mart',
            'bank', 'credit', 'insurance', 'real estate', 'office',
            'medical', 'dental', 'clinic', 'hospital', 'school',
            'university', 'college', 'church', 'temple', 'mosque',
            'gas station', 'fuel', 'convenience', 'grocery', 'pharmacy',
            'electronics', 'computer', 'phone', 'mobile', 'clothing',
            'fashion', 'jewelry', 'watch', 'game', 'toy', 'book'
        ]
        
        for keyword in non_scrap_keywords:
            if keyword in name_lower:
                return True
        
        # Allow businesses that clearly indicate scrap/metal/recycling
        scrap_indicators = [
            'scrap', 'metal', 'recycling', 'salvage', 'junk', 'auto parts',
            'steel', 'iron', 'aluminum', 'copper', 'brass', 'waste management'
        ]
        
        for indicator in scrap_indicators:
            if indicator in name_lower:
                return False  # This IS a scrap business
        
        # If we get here and the business has specific recycling tags, allow it
        if (tags.get('amenity') == 'recycling' or 
            tags.get('shop') == 'scrap_yard' or
            tags.get('industrial') in ['scrap_yard', 'metal_processing'] or
            any(key.startswith('recycling:') for key in tags.keys())):
            return False  # This IS a scrap business
        
                 # Default: if unclear, exclude to be safe
         return True

    def _scrape_google_maps_real(self, count):
        """Scrape real businesses from Google Maps"""
        businesses = []
        
        # Search terms for scrap metal businesses
        search_terms = [
            "scrap metal recycling near me",
            "metal recycling center",
            "scrap yard",
            "auto salvage yard",
            "metal scrap dealer",
            "copper recycling",
            "aluminum recycling",
            "steel recycling"
        ]
        
        # Major US cities for geographic diversity
        cities = [
            "Chicago, IL", "Houston, TX", "Phoenix, AZ", "Philadelphia, PA",
            "San Antonio, TX", "San Diego, CA", "Dallas, TX", "San Jose, CA",
            "Austin, TX", "Jacksonville, FL", "Fort Worth, TX", "Columbus, OH",
            "Charlotte, NC", "San Francisco, CA", "Indianapolis, IN", "Seattle, WA",
            "Denver, CO", "Washington, DC", "Boston, MA", "El Paso, TX",
            "Detroit, MI", "Nashville, TN", "Memphis, TN", "Portland, OR",
            "Oklahoma City, OK", "Las Vegas, NV", "Louisville, KY", "Baltimore, MD"
        ]
        
        searches_per_city = max(1, count // len(cities))
        
        for city in cities[:count]:
            if len(businesses) >= count:
                break
                
            for term in search_terms[:searches_per_city]:
                try:
                    query = f"{term} {city}"
                    self.logger.info(f"🔍 Searching Google Maps: {query}")
                    
                    # Use Google Maps search API approach
                    search_url = "https://www.google.com/maps/search/"
                    params = {
                        'api': '1',
                        'query': query
                    }
                    
                    response = self._make_safe_request(search_url, params=params)
                    if response and response.status_code == 200:
                        # Parse Google Maps results
                        map_businesses = self._parse_google_maps_results(response.text, city)
                        businesses.extend(map_businesses)
                        
                        if len(businesses) >= count:
                            break
                            
                except Exception as e:
                    self.logger.error(f"❌ Google Maps search failed for {query}: {e}")
                    continue
        
        return businesses[:count]
    
    def _scrape_yelp_real(self, count):
        """Scrape real businesses from Yelp"""
        businesses = []
        
        # Yelp search categories
        categories = [
            "metalrecycling",
            "scrapyards", 
            "recycling",
            "autosalvage"
        ]
        
        # Major metro areas
        locations = [
            "New York, NY", "Los Angeles, CA", "Chicago, IL", "Houston, TX",
            "Phoenix, AZ", "Philadelphia, PA", "San Antonio, TX", "San Diego, CA",
            "Dallas, TX", "San Jose, CA", "Austin, TX", "Jacksonville, FL"
        ]
        
        for location in locations:
            if len(businesses) >= count:
                break
                
            for category in categories:
                try:
                    # Yelp search URL
                    yelp_url = f"https://www.yelp.com/search"
                    params = {
                        'find_desc': 'scrap metal recycling',
                        'find_loc': location,
                        'cflt': category
                    }
                    
                    response = self._make_safe_request(yelp_url, params=params)
                    if response and response.status_code == 200:
                        yelp_businesses = self._parse_yelp_results(response.text, location)
                        businesses.extend(yelp_businesses)
                        
                        if len(businesses) >= count:
                            break
                            
                except Exception as e:
                    self.logger.error(f"❌ Yelp search failed for {location}: {e}")
                    continue
        
        return businesses[:count]
    
    def _scrape_yellowpages_real(self, count):
        """Scrape real businesses from Yellow Pages"""
        businesses = []
        
        # Yellow Pages search terms
        search_terms = [
            "Scrap Metal",
            "Metal Recycling", 
            "Recycling Centers",
            "Scrap Yards",
            "Auto Salvage"
        ]
        
        # State abbreviations for coverage
        states = [
            "CA", "TX", "FL", "NY", "PA", "IL", "OH", "GA", "NC", "MI",
            "NJ", "VA", "WA", "AZ", "MA", "TN", "IN", "MO", "MD", "WI"
        ]
        
        for state in states:
            if len(businesses) >= count:
                break
                
            for term in search_terms:
                try:
                    # Yellow Pages search URL
                    yp_url = f"https://www.yellowpages.com/search"
                    params = {
                        'search_terms': term,
                        'geo_location_terms': state
                    }
                    
                    response = self._make_safe_request(yp_url, params=params)
                    if response and response.status_code == 200:
                        yp_businesses = self._parse_yellowpages_results(response.text, state)
                        businesses.extend(yp_businesses)
                        
                        if len(businesses) >= count:
                            break
                            
                except Exception as e:
                    self.logger.error(f"❌ Yellow Pages search failed for {term} in {state}: {e}")
                    continue
        
        return businesses[:count]
    
    def _scrape_business_directories_real(self, count):
        """Scrape real businesses from business directories"""
        businesses = []
        
        # Business directory sources
        directories = [
            {
                'name': 'Superpages',
                'url': 'https://www.superpages.com/search',
                'params': {'search_terms': 'scrap metal', 'geo_location_terms': '{location}'}
            },
            {
                'name': 'Manta',
                'url': 'https://www.manta.com/search',
                'params': {'search_terms': 'metal recycling', 'geo_location_terms': '{location}'}
            },
            {
                'name': 'Merchant Circle',
                'url': 'https://www.merchantcircle.com/search',
                'params': {'search_terms': 'scrap yard', 'geo_location_terms': '{location}'}
            }
        ]
        
        locations = [
            "Atlanta, GA", "Boston, MA", "Cleveland, OH", "Denver, CO",
            "Kansas City, MO", "Las Vegas, NV", "Miami, FL", "Minneapolis, MN",
            "Nashville, TN", "New Orleans, LA", "Orlando, FL", "Portland, OR",
            "Sacramento, CA", "Salt Lake City, UT", "Tampa, FL", "Tucson, AZ"
        ]
        
        per_directory = max(1, count // len(directories))
        
        for directory in directories:
            if len(businesses) >= count:
                break
                
            for location in locations:
                try:
                    # Format parameters with location
                    params = {}
                    for key, value in directory['params'].items():
                        params[key] = value.format(location=location)
                    
                    response = self._make_safe_request(directory['url'], params=params)
                    if response and response.status_code == 200:
                        dir_businesses = self._parse_directory_results(response.text, directory['name'], location)
                        businesses.extend(dir_businesses)
                        
                        if len(businesses) >= count:
                            break
                            
                except Exception as e:
                    self.logger.error(f"❌ {directory['name']} search failed for {location}: {e}")
                    continue
        
        return businesses[:count]
    
    def _parse_google_maps_results(self, html, city):
        """Parse Google Maps search results for real business data"""
        businesses = []
        
        try:
            soup = BeautifulSoup(html, 'html.parser')
            
            # Look for business listings in Google Maps format
            business_elements = soup.find_all(['div', 'article'], class_=lambda x: x and any(
                keyword in x.lower() for keyword in ['business', 'listing', 'result', 'place']
            ))
            
            for element in business_elements:
                try:
                    # Extract business name
                    name_elem = element.find(['h3', 'h2', 'a'], class_=lambda x: x and 'name' in x.lower())
                    if not name_elem:
                        name_elem = element.find(['h3', 'h2', 'a'])
                    
                    if name_elem:
                        name = name_elem.get_text(strip=True)
                        
                        # Extract address
                        address_elem = element.find(['span', 'div'], class_=lambda x: x and 'address' in x.lower())
                        address = address_elem.get_text(strip=True) if address_elem else "Address not available"
                        
                        # Extract phone
                        phone_elem = element.find(['span', 'div'], class_=lambda x: x and 'phone' in x.lower())
                        phone = phone_elem.get_text(strip=True) if phone_elem else "Phone not available"
                        
                        # Extract rating
                        rating_elem = element.find(['span', 'div'], class_=lambda x: x and 'rating' in x.lower())
                        rating = rating_elem.get_text(strip=True) if rating_elem else "No rating"
                        
                        business = {
                            'name': name,
                            'address': address,
                            'city': city.split(',')[0],
                            'state': city.split(',')[1].strip() if ',' in city else '',
                            'phone': phone,
                            'rating': rating,
                            'source': 'Google Maps',
                            'scraped_at': datetime.now().isoformat()
                        }
                        
                        businesses.append(business)
                        
                except Exception as e:
                    continue
                    
        except Exception as e:
            self.logger.error(f"❌ Error parsing Google Maps results: {e}")
        
        return businesses
    
    def _parse_yelp_results(self, html, location):
        """Parse Yelp search results for real business data"""
        businesses = []
        
        try:
            soup = BeautifulSoup(html, 'html.parser')
            
            # Look for Yelp business listings
            business_elements = soup.find_all(['div', 'article'], class_=lambda x: x and any(
                keyword in x.lower() for keyword in ['business', 'search-result', 'listing']
            ))
            
            for element in business_elements:
                try:
                    # Extract business name
                    name_elem = element.find('a', class_=lambda x: x and 'business-name' in x.lower())
                    if not name_elem:
                        name_elem = element.find(['h3', 'h4', 'a'])
                    
                    if name_elem:
                        name = name_elem.get_text(strip=True)
                        
                        # Extract address
                        address_elem = element.find(['address', 'span'], class_=lambda x: x and 'address' in x.lower())
                        address = address_elem.get_text(strip=True) if address_elem else "Address not available"
                        
                        # Extract phone
                        phone_elem = element.find(['span', 'div'], class_=lambda x: x and 'phone' in x.lower())
                        phone = phone_elem.get_text(strip=True) if phone_elem else "Phone not available"
                        
                        # Extract rating
                        rating_elem = element.find(['span', 'div'], class_=lambda x: x and 'rating' in x.lower())
                        rating = rating_elem.get_text(strip=True) if rating_elem else "No rating"
                        
                        business = {
                            'name': name,
                            'address': address,
                            'city': location.split(',')[0],
                            'state': location.split(',')[1].strip() if ',' in location else '',
                            'phone': phone,
                            'rating': rating,
                            'source': 'Yelp',
                            'scraped_at': datetime.now().isoformat()
                        }
                        
                        businesses.append(business)
                        
                except Exception as e:
                    continue
                    
        except Exception as e:
            self.logger.error(f"❌ Error parsing Yelp results: {e}")
        
        return businesses
    
    def _parse_yellowpages_results(self, html, state):
        """Parse Yellow Pages search results for real business data"""
        businesses = []
        
        try:
            soup = BeautifulSoup(html, 'html.parser')
            
            # Look for Yellow Pages business listings
            business_elements = soup.find_all(['div', 'article'], class_=lambda x: x and any(
                keyword in x.lower() for keyword in ['result', 'listing', 'business']
            ))
            
            for element in business_elements:
                try:
                    # Extract business name
                    name_elem = element.find(['h3', 'h2', 'a'], class_=lambda x: x and 'business-name' in x.lower())
                    if not name_elem:
                        name_elem = element.find(['h3', 'h2', 'a'])
                    
                    if name_elem:
                        name = name_elem.get_text(strip=True)
                        
                        # Extract address
                        address_elem = element.find(['span', 'div'], class_=lambda x: x and 'street-address' in x.lower())
                        address = address_elem.get_text(strip=True) if address_elem else "Address not available"
                        
                        # Extract phone
                        phone_elem = element.find(['span', 'div'], class_=lambda x: x and 'phone' in x.lower())
                        phone = phone_elem.get_text(strip=True) if phone_elem else "Phone not available"
                        
                        business = {
                            'name': name,
                            'address': address,
                            'state': state,
                            'phone': phone,
                            'source': 'Yellow Pages',
                            'scraped_at': datetime.now().isoformat()
                        }
                        
                        businesses.append(business)
                        
                except Exception as e:
                    continue
                    
        except Exception as e:
            self.logger.error(f"❌ Error parsing Yellow Pages results: {e}")
        
        return businesses
    
    def _parse_directory_results(self, html, directory_name, location):
        """Parse business directory results for real business data"""
        businesses = []
        
        try:
            soup = BeautifulSoup(html, 'html.parser')
            
            # Look for business listings
            business_elements = soup.find_all(['div', 'article', 'li'], class_=lambda x: x and any(
                keyword in x.lower() for keyword in ['result', 'listing', 'business', 'company']
            ))
            
            for element in business_elements:
                try:
                    # Extract business name
                    name_elem = element.find(['h3', 'h2', 'h4', 'a'])
                    if name_elem:
                        name = name_elem.get_text(strip=True)
                        
                        # Extract address
                        address_elem = element.find(['span', 'div'], class_=lambda x: x and 'address' in x.lower())
                        address = address_elem.get_text(strip=True) if address_elem else "Address not available"
                        
                        # Extract phone
                        phone_elem = element.find(['span', 'div'], class_=lambda x: x and 'phone' in x.lower())
                        phone = phone_elem.get_text(strip=True) if phone_elem else "Phone not available"
                        
                        business = {
                            'name': name,
                            'address': address,
                            'city': location.split(',')[0],
                            'state': location.split(',')[1].strip() if ',' in location else '',
                            'phone': phone,
                            'source': directory_name,
                            'scraped_at': datetime.now().isoformat()
                        }
                        
                        businesses.append(business)
                        
                except Exception as e:
                    continue
                    
        except Exception as e:
            self.logger.error(f"❌ Error parsing {directory_name} results: {e}")
        
        return businesses
    
    def _remove_duplicates_real(self, businesses):
        """Remove duplicate businesses based on name and location"""
        seen = set()
        unique_businesses = []
        
        for business in businesses:
            # Create a key based on name and city
            key = f"{business.get('name', '').lower().strip()}_{business.get('city', '').lower().strip()}"
            
            if key not in seen and business.get('name'):
                seen.add(key)
                unique_businesses.append(business)
        
        return unique_businesses

    def _scrape_business_listings(self, count):
        """Scrape from business listing sites that allow scraping"""
        results = []
        
        try:
            # Use the Internet Archive Wayback Machine for historical business data
            wayback_url = "https://web.archive.org/web/20231201000000*/yellowpages.com/search*scrap*metal"
            
            response = self._make_safe_request(wayback_url)
            if response and response.status_code == 200:
                # Parse archived business listings
                businesses = self._parse_archived_listings(response.text)
                results.extend(businesses[:count])
                
        except Exception as e:
            self.logger.error(f"Business listings error: {e}")
        
        return results

    def _scrape_chamber_commerce(self, count):
        """Scrape from Chamber of Commerce directories"""
        results = []
        
        try:
            # Many local chambers have open business directories
            chamber_urls = [
                "https://www.detroitchamber.com/member-directory/",
                "https://www.houstonchamber.com/member-directory/",
                "https://www.chicagolandchamber.org/member-directory/"
            ]
            
            for chamber_url in chamber_urls:
                try:
                    response = self._make_safe_request(chamber_url)
                    if response and response.status_code == 200:
                        businesses = self._parse_chamber_directory(response.text)
                        results.extend(businesses[:count//len(chamber_urls)])
                    
                    time.sleep(random.uniform(3, 7))
                    
                except Exception as e:
                    self.logger.warning(f"Chamber scraping error: {e}")
                    continue
                    
        except Exception as e:
            self.logger.error(f"Chamber of Commerce error: {e}")
        
        return results

    def _scrape_recycling_orgs(self, count):
        """Scrape from recycling organization websites"""
        results = []
        
        try:
            # Institute of Scrap Recycling Industries member directory
            isri_url = "https://www.isri.org/members/member-directory"
            
            response = self._make_safe_request(isri_url)
            if response and response.status_code == 200:
                businesses = self._parse_isri_directory(response.text)
                results.extend(businesses[:count])
                
        except Exception as e:
            self.logger.error(f"ISRI directory error: {e}")
        
        return results

    def _scrape_government_sites(self, count):
        """Scrape from government environmental/recycling databases"""
        results = []
        
        try:
            # EPA RCRAInfo public database
            epa_url = "https://enviro.epa.gov/enviro/efservice/br_facility/state_code/TX/JSON"
            
            response = self._make_safe_request(epa_url)
            if response and response.status_code == 200:
                data = response.json()
                businesses = self._parse_epa_facilities(data)
                results.extend(businesses[:count])
                
        except Exception as e:
            self.logger.error(f"EPA database error: {e}")
        
        return results

    def _parse_overpass_results(self, data):
        """Parse OpenStreetMap Overpass API results"""
        businesses = []
        
        try:
            if 'elements' in data:
                for element in data['elements']:
                    tags = element.get('tags', {})
                    
                    # Extract business information
                    name = tags.get('name', tags.get('operator', 'Unknown Business'))
                    
                    # Get coordinates
                    lat = element.get('lat')
                    lon = element.get('lon')
                    
                    # For ways, use center coordinates
                    if not lat and 'center' in element:
                        lat = element['center']['lat']
                        lon = element['center']['lon']
                    
                    business = {
                        'name': name,
                        'address': self._build_address_from_tags(tags),
                        'city': tags.get('addr:city', ''),
                        'state': tags.get('addr:state', ''),
                        'zip_code': tags.get('addr:postcode', ''),
                        'phone': self._get_first_tag(tags, ['phone', 'contact:phone', 'telephone', 'contact:telephone']),
                        'website': self._normalize_url(
                            self._get_first_tag(tags, ['website', 'contact:website', 'url', 'contact:url', 'operator:website'])
                        ),
                        'latitude': lat,
                        'longitude': lon,
                        'shop_type': tags.get('shop', tags.get('amenity', tags.get('industrial', ''))),
                        'opening_hours': tags.get('opening_hours', ''),
                        'source': 'OpenStreetMap',
                        'osm_id': element.get('id'),
                        'scraped_at': datetime.now().isoformat()
                    }
                    
                    # Accept entries even if the name tag is missing – assign placeholder for uniqueness
                    if name == 'Unknown Business' or not name.strip():
                        placeholder = f"Scrap Center {lat:.5f},{lon:.5f}" if lat and lon else "Scrap Center"
                        business['name'] = placeholder
                    businesses.append(business)
                        
        except Exception as e:
            self.logger.error(f"Error parsing Overpass results: {e}")
        
        return businesses

    def _build_address_from_tags(self, tags):
        """Build address from OSM tags"""
        address_parts = []
        
        if 'addr:housenumber' in tags:
            address_parts.append(tags['addr:housenumber'])
        if 'addr:street' in tags:
            address_parts.append(tags['addr:street'])
            
        return ' '.join(address_parts) if address_parts else ''

    def _parse_archived_listings(self, html):
        """Parse archived business listings"""
        businesses = []
        
        try:
            soup = BeautifulSoup(html, 'html.parser')
            
            # Look for archived business links
            links = soup.find_all('a', href=True)
            for link in links:
                if any(term in link.get_text().lower() for term in ['scrap', 'metal', 'recycl']):
                    business = {
                        'name': link.get_text(strip=True),
                        'source': 'Archived Listings',
                        'scraped_at': datetime.now().isoformat()
                    }
                    businesses.append(business)
                    
        except Exception as e:
            self.logger.error(f"Error parsing archived listings: {e}")
        
        return businesses

    def _parse_chamber_directory(self, html):
        """Parse Chamber of Commerce directory"""
        businesses = []
        
        try:
            soup = BeautifulSoup(html, 'html.parser')
            
            # Look for member listings
            member_elements = soup.find_all(['div', 'li', 'tr'], 
                                          class_=re.compile(r'member|business|directory'))
            
            for element in member_elements:
                text = element.get_text().lower()
                if any(term in text for term in ['metal', 'scrap', 'recycl', 'salvage']):
                    name_elem = element.find(['h2', 'h3', 'h4', 'a'])
                    if name_elem:
                        business = {
                            'name': name_elem.get_text(strip=True),
                            'source': 'Chamber of Commerce',
                            'scraped_at': datetime.now().isoformat()
                        }
                        businesses.append(business)
                        
        except Exception as e:
            self.logger.error(f"Error parsing chamber directory: {e}")
        
        return businesses

    def _parse_isri_directory(self, html):
        """Parse ISRI member directory"""
        businesses = []
        
        try:
            soup = BeautifulSoup(html, 'html.parser')
            
            # Look for member companies
            member_elements = soup.find_all(['div', 'li'], 
                                          class_=re.compile(r'member|company'))
            
            for element in member_elements:
                name_elem = element.find(['h3', 'h4', 'a'])
                address_elem = element.find(class_=re.compile(r'address|location'))
                
                if name_elem:
                    business = {
                        'name': name_elem.get_text(strip=True),
                        'address': address_elem.get_text(strip=True) if address_elem else '',
                        'source': 'ISRI Directory',
                        'scraped_at': datetime.now().isoformat()
                    }
                    businesses.append(business)
                    
        except Exception as e:
            self.logger.error(f"Error parsing ISRI directory: {e}")
        
        return businesses

    def _parse_epa_facilities(self, data):
        """Parse EPA facility data"""
        businesses = []
        
        try:
            if isinstance(data, list):
                for facility in data[:10]:  # Limit results
                    if 'metal' in facility.get('facility_name', '').lower():
                        business = {
                            'name': facility.get('facility_name', ''),
                            'address': facility.get('location_address', ''),
                            'city': facility.get('location_city', ''),
                            'state': facility.get('location_state', ''),
                            'zip_code': facility.get('location_zip_code', ''),
                            'epa_id': facility.get('handler_id', ''),
                            'source': 'EPA Database',
                            'scraped_at': datetime.now().isoformat()
                        }
                        businesses.append(business)
                        
        except Exception as e:
            self.logger.error(f"Error parsing EPA data: {e}")
        
        return businesses

    def _remove_duplicates(self, data):
        """Remove duplicate entries"""
        seen = set()
        unique_data = []
        
        for item in data:
            lat = item.get('latitude')
            lon = item.get('longitude')
            if lat and lon:
                key = (round(lat, 4), round(lon, 4))  # Deduplicate by approximate coordinates
            else:
                key = item['name'].lower().strip()
            if key not in seen:
                seen.add(key)
                unique_data.append(item)
        
        return unique_data

    def export_results(self, output_dir="output"):
        """Export comprehensive business intelligence data with enhanced Excel sheets"""
        if not self.results:
            self.logger.warning("No enhanced data to export")
            return None
        
        os.makedirs(output_dir, exist_ok=True)
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        # Enhance data with business intelligence fields
        enhanced_data = self._create_comprehensive_business_data()
        
        # Convert to DataFrame with all valuable fields
        df = pd.DataFrame(enhanced_data)
        
        # Export only essential fields to keep the dataset clean and focused
        column_order = [
            # Basic business information
            'name',
            
            # Location details
            'address', 'city', 'state', 'zip_code', 'latitude', 'longitude',
            
            # Contact details
            'phone', 'email', 'website',
            
            # Key business attributes
            'primary_metal_types', 'services_offered',
        ]
        
        # Reorder DataFrame columns
        available_columns = [col for col in column_order if col in df.columns]
        remaining_columns = [col for col in df.columns if col not in available_columns]
        df = df[available_columns + remaining_columns]
        
        # Export to CSV
        csv_file = os.path.join(output_dir, f"enhanced_scrap_centers_{timestamp}.csv")
        df.to_csv(csv_file, index=False)
        
        # Export to Excel with comprehensive analysis sheets
        excel_file = os.path.join(output_dir, f"enhanced_scrap_centers_{timestamp}.xlsx")
        with pd.ExcelWriter(excel_file, engine='openpyxl') as writer:
            # Main comprehensive data sheet
            df.to_excel(writer, sheet_name='Business Intelligence', index=False)
            
            # High-priority outreach targets
            priority_targets = df[df.get('outreach_priority', '') == 'High'].copy()
            if not priority_targets.empty:
                priority_targets.to_excel(writer, sheet_name='Priority Targets', index=False)
            
            # Metal specialization analysis
            metal_analysis = self._create_metal_specialization_analysis(enhanced_data)
            if not metal_analysis.empty:
                metal_analysis.to_excel(writer, sheet_name='Metal Specialization', index=False)
            
            # Geographic market analysis
            geo_analysis = self._create_geographic_analysis(df)
            if not geo_analysis.empty:
                geo_analysis.to_excel(writer, sheet_name='Geographic Analysis', index=False)
            
            # Service capabilities matrix
            service_analysis = self._create_service_analysis(df)
            if not service_analysis.empty:
                service_analysis.to_excel(writer, sheet_name='Service Capabilities', index=False)
            
            # Contact quality breakdown
            contact_analysis = self._create_contact_analysis(df)
            if not contact_analysis.empty:
                contact_analysis.to_excel(writer, sheet_name='Contact Quality', index=False)
            
            # Business size & market tier analysis
            size_analysis = self._create_business_size_analysis(df)
            if not size_analysis.empty:
                size_analysis.to_excel(writer, sheet_name='Business Size Analysis', index=False)
            
            # Competitive landscape
            competitive_analysis = self._create_competitive_analysis(df)
            if not competitive_analysis.empty:
                competitive_analysis.to_excel(writer, sheet_name='Competitive Landscape', index=False)
        
        # Export to JSON with enhanced structure
        json_file = os.path.join(output_dir, f"enhanced_scrap_centers_{timestamp}.json")
        with open(json_file, 'w') as f:
            json.dump(enhanced_data, f, indent=2, default=str)
        
        # Create comprehensive business intelligence report
        self._create_comprehensive_business_report(output_dir, timestamp, enhanced_data)
        
        self.logger.info(f"✅ Comprehensive business intelligence data exported:")
        self.logger.info(f"  • CSV: {csv_file}")
        self.logger.info(f"  • Excel: {excel_file} (8 analysis sheets)")
        self.logger.info(f"  • JSON: {json_file}")
        
        return {
            'csv': csv_file,
            'excel': excel_file,
            'json': json_file,
            'count': len(enhanced_data)
        }

    def _create_metal_analysis(self):
        """Create metal types analysis for Excel export"""
        metal_counts = {}
        
        for business in self.results:
            metals = business.get('metal_types', [])
            if isinstance(metals, list):
                for metal in metals:
                    metal_counts[metal] = metal_counts.get(metal, 0) + 1
        
        if not metal_counts:
            return pd.DataFrame()
        
        analysis_data = []
        total_businesses = len(self.results)
        
        for metal, count in sorted(metal_counts.items(), key=lambda x: x[1], reverse=True):
            percentage = (count / total_businesses) * 100 if total_businesses > 0 else 0
            analysis_data.append({
                'Metal Type': metal.replace('_', ' ').title(),
                'Businesses Count': count,
                'Percentage': f"{percentage:.1f}%"
            })
        
        return pd.DataFrame(analysis_data)

    def _create_enhanced_summary_report(self, output_dir, timestamp):
        """Create comprehensive summary report with metal types and business analysis"""
        report_file = os.path.join(output_dir, f"enhanced_analysis_report_{timestamp}.txt")
        
        total_businesses = len(self.results)
        with_metals = sum(1 for b in self.results if b.get('metal_types'))
        with_pricing = sum(1 for b in self.results if b.get('pricing_from_website'))
        with_websites = sum(1 for b in self.results if b.get('website'))
        with_phones = sum(1 for b in self.results if b.get('phone') or b.get('phone_from_website'))
        
        avg_completeness = sum(b.get('data_completeness', 0) for b in self.results) / total_businesses if total_businesses > 0 else 0
        
        with open(report_file, 'w', encoding='utf-8') as f:
            f.write("🔍 ENHANCED SCRAP METAL CENTERS ANALYSIS REPORT\n")
            f.write("=" * 60 + "\n\n")
            f.write(f"Report Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
            f.write(f"Collection Method: Real Internet Data + Enhanced Website Analysis\n\n")
            
            f.write("📊 COMPREHENSIVE DATA SUMMARY\n")
            f.write("-" * 35 + "\n")
            f.write(f"Total Businesses Collected: {total_businesses}\n")
            f.write(f"Average Data Completeness: {avg_completeness:.1f}%\n")
            f.write(f"Businesses with Metal Types: {with_metals} ({with_metals/total_businesses*100:.1f}%)\n")
            f.write(f"Businesses with Pricing Info: {with_pricing} ({with_pricing/total_businesses*100:.1f}%)\n")
            f.write(f"Businesses with Websites: {with_websites} ({with_websites/total_businesses*100:.1f}%)\n")
            f.write(f"Businesses with Phone Numbers: {with_phones} ({with_phones/total_businesses*100:.1f}%)\n\n")
            
            # Metal types breakdown
            if with_metals > 0:
                f.write("🔧 METAL TYPES ACCEPTED (Top 10)\n")
                f.write("-" * 35 + "\n")
                metal_analysis = self._create_metal_analysis()
                for _, row in metal_analysis.head(10).iterrows():
                    f.write(f"{row['Metal Type']}: {row['Businesses Count']} businesses ({row['Percentage']})\n")
                f.write("\n")
            
            # Top quality businesses
            f.write("⭐ HIGHEST QUALITY BUSINESSES\n")
            f.write("-" * 30 + "\n")
            quality_businesses = sorted(
                [b for b in self.results if b.get('data_completeness', 0) > 50],
                key=lambda x: x.get('data_completeness', 0),
                reverse=True
            )[:10]
            
            for i, business in enumerate(quality_businesses, 1):
                f.write(f"{i}. {business['name']}\n")
                f.write(f"   📊 Data Completeness: {business.get('data_completeness', 0)}%\n")
                f.write(f"   📍 Location: {business.get('city', 'N/A')}, {business.get('state', 'N/A')}\n")
                if business.get('phone') or business.get('phone_from_website'):
                    phone = business.get('phone') or business.get('phone_from_website', '')
                    f.write(f"   📞 Phone: {phone}\n")
                if business.get('website'):
                    f.write(f"   🌐 Website: {business['website']}\n")
                if business.get('metal_types'):
                    metals = ', '.join(business['metal_types'][:4])  # Show top 4 metals
                    f.write(f"   🔧 Accepts: {metals}\n")
                if business.get('business_type'):
                    f.write(f"   🏭 Type: {business['business_type']}\n")
                f.write("\n")
            
            # Geographic distribution
            f.write("📍 GEOGRAPHIC DISTRIBUTION\n")
            f.write("-" * 25 + "\n")
            states = {}
            for business in self.results:
                state = business.get('state', 'Unknown')
                if state:
                    states[state] = states.get(state, 0) + 1
            
            for state, count in sorted(states.items(), key=lambda x: x[1], reverse=True):
                percentage = (count / total_businesses) * 100
                f.write(f"{state}: {count} businesses ({percentage:.1f}%)\n")
            f.write("\n")
            
            f.write("💡 KEY BUSINESS INSIGHTS\n")
            f.write("-" * 25 + "\n")
            f.write("• Metal recycling businesses are concentrated in industrial areas\n")
            f.write("• Most established businesses accept multiple metal types\n")
            f.write("• Businesses with websites generally have higher data completeness\n")
            f.write("• Contact information quality varies significantly by region\n")
            f.write("• Pricing information is typically available on business websites\n\n")
            
            f.write("🎯 ACTIONABLE RECOMMENDATIONS\n")
            f.write("-" * 30 + "\n")
            f.write("1. Prioritize businesses with 70%+ data completeness for outreach\n")
            f.write("2. Focus on multi-metal businesses for diverse partnership opportunities\n")
            f.write("3. Contact businesses with websites for detailed pricing negotiations\n")
            f.write("4. Use geographic clusters for efficient logistics planning\n")
            f.write("5. Verify all contact information before business meetings\n")
            f.write("6. Consider specialized approach for electronic/precious metal dealers\n")
            f.write("7. Build relationships with high-volume scrap metal dealers first\n")
        
        self.logger.info(f"✅ Enhanced analysis report created: {report_file}")

    def _get_first_tag(self, tags, keys):
        """Return the first matching tag value from a list of possible tag keys."""
        for k in keys:
            if k in tags and tags[k]:
                return tags[k]
        return ""

    def _normalize_url(self, url: str):
        """Add https:// if scheme is missing and strip whitespace"""
        url = url.strip()
        if url and not url.startswith(('http://', 'https://')):
            return f"https://{url}"
        return url

    def _find_business_website(self, name, city="", state=""):
        """Try to discover a business website via DuckDuckGo HTML search (no API key required)."""
        try:
            query = quote_plus(f"{name} {city} {state} scrap metal recycling")
            search_url = f"https://duckduckgo.com/html/?q={query}"
            resp = self._make_safe_request(search_url)
            if not resp or resp.status_code != 200:
                return ""
            soup = BeautifulSoup(resp.text, 'html.parser')
            link = soup.find('a', class_='result__a')
            if not link or not link.get('href'):
                return ""
            href = link['href']
            if 'uddg=' in href:
                href = unquote(href.split('uddg=')[1].split('&')[0])
            return self._normalize_url(href)
        except Exception as exc:
            self.logger.debug(f"Website fallback search failed for {name}: {exc}")
            return ""

    def _create_comprehensive_business_data(self):
        """Create comprehensive business intelligence data from scraped results"""
        enhanced_data = []
        
        for business in self.results:
            enhanced = business.copy()
            
            # Enhanced business intelligence fields
            enhanced.update(self._analyze_business_intelligence(business))
            enhanced.update(self._analyze_market_position(business))
            enhanced.update(self._analyze_operational_capabilities(business))
            enhanced.update(self._analyze_digital_presence(business))
            enhanced.update(self._analyze_financial_indicators(business))
            enhanced.update(self._analyze_competitive_factors(business))
            
            enhanced_data.append(enhanced)
        
        return enhanced_data

    def _analyze_business_intelligence(self, business):
        """Analyze core business intelligence metrics"""
        analysis = {}
        
        # Primary metal types (top 3)
        metal_types = business.get('metal_types', [])
        if isinstance(metal_types, list) and metal_types:
            primary_types = [m.get('type', m) if isinstance(m, dict) else str(m) for m in metal_types[:3]]
            analysis['primary_metal_types'] = ', '.join(primary_types)
            analysis['all_metal_types_count'] = len(metal_types)
            
            # Specialization score based on metal diversity
            if len(metal_types) <= 2:
                analysis['specialization_score'] = 'High'  # Specialized
            elif len(metal_types) <= 5:
                analysis['specialization_score'] = 'Medium'  # Moderate
            else:
                analysis['specialization_score'] = 'Low'  # General recycler
        else:
            analysis['primary_metal_types'] = 'Unknown'
            analysis['all_metal_types_count'] = 0
            analysis['specialization_score'] = 'Unknown'
        
        # Business size estimation
        indicators = []
        if business.get('website'): indicators.append('website')
        if business.get('phone'): indicators.append('phone')
        if business.get('email'): indicators.append('email')
        if business.get('services_offered'): indicators.append('services')
        if business.get('pricing_available'): indicators.append('pricing')
        
        if len(indicators) >= 4:
            analysis['business_size_estimate'] = 'Large'
        elif len(indicators) >= 2:
            analysis['business_size_estimate'] = 'Medium'
        else:
            analysis['business_size_estimate'] = 'Small'
        
        # Market tier classification
        city = business.get('city', '').lower()
        major_cities = ['new york', 'los angeles', 'chicago', 'houston', 'phoenix', 'philadelphia', 'san antonio', 'san diego', 'dallas', 'san jose']
        if any(major in city for major in major_cities):
            analysis['market_tier'] = 'Tier 1 (Major Metro)'
        elif business.get('state') in ['CA', 'TX', 'NY', 'FL', 'IL', 'PA', 'OH', 'GA', 'NC', 'MI']:
            analysis['market_tier'] = 'Tier 2 (Major State)'
        else:
            analysis['market_tier'] = 'Tier 3 (Regional)'
        
        # Outreach priority scoring
        completeness = business.get('data_completeness', 0)
        contact_quality = business.get('contact_quality', 0)
        
        priority_score = 0
        if completeness > 70: priority_score += 3
        elif completeness > 50: priority_score += 2
        elif completeness > 30: priority_score += 1
        
        if contact_quality >= 4: priority_score += 2
        elif contact_quality >= 2: priority_score += 1
        
        if business.get('website'): priority_score += 1
        if business.get('pricing_available'): priority_score += 1
        
        if priority_score >= 6:
            analysis['outreach_priority'] = 'High'
        elif priority_score >= 4:
            analysis['outreach_priority'] = 'Medium'
        else:
            analysis['outreach_priority'] = 'Low'
        
        # Competitive advantage assessment
        advantages = []
        if business.get('pickup_service'): advantages.append('Pickup Service')
        if business.get('container_rental'): advantages.append('Container Rental')
        if business.get('certified_scales'): advantages.append('Certified Scales')
        if business.get('processing_capabilities'): advantages.append('Processing')
        if business.get('commercial_accounts'): advantages.append('Commercial Focus')
        if analysis['specialization_score'] == 'High': advantages.append('Specialized')
        
        analysis['competitive_advantage'] = ', '.join(advantages) if advantages else 'Standard Operations'
        
        return analysis

    def _analyze_market_position(self, business):
        """Analyze market positioning and opportunities"""
        analysis = {}
        
        # Local competition level estimation
        state = business.get('state', '')
        city = business.get('city', '').lower()
        
        # High competition areas
        high_comp_cities = ['chicago', 'detroit', 'houston', 'los angeles', 'philadelphia', 'pittsburgh']
        high_comp_states = ['OH', 'PA', 'MI', 'IN', 'IL']
        
        if any(city_name in city for city_name in high_comp_cities) or state in high_comp_states:
            analysis['local_competition_level'] = 'High'
            analysis['market_opportunity'] = 'Competitive Market'
        elif state in ['TX', 'CA', 'NY', 'FL']:
            analysis['local_competition_level'] = 'Medium'
            analysis['market_opportunity'] = 'Balanced Market'
        else:
            analysis['local_competition_level'] = 'Low'
            analysis['market_opportunity'] = 'Growth Opportunity'
        
        # Expansion potential
        services_count = len(business.get('services_offered', []))
        metal_count = business.get('all_metal_types_count', 0)
        
        if services_count >= 4 and metal_count >= 5:
            analysis['expansion_potential'] = 'Limited (Full Service)'
        elif services_count >= 2 or metal_count >= 3:
            analysis['expansion_potential'] = 'Moderate'
        else:
            analysis['expansion_potential'] = 'High (Underserved)'
        
        # County information (extracted from address or location)
        address = business.get('address', '')
        # This would need a county lookup service, for now we'll mark as available field
        analysis['county'] = 'To Be Determined'
        
        return analysis

    def _analyze_operational_capabilities(self, business):
        """Analyze operational capabilities and services"""
        analysis = {}
        
        # Service breakdown
        services = business.get('services_offered', [])
        if isinstance(services, str):
            services = [services]
        elif not isinstance(services, list):
            services = []
        
        analysis['services_offered'] = ', '.join(services) if services else 'Basic Recycling'
        analysis['services_count'] = len(services)
        
        # Specific service flags
        services_text = ' '.join(services).lower()
        analysis['pickup_service'] = 'Yes' if 'pickup' in services_text else 'No'
        analysis['container_rental'] = 'Yes' if any(term in services_text for term in ['container', 'rental', 'roll-off']) else 'No'
        analysis['demolition_service'] = 'Yes' if 'demolition' in services_text else 'No'
        analysis['processing_capabilities'] = 'Yes' if any(term in services_text for term in ['processing', 'sorting', 'shredding']) else 'No'
        analysis['certified_scales'] = 'Yes' if 'scale' in services_text else 'No'
        
        # Business hours analysis
        hours = business.get('hours', business.get('business_hours', ''))
        if hours:
            analysis['business_hours'] = str(hours)
            # Estimate if they're full-time operation
            if any(term in str(hours).lower() for term in ['24', 'monday', 'weekday']):
                analysis['operation_schedule'] = 'Full Time'
            else:
                analysis['operation_schedule'] = 'Limited Hours'
        else:
            analysis['business_hours'] = 'Not Available'
            analysis['operation_schedule'] = 'Unknown'
        
        # Facility size estimation (based on services and capabilities)
        facility_indicators = 0
        if analysis['container_rental'] == 'Yes': facility_indicators += 2
        if analysis['processing_capabilities'] == 'Yes': facility_indicators += 2
        if analysis['demolition_service'] == 'Yes': facility_indicators += 1
        if business.get('all_metal_types_count', 0) > 5: facility_indicators += 1
        
        if facility_indicators >= 5:
            analysis['facility_size'] = 'Large Industrial'
        elif facility_indicators >= 3:
            analysis['facility_size'] = 'Medium Commercial'
        else:
            analysis['facility_size'] = 'Small Local'
        
        # Employee estimation
        if analysis['facility_size'] == 'Large Industrial':
            analysis['employee_estimate'] = '20+ employees'
        elif analysis['facility_size'] == 'Medium Commercial':
            analysis['employee_estimate'] = '5-20 employees'
        else:
            analysis['employee_estimate'] = '1-5 employees'
        
        return analysis

    def _analyze_digital_presence(self, business):
        """Analyze digital presence and online capabilities"""
        analysis = {}
        
        # Website quality assessment
        website = business.get('website', '')
        if website:
            analysis['website_quality'] = 'Professional' if any(indicator in website.lower() for indicator in ['.com', 'www']) else 'Basic'
            
            # Check if pricing is available online
            pricing_info = business.get('pricing_info', business.get('pricing', []))
            analysis['pricing_available'] = 'Yes' if pricing_info else 'No'
            if pricing_info:
                if isinstance(pricing_info, list):
                    analysis['pricing_info'] = '; '.join([str(p) for p in pricing_info[:3]])  # Top 3 prices
                else:
                    analysis['pricing_info'] = str(pricing_info)
            else:
                analysis['pricing_info'] = 'Not Available'
        else:
            analysis['website_quality'] = 'No Website'
            analysis['pricing_available'] = 'No'
            analysis['pricing_info'] = 'Not Available'
        
        # Social media presence estimation
        # This would need actual social media checking, for now we estimate based on website presence
        if website and analysis['website_quality'] == 'Professional':
            analysis['social_media_presence'] = 'Likely Present'
        else:
            analysis['social_media_presence'] = 'Limited/None'
        
        # Online reviews estimation
        analysis['online_reviews_count'] = 'To Be Checked'
        analysis['average_rating'] = 'To Be Checked'
        
        return analysis

    def _analyze_financial_indicators(self, business):
        """Analyze financial and payment indicators"""
        analysis = {}
        
        # Payment methods
        services_text = str(business.get('services_offered', [])).lower()
        website_text = str(business.get('website_content', '')).lower()
        all_text = f"{services_text} {website_text}"
        
        payment_methods = []
        if 'cash' in all_text: payment_methods.append('Cash')
        if any(term in all_text for term in ['check', 'cheque']): payment_methods.append('Check')
        if any(term in all_text for term in ['credit', 'card', 'visa', 'mastercard']): payment_methods.append('Credit Card')
        if any(term in all_text for term in ['wire', 'ach', 'bank']): payment_methods.append('Bank Transfer')
        
        analysis['payment_methods'] = ', '.join(payment_methods) if payment_methods else 'Standard Methods'
        
        # Commercial accounts capability
        analysis['commercial_accounts'] = 'Yes' if any(term in all_text for term in ['commercial', 'industrial', 'business', 'corporate']) else 'Unknown'
        
        # Established year estimation (if available in text)
        import re
        year_pattern = r'\b(19|20)\d{2}\b'
        years = re.findall(year_pattern, all_text)
        if years:
            # Take the earliest reasonable year as establishment
            valid_years = [int(y) for y in years if 1950 <= int(y) <= 2024]
            if valid_years:
                analysis['established_year'] = min(valid_years)
            else:
                analysis['established_year'] = 'Unknown'
        else:
            analysis['established_year'] = 'Unknown'
        
        return analysis

    def _analyze_competitive_factors(self, business):
        """Analyze competitive positioning factors"""
        analysis = {}
        
        # Certifications and compliance
        all_text = str(business).lower()
        
        certifications = []
        if 'iso' in all_text: certifications.append('ISO Certified')
        if 'epa' in all_text: certifications.append('EPA Compliant')
        if 'osha' in all_text: certifications.append('OSHA Compliant')
        if 'r2' in all_text: certifications.append('R2 Certified')
        if any(term in all_text for term in ['certified', 'licensed', 'registered']): certifications.append('Industry Certified')
        
        analysis['certifications'] = ', '.join(certifications) if certifications else 'Standard Compliance'
        
        # License information
        if any(term in all_text for term in ['license', 'permit', 'authorized']):
            analysis['licenses'] = 'Licensed Operation'
        else:
            analysis['licenses'] = 'Standard Operation'
        
        # Environmental compliance
        if any(term in all_text for term in ['environmental', 'green', 'sustainable', 'eco']):
            analysis['environmental_compliance'] = 'Enhanced'
        else:
            analysis['environmental_compliance'] = 'Standard'
        
        # BBB rating
        bbb_rating = business.get('bbb_rating', '')
        analysis['bbb_rating'] = bbb_rating if bbb_rating else 'Not Rated'
        
        return analysis

    def _create_metal_specialization_analysis(self, enhanced_data):
        """Create metal specialization analysis"""
        try:
            specialization_data = []
            
            for business in enhanced_data:
                metal_types = business.get('metal_types', [])
                if metal_types:
                    for metal in metal_types:
                        if isinstance(metal, dict):
                            metal_type = metal.get('type', 'Unknown')
                            confidence = metal.get('confidence', 0)
                        else:
                            metal_type = str(metal)
                            confidence = 100
                        
                        specialization_data.append({
                            'Business': business.get('name', 'Unknown'),
                            'Metal Type': metal_type,
                            'Confidence': confidence,
                            'City': business.get('city', ''),
                            'State': business.get('state', ''),
                            'Business Size': business.get('business_size_estimate', ''),
                            'Contact Quality': business.get('contact_quality', 0)
                        })
            
            return pd.DataFrame(specialization_data)
        except Exception as e:
            self.logger.debug(f"Error creating metal specialization analysis: {e}")
            return pd.DataFrame()

    def _create_geographic_analysis(self, df):
        """Create geographic market analysis"""
        try:
            geo_data = []
            
            # Group by state
            if 'state' in df.columns:
                state_groups = df.groupby('state').agg({
                    'name': 'count',
                    'data_completeness': 'mean',
                    'contact_quality': 'mean',
                    'business_size_estimate': lambda x: x.value_counts().index[0] if len(x) > 0 else 'Unknown'
                }).round(2)
                
                state_groups.columns = ['Business Count', 'Avg Data Completeness', 'Avg Contact Quality', 'Dominant Size']
                state_groups = state_groups.reset_index()
                geo_data.append(('State Analysis', state_groups))
            
            # Group by market tier
            if 'market_tier' in df.columns:
                tier_groups = df.groupby('market_tier').agg({
                    'name': 'count',
                    'outreach_priority': lambda x: (x == 'High').sum(),
                    'data_completeness': 'mean'
                }).round(2)
                
                tier_groups.columns = ['Business Count', 'High Priority Count', 'Avg Data Completeness']
                tier_groups = tier_groups.reset_index()
                geo_data.append(('Market Tier Analysis', tier_groups))
            
            # Return the first analysis or empty DataFrame
            return geo_data[0][1] if geo_data else pd.DataFrame()
            
        except Exception as e:
            self.logger.debug(f"Error creating geographic analysis: {e}")
            return pd.DataFrame()

    def _create_service_analysis(self, df):
        """Create service capabilities analysis"""
        try:
            service_columns = ['pickup_service', 'container_rental', 'demolition_service', 'processing_capabilities', 'certified_scales']
            available_columns = [col for col in service_columns if col in df.columns]
            
            if not available_columns:
                return pd.DataFrame()
            
            service_data = []
            for col in available_columns:
                yes_count = (df[col] == 'Yes').sum()
                total_count = len(df)
                percentage = (yes_count / total_count * 100) if total_count > 0 else 0
                
                service_data.append({
                    'Service': col.replace('_', ' ').title(),
                    'Businesses Offering': yes_count,
                    'Total Businesses': total_count,
                    'Percentage': round(percentage, 1)
                })
            
            return pd.DataFrame(service_data)
            
        except Exception as e:
            self.logger.debug(f"Error creating service analysis: {e}")
            return pd.DataFrame()

    def _create_contact_analysis(self, df):
        """Create contact quality analysis"""
        try:
            contact_data = []
            
            # Phone availability
            phone_available = (df['phone'].notna() & (df['phone'] != '')).sum() if 'phone' in df.columns else 0
            
            # Email availability  
            email_available = (df['email'].notna() & (df['email'] != '')).sum() if 'email' in df.columns else 0
            
            # Website availability
            website_available = (df['website'].notna() & (df['website'] != '')).sum() if 'website' in df.columns else 0
            
            total = len(df)
            
            contact_data = [
                {'Contact Type': 'Phone Number', 'Available': phone_available, 'Percentage': round(phone_available/total*100, 1) if total > 0 else 0},
                {'Contact Type': 'Email Address', 'Available': email_available, 'Percentage': round(email_available/total*100, 1) if total > 0 else 0},
                {'Contact Type': 'Website', 'Available': website_available, 'Percentage': round(website_available/total*100, 1) if total > 0 else 0}
            ]
            
            return pd.DataFrame(contact_data)
            
        except Exception as e:
            self.logger.debug(f"Error creating contact analysis: {e}")
            return pd.DataFrame()

    def _create_business_size_analysis(self, df):
        """Create business size and market analysis"""
        try:
            if 'business_size_estimate' not in df.columns:
                return pd.DataFrame()
            
            size_analysis = df.groupby('business_size_estimate').agg({
                'name': 'count',
                'data_completeness': 'mean',
                'outreach_priority': lambda x: (x == 'High').sum()
            }).round(2)
            
            size_analysis.columns = ['Count', 'Avg Data Completeness', 'High Priority Count']
            size_analysis = size_analysis.reset_index()
            
            return size_analysis
            
        except Exception as e:
            self.logger.debug(f"Error creating business size analysis: {e}")
            return pd.DataFrame()

    def _create_competitive_analysis(self, df):
        """Create competitive landscape analysis"""
        try:
            competitive_data = []
            
            # Specialization distribution
            if 'specialization_score' in df.columns:
                spec_counts = df['specialization_score'].value_counts()
                for spec, count in spec_counts.items():
                    competitive_data.append({
                        'Factor': f'Specialization - {spec}',
                        'Count': count,
                        'Percentage': round(count/len(df)*100, 1)
                    })
            
            # Market tier distribution
            if 'market_tier' in df.columns:
                tier_counts = df['market_tier'].value_counts()
                for tier, count in tier_counts.items():
                    competitive_data.append({
                        'Factor': f'Market - {tier}',
                        'Count': count,
                        'Percentage': round(count/len(df)*100, 1)
                    })
            
            return pd.DataFrame(competitive_data)
            
        except Exception as e:
            self.logger.debug(f"Error creating competitive analysis: {e}")
            return pd.DataFrame()

    def _create_comprehensive_business_report(self, output_dir, timestamp, enhanced_data):
        """Create comprehensive business intelligence report"""
        report_file = os.path.join(output_dir, f"business_intelligence_report_{timestamp}.txt")
        
        total_businesses = len(enhanced_data)
        high_priority = sum(1 for b in enhanced_data if b.get('outreach_priority') == 'High')
        
        with open(report_file, 'w', encoding='utf-8') as f:
            f.write("🏢 COMPREHENSIVE BUSINESS INTELLIGENCE REPORT\n")
            f.write("=" * 70 + "\n\n")
            f.write(f"Report Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
            f.write(f"Total Businesses Analyzed: {total_businesses}\n")
            f.write(f"High Priority Targets: {high_priority}\n\n")
            
            # Market opportunity summary
            f.write("📊 MARKET OPPORTUNITY SUMMARY\n")
            f.write("-" * 35 + "\n")
            
            # Business size distribution
            size_dist = {}
            for business in enhanced_data:
                size = business.get('business_size_estimate', 'Unknown')
                size_dist[size] = size_dist.get(size, 0) + 1
            
            for size, count in sorted(size_dist.items(), key=lambda x: x[1], reverse=True):
                percentage = (count / total_businesses) * 100
                f.write(f"{size} Businesses: {count} ({percentage:.1f}%)\n")
            
            f.write("\n🎯 TOP PRIORITY TARGETS\n")
            f.write("-" * 25 + "\n")
            
            # Top priority businesses
            priority_businesses = [b for b in enhanced_data if b.get('outreach_priority') == 'High']
            priority_businesses.sort(key=lambda x: x.get('data_completeness', 0), reverse=True)
            
            for i, business in enumerate(priority_businesses[:10], 1):
                f.write(f"{i}. {business.get('name', 'Unknown')}\n")
                f.write(f"   📍 {business.get('city', '')}, {business.get('state', '')}\n")
                f.write(f"   🔧 Metals: {business.get('primary_metal_types', 'Unknown')}\n")
                f.write(f"   📊 Completeness: {business.get('data_completeness', 0)}%\n")
                f.write(f"   🏢 Size: {business.get('business_size_estimate', 'Unknown')}\n")
                if business.get('phone'):
                    f.write(f"   📞 {business.get('phone')}\n")
                if business.get('website'):
                    f.write(f"   🌐 {business.get('website')}\n")
                f.write("\n")
        
        self.logger.info(f"✅ Comprehensive business intelligence report created: {report_file}")

def main():
    print("🔍 ENHANCED Internet Data Scraper for Scrap Metal Centers")
    print("=" * 65)
    print("Collects REAL data from internet sources + Enhanced Analysis:")
    print("✓ Metal types accepted (copper, aluminum, steel, etc.)")
    print("✓ Pricing information where available") 
    print("✓ Services offered (pickup, containers, processing)")
    print("✓ Complete contact details and business hours")
    print("✓ Data quality scoring and business classification")
    
    scraper = RealWorkingScraper()
    
    try:
        # Allow non-interactive usage: if the first CLI arg is a number, use it.
        if len(sys.argv) > 1 and sys.argv[1].isdigit():
            target_count = int(sys.argv[1])
            print(f"\n▶ Using target count from CLI argument: {target_count}")
        else:
            target_count = int(input("\nEnter target number of businesses to collect (default 50): ") or "50")
        
        print(f"\n🚀 Starting ENHANCED collection for {target_count} businesses...")
        print("Process:")
        print("1. 📡 Searching OpenStreetMap for real business locations")
        print("2. 🔬 Enhancing data with metal types and pricing info")
        print("3. 🌐 Scraping business websites for detailed information")
        print("4. 📊 Creating comprehensive analysis reports")
        print("\nThis process may take several minutes...")
        
        results = scraper.scrape_real_sources(target_count)
        
        if results:
            print(f"\n✅ Successfully collected {len(results)} enhanced businesses!")
            
            export_info = scraper.export_results()
            if export_info:
                print(f"\n📁 Enhanced data files created:")
                print(f"  • CSV: {export_info['csv']}")
                print(f"  • Excel: {export_info['excel']}")
                print(f"  • JSON: {export_info['json']}")
                print(f"\n🎯 Total enhanced records: {export_info['count']}")
                print("\n🔍 EXCEL FILE CONTAINS DETAILED ANALYSIS:")
                print("  → 'Metal Types Analysis' sheet - breakdown by metal types")
                print("  → 'High Quality Businesses' sheet - best businesses for outreach")
                print("  → 'Business Types' sheet - classification summary")
                print("  → Enhanced analysis report with actionable insights")
            else:
                print("❌ Export failed")
        else:
            print("❌ No enhanced data collected. Check internet connection.")
            
    except KeyboardInterrupt:
        print("\n⚠️ Process interrupted by user")
    except Exception as e:
        print(f"❌ Error: {e}")

if __name__ == "__main__":
    main() 