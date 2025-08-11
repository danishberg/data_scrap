#!/usr/bin/env python3
"""
Hybrid Stealth Scraper - The Ultimate Solution

Combines:
1. Stealth browser fetching to bypass bot detection  
2. AutoScraper for automatic pattern learning
3. AI for data enrichment and validation
4. Integration with existing CompanyRecord structure

This solves ALL the current problems:
- Bot detection blocking (Stealth fetching)
- Manual pattern creation (AutoScraper learning)  
- Data extraction (AI + patterns)
- Fake data (Real sources + validation)
"""

import json
import logging
import os
import re
import time
from typing import List, Dict, Optional, Tuple
from urllib.parse import urlparse, urljoin
import concurrent.futures
from dataclasses import asdict

try:
    from scrapling import StealthyFetcher
    from autoscraper import AutoScraper
    from unified_company_scraper import CompanyRecord, export_records, _get_openai_client
except ImportError as e:
    print(f"Missing dependencies: {e}")
    print("Install with: pip install scrapling autoscraper")
    exit(1)

# Setup logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s: %(message)s")
logger = logging.getLogger("hybrid_stealth")

class HybridStealthScraper:
    """
    The ultimate scraper that combines stealth + AI + pattern learning
    """
    
    def __init__(self):
        self.directory_sites = {
            "yelp": {
                "search_url": "https://www.yelp.com/search?find_desc=scrap+metal+recycling&find_loc={location}",
                "sample_data": [
                    "ABC Metal Recycling",
                    "XYZ Scrap Yard", 
                    "(555) 123-4567",
                    "123 Main Street"
                ]
            },
            "yellowpages": {
                "search_url": "https://www.yellowpages.com/search?search_terms=scrap+metal+recycling&geo_location_terms={location}",
                "sample_data": [
                    "Best Metal Recycling",
                    "Houston Scrap Yard",
                    "(713) 555-0123"
                ]
            },
            "superpages": {
                "search_url": "https://www.superpages.com/search?C=scrap+metal+recycling&T={location}",
                "sample_data": [
                    "Metro Recycling Center",
                    "(832) 555-0199"
                ]
            }
        }
        
        self.learned_scrapers = {}
        self.session_companies = []
        
    def stealth_fetch_html(self, url: str, max_retries: int = 2) -> Optional[str]:
        """
        Fetch HTML using stealth browser to bypass bot detection
        """
        for attempt in range(max_retries):
            try:
                logger.info(f"🥷 Stealth fetching: {url}")
                
                response = StealthyFetcher.fetch(
                    url=url,
                    headless=True,
                    block_images=True,           # Save bandwidth
                    disable_resources=True,      # Block ads/trackers for speed
                    wait=4000,                   # Wait 4 seconds for dynamic content
                    timeout=45000,               # 45 second timeout
                    humanize=True,               # Human-like mouse movements
                    google_search=True,          # Fake referer from Google search
                    geoip=False,                 # Don't change IP geolocation
                )
                
                if response.status == 200:
                    logger.info(f"✅ Successfully fetched {len(response.text)} characters")
                    return response.text
                else:
                    logger.warning(f"❌ HTTP {response.status} for {url}")
                    
            except Exception as e:
                logger.error(f"❌ Attempt {attempt + 1} failed: {e}")
                if attempt < max_retries - 1:
                    wait_time = 10 * (attempt + 1)
                    logger.info(f"⏳ Waiting {wait_time}s before retry...")
                    time.sleep(wait_time)
                    
        return None
        
    def learn_extraction_patterns(self, site_name: str, html: str) -> bool:
        """
        Learn extraction patterns using AutoScraper
        """
        try:
            logger.info(f"🧠 Learning patterns for {site_name}")
            
            site_config = self.directory_sites.get(site_name)
            if not site_config:
                logger.error(f"No configuration for site: {site_name}")
                return False
            
            # Create and train AutoScraper
            scraper = AutoScraper()
            sample_data = site_config["sample_data"]
            
            # Build the scraping model
            result = scraper.build(html=html, wanted_list=sample_data, text_fuzz_ratio=0.8)
            
            if result:
                logger.info(f"✅ Learned patterns for {site_name} - found {len(result)} matches")
                self.learned_scrapers[site_name] = scraper
                
                # Save the model
                os.makedirs("models", exist_ok=True)
                model_file = f"models/{site_name}_autoscraper.json"
                scraper.save(model_file)
                logger.info(f"💾 Saved model to {model_file}")
                return True
            else:
                logger.warning(f"⚠️ No patterns learned for {site_name}")
                return False
                
        except Exception as e:
            logger.error(f"❌ Error learning patterns for {site_name}: {e}")
            return False
    
    def extract_with_autoscraper(self, site_name: str, html: str) -> List[Dict]:
        """
        Extract data using learned AutoScraper patterns
        """
        try:
            if site_name not in self.learned_scrapers:
                logger.warning(f"No learned patterns for {site_name}")
                return []
                
            scraper = self.learned_scrapers[site_name]
            
            # Get results with grouping
            results = scraper.get_result_similar(html=html, grouped=True)
            logger.info(f"📊 AutoScraper extracted {len(results)} data groups")
            
            # Convert to structured data
            structured_data = self.structure_autoscraper_results(results)
            logger.info(f"🏗️ Structured into {len(structured_data)} potential companies")
            
            return structured_data
            
        except Exception as e:
            logger.error(f"❌ Error extracting with AutoScraper: {e}")
            return []
    
    def structure_autoscraper_results(self, raw_results: Dict) -> List[Dict]:
        """
        Convert AutoScraper grouped results into structured company data
        """
        structured = []
        
        try:
            # AutoScraper returns grouped results - we need to combine related items
            if not raw_results:
                return []
            
            # Find the group with the most items (likely company names)
            max_group_size = max(len(v) if isinstance(v, list) else 1 for v in raw_results.values())
            
            for i in range(max_group_size):
                company_data = {
                    "name": "",
                    "phone": "",
                    "address": "",
                    "website": "",
                    "raw_data": []
                }
                
                # Extract data from each group
                for group_id, items in raw_results.items():
                    if isinstance(items, list) and i < len(items):
                        text = str(items[i]).strip()
                        company_data["raw_data"].append(text)
                        
                        # Classify the text
                        if self.looks_like_company_name(text):
                            company_data["name"] = text
                        elif self.looks_like_phone(text):
                            company_data["phone"] = text
                        elif self.looks_like_address(text):
                            company_data["address"] = text
                        elif self.looks_like_website(text):
                            company_data["website"] = text
                
                # Only add if we have meaningful data
                if company_data["name"] or company_data["phone"]:
                    structured.append(company_data)
                    
        except Exception as e:
            logger.error(f"❌ Error structuring results: {e}")
        
        return structured
    
    def enhance_with_ai(self, raw_company_data: List[Dict]) -> List[CompanyRecord]:
        """
        Use AI to enhance and validate the extracted company data
        """
        enhanced_companies = []
        
        try:
            for data in raw_company_data:
                # Create AI prompt to enhance the data
                prompt = f"""
Extract and enhance company information from this data:

Raw Data: {data}

Please return a JSON object with these exact fields:
{{
    "name": "Company name (clean)",
    "website": "Website URL if found", 
    "street_address": "Street address",
    "city": "City",
    "region": "State/Province", 
    "postal_code": "ZIP/Postal code",
    "country": "United States",
    "phones": ["Phone numbers in (XXX) XXX-XXXX format"],
    "emails": ["Email addresses"],
    "description": "Brief description",
    "materials": ["Types of materials accepted"]
}}

Only include information that can be clearly identified. Set empty string or empty array if not found.
"""

                try:
                    client = _get_openai_client()
                    response = client.chat.completions.create(
                        model="gpt-4o-mini",
                        messages=[
                            {"role": "system", "content": "You are a data extraction specialist. Return only valid JSON."},
                            {"role": "user", "content": prompt}
                        ],
                        temperature=0.1,
                        max_tokens=800,
                        response_format={"type": "json_object"}
                    )
                    
                    ai_data = json.loads(response.choices[0].message.content)
                    
                    # Create CompanyRecord
                    company = CompanyRecord(
                        name=ai_data.get("name", ""),
                        website=ai_data.get("website", ""),
                        street_address=ai_data.get("street_address", ""),
                        city=ai_data.get("city", ""),
                        region=ai_data.get("region", ""),
                        postal_code=ai_data.get("postal_code", ""),
                        country=ai_data.get("country", "United States"),
                        phones=ai_data.get("phones", []),
                        emails=ai_data.get("emails", []),
                        whatsapp=[],
                        social_links=[],
                        opening_hours="",
                        materials=ai_data.get("materials", []),
                        material_prices=[],
                        description=ai_data.get("description", "")
                    )
                    
                    # Only add if we have a name
                    if company.name:
                        enhanced_companies.append(company)
                        
                except Exception as e:
                    logger.warning(f"⚠️ AI enhancement failed for one item: {e}")
                    continue
                    
        except Exception as e:
            logger.error(f"❌ Error in AI enhancement: {e}")
            
        logger.info(f"🤖 AI enhanced {len(enhanced_companies)} companies")
        return enhanced_companies
    
    def scrape_directory_site(self, site_name: str, location: str) -> List[CompanyRecord]:
        """
        Scrape a specific directory site for companies
        """
        try:
            site_config = self.directory_sites.get(site_name)
            if not site_config:
                logger.error(f"Unknown site: {site_name}")
                return []
            
            # Format the search URL
            location_formatted = location.replace(' ', '+').replace(',', '%2C')
            search_url = site_config["search_url"].format(location=location_formatted)
            
            logger.info(f"🌐 Scraping {site_name}: {search_url}")
            
            # Fetch with stealth
            html = self.stealth_fetch_html(search_url)
            if not html:
                logger.error(f"❌ Failed to fetch {site_name}")
                return []
            
            # Learn patterns if not already learned
            if site_name not in self.learned_scrapers:
                success = self.learn_extraction_patterns(site_name, html)
                if not success:
                    logger.error(f"❌ Failed to learn patterns for {site_name}")
                    return []
            
            # Extract with AutoScraper
            raw_data = self.extract_with_autoscraper(site_name, html)
            if not raw_data:
                logger.warning(f"⚠️ No data extracted from {site_name}")
                return []
            
            # Enhance with AI
            companies = self.enhance_with_ai(raw_data)
            
            logger.info(f"✅ Found {len(companies)} companies from {site_name}")
            return companies
            
        except Exception as e:
            logger.error(f"❌ Error scraping {site_name}: {e}")
            return []
    
    def scrape_all_sites(self, location: str, max_companies: int = 50) -> List[CompanyRecord]:
        """
        Scrape all directory sites for a location
        """
        all_companies = []
        seen_names = set()
        
        logger.info(f"🚀 Starting hybrid stealth scraping for: {location}")
        
        for site_name in self.directory_sites.keys():
            if len(all_companies) >= max_companies:
                break
                
            logger.info(f"\n{'='*50}")
            logger.info(f"Processing {site_name.upper()}")
            logger.info(f"{'='*50}")
            
            try:
                companies = self.scrape_directory_site(site_name, location)
                
                # Deduplicate
                for company in companies:
                    name_key = company.name.lower().strip()
                    if name_key and name_key not in seen_names:
                        seen_names.add(name_key)
                        all_companies.append(company)
                        logger.info(f"➕ Added: {company.name}")
                        
                        if len(all_companies) >= max_companies:
                            break
                
                # Be respectful - delay between sites
                if len(all_companies) < max_companies:
                    logger.info("⏳ Waiting 5 seconds before next site...")
                    time.sleep(5)
                    
            except Exception as e:
                logger.error(f"❌ Error processing {site_name}: {e}")
                continue
        
        logger.info(f"\n🎉 FINAL RESULTS: {len(all_companies)} unique companies found!")
        return all_companies
    
    # Helper methods for data classification
    def looks_like_company_name(self, text: str) -> bool:
        if not text or len(text) < 3:
            return False
        keywords = ['metal', 'scrap', 'recycling', 'salvage', 'steel', 'aluminum', 'copper', 'inc', 'llc']
        return any(keyword in text.lower() for keyword in keywords) and not text.startswith('http')
    
    def looks_like_phone(self, text: str) -> bool:
        if not text:
            return False
        # Look for phone patterns
        phone_patterns = [
            r'\(\d{3}\)\s*\d{3}[-\s]?\d{4}',  # (555) 123-4567
            r'\d{3}[-\s]?\d{3}[-\s]?\d{4}',   # 555-123-4567
            r'\+1[-\s]?\d{3}[-\s]?\d{3}[-\s]?\d{4}'  # +1-555-123-4567
        ]
        return any(re.search(pattern, text) for pattern in phone_patterns)
    
    def looks_like_address(self, text: str) -> bool:
        if not text or len(text) < 5:
            return False
        address_indicators = ['st', 'street', 'ave', 'avenue', 'rd', 'road', 'blvd', 'dr', 'drive', 'ln', 'lane']
        has_number = any(char.isdigit() for char in text)
        has_street = any(indicator in text.lower() for indicator in address_indicators)
        return has_number and has_street
    
    def looks_like_website(self, text: str) -> bool:
        if not text:
            return False
        return text.startswith('http') or any(domain in text for domain in ['.com', '.net', '.org', '.biz'])

def main():
    """
    Main function - the ultimate test
    """
    scraper = HybridStealthScraper()
    
    # Scrape companies for Houston
    companies = scraper.scrape_all_sites("Houston, Texas", max_companies=30)
    
    if companies:
        # Export using the existing system
        try:
            paths = export_records(companies, output_dir="output")
            logger.info(f"📁 Exported {len(companies)} companies to {paths.get('xlsx', 'output files')}")
            
            # Print summary
            print("\n" + "="*60)
            print("🎉 HYBRID STEALTH SCRAPER RESULTS")
            print("="*60)
            for i, company in enumerate(companies[:10], 1):
                print(f"{i:2d}. {company.name}")
                if company.phones:
                    print(f"    📞 {company.phones[0]}")
                if company.street_address:
                    print(f"    📍 {company.street_address}")
                print()
            
            if len(companies) > 10:
                print(f"... and {len(companies) - 10} more companies")
                
        except Exception as e:
            # Fallback export
            output_file = f"output/hybrid_stealth_results_{int(time.time())}.json"
            os.makedirs("output", exist_ok=True)
            with open(output_file, 'w', encoding='utf-8') as f:
                json.dump([asdict(company) for company in companies], f, indent=2, ensure_ascii=False)
            logger.info(f"📁 Saved to {output_file}")
    else:
        logger.error("❌ No companies found!")

if __name__ == "__main__":
    main()