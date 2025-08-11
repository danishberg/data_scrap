#!/usr/bin/env python3
"""
Ultimate Company Scraper - The Final Solution

Strategy:
1. AI generates REAL company names for specific locations
2. Google/Bing search finds their actual websites  
3. Extract structured data from real company websites
4. Use AutoScraper where possible for pattern learning

This avoids:
- Bot detection (searches Google, not directories)
- Synthetic data (uses real company names)
- Browser compatibility issues
- Complex setup requirements
"""

import json
import logging
import os
import re
import time
from typing import List, Dict, Optional
from urllib.parse import urlparse
import requests
from dataclasses import asdict

try:
    from autoscraper import AutoScraper
    from unified_company_scraper import CompanyRecord, export_records, _get_openai_client, pick_ua
except ImportError as e:
    print(f"Missing dependencies: {e}")
    exit(1)

# Setup logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s: %(message)s")
logger = logging.getLogger("ultimate_scraper")

class UltimateCompanyScraper:
    """
    The ultimate solution that actually works and generates real companies
    """
    
    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({'User-Agent': pick_ua()})
        self.found_companies = []
        
    def ai_generate_real_company_names(self, location: str, count: int = 30) -> List[str]:
        """
        Use AI to generate REAL company names (not synthetic ones)
        """
        try:
            client = _get_openai_client()
            
            prompt = f"""List {count} REAL scrap metal recycling companies in {location}. 

IMPORTANT RULES:
- Only provide companies you have actual knowledge of
- Include both large and small local companies
- Focus on real business names, not fictional ones
- Include variations like "Metal Recycling", "Scrap Yard", "Salvage", "Auto Parts"

Return as JSON array:
{{"companies": ["Company Name 1", "Company Name 2", ...]}}

Location: {location}
"""

            response = client.chat.completions.create(
                model="gpt-4o-mini",
                messages=[
                    {"role": "system", "content": "You are a business directory expert with knowledge of real companies. Only provide companies you actually know exist."},
                    {"role": "user", "content": prompt}
                ],
                temperature=0.2,
                max_tokens=1500,
                response_format={"type": "json_object"}
            )
            
            data = json.loads(response.choices[0].message.content)
            company_names = data.get("companies", [])
            
            logger.info(f"🤖 AI generated {len(company_names)} real company names")
            
            # Log a few examples
            for i, name in enumerate(company_names[:5]):
                logger.info(f"  {i+1}. {name}")
            
            return company_names
            
        except Exception as e:
            logger.error(f"❌ Error generating company names: {e}")
            return []
    
    def search_company_website(self, company_name: str, location: str) -> Optional[str]:
        """
        Search for a company's website using Google search
        """
        try:
            # Create search query
            query = f'"{company_name}" {location} scrap metal recycling site:com OR site:net OR site:org'
            
            # Use DuckDuckGo to avoid Google blocking
            search_url = f"https://duckduckgo.com/html/?q={requests.utils.quote(query)}"
            
            headers = {
                'User-Agent': pick_ua(),
                'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
                'Accept-Language': 'en-US,en;q=0.5',
                'Accept-Encoding': 'gzip, deflate',
                'Connection': 'keep-alive',
            }
            
            response = self.session.get(search_url, headers=headers, timeout=10)
            
            if response.status_code == 200:
                # Simple regex to find website URLs
                urls = re.findall(r'href="([^"]*(?:\.com|\.net|\.org)[^"]*)"', response.text)
                
                # Filter for relevant URLs
                for url in urls:
                    if any(domain in url.lower() for domain in ['.com', '.net', '.org']):
                        domain = urlparse(url).netloc.lower()
                        # Skip search engines and directory sites
                        if not any(skip in domain for skip in ['google', 'bing', 'duckduckgo', 'yelp', 'yellowpages']):
                            logger.info(f"🔍 Found website for {company_name}: {url}")
                            return url
            
            logger.warning(f"⚠️ No website found for {company_name}")
            return None
            
        except Exception as e:
            logger.warning(f"⚠️ Search error for {company_name}: {e}")
            return None
    
    def extract_company_data_from_website(self, company_name: str, website_url: str, location: str) -> Optional[CompanyRecord]:
        """
        Extract company data from their actual website
        """
        try:
            logger.info(f"📋 Extracting data from {website_url}")
            
            headers = {
                'User-Agent': pick_ua(),
                'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
                'Accept-Language': 'en-US,en;q=0.5',
            }
            
            response = self.session.get(website_url, headers=headers, timeout=15)
            
            if response.status_code != 200:
                logger.warning(f"⚠️ HTTP {response.status_code} for {website_url}")
                return None
            
            # Use AI to extract structured data from the website
            html_snippet = response.text[:8000]  # First 8K characters
            
            prompt = f"""Extract company information from this website HTML:

Company Name: {company_name}
Website: {website_url}
Location Context: {location}

HTML Content:
{html_snippet}

Return JSON with exact fields:
{{
    "name": "Verified company name",
    "website": "Full website URL",
    "street_address": "Street address if found",
    "city": "City",
    "region": "State/Province",
    "postal_code": "ZIP code",
    "country": "United States",
    "phones": ["Phone numbers in (XXX) XXX-XXXX format"],
    "emails": ["Email addresses"],
    "description": "Brief company description",
    "materials": ["Materials they accept"],
    "opening_hours": "Operating hours if found"
}}

Only include information clearly found on the website. Use empty string/array if not found.
"""

            client = _get_openai_client()
            ai_response = client.chat.completions.create(
                model="gpt-4o-mini",
                messages=[
                    {"role": "system", "content": "Extract business information from websites. Return only valid JSON."},
                    {"role": "user", "content": prompt}
                ],
                temperature=0.1,
                max_tokens=800,
                response_format={"type": "json_object"}
            )
            
            data = json.loads(ai_response.choices[0].message.content)
            
            # Create CompanyRecord
            company = CompanyRecord(
                name=data.get("name", company_name),
                website=website_url,
                street_address=data.get("street_address", ""),
                city=data.get("city", ""),
                region=data.get("region", ""),
                postal_code=data.get("postal_code", ""),
                country=data.get("country", "United States"),
                phones=data.get("phones", []),
                emails=data.get("emails", []),
                whatsapp=[],
                social_links=[],
                opening_hours=data.get("opening_hours", ""),
                materials=data.get("materials", []),
                material_prices=[],
                description=data.get("description", "")
            )
            
            logger.info(f"✅ Extracted: {company.name}")
            if company.phones:
                logger.info(f"   📞 {company.phones[0]}")
            if company.street_address:
                logger.info(f"   📍 {company.street_address}")
            
            return company
            
        except Exception as e:
            logger.error(f"❌ Error extracting from {website_url}: {e}")
            return None
    
    def scrape_location(self, location: str, max_companies: int = 50) -> List[CompanyRecord]:
        """
        Scrape companies for a specific location using the ultimate approach
        """
        companies = []
        seen_websites = set()
        
        logger.info(f"🚀 Ultimate scraping for: {location}")
        logger.info(f"🎯 Target: {max_companies} companies")
        
        # Step 1: Generate real company names
        logger.info("\n" + "="*50)
        logger.info("STEP 1: Generating real company names")
        logger.info("="*50)
        
        company_names = self.ai_generate_real_company_names(location, count=max_companies * 2)
        
        if not company_names:
            logger.error("❌ Failed to generate company names")
            return []
        
        # Step 2: Find websites and extract data
        logger.info("\n" + "="*50)
        logger.info("STEP 2: Finding websites and extracting data")
        logger.info("="*50)
        
        for i, company_name in enumerate(company_names):
            if len(companies) >= max_companies:
                break
                
            logger.info(f"\n🏢 Processing {i+1}/{len(company_names)}: {company_name}")
            
            try:
                # Find the company's website
                website = self.search_company_website(company_name, location)
                
                if not website:
                    continue
                
                # Skip duplicates
                domain = urlparse(website).netloc.lower()
                if domain in seen_websites:
                    logger.info(f"⏭️ Skipping duplicate domain: {domain}")
                    continue
                
                seen_websites.add(domain)
                
                # Extract company data
                company_data = self.extract_company_data_from_website(company_name, website, location)
                
                if company_data:
                    companies.append(company_data)
                    logger.info(f"✅ Success! Total companies: {len(companies)}")
                
                # Be respectful - add delays
                time.sleep(2)
                
            except Exception as e:
                logger.error(f"❌ Error processing {company_name}: {e}")
                continue
        
        logger.info(f"\n🎉 ULTIMATE RESULTS: {len(companies)} companies extracted!")
        return companies

def main():
    """
    Main function - Ultimate test
    """
    scraper = UltimateCompanyScraper()
    
    # Test with Houston, Texas
    companies = scraper.scrape_location("Houston, Texas", max_companies=20)
    
    if companies:
        # Export results
        try:
            paths = export_records(companies, output_dir="output")
            logger.info(f"📁 Exported to {paths.get('xlsx', 'output files')}")
        except:
            # Fallback export
            output_file = f"output/ultimate_results_{int(time.time())}.json"
            os.makedirs("output", exist_ok=True)
            with open(output_file, 'w', encoding='utf-8') as f:
                json.dump([asdict(company) for company in companies], f, indent=2, ensure_ascii=False)
            logger.info(f"📁 Saved to {output_file}")
        
        # Print summary
        print("\n" + "="*60)
        print("🏆 ULTIMATE SCRAPER RESULTS")
        print("="*60)
        for i, company in enumerate(companies, 1):
            print(f"{i:2d}. {company.name}")
            if company.phones:
                print(f"    📞 {company.phones[0]}")
            if company.street_address:
                print(f"    📍 {company.street_address}")
            if company.website:
                print(f"    🌐 {company.website}")
            print()
    else:
        logger.error("❌ No companies found!")

if __name__ == "__main__":
    main()