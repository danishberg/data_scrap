#!/usr/bin/env python3
"""
Stealth Company Scraper using Scrapling + AutoScraper
Combines bot-detection bypass with automatic pattern learning
"""

import json
import logging
import time
from typing import List, Dict, Optional
from urllib.parse import urlparse
import sys
import os

try:
    from scrapling import StealthyFetcher
    from autoscraper import AutoScraper
except ImportError as e:
    print(f"Error importing required libraries: {e}")
    print("Please install with: pip install scrapling autoscraper")
    sys.exit(1)

# Import from the existing script
try:
    from unified_company_scraper import CompanyRecord, export_records
except ImportError:
    # Define minimal CompanyRecord if import fails
    from dataclasses import dataclass
    
    @dataclass
    class CompanyRecord:
        name: str = ""
        website: str = ""
        street_address: str = ""
        city: str = ""
        region: str = ""
        postal_code: str = ""
        country: str = "United States"
        phones: List[str] = None
        emails: List[str] = None
        whatsapp: List[str] = None
        social_links: List[str] = None
        opening_hours: str = ""
        materials: List[str] = None
        material_prices: List[str] = None
        description: str = ""
        
        def __post_init__(self):
            if self.phones is None:
                self.phones = []
            if self.emails is None:
                self.emails = []
            if self.whatsapp is None:
                self.whatsapp = []
            if self.social_links is None:
                self.social_links = []
            if self.materials is None:
                self.materials = []
            if self.material_prices is None:
                self.material_prices = []

# Setup logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s: %(message)s")
logger = logging.getLogger("stealth_scraper")

class StealthCompanyScraper:
    """
    Stealth scraper that bypasses bot detection and automatically learns extraction patterns
    """
    
    def __init__(self):
        self.directory_urls = {
            "yelp": "https://www.yelp.com/search?find_desc=scrap+metal+recycling&find_loc={location}",
            "yellowpages": "https://www.yellowpages.com/search?search_terms=scrap+metal+recycling&geo_location_terms={location}",
            "superpages": "https://www.superpages.com/search?C=scrap+metal+recycling&T={location}",
            "manta": "https://www.manta.com/search?search={location}+scrap+metal+recycling",
        }
        self.learned_patterns = {}
        
    def get_stealth_html(self, url: str, max_retries: int = 3) -> Optional[str]:
        """
        Fetch HTML using stealth browser to bypass bot detection
        """
        for attempt in range(max_retries):
            try:
                logger.info(f"Fetching {url} with stealth browser (attempt {attempt + 1})")
                
                # Use StealthyFetcher with optimized settings
                response = StealthyFetcher.fetch(
                    url=url,
                    headless=True,
                    block_images=True,  # Save bandwidth
                    disable_resources=True,  # Speed boost
                    wait=2000,  # Wait 2 seconds for content to load
                    timeout=30000,  # 30 second timeout
                    humanize=True,  # Human-like behavior
                    google_search=True,  # Set referer as from Google search
                )
                
                if response.status == 200:
                    logger.info(f"Successfully fetched {url} - Status: {response.status}")
                    return response.text
                else:
                    logger.warning(f"HTTP {response.status} for {url}")
                    
            except Exception as e:
                logger.error(f"Error fetching {url} (attempt {attempt + 1}): {e}")
                if attempt < max_retries - 1:
                    time.sleep(5 * (attempt + 1))  # Exponential backoff
                    
        return None
    
    def learn_extraction_patterns(self, sample_url: str, wanted_data: List[str]) -> bool:
        """
        Learn extraction patterns using AutoScraper from a sample page
        """
        try:
            logger.info(f"Learning extraction patterns from {sample_url}")
            
            # Fetch the sample page with stealth
            html = self.get_stealth_html(sample_url)
            if not html:
                logger.error(f"Could not fetch sample page: {sample_url}")
                return False
            
            # Create AutoScraper instance
            scraper = AutoScraper()
            
            # Build scraping model from sample data
            result = scraper.build(html=html, wanted_list=wanted_data)
            logger.info(f"AutoScraper learned patterns, found {len(result)} similar items")
            
            # Store the learned model
            domain = urlparse(sample_url).netloc
            self.learned_patterns[domain] = scraper
            
            # Save the model for reuse
            model_file = f"models/autoscraper_{domain.replace('.', '_')}.json"
            os.makedirs("models", exist_ok=True)
            scraper.save(model_file)
            logger.info(f"Saved learned patterns to {model_file}")
            
            return True
            
        except Exception as e:
            logger.error(f"Error learning patterns from {sample_url}: {e}")
            return False
    
    def extract_companies_from_page(self, url: str) -> List[CompanyRecord]:
        """
        Extract companies from a page using learned patterns
        """
        try:
            domain = urlparse(url).netloc
            
            # Check if we have learned patterns for this domain
            if domain not in self.learned_patterns:
                logger.warning(f"No learned patterns for domain {domain}")
                return []
            
            # Fetch the page
            html = self.get_stealth_html(url)
            if not html:
                return []
            
            # Use AutoScraper to extract data
            scraper = self.learned_patterns[domain]
            extracted_data = scraper.get_result_similar(html=html, grouped=True)
            
            logger.info(f"Extracted data groups: {list(extracted_data.keys())}")
            
            # Convert extracted data to CompanyRecord objects
            companies = self.convert_to_company_records(extracted_data)
            logger.info(f"Converted to {len(companies)} company records")
            
            return companies
            
        except Exception as e:
            logger.error(f"Error extracting from {url}: {e}")
            return []
    
    def convert_to_company_records(self, extracted_data: Dict) -> List[CompanyRecord]:
        """
        Convert AutoScraper extracted data to CompanyRecord objects
        """
        companies = []
        
        try:
            # AutoScraper returns grouped data - we need to combine related fields
            # This is a basic implementation - might need adjustment based on actual data structure
            
            # Get the longest list to determine number of companies
            max_length = max(len(v) if isinstance(v, list) else 1 for v in extracted_data.values())
            
            for i in range(max_length):
                company = CompanyRecord()
                
                # Map extracted data to company fields
                for key, values in extracted_data.items():
                    if not isinstance(values, list):
                        values = [values]
                    
                    if i < len(values):
                        value = values[i].strip() if isinstance(values[i], str) else str(values[i])
                        
                        # Map based on content patterns
                        if self.looks_like_company_name(value):
                            company.name = value
                        elif self.looks_like_phone(value):
                            company.phones.append(value)
                        elif self.looks_like_email(value):
                            company.emails.append(value)
                        elif self.looks_like_address(value):
                            company.street_address = value
                        elif self.looks_like_website(value):
                            company.website = value
                
                # Only add if we have at least a name
                if company.name:
                    companies.append(company)
        
        except Exception as e:
            logger.error(f"Error converting extracted data to records: {e}")
        
        return companies
    
    def looks_like_company_name(self, text: str) -> bool:
        """Simple heuristic to identify company names"""
        if not text or len(text) < 3:
            return False
        # Company names often contain these keywords
        company_keywords = ['metal', 'scrap', 'recycling', 'salvage', 'inc', 'llc', 'corp', 'company', 'co.']
        text_lower = text.lower()
        return any(keyword in text_lower for keyword in company_keywords) and not text.startswith('http')
    
    def looks_like_phone(self, text: str) -> bool:
        """Simple heuristic to identify phone numbers"""
        import re
        if not text:
            return False
        # Look for phone number patterns
        phone_pattern = r'[\(\d][\d\s\-\(\)\.]{8,}'
        return bool(re.search(phone_pattern, text))
    
    def looks_like_email(self, text: str) -> bool:
        """Simple heuristic to identify emails"""
        import re
        if not text:
            return False
        email_pattern = r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b'
        return bool(re.search(email_pattern, text))
    
    def looks_like_address(self, text: str) -> bool:
        """Simple heuristic to identify addresses"""
        if not text or len(text) < 5:
            return False
        # Addresses often contain numbers and street keywords
        street_keywords = ['st', 'street', 'ave', 'avenue', 'rd', 'road', 'blvd', 'boulevard', 'dr', 'drive']
        text_lower = text.lower()
        has_number = any(char.isdigit() for char in text)
        has_street_keyword = any(keyword in text_lower for keyword in street_keywords)
        return has_number and has_street_keyword
    
    def looks_like_website(self, text: str) -> bool:
        """Simple heuristic to identify websites"""
        if not text:
            return False
        return text.startswith('http') or '.com' in text or '.net' in text or '.org' in text
    
    def scrape_location(self, location: str, max_companies: int = 100) -> List[CompanyRecord]:
        """
        Scrape companies for a specific location using stealth + AutoScraper
        """
        all_companies = []
        seen_names = set()
        
        # Format location for URLs
        location_formatted = location.replace(' ', '+').replace(',', '%2C')
        
        for directory_name, url_template in self.directory_urls.items():
            if len(all_companies) >= max_companies:
                break
                
            try:
                # Create the search URL
                search_url = url_template.format(location=location_formatted)
                logger.info(f"Processing {directory_name}: {search_url}")
                
                # For the first directory, we need to learn patterns manually
                # In a real implementation, you'd provide sample data or use AI to identify patterns
                if directory_name not in [d for d in self.learned_patterns.keys() if directory_name in d]:
                    logger.info(f"Need to learn patterns for {directory_name} - skipping for now")
                    # This is where you'd implement pattern learning
                    # For now, let's use a basic approach
                    continue
                
                # Extract companies using learned patterns
                companies = self.extract_companies_from_page(search_url)
                
                # Deduplicate
                for company in companies:
                    if company.name and company.name.lower() not in seen_names:
                        seen_names.add(company.name.lower())
                        all_companies.append(company)
                        
                        if len(all_companies) >= max_companies:
                            break
                
                logger.info(f"Found {len(companies)} companies from {directory_name}")
                
                # Be respectful - add delay between requests
                time.sleep(3)
                
            except Exception as e:
                logger.error(f"Error processing {directory_name}: {e}")
                continue
        
        logger.info(f"Total companies found: {len(all_companies)}")
        return all_companies

def main():
    """
    Main function to demonstrate the stealth scraper
    """
    scraper = StealthCompanyScraper()
    
    # Example: Learn patterns from a sample Yelp page
    # In practice, you'd either:
    # 1. Manually provide sample data to train AutoScraper
    # 2. Use the training mode to teach it what to extract
    # 3. Pre-train it with known good examples
    
    sample_data = [
        "ABC Metal Recycling",  # Company name examples
        "XYZ Scrap Yard",
        "(555) 123-4567",      # Phone examples
        "(555) 987-6543"
    ]
    
    # Try to learn from a sample page
    sample_url = "https://www.yelp.com/search?find_desc=scrap+metal+recycling&find_loc=Houston%2C+TX"
    success = scraper.learn_extraction_patterns(sample_url, sample_data)
    
    if success:
        # Now scrape for real
        companies = scraper.scrape_location("Houston, Texas", max_companies=20)
        
        if companies:
            # Export results
            try:
                from unified_company_scraper import export_records
                paths = export_records(companies, output_dir="output")
                logger.info(f"Exported {len(companies)} companies to {paths.get('xlsx', 'output files')}")
            except:
                # Fallback: save as JSON
                output_file = f"output/stealth_scrape_{int(time.time())}.json"
                os.makedirs("output", exist_ok=True)
                with open(output_file, 'w') as f:
                    json.dump([company.__dict__ for company in companies], f, indent=2)
                logger.info(f"Saved to {output_file}")
        else:
            logger.warning("No companies found")
    else:
        logger.error("Failed to learn extraction patterns")

if __name__ == "__main__":
    main()