import json
import time
import re
from urllib.parse import quote_plus, urljoin
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException
from .base_scraper import BaseScraper

class YellowPagesScraper(BaseScraper):
    def __init__(self):
        super().__init__()
        self.base_url = "https://www.yellowpages.com/search"
        
    def scrape(self, search_term, location="", limit=100):
        """Scrape Yellow Pages for scrap metal centers"""
        results = []
        
        # Clean search terms for Yellow Pages
        clean_search = search_term.replace(" centers", "").replace(" facilities", "")
        
        page = 1
        while len(results) < limit and page <= 10:  # Max 10 pages
            
            # Construct URL for Yellow Pages search
            params = f"?search_terms={quote_plus(clean_search)}"
            if location:
                params += f"&geo_location_terms={quote_plus(location)}"
            params += f"&page={page}"
            
            search_url = self.base_url + params
            
            self.logger.info(f"Scraping Yellow Pages page {page} for: {clean_search} in {location}")
            
            html_content = self.get_page_content(search_url)
            if not html_content:
                break
                
            soup = self.parse_html(html_content)
            if not soup:
                break
            
            # Extract business listings
            business_listings = soup.select('.result, .organic')
            
            if not business_listings:
                self.logger.info("No more listings found")
                break
            
            for listing in business_listings:
                if len(results) >= limit:
                    break
                    
                try:
                    business_data = self._extract_business_from_listing(listing, search_url)
                    if business_data and business_data.get('name'):
                        business_data['source_url'] = search_url
                        results.append(business_data)
                        self.logger.info(f"Extracted: {business_data.get('name', 'Unknown')}")
                except Exception as e:
                    self.logger.error(f"Error extracting business: {e}")
                    continue
                
                self.add_request_delay()
            
            page += 1
            self.add_request_delay()
        
        self.logger.info(f"Yellow Pages scraping completed. Found {len(results)} businesses.")
        return results
    
    def _extract_business_from_listing(self, listing, base_url):
        """Extract business data from Yellow Pages listing"""
        data = {}
        
        try:
            # Extract business name
            name_selectors = [
                '.business-name a',
                '.listing-title a', 
                'h3 a',
                '.n a'
            ]
            
            for selector in name_selectors:
                name_element = listing.select_one(selector)
                if name_element:
                    data['name'] = self.data_processor.clean_text(name_element.get_text())
                    
                    # Get business detail URL
                    detail_url = name_element.get('href', '')
                    if detail_url and not detail_url.startswith('http'):
                        detail_url = urljoin('https://www.yellowpages.com', detail_url)
                    break
            
            if not data.get('name'):
                return None
            
            # Extract address
            address_selectors = [
                '.adr',
                '.address',
                '.street-address',
                '.locality'
            ]
            
            for selector in address_selectors:
                address_element = listing.select_one(selector)
                if address_element:
                    address_text = address_element.get_text(strip=True)
                    if address_text:
                        data['full_address'] = self.data_processor.clean_text(address_text)
                        
                        # Parse address components
                        address_parts = self.data_processor.parse_address(address_text)
                        data.update(address_parts)
                        break
            
            # Extract phone number
            phone_selectors = [
                '.phone',
                '.phones .phone',
                '[class*="phone"]'
            ]
            
            for selector in phone_selectors:
                phone_element = listing.select_one(selector)
                if phone_element:
                    phone_text = phone_element.get_text(strip=True)
                    phones = self.data_processor.extract_phone_numbers(phone_text)
                    if phones:
                        data['phone_primary'] = phones[0]
                        break
            
            # Extract categories/business type
            category_selectors = [
                '.categories a',
                '.business-categories',
                '.category'
            ]
            
            categories = []
            for selector in category_selectors:
                category_elements = listing.select(selector)
                for cat_element in category_elements:
                    cat_text = cat_element.get_text(strip=True)
                    if cat_text:
                        categories.append(cat_text)
            
            if categories:
                data['business_categories'] = categories
            
            # Extract website if available
            website_selectors = [
                '.website a',
                '.links a[href*="http"]',
                'a[title*="website"]'
            ]
            
            for selector in website_selectors:
                website_element = listing.select_one(selector)
                if website_element:
                    website_url = website_element.get('href', '')
                    if website_url and 'http' in website_url:
                        data['website'] = website_url
                        break
            
            # If we have a detail URL, scrape additional information
            if 'detail_url' in locals() and detail_url:
                additional_data = self._scrape_business_detail_page(detail_url)
                if additional_data:
                    data.update(additional_data)
            
            # Geocode address for coordinates
            if data.get('full_address'):
                lat, lng = self.data_processor.geocode_address(data['full_address'])
                if lat and lng:
                    data['latitude'] = lat
                    data['longitude'] = lng
            
            return data
            
        except Exception as e:
            self.logger.error(f"Error extracting business data from listing: {e}")
            return None
    
    def _scrape_business_detail_page(self, detail_url):
        """Scrape additional details from business detail page"""
        try:
            html_content = self.get_page_content(detail_url)
            if not html_content:
                return {}
            
            soup = self.parse_html(html_content)
            if not soup:
                return {}
            
            data = {}
            
            # Extract detailed business information
            additional_data = self.extract_business_data(soup, detail_url)
            data.update(additional_data)
            
            # Extract business hours
            hours_selectors = [
                '.hours-table tr',
                '.business-hours tr',
                '.hours tr'
            ]
            
            for selector in hours_selectors:
                hour_rows = soup.select(selector)
                if hour_rows:
                    hours_data = {}
                    for row in hour_rows:
                        cells = row.select('td')
                        if len(cells) >= 2:
                            day = cells[0].get_text(strip=True).lower()
                            time_text = cells[1].get_text(strip=True)
                            if day and time_text:
                                hours_data[day] = time_text
                    
                    if hours_data:
                        data['working_hours'] = hours_data
                        break
            
            # Extract additional contact information
            contact_section = soup.select_one('.contact-info, .business-card')
            if contact_section:
                contact_text = contact_section.get_text()
                
                # Extract emails
                emails = self.data_processor.extract_emails(contact_text)
                if emails:
                    data['email_primary'] = emails[0]
                    if len(emails) > 1:
                        data['email_secondary'] = emails[1]
                
                # Extract additional phone numbers
                phones = self.data_processor.extract_phone_numbers(contact_text)
                if phones and not data.get('phone_primary'):
                    data['phone_primary'] = phones[0]
                elif phones and len(phones) > 1 and not data.get('phone_secondary'):
                    data['phone_secondary'] = phones[1]
            
            # Extract business description
            description_selectors = [
                '.business-description',
                '.about-section',
                '.business-summary'
            ]
            
            for selector in description_selectors:
                desc_element = soup.select_one(selector)
                if desc_element:
                    desc_text = desc_element.get_text(strip=True)
                    if desc_text:
                        data['description'] = self.data_processor.clean_text(desc_text)
                        break
            
            return data
            
        except Exception as e:
            self.logger.error(f"Error scraping business detail page {detail_url}: {e}")
            return {}

class YellowPagesCanadaScraper(BaseScraper):
    """Scraper for Yellow Pages Canada"""
    
    def __init__(self):
        super().__init__()
        self.base_url = "https://www.yellowpages.ca/search/si"
        
    def scrape(self, search_term, location="", limit=100):
        """Scrape Yellow Pages Canada for scrap metal centers"""
        results = []
        
        page = 1
        while len(results) < limit and page <= 10:
            
            # Construct URL for Yellow Pages Canada search
            params = f"/{page}/{quote_plus(search_term)}"
            if location:
                params += f"/{quote_plus(location)}"
            
            search_url = self.base_url + params
            
            self.logger.info(f"Scraping Yellow Pages Canada page {page} for: {search_term} in {location}")
            
            html_content = self.get_page_content(search_url)
            if not html_content:
                break
                
            soup = self.parse_html(html_content)
            if not soup:
                break
            
            # Extract business listings
            business_listings = soup.select('.listing, .organic')
            
            if not business_listings:
                break
            
            for listing in business_listings:
                if len(results) >= limit:
                    break
                    
                try:
                    business_data = self._extract_canadian_business(listing, search_url)
                    if business_data and business_data.get('name'):
                        business_data['source_url'] = search_url
                        business_data['country'] = 'CA'
                        results.append(business_data)
                        self.logger.info(f"Extracted: {business_data.get('name', 'Unknown')}")
                except Exception as e:
                    self.logger.error(f"Error extracting Canadian business: {e}")
                    continue
                
                self.add_request_delay()
            
            page += 1
            self.add_request_delay()
        
        self.logger.info(f"Yellow Pages Canada scraping completed. Found {len(results)} businesses.")
        return results
    
    def _extract_canadian_business(self, listing, base_url):
        """Extract business data from Canadian Yellow Pages listing"""
        # Similar to US Yellow Pages but with Canadian-specific selectors
        return self._extract_business_from_listing(listing, base_url) 