import json
import time
import re
from urllib.parse import quote_plus, urljoin
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException
from .base_scraper import BaseScraper

class YelpScraper(BaseScraper):
    def __init__(self):
        super().__init__()
        self.base_url = "https://www.yelp.com/search"
        
    def scrape(self, search_term, location="", limit=100):
        """Scrape Yelp for scrap metal centers"""
        results = []
        
        page_offset = 0
        while len(results) < limit and page_offset < 1000:  # Yelp typically shows up to 1000 results
            
            # Construct Yelp search URL
            params = f"?find_desc={quote_plus(search_term)}"
            if location:
                params += f"&find_loc={quote_plus(location)}"
            if page_offset > 0:
                params += f"&start={page_offset}"
            
            search_url = self.base_url + params
            
            self.logger.info(f"Scraping Yelp page {page_offset//10 + 1} for: {search_term} in {location}")
            
            html_content = self.get_page_content(search_url, use_selenium=True)
            if not html_content:
                break
                
            soup = self.parse_html(html_content)
            if not soup:
                break
            
            # Extract business listings
            business_listings = soup.select('[data-testid="serp-ia-card"], .searchResult')
            
            if not business_listings:
                self.logger.info("No more listings found")
                break
            
            page_results = 0
            for listing in business_listings:
                if len(results) >= limit:
                    break
                    
                try:
                    business_data = self._extract_business_from_listing(listing, search_url)
                    if business_data and business_data.get('name'):
                        business_data['source_url'] = search_url
                        results.append(business_data)
                        page_results += 1
                        self.logger.info(f"Extracted: {business_data.get('name', 'Unknown')}")
                except Exception as e:
                    self.logger.error(f"Error extracting Yelp business: {e}")
                    continue
                
                self.add_request_delay()
            
            if page_results == 0:
                break
                
            page_offset += 10
            self.add_request_delay()
        
        self.logger.info(f"Yelp scraping completed. Found {len(results)} businesses.")
        return results
    
    def _extract_business_from_listing(self, listing, base_url):
        """Extract business data from Yelp listing"""
        data = {}
        
        try:
            # Extract business name
            name_selectors = [
                'h3 a span',
                '.businessName span',
                '[data-testid="serp-ia-card"] h3 a',
                'h4 a span'
            ]
            
            for selector in name_selectors:
                name_element = listing.select_one(selector)
                if name_element:
                    name_text = name_element.get_text(strip=True)
                    if name_text:
                        data['name'] = self.data_processor.clean_text(name_text)
                        
                        # Get business detail URL
                        link_element = name_element.find_parent('a')
                        if link_element:
                            detail_url = link_element.get('href', '')
                            if detail_url and not detail_url.startswith('http'):
                                detail_url = urljoin('https://www.yelp.com', detail_url)
                        break
            
            if not data.get('name'):
                return None
            
            # Extract address
            address_selectors = [
                '[data-testid="address"]',
                '.address',
                '.secondaryAttributes .address'
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
                '[data-testid="phone"]',
                '.phone',
                '.biz-phone'
            ]
            
            for selector in phone_selectors:
                phone_element = listing.select_one(selector)
                if phone_element:
                    phone_text = phone_element.get_text(strip=True)
                    phones = self.data_processor.extract_phone_numbers(phone_text)
                    if phones:
                        data['phone_primary'] = phones[0]
                        break
            
            # Extract rating and review count
            rating_selectors = [
                '[data-testid="rating"]',
                '.rating',
                '.i-stars'
            ]
            
            for selector in rating_selectors:
                rating_element = listing.select_one(selector)
                if rating_element:
                    rating_text = rating_element.get('aria-label', '') or rating_element.get_text()
                    rating_match = re.search(r'(\d+\.?\d*)\s*star', rating_text, re.IGNORECASE)
                    if rating_match:
                        data['rating'] = float(rating_match.group(1))
                        break
            
            # Extract business categories
            category_selectors = [
                '.priceCategory',
                '.category-str-list a',
                '.businessCategories'
            ]
            
            categories = []
            for selector in category_selectors:
                category_elements = listing.select(selector)
                for cat_element in category_elements:
                    cat_text = cat_element.get_text(strip=True)
                    if cat_text and cat_text not in categories:
                        categories.append(cat_text)
            
            if categories:
                data['business_categories'] = categories
            
            # Extract price range
            price_selectors = [
                '.priceRange',
                '.price-range'
            ]
            
            for selector in price_selectors:
                price_element = listing.select_one(selector)
                if price_element:
                    price_text = price_element.get_text(strip=True)
                    if price_text:
                        data['price_range'] = price_text
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
            self.logger.error(f"Error extracting Yelp business data: {e}")
            return None
    
    def _scrape_business_detail_page(self, detail_url):
        """Scrape additional details from Yelp business page"""
        try:
            html_content = self.get_page_content(detail_url, use_selenium=True)
            if not html_content:
                return {}
            
            soup = self.parse_html(html_content)
            if not soup:
                return {}
            
            data = {}
            
            # Extract business hours
            hours_selectors = [
                '.hours-table tr',
                '.business-hours tr',
                '.hours-details tr'
            ]
            
            for selector in hours_selectors:
                hour_rows = soup.select(selector)
                if hour_rows:
                    hours_data = {}
                    for row in hour_rows:
                        day_element = row.select_one('th, td:first-child')
                        time_element = row.select_one('td:last-child')
                        
                        if day_element and time_element:
                            day = day_element.get_text(strip=True).lower()
                            time_text = time_element.get_text(strip=True)
                            if day and time_text:
                                hours_data[day] = time_text
                    
                    if hours_data:
                        data['working_hours'] = hours_data
                        break
            
            # Extract website
            website_selectors = [
                '.biz-website a',
                '[data-testid="website-link"]',
                '.website a'
            ]
            
            for selector in website_selectors:
                website_element = soup.select_one(selector)
                if website_element:
                    website_url = website_element.get('href', '')
                    if website_url and 'http' in website_url:
                        data['website'] = website_url
                        break
            
            # Extract business description from about section or reviews
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
            
            # Extract additional contact information and social links
            page_text = soup.get_text()
            
            # Extract emails
            emails = self.data_processor.extract_emails(page_text)
            if emails:
                data['email_primary'] = emails[0]
                if len(emails) > 1:
                    data['email_secondary'] = emails[1]
            
            # Extract social media links
            social_links = self.data_processor.extract_social_media_links(page_text, detail_url)
            for platform, url in social_links.items():
                if platform == 'facebook':
                    data['facebook_url'] = url
                elif platform == 'twitter':
                    data['twitter_url'] = url
                elif platform == 'instagram':
                    data['instagram_url'] = url
                elif platform == 'linkedin':
                    data['linkedin_url'] = url
                elif platform == 'whatsapp':
                    data['whatsapp_number'] = url
            
            # Extract materials from business description and reviews
            materials = self.data_processor.extract_materials(page_text)
            if materials:
                data['materials'] = materials
            
            return data
            
        except Exception as e:
            self.logger.error(f"Error scraping Yelp detail page {detail_url}: {e}")
            return {} 