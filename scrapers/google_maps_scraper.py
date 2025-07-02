import json
import time
import re
from urllib.parse import quote_plus, urljoin
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException
from .base_scraper import BaseScraper

class GoogleMapsScraper(BaseScraper):
    def __init__(self):
        super().__init__()
        self.base_url = "https://www.google.com/maps/search/"
        
    def scrape(self, search_term, location="", limit=100):
        """Scrape Google Maps for scrap metal centers"""
        results = []
        
        # Construct search query
        if location:
            query = f"{search_term} in {location}"
        else:
            query = search_term
        
        search_url = f"{self.base_url}{quote_plus(query)}"
        
        self.logger.info(f"Starting Google Maps scrape for: {query}")
        
        # Setup Selenium driver
        if not self.setup_selenium_driver():
            self.logger.error("Failed to setup Selenium driver")
            return results
        
        try:
            self.driver.get(search_url)
            time.sleep(3)
            
            # Wait for search results to load
            try:
                WebDriverWait(self.driver, 10).until(
                    EC.presence_of_element_located((By.CSS_SELECTOR, '[role="main"]'))
                )
            except TimeoutException:
                self.logger.error("Timeout waiting for search results")
                return results
            
            # Scroll to load more results
            self._scroll_to_load_results(limit)
            
            # Extract business listings
            business_elements = self.driver.find_elements(By.CSS_SELECTOR, 'div[data-result-index]')
            
            for i, element in enumerate(business_elements[:limit]):
                if i >= limit:
                    break
                    
                try:
                    business_data = self._extract_business_from_element(element)
                    if business_data and business_data.get('name'):
                        business_data['source_url'] = search_url
                        results.append(business_data)
                        self.logger.info(f"Extracted: {business_data.get('name', 'Unknown')}")
                except Exception as e:
                    self.logger.error(f"Error extracting business {i}: {e}")
                    continue
                
                self.add_request_delay()
            
        except Exception as e:
            self.logger.error(f"Error during Google Maps scraping: {e}")
        
        finally:
            self.cleanup()
        
        self.logger.info(f"Google Maps scraping completed. Found {len(results)} businesses.")
        return results
    
    def _scroll_to_load_results(self, target_count):
        """Scroll to load more search results"""
        last_height = 0
        attempts = 0
        max_attempts = 10
        
        while attempts < max_attempts:
            try:
                # Scroll to bottom of results panel
                results_panel = self.driver.find_element(By.CSS_SELECTOR, '[role="main"]')
                self.driver.execute_script("arguments[0].scrollTop = arguments[0].scrollHeight", results_panel)
                
                time.sleep(2)
                
                # Check if new results loaded
                current_results = len(self.driver.find_elements(By.CSS_SELECTOR, 'div[data-result-index]'))
                if current_results >= target_count:
                    break
                
                # Check if scroll height changed
                new_height = self.driver.execute_script("return arguments[0].scrollHeight", results_panel)
                if new_height == last_height:
                    attempts += 1
                else:
                    attempts = 0
                
                last_height = new_height
                
            except Exception as e:
                self.logger.error(f"Error scrolling: {e}")
                break
    
    def _extract_business_from_element(self, element):
        """Extract business data from a Google Maps search result element"""
        data = {}
        
        try:
            # Click on the element to get more details
            self.driver.execute_script("arguments[0].click();", element)
            time.sleep(2)
            
            # Extract name
            try:
                name_element = self.driver.find_element(By.CSS_SELECTOR, 'h1[data-attrid="title"]')
                data['name'] = self.data_processor.clean_text(name_element.text)
            except NoSuchElementException:
                try:
                    name_element = self.driver.find_element(By.CSS_SELECTOR, '[data-section-id="oh"] h1')
                    data['name'] = self.data_processor.clean_text(name_element.text)
                except NoSuchElementException:
                    return None
            
            # Extract address
            try:
                address_element = self.driver.find_element(By.CSS_SELECTOR, '[data-section-id="ad"] [data-value="Address"]')
                address_text = address_element.find_element(By.XPATH, '..').text
                data['full_address'] = self.data_processor.clean_text(address_text)
                
                # Parse address components
                address_parts = self.data_processor.parse_address(data['full_address'])
                data.update(address_parts)
                
                # Get coordinates
                lat, lng = self.data_processor.geocode_address(data['full_address'])
                if lat and lng:
                    data['latitude'] = lat
                    data['longitude'] = lng
                    
            except NoSuchElementException:
                pass
            
            # Extract phone number
            try:
                phone_element = self.driver.find_element(By.CSS_SELECTOR, '[data-section-id="pn0"] [data-value*="phone"]')
                phone_text = phone_element.find_element(By.XPATH, '..').text
                phones = self.data_processor.extract_phone_numbers(phone_text)
                if phones:
                    data['phone_primary'] = phones[0]
            except NoSuchElementException:
                pass
            
            # Extract website
            try:
                website_element = self.driver.find_element(By.CSS_SELECTOR, '[data-section-id="ap"] a[href*="http"]')
                data['website'] = website_element.get_attribute('href')
            except NoSuchElementException:
                pass
            
            # Extract business hours
            try:
                hours_button = self.driver.find_element(By.CSS_SELECTOR, '[data-section-id="oh"] button')
                self.driver.execute_script("arguments[0].click();", hours_button)
                time.sleep(1)
                
                hours_elements = self.driver.find_elements(By.CSS_SELECTOR, '[data-section-id="oh"] tr')
                hours_data = {}
                
                for hour_element in hours_elements:
                    try:
                        day_text = hour_element.find_element(By.TAG_NAME, 'td').text.lower()
                        time_text = hour_element.find_elements(By.TAG_NAME, 'td')[1].text
                        hours_data[day_text] = time_text
                    except:
                        continue
                
                if hours_data:
                    data['working_hours'] = hours_data
                    
            except NoSuchElementException:
                pass
            
            # Extract reviews and rating info for additional context
            try:
                page_source = self.driver.page_source
                # Look for material mentions in reviews and business description
                materials = self.data_processor.extract_materials(page_source)
                if materials:
                    data['materials'] = materials
            except:
                pass
            
            # Extract all text content for additional processing
            try:
                all_text = self.driver.find_element(By.CSS_SELECTOR, '[role="main"]').text
                
                # Extract emails
                emails = self.data_processor.extract_emails(all_text)
                if emails:
                    data['email_primary'] = emails[0]
                
                # Extract social media links
                social_links = self.data_processor.extract_social_media_links(all_text)
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
                
            except:
                pass
            
            return data
            
        except Exception as e:
            self.logger.error(f"Error extracting business data: {e}")
            return None

class GoogleSearchScraper(BaseScraper):
    def __init__(self):
        super().__init__()
        self.base_url = "https://www.google.com/search"
        
    def scrape(self, search_term, location="", limit=100):
        """Scrape Google search results for scrap metal centers"""
        results = []
        
        # Construct search query
        if location:
            query = f"{search_term} {location} contact information"
        else:
            query = f"{search_term} contact information"
        
        self.logger.info(f"Starting Google search for: {query}")
        
        page = 0
        while len(results) < limit and page < 10:  # Max 10 pages
            search_url = f"{self.base_url}?q={quote_plus(query)}&start={page * 10}"
            
            html_content = self.get_page_content(search_url)
            if not html_content:
                break
                
            soup = self.parse_html(html_content)
            if not soup:
                break
            
            # Extract search result links
            search_results = soup.select('div.g')
            
            for result in search_results:
                if len(results) >= limit:
                    break
                    
                try:
                    link_element = result.select_one('h3 a, a h3')
                    if not link_element:
                        continue
                    
                    url = link_element.get('href', '')
                    if not url or url.startswith('#'):
                        continue
                    
                    title = link_element.get_text(strip=True)
                    
                    # Skip non-relevant results
                    if not any(keyword in title.lower() for keyword in 
                              ['scrap', 'metal', 'recycling', 'junk', 'salvage']):
                        continue
                    
                    # Visit the actual business website
                    business_data = self._scrape_business_website(url, title)
                    if business_data:
                        business_data['source_url'] = url
                        results.append(business_data)
                        self.logger.info(f"Extracted: {business_data.get('name', title)}")
                
                except Exception as e:
                    self.logger.error(f"Error processing search result: {e}")
                    continue
                
                self.add_request_delay()
            
            page += 1
            self.add_request_delay()
        
        self.logger.info(f"Google search completed. Found {len(results)} businesses.")
        return results
    
    def _scrape_business_website(self, url, title):
        """Scrape individual business website"""
        try:
            html_content = self.get_page_content(url)
            if not html_content:
                return None
            
            soup = self.parse_html(html_content)
            if not soup:
                return None
            
            # Extract business data using base scraper methods
            data = self.extract_business_data(soup, url)
            
            # Use title from search result if no name found
            if not data.get('name'):
                data['name'] = self.data_processor.clean_text(title)
            
            # Add website URL
            data['website'] = url
            
            return data
            
        except Exception as e:
            self.logger.error(f"Error scraping business website {url}: {e}")
            return None 