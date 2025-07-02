import os
import sys
import requests
import time
import logging
import random
from abc import ABC, abstractmethod
from urllib.parse import urljoin, urlparse
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, WebDriverException
from webdriver_manager.chrome import ChromeDriverManager
from webdriver_manager.firefox import GeckoDriverManager
from webdriver_manager.microsoft import EdgeChromiumDriverManager
from typing import List, Dict, Any, Optional
import zipfile
import subprocess

# Add parent directory to path to import config and utils
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config import Config
from utils import DataProcessor

class BaseScraper(ABC):
    def __init__(self, delay_range=(2, 5), max_retries=3):
        self.delay_range = delay_range
        self.max_retries = max_retries
        self.data_processor = DataProcessor()
        self.session = requests.Session()
        self.driver = None
        self.logger = self._setup_logger()
        
        # Setup session headers
        self.session.headers.update({
            'User-Agent': self._get_random_user_agent(),
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.5',
            'Accept-Encoding': 'gzip, deflate',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1',
        })
    
    def _setup_logger(self):
        """Setup logging for the scraper"""
        logger = logging.getLogger(self.__class__.__name__)
        logger.setLevel(logging.INFO)
        
        if not logger.handlers:
            handler = logging.StreamHandler()
            formatter = logging.Formatter(
                '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
            )
            handler.setFormatter(formatter)
            logger.addHandler(handler)
        
        return logger
    
    def _get_random_user_agent(self):
        """Get a random user agent"""
        user_agents = [
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36',
            'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
        ]
        return random.choice(user_agents)

    def _fix_chromedriver_windows(self, driver_path):
        """Fix ChromeDriver for Windows systems"""
        try:
            # Check if it's a zip file that needs extraction
            if driver_path.endswith('.zip'):
                extract_dir = os.path.dirname(driver_path)
                with zipfile.ZipFile(driver_path, 'r') as zip_ref:
                    zip_ref.extractall(extract_dir)
                # Find the actual executable
                for file in os.listdir(extract_dir):
                    if file.startswith('chromedriver') and file.endswith('.exe'):
                        return os.path.join(extract_dir, file)
            
            # If it's a directory, look for the exe
            if os.path.isdir(driver_path):
                for root, dirs, files in os.walk(driver_path):
                    for file in files:
                        if file.startswith('chromedriver') and file.endswith('.exe'):
                            return os.path.join(root, file)
            
            # If it's already an exe file
            if driver_path.endswith('.exe') and os.path.exists(driver_path):
                return driver_path
                
            return None
        except Exception as e:
            self.logger.error(f"Error fixing ChromeDriver: {e}")
            return None

    def setup_selenium_driver(self):
        """Setup Selenium WebDriver with enhanced Windows compatibility"""
        try:
            options = Options()
            options.add_argument('--headless')
            options.add_argument('--no-sandbox')
            options.add_argument('--disable-dev-shm-usage')
            options.add_argument('--disable-gpu')
            options.add_argument('--disable-web-security')
            options.add_argument('--disable-extensions')
            options.add_argument('--disable-plugins')
            options.add_argument('--disable-images')
            options.add_argument('--disable-javascript')
            options.add_argument('--window-size=1920,1080')
            options.add_argument(f'--user-agent={self._get_random_user_agent()}')
            
            # Try multiple approaches to get working ChromeDriver
            driver_attempts = [
                self._try_system_chrome,
                self._try_webdriver_manager,
                self._try_manual_download
            ]
            
            for attempt in driver_attempts:
                try:
                    driver_path = attempt()
                    if driver_path:
                        # Fix Windows path issues
                        if sys.platform.startswith('win'):
                            driver_path = self._fix_chromedriver_windows(driver_path)
                        
                        if driver_path and os.path.exists(driver_path):
                            service = Service(driver_path)
                            self.driver = webdriver.Chrome(service=service, options=options)
                            self.logger.info(f"✅ ChromeDriver successfully initialized: {driver_path}")
                            return True
                except Exception as e:
                    self.logger.warning(f"Driver attempt failed: {e}")
                    continue
            
            raise Exception("All ChromeDriver setup attempts failed")
            
        except Exception as e:
            self.logger.error(f"Failed to setup Selenium driver: {e}")
            return False

    def _try_system_chrome(self):
        """Try to use system Chrome"""
        if sys.platform.startswith('win'):
            possible_paths = [
                r"C:\Program Files\Google\Chrome\Application\chrome.exe",
                r"C:\Program Files (x86)\Google\Chrome\Application\chrome.exe"
            ]
            for path in possible_paths:
                if os.path.exists(path):
                    return path
        return None

    def _try_webdriver_manager(self):
        """Try webdriver-manager"""
        try:
            return ChromeDriverManager().install()
        except Exception as e:
            self.logger.warning(f"WebDriver Manager failed: {e}")
            return None

    def _try_manual_download(self):
        """Try manual ChromeDriver download"""
        try:
            # Get Chrome version
            chrome_version = self._get_chrome_version()
            if not chrome_version:
                return None
            
            # Download appropriate ChromeDriver
            driver_url = f"https://chromedriver.storage.googleapis.com/{chrome_version}/chromedriver_win32.zip"
            response = requests.get(driver_url, timeout=30)
            
            if response.status_code == 200:
                driver_dir = os.path.join(os.getcwd(), "drivers")
                os.makedirs(driver_dir, exist_ok=True)
                
                zip_path = os.path.join(driver_dir, "chromedriver.zip")
                with open(zip_path, 'wb') as f:
                    f.write(response.content)
                
                # Extract
                with zipfile.ZipFile(zip_path, 'r') as zip_ref:
                    zip_ref.extractall(driver_dir)
                
                exe_path = os.path.join(driver_dir, "chromedriver.exe")
                if os.path.exists(exe_path):
                    return exe_path
            
            return None
        except Exception as e:
            self.logger.warning(f"Manual download failed: {e}")
            return None

    def _get_chrome_version(self):
        """Get installed Chrome version"""
        try:
            if sys.platform.startswith('win'):
                result = subprocess.run([
                    'reg', 'query', 
                    r'HKEY_CURRENT_USER\Software\Google\Chrome\BLBeacon',
                    '/v', 'version'
                ], capture_output=True, text=True)
                
                if result.returncode == 0:
                    version = result.stdout.split()[-1]
                    return version.split('.')[0]  # Major version
            return None
        except Exception:
            return None
    
    def get_page_content(self, url, use_selenium=False, wait_for_element=None):
        """Get page content using requests or Selenium"""
        try:
            if use_selenium:
                return self._get_page_with_selenium(url, wait_for_element)
            else:
                return self._get_page_with_requests(url)
        except Exception as e:
            self.logger.error(f"Failed to get page content from {url}: {e}")
            return None
    
    def _get_page_with_requests(self, url):
        """Get page content using requests"""
        try:
            response = self.session.get(url, timeout=Config.TIMEOUT)
            response.raise_for_status()
            return response.text
        except requests.RequestException as e:
            self.logger.error(f"Request failed for {url}: {e}")
            return None
    
    def _get_page_with_selenium(self, url, wait_for_element=None):
        """Get page content using Selenium"""
        if not self.driver:
            if not self.setup_selenium_driver():
                return None
        
        try:
            self.driver.get(url)
            
            # Wait for specific element if provided
            if wait_for_element:
                wait = WebDriverWait(self.driver, 10)
                wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, wait_for_element)))
            
            return self.driver.page_source
            
        except (TimeoutException, WebDriverException) as e:
            self.logger.error(f"Selenium failed for {url}: {e}")
            return None
    
    def parse_html(self, html_content):
        """Parse HTML content with BeautifulSoup"""
        if not html_content:
            return None
        return BeautifulSoup(html_content, 'html.parser')
    
    def extract_business_data(self, soup, base_url=""):
        """Extract business data from parsed HTML"""
        if not soup:
            return {}
        
        data = {}
        
        # Extract title/name
        title_selectors = ['h1', '.business-name', '.company-name', '[data-name]', 'title']
        for selector in title_selectors:
            element = soup.select_one(selector)
            if element and element.get_text(strip=True):
                data['name'] = self.data_processor.clean_text(element.get_text())
                break
        
        # Extract description
        desc_selectors = ['.description', '.about', '.overview', 'meta[name="description"]']
        for selector in desc_selectors:
            element = soup.select_one(selector)
            if element:
                if selector.startswith('meta'):
                    text = element.get('content', '')
                else:
                    text = element.get_text(strip=True)
                if text:
                    data['description'] = self.data_processor.clean_text(text)
                    break
        
        # Extract all text for pattern matching
        page_text = soup.get_text()
        
        # Extract contact information
        phones = self.data_processor.extract_phone_numbers(page_text)
        if phones:
            data['phone_primary'] = phones[0]
            if len(phones) > 1:
                data['phone_secondary'] = phones[1]
        
        emails = self.data_processor.extract_emails(page_text)
        if emails:
            data['email_primary'] = emails[0]
            if len(emails) > 1:
                data['email_secondary'] = emails[1]
        
        # Extract address
        address_selectors = [
            '.address', '.location', '[data-address]', '.contact-info',
            '.business-address', '.street-address'
        ]
        for selector in address_selectors:
            elements = soup.select(selector)
            for element in elements:
                text = element.get_text(strip=True)
                if text and any(word in text.lower() for word in ['street', 'ave', 'road', 'drive', 'blvd']):
                    data['full_address'] = self.data_processor.clean_text(text)
                    break
            if 'full_address' in data:
                break
        
        # Extract social media links
        social_links = self.data_processor.extract_social_media_links(page_text, base_url)
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
            elif platform == 'telegram':
                data['telegram_contact'] = url
        
        # Extract working hours
        hours = self.data_processor.extract_working_hours(page_text)
        if hours:
            data['working_hours'] = hours
        
        # Extract materials
        materials = self.data_processor.extract_materials(page_text)
        if materials:
            data['materials'] = materials
        
        # Extract website from links
        website_selectors = ['a[href*="http"]', 'link[rel="canonical"]']
        for selector in website_selectors:
            element = soup.select_one(selector)
            if element:
                href = element.get('href', '')
                if href and 'http' in href:
                    data['website'] = self.data_processor.validate_url(href, base_url)
                    break
        
        return data
    
    def add_request_delay(self):
        """Add delay between requests"""
        self.data_processor.add_delay()
    
    @abstractmethod
    def scrape(self, search_term, location="", limit=100):
        """Abstract method for scraping data"""
        pass
    
    def cleanup(self):
        """Cleanup resources"""
        if self.driver:
            try:
                self.driver.quit()
            except Exception as e:
                self.logger.error(f"Error closing driver: {e}")
            finally:
                self.driver = None
        
        if self.session:
            self.session.close()
    
    def __del__(self):
        """Destructor to cleanup resources"""
        self.cleanup()

    def make_request(self, url, retries=None, **kwargs):
        """Make HTTP request with retries and rate limiting"""
        if retries is None:
            retries = self.max_retries
            
        for attempt in range(retries + 1):
            try:
                # Random delay to avoid rate limiting
                time.sleep(random.uniform(*self.delay_range))
                
                # Rotate user agent for each request
                self.session.headers['User-Agent'] = self._get_random_user_agent()
                
                # Add random delays between requests
                if attempt > 0:
                    delay = random.uniform(5, 15)  # Longer delay for retries
                    self.logger.info(f"Waiting {delay:.1f}s before retry {attempt}")
                    time.sleep(delay)
                
                response = self.session.get(url, timeout=30, **kwargs)
                
                # Check for rate limiting
                if response.status_code == 429:
                    wait_time = random.uniform(30, 60)
                    self.logger.warning(f"Rate limited (429). Waiting {wait_time:.1f}s")
                    time.sleep(wait_time)
                    continue
                
                # Check for blocking
                if response.status_code == 403:
                    self.logger.warning(f"Access forbidden (403) for {url}")
                    # Try with different headers
                    self.session.headers.update({
                        'Referer': 'https://www.google.com/',
                        'Origin': 'https://www.google.com',
                        'X-Requested-With': 'XMLHttpRequest'
                    })
                    continue
                
                response.raise_for_status()
                return response
                
            except requests.exceptions.RequestException as e:
                self.logger.warning(f"Request failed (attempt {attempt + 1}/{retries + 1}): {e}")
                if attempt == retries:
                    self.logger.error(f"Request failed for {url}: {e}")
                    raise
                    
        return None

    def extract_page_data(self, soup, url):
        """Extract business data from BeautifulSoup object"""
        try:
            # Try multiple extraction strategies
            business_data = {}
            
            # Extract basic info
            business_data['name'] = self._extract_business_name(soup)
            business_data['address'] = self._extract_address(soup)
            business_data['phone'] = self.data_processor.extract_phone_numbers(soup.get_text())
            business_data['email'] = self.data_processor.extract_emails(soup.get_text())
            business_data['website'] = self._extract_website(soup, url)
            business_data['hours'] = self.data_processor.extract_working_hours(soup.get_text())
            
            # Extract social media
            social_links = self.data_processor.extract_social_media_links(soup)
            business_data.update(social_links)
            
            # Extract materials and services
            text_content = soup.get_text().lower()
            business_data['materials'] = self._extract_materials(text_content)
            business_data['services'] = self._extract_services(text_content)
            
            # Add metadata
            business_data['source_url'] = url
            business_data['scraped_at'] = time.time()
            
            return business_data
            
        except Exception as e:
            self.logger.error(f"Error extracting data from {url}: {e}")
            return {}

    def _extract_business_name(self, soup):
        """Extract business name from various HTML elements"""
        selectors = [
            'h1', 'h2', '.business-name', '.company-name', 
            '[data-testid="business-name"]', '.name', '.title'
        ]
        
        for selector in selectors:
            element = soup.select_one(selector)
            if element and element.get_text(strip=True):
                return element.get_text(strip=True)
        
        # Fallback to title tag
        title = soup.find('title')
        if title:
            return title.get_text(strip=True)
        
        return ""

    def _extract_address(self, soup):
        """Extract address from various HTML elements"""
        selectors = [
            '.address', '.location', '[data-testid="address"]',
            '.contact-info address', '.business-address'
        ]
        
        for selector in selectors:
            element = soup.select_one(selector)
            if element:
                return element.get_text(strip=True)
        
        return ""

    def _extract_website(self, soup, current_url):
        """Extract website URL"""
        links = soup.find_all('a', href=True)
        for link in links:
            href = link['href']
            if any(indicator in href.lower() for indicator in ['website', 'www.', 'http']):
                if href.startswith('http'):
                    return href
        
        return current_url

    def _extract_materials(self, text):
        """Extract accepted materials from text"""
        materials = []
        material_keywords = [
            'scrap metal', 'steel', 'aluminum', 'copper', 'brass', 'iron',
            'lead', 'zinc', 'stainless steel', 'carbide', 'nickel',
            'electronics', 'ewaste', 'batteries', 'catalytic converters'
        ]
        
        for material in material_keywords:
            if material in text:
                materials.append(material)
        
        return materials

    def _extract_services(self, text):
        """Extract services from text"""
        services = []
        service_keywords = [
            'pickup', 'demolition', 'container', 'roll-off', 'dumpster',
            'processing', 'recycling', 'disposal', 'buying', 'selling'
        ]
        
        for service in service_keywords:
            if service in text:
                services.append(service)
        
        return services

    def selenium_get_page(self, url):
        """Get page using Selenium with error handling"""
        try:
            if not self.driver:
                if not self.setup_selenium_driver():
                    return None
            
            # Random delay
            time.sleep(random.uniform(*self.delay_range))
            
            self.driver.get(url)
            
            # Wait for page to load
            WebDriverWait(self.driver, 10).until(
                EC.presence_of_element_located((By.TAG_NAME, "body"))
            )
            
            return self.driver.page_source
            
        except TimeoutException:
            self.logger.warning(f"Page load timeout for {url}")
            return None
        except WebDriverException as e:
            self.logger.error(f"Selenium error for {url}: {e}")
            return None

    def cleanup_selenium_driver(self):
        """Clean up Selenium driver"""
        if self.driver:
            try:
                self.driver.quit()
                self.driver = None
            except Exception as e:
                self.logger.warning(f"Error closing driver: {e}")

    def __del__(self):
        """Cleanup when object is destroyed"""
        self.cleanup_selenium_driver() 