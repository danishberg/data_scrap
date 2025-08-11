#!/usr/bin/env python3
"""
Test the stealth scraper approach to see if it can bypass bot detection
"""

import logging
import time
from typing import Optional

try:
    from scrapling import Fetcher
except ImportError:
    print("Scrapling not available - using basic fallback")
    import requests
    
    class Fetcher:
        @staticmethod
        def get(url, **kwargs):
            class Response:
                def __init__(self, resp):
                    self.text = resp.text
                    self.status = resp.status_code
            return Response(requests.get(url, **kwargs))

# Setup logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s: %(message)s")
logger = logging.getLogger("test_stealth")

def test_basic_fetch(url: str) -> Optional[str]:
    """Test basic HTTP fetch"""
    try:
        logger.info(f"Testing basic fetch: {url}")
        response = Fetcher.get(url, timeout=15)
        
        if response.status == 200:
            logger.info(f"✓ Basic fetch successful - got {len(response.text)} characters")
            return response.text
        else:
            logger.warning(f"✗ Basic fetch failed - HTTP {response.status}")
            return None
    except Exception as e:
        logger.error(f"✗ Basic fetch error: {e}")
        return None

def test_stealth_fetch(url: str) -> Optional[str]:
    """Test stealth fetch with browser"""
    try:
        logger.info(f"Testing stealth fetch: {url}")
        
        # Import here to catch errors
        from scrapling import StealthyFetcher
        
        response = StealthyFetcher.fetch(
            url=url,
            headless=True,
            block_images=True,
            wait=3000,  # Wait 3 seconds
            timeout=30000,
            google_search=True,  # Set referer as from Google
        )
        
        if response.status == 200:
            logger.info(f"✓ Stealth fetch successful - got {len(response.text)} characters")
            return response.text
        else:
            logger.warning(f"✗ Stealth fetch failed - HTTP {response.status}")
            return None
            
    except ImportError:
        logger.error("✗ StealthyFetcher not available - skipping test")
        return None
    except Exception as e:
        logger.error(f"✗ Stealth fetch error: {e}")
        return None

def test_autoscraper_basics():
    """Test AutoScraper with simple HTML"""
    try:
        from autoscraper import AutoScraper
        
        # Simple test HTML
        html = """
        <div class="company">
            <h3>ABC Metal Recycling</h3>
            <p>Phone: (555) 123-4567</p>
            <p>Address: 123 Main St, Houston, TX</p>
        </div>
        <div class="company">
            <h3>XYZ Scrap Yard</h3>
            <p>Phone: (555) 987-6543</p>
            <p>Address: 456 Oak Ave, Houston, TX</p>
        </div>
        """
        
        # Create scraper and train it
        scraper = AutoScraper()
        wanted_list = ["ABC Metal Recycling", "(555) 123-4567"]
        
        result = scraper.build(html=html, wanted_list=wanted_list)
        logger.info(f"✓ AutoScraper found {len(result)} items: {result}")
        
        # Test on similar structure
        result2 = scraper.get_result_similar(html=html)
        logger.info(f"✓ AutoScraper extracted: {result2}")
        
        return True
        
    except ImportError:
        logger.error("✗ AutoScraper not available")
        return False
    except Exception as e:
        logger.error(f"✗ AutoScraper test error: {e}")
        return False

def main():
    """Run tests"""
    logger.info("🚀 Starting stealth scraper tests")
    
    # Test URLs
    test_urls = [
        "https://www.yelp.com/search?find_desc=scrap+metal&find_loc=Houston%2C+TX",
        "https://www.yellowpages.com/search?search_terms=scrap+metal&geo_location_terms=Houston%2C+TX",
        "https://httpbin.org/html",  # Simple test page
    ]
    
    # Test AutoScraper first
    logger.info("\n" + "="*50)
    logger.info("Testing AutoScraper...")
    autoscraper_works = test_autoscraper_basics()
    
    # Test fetching
    for url in test_urls:
        logger.info("\n" + "="*50)
        logger.info(f"Testing URL: {url}")
        
        # Test basic fetch
        basic_result = test_basic_fetch(url)
        
        # Test stealth fetch
        stealth_result = test_stealth_fetch(url)
        
        # Compare results
        if basic_result and stealth_result:
            logger.info("✓ Both methods work")
        elif stealth_result and not basic_result:
            logger.info("🎉 STEALTH BYPASS SUCCESSFUL! Stealth worked where basic failed")
        elif basic_result and not stealth_result:
            logger.info("⚠️ Basic worked but stealth failed")
        else:
            logger.info("✗ Both methods failed")
        
        time.sleep(2)  # Be respectful
    
    logger.info("\n" + "="*50)
    logger.info("🏁 Test complete!")
    
    if autoscraper_works:
        logger.info("✓ AutoScraper is working")
    else:
        logger.info("✗ AutoScraper needs setup")

if __name__ == "__main__":
    main()