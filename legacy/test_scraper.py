#!/usr/bin/env python3
"""
Test script for the Scrap Metal Centers scraping application
This script runs a small test to verify everything is working properly.
"""

import os
import sys
import json

# Add current directory to Python path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from main import ScrapMetalScraper
from config import Config

def test_basic_functionality():
    """Test basic scraping functionality with limited scope"""
    print("Testing Scrap Metal Centers Scraper...")
    print("=" * 50)
    
    # Override config for testing
    Config.OUTPUT_DIR = "test_output"
    Config.REQUEST_DELAY = 2.0  # Slower for testing
    
    # Create scraper instance
    scraper = ScrapMetalScraper()
    
    # Test with limited parameters
    test_sources = ['google_search']  # Start with simplest scraper
    test_search_terms = ['scrap metal recycling']
    test_locations = ['New York, NY']  # Single location
    test_limit = 5  # Very small limit for testing
    
    print(f"Test Sources: {test_sources}")
    print(f"Test Search Terms: {test_search_terms}")
    print(f"Test Locations: {test_locations}")
    print(f"Test Limit: {test_limit}")
    print()
    
    try:
        # Run the test scraping
        results = scraper.run_scraping(
            sources=test_sources,
            search_terms=test_search_terms,
            locations=test_locations,
            limit_per_source=test_limit
        )
        
        print(f"\nTest Results:")
        print(f"Number of results: {len(results)}")
        
        if results:
            print("\nSample result:")
            sample = results[0]
            for key, value in sample.items():
                if isinstance(value, (dict, list)):
                    print(f"  {key}: {json.dumps(value, indent=4)}")
                else:
                    print(f"  {key}: {value}")
        
        print(f"\nTest completed successfully!")
        print(f"Check the '{Config.OUTPUT_DIR}' directory for exported files.")
        
        return True
        
    except Exception as e:
        print(f"Error during testing: {e}")
        import traceback
        traceback.print_exc()
        return False

def verify_dependencies():
    """Verify that all required dependencies are installed"""
    print("Verifying dependencies...")
    
    try:
        import requests
        print("✓ requests")
    except ImportError:
        print("✗ requests - Run: pip install requests")
        return False
    
    try:
        import bs4
        print("✓ beautifulsoup4")
    except ImportError:
        print("✗ beautifulsoup4 - Run: pip install beautifulsoup4")
        return False
    
    try:
        import selenium
        print("✓ selenium")
    except ImportError:
        print("✗ selenium - Run: pip install selenium")
        return False
    
    try:
        import pandas
        print("✓ pandas")
    except ImportError:
        print("✗ pandas - Run: pip install pandas")
        return False
    
    try:
        import sqlalchemy
        print("✓ sqlalchemy")
    except ImportError:
        print("✗ sqlalchemy - Run: pip install sqlalchemy")
        return False
    
    print("All core dependencies are installed!")
    return True

if __name__ == "__main__":
    print("Scrap Metal Centers Scraper - Test Script")
    print("=" * 50)
    
    # Verify dependencies first
    if not verify_dependencies():
        print("\nPlease install missing dependencies and try again.")
        sys.exit(1)
    
    # Run basic functionality test
    success = test_basic_functionality()
    
    if success:
        print("\n" + "=" * 50)
        print("✅ Test completed successfully!")
        print("The scraper is ready to use.")
        print("\nTo run the full application:")
        print("python main.py")
        print("\nTo run with custom parameters:")
        print("python main.py --sources google_maps --limit 50 --locations 'Chicago, IL'")
    else:
        print("\n" + "=" * 50)
        print("❌ Test failed!")
        print("Please check the error messages above and fix any issues.")
        sys.exit(1) 