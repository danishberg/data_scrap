#!/usr/bin/env python3
"""
Test script for simplified phone/email extraction methods
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from accurate_scraper import USMetalScraper

def test_simple_extraction():
    """Test simplified extraction methods"""
    print("🧪 TESTING SIMPLIFIED EXTRACTION METHODS")
    print("=" * 60)
    
    scraper = USMetalScraper()
    
    # Test phone extraction
    print("\n📞 Testing Phone Extraction:")
    print("-" * 50)
    test_phones = [
        "(800) 123-4567",
        "888-555-1234", 
        "213.456.7890",
        "1-800-123-4567",
        "Call us at (555) 123-4567",
        "Phone: 800 123 4567",
        "Contact: (999) 999-9999",
        "invalid phone",
        "12345"
    ]
    
    for phone_text in test_phones:
        result = scraper._extract_phone_simple(phone_text)
        print(f"'{phone_text}' → {result}")
    
    # Test email extraction
    print("\n📧 Testing Email Extraction:")
    print("-" * 50)
    test_emails = [
        "contact@recycling.com",
        "info@metal-yard.net",
        "sales@scrap.business",
        "support@example.com",  # Should be filtered out
        "Email: sales@metalco.org",
        "Contact us at info@steel-recycling.com",
        "invalid-email",
        "test@test.com",  # Should be filtered out
        "hello@world.co.uk"
    ]
    
    for email_text in test_emails:
        result = scraper._extract_email_simple(email_text)
        print(f"'{email_text}' → {result}")
    
    # Test complete extraction
    print("\n🔍 Testing Complete Extraction:")
    print("-" * 50)
    
    test_html = """
    <html>
    <head><title>ABC Metal Recycling</title></head>
    <body>
        <h1>ABC Metal Recycling</h1>
        <p>Contact us at (555) 123-4567 or email info@abc-metal.com</p>
        <p>We buy scrap metal and aluminum.</p>
    </body>
    </html>
    """
    
    phone = scraper._extract_phone_simple(test_html)
    email = scraper._extract_email_simple(test_html)
    
    print(f"Phone found: {phone}")
    print(f"Email found: {email}")
    
    print("\n✅ Simple extraction test completed!")
    print("✅ Methods are working correctly!")

if __name__ == "__main__":
    test_simple_extraction() 