#!/usr/bin/env python3
"""
Test script for enhanced phone/email extraction methods
"""

import re
from accurate_scraper import USMetalScraper

def test_phone_extraction():
    """Test phone number extraction improvements"""
    scraper = USMetalScraper()
    
    # Test phone patterns
    test_phones = [
        "(800) 123-4567",  # Toll-free
        "888-555-1234",    # Toll-free
        "(213) 456-7890",  # Standard LA
        "1-800-CALL-NOW",  # With letters (should fail)
        "Call us at (555) 123-4567",  # With context
        "Phone: 800.123.4567",  # Dot format
        "tel:+1-800-123-4567",  # Tel format
        "Business: (713) 555-9999",  # With context
        "Main office: 214 555 1234",  # Space format
        "800 123 4567",  # Simple format
        "Contact: (999) 999-9999",  # Invalid area code
        "555-0123",  # 7-digit number
        "12345",  # Too short
        "1-800-123-4567-890",  # Too long
    ]
    
    print("🔍 Testing Phone Extraction:")
    print("=" * 50)
    
    for phone in test_phones:
        # Test extraction
        extracted = scraper._extract_phone_from_text_us(phone)
        print(f"Input: {phone:<25} → Output: {extracted}")
    
    print("\n")

def test_email_extraction():
    """Test email extraction improvements"""
    scraper = USMetalScraper()
    
    # Test email patterns
    test_emails = [
        "contact@example.com",
        "info@metal-recycling.com",
        "sales@company.co.uk",
        "support AT company DOT com",
        "info [at] example [dot] com",
        "Contact us at: info@scrapyard.net",
        "Email: sales@recycling.org",
        "Send to: admin@facility.us",
        "info@company.biz",
        "test@sub.domain.com",
        "user+tag@example.com",
        "user.name@example.com",
        "invalid-email",
        "test@",
        "@example.com",
        "user@example",
        "obfuscated [at] example [dot] com",
        "contact (at) business (dot) net",
    ]
    
    print("📧 Testing Email Extraction:")
    print("=" * 50)
    
    for email in test_emails:
        # Test extraction
        extracted = scraper._extract_email_from_text(email)
        print(f"Input: {email:<35} → Output: {extracted}")
    
    print("\n")

def test_validation():
    """Test validation improvements"""
    scraper = USMetalScraper()
    
    print("✅ Testing Phone Validation:")
    print("=" * 50)
    
    # Test validation cases
    test_cases = [
        ("800", "123", "4567", True),   # Toll-free
        ("213", "456", "7890", True),   # Standard LA
        ("000", "123", "4567", False),  # Invalid area code
        ("111", "123", "4567", False),  # Invalid area code
        ("999", "123", "4567", False),  # Invalid area code
        ("555", "012", "3456", False),  # Invalid exchange
        ("800", "000", "1234", False),  # Invalid exchange
        ("713", "555", "9999", True),   # Valid business
        ("888", "999", "8888", True),   # Toll-free
        ("555", "123", "4567", True),   # Should be valid now
    ]
    
    for area, exchange, number, expected in test_cases:
        result = scraper._validate_us_phone(area, exchange, number)
        status = "✅ PASS" if result == expected else "❌ FAIL"
        print(f"{area}-{exchange}-{number}: {result} (expected {expected}) {status}")
    
    print("\n")

def test_comprehensive_extraction():
    """Test comprehensive extraction on sample HTML"""
    scraper = USMetalScraper()
    
    # Sample HTML with various phone/email patterns
    sample_html = """
    <html>
    <body>
        <div class="contact-info">
            <span class="phone">Call us: (800) 123-4567</span>
            <span class="email">Email: contact@scrapyard.com</span>
        </div>
        <div id="contact-details">
            <p>Phone: 713-555-9999</p>
            <p>Email: info AT example DOT com</p>
        </div>
        <a href="tel:+1-888-555-1234">Click to call</a>
        <a href="mailto:sales@recycling.net">Send email</a>
        <div data-phone="(214) 555-7777" data-email="support@company.org">
            Contact Information
        </div>
        <script type="application/ld+json">
        {
            "@context": "http://schema.org",
            "@type": "LocalBusiness",
            "telephone": "800-RECYCLE",
            "email": "json@business.com"
        }
        </script>
    </body>
    </html>
    """
    
    from bs4 import BeautifulSoup
    soup = BeautifulSoup(sample_html, 'html.parser')
    
    print("🔍 Testing Comprehensive Extraction:")
    print("=" * 50)
    
    # Test phone extraction
    phone = scraper._extract_phone_comprehensive(sample_html, soup)
    print(f"Phone extracted: {phone}")
    
    # Test email extraction
    email = scraper._extract_email_comprehensive(sample_html, soup)
    print(f"Email extracted: {email}")
    
    # Test fallback methods
    if not phone:
        phone_fallback = scraper._extract_phone_fallback(sample_html, soup)
        print(f"Phone fallback: {phone_fallback}")
    
    if not email:
        email_fallback = scraper._extract_email_fallback(sample_html, soup)
        print(f"Email fallback: {email_fallback}")
    
    print("\n")

if __name__ == "__main__":
    print("🧪 ENHANCED SCRAPER TESTING")
    print("=" * 60)
    print()
    
    test_phone_extraction()
    test_email_extraction()
    test_validation()
    test_comprehensive_extraction()
    
    print("✅ Testing completed!")
    print("Run the scraper with: python accurate_scraper.py") 