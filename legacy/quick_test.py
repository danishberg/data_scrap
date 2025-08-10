#!/usr/bin/env python3
"""
Quick test to debug extraction issues
"""

import requests
from bs4 import BeautifulSoup
import re

def test_phone_extraction(text):
    """Test phone extraction"""
    patterns = [
        r'\((\d{3})\)[\s\-]?(\d{3})[\s\-]?(\d{4})',  # (123) 456-7890
        r'(\d{3})[\s\-\.](\d{3})[\s\-\.](\d{4})',     # 123-456-7890 or 123.456.7890
        r'(\d{3})\s(\d{3})\s(\d{4})',                 # 123 456 7890
        r'1[\s\-]?(\d{3})[\s\-]?(\d{3})[\s\-]?(\d{4})', # 1-123-456-7890
    ]
    
    for pattern in patterns:
        matches = re.findall(pattern, text)
        if matches:
            match = matches[0]
            if len(match) == 3:
                area, exchange, number = match
                if area != '000' and exchange != '000' and number != '0000':
                    return f"({area}) {exchange}-{number}"
            elif len(match) == 4:
                area, exchange, number = match[1], match[2], match[3]
                if area != '000' and exchange != '000' and number != '0000':
                    return f"({area}) {exchange}-{number}"
    
    return None

def test_email_extraction(text):
    """Test email extraction"""
    email_pattern = r'[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}'
    matches = re.findall(email_pattern, text)
    for email in matches:
        email = email.lower()
        if not any(bad in email for bad in ['example.com', 'test.com', 'sample.com']):
            return email
    return None

def test_website(url):
    """Test extraction on a real website"""
    print(f"🔍 Testing: {url}")
    
    try:
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        }
        
        response = requests.get(url, headers=headers, timeout=10)
        print(f"  Status: {response.status_code}")
        
        if response.status_code == 200:
            soup = BeautifulSoup(response.text, 'html.parser')
            page_text = response.text
            
            # Extract title
            title = soup.find('title')
            title_text = title.get_text().strip() if title else "No title"
            print(f"  Title: {title_text}")
            
            # Test phone extraction
            phone = test_phone_extraction(page_text)
            print(f"  Phone: {phone}")
            
            # Test email extraction
            email = test_email_extraction(page_text)
            print(f"  Email: {email}")
            
            # Check for metal keywords
            metal_keywords = ['scrap', 'metal', 'recycling', 'steel', 'aluminum', 'copper']
            found_keywords = [kw for kw in metal_keywords if kw in page_text.lower()]
            print(f"  Metal keywords found: {found_keywords}")
            
            # Check if it would pass validation
            has_name = title_text and len(title_text) > 3
            has_contact = phone or email
            has_relevance = len(found_keywords) > 0
            
            print(f"  Would pass validation: {has_name and has_contact and has_relevance}")
            print(f"    - Has name: {has_name}")
            print(f"    - Has contact: {has_contact}")
            print(f"    - Has relevance: {has_relevance}")
            
            return {
                'name': title_text,
                'phone': phone,
                'email': email,
                'keywords': found_keywords,
                'passes_validation': has_name and has_contact and has_relevance
            }
        
    except Exception as e:
        print(f"  ❌ Error: {e}")
        return None

def main():
    print("🧪 QUICK EXTRACTION TEST")
    print("=" * 40)
    
    # Test some known scrap metal websites
    test_urls = [
        "https://www.schnitzersteel.com",
        "https://www.metalrecyclers.org",
        "https://www.scrapmetaljunkie.com"
    ]
    
    for url in test_urls:
        result = test_website(url)
        print("-" * 40)
    
    print("\n🔧 If no results are found, the issue might be:")
    print("1. Websites are blocking requests")
    print("2. Phone/email patterns need adjustment")
    print("3. Relevance criteria too strict")
    print("4. Network/timeout issues")

if __name__ == "__main__":
    main() 