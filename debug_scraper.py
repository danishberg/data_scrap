#!/usr/bin/env python3
"""
Debug script for the AI-Enhanced Metal Scraper
"""

from accurate_scraper import AIEnhancedMetalScraper
import logging

# Enable debug logging
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

def test_scraper():
    print("🐛 DEBUGGING AI-ENHANCED METAL SCRAPER")
    print("=" * 50)
    
    # Initialize scraper
    scraper = AIEnhancedMetalScraper()
    
    print("\n🔧 Testing with debug output...")
    print("Will search for just 5 businesses to debug the process")
    
    try:
        # Test with a very small number for debugging
        results = scraper.run_comprehensive_scraping(target_businesses=5)
        
        print(f"\n📊 Results: {len(results) if results else 0} businesses found")
        
        if results:
            print("\n✅ Sample results:")
            for i, business in enumerate(results[:3]):
                print(f"  {i+1}. {business.get('name', 'N/A')}")
                print(f"     📞 Phone: {business.get('phone', 'N/A')}")
                print(f"     📧 Email: {business.get('email', 'N/A')}")
                print(f"     🌐 Website: {business.get('website', 'N/A')}")
                print(f"     🎯 Relevance: {business.get('ai_relevance_score', 0)}")
                print()
        else:
            print("\n❌ No businesses found. Debug information should be above.")
            
            # Let's test the extraction methods directly
            print("\n🔧 Testing extraction methods...")
            test_text = """
            ABC Metal Recycling LLC
            We buy scrap metal, copper, aluminum, steel
            Phone: (555) 123-4567
            Email: info@abcmetal.com
            """
            
            phone = scraper._extract_phone_simple(test_text)
            email = scraper._extract_email_simple(test_text)
            
            print(f"  Simple phone extraction: {phone}")
            print(f"  Simple email extraction: {email}")
            
            # Test minimum requirements
            test_business = {
                'name': 'ABC Metal Recycling',
                'phone': phone,
                'email': email,
                'website': 'http://abcmetal.com',
                'description': 'We buy scrap metal'
            }
            
            meets_req = scraper._meets_minimum_requirements(test_business)
            print(f"  Meets minimum requirements: {meets_req}")
            
    except Exception as e:
        print(f"\n❌ Error during debugging: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    test_scraper() 