#!/usr/bin/env python3
"""
Simple test script for Scraptraffic blog scraper
Tests basic functionality without Unicode issues
"""

import os
import sys
import time

# Add current directory to path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from parsers.news_api_collector import NewsAPICollector
from core.config import BlogConfig
from database.models import db_manager

def test_basic_functionality():
    """Test basic functionality"""
    print("SCRAPTRAFFIC TEST SUITE")
    print("=" * 50)

    # Test 1: API Keys
    print("Test 1: API Keys")
    print("-" * 20)

    newsapi_ok = bool(BlogConfig.NEWS_API_KEYS.get('newsapi'))
    openai_ok = bool(BlogConfig.OPENAI_API_KEY)

    print(f"NewsAPI: {'OK' if newsapi_ok else 'MISSING'}")
    print(f"OpenAI: {'OK' if openai_ok else 'MISSING'}")

    # Test 2: Database
    print("\nTest 2: Database")
    print("-" * 20)

    try:
        session = db_manager.get_session()
        session.close()
        print("Database: OK")
        db_ok = True
    except Exception as e:
        print(f"Database: ERROR - {e}")
        db_ok = False

    # Test 3: NewsAPI Collection
    print("\nTest 3: NewsAPI Collection")
    print("-" * 20)

    try:
        collector = NewsAPICollector('ru')
        urls = collector.get_news_urls(limit=5)

        print(f"Collected URLs: {len(urls)}")
        if urls:
            print(f"Sample URL: {urls[0][:60]}...")

        collection_ok = len(urls) > 0
        print(f"Collection: {'OK' if collection_ok else 'NO DATA'}")

    except Exception as e:
        print(f"Collection: ERROR - {e}")
        collection_ok = False

    # Summary
    print("\n" + "=" * 50)
    print("SUMMARY")

    tests = [
        ("API Keys", newsapi_ok and openai_ok),
        ("Database", db_ok),
        ("Collection", collection_ok)
    ]

    passed = sum(1 for _, result in tests if result)
    total = len(tests)

    for test_name, result in tests:
        status = "PASSED" if result else "FAILED"
        print(f"{test_name:15} {status}")

    print(f"\nOverall: {passed}/{total} tests passed")

    if passed == total:
        print("System is ready!")
        return True
    else:
        print("System needs fixes!")
        return False

if __name__ == "__main__":
    try:
        success = test_basic_functionality()
        sys.exit(0 if success else 1)
    except Exception as e:
        print(f"Test crashed: {e}")
        sys.exit(1)
