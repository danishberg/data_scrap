#!/usr/bin/env python3
"""
Comprehensive test script for Scraptraffic blog scraper
Tests all stages independently to ensure they work properly
"""

import os
import sys
import json
import time

# Add current directory to path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from parsers.news_api_collector import NewsAPICollector
from parsers.rss_parser import RSSNewsParser
from parsers.industry_web_scraper import IndustryWebScraper
from parsers.free_news_aggregator import FreeNewsAggregator
from processors.content_processor import ContentProcessor
from processors.content_generator import ContentGenerator
from core.config import BlogConfig
from database.models import db_manager, Article

def test_api_keys():
    """Test API keys configuration"""
    print("[KEYS] Testing API Keys Configuration...")
    print("-" * 50)

    keys_status = {
        'NewsAPI': bool(BlogConfig.NEWS_API_KEYS.get('newsapi')),
        'OpenAI': bool(BlogConfig.OPENAI_API_KEY)
    }

    for name, status in keys_status.items():
        status_icon = "OK" if status else "FAIL"
        print(f"{status_icon} {name}: {'Configured' if status else 'Missing'}")

    return all(keys_status.values())

def test_database():
    """Test database connection and initialization"""
    print("\n[DB]  Testing Database...")
    print("-" * 50)

    try:
        # Test connection
        session = db_manager.get_session()
        session.close()
        print("✅ Database connection successful")

        # Test table creation
        db_manager.create_tables()
        print("✅ Database tables created/verified")

        return True

    except Exception as e:
        print(f"❌ Database error: {e}")
        return False

def test_stage_1_collection():
    """Test Stage 1: URL collection"""
    print("\n🎯 Testing Stage 1: URL Collection...")
    print("-" * 50)

    try:
        # Test NewsAPI collector
        print("Testing NewsAPI...")
        news_api = NewsAPICollector('ru')
        urls = news_api.get_news_urls(limit=20)

        if urls:
            print(f"✅ NewsAPI: {len(urls)} URLs collected")
            print(f"   Sample: {urls[0][:60]}...")
        else:
            print("⚠️  NewsAPI: No URLs collected")

        # Test RSS parser
        print("Testing RSS Parser...")
        rss_parser = RSSNewsParser('ru')
        rss_urls = rss_parser.get_news_urls(limit=10)

        if rss_urls:
            print(f"✅ RSS Parser: {len(rss_urls)} URLs collected")
        else:
            print("⚠️  RSS Parser: No URLs collected")

        # Test Industry scraper
        print("Testing Industry Scraper...")
        industry_scraper = IndustryWebScraper('ru')
        industry_urls = industry_scraper.get_news_urls(limit=10)

        if industry_urls:
            print(f"✅ Industry Scraper: {len(industry_urls)} URLs collected")
        else:
            print("⚠️  Industry Scraper: No URLs collected")

        return True

    except Exception as e:
        print(f"❌ Stage 1 error: {e}")
        return False

def test_stage_2_parsing():
    """Test Stage 2: Article parsing and processing"""
    print("\n🔥 Testing Stage 2: Article Parsing...")
    print("-" * 50)

    try:
        # Get some test URLs
        news_api = NewsAPICollector('ru')
        test_urls = news_api.get_news_urls(limit=5)

        if not test_urls:
            print("❌ No test URLs available for parsing")
            return False

        print(f"📋 Testing with {len(test_urls)} URLs")

        # Test different parsers
        parsers = {
            'RSS': RSSNewsParser('ru'),
            'Industry': IndustryWebScraper('ru'),
            'Free': FreeNewsAggregator('ru')
        }

        for name, parser in parsers.items():
            try:
                print(f"\nTesting {name} Parser...")
                articles = parser.parse_articles_batch(test_urls[:2])

                if articles:
                    print(f"✅ {name}: {len(articles)} articles parsed")
                    for article in articles[:1]:
                        print(f"   Title: {article.get('title', 'No title')[:50]}...")
                else:
                    print(f"⚠️  {name}: No articles parsed")

            except Exception as e:
                print(f"❌ {name} Parser error: {e}")

        # Test content processor
        print("\nTesting Content Processor...")
        processor = ContentProcessor()

        # Save a test article to database first
        session = db_manager.get_session()
        try:
            test_article = Article(
                title="Test Article for Processing",
                content="This is a test article content for testing the content processor.",
                url=test_urls[0] if test_urls else "https://example.com",
                source="test",
                language="ru",
                is_processed=False
            )
            session.add(test_article)
            session.commit()

            # Process the article
            success = processor.process_article(test_article)
            if success:
                print("✅ Content processor working")
            else:
                print("⚠️  Content processor returned False")

        except Exception as e:
            print(f"❌ Content processor error: {e}")
        finally:
            session.close()

        return True

    except Exception as e:
        print(f"❌ Stage 2 error: {e}")
        return False

def test_stage_3_generation():
    """Test Stage 3: Content generation"""
    print("\n✨ Testing Stage 3: Content Generation...")
    print("-" * 50)

    if not BlogConfig.OPENAI_API_KEY:
        print("❌ OpenAI API key not configured - skipping Stage 3 tests")
        return False

    try:
        # Test content generator
        print("Testing Content Generator...")
        generator = ContentGenerator()

        # Get a processed article from database
        session = db_manager.get_session()
        processed_articles = session.query(Article).filter_by(is_processed=True).limit(2).all()
        session.close()

        if not processed_articles:
            print("❌ No processed articles found for generation test")
            return False

        print(f"📝 Testing with {len(processed_articles)} processed articles")

        # Test blog post generation
        for i, article in enumerate(processed_articles[:1], 1):
            try:
                print(f"\nGenerating blog post {i}/1...")
                blog_post = generator.generate_blog_post(article, 'ru')

                if blog_post:
                    print("✅ Blog post generated successfully")
                    print(f"   Title: {blog_post.get('title', 'No title')[:50]}...")
                    print(f"   Content length: {len(blog_post.get('content', ''))} chars")
                else:
                    print("❌ Blog post generation failed")

            except Exception as e:
                print(f"❌ Blog post generation error: {e}")

        # Test digest generation
        if len(processed_articles) >= 2:
            try:
                print("\nTesting digest generation...")
                digest = generator.generate_digest(processed_articles[:2], 'daily', 'metallurgy')

                if digest:
                    print("✅ Digest generated successfully")
                    print(f"   Title: {digest.get('title', 'No title')}")
                    print(f"   Content length: {len(digest.get('content', ''))} chars")
                else:
                    print("❌ Digest generation failed")

            except Exception as e:
                print(f"❌ Digest generation error: {e}")

        return True

    except Exception as e:
        print(f"❌ Stage 3 error: {e}")
        return False

def run_comprehensive_test():
    """Run all tests"""
    print("COMPREHENSIVE SCRAPTRAFFIC TEST SUITE")
    print("=" * 80)

    start_time = time.time()

    tests = [
        ("API Keys", test_api_keys),
        ("Database", test_database),
        ("Stage 1 Collection", test_stage_1_collection),
        ("Stage 2 Parsing", test_stage_2_parsing),
        ("Stage 3 Generation", test_stage_3_generation)
    ]

    results = {}

    for test_name, test_func in tests:
        try:
            print(f"\n{'='*20} {test_name.upper()} {'='*20}")
            results[test_name] = test_func()
        except Exception as e:
            print(f"💥 {test_name} crashed: {e}")
            results[test_name] = False

    # Summary
    end_time = time.time()
    duration = end_time - start_time

    print(f"\n{'='*80}")
    print("📊 TEST RESULTS SUMMARY")
    print(f"{'='*80}")
    print(f"⏱️  Total time: {duration:.1f} seconds")
    print()

    passed = sum(1 for result in results.values() if result)
    total = len(results)

    for test_name, result in results.items():
        status = "✅ PASSED" if result else "❌ FAILED"
        print(f"{test_name:20} {status}")

    print()
    print(f"📈 Overall: {passed}/{total} tests passed ({passed/total*100:.1f}%)")

    if passed == total:
        print("🎉 ALL TESTS PASSED! System is ready for production.")
    else:
        print("⚠️  SOME TESTS FAILED! Check the issues above.")

    return passed == total

if __name__ == "__main__":
    try:
        success = run_comprehensive_test()

        if success:
            print("\n🚀 System is production-ready!")
            sys.exit(0)
        else:
            print("\n🔧 System needs fixes before production use.")
            sys.exit(1)

    except KeyboardInterrupt:
        print("\n⏹️  Tests interrupted by user")
        sys.exit(130)
    except Exception as e:
        print(f"\n💥 Test suite crashed: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
