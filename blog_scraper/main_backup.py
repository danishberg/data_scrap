#!/usr/bin/env python3
"""
   Scraptraffic - Production-Ready Blog Scraper
   Complete pipeline for collecting, processing, and generating metal industry content
"""


import sys
import os
import logging
from datetime import datetime, timedelta
from typing import List, Dict, Any, Optional
import argparse
import json
import glob

import asyncio
import aiohttp
from aiohttp import ClientSession, TCPConnector
import concurrent.futures
from urllib.parse import urlparse
import time
from typing import List, Dict, Any, Optional
import random

# Add current directory to path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from database.models import db_manager, Article, Category, Digest, ProcessingLog
from core.config import BlogConfig
from parsers.news_api_collector import NewsAPICollector
from parsers.rss_parser import RSSNewsParser
from parsers.industry_web_scraper import IndustryWebScraper
from parsers.free_news_aggregator import FreeNewsAggregator
from parsers.metallurgy_collector import MetallurgyCollector
from processors.content_processor import ContentProcessor
from processors.content_generator import ContentGenerator
from core.vector_segmentation import VectorSegmentation
from core.data_exporter import DataExporter

# Global connection pool for async requests
connector = TCPConnector(limit=100, limit_per_host=20, force_close=False)

# Retry configuration
MAX_RETRIES = 3
RETRY_DELAY = 1.0  # seconds

# Timeout configuration
REQUEST_TIMEOUT = 30
CONNECTION_TIMEOUT = 10

def setup_logging():
    """ """
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler('blog_scraper.log'),
            logging.StreamHandler()
        ]
    )

def init_database():
    """  """
    print("  ...")

    try:
        #    
        db_manager.create_tables()

        #   
        session = db_manager.get_session()

        base_categories = [
            'ferrous', 'non_ferrous', 'precious', 'scrap',
            'prices', 'production', 'trade', 'policy',
            'companies', 'technology', 'environment',
            'market_analysis', 'logistics', 'other'
        ]

        for cat_name in base_categories:
            category = Category(
                name=cat_name,
                slug=cat_name.lower().replace(' ', '-')
            )
            session.add(category)

        session.commit()
        print(f"  {len(base_categories)}  ")

        session.close()
        print("   ")
        return True

    except Exception as e:
        print(f"    : {e}")
        return False

def scrape_news(language: str = 'ru', limit: int = 50, use_api: bool = True):
    """ """
    print(f"     {language} (limit: {limit})...")

    articles_data = []

    try:
        # : NewsAPI      
        if use_api and BlogConfig.NEWS_API_KEYS.get('newsapi'):
            print("  NewsAPI   ...")
            from parsers.news_api_collector import NewsAPICollector
            news_api = NewsAPICollector(language)
            api_articles = news_api.get_metal_industry_news(limit=min(limit, 100))  # NewsAPI limit is 100 per request
            articles_data.extend(api_articles)
            print(f" NewsAPI  {len(api_articles)} ")

        #  RSS 
        remaining_limit = limit - len(articles_data)
        if remaining_limit > 0:
            rss_parser = RSSNewsParser(language)
            rss_articles = rss_parser.search_relevant_articles(remaining_limit)
            articles_data.extend(rss_articles)
            print(f" RSS   {len(rss_articles)} ")

        #   API   
        remaining_limit = limit - len(articles_data)
        if remaining_limit > 0 and use_api:
            from parsers.api_parser import APINewsParser
            api_parser = APINewsParser(language)
            api_articles = api_parser.search_relevant_articles(remaining_limit)
            articles_data.extend(api_articles)
            print(f"  API   {len(api_articles)} ")

        #     
        saved_count = save_articles_to_db(articles_data)
        print(f"  {saved_count}    ")

        return len(articles_data)

    except Exception as e:
        print(f"    : {e}")
        return 0

def save_articles_to_db(articles_data: List[Dict[str, Any]]) -> int:
    """    """
    if not articles_data:
        return 0

    session = db_manager.get_session()
    saved_count = 0

    try:
        for article_data in articles_data:
            try:
                # ,      
                existing = session.query(Article).filter_by(url=article_data.get('url')).first()
                if existing:
                    continue

                #   
                article = Article(
                    title=article_data.get('title', ''),
                    original_title=article_data.get('original_title', ''),
                    content=article_data.get('content', ''),
                    original_content=article_data.get('original_content', ''),
                    url=article_data.get('url', ''),
                    source=article_data.get('source', ''),
                    language=article_data.get('language', 'ru'),
                    author=article_data.get('author', ''),
                    published_at=article_data.get('published_at'),
                    scraped_at=article_data.get('scraped_at', datetime.utcnow()),
                    is_processed=False
                )

                session.add(article)
                saved_count += 1

            except Exception as e:
                print(f"    : {e}")
                continue

        session.commit()

    except Exception as e:
        print(f"      : {e}")
        session.rollback()
    finally:
        session.close()

    return saved_count

def run_filtered_search(language: str, limit: int) -> int:
    """
          
    """
    print("   ")
    print("=" * 50)

    #     
    print("      ( ):")
    print("   : , , , ")
    keywords_input = input("    : ").strip()

    if not keywords_input:
        print("    ")
        return 0

    #   
    keywords = [kw.strip() for kw in keywords_input.split(',') if kw.strip()]
    if not keywords:
        print("    ")
        return 0

    print(f"    : {', '.join(keywords)}")
    print(f" :  {limit} ")
    print("-" * 50)

    articles_data = []
    total_collected = 0

    # 1. NewsAPI    
    try:
        from parsers.news_api_collector import NewsAPICollector
        news_api = NewsAPICollector(language)
        api_articles = news_api.get_general_news(keywords=keywords, limit=min(limit, 100))
        articles_data.extend(api_articles)
        total_collected += len(api_articles)
        print(f" NewsAPI: {len(api_articles)} ")
    except Exception as e:
        print(f"   NewsAPI: {e}")

    # 2. RSS  (,  )
    if total_collected < limit:
        try:
            remaining = limit - total_collected
            rss_parser = RSSNewsParser(language)
            #    RSS     
            rss_articles = rss_parser.search_relevant_articles(remaining, skip_relevance_check=True)
            articles_data.extend(rss_articles)
            total_collected += len(rss_articles)
            print(f" RSS : {len(rss_articles)} ")
        except Exception as e:
            print(f"   RSS: {e}")

    # 3.  - ( )
    if total_collected < limit:
        try:
            remaining = limit - total_collected
            from parsers.industry_web_scraper import IndustryWebScraper
            industry_scraper = IndustryWebScraper(language)
            industry_articles = industry_scraper.get_news(limit=remaining, skip_filters=True)
            articles_data.extend(industry_articles)
            total_collected += len(industry_articles)
            print(f"  : {len(industry_articles)} ")
        except Exception as e:
            print(f"    : {e}")

    #    
    if articles_data:
        saved_count = save_articles_to_db(articles_data)
        print(f"    : {saved_count} ")
        print(f"   : {', '.join(keywords)}")
    else:
        print("    ")

    return total_collected

def process_articles(limit: int = 10):
    """  (, , )"""
    print(f"    (limit: {limit})...")

    session = db_manager.get_session()

    try:
        #   
        unprocessed_articles = session.query(Article).filter_by(is_processed=False).limit(limit).all()

        if not unprocessed_articles:
            print("    ")
            return 0

        print(f"  {len(unprocessed_articles)}  ")

        processor = ContentProcessor()
        processed_count = 0

        for article in unprocessed_articles:
            print(f"  : {article.title[:50]}...")
            success = processor.process_article(article)

            if success:
                processed_count += 1
                session.commit()  #     
            else:
                print(f"        {article.id}")

        print(f"  {processed_count} ")
        return processed_count

    except Exception as e:
        print(f"    : {e}")
        return 0
    finally:
        session.close()

def async_retry(max_retries: int = MAX_RETRIES, delay: float = RETRY_DELAY):
    """Decorator for async function retries"""
    def decorator(func):
        async def wrapper(*args, **kwargs):
            for attempt in range(max_retries):
                try:
                    return await func(*args, **kwargs)
                except Exception as e:
                    if attempt == max_retries - 1:
                        raise
                    await asyncio.sleep(delay * (2 ** attempt) + random.uniform(0, 0.1))
            return None
        return wrapper
    return decorator

def sync_retry(max_retries: int = MAX_RETRIES, delay: float = RETRY_DELAY):
    """Decorator for sync function retries"""
    def decorator(func):
        def wrapper(*args, **kwargs):
            for attempt in range(max_retries):
                try:
                    return func(*args, **kwargs)
                except Exception as e:
                    if attempt == max_retries - 1:
                        raise
                    time.sleep(delay * (2 ** attempt) + random.uniform(0, 0.1))
            return None
        return wrapper
    return decorator

def stage_1_data_collection(language: str, articles_limit: int) -> int:
    """Stage 1: Production-Ready Collection of Metallurgy URLs"""
    print("=" * 80)
    print("STAGE 1: PRODUCTION-READY METALLURGY URL COLLECTION")
    print("=" * 80)

    # Create directory for Stage 1
    stage1_dir = os.path.join("blog_database", "stage_1_raw_articles")
    os.makedirs(stage1_dir, exist_ok=True)

    start_time = datetime.now()

    # Production-ready sources - focus on reliable, fast sources
    production_sources = {
        'newsapi': {
            'type': 'api',
            'priority': 1,
            'expected_urls': 50,
            'timeout': 30
        },
        'reliable_rss': {
            'type': 'rss',
            'priority': 1,
            'sources': [
                'https://ria.ru/export/rss2/economy.xml',
                'https://tass.ru/rss/v2.xml',
                'https://www.interfax.ru/rss.asp',
                'https://www.vedomosti.ru/rss/news.xml',
                'https://rg.ru/xml/index.xml'
            ],
            'expected_urls': 100,
            'timeout': 15
        },
        'metallurgy_focused': {
            'type': 'scraping',
            'priority': 2,
            'sources': [
                {'url': 'https://www.metalinfo.ru/ru/news', 'name': 'metalinfo'},
                {'url': 'https://www.metallplace.ru/news/', 'name': 'metallplace'},
                {'url': 'https://www.metallurg.ru/news/', 'name': 'metallurg'},
                {'url': 'https://www.steelguru.com/steel-news', 'name': 'steelguru'},
                {'url': 'https://www.metalbulletin.com/news', 'name': 'metalbulletin'}
            ],
            'expected_urls': 50,
            'timeout': 20
        },
        'business_news': {
            'type': 'scraping',
            'priority': 3,
            'sources': [
                {'url': 'https://www.kommersant.ru/finance', 'name': 'kommersant'},
                {'url': 'https://www.vedomosti.ru/business', 'name': 'vedomosti'},
                {'url': 'https://www.rbc.ru/business/', 'name': 'rbc'}
            ],
            'expected_urls': 30,
            'timeout': 15
        }
    }

    # Advanced relevance scoring for metallurgy content - STRICT FILTERING
    METALLURGY_KEYWORDS = {
        # MUST-HAVE Core metallurgy terms (highest priority)
        'металлург': 30, 'metallurg': 30, 'steel': 30, 'сталь': 30,
        'металлообработка': 28, 'metalworking': 28, 'mining': 28, 'горнодобыча': 28,
        'scrap': 25, 'лом': 25, 'metal recycling': 25, 'переработка металла': 25,
        'ferrous': 25, 'черные металлы': 25, 'non-ferrous': 25, 'цветные металлы': 25,
        'metalinfo': 30, 'metaltorg': 30, 'steelland': 30,  # Domain names

        # Metal types (high priority)
        'aluminum': 20, 'алюмини': 20, 'copper': 20, 'медь': 20, 'iron': 20, 'железо': 20,
        'zinc': 18, 'цинк': 18, 'nickel': 18, 'никель': 18, 'chromium': 18, 'хром': 18,
        'titanium': 18, 'титан': 18, 'magnesium': 18, 'магний': 18, 'brass': 18, 'латунь': 18,

        # Industry terms (medium priority)
        'ore': 15, 'руда': 15, 'commodity': 15, 'сырье': 15, 'alloy': 15, 'сплав': 15,
        'завод': 15, 'plant': 15, 'комбинат': 15, 'mill': 15, 'foundry': 15, 'литейный': 15,
        'производство': 15, 'production': 15, 'manufactur': 15, 'metallplace': 20,

        # Business terms (ONLY if combined with metallurgy)
        'metal price': 12, 'цена металл': 12, 'metal market': 12, 'рынок металл': 12,
        'metal export': 12, 'экспорт металл': 12, 'metal import': 12, 'импорт металл': 12
    }

    def calculate_relevance_score(url: str) -> int:
        """Calculate sophisticated relevance score for metallurgy content"""
        url_lower = url.lower()
        score = 0

        # Base scoring from keywords
        for keyword, weight in METALLURGY_KEYWORDS.items():
            if keyword.lower() in url_lower:
                score += weight

        # Bonus for metallurgy-specific domains
        metallurgy_domains = [
            'metallurg.ru', 'metalinfo.ru', 'metallplace.ru', 'steelguru.com',
            'metalbulletin.com', 'mining.com', 'severstal.com', 'evraz.com',
            'mmk.ru', 'nornickel.ru', 'aluminum.org', 'copper.org'
        ]

        domain = urlparse(url).netloc.lower()
        if any(met_domain in domain for met_domain in metallurgy_domains):
            score += 15  # Significant bonus for specialized sources

        # Bonus for news/article patterns
        if any(pattern in url_lower for pattern in ['/news/', '/article/', '/story/', '/новости/', '/статья/']):
            score += 5

        # Penalty for irrelevant content
        irrelevant_terms = ['politics', 'политика', 'sport', 'спорт', 'entertainment', 'развлечения']
        if any(term in url_lower for term in irrelevant_terms):
            score -= 10

        return max(0, score)  # No negative scores

    def is_relevant_metallurgy_url(url: str, source_type: str = 'general') -> bool:
        """Determine if URL is relevant to metallurgy - pragmatic approach"""
        url_lower = url.lower()
        score = calculate_relevance_score(url)

        # More permissive thresholds for production volume
        thresholds = {
            'metallurgy_focused': 10,   # Lower threshold for specialized sources
            'newsapi': 15,              # Lower for API
            'business_news': 15,        # Lower for business news
            'general': 20,              # Lower for general sources
            'rss': 12                   # Lower for RSS
        }

        threshold = thresholds.get(source_type, 15)  # Default moderate threshold

        # More flexible required terms - allow broader industry content
        required_terms = [
            'металл', 'metal', 'сталь', 'steel', 'лом', 'scrap',
            'алюмини', 'aluminum', 'медь', 'copper', 'железо', 'iron',
            'metallurg', 'металлург', 'mining', 'горнодобыча', 'commodity', 'сырье',
            'ore', 'руда', 'завод', 'plant', 'производство', 'production'
        ]

        has_required_term = any(term in url_lower for term in required_terms)

        # Accept if score is good OR has metallurgy term
        return score >= threshold or has_required_term

    async def collect_all_sources():
        """Main async function for parallel collection"""
        
        @async_retry(max_retries=2, delay=1.0)
        async def collect_from_newsapi():
            """Fast NewsAPI collection with proper filtering"""
            try:
                print("  [1/4] Collecting from NewsAPI...")
        news_api = NewsAPICollector(language)
                urls = news_api.get_news_urls(limit=min(max(10, articles_limit // 3), 50))

                filtered_urls = []
                for url in urls:
                    if is_relevant_metallurgy_url(url, 'newsapi'):
                        filtered_urls.append(url)

                print(f"    NewsAPI: {len(filtered_urls)} quality URLs from {len(urls)} collected")
                return filtered_urls
    except Exception as e:
                print(f"    NewsAPI failed: {e}")
                return []

        @async_retry(max_retries=2, delay=1.0)
        async def collect_from_rss():
            """RSS collection with content-aware filtering"""
            try:
                print("  [2/4] Collecting from RSS feeds...")
                rss_parser = RSSNewsParser(language)
                # Get entries with titles to improve filtering
                entries = rss_parser.get_news_entries(limit=min(articles_limit, 200))

                def is_entry_relevant(entry: Dict[str, Any]) -> bool:
                    url = (entry.get('url') or '').lower()
                    title = (entry.get('title') or '').lower()
                    summary = (entry.get('summary') or '').lower()
                    # Check URL relevance
                    if is_relevant_metallurgy_url(url, 'rss'):
                        return True
                    # Check title/summary for metallurgy terms
                    terms = [
                        'металл', 'сталь', 'лом', 'руда', 'алюмини', 'медь', 'никель', 'цинк',
                        'steel', 'metal', 'scrap', 'ore', 'aluminum', 'copper', 'nickel', 'zinc'
                    ]
                    text = f"{title} {summary}"
                    return any(t in text for t in terms)

                filtered_urls = []
                for e in entries:
                    if is_entry_relevant(e):
                        filtered_urls.append(e['url'])

                # If still low, try EN feeds
                if language != 'en' and len(filtered_urls) < max(10, articles_limit // 4):
                    try:
                        rss_en = RSSNewsParser('en')
                        en_entries = rss_en.get_news_entries(limit=30)
                        for e in en_entries:
                            if is_entry_relevant(e):
                                filtered_urls.append(e['url'])
                    except Exception:
                        pass

                # Dedupe and limit
                filtered_urls = list(dict.fromkeys(filtered_urls))[:min(articles_limit // 2, 100)]

                print(f"    RSS: {len(filtered_urls)} quality URLs from {len(entries)} entries")
                return filtered_urls
        except Exception as e:
                print(f"    RSS failed: {e}")
                return []

        @async_retry(max_retries=2, delay=1.0)
        async def collect_from_metallurgy_sources():
            """Targeted collection from metallurgy-specific sources"""
            try:
                print("  [3/4] Collecting from metallurgy-focused sources...")

                # Use specialized metallurgy collector
                def collect_metallurgy_sync():
                    """Collect from metallurgy sources (synchronous)"""
                    try:
                        collector = MetallurgyCollector(language)
                        # Collect URLs from real metallurgy sites
                        urls = collector.collect_metallurgy_urls(limit=min(100, articles_limit))
                        
                        # Filter for relevance
                        filtered_urls = []
                        for url in urls:
                            if is_relevant_metallurgy_url(url, 'metallurgy_focused'):
                                filtered_urls.append(url)
                        
                        return filtered_urls
        except Exception as e:
                        print(f"    Metallurgy collector error: {e}")
                        return []

                # Execute in thread pool
                loop = asyncio.get_event_loop()
                with concurrent.futures.ThreadPoolExecutor(max_workers=1) as executor:
                    all_urls = await loop.run_in_executor(executor, collect_metallurgy_sync)

                print(f"    Metallurgy sources: {len(all_urls)} quality URLs collected")
                return all_urls
            except Exception as e:
                print(f"    Metallurgy sources failed: {e}")
                return []

        @async_retry(max_retries=2, delay=1.0)
        async def collect_from_business_sources():
            """Skip business sources to avoid off-topic URLs"""
            print("  [4/4] Skipping business sources for relevance")
            return []

        # Execute all collection tasks in parallel
        print(f"Target: {articles_limit} high-quality metallurgy URLs")
        print("Starting parallel collection from 4 sources...")

        @async_retry(max_retries=2, delay=1.0)
        async def collect_from_newsapi_limited():
            try:
                print("  [1/4] Collecting from NewsAPI (limited)...")
                # Run sync work in thread executor with small payload to avoid delays
                import concurrent.futures
                def run_sync():
                    try:
                        api = NewsAPICollector(language)
                        # Single broad query to reduce 429 risk
                        q = 'металл OR steel OR metal OR алюминий OR copper'
                        articles = api.get_structured_news(q, limit=min(8, max(4, articles_limit // 5)))
                        return [a.get('url') for a in articles if a.get('url')]
                    except Exception as e:
                        print(f"    NewsAPI limited failed: {e}")
                        return []
                loop = asyncio.get_event_loop()
                with concurrent.futures.ThreadPoolExecutor(max_workers=1) as ex:
                    fut = loop.run_in_executor(ex, run_sync)
                    try:
                        urls = await asyncio.wait_for(fut, timeout=20.0)
                    except asyncio.TimeoutError:
                        print("    NewsAPI skipped due to timeout")
                        urls = []
                filtered = []
                for url in urls:
                    if is_relevant_metallurgy_url(url, 'newsapi'):
                        filtered.append(url)
                print(f"    NewsAPI: {len(filtered)} quality URLs from {len(urls)} collected")
                return filtered
            except Exception as e:
                print(f"    NewsAPI limited failed: {e}")
                return []

        @async_retry(max_retries=2, delay=1.0)
        async def collect_from_industry_scraper():
            """Additional collection from industry web scraper"""
            try:
                print("  [5/5] Collecting from industry web scraper...")
                def scrape_industry_sync():
                    try:
                        scraper = IndustryWebScraper(language)
                        urls = scraper.get_news_urls(limit=min(80, articles_limit // 2))

                        filtered_urls = []
                        for url in urls:
                            if is_relevant_metallurgy_url(url, 'metallurgy_focused'):
                                filtered_urls.append(url)

                        return filtered_urls
        except Exception as e:
                        print(f"    Industry scraper error: {e}")
                        return []

                loop = asyncio.get_event_loop()
                with concurrent.futures.ThreadPoolExecutor(max_workers=1) as executor:
                    urls = await loop.run_in_executor(executor, scrape_industry_sync)

                print(f"    Industry scraper: {len(urls)} quality URLs collected")
                return urls
            except Exception as e:
                print(f"    Industry scraper failed: {e}")
                return []

        tasks = [
            collect_from_newsapi_limited(),
            collect_from_rss(),
            collect_from_metallurgy_sources(),
            collect_from_business_sources(),
            collect_from_industry_scraper()
        ]

        results = await asyncio.gather(*tasks, return_exceptions=True)

        # Combine and filter results
        all_collected_urls = []
        sources_stats = {'newsapi': 0, 'rss': 0, 'metallurgy': 0, 'business': 0}

        for i, result in enumerate(results):
            if isinstance(result, list):
                source_names = ['newsapi', 'rss', 'metallurgy', 'business']
                source_name = source_names[i]
                sources_stats[source_name] = len(result)
                all_collected_urls.extend(result)
            else:
                print(f"Task {i} failed with exception: {result}")

        # Remove duplicates and ensure quality
        unique_urls = []
        seen_urls = set()

        for url in all_collected_urls:
            if url not in seen_urls:
                unique_urls.append(url)
                seen_urls.add(url)

        # Limit to target amount
        final_urls = unique_urls[:articles_limit]

        collection_time = datetime.now() - start_time

        # Create comprehensive report
    stage1_data = {
        'stage': 1,
        'language': language,
            'total_urls': len(final_urls),
            'target_limit': articles_limit,
            'collection_time_seconds': collection_time.total_seconds(),
        'collected_at': datetime.now().isoformat(),
        'sources': sources_stats,
            'urls': final_urls,
        'quality_metrics': {
            'relevance_filtering_applied': True,
                'duplicates_removed': len(all_collected_urls) - len(unique_urls),
                'quality_threshold_used': True,
                'parallel_collection': True,
                'average_urls_per_minute': len(final_urls) / max(collection_time.total_seconds() / 60, 1)
        }
    }

        # Save JSON file
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        json_path = os.path.join(stage1_dir, f"raw_urls_production_{timestamp}.json")

    with open(json_path, 'w', encoding='utf-8') as f:
        json.dump(stage1_data, f, ensure_ascii=False, indent=2)

        print("=" * 80)
        print("STAGE 1 COMPLETED SUCCESSFULLY!")
        print("=" * 80)
        print(f"Collected: {len(final_urls)} high-quality metallurgy URLs")
        print(f"Time taken: {collection_time.total_seconds():.1f} seconds")
        print(f"Speed: {len(final_urls) / max(collection_time.total_seconds() / 60, 1):.1f} URLs/minute")
        print(f"Source breakdown: {sources_stats}")
        print(f"Saved to: {json_path}")

        return len(final_urls)

    # Main execution with proper async handling
    try:
        # Try to get existing event loop
        loop = asyncio.get_event_loop()
        if loop.is_running():
            # If loop is already running, we need to handle differently
            import nest_asyncio
            nest_asyncio.apply()
            return loop.run_until_complete(collect_all_sources())
        else:
            return loop.run_until_complete(collect_all_sources())
    except RuntimeError:
        # Create new event loop
        return asyncio.run(collect_all_sources())

def stage_2_vector_segmentation(language: str, limit: int) -> int:
    """Stage 2: Парсинг и сегментация статей металлургической отрасли"""
    print("Stage 2: Парсинг и сегментация статей металлургической отрасли")
    print("=" * 70)

    # Создаем директорию для Stage 2
    stage1_dir = os.path.join("blog_database", "stage_1_raw_articles")
    stage2_dir = os.path.join("blog_database", "stage_2_segmented")
    os.makedirs(stage2_dir, exist_ok=True)

    # Ищем последний Stage 1 JSON файл
    json_files = glob.glob(os.path.join(stage1_dir, "*.json"))
    if not json_files:
        print("Нет файлов Stage 1 JSON. Запустите Stage 1 сначала.")
        return 0

    # Берем самый свежий файл
    latest_json = max(json_files, key=os.path.getctime)
    print(f"Используем Stage 1 данные: {latest_json}")

    with open(latest_json, 'r', encoding='utf-8') as f:
        stage1_data = json.load(f)

    urls = stage1_data.get('urls', [])[:limit]
    print(f"Обрабатываем {len(urls)} URL")

    if not urls:
        print("Нет URL для обработки")
        return 0

    # Парсинг статей через разные источники
    parsed_articles = []
    parsing_stats = {'rss': 0, 'industry': 0, 'newspaper': 0}

    # 1. RSS парсер (самый надежный для структурированных данных)
    try:
        print("Парсинг через RSS...")
        rss_parser = RSSNewsParser(language)
        rss_articles = rss_parser.parse_articles_batch(urls[:min(len(urls)//2, 30)])
        parsed_articles.extend(rss_articles)
        parsing_stats['rss'] = len(rss_articles)
        print(f"RSS парсер: {len(rss_articles)} статей")

    except Exception as e:
        print(f"Ошибка RSS парсера: {e}")

    # 2. Industry web scraper (специализированные источники)
    if len(parsed_articles) < len(urls):
        try:
            print("Парсинг через отраслевые источники...")
            remaining_urls = [url for url in urls if url not in [a.get('url') for a in parsed_articles]]
            industry_scraper = IndustryWebScraper(language)
            # Увеличим лимит и дадим скраперу шанс обработать больше ссылок
            industry_articles = industry_scraper.parse_articles_batch(remaining_urls[:40])
            parsed_articles.extend(industry_articles)
            parsing_stats['industry'] = len(industry_articles)
            print(f"Industry scraper: {len(industry_articles)} статей")

        except Exception as e:
            print(f"Ошибка industry scraper: {e}")

    # 3. Newspaper3k (универсальный парсер)
    if len(parsed_articles) < len(urls):
        try:
            print("Парсинг через Newspaper3k...")
            remaining_urls = [url for url in urls if url not in [a.get('url') for a in parsed_articles]]
            free_aggregator = FreeNewsAggregator(language)
            newspaper_articles = free_aggregator.parse_articles_batch(remaining_urls[:50])
            parsed_articles.extend(newspaper_articles)
            parsing_stats['newspaper'] = len(newspaper_articles)
            print(f"Newspaper3k: {len(newspaper_articles)} статей")

        except Exception as e:
            print(f"Ошибка Newspaper3k: {e}")

    print(f"Результаты парсинга: {parsing_stats}")

    if not parsed_articles:
        print("Не удалось распарсить ни одной статьи")
        return 0

    # Сохраняем статьи в базу данных
    print("Сохранение в базу данных...")
    saved_count = save_articles_to_db(parsed_articles)
    print(f"Сохранено в БД: {saved_count} статей")

    # Обрабатываем статьи контент-процессором
    print("Обработка контента...")
    session = db_manager.get_session()
    try:
        # Получаем необработанные статьи
        unprocessed_articles = session.query(Article).filter_by(is_processed=False).limit(len(parsed_articles)).all()

        if unprocessed_articles:
            processor = ContentProcessor()
            processed_count = 0

            for article in unprocessed_articles:
                try:
                    print(f"Обрабатываем: {article.title[:50]}...")
                    success = processor.process_article(article)
                    if success:
                        processed_count += 1
                        session.commit()
                        print(f"  Обработано: {article.title[:50]}...")
                    else:
                        print(f"  Ошибка обработки: {article.title[:50]}...")

                except Exception as e:
                    print(f"  Критическая ошибка: {article.id}: {e}")
                    continue

            print(f"Обработано статей: {processed_count}")

        # Создаем статистику категорий
        category_stats = {}
        processed_articles = session.query(Article).filter_by(is_processed=True).all()
        try:
            vector_indexer = VectorSegmentation()
            added = vector_indexer.add_articles_to_vector_db(processed_articles)
            print(f"[VEC] Добавлено в векторный индекс: {added}")
        except Exception as e:
            print(f"[WARN] Векторный индекс недоступен: {e}")

        for article in processed_articles:
            if article.categories_list:
                categories = [cat.strip() for cat in article.categories_list.split(',')]
                for cat in categories:
                    category_stats[cat] = category_stats.get(cat, 0) + 1

        # Создаем JSON отчет для Stage 2 с сегментацией по категориям
        stage2_data = {
            'stage': 2,
            'language': language,
            'total_articles_parsed': len(parsed_articles),
            'total_articles_saved': saved_count,
            'total_articles_processed': processed_count,
            'segmented_at': datetime.now().isoformat(),
            'from_stage1': os.path.basename(latest_json),
            'parsing_stats': parsing_stats,
            'category_distribution': category_stats,
            'categories': {},  # Сегментация по категориям
            'uncategorized': []  # Статьи без категорий
        }

        # Сегментируем статьи по категориям
        for article in processed_articles:
            article_data = {
                'id': article.id,
                'title': article.title,
                'url': article.url,
                'source': article.source,
                'content_preview': article.content[:200] if article.content else '',
                'published_at': article.published_at.isoformat() if article.published_at else None,
                'language': article.language
            }
            
            if article.categories_list:
                # Разбиваем на категории
                categories = [cat.strip() for cat in article.categories_list.split(',')]
                
                for category in categories:
                    if category not in stage2_data['categories']:
                        stage2_data['categories'][category] = []
                    
                    # Добавляем статью в категорию (если еще не добавлена)
                    if not any(a['id'] == article.id for a in stage2_data['categories'][category]):
                        stage2_data['categories'][category].append(article_data)
            else:
                # Без категорий
                stage2_data['uncategorized'].append(article_data)

        # Сохраняем JSON файл
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        json_path = os.path.join(stage2_dir, f"segmented_articles_{timestamp}.json")
        with open(json_path, 'w', encoding='utf-8') as f:
            json.dump(stage2_data, f, ensure_ascii=False, indent=2)

        print("=" * 70)
        print(f"[OK] Stage 2 завершен: {processed_count} статей обработано")
        print(f"[SAVE] Отчет сохранен: {json_path}")
        return processed_count

    except Exception as e:
        print(f"[ERROR] Критическая ошибка в Stage 2: {e}")
        return 0
    finally:
        session.close()

def stage_3_content_generation(limit: int = 20, target_language: str = 'ru') -> int:
    """Stage 3: Генерация блог-постов и контента с использованием ИИ"""
    print("[STAGE 3] Генерация блог-постов и контента металлургической отрасли")
    print("=" * 70)

    # Создаем директорию для Stage 3
    stage3_dir = os.path.join("blog_database", "stage_3_generated")
    os.makedirs(stage3_dir, exist_ok=True)

    try:
        # Проверяем наличие API ключа OpenAI
        if not BlogConfig.OPENAI_API_KEY:
            print("[ERROR] OpenAI API ключ не найден. Генерация блог-постов невозможна.")
            print("[INFO] Настройте OPENAI_API_KEY в файле .env")
            return 0

        # Инициализируем генератор контента
        content_gen = ContentGenerator()
        print("[AI] Инициализирован генератор контента с OpenAI")

        # Получаем обработанные статьи из базы данных
        session = db_manager.get_session()
        processed_articles = session.query(Article).filter_by(is_processed=True).limit(limit).all()
        session.close()

        if not processed_articles:
            print("[ERROR] Нет обработанных статей для генерации блог-постов")
            print("[INFO] Запустите Stage 2 сначала для обработки статей")
            return 0

        print(f"[ARTICLES] Найдено {len(processed_articles)} обработанных статей для генерации")

        # Генерируем блог-посты
        print("[GENERATING] Генерация блог-постов...")
        generated_posts = []

        for i, article in enumerate(processed_articles, 1):
            try:
                print(f"  [{i}/{len(processed_articles)}] Генерация поста: {article.title[:50]}...")

                # Генерируем блог-пост
                blog_post = content_gen.generate_blog_post(article, target_language)

                if blog_post:
                    generated_posts.append(blog_post)
                    print(f"    [OK] Сгенерирован: {blog_post.get('title', 'Без названия')[:50]}...")
                else:
                    print(f"    [ERROR] Не удалось сгенерировать пост для: {article.title[:50]}...")

            except Exception as e:
                print(f"    [ERROR] Ошибка генерации для статьи {article.id}: {e}")
                continue

        if not generated_posts:
            print("[ERROR] Не удалось сгенерировать ни одного блог-поста")
            return 0

        print(f"[OK] Сгенерировано {len(generated_posts)} блог-постов")

        # Генерируем дайджест (если есть минимум 3 статьи)
        digest = None
        if len(processed_articles) >= 3:
            try:
                print("[DIGEST] Генерация ежедневного дайджеста...")
                digest = content_gen.generate_digest(processed_articles[:5], 'daily', 'metallurgy')

                if digest:
                    print(f"[OK] Дайджест сгенерирован: {digest.get('title', 'Без названия')}")
                    print(f"   Статьи в дайджесте: {digest.get('article_count', 0)}")
                else:
                    print("[ERROR] Не удалось сгенерировать дайджест")

            except Exception as e:
                print(f"[ERROR] Ошибка генерации дайджеста: {e}")

        # Сохраняем результаты в JSON
        stage3_data = {
            'stage': 3,
            'target_language': target_language,
            'total_articles_processed': len(processed_articles),
            'total_posts_generated': len(generated_posts),
            'generated_at': datetime.now().isoformat(),
            'digest_generated': digest is not None,
            'posts': generated_posts
        }

        # Добавляем информацию о дайджесте если он был создан
        if digest:
            stage3_data['digest'] = {
                'title': digest.get('title', ''),
                'content_preview': digest.get('content', '')[:200] + '...' if len(digest.get('content', '')) > 200 else digest.get('content', ''),
                'article_count': digest.get('article_count', 0),
                'frequency': digest.get('frequency', 'daily'),
                'theme': digest.get('theme', 'metallurgy')
            }

        # Сохраняем JSON файл
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        json_path = os.path.join(stage3_dir, f"generated_content_{timestamp}.json")
        with open(json_path, 'w', encoding='utf-8') as f:
            json.dump(stage3_data, f, ensure_ascii=False, indent=2)

        # Экспортируем готовые посты для публикации
        try:
            exporter = DataExporter()
            export_path = exporter.export_for_publication()
            print(f"[EXPORT] Готовые посты экспортированы: {export_path}")
        except Exception as e:
            print(f"[ERROR] Ошибка экспорта: {e}")

        print("=" * 70)
        print(f"[OK] Stage 3 завершен: {len(generated_posts)} блог-постов сгенерировано")
        if digest:
            print(f"[DIGEST] Дайджест также создан: {digest.get('title', 'Без названия')}")
        print(f"[SAVE] Результаты сохранены: {json_path}")

        return len(generated_posts)

    except Exception as e:
        print(f"[ERROR] Критическая ошибка в Stage 3: {e}")
        return 0

def run_3_stage_system(language: str = 'ru', articles_limit: int = 50):
    """
    Запуск полной 3-этапной системы сбора и обработки металлургического контента
    """
    print("ЗАПУСК ПОЛНОЙ 3-ЭТАПНОЙ СИСТЕМЫ SCRAPTRAFFIC")
    print("=" * 80)

    # Проверяем наличие API ключей
    warnings = []
    if not BlogConfig.NEWS_API_KEYS.get('newsapi'):
        warnings.append("❌ NewsAPI ключ не настроен")
    if not BlogConfig.OPENAI_API_KEY:
        warnings.append("❌ OpenAI API ключ не настроен")

    if warnings:
        print("⚠️  ПРЕДУПРЕЖДЕНИЯ:")
        for warning in warnings:
            print(f"  {warning}")
        print("💡 Настройте недостающие ключи в файле .env")
        print()

    start_time = datetime.now()
    results = {'stage1': 0, 'stage2': 0, 'stage3': 0}

    try:
        print("ЭТАП 1: Сбор сырых URL статей металлургической отрасли")
        print("-" * 60)

        # Этап 1: Сбор URL
        results['stage1'] = stage_1_data_collection(language, articles_limit)

        if results['stage1'] == 0:
            print("[ERROR] Этап 1 завершился неудачно - нет собранных URL")
            return results

        print()
        print("ЭТАП 2: Парсинг и сегментация статей")
        print("-" * 60)

        # Этап 2: Парсинг и обработка
        results['stage2'] = stage_2_vector_segmentation(language, results['stage1'])

        if results['stage2'] == 0:
            print("[WARNING] Этап 2 завершился с предупреждениями - статьи распарсены, но не обработаны")
        else:
            print(f"[OK] Этап 2 успешно завершен: {results['stage2']} статей обработано")

        print()
        print("ЭТАП 3: Генерация блог-постов с использованием ИИ")
        print("-" * 60)

        # Этап 3: Генерация контента
        results['stage3'] = stage_3_content_generation(results['stage2'], language)

        if results['stage3'] == 0:
            print("[ERROR] Этап 3 завершился неудачно - блог-посты не сгенерированы")
        else:
            print(f"[OK] Этап 3 успешно завершен: {results['stage3']} блог-постов создано")

        # Финальная статистика
        end_time = datetime.now()
        duration = end_time - start_time

        print()
        print("=" * 80)
        print("СИСТЕМА ЗАВЕРШЕНА УСПЕШНО!")
        print("=" * 80)
        print(f"Общее время выполнения: {duration.total_seconds():.1f} секунд")
        print(f"РЕЗУЛЬТАТЫ:")
        print(f"   • Этап 1 (URL сбор): {results['stage1']} URL")
        print(f"   • Этап 2 (обработка): {results['stage2']} статей")
        print(f"   • Этап 3 (генерация): {results['stage3']} блог-постов")

        success_rate = (results['stage3'] / results['stage1'] * 100) if results['stage1'] > 0 else 0
        print(f"Коэффициент конверсии: {success_rate:.1f}% (от URL к готовым постам)")

        return results

    except Exception as e:
        print(f"КРИТИЧЕСКАЯ ОШИБКА В СИСТЕМЕ: {e}")
        print("Проверьте логи для диагностики проблемы")
        return results

def show_statistics():
    """  """
    print("   :")
    print("-" * 50)

    session = db_manager.get_session()

    try:
        #  
        total_articles = session.query(Article).count()
        processed_articles = session.query(Article).filter_by(is_processed=True).count()
        published_articles = session.query(Article).filter_by(is_published=True).count()
        total_digests = session.query(Digest).count()

        print(f"  : {total_articles}")
        print(f"  : {processed_articles}")
        print(f" : {published_articles}")
        print(f" : {total_digests}")

        #   
        print("\n  :")
        from sqlalchemy import func
        source_stats = session.query(Article.source, func.count(Article.id)).group_by(Article.source).all()
        for source, count in source_stats:
            print(f"  {source}: {count}")

        #   
        print("\n   :")
        category_stats = session.query(Category.name, func.count(Article.id)).\
            join(Article.categories).\
            group_by(Category.name).all()
        for category, count in category_stats:
            print(f"  {category}: {count}")

    except Exception as e:
        print(f"   : {e}")
    finally:
        session.close()

def export_data():
    """ """
    print("  ...")

    try:
        exporter = DataExporter()
        output_file = exporter.export_all_data()
        print(f"   : {output_file}")
    except Exception as e:
        print(f"  : {e}")

def main():
    """ """
    setup_logging()

    print("  Scraptraffic v1.0.0")
    print("=" * 50)

    #  
    warnings = []
    if not BlogConfig.NEWS_API_KEYS.get('newsapi'):
        warnings.append("  API    ")
    if not BlogConfig.OPENAI_API_KEY:
        warnings.append("OPENAI_API_KEY   -   ")
    if not BlogConfig.MAIN_SITE_API_KEY:
        warnings.append("MAIN_SITE_API_KEY   -      ")

    if warnings:
        print(":")
        for warning in warnings:
            print(f"  - {warning}")
    else:
        print(" ")

    #  
    parser = argparse.ArgumentParser(description='  Scraptraffic')

    #  
    parser.add_argument('--init', action='store_true',
                       help='     ')

    parser.add_argument('--3-stage-system', action='store_true', dest='three_stage_system',
                       help='  :      ')

    parser.add_argument('--filtered-search', action='store_true',
                       help='  :      ')

    parser.add_argument('--stage-1', action='store_true',
                       help='  1:      ')

    parser.add_argument('--stage-2', action='store_true',
                       help='  2:     ')

    parser.add_argument('--stage-3', action='store_true',
                       help='   3:    (  )')

    parser.add_argument('--process', action='store_true',
                       help='  (, )')

    parser.add_argument('--scrape', action='store_true',
                       help='   ')

    parser.add_argument('--digest', action='store_true',
                       help=' ')

    parser.add_argument('--stats', action='store_true',
                       help='  ')

    parser.add_argument('--export-data', action='store_true',
                       help='    Excel')

    parser.add_argument('--export-ready', action='store_true',
                       help='    ')

    # 
    parser.add_argument('--language', default='ru',
                       help='  (ru, en, zh)')

    parser.add_argument('--limit', type=int, default=50,
                       help=' /')

    parser.add_argument('--frequency', default='daily',
                       help='  (daily, weekly, monthly)')

    #  
    parser.add_argument('--full-cycle', action='store_true',
                       help='   ()')

    args = parser.parse_args()

    #  
    success = False

    if args.init:
        success = init_database()
        if success:
            print("   !")

    elif args.three_stage_system:
        run_3_stage_system(args.language, args.limit)
        success = True

    elif args.filtered_search:
        collected = run_filtered_search(args.language, args.limit)
        print(f"   :  {collected} ")
        success = collected > 0

    elif args.stage_1:
        collected = stage_1_data_collection(args.language, args.limit)
        print(f"  1 :  {collected} ")
        success = collected > 0

    elif args.stage_2:
        segmented = stage_2_vector_segmentation(args.language, args.limit)
        print(f"  2 :  {segmented} ")
        success = segmented > 0

    elif args.stage_3:
        generated = stage_3_content_generation(args.limit, args.language)
        print(f"   3 :  {generated} ")
        success = generated > 0

    elif args.process:
        processed = process_articles(args.limit)
        success = processed > 0

    elif args.scrape:
        scraped = scrape_news(args.language, args.limit)
        success = scraped > 0

    elif args.digest:
        #  
        from processors.content_processor import ContentProcessor
        processor = ContentProcessor()
        digest = processor.generate_digest(args.frequency, datetime.now() - timedelta(days=1), datetime.now())
        if digest:
            print(f"  '{digest.title}' ")
            success = True
        else:
            print("    ")

    elif args.stats:
        show_statistics()
        success = True

    elif args.export_data:
        export_data()
        success = True

    elif args.export_ready:
        exporter = DataExporter()
        output_file = exporter.export_for_publication()
        print(f"   : {output_file}")
        success = True

    else:
        parser.print_help()

    # 
    if success:
        print("\n   !")
    else:
        print("\n    !")
        sys.exit(1)

if __name__ == "__main__":
    main()
