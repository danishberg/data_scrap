"""
Парсер RSS-лент новостных источников
"""

import feedparser
import requests
from datetime import datetime, timedelta
from typing import List, Dict, Optional, Any
from urllib.parse import urljoin
import logging
from bs4 import BeautifulSoup

from .base_parser import BaseNewsParser
from core.config import BlogConfig

class RSSNewsParser(BaseNewsParser):
    """Парсер новостей из RSS-лент"""

    def __init__(self, language: str = 'en'):
        super().__init__(language)
        self.rss_feeds = BlogConfig.RSS_FEEDS.get(self.language, []) or BlogConfig.RSS_FEEDS.get('en', [])
        self.logger = logging.getLogger(__name__)

    # Paywalled domains to exclude
    PAYWALLED_DOMAINS = {
        'wsj.com', 'ft.com', 'bloomberg.com', 'economist.com',
        'nytimes.com', 'washingtonpost.com', 'theguardian.com'
    }
    
    # Category page indicators to filter out
    CATEGORY_INDICATORS = {
        '/category/', '/section/', '/rubric/', '/tag/',
        '/archive/', '/search/', '/?s=', '&s=',
        '/news/', '/articles/'  # These are ambiguous, need careful handling
    }

    def _resolve_google_news_url(self, google_news_url: str) -> Optional[str]:
        """
        Resolve Google News RSS redirect URL to actual article URL with multiple strategies
        """
        try:
            # Strategy 1: Extract from URL parameters
            if 'url=' in google_news_url:
                from urllib.parse import parse_qs, urlparse, unquote
                parsed = urlparse(google_news_url)
                query_params = parse_qs(parsed.query)
                if 'url' in query_params:
                    actual_url = unquote(query_params['url'][0])
                    if actual_url and 'news.google.com' not in actual_url:
                        if self._is_valid_article_url(actual_url):
                            return actual_url
            
            # Strategy 2: Use HEAD request with proper headers
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
                'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
                'Accept-Language': 'en-US,en;q=0.5',
                'Accept-Encoding': 'gzip, deflate',
                'Connection': 'keep-alive',
                'Upgrade-Insecure-Requests': '1',
            }
            
            try:
                response = self.session.head(google_news_url, headers=headers, 
                                           allow_redirects=True, timeout=10)
                final_url = str(response.url)
                
                # Check if we got a real article URL
                if 'news.google.com' not in final_url and self._is_valid_article_url(final_url):
                    return final_url
                    
                # Check redirect history
                if hasattr(response, 'history') and response.history:
                    for resp in response.history:
                        redirect_url = str(resp.url)
                        if 'news.google.com' not in redirect_url and self._is_valid_article_url(redirect_url):
                            return redirect_url
                            
            except (requests.exceptions.Timeout, requests.exceptions.ConnectionError):
                # If HEAD fails, try GET with shorter timeout
                try:
                    response = self.session.get(google_news_url, headers=headers, 
                                              timeout=8, stream=True)
                    final_url = str(response.url)
                    response.close()
                    
                    if 'news.google.com' not in final_url and self._is_valid_article_url(final_url):
                        return final_url
                        
                except Exception:
                    pass
                    
            # Strategy 3: Pattern matching in Google News URL structure
            if '/articles/' in google_news_url:
                # Try to extract from the path pattern
                try:
                    from urllib.parse import urlparse
                    parsed = urlparse(google_news_url)
                    if parsed.path.startswith('/rss/articles/'):
                        # This might be a direct article link in some formats
                        potential_url = google_news_url.replace('/rss/articles/', '/article/')
                        if self._is_valid_article_url(potential_url):
                            return potential_url
                except:
                    pass
            
            return None
            
        except Exception as e:
            self.logger.warning(f"Failed to resolve Google News URL {google_news_url}: {e}")
            return None

    def _is_valid_article_url(self, url: str) -> bool:
        """
        More permissive URL validation to collect more articles
        """
        from urllib.parse import urlparse

        # Check if URL is from paywalled domain
        parsed_url = urlparse(url)
        domain = parsed_url.netloc.lower()

        for paywalled_domain in self.PAYWALLED_DOMAINS:
            if paywalled_domain in domain:
                return False

        # Basic checks - reject obvious non-article URLs
        path = parsed_url.path.lower()
        query = parsed_url.query.lower()

        # Reject search, category, and archive pages
        bad_patterns = ['/category/', '/section/', '/rubric/', '/tag/', '/archive/', '/search/', '/?s=', '&s=', '/products/', '/price-data/', '/author/', '/page/', '/contact/', '/about/', '/privacy/', '/terms/']

        if any(pattern in path for pattern in bad_patterns):
            return False

        if any(pattern in query for pattern in ['?s=', '&s=', 'search=', 'page=', 'cat=']):
            return False

        # Accept URLs that look like articles
        good_patterns = ['/article/', '/story/', '/news/', '/202', '/20', '/новости/', '/статья/', '/news.html', '/article.html', '/commodities/', '/metals/', '/mining/', '/steel/', '/aluminum/', '/copper/', '/iron/', '/scrap/']

        has_good_pattern = any(pattern in path for pattern in good_patterns)

        # Accept URLs with numeric components (likely article IDs)
        path_parts = [p for p in path.split('/') if p]
        has_numbers = any(any(c.isdigit() for c in part) for part in path_parts)

        # Accept most URLs that aren't obviously bad
        return has_good_pattern or has_numbers or len(path_parts) > 1

    def get_news_urls(self, limit: int = 50) -> List[str]:
        """
        Получение URL новостей из RSS-лент с fallback к прямому скрепингу
        """
        urls = []
        urls_per_feed = max(10, limit // len(self.rss_feeds)) if self.rss_feeds else 10

        # First try RSS feeds
        if self.rss_feeds:
            self.logger.info(f"Парсинг {len(self.rss_feeds)} RSS-лент для языка {self.language}")

            for feed_url in self.rss_feeds:
                try:
                    feed = feedparser.parse(feed_url)
                    if feed.get('entries'):
                        entries = feed.get('entries', [])[:urls_per_feed]
                        for entry in entries:
                            if entry.get('link'):
                                url = entry['link']

                                # Resolve Google News redirect URLs
                                if 'news.google.com' in url:
                                    resolved_url = self._resolve_google_news_url(url)
                                    if resolved_url:
                                        url = resolved_url

                                # Validate URL before adding (less strict)
                                if self._is_valid_article_url(url):
                                    urls.append(url)

                        self.logger.info(f"Получено {len(entries)} URL из {feed_url}")

                        if len(urls) >= limit:
                            break

                except Exception as e:
                    self.logger.debug(f"RSS failed {feed_url}: {e}")
                    continue

        # Fallback: Direct scraping if RSS failed
        if len(urls) < limit:
            self.logger.info(f"RSS collected {len(urls)} URLs, falling back to direct scraping")
            fallback_urls = self._get_urls_from_direct_scraping(limit - len(urls))
            urls.extend(fallback_urls)

        unique_urls = list(set(urls))[:limit]
        self.logger.info(f"Total collected {len(unique_urls)} уникальных URL")

        return unique_urls

    def _get_urls_from_direct_scraping(self, limit: int = 50) -> List[str]:
        """
        Direct web scraping fallback for reliable sources
        """
        urls = []

        # Reliable sources that work
        direct_sources = {
            'en': [
                ('https://www.reuters.com/markets/commodities/', 'commodities'),
                ('https://www.spglobal.com/platts/en/market-insights/latest-news/metals/', 'metals'),
                ('https://www.aluminum.org/news/', 'aluminum'),
                ('https://www.fastmarkets.com/metals-and-mining/', 'metals'),
                ('https://www.kitco.com/news', 'precious_metals'),
                ('https://www.bloomberg.com/markets/commodities', 'commodities'),
                ('https://www.cnbc.com/commodities/', 'commodities'),
                ('https://seekingalpha.com/market-news/commodities', 'commodities'),
                ('https://www.marketwatch.com/commodities', 'commodities'),
                ('https://www.wsj.com/news/markets/oil-gold-commodities', 'commodities'),
                ('https://www.ft.com/commodities', 'commodities'),
                ('https://www.economist.com/finance-and-economics', 'economics'),
                ('https://www.forbes.com/money/', 'business'),
                ('https://www.businessinsider.com/category/commodities', 'commodities'),
                ('https://finance.yahoo.com/commodities', 'commodities'),
            ],
            'ru': [
                ('https://www.metalinfo.ru/ru/news', 'metallurgy'),
                ('https://ria.ru/economy/', 'economy'),
                ('https://www.interfax.ru/business/commodities/', 'commodities'),
            ],
            'zh': [
                ('http://www.xinhuanet.com/english/world.htm', 'world'),
                ('http://www.chinadaily.com.cn/business', 'business'),
            ]
        }

        sources = direct_sources.get(self.language, direct_sources['en'])

        for source_url, category in sources:
            try:
                response = self._make_request(source_url)
                if response:
                    soup = BeautifulSoup(response.content, 'html.parser')

                    # Find all article links
                    article_links = soup.find_all('a', href=True)

                    for link in article_links[:200]:  # Check first 200 links per page
                        href = link.get('href')
                        if href:
                            # Convert relative to absolute URLs
                            full_url = urljoin(source_url, href)

                            # More permissive validation for direct scraping
                            if self._is_article_like_url(full_url):
                                urls.append(full_url)

                                if len(urls) >= limit:
                                    break

                    if len(urls) >= limit:
                        break

            except Exception as e:
                self.logger.debug(f"Direct scraping failed {source_url}: {e}")
                continue

        return list(set(urls))[:limit]

    def _is_article_like_url(self, url: str) -> bool:
        """
        More permissive URL validation for direct scraping
        """
        from urllib.parse import urlparse

        if not url or len(url) < 10:
            return False

        parsed_url = urlparse(url)
        path = parsed_url.path.lower()
        query = parsed_url.query.lower()

        # Exclude obvious non-article URLs
        exclude_patterns = [
            '/category/', '/section/', '/tag/', '/archive/', '/search/',
            '/author/', '/page/', '/feed/', '/rss/', '/comments/',
            '/login/', '/register/', '/contact/', '/about/', '/privacy/',
            '/terms/', '/sitemap/', '/robots.txt', '/favicon.ico',
            '.jpg', '.png', '.gif', '.css', '.js', '.pdf', '.zip'
        ]

        for pattern in exclude_patterns:
            if pattern in path or pattern in query:
                return False

        # Must have some indication it's an article
        article_indicators = [
            '/article/', '/news/', '/story/', '/post/', '/202', '/20',
            '/новости/', '/статья/', '/news.html', '/article.html',
            '-news-', '-article-', '-story-'
        ]

        has_indicator = any(indicator in path for indicator in article_indicators)

        # Or check for date-like patterns in URL
        has_date = any(char.isdigit() for char in path.split('/')[-1] if path.split('/')[-1])

        return has_indicator or has_date

    def get_news_entries(self, limit: int = 50) -> List[Dict[str, Any]]:
        """Возвращает элементы новостей (url, title, summary) из RSS-лент"""
        entries_out: List[Dict[str, Any]] = []
        if not self.rss_feeds:
            return entries_out

        urls_per_feed = max(5, limit // len(self.rss_feeds))

        self.logger.info(f"Парсинг {len(self.rss_feeds)} RSS-лент (entries) для языка {self.language}")

        for feed_url in self.rss_feeds:
            try:
                self.logger.info(f"Парсинг RSS (entries): {feed_url}")
                
                # First try to get URLs using the more robust get_news_urls method
                urls = self.get_news_urls(limit=urls_per_feed)
                
                # If we got URLs, create basic entries
                for url in urls:
                    entries_out.append({'url': url, 'title': '', 'summary': ''})

                if len(entries_out) >= limit:
                    break

            except Exception as e:
                self.logger.error(f"Ошибка при парсинге RSS {feed_url}: {e}")
                continue

        # Дедуп по URL, сохраняя первый вариант
        seen = set()
        unique_entries: List[Dict[str, Any]] = []
        for item in entries_out:
            u = item.get('url')
            if u and u not in seen:
                seen.add(u)
                unique_entries.append(item)

        return unique_entries[:limit]

    def parse_article(self, url: str) -> Optional[Dict[str, Any]]:
        """
        Парсинг отдельной статьи из RSS

        Args:
            url: URL статьи

        Returns:
            Структурированные данные статьи
        """
        try:
            response = self._make_request(url)
            if not response:
                return None

            soup = BeautifulSoup(response.content, 'html.parser')

            # Извлекаем метаданные
            metadata = self._extract_article_metadata(soup, url)
            content = self._extract_text_content(soup)

            if not metadata.get('title') or len(content) < BlogConfig.MIN_ARTICLE_LENGTH:
                return None

            article_data = {
                'title': metadata['title'],
                'original_title': metadata['title'],
                'content': content,
                'original_content': content,
                'url': url,
                'source': self._extract_domain(url),
                'language': self.language,
                'author': metadata.get('author', ''),
                'published_at': metadata.get('published_at'),
                'summary': metadata.get('description', ''),
                'scraped_at': datetime.utcnow(),
                'is_processed': False,
                'is_published': False
            }

            return article_data

        except Exception as e:
            self.logger.error(f"Ошибка при парсинге статьи {url}: {e}")
            return None

    def _extract_domain(self, url: str) -> str:
        """Извлечение домена из URL"""
        try:
            from urllib.parse import urlparse
            parsed = urlparse(url)
            return parsed.netloc
        except:
            return url
