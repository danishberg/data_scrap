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
        self.rss_feeds = self._get_rss_feeds()
        # Merge in config-driven feeds and dedupe
        try:
            from core.config import BlogConfig as _Cfg
            extra = _Cfg.RSS_FEEDS.get(self.language, []) or []
            if not extra and self.language != 'en':
                extra = _Cfg.RSS_FEEDS.get('en', []) or []
            merged, seen = [], set()
            for u in list(self.rss_feeds or []) + list(extra or []):
                if u and u not in seen:
                    seen.add(u)
                    merged.append(u)
            self.rss_feeds = merged
        except Exception:
            # As-is if config not available
            self.rss_feeds = self.rss_feeds or []
        self.logger = logging.getLogger(__name__)

    def _get_rss_feeds(self) -> List[str]:
        """Получение списка RSS-лент для данного языка"""
        rss_feeds = []

        if self.language == 'ru':
            rss_feeds = [
                'https://ria.ru/export/rss2/archive/index.xml',
                'https://tass.ru/rss/v2.xml',
                'https://www.interfax.ru/rss.asp',
                'https://www.kommersant.ru/rss/regions.xml',
                'https://www.vedomosti.ru/rss/news',
                'https://rg.ru/xml/index.xml'
            ]
        elif self.language == 'en':
            rss_feeds = [
                'https://feeds.reuters.com/Reuters/worldNews',
                'https://feeds.bloomberg.com/bloomberg/markets/news.rss',
                'https://feeds.a.dj.com/rss/RSSWorldNews.xml',
                'https://www.ft.com/rss/home/uk',
                'http://feeds.bbci.co.uk/news/rss.xml'
            ]
        elif self.language == 'zh':
            rss_feeds = [
                'http://www.xinhuanet.com/rss/worldrss.xml',
                'http://www.chinadaily.com.cn/rss/world_rss.xml',
                'https://www.globaltimes.cn/rss/world.xml'
            ]

        return rss_feeds

    def get_news_urls(self, limit: int = 50) -> List[str]:
        """
        Получение URL новостей из RSS-лент

        Args:
            limit: Максимальное количество URL

        Returns:
            Список URL новостей
        """
        urls = []
        urls_per_feed = max(5, limit // len(self.rss_feeds))

        self.logger.info(f"Парсинг {len(self.rss_feeds)} RSS-лент для языка {self.language}")

        for feed_url in self.rss_feeds:
            try:
                self.logger.info(f"Парсинг RSS: {feed_url}")
                feed = feedparser.parse(feed_url)

                if feed.get('status') != 200:
                    self.logger.warning(f"Не удалось загрузить RSS: {feed_url}")
                    continue

                entries = feed.get('entries', [])[:urls_per_feed]

                for entry in entries:
                    if entry.get('link'):
                        urls.append(entry['link'])

                self.logger.info(f"Получено {len(entries)} URL из {feed_url}")

                if len(urls) >= limit:
                    break

            except Exception as e:
                self.logger.error(f"Ошибка при парсинге RSS {feed_url}: {e}")
                continue

        unique_urls = list(set(urls))[:limit]
        self.logger.info(f"RSS парсер нашел {len(unique_urls)} уникальных URL")

        return unique_urls

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
                feed = feedparser.parse(feed_url)
                if feed.get('status') != 200:
                    self.logger.warning(f"Не удалось загрузить RSS: {feed_url}")
                    continue

                entries = feed.get('entries', [])[:urls_per_feed]
                for entry in entries:
                    link = entry.get('link')
                    title = entry.get('title') or ''
                    summary = entry.get('summary') or entry.get('description') or ''
                    if link:
                        entries_out.append({'url': link, 'title': title, 'summary': summary})

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
