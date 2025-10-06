"""
Базовый парсер для всех источников новостей
"""

import requests
try:
    import requests_cache
except Exception:
    requests_cache = None
import logging
from datetime import datetime, timedelta
from typing import List, Dict, Optional, Any
from urllib.parse import urljoin, urlparse
from abc import ABC, abstractmethod

from bs4 import BeautifulSoup
from core.config import BlogConfig
from random import choice
import time

class BaseNewsParser(ABC):
    """Базовый класс для парсеров новостей"""

    def __init__(self, language: str = 'en'):
        self.language = language
        self.logger = logging.getLogger(__name__)
        # Включаем кешированный сеанс, если доступен
        if requests_cache is not None:
            try:
                self.session = requests_cache.CachedSession(
                    cache_name='scraper_cache',
                    backend='sqlite',
                    expire_after=timedelta(hours=BlogConfig.CACHE_EXPIRATION_HOURS)
                )
            except Exception:
                self.session = requests.Session()
        else:
            self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': BlogConfig.USER_AGENT,
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
            'Accept-Language': f'{language}-{language.upper()};q=0.9,en;q=0.8',
            'Accept-Encoding': 'gzip, deflate, br',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1',
        })
        try:
            from requests.adapters import HTTPAdapter
            self.session.mount('https://', HTTPAdapter(pool_connections=20, pool_maxsize=50))
            self.session.mount('http://', HTTPAdapter(pool_connections=20, pool_maxsize=50))
        except Exception:
            pass

        # Пул User-Agent для ротации
        self._user_agents = [
            BlogConfig.USER_AGENT,
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125 Safari/537.36',
            'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124 Safari/537.36',
            'Mozilla/5.0 (Macintosh; Intel Mac OS X 12_4) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/15.5 Safari/605.1.15'
        ]

    def _make_request(self, url: str, timeout: int = None) -> Optional[requests.Response]:
        """Выполнение HTTP запроса с повторными попытками"""
        if timeout is None:
            timeout = BlogConfig.REQUEST_TIMEOUT

        # DB cache fast-path
        try:
            from core.cache_utils import get_cached
            cached = get_cached(url)
        except Exception:
            cached = None

        if cached:
            from requests.models import Response as _Resp
            content, content_type = cached
            resp = _Resp()
            resp.status_code = 200
            resp._content = (content or "").encode('utf-8', errors='ignore')
            resp.headers['Content-Type'] = content_type or 'text/html; charset=utf-8'
            resp.url = url
            return resp

        for attempt in range(BlogConfig.MAX_RETRIES):
            try:
                # Ротация UA при повторных попытках
                if attempt > 0:
                    ua = choice(self._user_agents)
                    self.session.headers['User-Agent'] = ua
                    # Вежливая задержка перед повтором
                    time.sleep(0.7 * attempt)

                response = self.session.get(url, timeout=timeout)
                response.raise_for_status()
                # Best-effort store in cache
                try:
                    from core.cache_utils import set_cached
                    set_cached(
                        url,
                        response.text,
                        response.headers.get('Content-Type', ''),
                        ttl_hours=BlogConfig.CACHE_EXPIRATION_HOURS,
                    )
                except Exception:
                    pass
                return response
            except Exception as e:
                self.logger.warning(f"Попытка {attempt + 1} failed for {url}: {e}")
                if attempt < BlogConfig.MAX_RETRIES - 1:
                    continue
                return None
        return None

    def _resolve_final_url(self, url: str, timeout: int = None) -> str:
        """Следует по редиректам и возвращает финальный URL"""
        try:
            if timeout is None:
                timeout = BlogConfig.REQUEST_TIMEOUT
            resp = self.session.get(url, timeout=timeout, allow_redirects=True)
            return resp.url or url
        except Exception:
            return url

    def _extract_text_content(self, soup: BeautifulSoup) -> str:
        """Извлечение текстового контента из HTML"""
        # Удаляем скрипты и стили
        for script in soup(["script", "style", "nav", "header", "footer", "aside"]):
            script.decompose()

        # Ищем основные контейнеры контента
        content_selectors = [
            'article', '.article-content', '.post-content', '.entry-content',
            '.content', '#content', '.main-content', '.article-body',
            '[data-testid="article-body"]', '.story-body', '.article-text'
        ]

        for selector in content_selectors:
            content_elem = soup.select_one(selector)
            if content_elem:
                # Извлекаем текст и очищаем
                text = content_elem.get_text(separator=' ', strip=True)
                if len(text) > BlogConfig.MIN_ARTICLE_LENGTH:
                    return text

        # Fallback: извлекаем текст из всего body
        body = soup.find('body')
        if body:
            text = body.get_text(separator=' ', strip=True)
            return text

        return ""

    def _extract_article_metadata(self, soup: BeautifulSoup, url: str) -> Dict[str, Any]:
        """Извлечение метаданных статьи"""
        metadata = {
            'title': '',
            'author': '',
            'published_at': None,
            'description': ''
        }

        # Title
        title_selectors = ['h1', '.article-title', '.post-title', '.entry-title', 'title']
        for selector in title_selectors:
            title_elem = soup.select_one(selector)
            if title_elem:
                metadata['title'] = title_elem.get_text(strip=True)
                break

        # Author
        author_selectors = ['.author', '.byline', '[rel="author"]', '.article-author']
        for selector in author_selectors:
            author_elem = soup.select_one(selector)
            if author_elem:
                metadata['author'] = author_elem.get_text(strip=True)
                break

        # Published date
        date_selectors = ['time', '.published', '.date', '.article-date', '[datetime]']
        for selector in date_selectors:
            date_elem = soup.select_one(selector)
            if date_elem:
                datetime_str = date_elem.get('datetime') or date_elem.get_text(strip=True)
                try:
                    # Пытаемся распарсить дату
                    metadata['published_at'] = datetime.fromisoformat(datetime_str.replace('Z', '+00:00'))
                except:
                    pass
                break

        # Description
        desc_selectors = ['meta[name="description"]', '.article-summary', '.excerpt']
        for selector in desc_selectors:
            desc_elem = soup.select_one(selector)
            if desc_elem:
                metadata['description'] = desc_elem.get('content') or desc_elem.get_text(strip=True)
                break

        return metadata

    def _is_relevant_article(self, title: str, content: str) -> bool:
        """Проверка релевантности статьи для металлургической отрасли"""
        if not title and not content:
            return False

        text_to_check = f"{title} {content}".lower()
        keywords = BlogConfig.KEYWORDS.get(self.language, [])

        # Проверяем наличие ключевых слов
        matches = sum(1 for keyword in keywords if keyword.lower() in text_to_check)

        # Минимум 2 совпадения для релевантности
        return matches >= 1

    @abstractmethod
    def get_news_urls(self, limit: int = 50) -> List[str]:
        """Получение URL новостей"""
        pass

    def search_relevant_articles(self, limit: int = 50, skip_relevance_check: bool = False) -> List[Dict[str, Any]]:
        """Поиск и парсинг релевантных статей"""
        urls = self.get_news_urls(limit * 2)  # Берем больше URL на случай фильтрации

        # Фильтруем релевантные URL (если не отключено)
        relevant_urls = []
        for url in urls:
            if skip_relevance_check:
                # Пропускаем проверку релевантности - берем все URL
                relevant_urls.append(url)
                if len(relevant_urls) >= limit:
                    break
                continue

            try:
                response = self._make_request(url)
                if not response:
                    continue

                soup = BeautifulSoup(response.content, 'html.parser')
                title = self._extract_article_metadata(soup, url).get('title', '')
                content_preview = self._extract_text_content(soup)[:1000]  # Превью контента

                if self._is_relevant_article(title, content_preview):
                    relevant_urls.append(url)

                if len(relevant_urls) >= limit:
                    break

            except Exception as e:
                self.logger.warning(f"Failed to check relevance for {url}: {e}")
                continue

        # Парсим релевантные статьи
        return self.parse_articles_batch(relevant_urls)

    def parse_articles_batch(self, urls: List[str]) -> List[Dict[str, Any]]:
        """Парсинг списка статей"""
        articles = []

        for url in urls:
            try:
                article_data = self.parse_article(url)
                if article_data:
                    articles.append(article_data)
            except Exception as e:
                self.logger.error(f"Failed to parse article {url}: {e}")
                continue

        return articles

    @abstractmethod
    def parse_article(self, url: str) -> Optional[Dict[str, Any]]:
        """Парсинг отдельной статьи"""
        pass
