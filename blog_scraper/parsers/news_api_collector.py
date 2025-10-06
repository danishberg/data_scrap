"""
Сборщик новостей через NewsAPI
Использует API ключ для получения структурированных новостей
"""

import requests
import logging
from datetime import datetime, timedelta
from typing import List, Dict, Optional, Any
import time
import json
from urllib.parse import urlencode

from core.config import BlogConfig
from core.cache_utils import get_cached, set_cached

class NewsAPICollector:
    """
    Сборщик новостей через NewsAPI
    Предоставляет структурированные новости с метаданными
    """

    def __init__(self, language: str = 'ru', api_key: str = None):
        self.language = language
        self.logger = logging.getLogger(__name__)

        # Используем API ключ из конфига или переданный
        self.api_key = api_key or BlogConfig.NEWS_API_KEYS.get('newsapi')

        if not self.api_key:
            self.logger.warning("NewsAPI ключ не найден. Используйте --news-api-key или настройте в .env")

        # NewsAPI endpoints
        self.base_url = "https://newsapi.org/v2"
        self.endpoints = {
            'everything': f"{self.base_url}/everything",
            'top_headlines': f"{self.base_url}/top_headlines"
        }

        # HTTP session
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        })
        try:
            from requests.adapters import HTTPAdapter
            self.session.mount('https://', HTTPAdapter(pool_connections=20, pool_maxsize=50))
            self.session.mount('http://', HTTPAdapter(pool_connections=20, pool_maxsize=50))
        except Exception:
            pass

        # Оптимизированные запросы для металлургической тематики (меньше запросов для избежания rate limiting)
        self.metal_queries = [
            # Основные металлургические запросы (более targeted)
            'металлургическая промышленность OR металлургия OR steel industry',
            'металлы OR черные металлы OR цветные металлы OR ferrous OR non-ferrous',
            'сталь OR steel production OR сталелитейная промышленность',
            'металлолом OR scrap metal OR metal recycling',
            'алюминий OR aluminum OR медь OR copper OR mining OR руда',
            'металлообработка OR metal processing OR metallurgical plant',
            'цены на металлы OR metal prices OR metal market'
        ]

    def _make_request(self, url: str, params: dict = None, timeout: int = 30) -> Optional[requests.Response]:
        """Выполнение HTTP запроса с повторными попытками и обработкой rate limit"""
        import time
        for attempt in range(3):
            try:
                response = self.session.get(url, params=params, timeout=timeout)
                
                # Если 429 (Too Many Requests), возвращаем None сразу, не повторяем
                if response.status_code == 429:
                    self.logger.warning(f"NewsAPI rate limit exceeded (429). Skipping further requests.")
                    return None
                    
                response.raise_for_status()
                return response
            except requests.exceptions.HTTPError as e:
                if hasattr(e.response, 'status_code') and e.response.status_code == 429:
                    self.logger.warning(f"NewsAPI rate limit exceeded. Skipping.")
                    return None
                self.logger.warning(f"Попытка {attempt + 1} failed for {url}: {e}")
                if attempt < 2:
                    time.sleep(1)
                    continue
                return None
            except Exception as e:
                self.logger.warning(f"Попытка {attempt + 1} failed for {url}: {e}")
                if attempt < 2:
                    time.sleep(0.5)
                    continue
                return None
        return None

    def get_news_urls(self, limit: int = 50) -> List[str]:
        """
        Получение URL новостей через NewsAPI для металлургической отрасли

        Args:
            limit: Максимальное количество URL

        Returns:
            Список URL новостей
        """
        if not self.api_key:
            self.logger.error("NewsAPI ключ не настроен")
            return []

        articles = self.get_metal_industry_news(min(limit, 100))
        urls = [article.get('url') for article in articles if article.get('url')]

        self.logger.info(f"NewsAPI собрал {len(urls)} URL металлургической отрасли")
        return urls

    def _search_news(self, query: str, limit: int) -> List[str]:
        """Поиск новостей по запросу"""
        if not self.api_key:
            return []

        params = {
            'q': query,
            'language': self.language,
            'sortBy': 'publishedAt',
            'pageSize': min(limit, 100),  # NewsAPI ограничивает до 100
            'apiKey': self.api_key
        }

        response = self._make_request(self.endpoints['everything'], params=params)
        if not response:
            return []

        try:
            data = response.json()

            if data.get('status') != 'ok':
                self.logger.error(f"NewsAPI вернул ошибку: {data}")
                return []

            articles = data.get('articles', [])
            urls = [article.get('url') for article in articles if article.get('url')]

            return urls

        except Exception as e:
            self.logger.error(f"Ошибка при парсинге ответа NewsAPI: {e}")
            return []

    def get_structured_news(self, query: str, limit: int = 10) -> List[Dict[str, Any]]:
        """
        Получение структурированных новостей с полными метаданными

        Args:
            query: Поисковый запрос
            limit: Количество статей

        Returns:
            Список структурированных статей
        """
        if not self.api_key:
            return []

        try:
            params = {
                'q': query,
                'language': self.language,
                'sortBy': 'publishedAt',
                'pageSize': min(limit, 100),
                'apiKey': self.api_key
            }

            # Cache key without apiKey
            cache_key_parts = [
                f"q={query}",
                f"lang={self.language}",
                f"sort=publishedAt",
                f"size={min(limit, 100)}",
            ]
            cache_key = f"newsapi:everything?{'&'.join(cache_key_parts)}"

            data = None
            try:
                cached = get_cached(cache_key)
                if cached and cached[0]:
                    import json as _json
                    data = _json.loads(cached[0])
            except Exception:
                data = None

            if data is None:
                response = self._make_request(self.endpoints['everything'], params=params)
                if not response:
                    return []
                data = response.json()
                try:
                    import json as _json
                    set_cached(cache_key, _json.dumps(data), 'application/json', ttl_hours=BlogConfig.CACHE_EXPIRATION_HOURS)
                except Exception:
                    pass

            if data.get('status') != 'ok':
                self.logger.error(f"NewsAPI вернул ошибку: {data}")
                return []

            articles = data.get('articles', [])
            structured_articles = []

            for article in articles:
                try:
                    structured_article = {
                        'title': article.get('title', ''),
                        'original_title': article.get('title', ''),
                        'content': article.get('content') or article.get('description', ''),
                        'original_content': article.get('content') or article.get('description', ''),
                        'url': article.get('url', ''),
                        'source': article.get('source', {}).get('name', ''),
                        'language': self.language,
                        'author': article.get('author', ''),
                        'published_at': self._parse_date(article.get('publishedAt')),
                        'summary': article.get('description', ''),
                        'scraped_at': datetime.utcnow(),
                        'is_processed': False,
                        'is_published': False
                    }

                    if structured_article['url']:  # Только если есть URL
                        structured_articles.append(structured_article)

                except Exception as e:
                    self.logger.warning(f"Ошибка обработки статьи: {e}")
                    continue

            return structured_articles[:limit]

        except Exception as e:
            self.logger.error(f"Ошибка при запросе к NewsAPI: {e}")
            return []

    def _parse_date(self, date_str: str) -> Optional[datetime]:
        """Парсинг даты из строки"""
        if not date_str:
            return None

        try:
            # NewsAPI возвращает даты в формате ISO
            return datetime.fromisoformat(date_str.replace('Z', '+00:00'))
        except:
            return None

    def get_metal_industry_news(self, limit: int = 100) -> List[Dict[str, Any]]:
        """
        Получение новостей металлургической отрасли

        Args:
            limit: Максимальное количество новостей

        Returns:
            Список структурированных новостей
        """
        if not self.api_key:
            self.logger.warning("NewsAPI ключ не найден")
            return []

        all_articles = []
        # Распределяем запросы равномерно, но не менее 5 статей на запрос
        articles_per_query = max(5, limit // len(self.metal_queries))
        if articles_per_query > 50:  # NewsAPI limit per request
            articles_per_query = 50

        self.logger.info(f"Сбор новостей металлургической отрасли (limit: {limit}, queries: {len(self.metal_queries)}, per_query: {articles_per_query})")

        for query in self.metal_queries:
            try:
                articles = self.get_structured_news(query, articles_per_query)
                all_articles.extend(articles)

                # Прерываем если набрали достаточно
                if len(all_articles) >= limit:
                    break

                # Задержка между запросами для избежания rate limiting
                import time
                time.sleep(1.0)  # Increase delay to avoid rate limiting

            except Exception as e:
                self.logger.warning(f"Ошибка при запросе '{query}': {e}")
                continue

        # Удаляем дубликаты по URL
        unique_articles = []
        seen_urls = set()
        for article in all_articles:
            if article.get('url') and article['url'] not in seen_urls:
                unique_articles.append(article)
                seen_urls.add(article['url'])

        result = unique_articles[:limit]
        self.logger.info(f"NewsAPI собрал {len(result)} уникальных статей")

        return result

    def get_general_news(self, keywords: List[str] = None, limit: int = 100) -> List[Dict[str, Any]]:
        """
        Получение общих новостей без строгой фильтрации металлургии

        Args:
            keywords: Дополнительные ключевые слова для поиска
            limit: Максимальное количество новостей

        Returns:
            Список структурированных новостей
        """
        if not self.api_key:
            self.logger.warning("NewsAPI ключ не найден")
            return []

        # Общие запросы для сбора разнообразных новостей
        base_queries = [
            'business OR economy OR industry OR бизнес OR экономика OR промышленность',
            'news OR новости OR события OR developments OR события',
            'market OR рынок OR торговля OR commerce OR торговля',
            'company OR компания OR предприятие OR corporation',
            'technology OR технологии OR инновации OR innovation'
        ]

        # Добавляем пользовательские ключевые слова если они заданы
        if keywords:
            custom_query = ' OR '.join(keywords)
            base_queries.insert(0, custom_query)

        all_articles = []
        articles_per_query = max(20, limit // len(base_queries))

        self.logger.info(f"Сбор общих новостей (limit: {limit}, keywords: {keywords or 'none'})")

        for query in base_queries:
            try:
                query_articles = self.get_structured_news(query, articles_per_query)
                all_articles.extend(query_articles)

                if len(all_articles) >= limit:
                    break

            except Exception as e:
                self.logger.warning(f"Ошибка при запросе '{query}': {e}")
                continue

        # Удаляем дубликаты
        seen_urls = set()
        unique_articles = []

        for article in all_articles:
            if article['url'] not in seen_urls:
                seen_urls.add(article['url'])
                unique_articles.append(article)

        result = unique_articles[:limit]
        self.logger.info(f"NewsAPI собрал {len(result)} уникальных общих статей")

        return result

    def get_api_limits(self) -> Dict[str, Any]:
        """
        Получение информации об ограничениях API

        Returns:
            Информация об ограничениях
        """
        return {
            'service': 'NewsAPI',
            'rate_limit': '500 запросов/день (бесплатно)',
            'articles_per_request': 100,
            'supported_languages': ['ar', 'de', 'en', 'es', 'fr', 'he', 'it', 'nl', 'no', 'pt', 'ru', 'se', 'ud', 'zh'],
            'has_api_key': bool(self.api_key)
        }
