"""
Парсер статей с помощью trafilatura
Устойчивый извлекатель текста для новостных сайтов
"""

import logging
from datetime import datetime
from typing import Optional, Dict, Any

from .base_parser import BaseNewsParser

class TrafalituraParser(BaseNewsParser):
    """Парсер с использованием trafilatura (fallback к newspaper3k)"""

    def __init__(self, language: str = 'ru'):
        super().__init__(language)
        self.logger = logging.getLogger(__name__)

        # Блок-лист доменов (paywall/часто блокируют ботов)
        self.blacklist_domains = {
            'wsj.com', 'ft.com', 'bloomberg.com', 'metalbulletin.com',
            'kitco.com', 'fastmarkets.com'
        }

    def _blocked(self, url: str) -> bool:
        try:
            from urllib.parse import urlparse
            domain = urlparse(url).netloc.lower()
            return any(domain == b or domain.endswith('.' + b) for b in self.blacklist_domains)
        except Exception:
            return False

    def parse_article(self, url: str) -> Optional[Dict[str, Any]]:
        """Извлечение статьи через trafilatura"""
        try:
            if self._blocked(url):
                return None

            import trafilatura

            downloaded = trafilatura.fetch_url(url)
            if not downloaded:
                return None

            extracted = trafilatura.extract(
                downloaded,
                include_comments=False,
                include_tables=False,
                include_links=False,
                favor_recall=True,
                target_language=self.language if self.language in {'ru', 'en'} else None
            )

            if not extracted or len(extracted) < 200:
                return None

            # Простое извлечение заголовка (trafilatura может возвращать метаданные, но упрощаем)
            title = ''
            try:
                metadata = trafilatura.extract_metadata(downloaded)
                if metadata and metadata.title:
                    title = metadata.title
            except Exception:
                pass

            if not title:
                # Фолбэк: используем кусок текста как заголовок
                title = extracted.split('\n', 1)[0][:120]

            return {
                'title': title,
                'original_title': title,
                'content': extracted,
                'original_content': extracted,
                'url': url,
                'source': self._extract_domain(url),
                'author': '',
                'published_at': None,
                'summary': extracted[:160],
                'scraped_at': datetime.utcnow(),
                'is_processed': False,
                'is_published': False
            }

        except Exception as e:
            self.logger.warning(f"Ошибка trafilatura для {url}: {e}")
            return None

    def get_news_urls(self, limit: int = 50):
        """trafilatura не ищет URL, используем как парсер: возвращаем пусто"""
        return []


