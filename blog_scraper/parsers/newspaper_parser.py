"""
Парсер статей с помощью newspaper3k
Автоматическое извлечение контента из новостных сайтов
"""

import logging
from datetime import datetime
from typing import Optional, Dict, Any, List
import newspaper

from .base_parser import BaseNewsParser
from core.config import BlogConfig

class NewspaperParser(BaseNewsParser):
    """Парсер статей с помощью newspaper3k"""

    def __init__(self, language: str = 'ru'):
        super().__init__(language)
        self.logger = logging.getLogger(__name__)

        # Настраиваем newspaper для языка
        self.config = newspaper.Config()
        self.config.language = language
        self.config.fetch_timeout = BlogConfig.REQUEST_TIMEOUT

        # Список поддерживаемых доменов для newspaper3k
        self.supported_domains = {
            # RU common
            'ria.ru', 'tass.ru', 'interfax.ru', 'kommersant.ru', 'vedomosti.ru', 'rg.ru',
            # Metallurgy-focused
            'metallurgprom.org', 'metaltorg.ru', 'metalinfo.ru', 'steelland.ru',
            # EN general biz (some may fail)
            'reuters.com', 'bbc.com', 'cnn.com',
        }
        # Явные paywall/blocked blacklist для пропуска
        self.blacklist_domains = {
            'wsj.com', 'ft.com', 'bloomberg.com', 'metalbulletin.com', 'kitco.com', 'fastmarkets.com'
        }

    def _is_url_supported(self, url: str) -> bool:
        """
        Проверка поддержки URL newspaper3k

        Args:
            url: URL для проверки

        Returns:
            True если поддерживается
        """
        try:
            from urllib.parse import urlparse
            domain = urlparse(url).netloc.lower()

            # Проверяем блок-лист
            for blocked in self.blacklist_domains:
                if domain == blocked or domain.endswith('.' + blocked):
                    return False

            # Проверяем точное совпадение домена
            if domain in self.supported_domains:
                return True

            # Проверяем поддомены
            for supported_domain in self.supported_domains:
                if domain.endswith('.' + supported_domain):
                    return True

            return False

        except Exception:
            return False

    def get_news_urls(self, limit: int = 50) -> List[str]:
        """
        Получение URL новостей через newspaper3k (не основной метод)

        Args:
            limit: Максимальное количество URL

        Returns:
            Список URL новостей
        """
        # Newspaper3k не предназначен для поиска URL, возвращаем пустой список
        self.logger.warning("NewspaperParser не предназначен для поиска URL новостей")
        return []

    def parse_article(self, url: str) -> Optional[Dict[str, Any]]:
        """
        Парсинг статьи с помощью newspaper3k

        Args:
            url: URL статьи

        Returns:
            Структурированные данные статьи
        """
        try:
            # Проверяем поддержку
            if not self._is_url_supported(url):
                self.logger.debug(f"URL не поддерживается newspaper3k: {url}")
                return None

            # Создаем статью newspaper
            article = newspaper.Article(url, config=self.config)

            # Загружаем статью
            article.download()
            article.parse()

            # Извлекаем данные
            title = article.title or ""
            content = article.text or ""
            authors = article.authors or []
            author = authors[0] if authors else ""
            publish_date = article.publish_date

            # Проверяем минимальные требования
            if not title or len(content) < 100:
                return None

            article_data = {
                'title': title,
                'original_title': title,
                'content': content,
                'original_content': content,
                'url': url,
                'source': self._extract_domain(url),
                'author': author,
                'published_at': publish_date,
                'summary': self._generate_summary(content),
                'scraped_at': datetime.utcnow(),
                'is_processed': False,
                'is_published': False
            }

            return article_data

        except Exception as e:
            self.logger.warning(f"Ошибка парсинга newspaper3k для {url}: {e}")
            return None

    def _generate_summary(self, content: str, max_length: int = 200) -> str:
        """Генерация краткого описания"""
        if len(content) <= max_length:
            return content

        # Берем первые предложения
        sentences = content.split('.')
        summary = ""

        for sentence in sentences:
            if len(summary + sentence) <= max_length - 10:
                summary += sentence + "."
            else:
                break

        return summary or content[:max_length - 3] + "..."

    def _extract_domain(self, url: str) -> str:
        """Извлечение домена из URL"""
        try:
            from urllib.parse import urlparse
            return urlparse(url).netloc
        except:
            return url
