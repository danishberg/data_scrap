"""
Агрегатор бесплатных новостей
Комбинирует RSS и Newspaper3k для сбора новостей без API
"""

import logging
from typing import List, Dict, Optional, Any

from .base_parser import BaseNewsParser
from .rss_parser import RSSNewsParser
from .newspaper_parser import NewspaperParser
from .trafilatura_parser import TrafalituraParser

class FreeNewsAggregator(BaseNewsParser):
    """Агрегатор бесплатных новостей"""

    def __init__(self, language: str = 'ru'):
        super().__init__(language)
        self.logger = logging.getLogger(__name__)

        self.rss_parser = RSSNewsParser(language)
        self.newspaper_parser = NewspaperParser(language)
        self.trafilatura_parser = TrafalituraParser(language)

    def get_news_urls(self, limit: int = 50) -> List[str]:
        """
        Получение URL новостей через RSS

        Args:
            limit: Максимальное количество URL

        Returns:
            Список URL новостей
        """
        # Используем RSS парсер для получения URL
        return self.rss_parser.get_news_urls(limit)

    def parse_articles_batch(self, urls: List[str]) -> List[Dict[str, Any]]:
        """
        Парсинг статей с помощью Newspaper3k

        Args:
            urls: Список URL для парсинга

        Returns:
            Список структурированных статей
        """
        articles = []

        for url in urls:
            try:
                article_data = None
                # Пытаемся newspaper3k
                if self.newspaper_parser._is_url_supported(url):
                    article_data = self.newspaper_parser.parse_article(url)

                # Фолбэк на trafilatura
                if not article_data:
                    article_data = self.trafilatura_parser.parse_article(url)

                if article_data:
                    # Добавляем язык
                    article_data['language'] = self.language
                    articles.append(article_data)

            except Exception as e:
                self.logger.warning(f"Ошибка парсинга статьи {url}: {e}")
                continue

        self.logger.info(f"Агрегатор обработал {len(articles)} статей")
        return articles

    def parse_article(self, url: str) -> Optional[Dict[str, Any]]:
        """
        Парсинг отдельной статьи

        Args:
            url: URL статьи

        Returns:
            Структурированные данные статьи
        """
        try:
            # Проверяем поддержку Newspaper3k
            if not self.newspaper_parser._is_url_supported(url):
                return None

            # Парсим статью
            article_data = self.newspaper_parser.parse_article(url)

            if article_data:
                # Добавляем язык
                article_data['language'] = self.language

            return article_data

        except Exception as e:
            self.logger.warning(f"Ошибка парсинга статьи {url}: {e}")
            return None