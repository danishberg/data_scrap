"""
Веб-скрапер для отраслевых новостных источников
Специализирован на металлургической и промышленной тематике
"""

import requests
import logging
from datetime import datetime, timedelta
from typing import List, Dict, Optional, Any
from urllib.parse import urljoin, urlparse

from bs4 import BeautifulSoup
from .base_parser import BaseNewsParser
from core.config import BlogConfig

class IndustryWebScraper(BaseNewsParser):
    """Скрапер новостей с отраслевых веб-сайтов"""

    def __init__(self, language: str = 'ru'):
        super().__init__(language)
        self.industry_sources = self._get_industry_sources()
        self.site_selectors = self._get_site_selectors()
        self.logger = logging.getLogger(__name__)

    def _get_industry_sources(self) -> Dict[str, Dict[str, Any]]:
        """Получение списка отраслевых источников"""
        sources = {}

        if self.language == 'ru':
            sources = {
                # General business/economy sources
                'kommersant': {
                    'base_url': 'https://www.kommersant.ru',
                    'news_url': 'https://www.kommersant.ru/finance',
                    'category': 'business'
                },
                'vedomosti': {
                    'base_url': 'https://www.vedomosti.ru',
                    'news_url': 'https://www.vedomosti.ru/business',
                    'category': 'business'
                },
                'ria_economy': {
                    'base_url': 'https://ria.ru',
                    'news_url': 'https://ria.ru/economy/',
                    'category': 'economy'
                },
                'tass_economy': {
                    'base_url': 'https://tass.ru',
                    'news_url': 'https://tass.ru/ekonomika',
                    'category': 'economy'
                },
                'rbc_business': {
                    'base_url': 'https://www.rbc.ru',
                    'news_url': 'https://www.rbc.ru/business/',
                    'category': 'business'
                },
                'interfax_business': {
                    'base_url': 'https://www.interfax.ru',
                    'news_url': 'https://www.interfax.ru/business/',
                    'category': 'business'
                },
                'rg_economy': {
                    'base_url': 'https://rg.ru',
                    'news_url': 'https://rg.ru/business/',
                    'category': 'economy'
                },
                # Metallurgy-specific sources
                'metallurg_news': {
                    'base_url': 'https://www.metallurg.ru',
                    'news_url': 'https://www.metallurg.ru/news/',
                    'category': 'metallurgy'
                },
                'metalinfo': {
                    'base_url': 'https://www.metalinfo.ru',
                    'news_url': 'https://www.metalinfo.ru/ru/news',
                    'category': 'metallurgy'
                },
                'metallplace': {
                    'base_url': 'https://www.metallplace.ru',
                    'news_url': 'https://www.metallplace.ru/news/',
                    'category': 'metallurgy'
                },
                'rusmet': {
                    'base_url': 'https://rusmet.ru',
                    'news_url': 'https://rusmet.ru/novosti/',
                    'category': 'metallurgy'
                },
                'metallportal': {
                    'base_url': 'https://metallportal.ru',
                    'news_url': 'https://metallportal.ru/news/',
                    'category': 'metallurgy'
                },
                'steel_express': {
                    'base_url': 'https://www.steel-express.ru',
                    'news_url': 'https://www.steel-express.ru/news/',
                    'category': 'metallurgy'
                },
                'severstal_news': {
                    'base_url': 'https://www.severstal.com',
                    'news_url': 'https://www.severstal.com/rus/investoram/news/',
                    'category': 'metallurgy'
                },
                'nornikel_news': {
                    'base_url': 'https://www.nornickel.ru',
                    'news_url': 'https://www.nornickel.ru/about/news/',
                    'category': 'metallurgy'
                },
                'evraz_news': {
                    'base_url': 'https://www.evraz.com',
                    'news_url': 'https://www.evraz.com/ru/media/news/',
                    'category': 'metallurgy'
                },
                'mmk_news': {
                    'base_url': 'https://www.mmk.ru',
                    'news_url': 'https://www.mmk.ru/ru/media-center/news/',
                    'category': 'metallurgy'
                }
            }
        elif self.language == 'en':
            sources = {
                # General business sources
                'reuters_business': {
                    'base_url': 'https://www.reuters.com',
                    'news_url': 'https://www.reuters.com/business/',
                    'category': 'business'
                },
                'bloomberg_markets': {
                    'base_url': 'https://www.bloomberg.com',
                    'news_url': 'https://www.bloomberg.com/markets',
                    'category': 'markets'
                },
                # Metallurgy-specific English sources
                'mining_com': {
                    'base_url': 'https://www.mining.com',
                    'news_url': 'https://www.mining.com/news/',
                    'category': 'mining'
                },
                'steel_guru': {
                    'base_url': 'https://www.steelguru.com',
                    'news_url': 'https://www.steelguru.com/steel-news',
                    'category': 'steel'
                },
                'metal_bulletin': {
                    'base_url': 'https://www.metalbulletin.com',
                    'news_url': 'https://www.metalbulletin.com/news',
                    'category': 'metals'
                },
                'fastmarkets': {
                    'base_url': 'https://www.fastmarkets.com',
                    'news_url': 'https://www.fastmarkets.com/news',
                    'category': 'commodities'
                },
                'kitco': {
                    'base_url': 'https://www.kitco.com',
                    'news_url': 'https://www.kitco.com/news',
                    'category': 'precious_metals'
                },
                'platts': {
                    'base_url': 'https://www.spglobal.com',
                    'news_url': 'https://www.spglobal.com/platts/en/market-insights/latest-news/metals',
                    'category': 'metals'
                },
                'aluminum_org': {
                    'base_url': 'https://www.aluminum.org',
                    'news_url': 'https://www.aluminum.org/news',
                    'category': 'aluminum'
                },
                'copper_org': {
                    'base_url': 'https://www.copper.org',
                    'news_url': 'https://www.copper.org/publications/newsletters/',
                    'category': 'copper'
                }
            }

        return sources

    def _get_site_selectors(self) -> Dict[str, Dict[str, str]]:
        """Получение селекторов для разных сайтов"""
        selectors = {
            'default': {
                'article_links': 'a[href*="/news/"], a[href*="/article/"], a[href*="/story/"]',
                'title': 'h1, .article-title, .post-title',
                'content': 'article, .article-content, .post-content, .entry-content',
                'date': 'time, .published, .date',
                'author': '.author, .byline'
            }
        }

        # Специфические селекторы для российских сайтов
        selectors.update({
            'kommersant': {
                'article_links': '.rubric_lenta a, .main_news a',
                'title': 'h1.article_name, .article_title',
                'content': '.article_text, .doc__text',
                'date': '.doc__time, .article_date',
                'author': '.author, .doc__author'
            },
            'vedomosti': {
                'article_links': '.news-item__title a, .b-news__item a',
                'title': 'h1.article-title, .article__title',
                'content': '.article__text, .js-article-content',
                'date': '.article__date, .article-date',
                'author': '.article__author, .author'
            },
            'ria_economy': {
                'article_links': '.list-item__content a, .cell-list__item a',
                'title': 'h1.article__title, .article__second-title',
                'content': '.article__text, .article__body',
                'date': '.article__info-date, .article__date',
                'author': '.article__authors, .author'
            },
            'tass_economy': {
                'article_links': '.news-list__item a, .tass_pkg a',
                'title': 'h1.news-header__title, .news-header__title',
                'content': '.text-content, .news-content',
                'date': '.news-date, .date',
                'author': '.author, .news-author'
            },
            'rbc_business': {
                'article_links': '.main__feed a, .news-feed__item a',
                'title': 'h1.article__header__title, .article__title',
                'content': '.article__text, .article__content',
                'date': '.article__header__date, .article__date',
                'author': '.article__authors, .author'
            }
        })

        return selectors

    def get_news_urls(self, limit: int = 50) -> List[str]:
        """
        Получение URL новостей с отраслевых сайтов

        Args:
            limit: Максимальное количество URL

        Returns:
            Список URL новостей
        """
        urls = []
        # Be conservative per source to avoid long runs
        total_sources = max(1, len(self.industry_sources))
        urls_per_source = max(1, min(3, (limit + total_sources - 1) // total_sources))

        self.logger.info(f"Начинаем скрапинг {len(self.industry_sources)} отраслевых источников")

        for source_key, source_info in self.industry_sources.items():
            try:
                source_urls = self._scrape_source(source_info, urls_per_source)
                urls.extend(source_urls)
                self.logger.info(f"Скрапинг {source_key}: {len(source_urls)} URL")

                if len(urls) >= limit:
                    break

            except Exception as e:
                self.logger.error(f"Ошибка при скрапинге {source_key}: {e}")
                continue

        unique_urls = list(set(urls))[:limit]
        self.logger.info(f"Всего собрано {len(unique_urls)} уникальных URL новостей")

        return unique_urls

    def _scrape_source(self, source_info: Dict[str, Any], limit: int) -> List[str]:
        """Скрапинг отдельного источника"""
        news_url = source_info['news_url']
        base_url = source_info['base_url']

        # Skip notoriously slow source to avoid long stalls
        if 'metallurg.ru' in (base_url or ''):
            return []

        # Use shorter timeout for flaky industry sources
        response = self._make_request(news_url, timeout=10)
        if not response:
            return []

        soup = BeautifulSoup(response.content, 'html.parser')
        source_key = self._identify_source(news_url)

        selectors = self.site_selectors.get(source_key, self.site_selectors['default'])

        # Ищем ссылки на статьи
        article_links = soup.select(selectors['article_links'])
        urls = []

        for link in article_links[:limit]:
            href = link.get('href')
            if href:
                # Преобразуем относительные ссылки в абсолютные
                full_url = urljoin(base_url, href)
                urls.append(full_url)

        return urls

    def _identify_source(self, url: str) -> str:
        """Определение источника по URL"""
        domain = self._extract_domain(url)

        for source_key, source_info in self.industry_sources.items():
            if domain in source_info['base_url']:
                return source_key

        return 'default'

    def parse_article(self, url: str) -> Optional[Dict[str, Any]]:
        """
        Парсинг статьи с отраслевого сайта

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
            source_key = self._identify_source(url)
            selectors = self.site_selectors.get(source_key, self.site_selectors['default'])

            # Извлекаем данные
            title = self._extract_title(soup, selectors)
            content = self._extract_content(soup, selectors)
            published_at = self._extract_date(soup, selectors)
            author = self._extract_author(soup, selectors)

            if not title or len(content) < BlogConfig.MIN_ARTICLE_LENGTH:
                return None

            article_data = {
                'title': title,
                'original_title': title,
                'content': content,
                'original_content': content,
                'url': url,
                'source': self._extract_domain(url),
                'language': self.language,
                'author': author,
                'published_at': published_at,
                'summary': self._generate_summary(content),
                'scraped_at': datetime.utcnow(),
                'is_processed': False,
                'is_published': False
            }

            return article_data

        except Exception as e:
            self.logger.error(f"Ошибка при парсинге статьи {url}: {e}")
            return None

    def get_news(self, limit: int = 50, skip_filters: bool = False) -> List[Dict[str, Any]]:
        """
        Получение новостей с опциональным пропуском фильтров

        Args:
            limit: Максимальное количество статей
            skip_filters: Пропустить фильтры релевантности

        Returns:
            Список структурированных статей
        """
        urls = self.get_news_urls(limit * 2)  # Берем больше URL на случай фильтрации

        # Фильтруем релевантные URL (если фильтры не отключены)
        if skip_filters:
            relevant_urls = urls[:limit]
        else:
            relevant_urls = []
            for url in urls:
                try:
                    # Проверяем релевантность URL
                    source_info = self._get_source_info_for_url(url)
                    if self._is_relevant_url(url, source_info):
                        relevant_urls.append(url)

                    if len(relevant_urls) >= limit:
                        break
                except Exception as e:
                    self.logger.warning(f"Ошибка проверки релевантности URL {url}: {e}")
                    continue

        # Парсим статьи
        articles = []
        for url in relevant_urls[:limit]:
            try:
                article_data = self.parse_article(url)
                if article_data:
                    articles.append(article_data)
            except Exception as e:
                self.logger.warning(f"Ошибка парсинга статьи {url}: {e}")
                continue

        return articles

    def _get_source_info_for_url(self, url: str) -> Dict[str, Any]:
        """Получение информации об источнике для URL"""
        domain = self._extract_domain(url)
        for source_key, source_info in self.industry_sources.items():
            if domain in source_info.get('base_url', ''):
                return source_info
        return {}

    def _is_relevant_url(self, url: str, source_info: Dict[str, Any]) -> bool:
        """Проверка релевантности URL"""
        # Все статьи с бизнес/экономических сайтов считаем релевантными
        if source_info.get('category') in ['business', 'economy', 'markets']:
            return True

        # Дополнительная проверка по ключевым словам в URL
        url_lower = url.lower()
        relevant_keywords = ['metal', 'steel', 'scrap', 'mining', 'industry', 'business']

        return any(keyword in url_lower for keyword in relevant_keywords)

    def _extract_title(self, soup: BeautifulSoup, selectors: Dict[str, str]) -> str:
        """Извлечение заголовка"""
        title_elem = soup.select_one(selectors.get('title', 'h1'))
        return title_elem.get_text(strip=True) if title_elem else ""

    def _extract_content(self, soup: BeautifulSoup, selectors: Dict[str, str]) -> str:
        """Извлечение контента"""
        content_elem = soup.select_one(selectors.get('content', 'article'))
        if content_elem:
            # Удаляем ненужные элементы
            for unwanted in content_elem.select('script, style, .ads, .social-share'):
                unwanted.decompose()
            return content_elem.get_text(separator=' ', strip=True)
        return ""

    def _extract_date(self, soup: BeautifulSoup, selectors: Dict[str, str]) -> Optional[datetime]:
        """Извлечение даты публикации"""
        date_elem = soup.select_one(selectors.get('date', 'time'))
        if date_elem:
            datetime_attr = date_elem.get('datetime')
            if datetime_attr:
                try:
                    return datetime.fromisoformat(datetime_attr.replace('Z', '+00:00'))
                except:
                    pass
            # Пробуем извлечь из текста
            date_text = date_elem.get_text(strip=True)
            # Здесь можно добавить более сложный парсинг дат
        return None

    def _extract_author(self, soup: BeautifulSoup, selectors: Dict[str, str]) -> str:
        """Извлечение автора"""
        author_elem = soup.select_one(selectors.get('author', '.author'))
        return author_elem.get_text(strip=True) if author_elem else ""

    def _generate_summary(self, content: str, max_length: int = 200) -> str:
        """Генерация краткого описания"""
        if len(content) <= max_length:
            return content

        # Берем первые max_length символов и обрезаем до последнего полного слова
        summary = content[:max_length]
        last_space = summary.rfind(' ')
        if last_space > 0:
            summary = summary[:last_space]

        return summary + "..."

    def _extract_domain(self, url: str) -> str:
        """Извлечение домена из URL"""
        try:
            parsed = urlparse(url)
            return parsed.netloc
        except:
            return url

    def get_sources_info(self) -> Dict[str, Any]:
        """Информация об источниках"""
        return {
            'language': self.language,
            'total_sources': len(self.industry_sources),
            'sources': list(self.industry_sources.keys()),
            'scraping_method': 'Direct web scraping',
            'categories_supported': True
        }
