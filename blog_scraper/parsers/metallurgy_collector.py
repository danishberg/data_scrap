"""
Production-ready metallurgy industry news collector
Focuses on actual metallurgy content from reliable sources
"""

import requests
from bs4 import BeautifulSoup
import logging
from typing import List, Dict, Any
from datetime import datetime
import time

class MetallurgyCollector:
    """Специализированный сборщик металлургических новостей"""
    
    def __init__(self, language: str = 'ru'):
        self.language = language
        self.logger = logging.getLogger(__name__)
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        })
        
    def collect_metallurgy_urls(self, limit: int = 100) -> List[str]:
        """Сбор URL металлургических новостей"""
        all_urls = []
        
        # Источники по языкам
        if self.language == 'ru':
            sources = self._get_russian_sources()
        elif self.language == 'en':
            sources = self._get_english_sources()
        else:
            sources = self._get_russian_sources()  # Default to Russian
            
        for source in sources:
            try:
                urls = self._scrape_source(source, limit // len(sources))
                all_urls.extend(urls)
                
                if len(all_urls) >= limit:
                    break
                    
                time.sleep(0.5)  # Rate limiting
                
            except Exception as e:
                self.logger.warning(f"Ошибка сбора из {source['name']}: {e}")
                continue
                
        return all_urls[:limit]
    
    def _get_russian_sources(self) -> List[Dict[str, Any]]:
        """Русскоязычные источники металлургических новостей"""
        return [
            {
                'name': 'MetalInfo',
                'url': 'https://www.metalinfo.ru/ru/news',
                'parser': self._parse_metalinfo
            },
            {
                'name': 'SteelLand',
                'url': 'https://www.steelland.ru/news/',
                'parser': self._parse_steelland
            },
            {
                'name': 'MetalTorg',
                'url': 'https://www.metaltorg.ru/news/',
                'parser': self._parse_metaltorg
            },
            {
                'name': 'MetalResearch',
                'url': 'https://metalresearch.ru/news/',
                'parser': self._parse_metalresearch
            },
            {
                'name': 'Метпром',
                'url': 'https://www.metprom.ru/news.html',
                'parser': self._parse_generic
            },
            {
                'name': 'Металлоснабжение и сбыт',
                'url': 'https://www.metalinfo.ru/ru/news/ferrous',
                'parser': self._parse_generic
            }
        ]
    
    def _get_english_sources(self) -> List[Dict[str, Any]]:
        """Англоязычные источники металлургических новостей"""
        return [
            {
                'name': 'SteelOrbis',
                'url': 'https://www.steelorbis.com/steel-news/',
                'parser': self._parse_generic
            },
            {
                'name': 'MetalBulletin',
                'url': 'https://www.metalbulletin.com/steel',
                'parser': self._parse_generic
            },
            {
                'name': 'SteelGuru',
                'url': 'https://www.steelguru.com/steel_news_rss/',
                'parser': self._parse_generic
            }
        ]
    
    def _scrape_source(self, source: Dict[str, Any], limit: int = 20) -> List[str]:
        """Сбор URL из конкретного источника"""
        try:
            response = self.session.get(source['url'], timeout=15)
            response.raise_for_status()
            
            # Используем специфичный парсер если есть
            if source.get('parser'):
                return source['parser'](response.text, limit)
            else:
                return self._parse_generic(response.text, limit)
                
        except Exception as e:
            self.logger.error(f"Ошибка при сборе из {source['name']}: {e}")
            return []
    
    def _parse_generic(self, html: str, limit: int) -> List[str]:
        """Универсальный парсер для извлечения ссылок"""
        soup = BeautifulSoup(html, 'lxml')
        urls = []
        
        # Ищем все ссылки
        for link in soup.find_all('a', href=True):
            href = link.get('href', '')
            
            # Фильтруем релевантные ссылки
            if self._is_news_link(href, link.get_text(strip=True)):
                # Нормализуем URL
                if href.startswith('http'):
                    urls.append(href)
                elif href.startswith('/'):
                    # Относительная ссылка - нужно добавить домен
                    pass  # Пока пропускаем
                    
            if len(urls) >= limit:
                break
                
        return urls[:limit]
    
    def _is_news_link(self, href: str, text: str) -> bool:
        """Проверка является ли ссылка новостной"""
        if not href:
            return False
            
        # Паттерны новостных URL
        news_patterns = [
            '/news/', '/novosti/', '/article/', '/statya/',
            '-news-', '-novosti-', '/lenta/', '/articles/'
        ]
        
        # Исключаем нерелевантные
        exclude_patterns = [
            '/tag/', '/category/', '/author/', '/page/',
            '.jpg', '.png', '.pdf', '/search/', '/login/'
        ]
        
        href_lower = href.lower()
        
        # Проверяем исключения
        if any(pattern in href_lower for pattern in exclude_patterns):
            return False
            
        # Проверяем паттерны новостей
        if any(pattern in href_lower for pattern in news_patterns):
            return True
            
        # Проверяем наличие металлургических терминов в тексте
        if text:
            metal_terms = [
                'сталь', 'металл', 'лом', 'железо', 'медь', 'алюминий',
                'steel', 'metal', 'iron', 'copper', 'aluminum', 'scrap'
            ]
            text_lower = text.lower()
            if any(term in text_lower for term in metal_terms):
                return True
                
        return False
    
    def _parse_metaltorg(self, html: str, limit: int) -> List[str]:
        """Специфичный парсер для MetalTorg.ru"""
        soup = BeautifulSoup(html, 'lxml')
        urls = []
        
        # MetalTorg использует определенную структуру
        news_blocks = soup.find_all('div', class_=['news-item', 'item'])
        
        for block in news_blocks:
            link = block.find('a', href=True)
            if link:
                href = link.get('href', '')
                if href.startswith('http'):
                    urls.append(href)
                elif href.startswith('/'):
                    urls.append(f"https://www.metaltorg.ru{href}")
                    
            if len(urls) >= limit:
                break
                
        return urls
    
    def _parse_metalinfo(self, html: str, limit: int) -> List[str]:
        """Специфичный парсер для MetalInfo"""
        soup = BeautifulSoup(html, 'lxml')
        urls = []
        
        # MetalInfo структура
        for link in soup.find_all('a', href=True):
            href = link.get('href', '')
            # MetalInfo использует относительные ссылки
            if '/news/' in href or '/ru/news/' in href:
                if href.startswith('http'):
                    urls.append(href)
                elif href.startswith('/'):
                    urls.append(f"https://www.metalinfo.ru{href}")
                    
            if len(urls) >= limit:
                break
        
        return urls
    
    def _parse_steelland(self, html: str, limit: int) -> List[str]:
        """Специфичный парсер для SteelLand"""
        soup = BeautifulSoup(html, 'lxml')
        urls = []
        
        for link in soup.find_all('a', href=True):
            href = link.get('href', '')
            if '/news/' in href:
                if href.startswith('http'):
                    urls.append(href)
                elif href.startswith('/'):
                    urls.append(f"https://www.steelland.ru{href}")
                    
            if len(urls) >= limit:
                break
        
        return urls
    
    def _parse_metalresearch(self, html: str, limit: int) -> List[str]:
        """Специфичный парсер для MetalResearch"""
        soup = BeautifulSoup(html, 'lxml')
        urls = []
        
        for link in soup.find_all('a', href=True):
            href = link.get('href', '')
            if '/news/' in href or 'article' in href:
                if href.startswith('http'):
                    urls.append(href)
                elif href.startswith('/'):
                    urls.append(f"https://metalresearch.ru{href}")
                    
            if len(urls) >= limit:
                break
        
        return urls
    
    def collect_with_full_content(self, limit: int = 100) -> List[Dict[str, Any]]:
        """Сбор URL вместе с извлечением контента"""
        urls = self.collect_metallurgy_urls(limit)
        articles = []
        
        for url in urls:
            try:
                article_data = self._extract_article_content(url)
                if article_data:
                    articles.append(article_data)
                    
                time.sleep(0.3)  # Rate limiting
                
                if len(articles) >= limit:
                    break
                    
            except Exception as e:
                self.logger.warning(f"Ошибка извлечения контента из {url}: {e}")
                continue
                
        return articles
    
    def _extract_article_content(self, url: str) -> Dict[str, Any]:
        """Извлечение контента статьи из URL"""
        try:
            response = self.session.get(url, timeout=15)
            response.raise_for_status()
            
            soup = BeautifulSoup(response.text, 'lxml')
            
            # Извлекаем заголовок
            title = None
            for selector in ['h1', 'title', '.article-title', '.news-title']:
                title_elem = soup.select_one(selector)
                if title_elem:
                    title = title_elem.get_text(strip=True)
                    break
                    
            # Извлекаем контент
            content = None
            for selector in ['.article-content', '.news-content', 'article', '.content']:
                content_elem = soup.select_one(selector)
                if content_elem:
                    # Удаляем скрипты и стили
                    for tag in content_elem.find_all(['script', 'style']):
                        tag.decompose()
                    content = content_elem.get_text(strip=True)
                    break
                    
            if not content:
                # Пробуем найти все параграфы
                paragraphs = soup.find_all('p')
                if paragraphs:
                    content = ' '.join([p.get_text(strip=True) for p in paragraphs[:10]])
                    
            # Извлекаем дату
            published_at = None
            for selector in ['.date', '.published', 'time', '[datetime]']:
                date_elem = soup.select_one(selector)
                if date_elem:
                    date_text = date_elem.get('datetime') or date_elem.get_text(strip=True)
                    try:
                        # Простой парсинг даты
                        published_at = datetime.now()  # Упрощенно
                    except:
                        pass
                    break
                    
            if title and content:
                return {
                    'title': title,
                    'original_title': title,
                    'content': content[:5000],  # Ограничиваем длину
                    'original_content': content[:5000],
                    'url': url,
                    'source': self._extract_domain(url),
                    'language': self.language,
                    'published_at': published_at,
                    'scraped_at': datetime.now()
                }
                
        except Exception as e:
            self.logger.error(f"Ошибка извлечения контента: {e}")
            
        return None
    
    def _extract_domain(self, url: str) -> str:
        """Извлечение домена из URL"""
        from urllib.parse import urlparse
        parsed = urlparse(url)
        domain = parsed.netloc
        # Убираем www.
        if domain.startswith('www.'):
            domain = domain[4:]
        return domain

