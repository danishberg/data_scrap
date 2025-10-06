"""
Конфигурация системы блога Scraptraffic
"""

import os
from dotenv import load_dotenv

# Загружаем переменные окружения из разных возможных мест
load_dotenv()  # Загружает .env из текущей директории
load_dotenv('blog_scraper.env')  # Загружает blog_scraper.env если существует
load_dotenv('../.env')  # Загружает из родительской директории

class BlogConfig:
    """Конфигурация приложения"""

    # База данных
    DATABASE_URL = os.getenv('DATABASE_URL', 'sqlite:///blog_scraper.db')

    # API ключи
    NEWS_API_KEYS = {
        'newsapi': os.getenv('NEWS_API_KEY', ''),
    }

    OPENAI_API_KEY = os.getenv('OPENAI_API_KEY', '')
    MAIN_SITE_API_KEY = os.getenv('MAIN_SITE_API_KEY', '')

    # Настройки scraping
    REQUEST_TIMEOUT = 30
    MAX_RETRIES = 3
    USER_AGENT = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'

    # Настройки контента
    MIN_ARTICLE_LENGTH = 100
    MAX_ARTICLE_LENGTH = 10000

    # Ключевые слова для металлургической отрасли
    KEYWORDS = {
        'ru': [
            'металлург', 'сталь', 'алюмини', 'медь', 'железо', 'лом', 'утиль',
            'переработка', 'производство', 'завод', 'комбинат', 'цена', 'рынок',
            'экспорт', 'импорт', 'торговля', 'сырье', 'ресурс'
        ],
        'en': [
            'metallurg', 'steel', 'aluminum', 'copper', 'iron', 'scrap', 'recycling',
            'production', 'plant', 'mill', 'price', 'market', 'export', 'import',
            'trade', 'raw materials', 'resources'
        ],
        'zh': [
            '冶金', '钢', '铝', '铜', '铁', '废金属', '回收',
            '生产', '工厂', '价格', '市场', '出口', '进口',
            '贸易', '原材料', '资源'
        ]
    }

    # RSS feeds - FOCUSED on metallurgy and metals industry ONLY
    RSS_FEEDS = {
        'ru': [
            # Russian metallurgy-specific RSS (HIGH PRIORITY)
            'https://www.metalinfo.ru/ru/rss/news',
            'https://ria.ru/export/rss2/economy.xml',
            'https://www.interfax.ru/rss/commodity.xml',
            'https://tass.ru/rss/v2.xml',
            'https://www.vedomosti.ru/rss/news'
        ],
        'en': [
            # English metallurgy-specific RSS (HIGH PRIORITY)
            'https://www.mining.com/feed/',
            'https://feeds.reuters.com/reuters/commodities',
            'https://www.kitco.com/rss.xml',
            'https://www.spglobal.com/platts/en/market-insights/latest-news/metals/rss',
            # Alternative working feeds
            'https://www.reutersagency.com/feed/?taxonomy=best-sectors&post_type=best',
            'https://www.bloomberg.com/feeds/bbiz/site.xml'
        ],
        'zh': [
            'http://www.xinhuanet.com/rss/worldrss.xml',
            'http://www.chinadaily.com.cn/rss/business.xml'
        ]
    }

    # Industry sources - FOCUSED on reliable metallurgy websites
    INDUSTRY_SOURCES = {
        'ru': [
            {'name': 'MetalInfo News', 'url': 'https://www.metalinfo.ru/ru/news', 'category': 'metallurgy'},
            {'name': 'Interfax Commodities', 'url': 'https://www.interfax.ru/business/commodities/', 'category': 'commodities'},
        ],
        'en': [
            {'name': 'Mining.com News', 'url': 'https://www.mining.com/news/', 'category': 'mining'},
            {'name': 'Kitco News', 'url': 'https://www.kitco.com/news', 'category': 'precious_metals'},
            {'name': 'Reuters Commodities', 'url': 'https://www.reuters.com/markets/commodities/', 'category': 'commodities'},
        ],
        'zh': [
            {'name': 'Xinhua World News', 'url': 'http://www.xinhuanet.com/english/world.htm', 'category': 'world'},
        ]
    }

    # Категории статей
    ARTICLE_CATEGORIES = [
        'ferrous', 'non_ferrous', 'precious', 'scrap',
        'prices', 'production', 'trade', 'policy',
        'companies', 'technology', 'environment',
        'market_analysis', 'logistics', 'other'
    ]

    # Настройки кэширования
    CACHE_EXPIRATION_HOURS = 6

    # Настройки отладки
    DEBUG = os.getenv('DEBUG', 'false').lower() == 'true'

    # Настройки экспорта
    EXPORT_DIR = 'blog_database'

    @classmethod
    def validate_config(cls):
        """Валидация конфигурации"""
        errors = []

        if not cls.NEWS_API_KEYS.get('newsapi'):
            errors.append("NEWS_API_KEY не настроен")

        if not cls.OPENAI_API_KEY:
            errors.append("OPENAI_API_KEY не настроен")

        return errors
