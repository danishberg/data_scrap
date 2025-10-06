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

    # RSS фиды - ТОЛЬКО металлургические источники + поисковые агрегаторы
    RSS_FEEDS = {
        'ru': [
            # Металлургические RSS (приоритет)
            'https://www.metalinfo.ru/ru/rss/news',
            'https://www.metaltorg.ru/rss/news.xml',
            'https://www.steelland.ru/rss/',
            'https://www.severstal.com/rus/media/news/rss/',
            'https://mmk.ru/rss/',
            'https://www.nlmk.com/ru/media-center/news/rss/',
            # Экономические с фокусом на металлы
            'https://www.interfax.ru/rss/commodity.xml',
            'https://www.vedomosti.ru/rss/rubric/business',
            'https://www.kommersant.ru/rss/section-business.xml',
            # Поисковые агрегаторы Google News (рус)
            'https://news.google.com/rss/search?q=%D1%81%D1%82%D0%B0%D0%BB%D1%8C+OR+%D0%BC%D0%B5%D1%82%D0%B0%D0%BB%D0%BB%D1%83%D1%80%D0%B3%D0%B8%D1%8F+OR+%D0%BC%D0%B5%D1%82%D0%B0%D0%BB%D0%BB%D0%BE%D0%BB%D0%BE%D0%BC&hl=ru&gl=RU&ceid=RU:ru',
            'https://news.google.com/rss/search?q=%D0%BC%D0%B5%D0%B4%D1%8C+OR+%D0%B0%D0%BB%D1%8E%D0%BC%D0%B8%D0%BD%D0%B8%D0%B9+OR+%D1%86%D0%B8%D0%BD%D0%BA+OR+%D0%BD%D0%B8%D0%BA%D0%B5%D0%BB%D1%8C&hl=ru&gl=RU&ceid=RU:ru'
        ],
        'en': [
            # Metallurgy specific
            'https://www.mining.com/feed/',
            'https://www.steelguru.com/rss.xml',
            'https://www.metalbulletin.com/rss.xml',
            'https://www.fastmarkets.com/rss.xml',
            'https://www.kitco.com/rss.xml',
            # Google News (EN) queries
            'https://news.google.com/rss/search?q=steel+OR+scrap+metal+OR+metallurgy&hl=en-US&gl=US&ceid=US:en',
            'https://news.google.com/rss/search?q=copper+OR+aluminum+OR+nickel+OR+zinc+prices&hl=en-US&gl=US&ceid=US:en'
        ],
        'zh': [
            'http://www.xinhuanet.com/rss/worldrss.xml',
            'http://www.chinadaily.com.cn/rss/world_rss.xml'
        ]
    }

    # Отраслевые источники
    INDUSTRY_SOURCES = {
        'ru': [
            {'name': 'Коммерсантъ Бизнес', 'url': 'https://www.kommersant.ru/themes/business', 'category': 'business'},
            {'name': 'Ведомости Бизнес', 'url': 'https://www.vedomosti.ru/business', 'category': 'business'},
            {'name': 'РИА Экономика', 'url': 'https://ria.ru/economy/', 'category': 'economy'},
            {'name': 'ТАСС Экономика', 'url': 'https://tass.ru/ekonomika', 'category': 'economy'},
            {'name': 'РБК Бизнес', 'url': 'https://www.rbc.ru/business/', 'category': 'business'}
        ],
        'en': [
            {'name': 'Reuters Business', 'url': 'https://www.reuters.com/business/', 'category': 'business'},
            {'name': 'Bloomberg Markets', 'url': 'https://www.bloomberg.com/markets', 'category': 'markets'}
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
