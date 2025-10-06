"""
Модели базы данных для системы блога Scraptraffic
"""

from sqlalchemy import create_engine, Column, Integer, String, Text, DateTime, Boolean, ForeignKey, Float
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, relationship
from datetime import datetime
from core.config import BlogConfig

Base = declarative_base()

class Article(Base):
    """Модель статьи"""
    __tablename__ = 'articles'

    id = Column(Integer, primary_key=True)
    title = Column(String(500), nullable=False)
    original_title = Column(String(500))
    content = Column(Text)
    original_content = Column(Text)
    url = Column(String(1000), unique=True, nullable=False)
    source = Column(String(100))
    language = Column(String(10), default='ru')
    author = Column(String(200))
    published_at = Column(DateTime)
    scraped_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

    # Обработка контента
    translated_title = Column(String(500))
    translated_content = Column(Text)
    rewritten_content = Column(Text)
    summary = Column(Text)

    # Категории (храним как строку для простоты)
    categories_list = Column(String(500))

    # Статусы
    is_processed = Column(Boolean, default=False)
    is_published = Column(Boolean, default=False)
    is_featured = Column(Boolean, default=False)

    # Метрики
    rating = Column(Float, default=0.0)

    # Связи
    processing_logs = relationship("ProcessingLog", back_populates="article")

class Category(Base):
    """Модель категории"""
    __tablename__ = 'categories'

    id = Column(Integer, primary_key=True)
    name = Column(String(100), unique=True, nullable=False)
    slug = Column(String(100), unique=True, nullable=False)
    description = Column(Text)

class Digest(Base):
    """Модель дайджеста новостей"""
    __tablename__ = 'digests'

    id = Column(Integer, primary_key=True)
    title = Column(String(500), nullable=False)
    content = Column(Text, nullable=False)
    digest_type = Column(String(50), default='daily')  # daily, weekly, monthly
    theme = Column(String(200))
    article_count = Column(Integer, default=0)
    categories = Column(String(500))  # Список категорий через запятую
    created_at = Column(DateTime, default=datetime.utcnow)
    published_at = Column(DateTime)

class ProcessingLog(Base):
    """Лог обработки статей"""
    __tablename__ = 'processing_logs'

    id = Column(Integer, primary_key=True)
    article_id = Column(Integer, ForeignKey('articles.id'))
    operation = Column(String(50), nullable=False)  # translate, rewrite, categorize, etc.
    status = Column(String(20), nullable=False)  # success, error, pending
    message = Column(Text)
    processed_at = Column(DateTime, default=datetime.utcnow)

    # Связь
    article = relationship("Article", back_populates="processing_logs")

class Cache(Base):
    """Кэш веб-контента"""
    __tablename__ = 'cache'

    id = Column(Integer, primary_key=True)
    url = Column(String(1000), unique=True, nullable=False)
    content = Column(Text)
    content_type = Column(String(50))  # html, json, xml
    cached_at = Column(DateTime, default=datetime.utcnow)
    expires_at = Column(DateTime)

class DatabaseManager:
    """Менеджер базы данных"""

    def __init__(self):
        self.engine = create_engine(BlogConfig.DATABASE_URL, echo=BlogConfig.DEBUG)
        self.SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=self.engine)

    def create_tables(self):
        """Создание всех таблиц"""
        Base.metadata.create_all(bind=self.engine)

    def get_session(self):
        """Получение сессии базы данных"""
        return self.SessionLocal()

    def drop_tables(self):
        """Удаление всех таблиц (для тестирования)"""
        Base.metadata.drop_all(bind=self.engine)

# Глобальный экземпляр менеджера БД
db_manager = DatabaseManager()
