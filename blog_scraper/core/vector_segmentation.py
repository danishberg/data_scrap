"""
Векторная сегментация контента для семантического поиска
Использует sentence-transformers для создания эмбеддингов
"""

import os
import logging
from typing import List, Dict, Any, Optional
from datetime import datetime

import chromadb
from sentence_transformers import SentenceTransformer
from database.models import db_manager, Article

class VectorSegmentation:
    """Векторная сегментация и семантический поиск"""

    def __init__(self):
        self.logger = logging.getLogger(__name__)

        # Настройки модели
        self.model_name = 'all-MiniLM-L6-v2'  # Быстрая и эффективная модель

        try:
            self.model = SentenceTransformer(self.model_name)
            self.logger.info(f"Модель {self.model_name} загружена успешно")
        except Exception as e:
            self.logger.error(f"Ошибка загрузки модели: {e}")
            self.model = None

        # Настройки ChromaDB
        self.persist_directory = "chroma_db"
        os.makedirs(self.persist_directory, exist_ok=True)

        try:
            self.client = chromadb.PersistentClient(path=self.persist_directory)
            self.collection = self.client.get_or_create_collection(
                name="blog_articles",
                metadata={"description": "Embeddings for blog articles"}
            )
            self.logger.info("ChromaDB инициализирован")
        except Exception as e:
            self.logger.error(f"Ошибка инициализации ChromaDB: {e}")
            self.client = None
            self.collection = None

    def segment_content(self, content: str) -> List[str]:
        """
        Simple content segmentation - split into paragraphs
        """
        if not content:
            return []

        # Split by double newlines (paragraphs) and filter out short segments
        segments = [s.strip() for s in content.split('\n\n') if s.strip() and len(s.strip()) > 50]
        return segments[:10]  # Limit to 10 segments

    def add_articles_to_vector_db(self, articles: List[Article]) -> int:
        """
        Добавление статей в векторную базу данных

        Args:
            articles: Список статей для индексации

        Returns:
            Количество добавленных статей
        """
        if not self.model or not self.collection:
            self.logger.error("Модель или ChromaDB не инициализированы")
            return 0

        added_count = 0

        try:
            # Подготавливаем данные
            texts = []
            metadatas = []
            ids = []

            for article in articles:
                try:
                    # Создаем текст для эмбеддинга
                    title = article.translated_title or article.title
                    content = article.translated_content or article.content
                    summary = article.summary or ""

                    # Комбинируем текст
                    combined_text = f"{title}. {summary}. {content[:1000]}"  # Ограничиваем длину
                    texts.append(combined_text)

                    # Метаданные
                    metadata = {
                        'id': str(article.id),
                        'title': title,
                        'url': article.url,
                        'language': article.language,
                        'categories': article.categories_list or '',
                        'published_at': article.published_at.isoformat() if article.published_at else '',
                        'is_processed': str(bool(article.is_processed)),
                        'is_published': str(bool(article.is_published)),
                        'source': article.source or ''
                    }
                    metadatas.append(metadata)

                    # ID для ChromaDB (должен быть строкой)
                    ids.append(str(article.id))

                except Exception as e:
                    self.logger.warning(f"Ошибка подготовки статьи {article.id}: {e}")
                    continue

            if not texts:
                self.logger.warning("Нет текстов для индексации")
                return 0

            # Генерируем эмбеддинги
            self.logger.info(f"Генерация эмбеддингов для {len(texts)} статей...")
            embeddings = self.model.encode(texts, show_progress_bar=True)

            # Добавляем в ChromaDB
            self.collection.add(
                embeddings=embeddings.tolist(),
                metadatas=metadatas,
                ids=ids
            )

            added_count = len(ids)
            self.logger.info(f"Добавлено {added_count} статей в векторную БД")

        except Exception as e:
            self.logger.error(f"Ошибка добавления статей в векторную БД: {e}")

        return added_count

    def segment_by_themes(self, themes: List[str], limit_per_theme: int = 5) -> Dict[str, List[Dict[str, Any]]]:
        """
        Сегментация статей по темам с помощью семантического поиска

        Args:
            themes: Список тем для поиска
            limit_per_theme: Максимальное количество статей на тему

        Returns:
            Словарь {тема: [статьи]}
        """
        if not self.model or not self.collection:
            self.logger.error("Модель или ChromaDB не инициализированы")
            return {}

        results = {}

        for theme in themes:
            try:
                self.logger.info(f"Поиск статей по теме: {theme}")

                # Генерируем эмбеддинг для темы
                theme_embedding = self.model.encode([theme])[0]

                # Выполняем семантический поиск
                search_results = self.collection.query(
                    query_embeddings=[theme_embedding.tolist()],
                    n_results=limit_per_theme * 2,  # Берем больше для фильтрации
                    include=['metadatas', 'distances']
                )

                # Обрабатываем результаты
                articles = []
                if search_results and search_results.get('metadatas'):
                    for i, metadata in enumerate(search_results['metadatas'][0]):
                        try:
                            distance = search_results['distances'][0][i]

                            # Фильтруем по релевантности (меньше расстояние = лучше)
                            if distance < 1.2:  # Порог релевантности
                                article_data = {
                                    'id': metadata.get('id'),
                                    'title': metadata.get('title'),
                                    'url': metadata.get('url'),
                                    'categories': metadata.get('categories', ''),
                                    'source': metadata.get('source', ''),
                                    'similarity_score': 1.0 - (distance / 2.0),  # Преобразуем в score 0-1
                                    'distance': distance
                                }
                                articles.append(article_data)

                        except Exception as e:
                            self.logger.warning(f"Ошибка обработки результата поиска: {e}")
                            continue

                # Ограничиваем количество
                results[theme] = articles[:limit_per_theme]
                self.logger.info(f"Найдено {len(results[theme])} статей по теме '{theme}'")

            except Exception as e:
                self.logger.error(f"Ошибка семантического поиска для темы '{theme}': {e}")
                results[theme] = []

        return results

    def find_similar_articles(self, query_text: str, limit: int = 10) -> List[Dict[str, Any]]:
        """
        Поиск похожих статей по текстовому запросу

        Args:
            query_text: Текстовый запрос
            limit: Максимальное количество результатов

        Returns:
            Список похожих статей
        """
        if not self.model or not self.collection:
            return []

        try:
            # Генерируем эмбеддинг для запроса
            query_embedding = self.model.encode([query_text])[0]

            # Выполняем поиск
            search_results = self.collection.query(
                query_embeddings=[query_embedding.tolist()],
                n_results=limit,
                include=['metadatas', 'distances']
            )

            articles = []
            if search_results and search_results.get('metadatas'):
                for i, metadata in enumerate(search_results['metadatas'][0]):
                    distance = search_results['distances'][0][i]
                    articles.append({
                        'id': metadata.get('id'),
                        'title': metadata.get('title'),
                        'url': metadata.get('url'),
                        'categories': metadata.get('categories', ''),
                        'similarity_score': 1.0 - (distance / 2.0),
                        'distance': distance
                    })

            return articles

        except Exception as e:
            self.logger.error(f"Ошибка поиска похожих статей: {e}")
            return []

    def get_segmentation_stats(self) -> Dict[str, Any]:
        """
        Получение статистики векторной сегментации

        Returns:
            Статистика сегментации
        """
        stats = {
            'collection_name': 'blog_articles',
            'model': self.model_name,
            'total_documents': 0,
            'is_initialized': bool(self.collection is not None and self.model is not None)
        }

        if self.collection:
            try:
                stats['total_documents'] = self.collection.count()
            except Exception as e:
                self.logger.warning(f"Ошибка получения статистики: {e}")

        return stats

    def clear_collection(self) -> bool:
        """
        Очистка коллекции (для тестирования)

        Returns:
            True если успешно
        """
        if not self.collection:
            return False

        try:
            # Удаляем все документы
            all_ids = self.collection.get()['ids']
            if all_ids:
                self.collection.delete(ids=all_ids)
            return True
        except Exception as e:
            self.logger.error(f"Ошибка очистки коллекции: {e}")
            return False

    def rebuild_index(self, articles: List[Article]) -> int:
        """
        Перестроение индекса

        Args:
            articles: Список статей для индексации

        Returns:
            Количество проиндексированных статей
        """
        # Очищаем старую коллекцию
        self.clear_collection()

        # Создаем новую
        self.collection = self.client.get_or_create_collection(
            name="blog_articles",
            metadata={"description": "Embeddings for blog articles", "rebuilt_at": datetime.now().isoformat()}
        )

        # Добавляем статьи
        return self.add_articles_to_vector_db(articles)
