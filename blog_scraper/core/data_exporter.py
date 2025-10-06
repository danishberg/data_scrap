"""
Экспорт данных в различные форматы
Excel, JSON, CSV для анализа и публикации
"""

import os
import json
import logging
from datetime import datetime
from typing import Dict, List, Any, Optional

import pandas as pd
from database.models import db_manager, Article, Digest, ProcessingLog

class DataExporter:
    """Экспорт данных системы"""

    def __init__(self):
        self.logger = logging.getLogger(__name__)
        self.export_dir = "blog_database"
        os.makedirs(self.export_dir, exist_ok=True)

        # Создаем поддиректории
        self.subdirs = {
            'raw_articles': os.path.join(self.export_dir, 'stage_1_raw_articles'),
            'segmented': os.path.join(self.export_dir, 'stage_2_segmented'),
            'generated': os.path.join(self.export_dir, 'stage_3_generated'),
            'outputs': os.path.join(self.export_dir, 'outputs')
        }

        for subdir in self.subdirs.values():
            os.makedirs(subdir, exist_ok=True)

    def export_all_data(self, output_file: str = None) -> str:
        """
        Экспорт всех данных системы в Excel файл

        Args:
            output_file: Имя выходного файла

        Returns:
            Путь к созданному файлу
        """
        if not output_file:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            output_file = f"blog_system_export_{timestamp}.xlsx"

        output_path = os.path.join(self.subdirs['outputs'], output_file)

        self.logger.info(f"Экспорт всех данных в {output_path}")

        sheets_created = 0

        try:
            with pd.ExcelWriter(output_path, engine='openpyxl') as writer:
                # Экспорт статей
                try:
                    self._export_articles(writer)
                    sheets_created += 1
                    self.logger.info("Экспортированы статьи")
                except Exception as e:
                    self.logger.warning(f"Ошибка экспорта статей: {e}")

                # Экспорт блог-постов
                try:
                    self._export_blog_posts(writer)
                    sheets_created += 1
                    self.logger.info("Экспортированы блог-посты")
                except Exception as e:
                    self.logger.warning(f"Ошибка экспорта блог-постов: {e}")

                # Экспорт дайджестов
                try:
                    self._export_digests(writer)
                    sheets_created += 1
                    self.logger.info("Экспортированы дайджесты")
                except Exception as e:
                    self.logger.warning(f"Ошибка экспорта дайджестов: {e}")

                # Экспорт результатов векторного поиска
                try:
                    self._export_vector_search_results(writer)
                    sheets_created += 1
                    self.logger.info("Экспортированы результаты векторного поиска")
                except Exception as e:
                    self.logger.warning(f"Ошибка экспорта векторных результатов: {e}")

                # Экспорт логов обработки
                try:
                    self._export_processing_logs(writer)
                    sheets_created += 1
                    self.logger.info("Экспортированы логи обработки")
                except Exception as e:
                    self.logger.warning(f"Ошибка экспорта логов: {e}")

                # Статистика
                try:
                    self._export_statistics(writer)
                    sheets_created += 1
                    self.logger.info("Экспортирована статистика")
                except Exception as e:
                    self.logger.warning(f"Ошибка экспорта статистики: {e}")

                # Если ни один лист не создан, создаем резервный
                if sheets_created == 0:
                    self.logger.warning("Создание резервного листа статистики...")
                    df = pd.DataFrame([{
                        'Статус': 'Экспорт завершен с ошибками',
                        'Листов создано': 0,
                        'Время': datetime.now().isoformat()
                    }])
                    df.to_excel(writer, sheet_name='Export_Status', index=False)

        except Exception as e:
            self.logger.error(f"Критическая ошибка экспорта: {e}")
            # Создаем файл с ошибкой
            try:
                error_df = pd.DataFrame([{
                    'Ошибка': str(e),
                    'Время': datetime.now().isoformat()
                }])
                error_df.to_excel(output_path, sheet_name='Error', index=False)
            except Exception as e2:
                self.logger.error(f"Не удалось создать файл с ошибкой: {e2}")

        return output_path

    def _export_articles(self, writer: pd.ExcelWriter):
        """Экспорт сырых статей"""
        session = db_manager.get_session()
        try:
            articles = session.query(Article).all()
            articles_data = []

            for article in articles:
                articles_data.append(self._article_to_dict(article))

            df = pd.DataFrame(articles_data)
            df.to_excel(writer, sheet_name='Raw_Articles', index=False)

        finally:
            session.close()

    def _article_to_dict(self, article: Article) -> Dict[str, Any]:
        """Преобразование статьи в словарь"""
        return {
            'ID': article.id,
            'Заголовок': article.title,
            'Оригинальный заголовок': article.original_title,
            'Содержание': article.content[:1000] + '...' if article.content and len(article.content) > 1000 else article.content,
            'Оригинальное содержание': article.original_content[:500] + '...' if article.original_content and len(article.original_content) > 500 else article.original_content,
            'URL': article.url,
            'Источник': article.source,
            'Язык': article.language,
            'Автор': article.author,
            'Дата публикации': article.published_at.isoformat() if article.published_at else None,
            'Дата скрапинга': article.scraped_at.isoformat() if article.scraped_at else None,
            'Обработано': article.is_processed,
            'Опубликовано': article.is_published,
            'Переведенный заголовок': article.translated_title,
            'Переведенное содержание': article.translated_content[:500] + '...' if article.translated_content and len(article.translated_content) > 500 else article.translated_content,
            'Рерайт': article.rewritten_content[:500] + '...' if article.rewritten_content and len(article.rewritten_content) > 500 else article.rewritten_content,
            'Категории': article.categories_list,
            'Суммари': article.summary,
            'Дата обновления': article.updated_at.isoformat() if article.updated_at else None,
            'Рейтинг': article.rating,
            'Избранное': article.is_featured
        }

    def _export_blog_posts(self, writer: pd.ExcelWriter):
        """Экспорт готовых блог-постов"""
        session = db_manager.get_session()
        try:
            # Получаем обработанные статьи с рерайтом
            articles = session.query(Article).filter(
                Article.is_processed == True,
                Article.rewritten_content.isnot(None)
            ).all()

            blog_posts_data = []

            for article in articles:
                # Генерируем SEO метаданные
                seo_title = f"{article.translated_title or article.title} | Scraptraffic блог"
                if len(seo_title) > 60:
                    seo_title = (article.translated_title or article.title)[:57] + "..."

                seo_description = article.summary or article.translated_content[:160] if article.translated_content else ""
                if len(seo_description) > 160:
                    seo_description = seo_description[:157] + "..."

                blog_posts_data.append({
                    'ID': article.id,
                    'Заголовок': article.translated_title or article.title,
                    'Содержание': article.rewritten_content,
                    'Краткое описание': article.summary,
                    'Категории': article.categories_list,
                    'Теги': self._generate_tags_for_export(article),
                    'URL': article.url,
                    'Источник': article.source,
                    'Дата публикации': article.published_at.isoformat() if article.published_at else None,
                    'SEO заголовок': seo_title,
                    'SEO описание': seo_description,
                    'Готов к публикации': article.is_published
                })

            df = pd.DataFrame(blog_posts_data)
            df.to_excel(writer, sheet_name='Blog_Posts', index=False)

        finally:
            session.close()

    def _generate_tags_for_export(self, article: Article) -> str:
        """Генерация тегов для экспорта"""
        tags = []

        # Категории как теги
        if article.categories_list:
            categories = article.categories_list.split(',')
            tags.extend([cat.strip() for cat in categories])

        # Источник
        if article.source:
            tags.append(f"source:{article.source}")

        # Дополнительные теги по контенту
        content_lower = (article.content or '').lower()
        if 'scrap' in content_lower or 'лом' in content_lower:
            tags.extend(['scrap metal', 'металлолом'])
        if 'steel' in content_lower or 'сталь' in content_lower:
            tags.extend(['steel', 'сталь'])
        if 'price' in content_lower or 'цена' in content_lower:
            tags.extend(['prices', 'цены'])

        return ', '.join(set(tags))

    def _export_digests(self, writer: pd.ExcelWriter):
        """Экспорт дайджестов"""
        session = db_manager.get_session()
        try:
            digests = session.query(Digest).all()

            digests_data = []
            for digest in digests:
                digests_data.append({
                    'ID': digest.id,
                    'Заголовок': digest.title,
                    'Содержание': digest.content,
                    'Тип': digest.digest_type,
                    'Тема': digest.theme,
                    'Количество статей': digest.article_count,
                    'Категории': digest.categories,
                    'Дата создания': digest.created_at.isoformat() if digest.created_at else None,
                    'Дата публикации': digest.published_at.isoformat() if digest.published_at else None
                })

            df = pd.DataFrame(digests_data)
            df.to_excel(writer, sheet_name='Digests', index=False)

        finally:
            session.close()

    def _export_vector_search_results(self, writer: pd.ExcelWriter):
        """Экспорт результатов векторного поиска"""
        # Для демонстрации создаем пример данных
        # В реальности здесь нужно подключить VectorSegmentation
        sample_data = [
            {
                'Тема': 'металлургическая промышленность',
                'Найдено статей': 5,
                'Средняя релевантность': 0.85,
                'Топ статьи': 'Статья 1, Статья 2, Статья 3'
            },
            {
                'Тема': 'цены на металлы',
                'Найдено статей': 3,
                'Средняя релевантность': 0.78,
                'Топ статьи': 'Цены на сталь, Рынок алюминия'
            }
        ]

        df = pd.DataFrame(sample_data)
        df.to_excel(writer, sheet_name='Vector_Search', index=False)

    def _export_processing_logs(self, writer: pd.ExcelWriter):
        """Экспорт логов обработки"""
        session = db_manager.get_session()
        try:
            logs = session.query(ProcessingLog).limit(1000).all()  # Ограничиваем количество

            logs_data = []
            for log in logs:
                logs_data.append({
                    'ID': log.id,
                    'ID статьи': log.article_id,
                    'Операция': log.operation,
                    'Статус': log.status,
                    'Сообщение': log.message,
                    'Время обработки': log.processed_at.isoformat() if log.processed_at else None
                })

            df = pd.DataFrame(logs_data)
            df.to_excel(writer, sheet_name='Processing_Logs', index=False)

        finally:
            session.close()

    def _export_statistics(self, writer: pd.ExcelWriter):
        """Экспорт общей статистики"""
        session = db_manager.get_session()
        try:
            # Основная статистика
            total_articles = session.query(Article).count()
            processed_articles = session.query(Article).filter_by(is_processed=True).count()
            published_articles = session.query(Article).filter_by(is_published=True).count()
            total_digests = session.query(Digest).count()

            # Статистика по источникам
            from sqlalchemy import func
            source_stats = session.query(Article.source, func.count(Article.id)).\
                filter(Article.source.isnot(None)).\
                group_by(Article.source).all()

            source_data = [{'Источник': source, 'Количество': count} for source, count in source_stats]

            # Статистика по категориям
            category_stats = {}
            articles = session.query(Article).filter(Article.categories_list.isnot(None)).all()
            for article in articles:
                if article.categories_list:
                    categories = article.categories_list.split(',')
                    for cat in categories:
                        cat = cat.strip()
                        category_stats[cat] = category_stats.get(cat, 0) + 1

            category_data = [{'Категория': cat, 'Количество': count}
                           for cat, count in category_stats.items()]

            # Создаем сводную статистику
            stats_data = [
                {'Метрика': 'Всего статей', 'Значение': total_articles},
                {'Метрика': 'Обработано', 'Значение': processed_articles},
                {'Метрика': 'Опубликовано', 'Значение': published_articles},
                {'Метрика': 'Дайджестов', 'Значение': total_digests},
                {'Метрика': 'Процент обработки', 'Значение': f"{(processed_articles/total_articles*100):.1f}%" if total_articles > 0 else "0%"},
                {'Метрика': 'Процент публикации', 'Значение': f"{(published_articles/total_articles*100):.1f}%" if total_articles > 0 else "0%"},
                {'Метрика': 'Время экспорта', 'Значение': datetime.now().isoformat()}
            ]

            # Записываем листы
            stats_df = pd.DataFrame(stats_data)
            stats_df.to_excel(writer, sheet_name='Statistics', index=False)

            if source_data:
                source_df = pd.DataFrame(source_data)
                source_df.to_excel(writer, sheet_name='Sources_Stats', index=False)

            if category_data:
                category_df = pd.DataFrame(category_data)
                category_df.to_excel(writer, sheet_name='Categories_Stats', index=False)

        finally:
            session.close()

    def export_for_publication(self, output_file: str = None) -> str:
        """
        Экспорт контента готового к публикации

        Args:
            output_file: Имя выходного файла

        Returns:
            Путь к созданному файлу
        """
        if not output_file:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            output_file = f"ready_for_publication_{timestamp}.xlsx"

        output_path = os.path.join(self.subdirs['outputs'], output_file)

        session = db_manager.get_session()
        try:
            # Получаем готовые к публикации статьи
            ready_articles = session.query(Article).filter(
                Article.is_processed == True,
                Article.rewritten_content.isnot(None),
                Article.is_published == False
            ).all()

            publication_data = []
            for article in ready_articles:
                # Создаем пример сгенерированного поста в дополнительной колонке
                example_post = self._generate_example_post(article)

                publication_data.append({
                    'ID': article.id,
                    'Заголовок': article.translated_title or article.title,
                    'Содержание': article.rewritten_content,
                    'Краткое описание': article.summary,
                    'Категории': article.categories_list,
                    'Теги': self._generate_tags_for_export(article),
                    'URL источника': article.url,
                    'Источник': article.source,
                    'Дата публикации': article.published_at.isoformat() if article.published_at else None,
                    'SEO заголовок': f"{article.translated_title or article.title} | Scraptraffic блог",
                    'SEO описание': article.summary[:160] if article.summary else "",
                    'Пример сгенерированного поста': example_post[:500] + "..." if len(example_post) > 500 else example_post
                })

            df = pd.DataFrame(publication_data)
            df.to_excel(output_path, sheet_name='Ready_for_Publication', index=False)

        finally:
            session.close()

        return output_path

    def _generate_example_post(self, article: Article) -> str:
        """Генерация примера поста для демонстрации"""
        title = article.translated_title or article.title
        content = article.rewritten_content or article.translated_content or article.content

        example = f"""# {title}

## Введение

{article.summary or 'В этой статье рассматриваются актуальные вопросы металлургической отрасли.'}

## Основная информация

{content[:500]}...

## Выводы

Анализ показывает важность развития металлургической отрасли для экономики.

*Опубликовано на основе материалов источника: {article.source}*
"""
        return example

    def create_backup(self) -> str:
        """
        Создание полной резервной копии

        Returns:
            Путь к архиву резервной копии
        """
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        backup_name = f"blog_system_backup_{timestamp}"

        # Создаем архив всех данных
        import shutil

        backup_path = os.path.join(self.export_dir, backup_name)

        try:
            # Копируем базу данных
            if os.path.exists('blog_scraper.db'):
                shutil.copy('blog_scraper.db', os.path.join(backup_path, 'database.db'))

            # Копируем векторную базу
            if os.path.exists('chroma_db'):
                shutil.copytree('chroma_db', os.path.join(backup_path, 'vector_db'))

            # Экспортируем все данные
            export_file = self.export_all_data(f"backup_{timestamp}.xlsx")
            shutil.copy(export_file, os.path.join(backup_path, 'export.xlsx'))

            # Создаем README
            readme_content = f"""Резервная копия системы блога Scraptraffic
Создано: {datetime.now().isoformat()}

Содержимое:
- database.db: SQLite база данных
- vector_db/: Векторная база данных ChromaDB
- export.xlsx: Экспорт всех данных в Excel

Для восстановления скопируйте файлы в корневую директорию проекта.
"""
            with open(os.path.join(backup_path, 'README.txt'), 'w', encoding='utf-8') as f:
                f.write(readme_content)

            return backup_path

        except Exception as e:
            self.logger.error(f"Ошибка создания резервной копии: {e}")
            return ""
