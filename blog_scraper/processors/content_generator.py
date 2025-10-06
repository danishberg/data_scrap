"""
Генератор контента с использованием OpenAI
Создает качественные блог-посты и дайджесты
"""

import logging
from datetime import datetime
from typing import List, Dict, Optional, Any

import openai
from database.models import db_manager, Article, Digest
from core.config import BlogConfig

class ContentGenerator:
    """Генератор контента с ИИ"""

    def __init__(self):
        self.logger = logging.getLogger(__name__)

        # Настройка OpenAI
        self.openai_api_key = BlogConfig.OPENAI_API_KEY
        self.model = "gpt-4o-mini" if self.openai_api_key else None
        self.openai_available = False

        if not self.model:
            self.logger.warning("OpenAI API ключ не найден. Генерация контента будет ограничена")
        else:
            # Проверяем доступность OpenAI API
            try:
                import openai
                client = openai.OpenAI(api_key=self.openai_api_key)
                # Простой тестовый запрос
                response = client.chat.completions.create(
                    model=self.model,
                    messages=[{"role": "user", "content": "test"}],
                    max_tokens=5
                )
                self.openai_available = True
                self.logger.info("OpenAI API настроен успешно и доступен")
            except Exception as e:
                self.logger.warning(f"OpenAI API недоступен (возможно региональная блокировка): {e}")
                self.logger.info("Будет использоваться упрощенная генерация без AI")
                self.openai_available = False

    def translate_text(self, text: str, target_language: str = 'ru') -> str:
        """
        Перевод текста с помощью OpenAI

        Args:
            text: Исходный текст
            target_language: Целевой язык

        Returns:
            Переведенный текст
        """
        if not text or not self.model:
            return text

        try:
            if not self.openai_api_key:
                return text

            prompt = f"""Переведи следующий текст на {target_language}.
Сохрани стиль и терминологию, особенно технические термины металлургической отрасли.

Текст:
{text}

Перевод:"""

            client = openai.OpenAI(api_key=self.openai_api_key)
            response = client.chat.completions.create(
                model=self.model,
                messages=[{"role": "user", "content": prompt}],
                max_tokens=2000,
                temperature=0.3
            )

            translated = response.choices[0].message.content.strip()
            return translated

        except Exception as e:
            self.logger.error(f"Ошибка перевода: {e}")
            return text

    def rewrite_content(self, content: str, language: str = 'ru') -> str:
        """
        Переписывание контента для создания уникального блог-поста

        Args:
            content: Исходный контент
            language: Язык контента

        Returns:
            Переписанный контент
        """
        if not content or not self.model or not self.openai_available:
            # Если OpenAI недоступен, делаем базовую обработку
            return self._basic_rewrite(content)

        try:
            if not self.openai_api_key:
                return self._basic_rewrite(content)

            prompt = f"""Создай качественный блог-пост на тему металлургической отрасли на основе предоставленной новости.

ТРЕБОВАНИЯ К БЛОГ-ПОСТУ:
1. **Структура**: Введение → Основная часть → Заключение
2. **Стиль**: Профессиональный, но доступный, engaging
3. **Длина**: 800-1200 слов
4. **Элементы**:
   - Привлекательный заголовок (не копировать оригинальный)
   - Краткое введение с хуком
   - Основная информация с анализом
   - Выводы и прогнозы
   - Использование заголовков H2, H3
   - Списки и таблицы где уместно
   - SEO-оптимизированные ключевые слова

КОНТЕКСТ: Это блог о металлургической отрасли, лому металлов, ценах, рынках.
ЯЗЫК: {language}

ИСХОДНАЯ НОВОСТЬ:
{content}

СОЗДАЙ ПОЛНОЦЕННЫЙ БЛОГ-ПОСТ:"""

            client = openai.OpenAI(api_key=self.openai_api_key)
            response = client.chat.completions.create(
                model=self.model,
                messages=[{"role": "user", "content": prompt}],
                max_tokens=4000,
                temperature=0.7
            )

            rewritten = response.choices[0].message.content.strip()
            return rewritten

        except Exception as e:
            self.logger.error(f"Ошибка рерайта: {e}")
            return self._basic_rewrite(content)

    def _basic_rewrite(self, content: str) -> str:
        """Базовая обработка контента без AI"""
        if not content:
            return content
        
        # Добавляем минимальную обработку
        lines = content.split('\n')
        processed_lines = []
        
        for line in lines:
            line = line.strip()
            if line:
                # Убираем лишние пробелы
                line = ' '.join(line.split())
                processed_lines.append(line)
        
        result = '\n\n'.join(processed_lines)
        
        # Добавляем заголовок если его нет
        if result and not result.startswith('#'):
            result = f"## Обзор\n\n{result}"
        
        return result

    def generate_blog_post(self, article: Article, target_language: str = 'ru') -> Dict[str, Any]:
        """
        Генерация поста для блога из статьи

        Args:
            article: Исходная статья
            target_language: Целевой язык

        Returns:
            Сгенерированный блог-пост
        """
        try:
            # Проверяем, есть ли уже обработанная версия
            if article.translated_title and article.translated_content:
                return self._format_blog_post(article)

            # Переводим контент (если OpenAI доступен, иначе используем оригинал)
            if self.openai_available:
                translated_title = self.translate_text(article.title, target_language)
                translated_content = self.translate_text(article.content, target_language)
            else:
                translated_title = article.title
                translated_content = article.content

            # Создаем рерайт (с AI или базовый)
            rewritten_content = self.rewrite_content(translated_content, target_language)

            # Обновляем статью в БД
            session = db_manager.get_session()
            try:
                db_article = session.query(Article).get(article.id)
                if db_article:
                    db_article.translated_title = translated_title
                    db_article.translated_content = translated_content
                    db_article.rewritten_content = rewritten_content
                    db_article.is_processed = True
                    db_article.updated_at = datetime.utcnow()
                    session.commit()

                    # Обновляем объект
                    article.translated_title = translated_title
                    article.translated_content = translated_content
                    article.rewritten_content = rewritten_content
                    article.is_processed = True

            except Exception as e:
                self.logger.error(f"Ошибка сохранения в БД: {e}")
                session.rollback()
            finally:
                session.close()

            return self._format_blog_post(article)

        except Exception as e:
            self.logger.error(f"Ошибка генерации блог-поста: {e}")
            return {}

    def batch_generate_blog_posts(self, articles: List[Article], target_language: str = 'ru') -> List[Dict[str, Any]]:
        """
        Генерация постов для блога из списка статей

        Args:
            articles: Список исходных статей
            target_language: Целевой язык

        Returns:
            Список сгенерированных постов
        """
        posts = []

        for article in articles:
            try:
                post = self.generate_blog_post(article, target_language)
                if post:
                    posts.append(post)

            except Exception as e:
                self.logger.error(f"Ошибка генерации поста для статьи {article.id}: {e}")
                continue

        self.logger.info(f"Сгенерировано {len(posts)} блог-постов из {len(articles)} статей")
        return posts

    def _format_blog_post(self, article: Article) -> Dict[str, Any]:
        """Форматирование блог-поста"""
        return {
            'id': article.id,
            'title': article.translated_title or article.title,
            'content': article.rewritten_content or article.translated_content or article.content,
            'original_title': article.title,
            'categories': article.categories_list.split(',') if article.categories_list else [],
            'tags': self._generate_tags(article),
            'url': article.url,
            'source': article.source,
            'language': article.language,
            'published_at': article.published_at.isoformat() if article.published_at else None,
            'processed_at': article.updated_at.isoformat() if article.updated_at else None,
            'is_published': article.is_published,
            'seo_meta': self._generate_seo_meta(article)
        }

    def _generate_tags(self, article: Article) -> List[str]:
        """Генерация тегов для статьи"""
        tags = []

        # Добавляем категории как теги
        if article.categories_list:
            categories = article.categories_list.split(',')
            tags.extend([cat.strip() for cat in categories])

        # Добавляем источник
        if article.source:
            tags.append(f"source:{article.source}")

        # Добавляем типы материалов
        if 'scrap' in (article.categories_list or '').lower():
            tags.extend(['scrap metal', 'металлолом', 'утильсырье'])
        if 'steel' in (article.title + ' ' + (article.content or '')).lower():
            tags.extend(['steel', 'сталь', 'металлургия'])

        return list(set(tags))  # Убираем дубликаты

    def _generate_seo_meta(self, article: Article) -> Dict[str, str]:
        """Генерация SEO метаданных"""
        title = article.translated_title or article.title
        content = article.rewritten_content or article.translated_content or article.content

        # SEO заголовок
        seo_title = f"{title} | Scraptraffic блог"
        if len(seo_title) > 60:
            seo_title = title[:57] + "..."

        # SEO описание
        if article.summary:
            seo_description = article.summary[:160]
        else:
            seo_description = content[:160] if content else f"Новости металлургической отрасли: {title[:100]}"

        if len(seo_description) > 160:
            seo_description = seo_description[:157] + "..."

        return {
            'title': seo_title,
            'description': seo_description,
            'keywords': ', '.join(self._generate_tags(article))
        }

    def generate_digest(self, articles: List[Article], frequency: str = 'daily', theme: str = 'metallurgy') -> Dict[str, Any]:
        """
        Генерация дайджеста новостей

        Args:
            articles: Список статей для дайджеста
            frequency: Частота дайджеста (daily, weekly, monthly)
            theme: Тема дайджеста

        Returns:
            Сгенерированный дайджест
        """
        if not articles:
            return {}

        # Если OpenAI недоступен, используем простую генерацию
        if not self.model or not self.openai_available:
            return self._generate_simple_digest(articles, frequency, theme)

        try:
            if not self.openai_api_key:
                return self._generate_simple_digest(articles, frequency, theme)

            # Собираем информацию о статьях
            articles_info = "\n".join([
                f"- {article.translated_title or article.title}: {article.summary or (article.content[:100] if article.content else '')}"
                for article in articles
            ])

            prompt = f"""Создай дайджест новостей металлургической отрасли.

ТИП ДАЙДЖЕСТА: {frequency}
ТЕМА: {theme}

СТАТЬИ ДЛЯ ДАЙДЖЕСТА:
{articles_info}

ТРЕБОВАНИЯ К ДАЙДЖЕСТУ:
1. Структурируй по категориям (цены, производство, торговля и т.д.)
2. Включи 3-5 наиболее важных новостей
3. Добавь краткий анализ трендов
4. Используй заголовки и списки
5. Закончи выводами и прогнозами

СОЗДАЙ ПОЛНЫЙ ДАЙДЖЕСТ:"""

            client = openai.OpenAI(api_key=self.openai_api_key)
            response = client.chat.completions.create(
                model=self.model,
                messages=[{"role": "user", "content": prompt}],
                max_tokens=2000,
                temperature=0.3
            )

            digest_content = response.choices[0].message.content.strip()

            return {
                'title': f'{frequency.title()} дайджест металлургической отрасли',
                'content': digest_content,
                'article_count': len(articles),
                'frequency': frequency,
                'theme': theme,
                'generated_at': datetime.utcnow().isoformat()
            }

        except Exception as e:
            self.logger.error(f"Ошибка генерации дайджеста: {e}")
            return self._generate_simple_digest(articles, frequency, theme)

    def _generate_simple_digest(self, articles: List[Article], frequency: str, theme: str) -> Dict[str, Any]:
        """Простая генерация дайджеста без ИИ"""
        content = f"# {frequency.title()} дайджест металлургической отрасли\n\n"

        # Группируем по категориям
        categories = {}
        for article in articles:
            cat_list = article.categories_list.split(',') if article.categories_list else ['other']
            for cat in cat_list:
                cat = cat.strip()
                if cat not in categories:
                    categories[cat] = []
                categories[cat].append(article)

        # Создаем контент
        for cat, cat_articles in categories.items():
            content += f"## {cat.replace(':', ' ').title()}\n\n"
            for article in cat_articles[:3]:
                title = article.translated_title or article.title
                content += f"- **{title}**\n"
                if article.summary:
                    content += f"  {article.summary[:100]}...\n"
                content += f"  [Читать]({article.url})\n\n"

        return {
            'title': f'{frequency.title()} дайджест металлургической отрасли',
            'content': content,
            'article_count': len(articles),
            'frequency': frequency,
            'theme': theme,
            'generated_at': datetime.utcnow().isoformat()
        }
