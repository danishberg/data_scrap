"""
Обработчик контента для статей блога
Включает перевод, рерайт, категоризацию и генерацию summary
"""

import logging
from datetime import datetime
from typing import List, Dict, Optional, Any

from database.models import Digest
from core.category_filters import CategoryFilters
from core.config import BlogConfig

class ContentProcessor:
    """Обработчик контента статей"""

    def __init__(self):
        self.logger = logging.getLogger(__name__)
        self.category_filters = CategoryFilters()

        # Загружаем spaCy модель для русского языка
        try:
            import spacy
            self.nlp = spacy.load("ru_core_news_sm")
            self.logger.info("spaCy модель загружена успешно")
        except Exception as e:
            self.logger.warning(f"Не удалось загрузить spaCy модель: {e}")
            self.nlp = None

    def process_url(self, url: str) -> Optional[Dict[str, Any]]:
        """Process a URL and extract article content"""
        try:
            import newspaper
            article = newspaper.Article(url)
            article.download()
            article.parse()

            if not article.title or not article.text:
                return None

            return {
                'title': article.title,
                'content': article.text,
                'url': url,
                'summary': article.summary if hasattr(article, 'summary') else '',
                'authors': article.authors,
                'publish_date': article.publish_date.isoformat() if article.publish_date else None,
                'processed_at': datetime.now().isoformat()
            }
        except Exception as e:
            self.logger.warning(f"Failed to process URL {url}: {e}")
            return None

    def process_article(self, article) -> bool:
        """Полная обработка статьи"""
        try:
            # 1. Перевод (если нужно) - пока заглушка
            if article.language != 'ru':
                translated_title = self.translate_text(article.title)
                translated_content = self.translate_text(article.content)

                if translated_title and translated_content:
                    article.translated_title = translated_title
                    article.translated_content = translated_content

            # 2. Рерайт
            content_to_rewrite = article.translated_content or article.content
            rewritten_content = self.rewrite_text(content_to_rewrite)

            if rewritten_content:
                article.rewritten_content = rewritten_content

            # 3. Категоризация
            title_for_categorization = article.translated_title or article.title
            content_for_categorization = article.translated_content or article.content

            categories = self.categorize_article(title_for_categorization, content_for_categorization)

            # Сохраняем категории как строку (упрощенная версия)
            if categories:
                article.categories_list = ', '.join(categories)

            # 4. Генерация summary
            content_for_summary = article.rewritten_content or article.translated_content or article.content
            summary = self.generate_summary(content_for_summary)

            if summary:
                article.summary = summary

            # 5. Обновление статуса
            article.is_processed = True
            article.updated_at = datetime.utcnow()

            self.logger.info(f"Статья {article.id} успешно обработана: категории {categories}")
            return True

        except Exception as e:
            self.logger.error(f"Ошибка при обработке статьи {article.id}: {e}")
            return False

    def translate_text(self, text: str) -> str:
        """Перевод текста (заглушка)"""
        # Пока просто возвращаем оригинал
        # В будущем здесь будет интеграция с переводчиком
        return text

    def rewrite_text(self, content: str) -> str:
        """Переписывание текста для уникальности"""
        if not self.nlp or not content:
            return content

        try:
            # Простая обработка с spaCy
            doc = self.nlp(content[:2000])  # Ограничиваем длину

            # Базовое переписывание: исправление регистра и пробелов
            rewritten = []

            for sent in doc.sents:
                sent_text = sent.text.strip()
                if sent_text:
                    # Исправляем регистр первого слова
                    if sent_text[0].islower():
                        sent_text = sent_text[0].upper() + sent_text[1:]
                    rewritten.append(sent_text)

            result = ' '.join(rewritten)

            # Убеждаемся, что результат не слишком короткий
            if len(result) < len(content) * 0.5:
                return content

            return result

        except Exception as e:
            self.logger.warning(f"Ошибка при рерайтинге: {e}")
            return content

    def categorize_article(self, title: str, content: str) -> List[str]:
        """Категоризация статьи"""
        text = f"{title} {content}"

        # Используем фильтры категорий
        filter_result = self.category_filters.filter_content(
            title, content, filter_types=['materials', 'regions', 'news_types']
        )

        categories = []

        # Добавляем категории материалов
        if filter_result.get('matches', {}).get('materials'):
            for material in filter_result['matches']['materials'].keys():
                categories.append(f"material:{material}")

        # Добавляем категории регионов
        if filter_result.get('matches', {}).get('regions'):
            for region in filter_result['matches']['regions'].keys():
                categories.append(f"region:{region}")

        # Добавляем типы новостей
        if filter_result.get('matches', {}).get('news_types'):
            for news_type in filter_result['matches']['news_types'].keys():
                categories.append(f"type:{news_type}")

        # Если нет категорий, добавляем "other"
        if not categories:
            categories.append("type:other")

        return categories

    def generate_summary(self, content: str, max_length: int = 200) -> str:
        """Генерация краткого описания статьи"""
        if not self.nlp or not content:
            # Простая обрезка если spaCy недоступен
            if len(content) <= max_length:
                return content
            return content[:max_length - 3] + "..."

        try:
            doc = self.nlp(content[:3000])  # Ограничиваем для производительности

            # Извлекаем предложения и ранжируем по важности
            sentences = list(doc.sents)

            if not sentences:
                return content[:max_length - 3] + "..." if len(content) > max_length else content

            # Простой скоринг предложений по наличию ключевых слов
            keywords = BlogConfig.KEYWORDS.get('ru', [])
            sentence_scores = []

            for sent in sentences:
                score = 0
                sent_text = sent.text.lower()

                for keyword in keywords:
                    if keyword.lower() in sent_text:
                        score += 1

                sentence_scores.append((sent, score))

            # Сортируем по скорингу
            sentence_scores.sort(key=lambda x: x[1], reverse=True)

            # Берем топ предложений
            selected_sentences = []
            current_length = 0

            for sent, score in sentence_scores:
                sent_text = sent.text.strip()
                if current_length + len(sent_text) <= max_length - 10:  # Резерв для "..."
                    selected_sentences.append(sent_text)
                    current_length += len(sent_text) + 1  # +1 для пробела
                else:
                    break

            if selected_sentences:
                summary = ' '.join(selected_sentences)
                if len(content) > max_length:
                    summary += "..."
                return summary
            else:
                return content[:max_length - 3] + "..."

        except Exception as e:
            self.logger.warning(f"Ошибка при генерации summary: {e}")
            # Fallback
            if len(content) <= max_length:
                return content
            return content[:max_length - 3] + "..."

    def generate_digest(self, frequency: str, period_start: datetime, period_end: datetime) -> Optional[Digest]:
        """Генерация дайджеста новостей"""
        try:
            from database.models import db_manager, Article

            session = db_manager.get_session()

            # Получаем статьи за период
            articles = session.query(Article).filter(
                Article.is_processed == True,
                Article.published_at.between(period_start, period_end)
            ).limit(20).all()

            if not articles:
                self.logger.info("Нет статей для дайджеста")
                session.close()
                return None

            # Группируем по категориям
            category_groups = {}
            for article in articles:
                categories = article.categories_list.split(', ') if article.categories_list else ['other']
                for category in categories:
                    if category not in category_groups:
                        category_groups[category] = []
                    category_groups[category].append(article)

            # Создаем контент дайджеста
            digest_content = f"# {frequency.title()} дайджест новостей металлургической отрасли\n\n"
            digest_content += f"Период: {period_start.strftime('%d.%m.%Y')} - {period_end.strftime('%d.%m.%Y')}\n\n"

            total_articles = 0
            for category, cat_articles in category_groups.items():
                if cat_articles:
                    digest_content += f"## {category.replace('_', ' ').title()}\n\n"
                    for article in cat_articles[:5]:  # Максимум 5 статей на категорию
                        title = article.translated_title or article.title
                        digest_content += f"- **{title}**\n"
                        if article.summary:
                            digest_content += f"  {article.summary[:100]}...\n"
                        digest_content += f"  [Читать далее]({article.url})\n\n"
                        total_articles += 1

            # Создаем дайджест в БД
            digest = Digest(
                title=f"{frequency.title()} дайджест металлургической отрасли",
                content=digest_content,
                digest_type=frequency,
                article_count=total_articles,
                categories=', '.join(category_groups.keys()),
                created_at=datetime.utcnow()
            )

            session.add(digest)
            session.commit()

            self.logger.info(f"Создан {frequency} дайджест с {total_articles} статьями")
            session.close()

            return digest

        except Exception as e:
            self.logger.error(f"Ошибка при генерации дайджеста: {e}")
            return None
