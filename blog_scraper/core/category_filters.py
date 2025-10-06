"""
Фильтры категорий для статей
Правило-основанная категоризация контента
"""

import logging
from typing import Dict, List, Any, Optional

class CategoryFilters:
    """Правило-основанная категоризация статей"""

    def __init__(self):
        self.logger = logging.getLogger(__name__)

        # Ключевые слова для материалов
        self.material_keywords = {
            'ferrous': [
                'ферросплав', 'черный металл', 'железо', 'сталь', 'чугун',
                'ferrous', 'iron', 'steel', 'cast iron', 'pig iron'
            ],
            'non_ferrous': [
                'цветной металл', 'алюмини', 'медь', 'цинк', 'никель', 'свинец',
                'non-ferrous', 'aluminum', 'copper', 'zinc', 'nickel', 'lead'
            ],
            'precious': [
                'драгоценный', 'золото', 'серебро', 'платина', 'палладий',
                'precious', 'gold', 'silver', 'platinum', 'palladium'
            ],
            'scrap': [
                'лом', 'утиль', 'scrap', 'waste', 'recycling', 'переработка',
                'вторичный', 'secondary', 'metal waste'
            ]
        }

        # Ключевые слова для регионов
        self.region_keywords = {
            'russia': [
                'россия', 'российск', 'москва', 'санкт-петербург', 'екатеринбург',
                'новолипецк', 'магнитогорск', 'череповец', 'нижний новгород',
                'russia', 'russian', 'moscow', 'st. petersburg', 'yekaterinburg'
            ],
            'europe': [
                'европа', 'европейск', 'германия', 'франция', 'великобритания',
                'италия', 'испания', 'польша', 'украина', 'беларусь',
                'europe', 'european', 'germany', 'france', 'uk', 'italy', 'spain'
            ],
            'asia': [
                'азия', 'азиатск', 'китай', 'япония', 'корея', 'индия',
                'asia', 'asian', 'china', 'japan', 'korea', 'india'
            ],
            'america': [
                'америка', 'сша', 'соединенные штаты', 'канада', 'бразилия',
                'america', 'usa', 'united states', 'canada', 'brazil'
            ]
        }

        # Типы новостей
        self.news_type_keywords = {
            'prices': [
                'цена', 'стоимость', 'рынок', 'торги', 'котировки', 'валюта',
                'price', 'cost', 'market', 'trading', 'quotes', 'currency'
            ],
            'production': [
                'производство', 'выпуск', 'завод', 'комбинат', 'предприятие',
                'production', 'output', 'plant', 'mill', 'facility'
            ],
            'trade': [
                'торговля', 'экспорт', 'импорт', 'поставки', 'контракт',
                'trade', 'export', 'import', 'supply', 'contract'
            ],
            'policy': [
                'политика', 'закон', 'регулирование', 'министерство', 'правительство',
                'policy', 'law', 'regulation', 'ministry', 'government'
            ],
            'technology': [
                'технология', 'инновации', 'оборудование', 'модернизация',
                'technology', 'innovation', 'equipment', 'modernization'
            ],
            'environment': [
                'экология', 'окружающая среда', 'загрязнение', 'эко',
                'environment', 'pollution', 'eco', 'green'
            ],
            'market_analysis': [
                'анализ', 'прогноз', 'тенденции', 'статистика', 'отчет',
                'analysis', 'forecast', 'trends', 'statistics', 'report'
            ],
            'logistics': [
                'логистика', 'транспорт', 'доставка', 'поставки', 'склад',
                'logistics', 'transport', 'delivery', 'supply', 'warehouse'
            ]
        }

    def filter_content(self, title: str, content: str, filter_types: List[str] = None) -> Dict[str, Any]:
        """
        Фильтрация контента по заданным типам

        Args:
            title: Заголовок статьи
            content: Содержимое статьи
            filter_types: Типы фильтров для применения

        Returns:
            Результаты фильтрации
        """
        if filter_types is None:
            filter_types = ['materials', 'regions', 'news_types']

        text_to_analyze = f"{title} {content}".lower()
        results = {
            'matches': {},
            'categories': [],
            'confidence': {},
            'filter_types_applied': filter_types
        }

        try:
            # Применяем фильтры материалов
            if 'materials' in filter_types:
                material_matches = self._filter_by_keywords(text_to_analyze, self.material_keywords)
                if material_matches:
                    results['matches']['materials'] = material_matches

            # Применяем фильтры регионов
            if 'regions' in filter_types:
                region_matches = self._filter_by_keywords(text_to_analyze, self.region_keywords)
                if region_matches:
                    results['matches']['regions'] = region_matches

            # Применяем фильтры типов новостей
            if 'news_types' in filter_types:
                news_type_matches = self._filter_by_keywords(text_to_analyze, self.news_type_keywords)
                if news_type_matches:
                    results['matches']['news_types'] = news_type_matches

            # Формируем итоговые категории
            all_matches = []
            for match_type, matches in results['matches'].items():
                for category, score in matches.items():
                    all_matches.append(f"{match_type[:-1]}:{category}")  # Убираем 's' и добавляем категорию

            results['categories'] = all_matches

            # Если нет совпадений, добавляем 'other'
            if not all_matches:
                results['categories'] = ['type:other']

        except Exception as e:
            self.logger.error(f"Ошибка при фильтрации контента: {e}")
            results['error'] = str(e)

        return results

    def _filter_by_keywords(self, text: str, keyword_dict: Dict[str, List[str]]) -> Dict[str, float]:
        """
        Фильтрация текста по словарю ключевых слов

        Args:
            text: Текст для анализа
            keyword_dict: Словарь {категория: [ключевые_слова]}

        Returns:
            Словарь {категория: score}
        """
        matches = {}

        for category, keywords in keyword_dict.items():
            score = 0

            for keyword in keywords:
                # Считаем вхождения ключевого слова
                count = text.count(keyword.lower())
                if count > 0:
                    score += count

            # Нормализуем score (0-1)
            if score > 0:
                # Простая нормализация: больше совпадений = выше score
                normalized_score = min(score / 5.0, 1.0)  # Максимум 5 совпадений = score 1.0
                matches[category] = normalized_score

        return matches

    def get_category_stats(self, articles: List[Dict[str, Any]]) -> Dict[str, Any]:
        """
        Получение статистики по категориям для списка статей

        Args:
            articles: Список статей с категориями

        Returns:
            Статистика по категориям
        """
        stats = {
            'total_articles': len(articles),
            'categories_count': {},
            'top_categories': [],
            'coverage': 0.0
        }

        category_counts = {}

        for article in articles:
            categories = article.get('categories', [])
            if isinstance(categories, str):
                categories = [cat.strip() for cat in categories.split(',')]

            for category in categories:
                category_counts[category] = category_counts.get(category, 0) + 1

        stats['categories_count'] = category_counts

        # Топ категории
        sorted_categories = sorted(category_counts.items(), key=lambda x: x[1], reverse=True)
        stats['top_categories'] = sorted_categories[:10]

        # Процент покрытия (статьи с хотя бы одной категорией)
        categorized_articles = sum(1 for article in articles
                                 if article.get('categories') and
                                 (len(article['categories']) > 1 if isinstance(article['categories'], list)
                                  else article['categories'] != 'type:other'))

        if stats['total_articles'] > 0:
            stats['coverage'] = categorized_articles / stats['total_articles']

        return stats

    def suggest_keywords(self, category: str, sample_texts: List[str], language: str = 'ru') -> List[str]:
        """
        Предложение новых ключевых слов на основе примеров текста

        Args:
            category: Категория для которой предлагаем ключевые слова
            sample_texts: Примеры текстов из этой категории
            language: Язык текстов

        Returns:
            Список предложенных ключевых слов
        """
        if not sample_texts:
            return []

        # Простая эвристика: наиболее частые существительные
        all_words = []
        for text in sample_texts:
            words = text.lower().split()
            # Фильтруем стоп-слова (простая версия)
            stop_words = {'и', 'в', 'на', 'с', 'по', 'для', 'от', 'до', 'из', 'к', 'о',
                         'the', 'and', 'or', 'but', 'in', 'on', 'at', 'to', 'for', 'of', 'with', 'by'}
            filtered_words = [word for word in words if len(word) > 3 and word not in stop_words]
            all_words.extend(filtered_words)

        # Считаем частоты
        from collections import Counter
        word_counts = Counter(all_words)

        # Возвращаем топ-10 наиболее частых слов
        suggested = [word for word, count in word_counts.most_common(10)]
        return suggested
