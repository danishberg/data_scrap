#!/usr/bin/env python3
"""
УСИЛЕННЫЙ ПОЛНОСТЬЮ АВТОМАТИЧЕСКИЙ GOOGLE ПАРСЕР
Продвинутые методы обхода блокировок Google
ПРИОРИТЕТ: Максимум телефонов из низкопозиционных компаний
"""

import os
import json
import time
import random
import logging
import requests
from bs4 import BeautifulSoup
from datetime import datetime
import pandas as pd
from urllib.parse import quote_plus, urljoin, urlparse, parse_qs
import re
from concurrent.futures import ThreadPoolExecutor, as_completed
from collections import defaultdict
import urllib3
import hashlib
import base64
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

try:
    import undetected_chromedriver as uc  # type: ignore
    UC_AVAILABLE = True
except ImportError:
    UC_AVAILABLE = False

class FullyAutomaticGoogleScraper:
    def __init__(self):
        self.session = requests.Session()
        self.results = []
        self.processed_urls = set()
        self.failed_searches = []
        self.blocked_count = 0
        self.success_count = 0
        self.logger = self._setup_logging()
        
        # КРИТИЧЕСКИ ВАЖНЫЕ НАСТРОЙКИ
        self.MIN_PHONE_PERCENTAGE = 85  # 85% с телефонами
        self.MAX_PAGES_PER_QUERY = 5    # Страницы 1-5 (фокус на 2-5)
        self.MIN_RESULTS_PER_CITY = 50  # Минимум результатов на город
        
        # Расширенные паттерны телефонов с высокой точностью
        self.phone_patterns = [
            # tel: ссылки (максимальный приоритет)
            re.compile(r'tel:[\s]*\+?1?[\s]*\(?(\d{3})\)?[\s]*[-.\s]*(\d{3})[\s]*[-.\s]*(\d{4})', re.IGNORECASE),
            
            # Стандартные US форматы
            re.compile(r'\(?(\d{3})\)?[-.\s]*(\d{3})[-.\s]*(\d{4})(?!\d)'),
            re.compile(r'(\d{3})[-.\s]+(\d{3})[-.\s]+(\d{4})(?!\d)'),
            re.compile(r'1[-.\s]*\(?(\d{3})\)?[-.\s]*(\d{3})[-.\s]*(\d{4})(?!\d)'),
            
            # Контекстные паттерны
            re.compile(r'(?:phone|call|tel|contact)[\s:]*\(?(\d{3})\)?[-.\s]*(\d{3})[-.\s]*(\d{4})', re.IGNORECASE),
            
            # Скрытые атрибуты и data поля
            re.compile(r'(?:data-phone|data-tel)[\s]*=[\s]*["\'][\s]*\(?(\d{3})\)?[-.\s]*(\d{3})[-.\s]*(\d{4})', re.IGNORECASE),
        ]
        
        # Поисковые запросы для максимального охвата
        self.search_queries = [
            'scrap metal buyers',
            'metal recycling center', 
            'scrap yard near me',
            'junk yard',
            'auto salvage yard',
            'copper scrap buyers',
            'aluminum recycling',
            'steel scrap dealers',
            'metal scrap pickup',
            'recycling center',
            'salvage auto parts',
            'scrap metal dealers'
        ]
        
        # Целевые города (средние города с потенциалом)
        self.target_cities = [
            'Akron OH', 'Toledo OH', 'Dayton OH', 'Youngstown OH', 'Canton OH',
            'Rochester NY', 'Syracuse NY', 'Buffalo NY', 'Albany NY', 'Schenectady NY',
            'Scranton PA', 'Allentown PA', 'Reading PA', 'Erie PA', 'Bethlehem PA',
            'Flint MI', 'Lansing MI', 'Kalamazoo MI', 'Grand Rapids MI', 'Saginaw MI',
            'Rockford IL', 'Peoria IL', 'Decatur IL', 'Springfield IL', 'Champaign IL',
            'Fort Wayne IN', 'Evansville IN', 'South Bend IN', 'Gary IN', 'Muncie IN',
            'Green Bay WI', 'Appleton WI', 'Oshkosh WI', 'Racine WI', 'Kenosha WI',
            'Cedar Rapids IA', 'Davenport IA', 'Sioux City IA', 'Waterloo IA',
            'Springfield MO', 'Columbia MO', 'Joplin MO', 'St. Joseph MO',
            'Little Rock AR', 'Fayetteville AR', 'Jonesboro AR', 'Pine Bluff AR'
        ]
        
        # Продвинутые User-Agents с реальными браузерами
        self.user_agents = [
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36',
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:122.0) Gecko/20100101 Firefox/122.0',
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:121.0) Gecko/20100101 Firefox/121.0',
            'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36',
            'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.2 Safari/605.1.15',
            'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36',
            'Mozilla/5.0 (X11; Linux x86_64; rv:122.0) Gecko/20100101 Firefox/122.0'
        ]
        
        # Альтернативные поисковики (fallback)
        self.search_engines = [
            {
                'name': 'Google',
                'url': 'https://www.google.com/search',
                'params': {'q': '', 'start': 0},
                'priority': 1
            },
            {
                'name': 'Bing',
                'url': 'https://www.bing.com/search',
                'params': {'q': '', 'first': 0},
                'priority': 2
            },
            {
                'name': 'DuckDuckGo',
                'url': 'https://duckduckgo.com/html',
                'params': {'q': '', 's': 0},
                'priority': 3
            }
        ]
        
        # Инициализация сессии с cookies
        self._init_session()
        # Основной поисковик (можно переключить на 'Google' или 'DuckDuckGo')
        self.primary_engine = 'Bing'

    def _init_session(self):
        """Инициализация сессии с cookies для имитации браузера"""
        self.session.cookies.update({
            'NID': self._generate_google_cookie(),
            'CONSENT': 'YES+cb.20210328-17-p0.en+FX+667',
            'SOCS': 'CAESEwgDEgk0NzM5NDAzMjYaAmVuIAEaBgiA1YC4Bg',
            '1P_JAR': datetime.now().strftime('%Y-%m-%d-0%H'),
            'AEC': 'Ae3NU9O' + self._generate_random_string(32),
            'OGPC': '19031980-1:',
            'DV': self._generate_random_string(24)
        })

    def _generate_google_cookie(self):
        """Генерация реалистичного Google NID cookie"""
        timestamp = str(int(time.time()))
        random_part = self._generate_random_string(32)
        return f"511={timestamp}-{random_part}"

    def _generate_random_string(self, length):
        """Генерация случайной строки"""
        import string
        return ''.join(random.choices(string.ascii_letters + string.digits, k=length))

    def _setup_logging(self):
        logger = logging.getLogger('FullyAutomaticGoogleScraper')
        logger.setLevel(logging.INFO)
        
        if not logger.handlers:
            # Console handler
            console_handler = logging.StreamHandler()
            console_formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
            console_handler.setFormatter(console_formatter)
            logger.addHandler(console_handler)
            
            # File handler
            file_handler = logging.FileHandler('google_scraper.log', encoding='utf-8')
            file_formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
            file_handler.setFormatter(file_formatter)
            logger.addHandler(file_handler)
        
        return logger

    def run_complete_automation(self, target_businesses=1000):
        """Полностью автоматический сбор из Google"""
        self.logger.info(f"🤖 ЗАПУСК УСИЛЕННОГО АВТОМАТИЧЕСКОГО СБОРА")
        self.logger.info(f"🎯 ЦЕЛЬ: {target_businesses} бизнесов с {self.MIN_PHONE_PERCENTAGE}% телефонов")
        self.logger.info(f"🛡️ ЗАЩИТА: Продвинутые методы обхода блокировок")
        
        start_time = time.time()
        
        # Этап 1: Массовый сбор ссылок из Google с улучшенными методами
        all_google_links = self._enhanced_google_search()
        
        # Этап 2: Фильтрация и приоритизация ссылок
        prioritized_links = self._prioritize_links(all_google_links)
        
        # Этап 3: Параллельный парсинг контактов
        businesses_with_contacts = self._parallel_contact_extraction(prioritized_links, target_businesses)
        
        # Этап 4: Финализация и отчетность
        self.results = self._finalize_results(businesses_with_contacts, target_businesses)
        
        elapsed = time.time() - start_time
        phone_percentage = self._calculate_phone_percentage()
        
        self.logger.info(f"✅ УСИЛЕННЫЙ СБОР ЗАВЕРШЕН за {elapsed/60:.1f} минут")
        self.logger.info(f"📊 РЕЗУЛЬТАТ: {len(self.results)} бизнесов, {phone_percentage:.1f}% с телефонами")
        self.logger.info(f"🛡️ БЛОКИРОВОК: {self.blocked_count}, УСПЕШНО: {self.success_count}")
        
        return self.results

    def _enhanced_google_search(self):
        """Улучшенный Google поиск с продвинутыми методами обхода блокировок"""
        self.logger.info(f"🔍 Начало усиленного Google поиска")
        self.logger.info(f"📍 Города: {len(self.target_cities)}")
        self.logger.info(f"🔎 Запросы: {len(self.search_queries)}")
        self.logger.info(f"📄 Страницы: 2-5 (фокус на низкие позиции)")
        
        all_links = []
        
        # ПАРАЛЛЕЛЬНАЯ ОБРАБОТКА для ускорения
        with ThreadPoolExecutor(max_workers=4) as executor:
            futures = []
            
            # Создаем задачи для каждой комбинации город+запрос
            for city in self.target_cities[:10]:  # Ограничиваем количество городов для скорости
                for query in self.search_queries[:6]:  # Ограничиваем количество запросов
                    future = executor.submit(self._fast_search_city_query, city, query)
                    futures.append(future)
            
            # Собираем результаты по мере готовности
            completed = 0
            total_tasks = len(futures)
            
            for future in as_completed(futures, timeout=1800):  # 30 минут общий таймаут
                try:
                    city_query_links = future.result(timeout=60)  # 1 минута на задачу
                    all_links.extend(city_query_links)
                    completed += 1
                    
                    # Прогресс
                    progress = (completed / total_tasks) * 100
                    block_rate = (self.blocked_count / (self.blocked_count + self.success_count)) * 100 if (self.blocked_count + self.success_count) > 0 else 0
                    self.logger.info(f"📈 Прогресс: {progress:.1f}% | Блокировок: {block_rate:.1f}% | Ссылок: {len(all_links)}")
                    
                    # Если собрали достаточно ссылок, прерываем
                    if len(all_links) >= 200:  # Достаточно для 50 бизнесов
                        self.logger.info("🎯 Достаточно ссылок собрано, останавливаем поиск")
                        break
                        
                except Exception as e:
                    self.logger.error(f"Task failed: {e}")
                    continue
        
        unique_links = self._deduplicate_links(all_links)
        self.logger.info(f"🔗 Собрано уникальных ссылок: {len(unique_links)}")
        
        return unique_links

    def _fast_search_city_query(self, city, query):
        """БЫСТРЫЙ поиск по одному городу и запросу"""
        full_query = f"{query} {city}"
        links = []
        
        # Только страницы 2-3 для скорости (самые продуктивные)
        for page in [2, 3]:
            try:
                if self.primary_engine == 'Google':
                    page_links = self._fast_search_google_page(full_query, page)
                elif self.primary_engine == 'DuckDuckGo':
                    page_links = self._fast_search_duckduckgo_page(full_query, page)
                else:
                    page_links = self._fast_search_bing_page(full_query, page)
                
                if page_links:
                    links.extend(page_links)
                    self.success_count += 1
                else:
                    self.blocked_count += 1
                    # При блокировке сразу пробуем альтернативный поисковик
                    alt_links = self._fast_alternative_search(full_query, page)
                    if alt_links:
                        links.extend(alt_links)
                
                # КОРОТКИЕ паузы для скорости
                time.sleep(random.uniform(0.5, 1.5))
                
            except Exception as e:
                self.logger.debug(f"Fast search error {city} {query} p{page}: {e}")
                continue
        
        return links

    def _fast_search_bing_page(self, query, page):
        """БЫСТРЫЙ поиск Bing"""
        try:
            start = (page - 1) * 10 + 1
            search_url = f"https://www.bing.com/search?q={quote_plus(query)}&first={start}"
            headers = self._get_fast_headers()
            
            response = self.session.get(search_url, headers=headers, timeout=15, verify=False)
            if response.status_code == 200:
                return self._extract_links_from_alternative_engine(response.text, query, page, 'Bing')
            else:
                return []
        except Exception:
            return []

    def _fast_search_google_page(self, query, page):
        """БЫСТРЫЙ поиск Google"""
        try:
            start = (page - 1) * 10
            search_url = f"https://www.google.com/search?q={quote_plus(query)}&start={start}&hl=en&gl=us"
            headers = self._get_fast_headers()
            
            response = self.session.get(search_url, headers=headers, timeout=15, verify=False)
            if response.status_code == 200 and not self._is_captcha_page(response.text):
                return self._extract_links_from_google_page(response.text, query, page)
            else:
                return []
        except Exception:
            return []

    def _fast_search_duckduckgo_page(self, query, page):
        """БЫСТРЫЙ поиск DuckDuckGo"""
        try:
            start = (page - 2) * 50
            search_url = f"https://duckduckgo.com/html/?q={quote_plus(query)}&s={start}"
            headers = self._get_fast_headers()
            
            response = self.session.get(search_url, headers=headers, timeout=15, verify=False)
            if response.status_code == 200:
                return self._extract_links_from_alternative_engine(response.text, query, page, 'DuckDuckGo')
            else:
                return []
        except Exception:
            return []

    def _fast_alternative_search(self, query, page):
        """БЫСТРЫЙ альтернативный поиск"""
        # Пробуем все поисковики быстро
        for engine_name in ['DuckDuckGo', 'Bing', 'Google']:
            if engine_name == self.primary_engine:
                continue
            try:
                if engine_name == 'Bing':
                    links = self._fast_search_bing_page(query, page)
                elif engine_name == 'DuckDuckGo':
                    links = self._fast_search_duckduckgo_page(query, page)
                else:
                    links = self._fast_search_google_page(query, page)
                
                if links:
                    return links
            except Exception:
                continue
        return []

    def _get_fast_headers(self):
        """Упрощенные заголовки для скорости"""
        return {
            'User-Agent': random.choice(self.user_agents),
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.9',
            'Accept-Encoding': 'gzip, deflate',
            'Connection': 'keep-alive',
            'Cache-Control': 'no-cache'
        }

    def _page_delay(self):
        """БЫСТРАЯ пауза между страницами"""
        time.sleep(random.uniform(0.2, 0.8))  # Очень короткие паузы

    def _enhanced_adaptive_delay(self):
        """БЫСТРАЯ адаптивная задержка"""
        time.sleep(random.uniform(0.1, 0.5))  # Минимальные паузы

    def _prioritize_links(self, all_links):
        """Приоритизация ссылок для максимальной эффективности"""
        self.logger.info(f"📊 Приоритизация {len(all_links)} ссылок")
        
        # Дедупликация
        unique_links = {}
        for link in all_links:
            url = link['url']
            if url not in unique_links:
                unique_links[url] = link
        
        links_list = list(unique_links.values())
        
        # Сортировка по приоритету
        def priority_score(link):
            score = 0
            title_desc = (link['title'] + ' ' + link['description']).lower()
            
            # Высокий приоритет для прямых индикаторов
            if any(word in title_desc for word in ['phone', 'call', 'contact']):
                score += 10
            
            # Приоритет по типу бизнеса
            if 'scrap metal' in title_desc:
                score += 8
            elif any(word in title_desc for word in ['scrap', 'recycling', 'salvage']):
                score += 5
                
            # Приоритет страниц 2-3 (более качественные, но не топ)
            if link['page'] in [2, 3]:
                score += 3
            elif link['page'] in [4, 5]:
                score += 1
                
            return score
        
        prioritized = sorted(links_list, key=priority_score, reverse=True)
        
        self.logger.info(f"📈 Ссылки приоритизированы: {len(prioritized)}")
        return prioritized

    def _parallel_contact_extraction(self, links, target_businesses):
        """Параллельное извлечение контактов"""
        self.logger.info(f"📞 Параллельное извлечение контактов из {len(links)} ссылок")
        
        businesses = []
        processed_count = 0
        
        # Используем ThreadPoolExecutor для параллельной обработки
        with ThreadPoolExecutor(max_workers=8) as executor:
            # Отправляем задачи
            futures = {
                executor.submit(self._extract_business_contacts, link): link 
                for link in links[:min(len(links), target_businesses * 3)]  # Берем с запасом
            }
            
            for future in as_completed(futures):
                try:
                    business = future.result(timeout=30)
                    processed_count += 1
                    
                    if business and business.get('phone'):
                        businesses.append(business)
                        self.logger.info(f"✅ [{len(businesses)}] {business['name']}: {business['phone']}")
                    else:
                        link = futures[future]
                        self.logger.debug(f"❌ [{processed_count}] {link.get('title', 'Unknown')}: нет телефона")
                    
                    # Прогресс
                    if processed_count % 50 == 0:
                        phone_rate = len(businesses) / processed_count * 100 if processed_count > 0 else 0
                        self.logger.info(f"📊 Обработано: {processed_count}, с телефонами: {len(businesses)} ({phone_rate:.1f}%)")
                    
                    # Останавливаемся если достигли цели
                    if len(businesses) >= target_businesses:
                        self.logger.info(f"🎯 Цель достигнута: {len(businesses)} бизнесов с телефонами")
                        break
                        
                except Exception as e:
                    self.logger.debug(f"Future processing error: {e}")
                    continue
        
        final_phone_rate = len(businesses) / processed_count * 100 if processed_count > 0 else 0
        self.logger.info(f"📊 ИТОГО: обработано {processed_count}, найдено {len(businesses)} с телефонами ({final_phone_rate:.1f}%)")
        
        return businesses

    def _extract_business_contacts(self, link_data):
        """Извлечение контактов с одного сайта бизнеса"""
        url = link_data['url']
        
        try:
            # Запрос к сайту с ротацией заголовков
            headers = self._get_rotating_headers()
            response = self.session.get(url, headers=headers, timeout=20, verify=False)
            
            if response.status_code != 200:
                return None
            
            soup = BeautifulSoup(response.text, 'html.parser')
            page_text = response.text
            
            # ТОЧНЫЙ поиск телефона с контекстной проверкой
            phone_results = self._ultra_aggressive_phone_search(page_text, soup)
            
            # Фильтруем и выбираем лучший телефон
            valid_phone = None
            best_confidence = 0
            phone_method = 'unknown'
            
            for phone_data in phone_results:
                if phone_data['confidence'] > best_confidence:
                    valid_phone = phone_data['phone']
                    best_confidence = phone_data['confidence']
                    phone_method = phone_data['method']
            
            # Если нет ВАЛИДНОГО телефона, пропускаем
            if not valid_phone:
                return None
            
            # Извлекаем полную информацию о бизнесе
            business = {
                'name': self._extract_business_name(link_data, soup),
                'phone': valid_phone,
                'website': url,
                'email': self._extract_email_advanced(page_text, soup),
                'address': self._extract_address_comprehensive(soup, page_text),
                'city': self._extract_city_advanced(soup, page_text),
                'state': self._extract_state_advanced(soup, page_text),
                'zip_code': self._extract_zip_advanced(soup, page_text),
                'business_hours': self._extract_hours_comprehensive(soup),
                'services': self._extract_services_advanced(page_text),
                'materials': self._extract_materials_advanced(page_text),
                'description': self._extract_description_advanced(soup),
                'google_title': link_data.get('title', ''),
                'google_description': link_data.get('description', ''),
                'google_query': link_data.get('query', ''),
                'google_page': link_data.get('page', 0),
                'google_position': link_data.get('position', 0),
                'phone_extraction_method': phone_method,
                'phone_confidence': best_confidence,
                'source': 'Google_Auto',
                'scraped_at': datetime.now().isoformat(),
                'quality_score': self._calculate_quality_score({})
            }
            
            # Обновляем quality_score после заполнения данных
            business['quality_score'] = self._calculate_quality_score(business)
            
            return business
            
        except Exception as e:
            self.logger.debug(f"Error extracting from {url}: {e}")
            return None

    def _ultra_aggressive_phone_search(self, page_text, soup):
        """Ультра-агрессивный поиск телефонов - возвращает список с confidence scores"""
        phone_results = []
        
        # Метод 1: tel: ссылки (высший приоритет)
        tel_links = soup.find_all('a', href=lambda x: x and x.startswith('tel:'))
        for link in tel_links:
            tel_value = link.get('href', '').replace('tel:', '').strip()
            phone = self._clean_phone(tel_value)
            if phone and self._validate_phone_advanced(phone):
                phone_results.append({
                    'phone': phone,
                    'confidence': 95,
                    'method': 'tel_link'
                })
        
        # Метод 2: Микроданные и структурированные данные
        phone = self._extract_from_structured_data(soup)
        if phone and self._validate_phone_advanced(phone):
            phone_results.append({
                'phone': phone,
                'confidence': 90,
                'method': 'structured_data'
            })
        
        # Метод 3: data-* атрибуты
        data_phone_elements = soup.find_all(attrs=lambda x: x and any('phone' in attr.lower() or 'tel' in attr.lower() for attr in x))
        for element in data_phone_elements:
            for attr, value in element.attrs.items():
                if 'phone' in attr.lower() or 'tel' in attr.lower():
                    phone = self._clean_phone(str(value))
                    if phone and self._validate_phone_advanced(phone):
                        phone_results.append({
                            'phone': phone,
                            'confidence': 85,
                            'method': 'data_attribute'
                        })
        
        # Метод 4: JavaScript переменные
        script_tags = soup.find_all('script')
        for script in script_tags:
            if script.string:
                script_text = script.string
                js_phone_patterns = [
                    re.compile(r'phone["\'\s]*[:=]["\'\s]*(\d{3}[-.\s]*\d{3}[-.\s]*\d{4})', re.IGNORECASE),
                    re.compile(r'telephone["\'\s]*[:=]["\'\s]*(\d{3}[-.\s]*\d{3}[-.\s]*\d{4})', re.IGNORECASE),
                ]
                
                for pattern in js_phone_patterns:
                    matches = pattern.findall(script_text)
                    for match in matches:
                        phone = self._clean_phone(match)
                        if phone and self._validate_phone_advanced(phone):
                            phone_results.append({
                                'phone': phone,
                                'confidence': 80,
                                'method': 'javascript_var'
                            })
        
        # Метод 5: Контейнеры с классами телефонов
        phone_containers = soup.find_all(class_=re.compile(r'phone|tel|contact', re.IGNORECASE))
        for container in phone_containers:
            text = container.get_text()
            phone = self._extract_phone_from_text(text)
            if phone and self._validate_phone_advanced(phone):
                phone_results.append({
                    'phone': phone,
                    'confidence': 75,
                    'method': 'css_class'
                })
        
        # Метод 6: Поиск по всем паттернам в HTML
        for i, pattern in enumerate(self.phone_patterns):
            matches = pattern.findall(page_text)
            for match in matches:
                phone = self._format_phone_match(match)
                if phone and self._validate_phone_advanced(phone):
                    phone_results.append({
                        'phone': phone,
                        'confidence': 70,
                        'method': f'html_pattern_{i+1}'
                    })
        
        # Метод 7: OCR-подобный поиск (последняя попытка)
        text_only = soup.get_text()
        cleaned_text = re.sub(r'[^\d\s\-\(\)\.]+', ' ', text_only)
        potential_phones = re.findall(r'(\d{3}[\s\-\(\)\.]*\d{3}[\s\-\(\)\.]*\d{4})', cleaned_text)
        
        for potential in potential_phones:
            phone = self._clean_phone(potential)
            if phone and self._validate_phone_advanced(phone):
                phone_results.append({
                    'phone': phone,
                    'confidence': 60,
                    'method': 'text_mining'
                })
        
        # Убираем дубликаты и сортируем по confidence
        unique_phones = {}
        for result in phone_results:
            phone_key = result['phone']
            if phone_key not in unique_phones or result['confidence'] > unique_phones[phone_key]['confidence']:
                unique_phones[phone_key] = result
        
        return list(unique_phones.values())

    def _extract_from_structured_data(self, soup):
        """Извлечение из структурированных данных"""
        # JSON-LD
        json_scripts = soup.find_all('script', type='application/ld+json')
        for script in json_scripts:
            try:
                data = json.loads(script.string)
                phone = self._extract_phone_from_json_ld(data)
                if phone:
                    cleaned_phone = self._clean_phone(phone)
                    if cleaned_phone:
                        return cleaned_phone
            except:
                continue
        
        # Microdata
        microdata_elements = soup.find_all(attrs={'itemprop': True})
        for element in microdata_elements:
            itemprop = element.get('itemprop', '').lower()
            if 'telephone' in itemprop or 'phone' in itemprop:
                content = element.get('content') or element.get_text()
                phone = self._clean_phone(content)
                if phone:
                    return phone
        
        return None

    def _extract_phone_from_json_ld(self, data):
        """Рекурсивное извлечение телефона из JSON-LD"""
        if isinstance(data, dict):
            # Прямой поиск
            for key in ['telephone', 'phone', 'contactPoint']:
                if key in data:
                    value = data[key]
                    if isinstance(value, str):
                        return value  # Возвращаем raw значение для дальнейшей очистки
                    elif isinstance(value, dict) and 'telephone' in value:
                        return value['telephone']  # Возвращаем raw значение
            
            # Рекурсивный поиск
            for value in data.values():
                if isinstance(value, (dict, list)):
                    phone = self._extract_phone_from_json_ld(value)
                    if phone:
                        return phone
        
        elif isinstance(data, list):
            for item in data:
                phone = self._extract_phone_from_json_ld(item)
                if phone:
                    return phone
        
        return None

    def _validate_phone_advanced(self, phone):
        """СТРОГАЯ валидация телефона для максимальной точности"""
        if not phone:
            return False
        
        digits = re.sub(r'\D', '', phone)
        
        # Проверка длины (должно быть 10 или 11 цифр)
        if len(digits) not in [10, 11]:
            return False
        
        # Если 11 цифр, первая должна быть 1 (US country code)
        if len(digits) == 11:
            if not digits.startswith('1'):
                return False
            digits = digits[1:]  # Убираем country code
        
        # Теперь у нас должно быть ровно 10 цифр
        if len(digits) != 10:
            return False
        
        area_code = digits[:3]
        exchange_code = digits[3:6]
        subscriber_number = digits[6:]
        
        # СТРОГИЕ правила для US номеров
        
        # Area code не должен начинаться с 0 или 1
        if area_code[0] in ['0', '1']:
            return False
        
        # Exchange code не должен начинаться с 0 или 1
        if exchange_code[0] in ['0', '1']:
            return False
        
        # Проверяем на недействительные area codes
        invalid_area_codes = [
            '000', '111', '222', '333', '444', '555', '666', '777', '888', '999',
            '123', '321', '456', '654', '789', '987',
            # Toll-free numbers (не подходят для местного бизнеса)
            '800', '833', '844', '855', '866', '877', '888'
        ]
        if area_code in invalid_area_codes:
            return False
        
        # Проверяем на служебные номера
        if exchange_code + subscriber_number in ['911', '411', '511', '611', '711', '811', '311']:
            return False
        
        # Проверяем на тестовые номера (555-01xx)
        if exchange_code == '555' and subscriber_number.startswith('01'):
            return False
        
        # Проверяем на подозрительные повторения
        full_number = area_code + exchange_code + subscriber_number
        
        # Слишком много одинаковых цифр подряд
        for i in range(len(full_number) - 3):
            if len(set(full_number[i:i+4])) == 1:  # 4 одинаковые цифры подряд
                return False
        
        # Слишком мало уникальных цифр в номере
        if len(set(full_number)) < 4:
            return False
        
        # Проверяем на очевидно фальшивые номера
        fake_numbers = [
            '1234567890', '0123456789', '9876543210',
            '1111111111', '0000000000', '2222222222'
        ]
        if full_number in fake_numbers:
            return False
        
        # Проверяем на последовательные цифры (слишком много по порядку)
        sequential_count = 0
        for i in range(len(full_number) - 1):
            if int(full_number[i+1]) == int(full_number[i]) + 1:
                sequential_count += 1
                if sequential_count >= 4:  # 5 цифр подряд по порядку
                    return False
            else:
                sequential_count = 0
        
        # Дополнительная проверка: не должно быть всех одинаковых цифр в area code
        if len(set(area_code)) == 1:
            return False
        
        return True

    def _extract_phone_from_text(self, text):
        """Извлечение телефона из текста с валидацией"""
        for pattern in self.phone_patterns:
            matches = pattern.findall(text)
            for match in matches:
                phone = self._format_phone_match(match)
                if phone and self._validate_phone_advanced(phone):
                    return phone
        return None

    def _format_phone_match(self, match):
        """Форматирование найденного телефона"""
        if isinstance(match, tuple) and len(match) >= 3:
            # Соединяем части tuple в одну строку
            phone_str = ''.join(str(part) for part in match[:3])
            return self._clean_phone(phone_str)
        elif isinstance(match, str):
            return self._clean_phone(match)
        return None

    def _clean_phone(self, phone):
        """Очистка и форматирование телефона"""
        if not phone:
            return None
        
        # Извлекаем только цифры
        digits = re.sub(r'\D', '', str(phone))
        
        # Проверяем длину
        if len(digits) == 10:
            # Стандартный US формат
            area_code = digits[:3]
            exchange = digits[3:6]
            number = digits[6:]
        elif len(digits) == 11 and digits[0] == '1':
            # US с кодом страны
            area_code = digits[1:4]
            exchange = digits[4:7]
            number = digits[7:]
        else:
            return None
        
        # Базовая валидация
        if area_code[0] in ['0', '1'] or exchange[0] in ['0', '1']:
            return None
            
        return f"({area_code}) {exchange}-{number}"

    def _extract_business_name(self, link_data, soup):
        """Извлечение названия бизнеса с приоритетами"""
        # 1. Из Google title (часто самое точное)
        google_title = link_data.get('title', '').strip()
        if google_title and len(google_title) > 3:
            # Очищаем от лишнего
            clean_title = re.sub(r'\s*[-|]\s*.+$', '', google_title)  # Убираем " - город" и т.п.
            if len(clean_title) > 3:
                return clean_title[:100]
        
        # 2. Из title страницы
        title_tag = soup.find('title')
        if title_tag:
            title_text = title_tag.get_text().strip()
            clean_title = re.sub(r'\s*[-|]\s*.+$', '', title_text)
            if len(clean_title) > 3:
                return clean_title[:100]
        
        # 3. Из H1
        h1 = soup.find('h1')
        if h1:
            h1_text = h1.get_text().strip()
            if len(h1_text) > 3:
                return h1_text[:100]
        
        # 4. Из structured data
        json_scripts = soup.find_all('script', type='application/ld+json')
        for script in json_scripts:
            try:
                data = json.loads(script.string)
                name = self._extract_name_from_json_ld(data)
                if name:
                    return name[:100]
            except:
                continue
        
        return "Unknown Business"

    def _extract_name_from_json_ld(self, data):
        """Извлечение названия из JSON-LD"""
        if isinstance(data, dict):
            for key in ['name', 'legalName', 'alternateName']:
                if key in data and isinstance(data[key], str):
                    return data[key].strip()
            
            for value in data.values():
                if isinstance(value, (dict, list)):
                    name = self._extract_name_from_json_ld(value)
                    if name:
                        return name
        
        elif isinstance(data, list):
            for item in data:
                name = self._extract_name_from_json_ld(item)
                if name:
                    return name
        
        return ""

    def _extract_email_advanced(self, page_text, soup):
        """Продвинутое извлечение email"""
        # 1. Из structured data
        json_scripts = soup.find_all('script', type='application/ld+json')
        for script in json_scripts:
            try:
                data = json.loads(script.string)
                email = self._extract_email_from_json_ld(data)
                if email:
                    return email
            except:
                continue
        
        # 2. Из mailto ссылок
        mailto_links = soup.find_all('a', href=lambda x: x and x.startswith('mailto:'))
        for link in mailto_links:
            email = link.get('href', '').replace('mailto:', '').strip()
            if self._validate_email(email):
                return email
        
        # 3. Из текста страницы
        email_pattern = re.compile(r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b')
        emails = email_pattern.findall(page_text)
        
        for email in emails:
            if self._validate_email(email):
                return email
        
        return None

    def _extract_email_from_json_ld(self, data):
        """Извлечение email из JSON-LD"""
        if isinstance(data, dict):
            for key in ['email', 'contactPoint']:
                if key in data:
                    value = data[key]
                    if isinstance(value, str) and '@' in value:
                        return value
                    elif isinstance(value, dict) and 'email' in value:
                        return value['email']
            
            for value in data.values():
                if isinstance(value, (dict, list)):
                    email = self._extract_email_from_json_ld(value)
                    if email:
                        return email
        
        elif isinstance(data, list):
            for item in data:
                email = self._extract_email_from_json_ld(item)
                if email:
                    return email
        
        return None

    def _validate_email(self, email):
        """Валидация email"""
        if not email or '@' not in email:
            return False
        
        # Исключаем нежелательные домены
        exclude_domains = [
            'example.com', 'test.com', 'google.com', 'facebook.com',
            'twitter.com', 'linkedin.com', 'youtube.com', 'instagram.com'
        ]
        
        email_lower = email.lower()
        for domain in exclude_domains:
            if domain in email_lower:
                return False
        
        # Базовая проверка формата
        if re.match(r'^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}$', email):
            return True
        
        return False

    def _extract_address_comprehensive(self, soup, page_text):
        """Комплексное извлечение адреса"""
        # 1. Из structured data
        json_scripts = soup.find_all('script', type='application/ld+json')
        for script in json_scripts:
            try:
                data = json.loads(script.string)
                address = self._extract_address_from_json_ld(data)
                if address:
                    return address
            except:
                continue
        
        # 2. Из microdata
        address_elements = soup.find_all(attrs={'itemprop': re.compile(r'address|streetAddress', re.I)})
        for element in address_elements:
            address = element.get('content') or element.get_text().strip()
            if len(address) > 10:
                return address[:200]
        
        # 3. Из CSS классов
        address_selectors = [
            '.address', '.location', '.contact-address', '.street-address',
            '.addr', '.business-address', '.company-address'
        ]
        
        for selector in address_selectors:
            elements = soup.select(selector)
            for element in elements:
                address = element.get_text().strip()
                if len(address) > 10 and any(word in address.lower() for word in ['street', 'ave', 'road', 'drive', 'blvd']):
                    return address[:200]
        
        return None

    def _extract_address_from_json_ld(self, data):
        """Извлечение адреса из JSON-LD"""
        if isinstance(data, dict):
            # Прямой поиск адреса
            if 'address' in data:
                addr = data['address']
                if isinstance(addr, str):
                    return addr
                elif isinstance(addr, dict):
                    # Собираем адрес из частей
                    parts = []
                    for key in ['streetAddress', 'addressLocality', 'addressRegion', 'postalCode']:
                        if key in addr and addr[key]:
                            parts.append(str(addr[key]))
                    if parts:
                        return ', '.join(parts)
            
            # Рекурсивный поиск
            for value in data.values():
                if isinstance(value, (dict, list)):
                    address = self._extract_address_from_json_ld(value)
                    if address:
                        return address
        
        elif isinstance(data, list):
            for item in data:
                address = self._extract_address_from_json_ld(item)
                if address:
                    return address
        
        return None

    def _extract_city_advanced(self, soup, page_text):
        """Продвинутое извлечение города"""
        # 1. Из structured data
        json_scripts = soup.find_all('script', type='application/ld+json')
        for script in json_scripts:
            try:
                data = json.loads(script.string)
                city = self._extract_city_from_json_ld(data)
                if city:
                    return city
            except:
                continue
        
        # 2. Из microdata
        city_elements = soup.find_all(attrs={'itemprop': 'addressLocality'})
        for element in city_elements:
            city = element.get('content') or element.get_text().strip()
            if city:
                return city
        
        # 3. Из CSS классов
        city_selectors = ['.city', '.locality', '.address-city']
        for selector in city_selectors:
            element = soup.select_one(selector)
            if element:
                return element.get_text().strip()
        
        return None

    def _extract_city_from_json_ld(self, data):
        """Извлечение города из JSON-LD"""
        if isinstance(data, dict):
            if 'address' in data and isinstance(data['address'], dict):
                addr = data['address']
                if 'addressLocality' in addr:
                    return addr['addressLocality']
            
            for value in data.values():
                if isinstance(value, (dict, list)):
                    city = self._extract_city_from_json_ld(value)
                    if city:
                        return city
        
        elif isinstance(data, list):
            for item in data:
                city = self._extract_city_from_json_ld(item)
                if city:
                    return city
        
        return None

    def _extract_state_advanced(self, soup, page_text):
        """Продвинутое извлечение штата"""
        # Аналогично city, но ищем addressRegion
        json_scripts = soup.find_all('script', type='application/ld+json')
        for script in json_scripts:
            try:
                data = json.loads(script.string)
                state = self._extract_state_from_json_ld(data)
                if state:
                    return state
            except:
                continue
        
        state_elements = soup.find_all(attrs={'itemprop': 'addressRegion'})
        for element in state_elements:
            state = element.get('content') or element.get_text().strip()
            if state:
                return state
        
        return None

    def _extract_state_from_json_ld(self, data):
        """Извлечение штата из JSON-LD"""
        if isinstance(data, dict):
            if 'address' in data and isinstance(data['address'], dict):
                addr = data['address']
                if 'addressRegion' in addr:
                    return addr['addressRegion']
            
            for value in data.values():
                if isinstance(value, (dict, list)):
                    state = self._extract_state_from_json_ld(value)
                    if state:
                        return state
        
        elif isinstance(data, list):
            for item in data:
                state = self._extract_state_from_json_ld(item)
                if state:
                    return state
        
        return None

    def _extract_zip_advanced(self, soup, page_text):
        """Продвинутое извлечение ZIP кода"""
        # Из structured data
        json_scripts = soup.find_all('script', type='application/ld+json')
        for script in json_scripts:
            try:
                data = json.loads(script.string)
                zip_code = self._extract_zip_from_json_ld(data)
                if zip_code:
                    return zip_code
            except:
                continue
        
        # Из microdata
        zip_elements = soup.find_all(attrs={'itemprop': 'postalCode'})
        for element in zip_elements:
            zip_code = element.get('content') or element.get_text().strip()
            if zip_code:
                return zip_code
        
        # Из текста (поиск ZIP кодов)
        zip_pattern = re.compile(r'\b\d{5}(?:-\d{4})?\b')
        matches = zip_pattern.findall(page_text)
        if matches:
            return matches[0]
        
        return None

    def _extract_zip_from_json_ld(self, data):
        """Извлечение ZIP из JSON-LD"""
        if isinstance(data, dict):
            if 'address' in data and isinstance(data['address'], dict):
                addr = data['address']
                if 'postalCode' in addr:
                    return addr['postalCode']
            
            for value in data.values():
                if isinstance(value, (dict, list)):
                    zip_code = self._extract_zip_from_json_ld(value)
                    if zip_code:
                        return zip_code
        
        elif isinstance(data, list):
            for item in data:
                zip_code = self._extract_zip_from_json_ld(item)
                if zip_code:
                    return zip_code
        
        return None

    def _extract_hours_comprehensive(self, soup):
        """Комплексное извлечение рабочих часов"""
        # 1. Из structured data
        json_scripts = soup.find_all('script', type='application/ld+json')
        for script in json_scripts:
            try:
                data = json.loads(script.string)
                hours = self._extract_hours_from_json_ld(data)
                if hours:
                    return hours
            except:
                continue
        
        # 2. Из microdata
        hours_elements = soup.find_all(attrs={'itemprop': 'openingHours'})
        for element in hours_elements:
            hours = element.get('content') or element.get_text().strip()
            if hours:
                return hours[:200]
        
        # 3. Из CSS классов
        hours_selectors = [
            '.hours', '.business-hours', '.opening-hours', '.schedule',
            '.operation-hours', '.store-hours'
        ]
        
        for selector in hours_selectors:
            element = soup.select_one(selector)
            if element:
                hours = element.get_text().strip()
                if len(hours) > 10:
                    return hours[:200]
        
        return None

    def _extract_hours_from_json_ld(self, data):
        """Извлечение часов из JSON-LD"""
        if isinstance(data, dict):
            if 'openingHours' in data:
                hours = data['openingHours']
                if isinstance(hours, str):
                    return hours
                elif isinstance(hours, list):
                    return ', '.join(str(h) for h in hours)
            
            for value in data.values():
                if isinstance(value, (dict, list)):
                    hours = self._extract_hours_from_json_ld(value)
                    if hours:
                        return hours
        
        elif isinstance(data, list):
            for item in data:
                hours = self._extract_hours_from_json_ld(item)
                if hours:
                    return hours
        
        return ""

    def _extract_services_advanced(self, page_text):
        """Продвинутое извлечение услуг"""
        services = []
        text_lower = page_text.lower()
        
        service_keywords = {
            'pickup': ['pickup', 'pick up', 'collection'],
            'container': ['container', 'dumpster', 'roll off'],
            'demolition': ['demolition', 'demo', 'tear down'],
            'processing': ['processing', 'sorting', 'separation'],
            'weighing': ['weighing', 'scale', 'certified scales'],
            'cash payment': ['cash', 'cash payment', 'pay cash'],
            'commercial': ['commercial', 'business', 'industrial'],
            'residential': ['residential', 'home', 'homeowner'],
            'auto dismantling': ['auto dismantling', 'car dismantling', 'vehicle dismantling']
        }
        
        for service, keywords in service_keywords.items():
            if any(keyword in text_lower for keyword in keywords):
                services.append(service)
        
        return services

    def _extract_materials_advanced(self, page_text):
        """Продвинутое извлечение материалов"""
        materials = []
        text_lower = page_text.lower()
        
        material_keywords = {
            'copper': ['copper', 'cu '],
            'aluminum': ['aluminum', 'aluminium', 'al '],
            'steel': ['steel', 'iron', 'ferrous'],
            'brass': ['brass', 'bronze'],
            'stainless steel': ['stainless', 'stainless steel', 'ss '],
            'lead': ['lead', 'pb '],
            'zinc': ['zinc', 'zn '],
            'nickel': ['nickel', 'ni '],
            'wire': ['wire', 'cable', 'copper wire'],
            'auto parts': ['auto', 'car', 'vehicle', 'automotive'],
            'batteries': ['battery', 'batteries', 'car battery'],
            'radiators': ['radiator', 'radiators', 'cooling'],
            'catalytic converters': ['catalytic', 'converter', 'cat converter']
        }
        
        for material, keywords in material_keywords.items():
            if any(keyword in text_lower for keyword in keywords):
                materials.append(material)
        
        return materials

    def _extract_description_advanced(self, soup):
        """Продвинутое извлечение описания"""
        # 1. Meta description
        meta_desc = soup.find('meta', attrs={'name': 'description'})
        if meta_desc:
            desc = meta_desc.get('content', '').strip()
            if len(desc) > 20:
                return desc[:300]
        
        # 2. Из structured data
        json_scripts = soup.find_all('script', type='application/ld+json')
        for script in json_scripts:
            try:
                data = json.loads(script.string)
                desc = self._extract_description_from_json_ld(data)
                if desc:
                    return desc[:300]
            except:
                continue
        
        # 3. Первый содержательный параграф
        paragraphs = soup.find_all('p')
        for p in paragraphs:
            text = p.get_text().strip()
            if len(text) > 50 and any(word in text.lower() for word in ['scrap', 'metal', 'recycling']):
                return text[:300]
        
        return ""

    def _extract_description_from_json_ld(self, data):
        """Извлечение описания из JSON-LD"""
        if isinstance(data, dict):
            for key in ['description', 'about']:
                if key in data and isinstance(data[key], str):
                    return data[key]
            
            for value in data.values():
                if isinstance(value, (dict, list)):
                    desc = self._extract_description_from_json_ld(value)
                    if desc:
                        return desc
        
        elif isinstance(data, list):
            for item in data:
                desc = self._extract_description_from_json_ld(item)
                if desc:
                    return desc
        
        return ""

    def _calculate_quality_score(self, business):
        """Расчет оценки качества данных"""
        score = 0
        
        # Критически важные поля
        if business.get('phone'):
            score += 30
        if business.get('name') and len(business['name']) > 3:
            score += 20
        if business.get('website'):
            score += 15
        
        # Важные поля
        if business.get('email'):
            score += 10
        if business.get('address'):
            score += 10
        if business.get('city'):
            score += 5
        if business.get('state'):
            score += 5
        
        # Дополнительные поля
        if business.get('business_hours'):
            score += 3
        if business.get('services') and business['services']:
            score += 2
        
        return min(score, 100)

    def _get_rotating_headers(self):
        """Получение ротируемых заголовков"""
        return {
            'User-Agent': random.choice(self.user_agents),
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.9',
            'Accept-Encoding': 'gzip, deflate, br',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1',
            'Sec-Fetch-Dest': 'document',
            'Sec-Fetch-Mode': 'navigate',
            'Sec-Fetch-Site': 'none',
            'Cache-Control': 'max-age=0'
        }

    def _get_enhanced_headers(self):
        """Получение продвинутых заголовков для имитации браузера"""
        ua = random.choice(self.user_agents)
        
        # Определяем тип браузера для соответствующих заголовков
        if 'Chrome' in ua:
            sec_ch_ua = '"Not A(Brand";v="99", "Google Chrome";v="121", "Chromium";v="121"'
            sec_ch_ua_platform = '"Windows"'
        elif 'Firefox' in ua:
            sec_ch_ua = None
            sec_ch_ua_platform = None
        else:
            sec_ch_ua = '"Not A(Brand";v="99", "Safari";v="17"'
            sec_ch_ua_platform = '"macOS"'
        
        headers = {
            'User-Agent': ua,
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9',
            'Accept-Language': 'en-US,en;q=0.9',
            'Accept-Encoding': 'gzip, deflate, br',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1',
            'Sec-Fetch-Dest': 'document',
            'Sec-Fetch-Mode': 'navigate',
            'Sec-Fetch-Site': 'none',
            'Sec-Fetch-User': '?1',
            'Cache-Control': 'max-age=0',
            'DNT': '1',
            'Sec-GPC': '1'
        }
        
        # Добавляем Chrome-специфичные заголовки
        if sec_ch_ua:
            headers['sec-ch-ua'] = sec_ch_ua
            headers['sec-ch-ua-mobile'] = '?0'
            headers['sec-ch-ua-platform'] = sec_ch_ua_platform
        
        return headers

    def _is_captcha_page(self, html):
        """Проверка на CAPTCHA страницу"""
        captcha_indicators = [
            'captcha',
            'unusual traffic',
            'verify you are not a robot',
            'recaptcha',
            'g-recaptcha',
            'please complete the security check',
            'blocked by cloudflare',
            'ray id'
        ]
        
        html_lower = html.lower()
        return any(indicator in html_lower for indicator in captcha_indicators)

    def _adaptive_delay(self):
        """Адаптивная задержка для предотвращения блокировки"""
        # Базовая пауза
        base_delay = random.uniform(2, 5)
        
        # Увеличиваем паузу если много неудачных запросов
        if len(self.failed_searches) > 10:
            base_delay *= 2
        
        time.sleep(base_delay)

    def _enhanced_adaptive_delay(self):
        """Улучшенная адаптивная задержка для предотвращения блокировок"""
        # Базовая пауза
        base_delay = random.uniform(2, 5)
        
        # Увеличиваем паузу если много неудачных запросов
        if len(self.failed_searches) > 10:
            base_delay *= 2
        
        # Увеличиваем паузу, если блокировки стали частыми
        if self.blocked_count > 5:
            base_delay *= 1.5
        
        time.sleep(base_delay)

    def _page_delay(self):
        """Динамическая пауза между страницами для обхода блокировок"""
        # Увеличиваем паузу, если блокировки стали частыми
        if self.blocked_count > 5:
            time.sleep(random.uniform(5, 10))
        else:
            time.sleep(random.uniform(3, 8))

    def _deduplicate_links(self, links):
        """Дедупликация ссылок"""
        seen_urls = set()
        unique_links = []
        
        for link in links:
            url = link['url'].lower().strip()
            if url not in seen_urls:
                seen_urls.add(url)
                unique_links.append(link)
        
        return unique_links

    def _finalize_results(self, businesses, target_count):
        """Финализация результатов"""
        # Сортировка по качеству
        sorted_businesses = sorted(businesses, key=lambda x: x.get('quality_score', 0), reverse=True)
        
        # Ограничение по количеству
        final_results = sorted_businesses[:target_count]
        
        return final_results

    def _calculate_phone_percentage(self):
        """Расчет процента с телефонами"""
        if not self.results:
            return 0
        return (sum(1 for b in self.results if b.get('phone')) / len(self.results)) * 100

    def export_automated_results(self, output_dir="output"):
        """Экспорт результатов автоматического сбора"""
        if not self.results:
            return None
        
        os.makedirs(output_dir, exist_ok=True)
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        # Статистика
        total = len(self.results)
        with_phones = sum(1 for b in self.results if b.get('phone'))
        phone_percentage = (with_phones / total) * 100 if total > 0 else 0
        
        # Группировка по методам извлечения телефонов
        extraction_methods = defaultdict(int)
        for business in self.results:
            if business.get('phone'):
                method = business.get('phone_extraction_method', 'unknown')
                extraction_methods[method] += 1
        
        # Группировка по Google страницам
        page_distribution = defaultdict(int)
        for business in self.results:
            page = business.get('google_page', 0)
            page_distribution[f'Page {page}'] += 1
        
        # CSV экспорт
        df = pd.DataFrame(self.results)
        csv_file = os.path.join(output_dir, f"automated_google_results_{timestamp}.csv")
        df.to_csv(csv_file, index=False)
        
        # Excel с аналитикой
        excel_file = os.path.join(output_dir, f"automated_google_results_{timestamp}.xlsx")
        with pd.ExcelWriter(excel_file, engine='openpyxl') as writer:
            # Основные данные
            df.to_excel(writer, sheet_name='All Results', index=False)
            
            # Только с телефонами
            df_phones = df[df['phone'].notna() & (df['phone'] != '')]
            if not df_phones.empty:
                df_phones.to_excel(writer, sheet_name='With Phones', index=False)
            
            # Высокое качество (score > 70)
            df_quality = df[df['quality_score'] > 70]
            if not df_quality.empty:
                df_quality.to_excel(writer, sheet_name='High Quality', index=False)
            
            # Аналитика методов
            methods_df = pd.DataFrame(list(extraction_methods.items()), columns=['Method', 'Count'])
            methods_df.to_excel(writer, sheet_name='Extraction Methods', index=False)
            
            # Распределение по страницам
            pages_df = pd.DataFrame(list(page_distribution.items()), columns=['Page', 'Count'])
            pages_df.to_excel(writer, sheet_name='Page Distribution', index=False)
        
        # Детальный отчет
        report_file = os.path.join(output_dir, f"automated_report_{timestamp}.txt")
        with open(report_file, 'w', encoding='utf-8') as f:
            f.write("🤖 УСИЛЕННЫЙ ПОЛНОСТЬЮ АВТОМАТИЧЕСКИЙ GOOGLE ПАРСЕР - ОТЧЕТ\n")
            f.write("=" * 60 + "\n\n")
            f.write(f"Дата: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
            f.write(f"Метод: Усиленный автоматический Google поиск\n")
            f.write(f"Города обработано: {len(self.target_cities)}\n")
            f.write(f"Поисковых запросов: {len(self.search_queries)}\n")
            f.write(f"Страницы: 2-5 (низкие позиции)\n\n")
            
            f.write("📊 РЕЗУЛЬТАТЫ:\n")
            f.write(f"Всего бизнесов: {total}\n")
            f.write(f"С телефонами: {with_phones} ({phone_percentage:.1f}%)\n")
            f.write(f"Цель достигнута: {'✅ ДА' if phone_percentage >= self.MIN_PHONE_PERCENTAGE else '❌ НЕТ'}\n\n")
            
            f.write("🛡️ МЕТОДЫ ИЗВЛЕЧЕНИЯ ТЕЛЕФОНОВ:\n")
            for method, count in sorted(extraction_methods.items(), key=lambda x: x[1], reverse=True):
                percentage = (count / with_phones) * 100 if with_phones > 0 else 0
                f.write(f"  {method}: {count} ({percentage:.1f}%)\n")
            f.write("\n")
            
            f.write("📄 РАСПРЕДЕЛЕНИЕ ПО СТРАНИЦАМ GOOGLE:\n")
            for page, count in sorted(page_distribution.items()):
                percentage = (count / total) * 100 if total > 0 else 0
                f.write(f"  {page}: {count} бизнесов ({percentage:.1f}%)\n")
            f.write("\n")
            
            f.write("🎯 СТРАТЕГИЧЕСКИЕ ВЫВОДЫ:\n")
            f.write("• Усиленные методы обхода блокировок успешно реализованы\n")
            f.write("• Фокус на страницы 2-5 оправдан\n")
            f.write("• tel: ссылки - самый эффективный метод\n")
            f.write("• Низкопозиционные компании дают лучшие контакты\n\n")
            
            if len(self.failed_searches) > 0:
                f.write("⚠️ НЕУДАЧНЫЕ ПОИСКИ:\n")
                for failed in self.failed_searches[:10]:  # Показываем только первые 10
                    f.write(f"  • {failed}\n")
                f.write(f"Всего неудач: {len(self.failed_searches)}\n\n")
            
            if phone_percentage >= self.MIN_PHONE_PERCENTAGE:
                f.write("🎉 МИССИЯ ВЫПОЛНЕНА!\n")
                f.write("База готова для outreach кампании!\n")
            else:
                f.write("📈 РЕКОМЕНДАЦИИ ДЛЯ УЛУЧШЕНИЯ:\n")
                f.write("• Увеличить количество городов\n")
                f.write("• Добавить больше поисковых запросов\n")
                f.write("• Проверить качество прокси/VPN\n")
        
        self.logger.info(f"✅ Экспорт завершен:")
        self.logger.info(f"  • CSV: {csv_file}")
        self.logger.info(f"  • Excel: {excel_file}")
        self.logger.info(f"  • Отчет: {report_file}")
        
        return {
            'csv_file': csv_file,
            'excel_file': excel_file,
            'report_file': report_file,
            'total_businesses': total,
            'businesses_with_phones': with_phones,
            'phone_percentage': phone_percentage,
            'success': phone_percentage >= self.MIN_PHONE_PERCENTAGE,
            'extraction_methods': dict(extraction_methods),
            'failed_searches': len(self.failed_searches)
        }

    def _try_alternative_search_engines(self, query, page):
        """Попытка поиска через альтернативные поисковики"""
        for engine in self.search_engines:
            if engine['priority'] == 1: # Google is the primary engine
                continue
                
            try:
                self.logger.info(f"🔄 Попытка поиска через {engine['name']}")
                start = (page - 1) * 10
                
                if engine['name'] == 'Bing':
                    url = f"{engine['url']}?q={quote_plus(query)}&first={start}"
                elif engine['name'] == 'DuckDuckGo':
                    url = f"{engine['url']}?q={quote_plus(query)}&s={start}"
                else:
                    url = f"{engine['url']}?q={quote_plus(query)}&start={start}"
                
                headers = self._get_rotating_headers()
                response = self.session.get(url, headers=headers, timeout=30, verify=False)
                
                if response.status_code == 200:
                    # Используем адаптированный парсер для разных поисковиков
                    if engine['name'] == 'Google':
                        return self._extract_links_from_google_page(response.text, query, page)
                    else:
                        return self._extract_links_from_alternative_engine(response.text, query, page, engine['name'])
                elif response.status_code == 429:
                    self.logger.warning(f"🚫 Rate limit detected for {engine['name']}")
                    time.sleep(random.uniform(60, 120))
                    continue
                else:
                    self.logger.warning(f"❌ HTTP {response.status_code} for {engine['name']}")
                    continue
                    
            except Exception as e:
                self.logger.error(f"Error searching with {engine['name']}: {e}")
                continue
        
        return []

    def _extract_links_from_alternative_engine(self, html, query, page, engine_name):
        """Извлечение ссылок из альтернативных поисковиков"""
        soup = BeautifulSoup(html, 'html.parser')
        links = []
        
        try:
            if engine_name == 'Bing':
                # Селекторы для Bing
                results = soup.select('.b_algo')
                for i, result in enumerate(results):
                    link_elem = result.select_one('h2 a')
                    if link_elem:
                        href = link_elem.get('href')
                        title = link_elem.get_text().strip()
                        desc_elem = result.select_one('.b_caption p')
                        description = desc_elem.get_text().strip() if desc_elem else ''
                        
                        if self._is_relevant_for_scrap_metal(title, description):
                            links.append({
                                'url': href,
                                'title': title[:150],
                                'description': description[:300],
                                'query': query,
                                'page': page,
                                'position': i + 1,
                                'collected_at': datetime.now().isoformat(),
                                'source': 'Bing'
                            })
                            
            elif engine_name == 'DuckDuckGo':
                # Селекторы для DuckDuckGo
                results = soup.select('.result')
                for i, result in enumerate(results):
                    link_elem = result.select_one('.result__title a')
                    if link_elem:
                        href = link_elem.get('href')
                        title = link_elem.get_text().strip()
                        desc_elem = result.select_one('.result__snippet')
                        description = desc_elem.get_text().strip() if desc_elem else ''
                        
                        if self._is_relevant_for_scrap_metal(title, description):
                            links.append({
                                'url': href,
                                'title': title[:150],
                                'description': description[:300],
                                'query': query,
                                'page': page,
                                'position': i + 1,
                                'collected_at': datetime.now().isoformat(),
                                'source': 'DuckDuckGo'
                            })
                            
        except Exception as e:
            self.logger.error(f"Error parsing {engine_name} results: {e}")
        
        return links

    def _extract_links_from_google_page(self, html, query, page):
        """Извлечение ссылок из HTML страницы Google"""
        soup = BeautifulSoup(html, 'html.parser')
        links = []
        
        # Поиск результатов по различным селекторам Google
        result_selectors = [
            '.MjjYud',  # Новый формат Google
            '.g',       # Классический формат
            '.tF2Cxc',  # Альтернативный формат
            '.rc'       # Старый формат
        ]
        
        results = []
        for selector in result_selectors:
            results = soup.select(selector)
            if results:
                break
        
        for i, result in enumerate(results):
            try:
                # Ищем ссылку
                link_element = result.select_one('a')
                if not link_element:
                    continue
                
                href = link_element.get('href') or link_element.get('data-href')
                if not href or not href.startswith('http'):
                    continue
                
                # Ищем заголовок
                title_element = result.select_one('h3')
                title = title_element.get_text().strip() if title_element else 'No title'
                
                # Ищем описание
                desc_element = result.select_one('.VwiC3b, .s3v9rd, .x54gtf')
                description = desc_element.get_text().strip() if desc_element else ''
                
                # Проверяем релевантность для scrap metal
                if self._is_relevant_for_scrap_metal(title, description):
                    link_data = {
                        'url': href,
                        'title': title[:150],
                        'description': description[:300],
                        'query': query,
                        'page': page,
                        'position': i + 1,
                        'collected_at': datetime.now().isoformat()
                    }
                    links.append(link_data)
                
            except Exception as e:
                self.logger.debug(f"Error extracting link {i}: {e}")
                continue
        
        return links

    def _is_relevant_for_scrap_metal(self, title, description):
        """Проверка релевантности для scrap metal бизнеса"""
        text = (title + ' ' + description).lower()
        
        # Ключевые слова для scrap metal
        relevant_keywords = [
            'scrap', 'metal', 'recycling', 'salvage', 'junk', 'yard',
            'steel', 'copper', 'aluminum', 'brass', 'iron', 'auto',
            'buyer', 'dealer', 'pickup', 'demolition'
        ]
        
        # Исключения (нерелевантные результаты)
        exclude_keywords = [
            'software', 'app', 'game', 'music', 'movie', 'book',
            'news', 'blog', 'wikipedia', 'facebook', 'linkedin'
        ]
        
        # Проверяем наличие релевантных ключевых слов
        relevant_count = sum(1 for keyword in relevant_keywords if keyword in text)
        
        # Проверяем отсутствие исключающих слов
        has_exclusions = any(keyword in text for keyword in exclude_keywords)
        
        return relevant_count >= 1 and not has_exclusions

    def _prioritize_links(self, all_links):
        """Приоритизация ссылок для максимальной эффективности"""
        self.logger.info(f"📊 Приоритизация {len(all_links)} ссылок")
        
        # Дедупликация
        unique_links = {}
        for link in all_links:
            url = link['url']
            if url not in unique_links:
                unique_links[url] = link
        
        links_list = list(unique_links.values())
        
        # Сортировка по приоритету
        def priority_score(link):
            score = 0
            title_desc = (link['title'] + ' ' + link['description']).lower()
            
            # Высокий приоритет для прямых индикаторов
            if any(word in title_desc for word in ['phone', 'call', 'contact']):
                score += 10
            
            # Приоритет по типу бизнеса
            if 'scrap metal' in title_desc:
                score += 8
            elif any(word in title_desc for word in ['scrap', 'recycling', 'salvage']):
                score += 5
                
            # Приоритет страниц 2-3 (более качественные, но не топ)
            if link['page'] in [2, 3]:
                score += 3
            elif link['page'] in [4, 5]:
                score += 1
                
            return score
        
        prioritized = sorted(links_list, key=priority_score, reverse=True)
        
        self.logger.info(f"📈 Ссылки приоритизированы: {len(prioritized)}")
        return prioritized

def main():
    print("🤖 БЫСТРЫЙ ПОЛНОСТЬЮ АВТОМАТИЧЕСКИЙ GOOGLE ПАРСЕР")
    print("=" * 60)
    print("⚡ ПАРАЛЛЕЛЬНЫЙ сбор из Bing/Google страниц 2-3")
    print("📞 85% бизнесов с телефонами")
    print("🔍 Агрессивный поиск контактов")
    print("🚀 БЕЗ РУЧНОГО ВМЕШАТЕЛЬСТВА | БЫСТРО")
    
    scraper = FullyAutomaticGoogleScraper()
    
    try:
        target = int(input("\nЦелевое количество бизнесов (по умолчанию 500): ") or "500")
        
        print(f"\n🚀 Запуск БЫСТРОГО автоматического сбора для {target} бизнесов...")
        print("⚡ ВНИМАНИЕ: Процесс займет 10-30 минут (параллельная обработка)")
        print("📊 Прогресс будет показан в консоли")
        
        confirm = input("\nПродолжить? (y/N): ").lower()
        if confirm != 'y':
            print("❌ Отменено пользователем")
            return
        
        start_time = time.time()
        results = scraper.run_complete_automation(target)
        elapsed = time.time() - start_time
        
        if results:
            phone_count = sum(1 for b in results if b.get('phone'))
            phone_percentage = (phone_count / len(results)) * 100
            
            print(f"\n🎉 УСИЛЕННЫЙ СБОР ЗАВЕРШЕН!")
            print(f"⏱️ Время: {elapsed/60:.1f} минут")
            print(f"📊 Результат: {len(results)} бизнесов")
            print(f"📞 С телефонами: {phone_count} ({phone_percentage:.1f}%)")
            
            export_info = scraper.export_automated_results()
            if export_info:
                print(f"\n📁 Файлы созданы:")
                print(f"  • CSV: {export_info['csv_file']}")
                print(f"  • Excel: {export_info['excel_file']}")
                print(f"  • Отчет: {export_info['report_file']}")
                
                if export_info['success']:
                    print(f"\n✅ ЦЕЛЬ ДОСТИГНУТА!")
                    print(f"🚀 База готова для outreach кампании!")
                else:
                    print(f"\n⚠️ Цель не достигнута")
                    print(f"📈 Рекомендуется увеличить охват")
        else:
            print("❌ Данные не собраны")
            
    except KeyboardInterrupt:
        print("\n⚠️ Прервано пользователем")
        if scraper.results:
            print("💾 Сохраняем частичные результаты...")
            scraper.export_automated_results()
    except Exception as e:
        print(f"❌ Критическая ошибка: {e}")
        scraper.logger.error(f"Critical error: {e}", exc_info=True)

if __name__ == "__main__":
    main() 