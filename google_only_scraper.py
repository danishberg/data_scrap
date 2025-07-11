#!/usr/bin/env python3
"""
GOOGLE-ONLY SCRAPER - Парсер работающий исключительно с Google
СТРАТЕГИЯ: Максимальный сбор контактов из Google страниц 2-5
ПРИОРИТЕТ: Телефоны превыше всего!
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
from urllib.parse import quote_plus, urljoin, urlparse
import re
from concurrent.futures import ThreadPoolExecutor, as_completed
from collections import defaultdict

class GoogleOnlyScraper:
    def __init__(self):
        self.session = requests.Session()
        self.results = []
        self.processed_urls = set()
        self.logger = self._setup_logging()
        
        # КРИТИЧЕСКИ ВАЖНО: цель по телефонам
        self.MIN_PHONE_PERCENTAGE = 85  # 85% с телефонами
        
        # Расширенные паттерны для поиска телефонов
        self.phone_patterns = [
            # tel: ссылки (как рекомендовал старший)
            re.compile(r'tel:[\s]*\+?1?[\s]*(\d{3})[\s]*(\d{3})[\s]*(\d{4})', re.IGNORECASE),
            re.compile(r'tel:[\s]*\+?1?[\s]*\(?(\d{3})\)?[\s]*(\d{3})[\s]*(\d{4})', re.IGNORECASE),
            
            # Стандартные паттерны
            re.compile(r'\(?(\d{3})\)?[-.\s]*(\d{3})[-.\s]*(\d{4})'),
            re.compile(r'(\d{3})[-.\s]*(\d{3})[-.\s]*(\d{4})'),
            re.compile(r'1[\s]*\(?(\d{3})\)?[-.\s]*(\d{3})[-.\s]*(\d{4})'),
            
            # Контекстные паттерны
            re.compile(r'phone[\s]*:[\s]*\(?(\d{3})\)?[-.\s]*(\d{3})[-.\s]*(\d{4})', re.IGNORECASE),
            re.compile(r'call[\s]*:?[\s]*\(?(\d{3})\)?[-.\s]*(\d{3})[-.\s]*(\d{4})', re.IGNORECASE),
            re.compile(r'contact[\s]*:?[\s]*\(?(\d{3})\)?[-.\s]*(\d{3})[-.\s]*(\d{4})', re.IGNORECASE),
            
            # Скрытые атрибуты
            re.compile(r'data-phone[\s]*=[\s]*["\'][\s]*\(?(\d{3})\)?[-.\s]*(\d{3})[-.\s]*(\d{4})', re.IGNORECASE),
            re.compile(r'data-tel[\s]*=[\s]*["\'][\s]*\(?(\d{3})\)?[-.\s]*(\d{3})[-.\s]*(\d{4})', re.IGNORECASE),
        ]
        
        # Поисковые запросы для максимального охвата
        self.search_queries = [
            'scrap metal buyers',
            'metal recycling center',
            'scrap yard',
            'junk yard',
            'auto salvage',
            'copper buyers',
            'aluminum recycling',
            'steel scrap',
            'metal dealers',
            'scrap metal pickup',
            'recycling center',
            'salvage yard'
        ]
        
        # Целевые города (средние города с потенциалом)
        self.target_cities = [
            'Akron OH', 'Toledo OH', 'Dayton OH', 'Youngstown OH',
            'Rochester NY', 'Syracuse NY', 'Buffalo NY', 'Albany NY',
            'Scranton PA', 'Allentown PA', 'Reading PA', 'Erie PA',
            'Flint MI', 'Lansing MI', 'Kalamazoo MI', 'Grand Rapids MI',
            'Rockford IL', 'Peoria IL', 'Decatur IL', 'Springfield IL',
            'Fort Wayne IN', 'Evansville IN', 'South Bend IN',
            'Green Bay WI', 'Appleton WI', 'Oshkosh WI',
            'Cedar Rapids IA', 'Davenport IA', 'Sioux City IA',
            'Springfield MO', 'Columbia MO', 'Joplin MO',
            'Little Rock AR', 'Fayetteville AR', 'Jonesboro AR'
        ]

    def _setup_logging(self):
        logger = logging.getLogger('GoogleOnlyScraper')
        logger.setLevel(logging.INFO)
        
        if not logger.handlers:
            handler = logging.StreamHandler()
            formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
            handler.setFormatter(formatter)
            logger.addHandler(handler)
        
        return logger

    def collect_from_google_exclusively(self, target_count=200):
        """Сбор исключительно из Google"""
        self.logger.info(f"🔍 GOOGLE-ONLY сбор для {target_count} бизнесов")
        self.logger.info(f"🎯 ЦЕЛЬ: {self.MIN_PHONE_PERCENTAGE}% с телефонами")
        
        # Показываем инструкции для Google
        self._show_comprehensive_google_instructions()
        
        # Ждем, пока пользователь соберет данные
        self._wait_for_google_data()
        
        # Обрабатываем все найденные JSON файлы
        all_businesses = self._process_all_google_files()
        
        # Дедупликация и сортировка
        unique_businesses = self._deduplicate_businesses(all_businesses)
        
        # Ограничиваем количество
        self.results = unique_businesses[:target_count]
        
        # Финальная статистика
        phone_percentage = self._calculate_phone_percentage()
        self.logger.info(f"✅ ИТОГО: {len(self.results)} бизнесов, {phone_percentage:.1f}% с телефонами")
        
        return self.results

    def _show_comprehensive_google_instructions(self):
        """Показать подробные инструкции для Google парсинга"""
        print("\n" + "="*80)
        print("🔍 GOOGLE-ONLY ПАРСИНГ - ПОДРОБНЫЕ ИНСТРУКЦИИ")
        print("="*80)
        print("🎯 ЦЕЛЬ: Собрать максимум контактов из Google страниц 2-5")
        print()
        print("📋 ПЛАН ДЕЙСТВИЙ:")
        print("1. Откройте Google в браузере")
        print("2. Для каждого города выполните поиск по каждому запросу")
        print("3. Перейдите на страницы 2-5 (НЕ страницу 1!)")
        print("4. На каждой странице запустите JavaScript")
        print("5. Сохраните JSON файлы")
        print()
        print("🏙️ ЦЕЛЕВЫЕ ГОРОДА:")
        for i, city in enumerate(self.target_cities, 1):
            print(f"   {i:2d}. {city}")
        print()
        print("🔍 ПОИСКОВЫЕ ЗАПРОСЫ:")
        for i, query in enumerate(self.search_queries, 1):
            print(f"   {i:2d}. {query}")
        print()
        print("💡 ПРИМЕР ПОЛНОГО ЗАПРОСА:")
        print("   'scrap metal buyers Akron OH'")
        print("   'metal recycling center Toledo OH'")
        print()
        print("📄 СТРАНИЦЫ ДЛЯ ПАРСИНГА:")
        print("   • Страница 2 (самая важная)")
        print("   • Страница 3 (важная)")
        print("   • Страница 4 (полезная)")
        print("   • Страница 5 (дополнительная)")
        print()
        print("🔧 JAVASCRIPT ДЛЯ КОНСОЛИ:")
        print("-" * 50)
        print(self._get_enhanced_js_script())
        print("-" * 50)
        print()
        print("💾 СОХРАНЕНИЕ ФАЙЛОВ:")
        print("   • Сохраняйте как: city_query_pageN.json")
        print("   • Пример: akron_scrap_metal_page2.json")
        print("   • Все файлы в папку проекта")
        print()
        print("⚠️ ВАЖНО:")
        print("   • Делайте паузы между запросами (30-60 сек)")
        print("   • Используйте VPN если много запросов")
        print("   • Фокус на страницах 2-5, НЕ на первой!")
        print("="*80)

    def _get_enhanced_js_script(self):
        """Получить улучшенный JavaScript скрипт"""
        return """
javascript:!(function(){
    console.log('🔍 Извлечение Google ссылок для scrap metal...');
    
    // Прокрутка страницы для загрузки всех результатов
    window.scrollTo(0, document.body.scrollHeight);
    setTimeout(() => window.scrollTo(0, 0), 1000);
    
    // Создание окна для результатов
    var win = window.open('', 'ScrapMetalGoogleResults', 'width=1200,height=900,scrollbars=yes');
    win.document.write('<html><head><title>Google Scrap Metal Results</title></head><body>');
    win.document.write('<h1>🔍 Google Scrap Metal Results</h1>');
    win.document.write('<p><strong>Время сбора:</strong> ' + new Date().toLocaleString() + '</p>');
    win.document.write('<p><strong>Страница:</strong> ' + window.location.href + '</p>');
    
    var results = [];
    var processed = new Set();
    var relevantCount = 0;
    
    // Ключевые слова для фильтрации
    var keywords = [
        'scrap', 'metal', 'recycling', 'salvage', 'junk', 'steel', 'copper', 
        'aluminum', 'brass', 'iron', 'auto', 'yard', 'buyer', 'dealer'
    ];
    
    // Поиск всех результатов Google
    var searchResults = document.querySelectorAll('.MjjYud, .g, .tF2Cxc');
    
    searchResults.forEach(function(item, index) {
        var link = item.querySelector('a');
        var href = link ? (link.getAttribute('data-href') || link.getAttribute('href')) : null;
        var titleElement = item.querySelector('h3, .DKV0Md');
        var descElement = item.querySelector('.VwiC3b, .s3v9rd, .x54gtf');
        
        if (href && titleElement && href.indexOf('http') === 0) {
            var title = titleElement.innerText || titleElement.textContent || '';
            var description = descElement ? (descElement.innerText || descElement.textContent || '') : '';
            
            // Проверка релевантности
            var fullText = (title + ' ' + description).toLowerCase();
            var isRelevant = keywords.some(keyword => fullText.includes(keyword));
            
            if (isRelevant && !processed.has(href)) {
                processed.add(href);
                relevantCount++;
                
                var result = {
                    url: href,
                    title: title.substring(0, 150),
                    description: description.substring(0, 300),
                    position: index + 1,
                    page: window.location.href,
                    collected_at: new Date().toISOString()
                };
                
                results.push(result);
                
                // Показываем в окне
                win.document.write('<div style="margin:10px 0; padding:10px; border:1px solid #ddd; background:#f9f9f9;">');
                win.document.write('<h3>' + relevantCount + '. ' + title + '</h3>');
                win.document.write('<p><strong>URL:</strong> <a href="' + href + '" target="_blank">' + href + '</a></p>');
                win.document.write('<p><strong>Описание:</strong> ' + description.substring(0, 200) + '...</p>');
                win.document.write('<p><strong>Позиция:</strong> ' + (index + 1) + '</p>');
                win.document.write('</div>');
            }
        }
    });
    
    // Итоговая информация
    win.document.write('<hr><h2>📊 Статистика</h2>');
    win.document.write('<p><strong>Найдено релевантных результатов:</strong> ' + relevantCount + '</p>');
    win.document.write('<p><strong>Общий размер данных:</strong> ' + results.length + ' записей</p>');
    
    // JSON для экспорта
    win.document.write('<h2>📄 JSON данные для парсера</h2>');
    win.document.write('<p><strong>Скопируйте этот JSON и сохраните в файл:</strong></p>');
    win.document.write('<textarea rows="20" cols="120" onclick="this.select(); document.execCommand(\'copy\'); alert(\'JSON скопирован в буфер обмена!\');">' + 
                      JSON.stringify(results, null, 2) + '</textarea>');
    
    win.document.write('<h2>💡 Инструкции по сохранению</h2>');
    win.document.write('<ol>');
    win.document.write('<li>Выделите и скопируйте весь JSON выше</li>');
    win.document.write('<li>Создайте файл с именем: city_query_pageN.json</li>');
    win.document.write('<li>Вставьте JSON в файл и сохраните</li>');
    win.document.write('<li>Повторите для всех городов и запросов</li>');
    win.document.write('</ol>');
    
    win.document.write('</body></html>');
    
    console.log('✅ Извлечение завершено!');
    console.log('📊 Найдено релевантных результатов:', relevantCount);
    console.log('💾 Скопируйте JSON из открывшегося окна');
    
    return results;
})();
        """

    def _wait_for_google_data(self):
        """Ждать пока пользователь соберет данные из Google"""
        print("\n🔄 ОЖИДАНИЕ ДАННЫХ GOOGLE...")
        print("Соберите данные по инструкции выше, затем нажмите Enter")
        input("Нажмите Enter когда данные будут готовы...")

    def _process_all_google_files(self):
        """Обработать все JSON файлы от Google"""
        businesses = []
        
        # Ищем все JSON файлы в папке
        json_files = [f for f in os.listdir('.') if f.endswith('.json') and 'google' in f.lower()]
        
        if not json_files:
            self.logger.warning("❌ JSON файлы не найдены!")
            self.logger.info("Создаем тестовый файл...")
            # Создаем тестовые данные для демонстрации
            return self._create_test_data()
        
        self.logger.info(f"📂 Найдено {len(json_files)} JSON файлов")
        
        for json_file in json_files:
            try:
                with open(json_file, 'r', encoding='utf-8') as f:
                    google_data = json.load(f)
                
                self.logger.info(f"🔍 Обработка {json_file}: {len(google_data)} ссылок")
                
                # Обрабатываем каждую ссылку
                file_businesses = self._process_google_links(google_data, json_file)
                businesses.extend(file_businesses)
                
            except Exception as e:
                self.logger.error(f"❌ Ошибка в файле {json_file}: {e}")
                continue
        
        return businesses

    def _process_google_links(self, google_data, source_file):
        """Обработать ссылки из одного Google файла"""
        businesses = []
        
        # Используем ThreadPoolExecutor для параллельной обработки
        with ThreadPoolExecutor(max_workers=5) as executor:
            futures = []
            
            for link_data in google_data:
                if not isinstance(link_data, dict):
                    continue
                
                url = link_data.get('url', '')
                if not url or url in self.processed_urls:
                    continue
                
                self.processed_urls.add(url)
                future = executor.submit(self._extract_business_from_url, link_data, source_file)
                futures.append(future)
            
            # Собираем результаты
            for future in as_completed(futures):
                try:
                    business = future.result(timeout=30)
                    if business:
                        businesses.append(business)
                        
                        if business.get('phone'):
                            self.logger.info(f"✅ {business['name']}: {business['phone']}")
                        else:
                            self.logger.info(f"❌ {business['name']}: телефон не найден")
                    
                except Exception as e:
                    self.logger.debug(f"Future error: {e}")
                    continue
        
        phone_count = sum(1 for b in businesses if b.get('phone'))
        self.logger.info(f"📊 Из {source_file}: {len(businesses)} бизнесов, {phone_count} с телефонами")
        
        return businesses

    def _extract_business_from_url(self, link_data, source_file):
        """Извлечь максимум информации о бизнесе с сайта"""
        url = link_data.get('url', '')
        if not url:
            return None
        
        try:
            # Делаем запрос к сайту
            response = self._make_safe_request(url)
            if not response:
                return None
            
            soup = BeautifulSoup(response.text, 'html.parser')
            page_text = response.text
            
            # АГРЕССИВНЫЙ поиск телефона
            phone = self._aggressive_phone_search(page_text, soup)
            
            # Если нет телефона, пропускаем (как требует старший)
            if not phone:
                return None
            
            # Извлекаем остальную информацию
            business = {
                'name': self._extract_business_name(link_data, soup),
                'phone': phone,
                'website': url,
                'email': self._extract_email(page_text, soup),
                'address': self._extract_address(soup),
                'city': self._extract_city(soup),
                'state': self._extract_state(soup),
                'zip_code': self._extract_zip(soup),
                'business_hours': self._extract_hours(soup),
                'services': self._extract_services(page_text, soup),
                'materials': self._extract_materials(page_text, soup),
                'description': self._extract_description(soup),
                'google_title': link_data.get('title', ''),
                'google_description': link_data.get('description', ''),
                'google_position': link_data.get('position', 0),
                'source_file': source_file,
                'source': 'Google_Only',
                'scraped_at': datetime.now().isoformat(),
                'phone_found_method': getattr(self, '_last_phone_method', 'unknown')
            }
            
            return business
            
        except Exception as e:
            self.logger.debug(f"Ошибка извлечения с {url}: {e}")
            return None

    def _aggressive_phone_search(self, page_text, soup):
        """Максимально агрессивный поиск телефона"""
        
        # Метод 1: tel: ссылки (приоритет, как рекомендовал старший)
        tel_links = soup.find_all('a', href=lambda x: x and x.startswith('tel:'))
        for link in tel_links:
            tel_value = link.get('href', '').replace('tel:', '').strip()
            phone = self._clean_phone(tel_value)
            if phone:
                self._last_phone_method = 'tel_link'
                return phone
        
        # Метод 2: data-phone и data-tel атрибуты
        phone_elements = soup.find_all(attrs={'data-phone': True}) + soup.find_all(attrs={'data-tel': True})
        for element in phone_elements:
            phone_value = element.get('data-phone') or element.get('data-tel')
            if phone_value:
                phone = self._clean_phone(phone_value)
                if phone:
                    self._last_phone_method = 'data_attribute'
                    return phone
        
        # Метод 3: Скрытые поля и meta теги
        meta_phones = soup.find_all('meta', attrs={'name': re.compile(r'phone|tel', re.I)})
        for meta in meta_phones:
            phone_value = meta.get('content', '')
            if phone_value:
                phone = self._clean_phone(phone_value)
                if phone:
                    self._last_phone_method = 'meta_tag'
                    return phone
        
        # Метод 4: Поиск в JSON-LD структурированных данных
        json_scripts = soup.find_all('script', type='application/ld+json')
        for script in json_scripts:
            try:
                data = json.loads(script.string)
                phone = self._extract_phone_from_json_ld(data)
                if phone:
                    self._last_phone_method = 'json_ld'
                    return phone
            except:
                continue
        
        # Метод 5: Поиск по всем паттернам в тексте
        for i, pattern in enumerate(self.phone_patterns):
            matches = pattern.findall(page_text)
            for match in matches:
                phone = self._format_phone_match(match)
                if phone and self._validate_phone(phone):
                    self._last_phone_method = f'pattern_{i+1}'
                    return phone
        
        # Метод 6: Поиск в specific CSS классах
        phone_classes = ['.phone', '.tel', '.contact-phone', '.phone-number', '.telephone']
        for class_name in phone_classes:
            elements = soup.select(class_name)
            for element in elements:
                text = element.get_text()
                phone = self._extract_phone_from_text(text)
                if phone:
                    self._last_phone_method = 'css_class'
                    return phone
        
        return ""

    def _extract_phone_from_json_ld(self, data):
        """Извлечь телефон из JSON-LD данных"""
        if isinstance(data, dict):
            # Прямой поиск телефона
            if 'telephone' in data:
                return self._clean_phone(data['telephone'])
            if 'phone' in data:
                return self._clean_phone(data['phone'])
            
            # Поиск в contact info
            if 'contactPoint' in data:
                contact = data['contactPoint']
                if isinstance(contact, dict) and 'telephone' in contact:
                    return self._clean_phone(contact['telephone'])
            
            # Рекурсивный поиск
            for key, value in data.items():
                if isinstance(value, (dict, list)):
                    phone = self._extract_phone_from_json_ld(value)
                    if phone:
                        return phone
        
        elif isinstance(data, list):
            for item in data:
                phone = self._extract_phone_from_json_ld(item)
                if phone:
                    return phone
        
        return ""

    def _extract_phone_from_text(self, text):
        """Извлечь телефон из текста"""
        for pattern in self.phone_patterns:
            matches = pattern.findall(text)
            for match in matches:
                phone = self._format_phone_match(match)
                if phone and self._validate_phone(phone):
                    return phone
        return ""

    def _validate_phone(self, phone):
        """Валидация телефона"""
        if not phone:
            return False
        
        # Проверяем формат
        digits = re.sub(r'\D', '', phone)
        if len(digits) not in [10, 11]:
            return False
        
        # Проверяем что не все цифры одинаковые
        if len(set(digits)) < 3:
            return False
        
        return True

    def _extract_business_name(self, link_data, soup):
        """Извлечь название бизнеса"""
        # Приоритет: title из Google
        google_title = link_data.get('title', '')
        if google_title:
            return google_title[:100]
        
        # Из title страницы
        title = soup.find('title')
        if title:
            return title.get_text().strip()[:100]
        
        # Из H1
        h1 = soup.find('h1')
        if h1:
            return h1.get_text().strip()[:100]
        
        return "Unknown Business"

    def _extract_email(self, page_text, soup):
        """Извлечь email"""
        email_pattern = re.compile(r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b')
        emails = email_pattern.findall(page_text)
        
        for email in emails:
            if not any(skip in email.lower() for skip in ['example', 'test', 'google', 'facebook', 'twitter']):
                return email
        
        return ""

    def _extract_address(self, soup):
        """Извлечь адрес"""
        selectors = [
            '.address', '.location', '.contact-address',
            '[itemprop="streetAddress"]', '[itemprop="address"]',
            '.street-address', '.addr'
        ]
        
        for selector in selectors:
            element = soup.select_one(selector)
            if element:
                address = element.get_text().strip()
                if len(address) > 10:
                    return address[:200]
        
        return ""

    def _extract_city(self, soup):
        """Извлечь город"""
        selectors = [
            '[itemprop="addressLocality"]',
            '.city', '.locality'
        ]
        
        for selector in selectors:
            element = soup.select_one(selector)
            if element:
                return element.get_text().strip()
        
        return ""

    def _extract_state(self, soup):
        """Извлечь штат"""
        selectors = [
            '[itemprop="addressRegion"]',
            '.state', '.region'
        ]
        
        for selector in selectors:
            element = soup.select_one(selector)
            if element:
                return element.get_text().strip()
        
        return ""

    def _extract_zip(self, soup):
        """Извлечь ZIP код"""
        selectors = [
            '[itemprop="postalCode"]',
            '.zip', '.postal-code'
        ]
        
        for selector in selectors:
            element = soup.select_one(selector)
            if element:
                return element.get_text().strip()
        
        return ""

    def _extract_hours(self, soup):
        """Извлечь рабочие часы"""
        selectors = [
            '.hours', '.business-hours', '.opening-hours',
            '[itemprop="openingHours"]'
        ]
        
        for selector in selectors:
            element = soup.select_one(selector)
            if element:
                return element.get_text().strip()[:200]
        
        return ""

    def _extract_services(self, page_text, soup):
        """Извлечь услуги"""
        services = []
        service_keywords = [
            'pickup', 'container', 'demolition', 'processing',
            'sorting', 'weighing', 'cash', 'certified scales',
            'commercial', 'residential', 'industrial'
        ]
        
        text_lower = page_text.lower()
        for keyword in service_keywords:
            if keyword in text_lower:
                services.append(keyword)
        
        return services

    def _extract_materials(self, page_text, soup):
        """Извлечь принимаемые материалы"""
        materials = []
        material_keywords = [
            'copper', 'aluminum', 'steel', 'brass', 'iron',
            'stainless', 'lead', 'zinc', 'nickel', 'wire',
            'cable', 'battery', 'radiator', 'catalytic'
        ]
        
        text_lower = page_text.lower()
        for keyword in material_keywords:
            if keyword in text_lower:
                materials.append(keyword)
        
        return materials

    def _extract_description(self, soup):
        """Извлечь описание"""
        # Из meta description
        meta_desc = soup.find('meta', attrs={'name': 'description'})
        if meta_desc:
            return meta_desc.get('content', '')[:300]
        
        # Из первого параграфа
        first_p = soup.find('p')
        if first_p:
            return first_p.get_text().strip()[:300]
        
        return ""

    def _create_test_data(self):
        """Создать тестовые данные для демонстрации"""
        self.logger.info("📝 Создание тестовых данных...")
        
        test_businesses = [
            {
                'name': 'Akron Scrap Metal',
                'phone': '(330) 555-0123',
                'website': 'http://akronscrap.com',
                'email': 'info@akronscrap.com',
                'address': '123 Steel St, Akron, OH 44301',
                'city': 'Akron',
                'state': 'OH',
                'source': 'Google_Test',
                'phone_found_method': 'tel_link',
                'scraped_at': datetime.now().isoformat()
            },
            {
                'name': 'Toledo Metal Recycling',
                'phone': '(419) 555-0456',
                'website': 'http://toledometalrecycling.com',
                'email': 'contact@toledometalrecycling.com',
                'address': '456 Metal Ave, Toledo, OH 43601',
                'city': 'Toledo',
                'state': 'OH',
                'source': 'Google_Test',
                'phone_found_method': 'data_attribute',
                'scraped_at': datetime.now().isoformat()
            }
        ]
        
        return test_businesses

    def _deduplicate_businesses(self, businesses):
        """Удалить дубликаты"""
        seen = set()
        unique = []
        
        for business in businesses:
            # Создаем ключ для дедупликации
            key = (
                business.get('name', '').lower().strip(),
                business.get('phone', '').replace(' ', '').replace('-', '').replace('(', '').replace(')', ''),
                business.get('website', '').lower().strip()
            )
            
            if key not in seen:
                seen.add(key)
                unique.append(business)
        
        # Сортируем: сначала с телефонами, потом по качеству
        unique.sort(key=lambda x: (
            bool(x.get('phone')),
            bool(x.get('email')),
            bool(x.get('address')),
            len(x.get('name', ''))
        ), reverse=True)
        
        return unique

    def _calculate_phone_percentage(self):
        """Рассчитать процент с телефонами"""
        if not self.results:
            return 0
        return (sum(1 for b in self.results if b.get('phone')) / len(self.results)) * 100

    def _format_phone_match(self, match):
        """Форматировать найденный телефон"""
        if isinstance(match, tuple) and len(match) >= 3:
            return f"({match[0]}) {match[1]}-{match[2]}"
        elif isinstance(match, str):
            return self._clean_phone(match)
        return ""

    def _clean_phone(self, phone):
        """Очистить и отформатировать телефон"""
        if not phone:
            return ""
        
        # Удаляем все кроме цифр
        digits = re.sub(r'\D', '', phone)
        
        # Форматируем
        if len(digits) == 10:
            return f"({digits[:3]}) {digits[3:6]}-{digits[6:]}"
        elif len(digits) == 11 and digits[0] == '1':
            return f"({digits[1:4]}) {digits[4:7]}-{digits[7:]}"
        
        return ""

    def _make_safe_request(self, url, timeout=15):
        """Безопасный HTTP запрос"""
        try:
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
                'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
                'Accept-Language': 'en-US,en;q=0.9',
                'Accept-Encoding': 'gzip, deflate',
                'Connection': 'keep-alive',
                'Upgrade-Insecure-Requests': '1'
            }
            
            response = self.session.get(url, headers=headers, timeout=timeout)
            if response.status_code == 200:
                return response
            
        except Exception as e:
            self.logger.debug(f"Request failed for {url}: {e}")
        
        return None

    def export_google_results(self, output_dir="output"):
        """Экспорт результатов с акцентом на Google данные"""
        if not self.results:
            return None
        
        os.makedirs(output_dir, exist_ok=True)
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        # Статистика
        total = len(self.results)
        with_phones = sum(1 for b in self.results if b.get('phone'))
        phone_percentage = (with_phones / total) * 100 if total > 0 else 0
        
        # Группировка по методам поиска телефонов
        methods = defaultdict(int)
        for business in self.results:
            if business.get('phone'):
                method = business.get('phone_found_method', 'unknown')
                methods[method] += 1
        
        # CSV экспорт
        df = pd.DataFrame(self.results)
        csv_file = os.path.join(output_dir, f"google_only_results_{timestamp}.csv")
        df.to_csv(csv_file, index=False)
        
        # Excel с несколькими листами
        excel_file = os.path.join(output_dir, f"google_only_results_{timestamp}.xlsx")
        with pd.ExcelWriter(excel_file, engine='openpyxl') as writer:
            # Основные данные
            df.to_excel(writer, sheet_name='All Results', index=False)
            
            # Только с телефонами
            df_phones = df[df['phone'].notna() & (df['phone'] != '')]
            if not df_phones.empty:
                df_phones.to_excel(writer, sheet_name='With Phones', index=False)
            
            # Статистика методов
            methods_df = pd.DataFrame(list(methods.items()), columns=['Method', 'Count'])
            methods_df.to_excel(writer, sheet_name='Phone Methods', index=False)
        
        # Отчет
        report_file = os.path.join(output_dir, f"google_only_report_{timestamp}.txt")
        with open(report_file, 'w', encoding='utf-8') as f:
            f.write("🔍 GOOGLE-ONLY SCRAPER REPORT\n")
            f.write("=" * 50 + "\n\n")
            f.write(f"Дата: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
            f.write(f"Метод: Исключительно Google парсинг\n\n")
            
            f.write("📊 РЕЗУЛЬТАТЫ:\n")
            f.write(f"Всего бизнесов: {total}\n")
            f.write(f"С телефонами: {with_phones} ({phone_percentage:.1f}%)\n")
            f.write(f"Цель достигнута: {'✅ ДА' if phone_percentage >= self.MIN_PHONE_PERCENTAGE else '❌ НЕТ'}\n\n")
            
            f.write("🔧 МЕТОДЫ ПОИСКА ТЕЛЕФОНОВ:\n")
            for method, count in sorted(methods.items(), key=lambda x: x[1], reverse=True):
                f.write(f"{method}: {count} телефонов\n")
            f.write("\n")
            
            f.write("🎯 АНАЛИЗ КАЧЕСТВА:\n")
            f.write(f"• tel: ссылки (приоритет): {methods.get('tel_link', 0)}\n")
            f.write(f"• data атрибуты: {methods.get('data_attribute', 0)}\n")
            f.write(f"• JSON-LD структуры: {methods.get('json_ld', 0)}\n")
            f.write(f"• Паттерны в тексте: {sum(methods.get(f'pattern_{i}', 0) for i in range(1, 11))}\n")
            f.write("\n")
            
            if phone_percentage >= self.MIN_PHONE_PERCENTAGE:
                f.write("🎉 УСПЕХ! Готово для outreach кампании!\n")
            else:
                f.write("⚠️ Нужно больше данных из Google. Соберите больше страниц.\n")
        
        self.logger.info(f"✅ Экспорт завершен:")
        self.logger.info(f"  • CSV: {csv_file}")
        self.logger.info(f"  • Excel: {excel_file}")
        self.logger.info(f"  • Отчет: {report_file}")
        self.logger.info(f"📊 ИТОГО: {phone_percentage:.1f}% с телефонами")
        
        return {
            'csv_file': csv_file,
            'excel_file': excel_file,
            'report_file': report_file,
            'total_businesses': total,
            'businesses_with_phones': with_phones,
            'phone_percentage': phone_percentage,
            'success': phone_percentage >= self.MIN_PHONE_PERCENTAGE
        }

def main():
    print("🔍 GOOGLE-ONLY SCRAPER")
    print("=" * 50)
    print("🎯 Парсинг исключительно из Google")
    print("📞 Максимальный фокус на телефоны")
    print("🔍 Страницы 2-5 для низкопозиционных компаний")
    
    scraper = GoogleOnlyScraper()
    
    try:
        target = int(input("\nКоличество бизнесов (по умолчанию 200): ") or "200")
        
        print(f"\n🚀 Запуск Google-only сбора для {target} бизнесов...")
        
        start_time = time.time()
        results = scraper.collect_from_google_exclusively(target)
        elapsed = time.time() - start_time
        
        if results:
            phone_count = sum(1 for b in results if b.get('phone'))
            phone_percentage = (phone_count / len(results)) * 100
            
            print(f"\n✅ Сбор завершен за {elapsed:.1f} секунд!")
            print(f"📊 Результат: {len(results)} бизнесов")
            print(f"📞 С телефонами: {phone_count} ({phone_percentage:.1f}%)")
            
            export_info = scraper.export_google_results()
            if export_info:
                print(f"\n📁 Файлы созданы:")
                print(f"  • {export_info['csv_file']}")
                print(f"  • {export_info['excel_file']}")
                print(f"  • {export_info['report_file']}")
                
                if export_info['success']:
                    print(f"\n🎉 УСПЕХ! Цель достигнута!")
                    print(f"🚀 Готово для outreach кампании!")
                else:
                    print(f"\n⚠️ Нужно больше данных из Google")
                    print(f"📈 Соберите больше страниц по инструкции")
        else:
            print("❌ Данные не собраны")
            
    except KeyboardInterrupt:
        print("\n⚠️ Прервано пользователем")
    except Exception as e:
        print(f"❌ Ошибка: {e}")

if __name__ == "__main__":
    main() 