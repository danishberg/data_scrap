#!/usr/bin/env python3
"""
Google Scraper Helper - Полуавтоматический помощник для парсинга выдачи Google
Адаптирует JavaScript-скрипт пользователя и автоматизирует поиск контактов
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
from urllib.parse import quote_plus, urljoin
import re
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

class GoogleScraperHelper:
    def __init__(self):
        self.session = requests.Session()
        self.logger = self._setup_logging()
        
        # Адаптированный JavaScript из пользовательского скрипта
        self.google_extraction_js = """
        // Адаптированная версия пользовательского скрипта
        function extractGoogleResults() {
            console.log('🔍 Извлекаем ссылки с Google страницы...');
            
            // Прокручиваем страницу вниз для загрузки всех результатов
            window.scrollTo(0, document.body.scrollHeight);
            
            var results = [];
            var processed = new Set();
            
            // Ищем все результаты поиска
            var resultElements = document.querySelectorAll('.MjjYud, .g, .rc');
            
            resultElements.forEach(function(item, index) {
                try {
                    // Ищем основную ссылку результата
                    var linkElement = item.querySelector('a[href]');
                    var titleElement = item.querySelector('h3');
                    var descElement = item.querySelector('.VwiC3b, .s3v9rd, .st');
                    
                    if (linkElement && titleElement) {
                        var href = linkElement.getAttribute('href') || linkElement.getAttribute('data-href');
                        var title = titleElement.innerText || titleElement.textContent;
                        var description = descElement ? (descElement.innerText || descElement.textContent) : '';
                        
                        // Очищаем ссылку от Google редиректов
                        if (href && href.startsWith('/url?q=')) {
                            href = decodeURIComponent(href.split('/url?q=')[1].split('&')[0]);
                        }
                        
                        // Проверяем валидность ссылки
                        if (href && href.startsWith('http') && !processed.has(href)) {
                            processed.add(href);
                            
                            results.push({
                                position: index + 1,
                                url: href,
                                title: title.trim(),
                                description: description.trim().substring(0, 300),
                                domain: new URL(href).hostname,
                                relevance_score: calculateRelevanceScore(title, description)
                            });
                        }
                    }
                } catch (e) {
                    console.log('Ошибка обработки элемента:', e);
                }
            });
            
            // Функция оценки релевантности
            function calculateRelevanceScore(title, description) {
                var text = (title + ' ' + description).toLowerCase();
                var metalKeywords = ['scrap', 'metal', 'recycling', 'salvage', 'junk', 'steel', 'copper', 'aluminum'];
                var score = 0;
                
                metalKeywords.forEach(function(keyword) {
                    if (text.includes(keyword)) score += 1;
                });
                
                return score;
            }
            
            // Сортируем по релевантности
            results.sort(function(a, b) {
                return b.relevance_score - a.relevance_score;
            });
            
            console.log('✅ Найдено результатов:', results.length);
            return results;
        }
        
        // Запускаем извлечение
        return extractGoogleResults();
        """
        
        # Поисковые запросы для разных типов скрап-бизнесов
        self.search_queries = [
            # Основные запросы
            'scrap metal recycling "{city}"',
            'metal scrap yard "{city}"',
            'junk yard "{city}"',
            'auto salvage "{city}"',
            'copper recycling "{city}"',
            'aluminum recycling "{city}"',
            'steel recycling "{city}"',
            
            # Длинные запросы (больше шансов найти низкопозиционные сайты)
            'where to sell scrap metal "{city}"',
            'metal recycling center near "{city}"',
            'cash for scrap metal "{city}"',
            'scrap metal prices "{city}"',
            'metal buyers "{city}"',
            'industrial metal recycling "{city}"',
            'construction metal recycling "{city}"',
            'automotive metal recycling "{city}"',
            
            # Региональные запросы
            'local scrap metal dealers "{city}"',
            'small scrap yards "{city}"',
            'family owned metal recycling "{city}"',
            'independent scrap metal "{city}"'
        ]

    def _setup_logging(self):
        logger = logging.getLogger('GoogleScraperHelper')
        logger.setLevel(logging.INFO)
        
        if not logger.handlers:
            handler = logging.StreamHandler()
            formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
            handler.setFormatter(formatter)
            logger.addHandler(handler)
        
        return logger

    def generate_google_urls(self, cities, max_pages=5):
        """Генерируем URL'ы для ручного парсинга в браузере"""
        self.logger.info(f"🔗 Генерируем Google URL'ы для {len(cities)} городов")
        
        urls_data = []
        
        for city in cities:
            city_urls = []
            
            for query_template in self.search_queries:
                query = query_template.format(city=city)
                
                # Генерируем URL'ы для разных страниц
                for page in range(0, max_pages):
                    start = page * 10
                    search_url = f"https://www.google.com/search?q={quote_plus(query)}&start={start}"
                    
                    city_urls.append({
                        'city': city,
                        'query': query,
                        'page': page + 1,
                        'url': search_url,
                        'expected_depth': 'deep' if page >= 2 else 'surface'
                    })
            
            urls_data.extend(city_urls)
        
        return urls_data

    def automated_google_scraping(self, cities, target_per_city=20):
        """Автоматизированный парсинг Google (осторожно!)"""
        self.logger.info(f"🤖 Автоматизированный Google парсинг для {len(cities)} городов")
        self.logger.warning("⚠️ Используйте VPN и будьте осторожны с частотой запросов!")
        
        all_results = []
        driver = self._setup_selenium_driver()
        
        try:
            for city in cities:
                city_results = []
                
                self.logger.info(f"🏙️ Парсинг для города: {city}")
                
                # Выбираем несколько лучших запросов для города
                selected_queries = self.search_queries[:5]  # Ограничиваем количество
                
                for query_template in selected_queries:
                    if len(city_results) >= target_per_city:
                        break
                    
                    query = query_template.format(city=city)
                    
                    try:
                        # Парсим глубокие страницы (2-4)
                        page_results = self._scrape_google_pages(driver, query, start_page=2, end_page=4)
                        city_results.extend(page_results)
                        
                        # Большая задержка между запросами
                        time.sleep(random.uniform(15, 30))
                        
                    except Exception as e:
                        self.logger.warning(f"Ошибка парсинга запроса '{query}': {e}")
                        continue
                
                all_results.extend(city_results[:target_per_city])
                self.logger.info(f"✅ {city}: собрано {len(city_results)} ссылок")
                
                # Большая пауза между городами
                time.sleep(random.uniform(60, 120))
        
        finally:
            driver.quit()
        
        return all_results

    def _setup_selenium_driver(self):
        """Настройка веб-драйвера с максимальной осторожностью"""
        options = Options()
        
        # Делаем браузер максимально похожим на обычный
        options.add_argument('--no-sandbox')
        options.add_argument('--disable-dev-shm-usage')
        options.add_argument('--disable-blink-features=AutomationControlled')
        options.add_experimental_option("excludeSwitches", ["enable-automation"])
        options.add_experimental_option('useAutomationExtension', False)
        
        # Рандомный User-Agent
        user_agents = [
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
        ]
        options.add_argument(f"user-agent={random.choice(user_agents)}")
        
        # Добавляем рандомные размеры окна
        window_sizes = ['1920,1080', '1366,768', '1536,864', '1280,720']
        selected_size = random.choice(window_sizes)
        options.add_argument(f"--window-size={selected_size}")
        
        driver = webdriver.Chrome(options=options)
        
        # Скрываем автоматизацию
        driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
        
        return driver

    def _scrape_google_pages(self, driver, query, start_page=2, end_page=4):
        """Парсим конкретные страницы Google для запроса"""
        results = []
        
        for page in range(start_page, end_page + 1):
            try:
                start = (page - 1) * 10
                search_url = f"https://www.google.com/search?q={quote_plus(query)}&start={start}"
                
                self.logger.info(f"📄 Страница {page}: {query}")
                driver.get(search_url)
                
                # Рандомная задержка для загрузки
                time.sleep(random.uniform(5, 10))
                
                # Прокручиваем страницу
                driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
                time.sleep(2)
                
                # Выполняем наш JavaScript для извлечения данных
                page_results = driver.execute_script(self.google_extraction_js)
                
                if page_results:
                    # Добавляем метаданные
                    for result in page_results:
                        result['search_query'] = query
                        result['google_page'] = page
                        result['scraped_at'] = datetime.now().isoformat()
                    
                    results.extend(page_results)
                    self.logger.info(f"  ✅ Найдено {len(page_results)} результатов")
                
                # Задержка между страницами
                time.sleep(random.uniform(8, 15))
                
            except Exception as e:
                self.logger.warning(f"Ошибка на странице {page}: {e}")
                continue
        
        return results

    def process_google_links(self, google_results, max_concurrent=5):
        """Обрабатываем собранные ссылки для поиска контактов"""
        self.logger.info(f"🔍 Обработка {len(google_results)} ссылок для поиска контактов")
        
        businesses = []
        
        for i, result in enumerate(google_results):
            try:
                self.logger.info(f"📄 Обработка {i+1}/{len(google_results)}: {result.get('domain', 'unknown')}")
                
                # Парсим сайт для поиска контактов
                business_data = self._extract_business_from_google_result(result)
                
                if business_data:
                    businesses.append(business_data)
                
                # Задержка между запросами
                time.sleep(random.uniform(2, 5))
                
            except Exception as e:
                self.logger.debug(f"Ошибка обработки {result.get('url', 'unknown')}: {e}")
                continue
        
        return businesses

    def _extract_business_from_google_result(self, google_result):
        """Извлекаем данные бизнеса из результата Google"""
        url = google_result.get('url', '')
        
        if not url:
            return None
        
        try:
            # Парсим страницу
            response = self._make_safe_request(url, timeout=15)
            if not response or response.status_code != 200:
                return None
            
            soup = BeautifulSoup(response.text, 'html.parser')
            
            # Агрессивный поиск контактов
            phone = self._extract_phone_aggressive(soup, response.text)
            email = self._extract_email_aggressive(soup, response.text)
            
            # Если нет контактов, пропускаем
            if not phone and not email:
                return None
            
            business = {
                'name': google_result.get('title', 'Unknown Business'),
                'description': google_result.get('description', ''),
                'website': url,
                'domain': google_result.get('domain', ''),
                'phone': phone,
                'email': email,
                'address': self._extract_address_from_page(soup),
                'google_position': google_result.get('position', 0),
                'google_page': google_result.get('google_page', 1),
                'google_query': google_result.get('search_query', ''),
                'relevance_score': google_result.get('relevance_score', 0),
                'source': 'Google_Deep_Search',
                'scraped_at': datetime.now().isoformat()
            }
            
            # Пытаемся извлечь город/штат из различных источников
            location_info = self._extract_location_info(soup, google_result)
            business.update(location_info)
            
            return business
            
        except Exception as e:
            self.logger.debug(f"Ошибка извлечения данных с {url}: {e}")
            return None

    def _extract_phone_aggressive(self, soup, page_text):
        """Агрессивное извлечение телефона"""
        # Паттерны для поиска телефонов
        phone_patterns = [
            r'tel:\+?1?[-.\s]?\(?(\d{3})\)?[-.\s]?(\d{3})[-.\s]?(\d{4})',
            r'phone[:\s]*\+?1?[-.\s]?\(?(\d{3})\)?[-.\s]?(\d{3})[-.\s]?(\d{4})',
            r'call[:\s]*\+?1?[-.\s]?\(?(\d{3})\)?[-.\s]?(\d{3})[-.\s]?(\d{4})',
            r'\+?1?[-.\s]?\(?(\d{3})\)?[-.\s]?(\d{3})[-.\s]?(\d{4})',
            r'(\d{3})[-.\s](\d{3})[-.\s](\d{4})',
            r'\((\d{3})\)\s*(\d{3})[-.\s](\d{4})',
        ]
        
        # Ищем в HTML атрибутах
        phone_elements = soup.select('a[href^="tel:"], [data-phone], .phone, #phone')
        for element in phone_elements:
            phone_text = element.get('href', '') or element.get('data-phone', '') or element.get_text()
            if phone_text:
                phone = self._clean_phone(phone_text.replace('tel:', ''))
                if phone:
                    return phone
        
        # Ищем в тексте страницы
        for pattern in phone_patterns:
            matches = re.findall(pattern, page_text, re.IGNORECASE)
            for match in matches:
                if isinstance(match, tuple) and len(match) == 3:
                    phone = f"({match[0]}) {match[1]}-{match[2]}"
                    return phone
        
        return ""

    def _extract_email_aggressive(self, soup, page_text):
        """Агрессивное извлечение email"""
        email_pattern = r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b'
        
        # Ищем в HTML
        email_elements = soup.select('a[href^="mailto:"], [data-email]')
        for element in email_elements:
            email_text = element.get('href', '').replace('mailto:', '') or element.get('data-email', '')
            if email_text and '@' in email_text:
                return email_text
        
        # Ищем в тексте
        emails = re.findall(email_pattern, page_text)
        
        # Фильтруем
        skip_domains = ['example.com', 'test.com', 'google.com', 'facebook.com']
        for email in emails:
            if not any(domain in email.lower() for domain in skip_domains):
                return email
        
        return ""

    def _extract_address_from_page(self, soup):
        """Извлечение адреса"""
        address_selectors = [
            '[itemtype*="PostalAddress"]',
            '.address', '.location', '.contact-address',
            '.street-address', '.business-address'
        ]
        
        for selector in address_selectors:
            element = soup.select_one(selector)
            if element:
                address = element.get_text().strip()
                if address and any(word in address.lower() for word in ['street', 'ave', 'road', 'drive', 'blvd']):
                    return address[:200]
        
        return ""

    def _extract_location_info(self, soup, google_result):
        """Извлечение информации о местоположении"""
        location = {}
        
        # Пытаемся извлечь из поискового запроса
        query = google_result.get('search_query', '')
        if '"' in query:
            # Извлекаем город из кавычек
            city_match = re.search(r'"([^"]+)"', query)
            if city_match:
                city_full = city_match.group(1)
                if ' ' in city_full:
                    parts = city_full.split()
                    location['city'] = ' '.join(parts[:-1])
                    location['state'] = parts[-1]
                else:
                    location['city'] = city_full
        
        # Пытаемся найти на странице
        if not location.get('city'):
            # Ищем в тексте страницы паттерны адресов
            page_text = soup.get_text()
            state_pattern = r'\b([A-Z]{2})\s+\d{5}'
            state_matches = re.findall(state_pattern, page_text)
            if state_matches:
                location['state'] = state_matches[0]
        
        return location

    def _clean_phone(self, phone):
        """Очистка телефона"""
        if not phone:
            return ""
        
        digits = re.sub(r'\D', '', phone)
        
        if len(digits) == 10:
            return f"({digits[:3]}) {digits[3:6]}-{digits[6:]}"
        elif len(digits) == 11 and digits[0] == '1':
            return f"({digits[1:4]}) {digits[4:7]}-{digits[7:]}"
        
        return ""

    def _make_safe_request(self, url, timeout=15, max_retries=3):
        """Безопасный HTTP запрос"""
        for attempt in range(max_retries):
            try:
                headers = {
                    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
                    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
                    'Accept-Language': 'en-US,en;q=0.9',
                    'Accept-Encoding': 'gzip, deflate',
                    'Connection': 'keep-alive'
                }
                
                time.sleep(random.uniform(1, 3))
                response = self.session.get(url, headers=headers, timeout=timeout)
                
                if response.status_code == 200:
                    return response
                
            except Exception as e:
                if attempt < max_retries - 1:
                    time.sleep(random.uniform(2, 5))
        
        return None

    def export_google_extraction_script(self, output_dir="output"):
        """Экспорт JavaScript для ручного использования в браузере"""
        os.makedirs(output_dir, exist_ok=True)
        
        # Создаем улучшенную версию скрипта пользователя
        enhanced_script = """
// УЛУЧШЕННЫЙ СКРИПТ ДЛЯ ИЗВЛЕЧЕНИЯ GOOGLE РЕЗУЛЬТАТОВ
// Запускайте в консоли браузера на странице результатов Google

javascript:!(function(){
    console.log('🔍 Запуск извлечения Google результатов...');
    
    // Прокручиваем вниз для загрузки всех результатов
    window.scrollTo(0, document.body.scrollHeight);
    
    var win = window.open('', 'GoogleResults', 'width=800,height=600,scrollbars=yes');
    win.document.write('<html><head><title>Google Results</title></head><body>');
    win.document.write('<h2>Собранные ссылки (' + new Date().toLocaleString() + ')</h2>');
    win.document.write('<p>Автоматически извлеченные ссылки с Google:</p>');
    
    var results = [];
    var processed = new Set();
    
    // Ищем все блоки результатов
    var resultElements = document.querySelectorAll('.MjjYud, .g, .rc');
    
    resultElements.forEach(function(item, index) {
        try {
            var linkElement = item.querySelector('a[href]');
            var titleElement = item.querySelector('h3');
            var descElement = item.querySelector('.VwiC3b, .s3v9rd, .st, .IsZvec');
            
            if (linkElement && titleElement) {
                var href = linkElement.getAttribute('href') || linkElement.getAttribute('data-href');
                var title = titleElement.innerText || titleElement.textContent;
                var description = descElement ? (descElement.innerText || descElement.textContent) : '';
                
                // Очищаем Google redirect
                if (href && href.startsWith('/url?q=')) {
                    href = decodeURIComponent(href.split('/url?q=')[1].split('&')[0]);
                }
                
                if (href && href.startsWith('http') && !processed.has(href)) {
                    processed.add(href);
                    
                    // Оценка релевантности
                    var text = (title + ' ' + description).toLowerCase();
                    var metalKeywords = ['scrap', 'metal', 'recycling', 'salvage', 'junk', 'steel', 'copper', 'aluminum'];
                    var relevanceScore = 0;
                    metalKeywords.forEach(function(keyword) {
                        if (text.includes(keyword)) relevanceScore++;
                    });
                    
                    results.push({
                        position: index + 1,
                        url: href,
                        title: title.trim(),
                        description: description.trim().substring(0, 200),
                        domain: new URL(href).hostname,
                        relevance: relevanceScore
                    });
                    
                    // Добавляем в окно
                    win.document.write('<div style="margin: 10px 0; padding: 10px; border: 1px solid #ddd;">');
                    win.document.write('<strong>Позиция ' + (index + 1) + ':</strong> ' + title + '<br>');
                    win.document.write('<a href="' + href + '" target="_blank">' + href + '</a><br>');
                    win.document.write('<small>' + description.substring(0, 150) + '...</small><br>');
                    win.document.write('<em>Релевантность: ' + relevanceScore + '/8</em>');
                    win.document.write('</div>');
                }
            }
        } catch (e) {
            console.log('Ошибка:', e);
        }
    });
    
    // Сортируем по релевантности
    results.sort(function(a, b) { return b.relevance - a.relevance; });
    
    // Добавляем JSON для экспорта
    win.document.write('<hr><h3>JSON для экспорта:</h3>');
    win.document.write('<textarea rows="10" cols="80">' + JSON.stringify(results, null, 2) + '</textarea>');
    win.document.write('<hr><p>Всего найдено ссылок: ' + results.length + '</p>');
    win.document.write('</body></html>');
    
    console.log('✅ Найдено результатов:', results.length);
    console.log('📊 Данные отображены в новом окне');
    
    return results;
})();
        """
        
        # Сохраняем скрипт
        script_file = os.path.join(output_dir, f"google_extraction_script_{datetime.now().strftime('%Y%m%d_%H%M%S')}.js")
        with open(script_file, 'w', encoding='utf-8') as f:
            f.write(enhanced_script)
        
        # Создаем инструкцию
        instruction_file = os.path.join(output_dir, f"google_scraping_instructions_{datetime.now().strftime('%Y%m%d_%H%M%S')}.txt")
        with open(instruction_file, 'w', encoding='utf-8') as f:
            f.write("🔍 ИНСТРУКЦИЯ ПО ИСПОЛЬЗОВАНИЮ GOOGLE SCRAPER\n")
            f.write("=" * 60 + "\n\n")
            f.write("1. ПОДГОТОВКА:\n")
            f.write("   • Используйте VPN для безопасности\n")
            f.write("   • Откройте браузер в режиме инкогнито\n")
            f.write("   • Очистите cookies Google\n\n")
            
            f.write("2. ПОИСК В GOOGLE:\n")
            f.write("   • Введите запрос: scrap metal recycling \"ваш_город\"\n")
            f.write("   • Перейдите на страницы 2-5 (именно там компании с низкими позициями)\n")
            f.write("   • Загрузите все результаты на странице\n\n")
            
            f.write("3. ЗАПУСК СКРИПТА:\n")
            f.write("   • Откройте Developer Tools (F12)\n")
            f.write("   • Перейдите на вкладку Console\n")
            f.write("   • Скопируйте и вставьте весь JavaScript код\n")
            f.write("   • Нажмите Enter\n\n")
            
            f.write("4. РЕЗУЛЬТАТ:\n")
            f.write("   • Откроется новое окно с собранными ссылками\n")
            f.write("   • Скопируйте JSON данные для дальнейшей обработки\n")
            f.write("   • Сохраните данные в файл\n\n")
            
            f.write("5. РЕКОМЕНДАЦИИ:\n")
            f.write("   • Делайте паузы между запросами (5-10 минут)\n")
            f.write("   • Фокусируйтесь на страницах 2-5 Google\n")
            f.write("   • Ищите в средних городах (меньше конкуренции)\n")
            f.write("   • Проверяйте релевантность результатов\n\n")
            
            f.write("6. ОБРАБОТКА ДАННЫХ:\n")
            f.write("   • Используйте полученный JSON с нашим парсером\n")
            f.write("   • Парсер автоматически извлечет контакты с сайтов\n")
            f.write("   • Приоритет отдается компаниям с контактами\n")
        
        self.logger.info(f"✅ Google extraction script exported:")
        self.logger.info(f"  • Script: {script_file}")
        self.logger.info(f"  • Instructions: {instruction_file}")
        
        return {
            'script_file': script_file,
            'instruction_file': instruction_file
        }

def main():
    print("🔍 GOOGLE SCRAPER HELPER - Полуавтоматический помощник")
    print("=" * 65)
    print("Инструменты для парсинга глубинных результатов Google")
    
    helper = GoogleScraperHelper()
    
    print("\nВыберите режим работы:")
    print("1. Создать JavaScript для ручного использования")
    print("2. Автоматический парсинг (осторожно!)")
    print("3. Обработать готовые данные Google")
    
    choice = input("\nВведите номер (1-3): ").strip()
    
    if choice == "1":
        # Экспорт JavaScript скрипта
        result = helper.export_google_extraction_script()
        print(f"\n✅ JavaScript скрипт создан:")
        print(f"  • {result['script_file']}")
        print(f"  • {result['instruction_file']}")
        print("\n📖 Следуйте инструкциям в файле для безопасного использования")
        
    elif choice == "2":
        # Автоматический парсинг
        print("\n⚠️ ВНИМАНИЕ: Автоматический парсинг Google может привести к блокировке!")
        confirm = input("Продолжить? (y/N): ").strip().lower()
        
        if confirm == 'y':
            cities = input("Введите города через запятую: ").split(',')
            cities = [city.strip() for city in cities if city.strip()]
            
            if cities:
                results = helper.automated_google_scraping(cities, target_per_city=10)
                print(f"\n✅ Собрано {len(results)} результатов Google")
                
                # Обрабатываем ссылки
                businesses = helper.process_google_links(results)
                print(f"📞 Найдено {len(businesses)} бизнесов с контактами")
        
    elif choice == "3":
        # Обработка готовых данных
        json_file = input("Введите путь к JSON файлу с Google данными: ").strip()
        
        if os.path.exists(json_file):
            with open(json_file, 'r') as f:
                google_data = json.load(f)
            
            businesses = helper.process_google_links(google_data)
            print(f"\n📞 Обработано: {len(businesses)} бизнесов с контактами")
            
            # Экспорт результатов
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            output_file = f"google_processed_businesses_{timestamp}.json"
            
            with open(output_file, 'w') as f:
                json.dump(businesses, f, indent=2, default=str)
            
            print(f"✅ Результаты сохранены: {output_file}")
        else:
            print("❌ Файл не найден")
    
    else:
        print("❌ Неверный выбор")

if __name__ == "__main__":
    main() 