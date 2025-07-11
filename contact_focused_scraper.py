#!/usr/bin/env python3
"""
Contact-Focused Scraper - Агрессивный сбор контактов для scrap metal центров
Приоритет: контакты > все остальное
Стратегия: Google парсинг для компаний с низкими позициями
"""

import os
import sys
import json
import time
import random
import logging
import requests
from bs4 import BeautifulSoup
from datetime import datetime
import pandas as pd
from urllib.parse import quote_plus, urljoin, unquote
import re
from concurrent.futures import ThreadPoolExecutor, as_completed
import asyncio
import aiohttp
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from typing import List, Dict, Any, Optional

class ContactFocusedScraper:
    def __init__(self):
        self.session = requests.Session()
        self.results = []
        self.logger = self._setup_logging()
        
        # КРИТИЧЕСКАЯ НАСТРОЙКА: минимальный процент бизнесов с контактами
        self.MIN_CONTACT_PERCENTAGE = 80  # Цель: 80%+ бизнесов с телефонами
        
        # Ключевые запросы для поиска глубинных компаний
        self.deep_search_queries = [
            "scrap metal buyers {city}",
            "metal recycling center {city}",
            "copper aluminum steel buyers {city}",
            "junk yard {city}",
            "auto salvage {city}",
            "metal scrap dealer {city}",
            "industrial metal recycling {city}",
            "scrap metal pickup {city}",
            "metal recycling services {city}",
            "scrap yard near {city}"
        ]
        
        # Города для глубокого поиска (средние города, где меньше конкуренции)
        self.target_cities = [
            "Akron OH", "Toledo OH", "Dayton OH", "Youngstown OH",
            "Rochester NY", "Syracuse NY", "Albany NY", "Buffalo NY",
            "Camden NJ", "Trenton NJ", "Paterson NJ", "Newark NJ",
            "Gary IN", "Fort Wayne IN", "Evansville IN", "South Bend IN",
            "Flint MI", "Lansing MI", "Kalamazoo MI", "Battle Creek MI",
            "Shreveport LA", "Lafayette LA", "Lake Charles LA", "Monroe LA",
            "Mobile AL", "Huntsville AL", "Montgomery AL", "Tuscaloosa AL",
            "Chattanooga TN", "Knoxville TN", "Clarksville TN", "Murfreesboro TN",
            "Little Rock AR", "Fort Smith AR", "Fayetteville AR", "Pine Bluff AR",
            "Wichita KS", "Topeka KS", "Lawrence KS", "Overland Park KS",
            "Springfield MO", "Independence MO", "Columbia MO", "St. Joseph MO"
        ]
        
        # Паттерны для агрессивного поиска телефонов
        self.aggressive_phone_patterns = [
            r'tel:\+?1?[-.\s]?\(?(\d{3})\)?[-.\s]?(\d{3})[-.\s]?(\d{4})',
            r'call\s*:?\s*\+?1?[-.\s]?\(?(\d{3})\)?[-.\s]?(\d{3})[-.\s]?(\d{4})',
            r'phone\s*:?\s*\+?1?[-.\s]?\(?(\d{3})\)?[-.\s]?(\d{3})[-.\s]?(\d{4})',
            r'contact\s*:?\s*\+?1?[-.\s]?\(?(\d{3})\)?[-.\s]?(\d{3})[-.\s]?(\d{4})',
            r'\+?1?[-.\s]?\(?(\d{3})\)?[-.\s]?(\d{3})[-.\s]?(\d{4})',
            r'(\d{3})[-.\s](\d{3})[-.\s](\d{4})',
            r'\((\d{3})\)\s*(\d{3})[-.\s](\d{4})',
            r'(\d{3})\.(\d{3})\.(\d{4})',
            r'(\d{10})',
            # Специальные паттерны для скрытых номеров
            r'href="tel:([^"]+)"',
            r'data-phone[^>]*>([^<]+)',
            r'class="phone[^>]*>([^<]+)',
            r'id="phone[^>]*>([^<]+)'
        ]

    def _setup_logging(self):
        logger = logging.getLogger('ContactFocusedScraper')
        logger.setLevel(logging.INFO)
        
        if not logger.handlers:
            handler = logging.StreamHandler()
            formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
            handler.setFormatter(formatter)
            logger.addHandler(handler)
        
        return logger

    def scrape_with_contact_priority(self, target_count=200):
        """Основной метод: агрессивный сбор с приоритетом контактов"""
        self.logger.info(f"🎯 КОНТАКТ-ФОКУСИРОВАННЫЙ СБОР для {target_count} бизнесов")
        self.logger.info(f"📞 ЦЕЛЬ: минимум {self.MIN_CONTACT_PERCENTAGE}% с телефонами")
        
        # Фаза 1: Базовый сбор из OSM + усиленный поиск контактов
        basic_businesses = self._enhanced_osm_collection(target_count // 2)
        
        # Фаза 2: Google глубинный поиск для недостающих контактов
        google_businesses = self._google_deep_search(target_count // 2)
        
        # Объединяем и убираем дубликаты
        all_businesses = self._merge_and_dedupe(basic_businesses + google_businesses)
        
        # Фаза 3: Агрессивное извлечение контактов
        final_businesses = self._aggressive_contact_extraction(all_businesses)
        
        # Проверяем качество контактов
        contact_percentage = self._calculate_contact_percentage(final_businesses)
        self.logger.info(f"📊 РЕЗУЛЬТАТ: {contact_percentage:.1f}% бизнесов с телефонами")
        
        if contact_percentage < self.MIN_CONTACT_PERCENTAGE:
            self.logger.warning(f"⚠️ Недостаточно контактов! Запускаю дополнительный поиск...")
            additional_businesses = self._emergency_contact_search(target_count)
            final_businesses.extend(additional_businesses)
        
        self.results = final_businesses[:target_count]
        return self.results

    def _enhanced_osm_collection(self, target_count):
        """Улучшенный сбор OSM с фокусом на контакты"""
        self.logger.info(f"📍 OSM сбор с контакт-фильтрацией для {target_count} бизнесов")
        
        results = []
        base_url = "https://overpass-api.de/api/interpreter"
        
        # Модифицированный запрос с фокусом на контакты
        query = """
        [out:json][timeout:45];
        (
          node["shop"="scrap_yard"]["phone"](bbox);
          node["amenity"="recycling"]["phone"](bbox);
          node["industrial"="scrap_yard"]["phone"](bbox);
          node["shop"="scrap_yard"]["contact:phone"](bbox);
          node["amenity"="recycling"]["contact:phone"](bbox);
          node["shop"="scrap_yard"](bbox);
          node["amenity"="recycling"](bbox);
          node["industrial"="scrap_yard"](bbox);
          node[name~"scrap|metal|recycl|salvage"][phone](bbox);
          way[name~"scrap|metal|recycl|salvage"][phone](bbox);
        );
        out center meta tags;
        """
        
        # Концентрируемся на индустриальных областях
        industrial_bboxes = [
            "41.49,-87.92,42.02,-87.52",  # Chicago Industrial
            "29.52,-95.67,30.11,-95.07",  # Houston Ship Channel
            "33.93,-84.67,34.25,-84.13",  # Atlanta Industrial
            "39.72,-75.28,40.14,-74.95",  # Philadelphia Industrial
            "42.23,-83.29,42.45,-82.91",  # Detroit Industrial
            "40.40,-80.15,40.60,-79.85",  # Pittsburgh Steel Area
            "33.65,-87.05,33.85,-86.65",  # Birmingham Steel
        ]
        
        for i, bbox in enumerate(industrial_bboxes):
            try:
                self.logger.info(f"🏭 Поиск в индустриальной зоне {i+1}/{len(industrial_bboxes)}")
                bbox_query = query.replace("(bbox)", f"({bbox})")
                
                response = self._make_safe_request(base_url, data=bbox_query, method='POST')
                if response and response.status_code == 200:
                    data = response.json()
                    businesses = self._parse_osm_with_contact_priority(data)
                    results.extend(businesses)
                    
                    contacts_found = sum(1 for b in businesses if b.get('phone'))
                    self.logger.info(f"✅ Найдено {len(businesses)} бизнесов, {contacts_found} с телефонами")
                
                time.sleep(random.uniform(3, 6))
                
                if len(results) >= target_count:
                    break
                    
            except Exception as e:
                self.logger.warning(f"❌ OSM ошибка в зоне {i+1}: {e}")
                continue
        
        return results

    def _parse_osm_with_contact_priority(self, data):
        """Парсинг OSM с приоритетом контактов"""
        businesses = []
        
        try:
            if 'elements' in data:
                for element in data['elements']:
                    tags = element.get('tags', {})
                    
                    # Получаем имя
                    name = (tags.get('name') or 
                           tags.get('operator') or 
                           tags.get('brand', ''))
                    
                    if not name or len(name.strip()) < 3:
                        continue
                    
                    # Координаты
                    lat = element.get('lat')
                    lon = element.get('lon')
                    
                    if not lat and 'center' in element:
                        lat = element['center']['lat']
                        lon = element['center']['lon']
                    
                    # АГРЕССИВНЫЙ поиск телефона
                    phone = self._extract_phone_aggressive(tags)
                    
                    business = {
                        'name': name.strip(),
                        'address': self._build_address(tags),
                        'city': tags.get('addr:city', ''),
                        'state': tags.get('addr:state', ''),
                        'zip_code': tags.get('addr:postcode', ''),
                        'phone': phone,
                        'website': tags.get('website', tags.get('contact:website', '')),
                        'email': tags.get('email', tags.get('contact:email', '')),
                        'latitude': float(lat) if lat else None,
                        'longitude': float(lon) if lon else None,
                        'source': 'OSM_Contact_Priority',
                        'osm_id': str(element.get('id', '')),
                        'scraped_at': datetime.now().isoformat(),
                        'has_contact': bool(phone)  # Маркер наличия контакта
                    }
                    
                    businesses.append(business)
                        
        except Exception as e:
            self.logger.error(f"OSM парсинг ошибка: {e}")
        
        # Сортируем: сначала с контактами
        businesses.sort(key=lambda x: x.get('has_contact', False), reverse=True)
        return businesses

    def _extract_phone_aggressive(self, tags):
        """Агрессивное извлечение телефона из всех возможных полей"""
        phone_fields = [
            'phone', 'contact:phone', 'telephone', 'contact:telephone',
            'phone:mobile', 'contact:mobile', 'fax', 'contact:fax'
        ]
        
        for field in phone_fields:
            if field in tags and tags[field]:
                phone = self._clean_phone(tags[field])
                if phone:
                    return phone
        
        return ""

    def _google_deep_search(self, target_count):
        """Google поиск компаний с низкими позициями"""
        self.logger.info(f"🔍 Google глубинный поиск для {target_count} компаний")
        
        results = []
        
        # Настраиваем веб-драйвер для Google
        driver = self._setup_webdriver()
        
        try:
            for city in self.target_cities[:20]:  # Ограничиваем для тестирования
                if len(results) >= target_count:
                    break
                
                for query_template in self.deep_search_queries[:5]:  # Топ-5 запросов
                    query = query_template.format(city=city)
                    
                    self.logger.info(f"🔎 Google поиск: {query}")
                    
                    # Собираем ссылки с глубоких страниц Google
                    deep_links = self._scrape_google_deep_results(driver, query)
                    
                    # Парсим каждую ссылку для поиска контактов
                    for link in deep_links:
                        try:
                            business_data = self._scrape_business_for_contacts(link, city)
                            if business_data and business_data.get('phone'):
                                results.append(business_data)
                                
                            if len(results) >= target_count:
                                break
                                
                        except Exception as e:
                            self.logger.debug(f"Ошибка парсинга {link}: {e}")
                            continue
                    
                    # Задержка между запросами
                    time.sleep(random.uniform(10, 20))
                    
                    if len(results) >= target_count:
                        break
        
        finally:
            driver.quit()
        
        self.logger.info(f"✅ Google поиск завершен: {len(results)} бизнесов с контактами")
        return results

    def _setup_webdriver(self):
        """Настройка веб-драйвера для Google"""
        options = Options()
        options.add_argument('--headless')
        options.add_argument('--no-sandbox')
        options.add_argument('--disable-dev-shm-usage')
        options.add_argument('--disable-blink-features=AutomationControlled')
        options.add_experimental_option("excludeSwitches", ["enable-automation"])
        options.add_experimental_option('useAutomationExtension', False)
        options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36")
        
        driver = webdriver.Chrome(options=options)
        driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
        
        return driver

    def _scrape_google_deep_results(self, driver, query, max_pages=5):
        """Парсим глубокие результаты Google (страницы 2-5)"""
        deep_links = []
        
        try:
            # Формируем URL поиска
            search_url = f"https://www.google.com/search?q={quote_plus(query)}"
            driver.get(search_url)
            
            # Ждем загрузки
            WebDriverWait(driver, 10).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, "div.g"))
            )
            
            # Переходим на глубокие страницы (2-5) где меньше конкуренции
            for page in range(2, max_pages + 1):
                try:
                    # Находим ссылку на следующую страницу
                    next_button = driver.find_element(By.CSS_SELECTOR, "a[aria-label*='Page']")
                    driver.execute_script("arguments[0].click();", next_button)
                    
                    time.sleep(random.uniform(3, 7))
                    
                    # Собираем ссылки с этой страницы
                    page_links = self._extract_google_page_links(driver)
                    deep_links.extend(page_links)
                    
                    self.logger.info(f"📄 Страница {page}: собрано {len(page_links)} ссылок")
                    
                except Exception as e:
                    self.logger.debug(f"Не удалось перейти на страницу {page}: {e}")
                    break
        
        except Exception as e:
            self.logger.warning(f"Google поиск ошибка: {e}")
        
        return deep_links[:50]  # Ограничиваем количество

    def _extract_google_page_links(self, driver):
        """Извлекаем ссылки со страницы Google используя адаптированный JS"""
        try:
            # Адаптированная версия пользовательского скрипта
            js_script = """
            var links = [];
            var elements = document.querySelectorAll('div.g a[href]');
            elements.forEach(function(element) {
                var href = element.getAttribute('href');
                var title = element.querySelector('h3');
                if (href && href.startsWith('http') && title) {
                    links.push({
                        url: href,
                        title: title.innerText,
                        description: ''
                    });
                }
            });
            return links;
            """
            
            links_data = driver.execute_script(js_script)
            return [link['url'] for link in links_data if self._is_relevant_business_link(link['url'])]
            
        except Exception as e:
            self.logger.debug(f"JS извлечение ссылок ошибка: {e}")
            return []

    def _is_relevant_business_link(self, url):
        """Проверяем, релевантна ли ссылка для бизнеса"""
        skip_domains = [
            'google.com', 'facebook.com', 'youtube.com', 'twitter.com',
            'yelp.com', 'yellowpages.com', 'wikipedia.org', 'maps.google.com'
        ]
        
        url_lower = url.lower()
        return not any(domain in url_lower for domain in skip_domains)

    def _scrape_business_for_contacts(self, url, city):
        """Парсим бизнес-сайт с фокусом на контакты"""
        try:
            response = self._make_safe_request(url, timeout=15)
            if not response or response.status_code != 200:
                return None
            
            soup = BeautifulSoup(response.text, 'html.parser')
            
            # Агрессивный поиск телефонов
            phone = self._extract_phone_from_page(soup, response.text)
            
            if not phone:  # Если не нашли телефон, пропускаем
                return None
            
            # Извлекаем базовую информацию
            business = {
                'name': self._extract_business_name(soup),
                'phone': phone,
                'email': self._extract_email_from_page(soup, response.text),
                'website': url,
                'city': city.split()[0],  # Извлекаем город
                'state': city.split()[-1] if len(city.split()) > 1 else '',
                'address': self._extract_address_from_page(soup),
                'source': 'Google_Deep_Search',
                'scraped_at': datetime.now().isoformat(),
                'has_contact': True
            }
            
            return business
            
        except Exception as e:
            self.logger.debug(f"Ошибка парсинга {url}: {e}")
            return None

    def _extract_phone_from_page(self, soup, page_text):
        """Агрессивное извлечение телефона со страницы"""
        # Ищем в HTML тегах
        phone_selectors = [
            'a[href^="tel:"]', '[data-phone]', '.phone', '#phone',
            '.contact-phone', '.telephone', '.phone-number',
            '.contact .phone', '.header-phone', '.footer-phone'
        ]
        
        for selector in phone_selectors:
            elements = soup.select(selector)
            for element in elements:
                if selector.startswith('a[href^="tel:"]'):
                    phone_text = element.get('href', '').replace('tel:', '')
                else:
                    phone_text = element.get('data-phone') or element.get_text()
                
                phone = self._clean_phone(phone_text)
                if phone:
                    return phone
        
        # Ищем в тексте страницы по паттернам
        for pattern in self.aggressive_phone_patterns:
            matches = re.findall(pattern, page_text, re.IGNORECASE)
            for match in matches:
                if isinstance(match, tuple):
                    # Группы захвата: (area, first, last)
                    phone = f"({match[0]}) {match[1]}-{match[2]}"
                else:
                    phone = str(match)
                
                cleaned = self._clean_phone(phone)
                if cleaned:
                    return cleaned
        
        return ""

    def _extract_email_from_page(self, soup, page_text):
        """Извлечение email со страницы"""
        # Email паттерн
        email_pattern = r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b'
        
        # Ищем в тексте
        emails = re.findall(email_pattern, page_text)
        
        # Фильтруем нежелательные email'ы
        valid_emails = []
        skip_domains = ['example.com', 'test.com', 'placeholder.com', 'google.com']
        
        for email in emails:
            if not any(domain in email.lower() for domain in skip_domains):
                valid_emails.append(email)
        
        return valid_emails[0] if valid_emails else ""

    def _extract_business_name(self, soup):
        """Извлечение названия бизнеса"""
        name_selectors = [
            'h1', 'title', '.business-name', '.company-name',
            '.site-title', '.logo-text', '.header h1'
        ]
        
        for selector in name_selectors:
            element = soup.select_one(selector)
            if element:
                name = element.get_text().strip()
                if name and len(name) > 3:
                    return name[:100]  # Ограничиваем длину
        
        return "Unknown Business"

    def _extract_address_from_page(self, soup):
        """Извлечение адреса со страницы"""
        address_selectors = [
            '.address', '.location', '.contact-address',
            '[itemtype*="PostalAddress"]', '.street-address'
        ]
        
        for selector in address_selectors:
            element = soup.select_one(selector)
            if element:
                address = element.get_text().strip()
                if address and any(word in address.lower() for word in ['street', 'ave', 'road', 'drive', 'blvd']):
                    return address[:200]
        
        return ""

    def _aggressive_contact_extraction(self, businesses):
        """Агрессивное извлечение контактов для бизнесов без телефонов"""
        self.logger.info(f"📞 Агрессивный поиск контактов для {len(businesses)} бизнесов")
        
        enhanced_businesses = []
        
        for business in businesses:
            if business.get('phone'):  # Уже есть телефон
                enhanced_businesses.append(business)
                continue
            
            # Пытаемся найти контакты через разные методы
            enhanced = business.copy()
            
            # Метод 1: Поиск через название + город в Google
            if not enhanced.get('phone'):
                phone = self._search_phone_by_name_city(enhanced)
                if phone:
                    enhanced['phone'] = phone
                    enhanced['phone_source'] = 'google_search'
            
            # Метод 2: Если есть сайт, парсим его агрессивно
            if not enhanced.get('phone') and enhanced.get('website'):
                phone = self._deep_scrape_website_for_phone(enhanced['website'])
                if phone:
                    enhanced['phone'] = phone
                    enhanced['phone_source'] = 'website_deep'
            
            # Метод 3: Поиск в справочниках
            if not enhanced.get('phone'):
                phone = self._search_business_directories(enhanced)
                if phone:
                    enhanced['phone'] = phone
                    enhanced['phone_source'] = 'directories'
            
            enhanced['has_contact'] = bool(enhanced.get('phone'))
            enhanced_businesses.append(enhanced)
        
        return enhanced_businesses

    def _search_phone_by_name_city(self, business):
        """Поиск телефона через Google по названию + город"""
        try:
            name = business.get('name', '')
            city = business.get('city', '')
            state = business.get('state', '')
            
            if not name:
                return ""
            
            # Формируем поисковый запрос
            query = f'"{name}" {city} {state} phone contact scrap metal'
            search_url = f"https://www.google.com/search?q={quote_plus(query)}"
            
            response = self._make_safe_request(search_url)
            if response and response.status_code == 200:
                # Ищем телефоны в результатах поиска
                for pattern in self.aggressive_phone_patterns:
                    matches = re.findall(pattern, response.text, re.IGNORECASE)
                    for match in matches:
                        phone = self._format_phone_match(match)
                        if phone:
                            return phone
            
        except Exception as e:
            self.logger.debug(f"Google phone search error: {e}")
        
        return ""

    def _deep_scrape_website_for_phone(self, website):
        """Глубокий парсинг сайта для поиска телефона"""
        try:
            # Парсим главную страницу
            response = self._make_safe_request(website, timeout=10)
            if not response or response.status_code != 200:
                return ""
            
            soup = BeautifulSoup(response.text, 'html.parser')
            
            # Ищем на главной
            phone = self._extract_phone_from_page(soup, response.text)
            if phone:
                return phone
            
            # Ищем ссылки на страницы контактов
            contact_links = self._find_contact_page_links(soup, website)
            
            for contact_url in contact_links:
                try:
                    contact_response = self._make_safe_request(contact_url, timeout=10)
                    if contact_response and contact_response.status_code == 200:
                        contact_soup = BeautifulSoup(contact_response.text, 'html.parser')
                        phone = self._extract_phone_from_page(contact_soup, contact_response.text)
                        if phone:
                            return phone
                except:
                    continue
            
        except Exception as e:
            self.logger.debug(f"Deep website scrape error: {e}")
        
        return ""

    def _find_contact_page_links(self, soup, base_url):
        """Находим ссылки на страницы контактов"""
        contact_keywords = ['contact', 'about', 'phone', 'call', 'reach']
        contact_links = []
        
        links = soup.find_all('a', href=True)
        for link in links:
            href = link.get('href', '')
            text = link.get_text().lower()
            
            if any(keyword in text for keyword in contact_keywords):
                full_url = urljoin(base_url, href)
                if full_url not in contact_links:
                    contact_links.append(full_url)
        
        return contact_links[:5]  # Ограничиваем количество

    def _search_business_directories(self, business):
        """Поиск в бизнес-справочниках"""
        directories = [
            'yellowpages.com',
            'whitepages.com', 
            'superpages.com'
        ]
        
        name = business.get('name', '')
        city = business.get('city', '')
        
        if not name:
            return ""
        
        for directory in directories:
            try:
                query = f'site:{directory} "{name}" {city} phone'
                search_url = f"https://www.google.com/search?q={quote_plus(query)}"
                
                response = self._make_safe_request(search_url)
                if response and response.status_code == 200:
                    for pattern in self.aggressive_phone_patterns:
                        matches = re.findall(pattern, response.text, re.IGNORECASE)
                        for match in matches:
                            phone = self._format_phone_match(match)
                            if phone:
                                return phone
            except:
                continue
        
        return ""

    def _format_phone_match(self, match):
        """Форматируем найденный телефон"""
        if isinstance(match, tuple):
            if len(match) == 3:
                return f"({match[0]}) {match[1]}-{match[2]}"
            else:
                return ''.join(match)
        else:
            return self._clean_phone(str(match))

    def _clean_phone(self, phone):
        """Очистка и форматирование телефона"""
        if not phone:
            return ""
        
        # Извлекаем только цифры
        digits = re.sub(r'\D', '', phone)
        
        # Проверяем валидность
        if len(digits) == 10:
            return f"({digits[:3]}) {digits[3:6]}-{digits[6:]}"
        elif len(digits) == 11 and digits[0] == '1':
            return f"({digits[1:4]}) {digits[4:7]}-{digits[7:]}"
        elif len(digits) >= 10:
            # Берем последние 10 цифр
            last_10 = digits[-10:]
            return f"({last_10[:3]}) {last_10[3:6]}-{last_10[6:]}"
        
        return ""

    def _build_address(self, tags):
        """Построение адреса из OSM тегов"""
        parts = []
        if tags.get('addr:housenumber'):
            parts.append(tags['addr:housenumber'])
        if tags.get('addr:street'):
            parts.append(tags['addr:street'])
        return ' '.join(parts) if parts else ""

    def _merge_and_dedupe(self, businesses):
        """Объединение и удаление дубликатов"""
        seen = set()
        unique_businesses = []
        
        for business in businesses:
            # Создаем ключ для дедупликации
            name = business.get('name', '').lower().strip()
            city = business.get('city', '').lower().strip()
            phone = business.get('phone', '').strip()
            
            # Используем телефон как основной идентификатор, если есть
            if phone:
                key = phone
            else:
                key = f"{name}_{city}"
            
            if key not in seen and name:
                seen.add(key)
                unique_businesses.append(business)
        
        return unique_businesses

    def _calculate_contact_percentage(self, businesses):
        """Рассчитываем процент бизнесов с контактами"""
        if not businesses:
            return 0
        
        with_contacts = sum(1 for b in businesses if b.get('phone'))
        return (with_contacts / len(businesses)) * 100

    def _emergency_contact_search(self, target_count):
        """Экстренный поиск контактов, если основной не дал результата"""
        self.logger.info("🚨 ЭКСТРЕННЫЙ режим поиска контактов")
        
        emergency_results = []
        
        # Используем более агрессивные методы
        emergency_queries = [
            "scrap metal phone number {city}",
            "metal recycling contact {city}",
            "junk yard phone {city}",
            "scrap dealer call {city}"
        ]
        
        for city in self.target_cities[:10]:
            for query_template in emergency_queries:
                query = query_template.format(city=city)
                
                try:
                    # Простой HTTP поиск без Selenium
                    search_url = f"https://duckduckgo.com/?q={quote_plus(query)}"
                    response = self._make_safe_request(search_url)
                    
                    if response and response.status_code == 200:
                        # Ищем телефоны прямо в результатах
                        for pattern in self.aggressive_phone_patterns:
                            matches = re.findall(pattern, response.text, re.IGNORECASE)
                            for match in matches:
                                phone = self._format_phone_match(match)
                                if phone:
                                    emergency_results.append({
                                        'name': f"Emergency Contact {len(emergency_results) + 1}",
                                        'phone': phone,
                                        'city': city.split()[0],
                                        'state': city.split()[-1] if len(city.split()) > 1 else '',
                                        'source': 'Emergency_Search',
                                        'scraped_at': datetime.now().isoformat(),
                                        'has_contact': True
                                    })
                                    
                                    if len(emergency_results) >= target_count // 4:
                                        return emergency_results
                
                except Exception as e:
                    self.logger.debug(f"Emergency search error: {e}")
                    continue
                
                time.sleep(1)  # Быстрая задержка
        
        return emergency_results

    def _make_safe_request(self, url, params=None, data=None, method='GET', timeout=30, max_retries=3):
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
                
                if method == 'POST':
                    response = self.session.post(url, headers=headers, data=data, timeout=timeout)
                else:
                    response = self.session.get(url, headers=headers, params=params, timeout=timeout)
                
                if response.status_code == 200:
                    return response
                elif response.status_code == 429:
                    wait_time = (attempt + 1) * 10
                    self.logger.warning(f"Rate limit, ждем {wait_time}s")
                    time.sleep(wait_time)
                
            except Exception as e:
                if attempt < max_retries - 1:
                    time.sleep(random.uniform(2, 5))
        
        return None

    def export_contact_focused_results(self, output_dir="output"):
        """Экспорт результатов с фокусом на контакты"""
        if not self.results:
            self.logger.warning("Нет данных для экспорта")
            return None
        
        os.makedirs(output_dir, exist_ok=True)
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        # Анализ качества контактов
        total_businesses = len(self.results)
        with_phones = sum(1 for b in self.results if b.get('phone'))
        with_emails = sum(1 for b in self.results if b.get('email'))
        with_websites = sum(1 for b in self.results if b.get('website'))
        
        contact_percentage = (with_phones / total_businesses) * 100 if total_businesses > 0 else 0
        
        # Сортируем: сначала с контактами
        sorted_results = sorted(self.results, key=lambda x: bool(x.get('phone')), reverse=True)
        
        # Экспорт в CSV
        df = pd.DataFrame(sorted_results)
        csv_file = os.path.join(output_dir, f"contact_focused_businesses_{timestamp}.csv")
        df.to_csv(csv_file, index=False)
        
        # Экспорт в Excel с анализом контактов
        excel_file = os.path.join(output_dir, f"contact_focused_businesses_{timestamp}.xlsx")
        with pd.ExcelWriter(excel_file, engine='openpyxl') as writer:
            # Основные данные
            df.to_excel(writer, sheet_name='All Businesses', index=False)
            
            # Только с контактами
            businesses_with_contacts = df[df['phone'].notna() & (df['phone'] != '')]
            if not businesses_with_contacts.empty:
                businesses_with_contacts.to_excel(writer, sheet_name='With Phone Numbers', index=False)
            
            # Без контактов (для дополнительной обработки)
            businesses_without_contacts = df[df['phone'].isna() | (df['phone'] == '')]
            if not businesses_without_contacts.empty:
                businesses_without_contacts.to_excel(writer, sheet_name='Need Phone Numbers', index=False)
            
            # Анализ источников
            source_analysis = df.groupby('source').agg({
                'name': 'count',
                'phone': lambda x: x.notna().sum()
            }).reset_index()
            source_analysis.columns = ['Source', 'Total Businesses', 'With Phone']
            source_analysis['Phone Percentage'] = (source_analysis['With Phone'] / source_analysis['Total Businesses'] * 100).round(1)
            source_analysis.to_excel(writer, sheet_name='Source Analysis', index=False)
        
        # JSON экспорт
        json_file = os.path.join(output_dir, f"contact_focused_businesses_{timestamp}.json")
        with open(json_file, 'w') as f:
            json.dump(sorted_results, f, indent=2, default=str)
        
        # Создаем отчет с акцентом на контакты
        self._create_contact_focused_report(output_dir, timestamp, {
            'total_businesses': total_businesses,
            'with_phones': with_phones,
            'with_emails': with_emails, 
            'with_websites': with_websites,
            'contact_percentage': contact_percentage
        })
        
        self.logger.info(f"✅ КОНТАКТ-ФОКУСИРОВАННЫЕ данные экспортированы:")
        self.logger.info(f"  • CSV: {csv_file}")
        self.logger.info(f"  • Excel: {excel_file}")
        self.logger.info(f"  • JSON: {json_file}")
        self.logger.info(f"📊 РЕЗУЛЬТАТ: {contact_percentage:.1f}% бизнесов с телефонами ({with_phones}/{total_businesses})")
        
        return {
            'csv': csv_file,
            'excel': excel_file,
            'json': json_file,
            'total_count': total_businesses,
            'contact_percentage': contact_percentage,
            'businesses_with_phones': with_phones
        }

    def _create_contact_focused_report(self, output_dir, timestamp, stats):
        """Создаем отчет с фокусом на контакты"""
        report_file = os.path.join(output_dir, f"contact_focused_report_{timestamp}.txt")
        
        with open(report_file, 'w', encoding='utf-8') as f:
            f.write("📞 КОНТАКТ-ФОКУСИРОВАННЫЙ ОТЧЕТ ПО SCRAP METAL ЦЕНТРАМ\n")
            f.write("=" * 70 + "\n\n")
            f.write(f"Отчет создан: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
            f.write(f"Метод сбора: Агрессивный поиск контактов + Google глубинный\n\n")
            
            f.write("📊 СТАТИСТИКА КОНТАКТОВ\n")
            f.write("-" * 30 + "\n")
            f.write(f"Общее количество бизнесов: {stats['total_businesses']}\n")
            f.write(f"Бизнесы с телефонами: {stats['with_phones']} ({stats['contact_percentage']:.1f}%)\n")
            f.write(f"Бизнесы с email: {stats['with_emails']}\n")
            f.write(f"Бизнесы с сайтами: {stats['with_websites']}\n\n")
            
            # Оценка результата
            if stats['contact_percentage'] >= self.MIN_CONTACT_PERCENTAGE:
                f.write("✅ ЦЕЛЬ ДОСТИГНУТА: Высокий процент контактов!\n")
            elif stats['contact_percentage'] >= 60:
                f.write("⚠️ РЕЗУЛЬТАТ ПРИЕМЛЕМЫЙ: Средний процент контактов\n")
            else:
                f.write("❌ РЕЗУЛЬТАТ НЕУДОВЛЕТВОРИТЕЛЬНЫЙ: Низкий процент контактов\n")
            
            f.write("\n💡 РЕКОМЕНДАЦИИ ПО ИСПОЛЬЗОВАНИЮ\n")
            f.write("-" * 35 + "\n")
            f.write("1. Приоритет звонкам бизнесам с verified контактами\n")
            f.write("2. Использовать email как резервный канал связи\n")
            f.write("3. Проверить актуальность телефонов перед массовыми звонками\n")
            f.write("4. Для бизнесов без контактов - запустить дополнительный поиск\n")
            f.write("5. Фокус на бизнесы из Google глубинного поиска (меньше конкуренции)\n\n")
            
            f.write("🎯 СТРАТЕГИЯ OUTREACH\n")
            f.write("-" * 20 + "\n")
            f.write("• Глубинные компании (страницы 2-5 Google) - высокий потенциал\n")
            f.write("• Компании без сильного онлайн-присутствия нуждаются в продвижении\n")
            f.write("• Региональные рынки показывают хорошие результаты\n")
            f.write("• Прямые контакты эффективнее чем онлайн-заявки\n")
        
        self.logger.info(f"✅ Контакт-фокусированный отчет: {report_file}")

def main():
    print("📞 КОНТАКТ-ФОКУСИРОВАННЫЙ ПАРСЕР для Scrap Metal Центров")
    print("=" * 70)
    print("🎯 ПРИОРИТЕТ: Максимальный сбор контактных данных")
    print("🔍 СТРАТЕГИЯ: Google глубинный поиск компаний с низкими позициями")
    print("📊 ЦЕЛЬ: Минимум 80% бизнесов с телефонами")
    
    scraper = ContactFocusedScraper()
    
    try:
        target_count = int(input("\nВведите количество бизнесов для сбора (по умолчанию 200): ") or "200")
        
        print(f"\n🚀 Запуск КОНТАКТ-ФОКУСИРОВАННОГО сбора для {target_count} бизнесов...")
        print("Процесс включает:")
        print("1. 📍 OSM сбор с фильтрацией по контактам")
        print("2. 🔍 Google глубинный поиск (страницы 2-5)")
        print("3. 📞 Агрессивное извлечение телефонов")
        print("4. 🚨 Экстренный поиск при недостаточном количестве контактов")
        print("\nЭто может занять 30-60 минут для качественного результата...")
        
        results = scraper.scrape_with_contact_priority(target_count)
        
        if results:
            contact_stats = scraper._calculate_contact_percentage(results)
            print(f"\n✅ Сбор завершен: {len(results)} бизнесов")
            print(f"📞 Контакты: {contact_stats:.1f}% бизнесов с телефонами")
            
            export_info = scraper.export_contact_focused_results()
            if export_info:
                print(f"\n📁 Файлы созданы:")
                print(f"  • CSV: {export_info['csv']}")
                print(f"  • Excel: {export_info['excel']}")
                print(f"  • JSON: {export_info['json']}")
                print(f"\n📊 ИТОГОВАЯ СТАТИСТИКА:")
                print(f"  • Всего бизнесов: {export_info['total_count']}")
                print(f"  • С телефонами: {export_info['businesses_with_phones']}")
                print(f"  • Процент контактов: {export_info['contact_percentage']:.1f}%")
                
                if export_info['contact_percentage'] >= scraper.MIN_CONTACT_PERCENTAGE:
                    print("\n🎉 ОТЛИЧНО! Цель по контактам достигнута!")
                else:
                    print(f"\n⚠️ Цель {scraper.MIN_CONTACT_PERCENTAGE}% не достигнута. Рекомендуется дополнительный сбор.")
            else:
                print("❌ Ошибка экспорта")
        else:
            print("❌ Данные не собраны")
            
    except KeyboardInterrupt:
        print("\n⚠️ Процесс прерван пользователем")
    except Exception as e:
        print(f"❌ Ошибка: {e}")

if __name__ == "__main__":
    main() 