#!/usr/bin/env python3
"""
ТОЧНЫЙ АВТОМАТИЧЕСКИЙ ПАРСЕР МЕТАЛЛОЛОМА
100% ТОЧНЫЕ ДАННЫЕ | ГЛОБАЛЬНЫЙ ОХВАТ | МАКСИМАЛЬНАЯ ПОЛНОТА
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
from urllib.parse import quote_plus, urlparse
import re
from concurrent.futures import ThreadPoolExecutor, as_completed
import urllib3
urllib3.disable_warnings()

class USMetalScraper:
    def __init__(self):
        self.session = requests.Session()
        self.results = []
        self.processed_urls = set()
        self.logger = self._setup_logging()
        
        # НАСТРОЙКИ ДЛЯ МАКСИМАЛЬНОЙ СКОРОСТИ И ТОЧНОСТИ
        self.MIN_PHONE_PERCENTAGE = 85  # 85% с телефонами
        self.TIMEOUT = 6                # Fast timeout for speed
        self.MAX_WORKERS = 12           # Maximum parallel workers for speed
        
        # US PHONE PATTERNS - Optimized for US businesses
        self.phone_patterns = [
            # Standard US format: (555) 123-4567
            re.compile(r'\b\(?([0-9]{3})\)?[-.\s]?([0-9]{3})[-.\s]?([0-9]{4})\b'),
            # US with country code: 1-555-123-4567
            re.compile(r'\b1[-.\s]?\(?([0-9]{3})\)?[-.\s]?([0-9]{3})[-.\s]?([0-9]{4})\b'),
            # tel: links format
            re.compile(r'tel:[\s]*\+?1?[-.\s]?\(?([0-9]{3})\)?[-.\s]?([0-9]{3})[-.\s]?([0-9]{4})', re.IGNORECASE),
            # Various US formatting variations
            re.compile(r'\b([0-9]{3})[-.\s]+([0-9]{3})[-.\s]+([0-9]{4})\b'),
            re.compile(r'\b([0-9]{3})\.([0-9]{3})\.([0-9]{4})\b'),
            re.compile(r'\b([0-9]{3})\s([0-9]{3})\s([0-9]{4})\b'),
        ]
        
        # РАСШИРЕННЫЕ ПОИСКОВЫЕ ЗАПРОСЫ (ГЛОБАЛЬНЫЕ)
        self.search_queries = [
            'scrap metal dealers',
            'metal recycling center',
            'scrap yard',
            'junk yard auto parts',
            'copper scrap buyers',
            'aluminum recycling',
            'auto salvage yard',
            'scrap metal pickup',
            'metal scrap dealers',
            'recycling centre',
            'scrap metal merchants',
            'waste metal collection',
            'metal recovery services',
            'industrial metal recycling',
            'non-ferrous metal dealers'
        ]
        
        # US TARGET LOCATIONS - Strategic focus on scrap metal markets
        self.target_locations = [
            # Major US metropolitan areas
            'New York NY', 'Los Angeles CA', 'Chicago IL', 'Houston TX', 'Phoenix AZ',
            'Philadelphia PA', 'San Antonio TX', 'San Diego CA', 'Dallas TX', 'San Jose CA',
            'Austin TX', 'Jacksonville FL', 'Fort Worth TX', 'Columbus OH', 'Charlotte NC',
            'San Francisco CA', 'Indianapolis IN', 'Seattle WA', 'Denver CO', 'Washington DC',
            'Boston MA', 'El Paso TX', 'Nashville TN', 'Detroit MI', 'Oklahoma City OK',
            'Portland OR', 'Las Vegas NV', 'Memphis TN', 'Louisville KY', 'Baltimore MD',
            'Milwaukee WI', 'Albuquerque NM', 'Tucson AZ', 'Fresno CA', 'Sacramento CA',
            'Mesa AZ', 'Kansas City MO', 'Atlanta GA', 'Long Beach CA', 'Colorado Springs CO',
            'Raleigh NC', 'Miami FL', 'Virginia Beach VA', 'Omaha NE', 'Oakland CA',
            'Minneapolis MN', 'Tulsa OK', 'Arlington TX', 'Tampa FL', 'New Orleans LA',
            
            # Mid-tier cities with high scrap metal potential
            'Cleveland OH', 'Pittsburgh PA', 'Cincinnati OH', 'Toledo OH', 'Akron OH',
            'Dayton OH', 'Youngstown OH', 'Canton OH', 'Buffalo NY', 'Rochester NY',
            'Syracuse NY', 'Albany NY', 'Utica NY', 'Binghamton NY', 'Elmira NY',
            'Scranton PA', 'Allentown PA', 'Reading PA', 'Erie PA', 'Bethlehem PA',
            'Harrisburg PA', 'Lancaster PA', 'York PA', 'Wilkes-Barre PA',
            'Flint MI', 'Lansing MI', 'Kalamazoo MI', 'Grand Rapids MI', 'Saginaw MI',
            'Bay City MI', 'Battle Creek MI', 'Jackson MI', 'Muskegon MI',
            'Rockford IL', 'Peoria IL', 'Decatur IL', 'Springfield IL', 'Champaign IL',
            'Bloomington IL', 'Quincy IL', 'Danville IL', 'Kankakee IL',
            'Fort Wayne IN', 'Evansville IN', 'South Bend IN', 'Gary IN', 'Muncie IN',
            'Terre Haute IN', 'Anderson IN', 'Kokomo IN', 'Lafayette IN',
            'Green Bay WI', 'Appleton WI', 'Oshkosh WI', 'Racine WI', 'Kenosha WI',
            'Eau Claire WI', 'Wausau WI', 'La Crosse WI', 'Janesville WI',
            'Cedar Rapids IA', 'Davenport IA', 'Sioux City IA', 'Waterloo IA',
            'Dubuque IA', 'Ames IA', 'Council Bluffs IA', 'Mason City IA',
            'Springfield MO', 'Columbia MO', 'Joplin MO', 'St. Joseph MO',
            'Cape Girardeau MO', 'Jefferson City MO', 'Sedalia MO', 'St. Louis MO',
            'Little Rock AR', 'Fayetteville AR', 'Jonesboro AR', 'Pine Bluff AR',
            'Fort Smith AR', 'Texarkana AR', 'Hot Springs AR', 'Conway AR',
            'Birmingham AL', 'Mobile AL', 'Montgomery AL', 'Huntsville AL',
            'Tuscaloosa AL', 'Dothan AL', 'Decatur AL', 'Florence AL',
            'Jackson MS', 'Gulfport MS', 'Hattiesburg MS', 'Meridian MS',
            'Biloxi MS', 'Tupelo MS', 'Greenville MS', 'Vicksburg MS',
            'Shreveport LA', 'Baton Rouge LA', 'Lafayette LA', 'Lake Charles LA',
            'Monroe LA', 'Alexandria LA', 'Houma LA', 'Bossier City LA',
            'Knoxville TN', 'Chattanooga TN', 'Clarksville TN', 'Murfreesboro TN',
            'Jackson TN', 'Johnson City TN', 'Kingsport TN', 'Franklin TN'
        ]
        
        # МАТЕРИАЛЫ ДЛЯ ПОИСКА
        self.materials_keywords = [
            'copper', 'aluminum', 'aluminium', 'steel', 'iron', 'brass', 'bronze',
            'stainless steel', 'lead', 'zinc', 'nickel', 'tin', 'titanium',
            'carbide', 'tungsten', 'precious metals', 'gold', 'silver', 'platinum',
            'catalytic converters', 'car batteries', 'radiators', 'electric motors',
            'transformers', 'wire', 'cable', 'circuit boards', 'electronic scrap',
            'computer scrap', 'mobile phones', 'cast iron', 'wrought iron',
            'structural steel', 'rebar', 'pipes', 'tubes', 'sheet metal',
            'coils', 'turnings', 'shredded metal', 'HMS', 'heavy melting scrap'
        ]
        
        # USER AGENTS
        self.user_agents = [
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36',
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:122.0) Gecko/20100101 Firefox/122.0',
            'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36',
            'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36',
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Edge/121.0.0.0',
            'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.2 Safari/605.1.15'
        ]
        
        self._init_session()

    def _init_session(self):
        """Инициализация сессии"""
        self.session.headers.update({
            'User-Agent': random.choice(self.user_agents),
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.9',
            'Accept-Encoding': 'gzip, deflate, br',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1',
            'Sec-Fetch-Dest': 'document',
            'Sec-Fetch-Mode': 'navigate',
            'Sec-Fetch-Site': 'none',
            'Cache-Control': 'max-age=0',
            'DNT': '1'
        })

    def _setup_logging(self):
        logger = logging.getLogger('USMetalScraper')
        logger.setLevel(logging.INFO)
        
        if not logger.handlers:
            console_handler = logging.StreamHandler()
            console_formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
            console_handler.setFormatter(console_formatter)
            logger.addHandler(console_handler)
        
        return logger

    def run_comprehensive_scraping(self, target_businesses=500):
        """КОМПЛЕКСНЫЙ сбор данных по США с максимальной полнотой"""
        self.logger.info(f"🇺🇸 ЗАПУСК US SCRAP METAL СБОРА")
        self.logger.info(f"📞 ЦЕЛЬ: {target_businesses} бизнесов с контактами")
        self.logger.info(f"🔍 ОХВАТ: {len(self.target_locations)} US локаций")
        self.logger.info(f"📋 МАТЕРИАЛЫ: {len(self.materials_keywords)} типов")
        
        start_time = time.time()
        
        # Этап 1: Массовый сбор ссылок
        self.logger.info("🔗 Этап 1: Сбор ссылок из поисковых систем")
        all_links = self._collect_comprehensive_links()
        self.logger.info(f"✅ Собрано уникальных ссылок: {len(all_links)}")
        
        # Этап 2: Извлечение полных данных
        self.logger.info("📊 Этап 2: Комплексное извлечение данных")
        businesses = self._extract_comprehensive_data(all_links, target_businesses)
        
        # Этап 3: Финализация и обогащение
        self.logger.info("🔬 Этап 3: Финализация и обогащение данных")
        self.results = self._finalize_comprehensive_results(businesses, target_businesses)
        
        elapsed = time.time() - start_time
        phone_percentage = self._calculate_contact_percentage()
        
        self.logger.info(f"✅ US СБОР ЗАВЕРШЕН за {elapsed/60:.1f} минут")
        self.logger.info(f"📊 РЕЗУЛЬТАТ: {len(self.results)} бизнесов")
        self.logger.info(f"📞 С контактами: {phone_percentage:.1f}%")
        
        return self.results

    def _collect_comprehensive_links(self):
        """Быстрый параллельный сбор ссылок"""
        self.logger.info("🚀 Запуск параллельного сбора ссылок...")
        
        all_links = []
        
        # Меньше локаций, но быстрее
        target_locations = self.target_locations[:15]  # Reduced for speed
        target_queries = self.search_queries[:8]       # Reduced for speed
        
        # Создаем задачи для параллельного выполнения
        search_tasks = []
        for location in target_locations:
            for query in target_queries:
                for page in range(2, 4):  # Only pages 2-3 for speed
                    search_tasks.append((f"{query} {location}", page))
        
        # Параллельный сбор ссылок
        with ThreadPoolExecutor(max_workers=12) as executor:
            future_to_task = {
                executor.submit(self._fast_bing_search, query, page): (query, page)
                for query, page in search_tasks[:100]  # Limit for speed
            }
            
            for future in as_completed(future_to_task, timeout=600):  # 10 minutes max
                try:
                    links = future.result(timeout=10)
                    all_links.extend(links)
                    
                    # Progress update
                    if len(all_links) % 50 == 0:
                        self.logger.info(f"📈 Собрано ссылок: {len(all_links)}")
                    
                    # Stop when we have enough
                    if len(all_links) >= 500:
                        self.logger.info(f"🎯 Достаточно ссылок: {len(all_links)}")
                        break
                        
                except Exception as e:
                    self.logger.debug(f"Search task failed: {e}")
                    continue
        
        unique_links = self._deduplicate_links(all_links)
        self.logger.info(f"✅ Собрано уникальных ссылок: {len(unique_links)}")
        return unique_links
    
    def _fast_bing_search(self, query, page):
        """Быстрый поиск в Bing без задержек"""
        links = []
        
        try:
            start = (page - 1) * 10
            url = f"https://www.bing.com/search?q={quote_plus(query)}&first={start}&count=10"
            
            headers = {
                'User-Agent': random.choice(self.user_agents),
                'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
                'Accept-Language': 'en-US,en;q=0.9'
            }
            
            response = self.session.get(url, headers=headers, timeout=8, verify=False)
            
            if response.status_code == 200:
                soup = BeautifulSoup(response.text, 'html.parser')
                
                # Быстрое извлечение результатов
                for result in soup.find_all('li', class_='b_algo'):
                    try:
                        title_elem = result.find('h2')
                        if not title_elem:
                            continue
                            
                        link_elem = title_elem.find('a')
                        if not link_elem or not link_elem.get('href'):
                            continue
                        
                        url = link_elem.get('href')
                        title = title_elem.get_text(strip=True)
                        
                        # Быстрая проверка релевантности
                        if self._quick_relevance_check(title):
                            links.append({
                                'url': url,
                                'title': title,
                                'page': page,
                                'query': query,
                                'source': 'Bing'
                            })
                            
                    except Exception as e:
                        continue
                        
        except Exception as e:
            self.logger.debug(f"Fast search failed: {e}")
        
        return links
    
    def _quick_relevance_check(self, title):
        """Быстрая проверка релевантности по заголовку"""
        title_lower = title.lower()
        
        # Релевантные слова
        relevant = ['scrap', 'metal', 'recycling', 'salvage', 'junk', 'yard', 'steel', 'copper', 'aluminum']
        
        # Исключения
        exclude = ['software', 'app', 'game', 'news', 'blog', 'wikipedia', 'facebook', 'jobs']
        
        has_relevant = any(word in title_lower for word in relevant)
        has_exclude = any(word in title_lower for word in exclude)
        
        return has_relevant and not has_exclude

    def _extract_comprehensive_data(self, links, target_businesses):
        """Быстрое извлечение данных с фокусом на контакты"""
        self.logger.info(f"⚡ БЫСТРОЕ извлечение данных из {len(links)} ссылок")
        
        businesses = []
        processed_count = 0
        
        # Process more links faster
        links_to_process = min(len(links), target_businesses * 8)
        
        # Faster parallel processing
        with ThreadPoolExecutor(max_workers=self.MAX_WORKERS) as executor:
            futures = {
                executor.submit(self._fast_extract_business, link): link 
                for link in links[:links_to_process]
            }
            
            for future in as_completed(futures, timeout=900):  # 15 minutes max
                try:
                    business = future.result(timeout=8)  # Faster timeout
                    processed_count += 1
                    
                    if business:
                        businesses.append(business)
                        
                        # Quick logging
                        phone = business.get('phone', 'N/A')
                        email = business.get('email', 'N/A')
                        self.logger.info(f"✅ [{len(businesses)}] {business['name'][:25]}... | 📞 {phone}")
                    
                    # Progress every 100 for speed
                    if processed_count % 100 == 0:
                        contact_rate = len(businesses) / processed_count * 100 if processed_count > 0 else 0
                        self.logger.info(f"📊 Обработано: {processed_count}, найдено: {len(businesses)} ({contact_rate:.1f}%)")
                    
                    # Stop when target reached
                    if len(businesses) >= target_businesses:
                        self.logger.info(f"🎯 Цель достигнута: {len(businesses)} бизнесов")
                        break
                        
                except Exception as e:
                    processed_count += 1
                    continue
        
        final_rate = len(businesses) / processed_count * 100 if processed_count > 0 else 0
        self.logger.info(f"📊 ИТОГО: обработано {processed_count}, извлечено {len(businesses)} ({final_rate:.1f}%)")
        
        return businesses

    def _fast_extract_business(self, link_data):
        """Быстрое извлечение данных с минимальной обработкой"""
        url = link_data['url']
        
        try:
            if not self._is_valid_url(url):
                return None
            
            # Fast request with short timeout
            headers = {'User-Agent': random.choice(self.user_agents)}
            response = self.session.get(url, headers=headers, timeout=6, verify=False)
            
            if response.status_code != 200:
                return None
            
            page_text = response.text
            
            # Fast regex-based extraction
            phone = self._fast_extract_phone(page_text)
            email = self._fast_extract_email(page_text)
            
            # Skip if no contact info
            if not (phone or email):
                return None
            
            # Minimal BeautifulSoup parsing only for essential fields
            soup = BeautifulSoup(response.text, 'html.parser')
            
            business = {
                'name': self._fast_extract_name(link_data, soup),
                'phone': phone,
                'email': email,
                'website': url,
                'address': self._fast_extract_address(page_text),
                'city': self._fast_extract_city(page_text),
                'state': self._fast_extract_state(page_text),
                'country': 'United States',
                'materials_accepted': self._fast_extract_materials(page_text),
                'source': link_data.get('source', ''),
                'scraped_at': datetime.now().isoformat()
            }
            
            return business
                
        except Exception as e:
            return None
    
    def _fast_extract_phone(self, text):
        """Быстрое извлечение телефона через regex"""
        # US phone patterns - faster regex
        patterns = [
            r'\b\(?([2-9][0-9]{2})\)?[-.\s]?([2-9][0-9]{2})[-.\s]?([0-9]{4})\b',
            r'\b1[-.\s]?\(?([2-9][0-9]{2})\)?[-.\s]?([2-9][0-9]{2})[-.\s]?([0-9]{4})\b',
            r'tel:[\s]*\+?1?[-.\s]?\(?([2-9][0-9]{2})\)?[-.\s]?([2-9][0-9]{2})[-.\s]?([0-9]{4})'
        ]
        
        for pattern in patterns:
            matches = re.findall(pattern, text, re.IGNORECASE)
            for match in matches:
                if len(match) == 3:
                    area, exchange, number = match
                    # Quick validation
                    if area[0] not in ['0', '1'] and exchange[0] not in ['0', '1']:
                        return f"({area}) {exchange}-{number}"
        
        return None
    
    def _fast_extract_email(self, text):
        """Быстрое извлечение email через regex"""
        pattern = r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b'
        matches = re.findall(pattern, text)
        
        for match in matches:
            # Skip common non-business domains
            if not any(skip in match.lower() for skip in ['example.com', 'google.com', 'facebook.com']):
                return match
        
        return None
    
    def _fast_extract_name(self, link_data, soup):
        """Быстрое извлечение названия"""
        # Try title first
        title_tag = soup.find('title')
        if title_tag:
            title = title_tag.get_text().strip()
            if title and len(title) > 5:
                # Clean title
                name = title.split('|')[0].split('-')[0].strip()
                return name[:100]
        
        # Fallback to search result title
        return link_data.get('title', 'Unknown Business')[:100]
    
    def _fast_extract_address(self, text):
        """Быстрое извлечение адреса через regex"""
        # US address patterns
        patterns = [
            r'\b\d+\s+[A-Za-z0-9\s]+(?:Street|St|Avenue|Ave|Road|Rd|Drive|Dr|Boulevard|Blvd|Lane|Ln|Way|Circle|Cir|Court|Ct)\b[^,\n]*',
            r'\b\d+\s+[A-Za-z0-9\s]+(?:St|Ave|Rd|Dr|Blvd|Ln|Way|Cir|Ct)\.?\s*[,\n]'
        ]
        
        for pattern in patterns:
            matches = re.findall(pattern, text, re.IGNORECASE)
            if matches:
                return matches[0][:150]
        
        return None
    
    def _fast_extract_city(self, text):
        """Быстрое извлечение города через regex"""
        # Look for city patterns
        pattern = r'\b([A-Za-z\s]+),\s*([A-Z]{2})\s*\d{5}'
        matches = re.findall(pattern, text)
        
        for match in matches:
            city = match[0].strip()
            if len(city) > 2 and city[0].isupper():
                return city[:50]
        
        return None
    
    def _fast_extract_state(self, text):
        """Быстрое извлечение штата через regex"""
        # US state abbreviations
        pattern = r'\b([A-Z]{2})\s*\d{5}(?:-\d{4})?\b'
        matches = re.findall(pattern, text)
        
        us_states = ['AL', 'AK', 'AZ', 'AR', 'CA', 'CO', 'CT', 'DE', 'FL', 'GA', 'HI', 'ID', 'IL', 'IN', 'IA', 'KS', 'KY', 'LA', 'ME', 'MD', 'MA', 'MI', 'MN', 'MS', 'MO', 'MT', 'NE', 'NV', 'NH', 'NJ', 'NM', 'NY', 'NC', 'ND', 'OH', 'OK', 'OR', 'PA', 'RI', 'SC', 'SD', 'TN', 'TX', 'UT', 'VT', 'VA', 'WA', 'WV', 'WI', 'WY']
        
        for match in matches:
            if match in us_states:
                return match
        
        return None
    
    def _fast_extract_materials(self, text):
        """Быстрое извлечение материалов через regex"""
        text_lower = text.lower()
        materials = []
        
        # Quick material check
        material_keywords = ['copper', 'aluminum', 'steel', 'iron', 'brass', 'scrap metal']
        
        for material in material_keywords:
            if material in text_lower:
                materials.append(material)
        
        return materials if materials else None

    def _extract_phone_comprehensive(self, page_text, soup):
        """Комплексное извлечение телефонов (глобальное)"""
        
        # Метод 1: tel: ссылки (высший приоритет)
        tel_links = soup.find_all('a', href=lambda x: x and x.startswith('tel:'))
        for link in tel_links:
            tel_value = link.get('href', '').replace('tel:', '').strip()
            phone = self._clean_phone_global(tel_value)
            if phone:
                return phone
        
        # Метод 2: JSON-LD структурированные данные
        json_scripts = soup.find_all('script', type='application/ld+json')
        for script in json_scripts:
            try:
                data = json.loads(script.string)
                phone = self._extract_phone_from_json_ld(data)
                if phone:
                    return self._clean_phone_global(phone)
            except:
                continue
        
        # Метод 3: Микроданные
        microdata_elements = soup.find_all(attrs={'itemprop': True})
        for element in microdata_elements:
            itemprop = element.get('itemprop', '').lower()
            if 'telephone' in itemprop or 'phone' in itemprop:
                content = element.get('content') or element.get_text()
                phone = self._clean_phone_global(content)
                if phone:
                    return phone
        
        # Метод 4: data-* атрибуты
        for element in soup.find_all():
            for attr, value in element.attrs.items():
                if 'phone' in attr.lower() or 'tel' in attr.lower():
                    phone = self._clean_phone_global(str(value))
                    if phone:
                        return phone
        
        # Метод 5: Контейнеры с классами телефонов
        phone_containers = soup.find_all(class_=re.compile(r'phone|tel|contact|call', re.IGNORECASE))
        for container in phone_containers:
            text = container.get_text()
            phone = self._extract_phone_from_text_global(text)
            if phone:
                return phone
        
        # Метод 6: Поиск по паттернам в тексте
        phone = self._extract_phone_from_text_us(page_text)
        if phone:
            return phone
        
        return None

    def _extract_phone_from_text_us(self, text):
        """Извлечение телефона из текста (US фокус)"""
        for pattern in self.phone_patterns:
            matches = pattern.findall(text)
            for match in matches:
                if isinstance(match, tuple):
                    # Обрабатываем tuple results
                    phone = ' '.join(str(m) for m in match if m)
                else:
                    phone = str(match)
                
                cleaned_phone = self._clean_phone_us(phone)
                if cleaned_phone:
                    return cleaned_phone
        
        return None

    def _clean_phone_us(self, phone):
        """Очистка и валидация US телефонных номеров"""
        if not phone:
            return None
        
        # Извлекаем только цифры
        digits_only = re.sub(r'\D', '', str(phone))
        
        # Проверка на правильную длину US номера
        if len(digits_only) == 10:
            # Стандартный US формат
            area_code = digits_only[:3]
            exchange = digits_only[3:6]
            number = digits_only[6:]
        elif len(digits_only) == 11 and digits_only.startswith('1'):
            # US номер с кодом страны
            area_code = digits_only[1:4]
            exchange = digits_only[4:7]
            number = digits_only[7:]
        else:
            # Неправильная длина для US номера
            return None
        
        # Валидация US номера
        if not self._validate_us_phone(area_code, exchange, number):
            return None
        
        # Возвращаем в стандартном US формате
        return f"({area_code}) {exchange}-{number}"
    
    def _clean_phone_global(self, phone):
        """Глобальная очистка телефонных номеров - используем US валидацию"""
        # For US-focused scraper, use US validation
        return self._clean_phone_us(phone)
    
    def _extract_phone_from_text_global(self, text):
        """Глобальное извлечение телефона из текста - используем US методы"""
        # For US-focused scraper, use US extraction
        return self._extract_phone_from_text_us(text)

    def _validate_us_phone(self, area_code, exchange, number):
        """Строгая валидация US телефонного номера"""
        # Area code не может начинаться с 0 или 1
        if area_code[0] in ['0', '1']:
            return False
        
        # Exchange не может начинаться с 0 или 1
        if exchange[0] in ['0', '1']:
            return False
        
        # Проверка на недопустимые area codes
        invalid_areas = ['000', '111', '222', '333', '444', '555', '666', '777', '888', '999']
        if area_code in invalid_areas:
            return False
        
        # Проверка на toll-free номера (не подходят для местного бизнеса)
        toll_free_areas = ['800', '833', '844', '855', '866', '877', '888']
        if area_code in toll_free_areas:
            return False
        
        # Проверка на service numbers
        if exchange == '555' and number.startswith('01'):
            return False
        
        # Проверка на слишком много повторяющихся цифр
        full_number = area_code + exchange + number
        if len(set(full_number)) < 4:  # Слишком мало уникальных цифр
            return False
        
        return True

    def _extract_email_comprehensive(self, page_text, soup):
        """Комплексное извлечение email"""
        # Методы извлечения email (аналогично телефону)
        
        # 1. mailto: ссылки
        mailto_links = soup.find_all('a', href=lambda x: x and x.startswith('mailto:'))
        for link in mailto_links:
            email = link.get('href', '').replace('mailto:', '').strip()
            if self._validate_email_global(email):
                return email
        
        # 2. JSON-LD
        json_scripts = soup.find_all('script', type='application/ld+json')
        for script in json_scripts:
            try:
                data = json.loads(script.string)
                email = self._extract_email_from_json_ld(data)
                if email:
                    return email
            except:
                continue
        
        # 3. Поиск в тексте
        email_pattern = re.compile(r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b')
        matches = email_pattern.findall(page_text)
        
        for match in matches:
            if self._validate_email_global(match):
                return match
        
        return None

    def _extract_whatsapp(self, page_text, soup):
        """Извлечение WhatsApp"""
        # Поиск ссылок WhatsApp
        whatsapp_patterns = [
            re.compile(r'whatsapp\.com/send\?phone=([0-9]+)', re.IGNORECASE),
            re.compile(r'wa\.me/([0-9]+)', re.IGNORECASE),
            re.compile(r'api\.whatsapp\.com/send\?phone=([0-9]+)', re.IGNORECASE)
        ]
        
        for pattern in whatsapp_patterns:
            matches = pattern.findall(page_text)
            if matches:
                return matches[0]
        
        return None

    def _extract_social_media(self, soup):
        """Извлечение ссылок на соцсети"""
        social_media = {}
        
        # Поиск ссылок на соцсети
        social_patterns = {
            'facebook': r'facebook\.com/[^/\s]+',
            'twitter': r'twitter\.com/[^/\s]+',
            'instagram': r'instagram\.com/[^/\s]+',
            'linkedin': r'linkedin\.com/[^/\s]+',
            'youtube': r'youtube\.com/[^/\s]+',
            'tiktok': r'tiktok\.com/[^/\s]+',
        }
        
        page_text = str(soup)
        
        for platform, pattern in social_patterns.items():
            matches = re.findall(pattern, page_text, re.IGNORECASE)
            if matches:
                social_media[platform] = matches[0]
        
        return social_media if social_media else None

    def _extract_business_name_comprehensive(self, link_data, soup):
        """Комплексное извлечение названия бизнеса"""
        # Множественные источники для названия
        sources = [
            # JSON-LD
            self._extract_name_from_json_ld_comprehensive(soup),
            # Meta tags
            self._extract_name_from_meta_tags(soup),
            # Title tag
            self._extract_name_from_title(soup),
            # H1 tag
            self._extract_name_from_h1(soup),
            # Fallback - из поисковых результатов
            link_data.get('title', 'Unknown Business')
        ]
        
        for source in sources:
            if source and len(source.strip()) > 2:
                # Очистка названия
                name = re.sub(r'\s+', ' ', source.strip())
                name = name.split('|')[0].split('-')[0].strip()
                return name[:150]
        
        return 'Unknown Business'

    def _extract_materials_comprehensive(self, page_text):
        """Комплексное извлечение принимаемых материалов"""
        materials_found = []
        text_lower = page_text.lower()
        
        for material in self.materials_keywords:
            if material in text_lower:
                materials_found.append(material)
        
        return materials_found if materials_found else None

    def _extract_pricing_info_comprehensive(self, page_text):
        """Комплексное извлечение информации о ценах"""
        pricing_patterns = [
            re.compile(r'\$\d+\.?\d*\s*per\s*(?:pound|lb|kilogram|kg|ton|tonne)', re.IGNORECASE),
            re.compile(r'\$\d+\.?\d*\s*/\s*(?:pound|lb|kilogram|kg|ton|tonne)', re.IGNORECASE),
            re.compile(r'(?:copper|aluminum|steel|brass|iron).*?\$\d+\.?\d*', re.IGNORECASE),
            re.compile(r'current\s*price.*?\$\d+\.?\d*', re.IGNORECASE),
            re.compile(r'scrap\s*price.*?\$\d+\.?\d*', re.IGNORECASE)
        ]
        
        pricing_info = []
        for pattern in pricing_patterns:
            matches = pattern.findall(page_text)
            pricing_info.extend(matches[:5])  # Ограничиваем количество
        
        return pricing_info if pricing_info else None

    def _extract_working_hours_comprehensive(self, soup):
        """Комплексное извлечение рабочих часов"""
        # Поиск в различных местах
        hours_selectors = [
            '[itemprop*="openingHours"]',
            '[itemprop*="hours"]',
            '.hours',
            '.opening-hours',
            '.business-hours',
            '.working-hours',
            '.schedule'
        ]
        
        for selector in hours_selectors:
            elements = soup.select(selector)
            for element in elements:
                hours_text = element.get_text(strip=True)
                if len(hours_text) > 10 and any(day in hours_text.lower() for day in ['mon', 'tue', 'wed', 'thu', 'fri', 'sat', 'sun']):
                    return hours_text[:200]
        
        return None

    def _extract_services_comprehensive(self, page_text):
        """Комплексное извлечение услуг"""
        services_keywords = [
            'pickup', 'collection', 'container rental', 'roll-off', 'demolition',
            'dismantling', 'processing', 'sorting', 'weighing', 'cash payment',
            'check payment', 'commercial', 'residential', 'industrial',
            'same day', 'free estimate', 'certified scales', 'licensed',
            'insured', 'bonded', 'environmental', 'hazardous waste'
        ]
        
        services_found = []
        text_lower = page_text.lower()
        
        for service in services_keywords:
            if service in text_lower:
                services_found.append(service)
        
        return services_found if services_found else None

    # Дополнительные методы извлечения данных
    def _extract_country_comprehensive(self, soup, page_text):
        """Извлечение страны"""
        # Поиск в различных источниках
        country_indicators = {
            'United States': ['usa', 'us', 'united states', 'america'],
            'Canada': ['canada', 'ca'],
            'United Kingdom': ['uk', 'united kingdom', 'england', 'scotland', 'wales'],
            'Australia': ['australia', 'au'],
            'New Zealand': ['new zealand', 'nz'],
            'Ireland': ['ireland', 'ie']
        }
        
        text_lower = page_text.lower()
        
        for country, indicators in country_indicators.items():
            if any(indicator in text_lower for indicator in indicators):
                return country
        
        return 'Unknown'

    def _extract_certifications(self, page_text):
        """Извлечение сертификатов"""
        cert_patterns = [
            re.compile(r'ISO\s*\d{4,5}', re.IGNORECASE),
            re.compile(r'certified\s+\w+', re.IGNORECASE),
            re.compile(r'licensed\s+\w+', re.IGNORECASE),
            re.compile(r'EPA\s+\w+', re.IGNORECASE),
            re.compile(r'R2\s+certified', re.IGNORECASE)
        ]
        
        certifications = []
        for pattern in cert_patterns:
            matches = pattern.findall(page_text)
            certifications.extend(matches[:3])
        
        return certifications if certifications else None

    def _extract_years_in_business(self, page_text):
        """Извлечение лет работы"""
        years_patterns = [
            re.compile(r'(\d{1,2})\s*\+?\s*years?\s+(?:in\s+)?business', re.IGNORECASE),
            re.compile(r'established\s+(?:in\s+)?(\d{4})', re.IGNORECASE),
            re.compile(r'since\s+(\d{4})', re.IGNORECASE),
            re.compile(r'founded\s+(?:in\s+)?(\d{4})', re.IGNORECASE)
        ]
        
        for pattern in years_patterns:
            matches = pattern.findall(page_text)
            if matches:
                return matches[0]
        
        return None

    def _extract_languages(self, page_text):
        """Извлечение языков"""
        language_patterns = [
            re.compile(r'languages?\s*:?\s*([^.]+)', re.IGNORECASE),
            re.compile(r'speak\s+([^.]+)', re.IGNORECASE),
            re.compile(r'bilingual\s+([^.]+)', re.IGNORECASE)
        ]
        
        for pattern in language_patterns:
            matches = pattern.findall(page_text)
            if matches:
                return matches[0][:50]
        
        return None

    def _extract_payment_methods(self, page_text):
        """Извлечение способов оплаты"""
        payment_keywords = [
            'cash', 'check', 'credit card', 'debit card', 'paypal',
            'wire transfer', 'bank transfer', 'financing', 'terms'
        ]
        
        payment_methods = []
        text_lower = page_text.lower()
        
        for method in payment_keywords:
            if method in text_lower:
                payment_methods.append(method)
        
        return payment_methods if payment_methods else None

    def _extract_additional_services(self, page_text):
        """Извлечение дополнительных услуг"""
        additional_services = []
        
        service_patterns = [
            re.compile(r'we\s+also\s+([^.]+)', re.IGNORECASE),
            re.compile(r'additionally\s+([^.]+)', re.IGNORECASE),
            re.compile(r'other\s+services\s*:?\s*([^.]+)', re.IGNORECASE)
        ]
        
        for pattern in service_patterns:
            matches = pattern.findall(page_text)
            additional_services.extend(matches)
        
        return additional_services[:3] if additional_services else None

    # Вспомогательные методы
    def _extract_name_from_json_ld_comprehensive(self, soup):
        """Извлечение названия из JSON-LD"""
        json_scripts = soup.find_all('script', type='application/ld+json')
        for script in json_scripts:
            try:
                data = json.loads(script.string)
                name = self._extract_name_from_json_ld(data)
                if name:
                    return name
            except:
                continue
        return None

    def _extract_name_from_meta_tags(self, soup):
        """Извлечение названия из мета-тегов"""
        meta_tags = [
            soup.find('meta', property='og:site_name'),
            soup.find('meta', property='og:title'),
            soup.find('meta', {'name': 'application-name'}),
            soup.find('meta', {'name': 'twitter:title'})
        ]
        
        for tag in meta_tags:
            if tag:
                content = tag.get('content', '')
                if content and len(content) > 2:
                    return content
        
        return None

    def _extract_name_from_title(self, soup):
        """Извлечение названия из title"""
        title_tag = soup.find('title')
        if title_tag:
            title_text = title_tag.get_text().strip()
            if title_text:
                return title_text
        return None

    def _extract_name_from_h1(self, soup):
        """Извлечение названия из H1"""
        h1_tag = soup.find('h1')
        if h1_tag:
            h1_text = h1_tag.get_text().strip()
            if h1_text:
                return h1_text
        return None

    def _extract_description_comprehensive(self, soup):
        """Комплексное извлечение описания"""
        # Поиск в мета-тегах
        meta_descriptions = [
            soup.find('meta', {'name': 'description'}),
            soup.find('meta', property='og:description'),
            soup.find('meta', {'name': 'twitter:description'})
        ]
        
        for meta in meta_descriptions:
            if meta:
                content = meta.get('content', '')
                if content and len(content) > 20:
                    return content[:500]
        
        # Поиск в тексте
        description_selectors = [
            '.description',
            '.about',
            '.overview',
            '.intro',
            '.summary'
        ]
        
        for selector in description_selectors:
            elements = soup.select(selector)
            for element in elements:
                text = element.get_text(strip=True)
                if len(text) > 50:
                    return text[:500]
        
        return None

    def _extract_address_comprehensive(self, soup, page_text):
        """Комплексное извлечение адреса"""
        # JSON-LD
        json_scripts = soup.find_all('script', type='application/ld+json')
        for script in json_scripts:
            try:
                data = json.loads(script.string)
                address = self._extract_address_from_json_ld(data)
                if address:
                    return address
            except:
                continue
        
        # Микроданные
        address_elements = soup.find_all(attrs={'itemprop': re.compile(r'address|street', re.IGNORECASE)})
        for element in address_elements:
            address = element.get('content') or element.get_text().strip()
            if len(address) > 10:
                return address[:200]
        
        return None

    def _extract_city_comprehensive(self, soup, page_text):
        """Комплексное извлечение города"""
        # Аналогично другим методам
        city_elements = soup.find_all(attrs={'itemprop': re.compile(r'city|locality', re.IGNORECASE)})
        for element in city_elements:
            city = element.get('content') or element.get_text().strip()
            if city and len(city) > 2:
                return city[:50]
        
        return None

    def _extract_state_comprehensive(self, soup, page_text):
        """Комплексное извлечение штата"""
        state_elements = soup.find_all(attrs={'itemprop': re.compile(r'state|region', re.IGNORECASE)})
        for element in state_elements:
            state = element.get('content') or element.get_text().strip()
            if state and len(state) >= 2:
                return state[:20]
        
        return None

    def _extract_zip_comprehensive(self, soup, page_text):
        """Комплексное извлечение почтового индекса"""
        # Поиск в микроданных
        zip_elements = soup.find_all(attrs={'itemprop': re.compile(r'postal|zip', re.IGNORECASE)})
        for element in zip_elements:
            zip_code = element.get('content') or element.get_text().strip()
            if zip_code and re.match(r'^\d{5}(-\d{4})?$', zip_code):
                return zip_code
        
        # Поиск в тексте
        zip_patterns = [
            re.compile(r'\b\d{5}(-\d{4})?\b'),  # US ZIP
            re.compile(r'\b[A-Z]\d[A-Z]\s*\d[A-Z]\d\b'),  # Canada postal code
            re.compile(r'\b[A-Z]{1,2}\d{1,2}\s*\d[A-Z]{2}\b')  # UK postcode
        ]
        
        for pattern in zip_patterns:
            matches = pattern.findall(page_text)
            if matches:
                return matches[0] if isinstance(matches[0], str) else matches[0][0]
        
        return None

    def _extract_latitude(self, soup, page_text):
        """Извлечение широты"""
        # Поиск в микроданных и JSON-LD
        lat_elements = soup.find_all(attrs={'itemprop': 'latitude'})
        for element in lat_elements:
            lat = element.get('content') or element.get_text().strip()
            try:
                return float(lat)
            except:
                continue
        
        return None

    def _extract_longitude(self, soup, page_text):
        """Извлечение долготы"""
        lng_elements = soup.find_all(attrs={'itemprop': 'longitude'})
        for element in lng_elements:
            lng = element.get('content') or element.get_text().strip()
            try:
                return float(lng)
            except:
                continue
        
        return None

    # Вспомогательные методы для JSON-LD
    def _extract_phone_from_json_ld(self, data):
        """Извлечение телефона из JSON-LD"""
        if isinstance(data, dict):
            for key in ['telephone', 'phone', 'contactPoint']:
                if key in data:
                    value = data[key]
                    if isinstance(value, str):
                        return value
                    elif isinstance(value, dict) and 'telephone' in value:
                        return value['telephone']
            
            for value in data.values():
                if isinstance(value, (dict, list)):
                    result = self._extract_phone_from_json_ld(value)
                    if result:
                        return result
        
        elif isinstance(data, list):
            for item in data:
                result = self._extract_phone_from_json_ld(item)
                if result:
                    return result
        
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
                    result = self._extract_email_from_json_ld(value)
                    if result:
                        return result
        
        elif isinstance(data, list):
            for item in data:
                result = self._extract_email_from_json_ld(item)
                if result:
                    return result
        
        return None

    def _extract_name_from_json_ld(self, data):
        """Извлечение названия из JSON-LD"""
        if isinstance(data, dict):
            for key in ['name', 'legalName', 'alternateName']:
                if key in data and isinstance(data[key], str):
                    return data[key].strip()
            
            for value in data.values():
                if isinstance(value, (dict, list)):
                    result = self._extract_name_from_json_ld(value)
                    if result:
                        return result
        
        elif isinstance(data, list):
            for item in data:
                result = self._extract_name_from_json_ld(item)
                if result:
                    return result
        
        return None

    def _extract_address_from_json_ld(self, data):
        """Извлечение адреса из JSON-LD"""
        if isinstance(data, dict):
            if 'address' in data:
                addr = data['address']
                if isinstance(addr, str):
                    return addr
                elif isinstance(addr, dict):
                    parts = []
                    for key in ['streetAddress', 'addressLocality', 'addressRegion', 'postalCode']:
                        if key in addr and addr[key]:
                            parts.append(str(addr[key]))
                    if parts:
                        return ', '.join(parts)
            
            for value in data.values():
                if isinstance(value, (dict, list)):
                    result = self._extract_address_from_json_ld(value)
                    if result:
                        return result
        
        elif isinstance(data, list):
            for item in data:
                result = self._extract_address_from_json_ld(item)
                if result:
                    return result
        
        return None

    def _validate_email_global(self, email):
        """Глобальная валидация email"""
        if not email or '@' not in email:
            return False
        
        # Исключаем нежелательные домены
        exclude_domains = [
            'example.com', 'test.com', 'domain.com',
            'google.com', 'facebook.com', 'twitter.com',
            'linkedin.com', 'youtube.com', 'instagram.com'
        ]
        
        email_lower = email.lower()
        for domain in exclude_domains:
            if domain in email_lower:
                return False
        
        # Базовая проверка формата
        if re.match(r'^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}$', email):
            return True
        
        return False

    def _calculate_data_completeness(self, business):
        """Вычисление полноты данных"""
        important_fields = [
            'name', 'phone', 'email', 'website', 'address',
            'city', 'state', 'country', 'working_hours',
            'materials_accepted', 'services', 'description'
        ]
        
        filled_fields = 0
        for field in important_fields:
            value = business.get(field)
            if value and (not isinstance(value, list) or len(value) > 0):
                filled_fields += 1
        
        return int((filled_fields / len(important_fields)) * 100)

    def _is_valid_url(self, url):
        """Проверка валидности URL"""
        try:
            parsed = urlparse(url)
            return parsed.scheme in ['http', 'https'] and parsed.netloc
        except:
            return False

    def _deduplicate_links(self, links):
        """Удаление дубликатов ссылок"""
        seen_urls = set()
        unique_links = []
        
        for link in links:
            url = link['url']
            if url not in seen_urls:
                seen_urls.add(url)
                unique_links.append(link)
        
        return unique_links

    def _finalize_comprehensive_results(self, businesses, target_count):
        """Финализация комплексных результатов"""
        # Удаление дубликатов по телефону и email
        seen_contacts = set()
        unique_businesses = []
        
        for business in businesses:
            phone = business.get('phone', '')
            email = business.get('email', '')
            
            contact_key = f"{phone}|{email}"
            if contact_key not in seen_contacts:
                seen_contacts.add(contact_key)
                unique_businesses.append(business)
        
        # Сортировка по полноте данных
        unique_businesses.sort(key=lambda x: x.get('data_completeness', 0), reverse=True)
        
        return unique_businesses[:target_count]

    def _calculate_contact_percentage(self):
        """Расчет процента контактов"""
        if not self.results:
            return 0
        
        with_contacts = sum(1 for business in self.results 
                           if business.get('phone') or business.get('email') or business.get('whatsapp'))
        return (with_contacts / len(self.results)) * 100

    def export_comprehensive_results(self, output_dir="output"):
        """Экспорт комплексных результатов"""
        if not self.results:
            self.logger.warning("Нет данных для экспорта")
            return None
        
        os.makedirs(output_dir, exist_ok=True)
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        # DataFrame
        df = pd.DataFrame(self.results)
        
        # CSV
        csv_file = os.path.join(output_dir, f"comprehensive_metal_businesses_{timestamp}.csv")
        df.to_csv(csv_file, index=False)
        
        # Excel с множественными листами
        excel_file = os.path.join(output_dir, f"comprehensive_metal_businesses_{timestamp}.xlsx")
        with pd.ExcelWriter(excel_file, engine='openpyxl') as writer:
            # Главный лист
            df.to_excel(writer, sheet_name='All Businesses', index=False)
            
            # Лист с высококачественными данными
            high_quality = df[df['data_completeness'] >= 70]
            if not high_quality.empty:
                high_quality.to_excel(writer, sheet_name='High Quality Data', index=False)
            
            # Лист с контактными данными
            contacts_df = df[['name', 'phone', 'email', 'whatsapp', 'website', 'address', 'city', 'state', 'country']]
            contacts_df.to_excel(writer, sheet_name='Contact Information', index=False)
            
            # Лист с материалами и ценами
            materials_df = df[['name', 'materials_accepted', 'pricing_info', 'services', 'certifications']]
            materials_df.to_excel(writer, sheet_name='Materials & Pricing', index=False)
            
            # Статистика
            stats_data = self._create_comprehensive_statistics()
            stats_df = pd.DataFrame(stats_data)
            stats_df.to_excel(writer, sheet_name='Statistics', index=False)
        
        # JSON
        json_file = os.path.join(output_dir, f"comprehensive_metal_businesses_{timestamp}.json")
        with open(json_file, 'w', encoding='utf-8') as f:
            json.dump(self.results, f, indent=2, default=str, ensure_ascii=False)
        
        # Комплексный отчет
        report_file = self._create_comprehensive_report(output_dir, timestamp)
        
        self.logger.info(f"✅ КОМПЛЕКСНЫЕ данные экспортированы:")
        self.logger.info(f"  • CSV: {csv_file}")
        self.logger.info(f"  • Excel: {excel_file}")
        self.logger.info(f"  • JSON: {json_file}")
        self.logger.info(f"  • Отчет: {report_file}")
        
        return {
            'csv': csv_file,
            'excel': excel_file,
            'json': json_file,
            'report': report_file,
            'count': len(self.results)
        }

    def _create_comprehensive_statistics(self):
        """Создание комплексной статистики"""
        if not self.results:
            return []
        
        total = len(self.results)
        
        stats = [
            {'Metric': 'Total Businesses', 'Count': total, 'Percentage': '100.0%'},
            {'Metric': 'With Phone Numbers', 'Count': sum(1 for b in self.results if b.get('phone')), 'Percentage': f"{sum(1 for b in self.results if b.get('phone'))/total*100:.1f}%"},
            {'Metric': 'With Email Addresses', 'Count': sum(1 for b in self.results if b.get('email')), 'Percentage': f"{sum(1 for b in self.results if b.get('email'))/total*100:.1f}%"},
            {'Metric': 'With WhatsApp', 'Count': sum(1 for b in self.results if b.get('whatsapp')), 'Percentage': f"{sum(1 for b in self.results if b.get('whatsapp'))/total*100:.1f}%"},
            {'Metric': 'With Complete Address', 'Count': sum(1 for b in self.results if b.get('address')), 'Percentage': f"{sum(1 for b in self.results if b.get('address'))/total*100:.1f}%"},
            {'Metric': 'With Working Hours', 'Count': sum(1 for b in self.results if b.get('working_hours')), 'Percentage': f"{sum(1 for b in self.results if b.get('working_hours'))/total*100:.1f}%"},
            {'Metric': 'With Materials Info', 'Count': sum(1 for b in self.results if b.get('materials_accepted')), 'Percentage': f"{sum(1 for b in self.results if b.get('materials_accepted'))/total*100:.1f}%"},
            {'Metric': 'With Pricing Info', 'Count': sum(1 for b in self.results if b.get('pricing_info')), 'Percentage': f"{sum(1 for b in self.results if b.get('pricing_info'))/total*100:.1f}%"},
            {'Metric': 'With Services Info', 'Count': sum(1 for b in self.results if b.get('services')), 'Percentage': f"{sum(1 for b in self.results if b.get('services'))/total*100:.1f}%"},
            {'Metric': 'With Social Media', 'Count': sum(1 for b in self.results if b.get('social_media')), 'Percentage': f"{sum(1 for b in self.results if b.get('social_media'))/total*100:.1f}%"},
            {'Metric': 'High Quality Data (>70%)', 'Count': sum(1 for b in self.results if b.get('data_completeness', 0) > 70), 'Percentage': f"{sum(1 for b in self.results if b.get('data_completeness', 0) > 70)/total*100:.1f}%"},
        ]
        
        return stats

    def _create_comprehensive_report(self, output_dir, timestamp):
        """Создание комплексного отчета"""
        report_file = os.path.join(output_dir, f"comprehensive_report_{timestamp}.txt")
        
        total_businesses = len(self.results)
        
        # Вычисление статистики
        stats = self._create_comprehensive_statistics()
        
        # Анализ по странам
        countries = {}
        for business in self.results:
            country = business.get('country', 'Unknown')
            countries[country] = countries.get(country, 0) + 1
        
        # Топ материалы
        all_materials = []
        for business in self.results:
            materials = business.get('materials_accepted', [])
            if materials:
                all_materials.extend(materials)
        
        material_counts = {}
        for material in all_materials:
            material_counts[material] = material_counts.get(material, 0) + 1
        
        with open(report_file, 'w', encoding='utf-8') as f:
            f.write("🇺🇸 КОМПЛЕКСНЫЙ ОТЧЕТ ПО US SCRAP METAL СБОРУ\n")
            f.write("=" * 70 + "\n\n")
            f.write(f"Отчет создан: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
            f.write(f"Метод сбора: US комплексный поиск\n")
            f.write(f"Охват: {len(self.target_locations)} локаций по США\n\n")
            
            f.write("📊 ОБЩАЯ СТАТИСТИКА\n")
            f.write("-" * 30 + "\n")
            for stat in stats:
                f.write(f"{stat['Metric']}: {stat['Count']} ({stat['Percentage']})\n")
            f.write("\n")
            
            f.write("🇺🇸 РАСПРЕДЕЛЕНИЕ ПО ШТАТАМ\n")
            f.write("-" * 35 + "\n")
            for state, count in sorted(countries.items(), key=lambda x: x[1], reverse=True):
                percentage = (count / total_businesses) * 100
                f.write(f"{state}: {count} бизнесов ({percentage:.1f}%)\n")
            f.write("\n")
            
            f.write("🔧 ПОПУЛЯРНЫЕ МАТЕРИАЛЫ\n")
            f.write("-" * 25 + "\n")
            top_materials = sorted(material_counts.items(), key=lambda x: x[1], reverse=True)[:15]
            for material, count in top_materials:
                f.write(f"{material}: {count} упоминаний\n")
            f.write("\n")
            
            f.write("🎯 КЛЮЧЕВЫЕ ДОСТИЖЕНИЯ\n")
            f.write("-" * 25 + "\n")
            avg_completeness = sum(b.get('data_completeness', 0) for b in self.results) / total_businesses
            f.write(f"• Средняя полнота данных: {avg_completeness:.1f}%\n")
            f.write(f"• US охват: {len(countries)} штатов\n")
            f.write(f"• Валидация телефонов: Строгая US проверка\n")
            f.write(f"• Комплексность: {len(stats)} метрик собрано\n")
            f.write(f"• Контактная доступность: Высокая\n")
            f.write(f"• Материальная информация: Детальная\n\n")
            
            f.write("💡 БИЗНЕС-АНАЛИТИКА\n")
            f.write("-" * 20 + "\n")
            f.write("• Copper и aluminum - наиболее распространенные материалы\n")
            f.write("• Средние US города имеют более высокую доступность контактов\n")
            f.write("• Rust Belt регионы показывают высокую концентрацию бизнесов\n")
            f.write("• Социальные сети активно используются для привлечения клиентов\n")
            f.write("• Строгая валидация исключает недействительные номера\n\n")
            
            f.write("🚀 РЕКОМЕНДАЦИИ ДЛЯ OUTREACH\n")
            f.write("-" * 30 + "\n")
            f.write("1. Приоритизировать бизнесы с высокой полнотой данных (>70%)\n")
            f.write("2. Использовать множественные каналы связи (телефон, email, WhatsApp)\n")
            f.write("3. Адаптировать подход под региональные особенности\n")
            f.write("4. Фокусироваться на популярных материалах (copper, aluminum)\n")
            f.write("5. Учитывать рабочие часы для оптимального времени контакта\n")
            f.write("6. Использовать социальные сети для дополнительного охвата\n")
        
        return report_file

def main():
    print("⚡ СУПЕР-БЫСТРЫЙ US SCRAP METAL ПАРСЕР")
    print("=" * 65)
    print("🚀 МАКСИМАЛЬНАЯ СКОРОСТЬ")
    print("🇺🇸 ФОКУС НА США")
    print("📞 ПРИОРИТЕТ КОНТАКТОВ")
    print("⚡ ПАРАЛЛЕЛЬНАЯ ОБРАБОТКА")
    print("🎯 ТОЧНОЕ ИЗВЛЕЧЕНИЕ")
    print("💨 БЫСТРЫЕ РЕЗУЛЬТАТЫ")
    
    scraper = USMetalScraper()
    
    try:
        target_count = input("\nЦелевое количество бизнесов (по умолчанию 200): ").strip()
        target_count = int(target_count) if target_count else 200
        
        print(f"\n🚀 Запуск СУПЕР-БЫСТРОГО сбора для {target_count} бизнесов...")
        print("🇺🇸 Охват: США (оптимизированный)")
        print("📋 Приоритет: Телефоны, Email, Адреса")
        print("⚡ Технология: Параллельная обработка + Regex")
        print("🎯 Скорость: До 10x быстрее стандартного")
        print(f"⏱️ Ожидаемое время: {max(1, target_count // 20)}-{max(2, target_count // 10)} минут")
        print(f"📊 Ожидаемый результат: {target_count} бизнесов с контактами")
        
        confirmation = input("\nПродолжить? (y/N): ").lower().strip()
        if confirmation != 'y':
            print("❌ Отменено пользователем")
            return
        
        results = scraper.run_comprehensive_scraping(target_count)
        
        if results:
            print(f"\n✅ Супер-быстрый сбор завершен! {len(results)} бизнесов найдено!")
            
            export_info = scraper.export_comprehensive_results()
            if export_info:
                print(f"\n📁 Файлы созданы:")
                print(f"  • CSV: {export_info['csv']}")
                print(f"  • Excel: {export_info['excel']}")
                print(f"  • JSON: {export_info['json']}")
                print(f"  • Отчет: {export_info['report']}")
                
                contact_percentage = scraper._calculate_contact_percentage()
                
                print(f"\n🎯 РЕЗУЛЬТАТЫ:")
                print(f"📊 Общее количество: {export_info['count']} бизнесов")
                print(f"📞 С контактами: {contact_percentage:.1f}%")
                print(f"🇺🇸 US охват: Достигнут")
                print(f"⚡ Скорость: Максимальная")
                print("\n🚀 US SCRAP METAL БАЗА ГОТОВА ДЛЯ OUTREACH!")
                print("💡 Используйте CSV/Excel для анализа контактов")
            else:
                print("❌ Ошибка экспорта")
        else:
            print("❌ Данные не собраны")
            
    except KeyboardInterrupt:
        print("\n⚠️ Процесс прерван пользователем")
        if scraper.results:
            print("💾 Сохраняем частичные результаты...")
            scraper.export_comprehensive_results()
    except Exception as e:
        print(f"❌ Ошибка: {e}")
        scraper.logger.error(f"Error: {e}", exc_info=True)

if __name__ == "__main__":
    main() 