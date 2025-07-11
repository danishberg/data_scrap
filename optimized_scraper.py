#!/usr/bin/env python3
"""
ОПТИМИЗИРОВАННЫЙ ПАРСЕР - Быстрый сбор контактов для scrap metal центров
ПРИОРИТЕТ: ТЕЛЕФОНЫ превыше всего!
СТРАТЕГИЯ: OSM + агрессивный поиск телефонов + полуавтоматический Google
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
from concurrent.futures import ThreadPoolExecutor, as_completed

class OptimizedScraper:
    def __init__(self):
        self.session = requests.Session()
        self.results = []
        self.logger = self._setup_logging()
        
        # КРИТИЧЕСКИ ВАЖНО: минимум телефонов
        self.MIN_PHONES_PERCENTAGE = 80  # Цель: 80% с телефонами
        
        # Простые и эффективные паттерны телефонов
        self.phone_patterns = [
            re.compile(r'(\d{3})[^\d]*(\d{3})[^\d]*(\d{4})'),  # Основной паттерн
            re.compile(r'tel:[\s]*\+?1?[\s]*(\d{3})[\s]*(\d{3})[\s]*(\d{4})'),  # tel: ссылки
            re.compile(r'phone[\s]*:[\s]*(\d{3})[\s]*[^\d]*(\d{3})[\s]*[^\d]*(\d{4})'),  # phone:
            re.compile(r'call[\s]*:?[\s]*(\d{3})[\s]*[^\d]*(\d{3})[\s]*[^\d]*(\d{4})')   # call:
        ]
        
        # JavaScript скрипт пользователя (улучшенная версия)
        self.google_js_script = """
// УЛУЧШЕННЫЙ GOOGLE ПАРСЕР (основан на скрипте пользователя)
javascript:!(function(){
    console.log('🔍 Запуск извлечения Google ссылок...');
    window.scrollTo(0, document.body.scrollHeight);
    
    var win = window.open('', 'ScrapMetalLinks', 'width=1000,height=800,scrollbars=yes');
    win.document.write('<html><head><title>Scrap Metal Links</title></head><body>');
    win.document.write('<h2>📞 Найденные scrap metal компании</h2>');
    win.document.write('<p>Время: ' + new Date().toLocaleString() + '</p>');
    
    var results = [];
    var processed = new Set();
    
    // Ищем все результаты Google
    [].forEach.call(document.getElementsByClassName('MjjYud'), function(item, index) {
        var link = item.querySelector('a');
        var href = link ? (link.getAttribute('data-href') || link.getAttribute('href')) : null;
        var title = link ? link.querySelector('h3') : null;
        var desc = item.querySelector('.VwiC3b') || item.querySelector('.s3v9rd');
        
        if (href && title && !processed.has(href)) {
            processed.add(href);
            var titleText = title.innerText || title.textContent || '';
            var descText = desc ? (desc.innerText || desc.textContent || '') : '';
            
            // Проверяем релевантность для scrap metal
            var text = (titleText + ' ' + descText).toLowerCase();
            var isRelevant = false;
            var keywords = ['scrap', 'metal', 'recycling', 'salvage', 'junk', 'steel', 'copper', 'aluminum'];
            
            for (var i = 0; i < keywords.length; i++) {
                if (text.indexOf(keywords[i]) !== -1) {
                    isRelevant = true;
                    break;
                }
            }
            
            if (isRelevant && href.indexOf('http') === 0) {
                results.push({
                    url: href,
                    title: titleText.substring(0, 100),
                    description: descText.substring(0, 200),
                    position: index + 1
                });
                
                // Показываем в окне
                win.document.write('<div style="margin:10px; padding:10px; border:1px solid #ccc;">');
                win.document.write('<strong>' + (index + 1) + '. ' + titleText + '</strong><br>');
                win.document.write('<a href="' + href + '" target="_blank">' + href + '</a><br>');
                win.document.write('<small>' + descText.substring(0, 150) + '</small>');
                win.document.write('</div>');
            }
        }
    });
    
    // JSON для экспорта
    win.document.write('<hr><h3>JSON данные для парсера:</h3>');
    win.document.write('<textarea rows="15" cols="100" onclick="this.select()">' + 
                      JSON.stringify(results, null, 2) + '</textarea>');
    win.document.write('<p><strong>Найдено релевантных ссылок: ' + results.length + '</strong></p>');
    win.document.write('<p>Скопируйте JSON и сохраните в файл google_links.json</p>');
    win.document.write('</body></html>');
    
    console.log('✅ Найдено ссылок:', results.length);
    return results;
})();
        """

    def _setup_logging(self):
        logger = logging.getLogger('OptimizedScraper')
        logger.setLevel(logging.INFO)
        
        if not logger.handlers:
            handler = logging.StreamHandler()
            formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
            handler.setFormatter(formatter)
            logger.addHandler(handler)
        
        return logger

    def collect_with_contact_priority(self, target_count=100):
        """Быстрый сбор с приоритетом контактов"""
        self.logger.info(f"⚡ БЫСТРЫЙ СБОР для {target_count} бизнесов")
        self.logger.info(f"🎯 ЦЕЛЬ: минимум {self.MIN_PHONES_PERCENTAGE}% с телефонами")
        
        # Этап 1: Быстрый сбор из OSM
        osm_businesses = self._fast_osm_collection(target_count)
        self.logger.info(f"📍 OSM: {len(osm_businesses)} бизнесов")
        
        # Этап 2: Агрессивный поиск телефонов для найденных бизнесов
        enhanced_businesses = self._aggressive_phone_mining(osm_businesses)
        
        # Этап 3: Проверяем процент с телефонами
        phone_percentage = self._calculate_phone_percentage(enhanced_businesses)
        self.logger.info(f"📞 Результат: {phone_percentage:.1f}% с телефонами")
        
        # Этап 4: Если недостаточно, показываем инструкции для Google
        if phone_percentage < self.MIN_PHONES_PERCENTAGE:
            self._show_google_instructions()
            google_file = "google_links.json"
            if os.path.exists(google_file):
                self.logger.info(f"📂 Найден файл {google_file}, обрабатываем...")
                google_businesses = self._process_google_links(google_file)
                enhanced_businesses.extend(google_businesses)
        
        self.results = enhanced_businesses[:target_count]
        final_percentage = self._calculate_phone_percentage(self.results)
        self.logger.info(f"✅ ИТОГО: {len(self.results)} бизнесов, {final_percentage:.1f}% с телефонами")
        
        return self.results

    def _fast_osm_collection(self, target_count):
        """Быстрый сбор из OSM без лишних запросов"""
        businesses = []
        
        # Сосредотачиваемся на самых продуктивных регионах
        productive_bboxes = [
            "42.23,-83.29,42.45,-82.91",  # Detroit (много металлургии)
            "41.49,-87.92,42.02,-87.52",  # Chicago 
            "40.60,-74.30,40.90,-73.90",  # New York
            "39.72,-75.28,40.14,-74.95",  # Philadelphia
            "29.52,-95.67,30.11,-95.07",  # Houston
        ]
        
        base_url = "https://overpass-api.de/api/interpreter"
        
        # Простой и эффективный запрос
        query_template = """
        [out:json][timeout:30];
        (
          node["shop"="scrap_yard"]({bbox});
          node["amenity"="recycling"]({bbox});
          node["industrial"="scrap_yard"]({bbox});
          node[name~"scrap|metal|recycling|salvage",i]({bbox});
        );
        out center tags;
        """
        
        for i, bbox in enumerate(productive_bboxes):
            try:
                self.logger.info(f"🔍 Регион {i+1}/{len(productive_bboxes)}")
                query = query_template.format(bbox=bbox)
                
                response = self._make_request(base_url, data=query, method='POST')
                if response:
                    data = response.json()
                    region_businesses = self._parse_osm_fast(data)
                    businesses.extend(region_businesses)
                    
                    phones_found = sum(1 for b in region_businesses if b.get('phone'))
                    self.logger.info(f"  ✅ +{len(region_businesses)} бизнесов, {phones_found} с телефонами")
                
                time.sleep(2)  # Быстрые запросы
                
                if len(businesses) >= target_count:
                    break
                    
            except Exception as e:
                self.logger.warning(f"❌ Ошибка в регионе {i+1}: {e}")
                continue
        
        return businesses

    def _parse_osm_fast(self, data):
        """Быстрый парсинг OSM с фокусом на телефоны"""
        businesses = []
        
        for element in data.get('elements', []):
            tags = element.get('tags', {})
            
            name = tags.get('name', '').strip()
            if not name or len(name) < 3:
                continue
            
            # Координаты
            lat = element.get('lat') or (element.get('center', {}).get('lat'))
            lon = element.get('lon') or (element.get('center', {}).get('lon'))
            
            # АГРЕССИВНЫЙ поиск телефона в тегах
            phone = self._extract_phone_from_tags(tags)
            
            business = {
                'name': name,
                'address': self._build_address(tags),
                'city': tags.get('addr:city', ''),
                'state': tags.get('addr:state', ''),
                'zip_code': tags.get('addr:postcode', ''),
                'phone': phone,
                'website': tags.get('website', tags.get('contact:website', '')),
                'email': tags.get('email', tags.get('contact:email', '')),
                'latitude': lat,
                'longitude': lon,
                'source': 'OSM_Fast',
                'has_phone': bool(phone),
                'scraped_at': datetime.now().isoformat()
            }
            
            businesses.append(business)
        
        # Сортируем: сначала с телефонами
        businesses.sort(key=lambda x: x.get('has_phone', False), reverse=True)
        return businesses

    def _extract_phone_from_tags(self, tags):
        """Агрессивное извлечение телефона из всех OSM полей"""
        phone_fields = [
            'phone', 'contact:phone', 'telephone', 'contact:telephone',
            'fax', 'contact:fax', 'mobile', 'contact:mobile'
        ]
        
        for field in phone_fields:
            if field in tags and tags[field]:
                phone = self._clean_phone(tags[field])
                if phone:
                    return phone
        
        return ""

    def _aggressive_phone_mining(self, businesses):
        """Агрессивный поиск телефонов для бизнесов без контактов"""
        self.logger.info(f"📞 Агрессивный поиск телефонов для {len(businesses)} бизнесов")
        
        enhanced = []
        
        for business in businesses:
            if business.get('phone'):  # Уже есть телефон
                enhanced.append(business)
                continue
            
            # Пытаемся найти телефон разными способами
            enhanced_business = business.copy()
            
            # Способ 1: Поиск через название + город
            phone = self._search_phone_by_name(business)
            if phone:
                enhanced_business['phone'] = phone
                enhanced_business['phone_source'] = 'name_search'
                enhanced_business['has_phone'] = True
            
            # Способ 2: Если есть сайт, парсим его
            elif enhanced_business.get('website'):
                phone = self._scrape_website_for_phone(enhanced_business['website'])
                if phone:
                    enhanced_business['phone'] = phone
                    enhanced_business['phone_source'] = 'website'
                    enhanced_business['has_phone'] = True
            
            enhanced.append(enhanced_business)
        
        phones_found = sum(1 for b in enhanced if b.get('phone'))
        self.logger.info(f"✅ Найдено дополнительно телефонов: {phones_found - sum(1 for b in businesses if b.get('phone'))}")
        
        return enhanced

    def _search_phone_by_name(self, business):
        """Поиск телефона через простой Google поиск"""
        try:
            name = business.get('name', '')
            city = business.get('city', '')
            state = business.get('state', '')
            
            if not name:
                return ""
            
            # Простой поисковый запрос
            query = f'"{name}" {city} {state} phone contact'
            search_url = f"https://www.google.com/search?q={quote_plus(query)}"
            
            response = self._make_request(search_url)
            if response:
                # Ищем телефоны в HTML
                for pattern in self.phone_patterns:
                    matches = pattern.findall(response.text)
                    for match in matches:
                        phone = self._format_phone_match(match)
                        if phone:
                            return phone
            
        except Exception as e:
            self.logger.debug(f"Поиск телефона по имени неудачен: {e}")
        
        return ""

    def _scrape_website_for_phone(self, website):
        """Быстрый парсинг сайта для поиска телефона"""
        try:
            response = self._make_request(website, timeout=10)
            if not response:
                return ""
            
            # Ищем телефоны в HTML и тексте
            soup = BeautifulSoup(response.text, 'html.parser')
            
            # Ищем tel: ссылки
            tel_links = soup.find_all('a', href=lambda x: x and x.startswith('tel:'))
            for link in tel_links:
                phone = self._clean_phone(link.get('href', '').replace('tel:', ''))
                if phone:
                    return phone
            
            # Ищем в тексте страницы
            page_text = response.text
            for pattern in self.phone_patterns:
                matches = pattern.findall(page_text)
                for match in matches:
                    phone = self._format_phone_match(match)
                    if phone:
                        return phone
            
        except Exception as e:
            self.logger.debug(f"Парсинг сайта неудачен: {e}")
        
        return ""

    def _show_google_instructions(self):
        """Показываем инструкции для Google парсинга"""
        print("\n" + "="*70)
        print("🔍 НУЖНЫ ДОПОЛНИТЕЛЬНЫЕ КОНТАКТЫ - GOOGLE ПАРСИНГ")
        print("="*70)
        print("1. Откройте браузер и перейдите на google.com")
        print("2. Введите запрос: scrap metal recycling \"ваш город\"")
        print("3. Перейдите на страницы 2-5 (там компании с низкими позициями)")
        print("4. Откройте Developer Tools (F12) → Console")
        print("5. Скопируйте и вставьте этот JavaScript:")
        print("\n" + "-"*50)
        print(self.google_js_script)
        print("-"*50)
        print("\n6. Скопируйте JSON из окна и сохраните как 'google_links.json'")
        print("7. Перезапустите парсер - он автоматически обработает ссылки")
        print("="*70)

    def _process_google_links(self, json_file):
        """Обрабатываем ссылки из Google"""
        businesses = []
        
        try:
            with open(json_file, 'r', encoding='utf-8') as f:
                google_links = json.load(f)
            
            self.logger.info(f"🔍 Обработка {len(google_links)} ссылок из Google")
            
            for link_data in google_links:
                url = link_data.get('url', '')
                if not url:
                    continue
                
                business = self._extract_business_from_url(url, link_data)
                if business and business.get('phone'):  # Только с телефонами
                    businesses.append(business)
            
            self.logger.info(f"✅ Извлечено {len(businesses)} бизнесов с телефонами из Google")
            
        except Exception as e:
            self.logger.error(f"Ошибка обработки Google ссылок: {e}")
        
        return businesses

    def _extract_business_from_url(self, url, link_data):
        """Извлекаем данные бизнеса с сайта"""
        try:
            response = self._make_request(url, timeout=15)
            if not response:
                return None
            
            soup = BeautifulSoup(response.text, 'html.parser')
            
            # Ищем телефон
            phone = self._scrape_website_for_phone(url)
            if not phone:  # Если нет телефона, пропускаем
                return None
            
            # Извлекаем остальные данные
            business = {
                'name': link_data.get('title', 'Unknown Business'),
                'website': url,
                'phone': phone,
                'email': self._extract_email_from_page(soup),
                'address': self._extract_address_from_page(soup),
                'source': 'Google_Links',
                'google_position': link_data.get('position', 0),
                'has_phone': True,
                'scraped_at': datetime.now().isoformat()
            }
            
            return business
            
        except Exception as e:
            self.logger.debug(f"Ошибка извлечения с {url}: {e}")
            return None

    def _extract_email_from_page(self, soup):
        """Быстрое извлечение email"""
        email_pattern = re.compile(r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b')
        
        # Ищем в тексте
        text = soup.get_text()
        emails = email_pattern.findall(text)
        
        # Фильтруем
        for email in emails:
            if not any(skip in email.lower() for skip in ['example.com', 'test.com', 'google.com']):
                return email
        
        return ""

    def _extract_address_from_page(self, soup):
        """Быстрое извлечение адреса"""
        selectors = ['.address', '.location', '[itemtype*="PostalAddress"]']
        
        for selector in selectors:
            element = soup.select_one(selector)
            if element:
                address = element.get_text().strip()
                if any(word in address.lower() for word in ['street', 'ave', 'road', 'drive']):
                    return address[:150]
        
        return ""

    def _format_phone_match(self, match):
        """Форматируем найденный телефон"""
        if isinstance(match, tuple) and len(match) >= 3:
            return f"({match[0]}) {match[1]}-{match[2]}"
        elif isinstance(match, str):
            return self._clean_phone(match)
        return ""

    def _clean_phone(self, phone):
        """Очистка и форматирование телефона"""
        if not phone:
            return ""
        
        digits = re.sub(r'\D', '', phone)
        
        if len(digits) == 10:
            return f"({digits[:3]}) {digits[3:6]}-{digits[6:]}"
        elif len(digits) == 11 and digits[0] == '1':
            return f"({digits[1:4]}) {digits[4:7]}-{digits[7:]}"
        
        return ""

    def _build_address(self, tags):
        """Быстрое построение адреса"""
        parts = []
        if tags.get('addr:housenumber'):
            parts.append(tags['addr:housenumber'])
        if tags.get('addr:street'):
            parts.append(tags['addr:street'])
        return ' '.join(parts) if parts else ""

    def _calculate_phone_percentage(self, businesses):
        """Рассчитываем процент с телефонами"""
        if not businesses:
            return 0
        return (sum(1 for b in businesses if b.get('phone')) / len(businesses)) * 100

    def _make_request(self, url, params=None, data=None, method='GET', timeout=30):
        """Быстрые HTTP запросы"""
        try:
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
            }
            
            if method == 'POST':
                response = self.session.post(url, headers=headers, data=data, timeout=timeout)
            else:
                response = self.session.get(url, headers=headers, params=params, timeout=timeout)
            
            if response.status_code == 200:
                return response
            
        except Exception as e:
            self.logger.debug(f"Запрос неудачен: {e}")
        
        return None

    def export_results(self, output_dir="output"):
        """Быстрый экспорт с акцентом на контакты"""
        if not self.results:
            return None
        
        os.makedirs(output_dir, exist_ok=True)
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        # Статистика
        total = len(self.results)
        with_phones = sum(1 for b in self.results if b.get('phone'))
        phone_percentage = (with_phones / total) * 100 if total > 0 else 0
        
        # Сортируем: сначала с телефонами
        sorted_results = sorted(self.results, key=lambda x: bool(x.get('phone')), reverse=True)
        
        # CSV экспорт
        df = pd.DataFrame(sorted_results)
        csv_file = os.path.join(output_dir, f"optimized_contacts_{timestamp}.csv")
        df.to_csv(csv_file, index=False)
        
        # Простой отчет
        report_file = os.path.join(output_dir, f"contact_report_{timestamp}.txt")
        with open(report_file, 'w', encoding='utf-8') as f:
            f.write("📞 ОТЧЕТ ПО КОНТАКТАМ\n")
            f.write("=" * 40 + "\n\n")
            f.write(f"Всего бизнесов: {total}\n")
            f.write(f"С телефонами: {with_phones} ({phone_percentage:.1f}%)\n")
            f.write(f"Цель достигнута: {'✅ ДА' if phone_percentage >= self.MIN_PHONES_PERCENTAGE else '❌ НЕТ'}\n\n")
            
            if phone_percentage >= self.MIN_PHONES_PERCENTAGE:
                f.write("🎉 ОТЛИЧНО! Можно начинать звонки!\n")
            else:
                f.write("⚠️ Нужно больше контактов. Используйте Google парсинг.\n")
        
        self.logger.info(f"✅ Экспорт завершен:")
        self.logger.info(f"  • CSV: {csv_file}")
        self.logger.info(f"  • Отчет: {report_file}")
        self.logger.info(f"📊 ИТОГ: {phone_percentage:.1f}% с телефонами")
        
        return {
            'csv_file': csv_file,
            'total_businesses': total,
            'businesses_with_phones': with_phones,
            'phone_percentage': phone_percentage,
            'success': phone_percentage >= self.MIN_PHONES_PERCENTAGE
        }

def main():
    print("⚡ ОПТИМИЗИРОВАННЫЙ ПАРСЕР - Быстрый сбор контактов")
    print("=" * 60)
    print("🎯 ПРИОРИТЕТ: Максимум телефонов, минимум времени")
    print("🔧 БЕЗ SELENIUM: Только быстрые HTTP запросы")
    print("🤝 ГИБРИД: OSM + полуавтоматический Google")
    
    scraper = OptimizedScraper()
    
    try:
        target = int(input("\nКоличество бизнесов (по умолчанию 100): ") or "100")
        
        print(f"\n⚡ Запуск быстрого сбора для {target} бизнесов...")
        print("Процесс:")
        print("1. 📍 Быстрый OSM сбор из продуктивных регионов")
        print("2. 📞 Агрессивный поиск телефонов")
        print("3. 🔍 При необходимости - инструкции для Google")
        
        start_time = time.time()
        results = scraper.collect_with_contact_priority(target)
        elapsed = time.time() - start_time
        
        if results:
            phone_count = sum(1 for b in results if b.get('phone'))
            phone_percentage = (phone_count / len(results)) * 100
            
            print(f"\n✅ Сбор завершен за {elapsed:.1f} секунд!")
            print(f"📊 Результат: {len(results)} бизнесов")
            print(f"📞 С телефонами: {phone_count} ({phone_percentage:.1f}%)")
            
            export_info = scraper.export_results()
            if export_info:
                print(f"\n📁 Файлы созданы:")
                print(f"  • {export_info['csv_file']}")
                
                if export_info['success']:
                    print(f"\n🎉 УСПЕХ! Цель достигнута - можно начинать outreach!")
                else:
                    print(f"\n⚠️ Нужно больше контактов. Следуйте инструкциям для Google парсинга.")
        else:
            print("❌ Данные не собраны")
            
    except KeyboardInterrupt:
        print("\n⚠️ Прервано пользователем")
    except Exception as e:
        print(f"❌ Ошибка: {e}")

if __name__ == "__main__":
    main() 