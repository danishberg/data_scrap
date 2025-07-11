#!/usr/bin/env python3
"""
GOOGLE HELPER - Простой помощник для Google парсинга
Обрабатывает данные из JavaScript скрипта
"""

import json
import os
import requests
from bs4 import BeautifulSoup
import re
import time
import logging

class GoogleHelper:
    def __init__(self):
        self.logger = logging.getLogger('GoogleHelper')
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        })
        
        # Паттерны для поиска телефонов
        self.phone_patterns = [
            re.compile(r'tel:[\s]*\+?1?[\s]*(\d{3})[\s]*(\d{3})[\s]*(\d{4})'),
            re.compile(r'(\d{3})[^\d]*(\d{3})[^\d]*(\d{4})'),
            re.compile(r'call[\s]*:?[\s]*(\d{3})[\s]*[^\d]*(\d{3})[\s]*[^\d]*(\d{4})')
        ]

    def show_instructions(self):
        """Показать инструкции для Google парсинга"""
        print("\n" + "="*70)
        print("🔍 GOOGLE ПАРСИНГ - ИНСТРУКЦИИ")
        print("="*70)
        print("1. Откройте браузер и перейдите на google.com")
        print("2. Поиск: 'scrap metal recycling [ваш город]'")
        print("3. Перейдите на страницы 2-5 (компании с низкими позициями)")
        print("4. F12 → Console, вставьте этот JavaScript:")
        print("\n" + "-"*50)
        print(self.get_js_script())
        print("-"*50)
        print("\n5. Скопируйте JSON из окна и сохраните как 'google_links.json'")
        print("6. Запустите: python google_helper.py")
        print("="*70)

    def get_js_script(self):
        """Получить JavaScript скрипт для Google"""
        return '''
javascript:!(function(){
    console.log('🔍 Извлечение Google ссылок...');
    window.scrollTo(0, document.body.scrollHeight);
    
    var win = window.open('', 'ScrapLinks', 'width=1000,height=800');
    win.document.write('<h2>📞 Scrap Metal Links</h2>');
    
    var results = [];
    var processed = new Set();
    
    [].forEach.call(document.getElementsByClassName('MjjYud'), function(item, index) {
        var link = item.querySelector('a');
        var href = link && (link.getAttribute('data-href') || link.getAttribute('href'));
        var title = link && link.querySelector('h3');
        
        if (href && title && !processed.has(href) && href.indexOf('http') === 0) {
            processed.add(href);
            var titleText = title.innerText || '';
            var text = titleText.toLowerCase();
            
            // Проверка на scrap metal
            if (text.includes('scrap') || text.includes('metal') || text.includes('recycling') || 
                text.includes('salvage') || text.includes('junk')) {
                
                results.push({
                    url: href,
                    title: titleText,
                    position: index + 1
                });
                
                win.document.write('<div><strong>' + (index + 1) + '</strong> ' + titleText + '<br>');
                win.document.write('<a href="' + href + '">' + href + '</a><br><br></div>');
            }
        }
    });
    
    win.document.write('<hr><h3>JSON:</h3>');
    win.document.write('<textarea rows="10" cols="80" onclick="this.select()">' + 
                      JSON.stringify(results, null, 2) + '</textarea>');
    win.document.write('<p><b>Найдено: ' + results.length + '</b></p>');
    
    console.log('✅ Готово:', results.length);
})();
        '''

    def process_google_links(self, json_file="google_links.json"):
        """Обработать ссылки из Google"""
        if not os.path.exists(json_file):
            print(f"❌ Файл {json_file} не найден")
            print("Сначала выполните Google парсинг по инструкции")
            return []
        
        try:
            with open(json_file, 'r', encoding='utf-8') as f:
                links = json.load(f)
            
            print(f"📂 Загружено {len(links)} ссылок")
            
            businesses = []
            for i, link in enumerate(links, 1):
                print(f"🔍 Обработка {i}/{len(links)}: {link.get('title', 'Unknown')}")
                
                business = self._extract_from_url(link)
                if business:
                    businesses.append(business)
                    print(f"  ✅ Найден телефон: {business.get('phone', 'НЕТ')}")
                else:
                    print(f"  ❌ Телефон не найден")
                
                # Пауза между запросами
                time.sleep(1)
            
            print(f"\n📊 Результат: {len(businesses)} бизнесов с телефонами")
            return businesses
            
        except Exception as e:
            print(f"❌ Ошибка: {e}")
            return []

    def _extract_from_url(self, link_data):
        """Извлечь данные бизнеса с URL"""
        url = link_data.get('url', '')
        if not url:
            return None
        
        try:
            response = self.session.get(url, timeout=15)
            if response.status_code != 200:
                return None
            
            soup = BeautifulSoup(response.text, 'html.parser')
            
            # Ищем телефон
            phone = self._find_phone_on_page(response.text, soup)
            if not phone:
                return None
            
            # Извлекаем другие данные
            business = {
                'name': link_data.get('title', 'Unknown'),
                'website': url,
                'phone': phone,
                'email': self._find_email(response.text),
                'address': self._find_address(soup),
                'source': 'Google',
                'google_position': link_data.get('position', 0)
            }
            
            return business
            
        except Exception as e:
            self.logger.debug(f"Ошибка с {url}: {e}")
            return None

    def _find_phone_on_page(self, text, soup):
        """Найти телефон на странице"""
        # Сначала ищем tel: ссылки
        tel_links = soup.find_all('a', href=lambda x: x and x.startswith('tel:'))
        for link in tel_links:
            phone = self._clean_phone(link.get('href', '').replace('tel:', ''))
            if phone:
                return phone
        
        # Потом ищем в тексте
        for pattern in self.phone_patterns:
            matches = pattern.findall(text)
            for match in matches:
                phone = self._format_phone(match)
                if phone:
                    return phone
        
        return ""

    def _find_email(self, text):
        """Найти email на странице"""
        pattern = re.compile(r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b')
        emails = pattern.findall(text)
        
        for email in emails:
            if not any(skip in email.lower() for skip in ['example', 'test', 'google']):
                return email
        return ""

    def _find_address(self, soup):
        """Найти адрес на странице"""
        selectors = ['.address', '.location', '[itemprop="streetAddress"]']
        
        for selector in selectors:
            element = soup.select_one(selector)
            if element:
                return element.get_text().strip()[:100]
        
        return ""

    def _format_phone(self, match):
        """Форматировать телефон"""
        if isinstance(match, tuple) and len(match) >= 3:
            return f"({match[0]}) {match[1]}-{match[2]}"
        return ""

    def _clean_phone(self, phone):
        """Очистить телефон"""
        if not phone:
            return ""
        
        digits = re.sub(r'\D', '', phone)
        
        if len(digits) == 10:
            return f"({digits[:3]}) {digits[3:6]}-{digits[6:]}"
        elif len(digits) == 11 and digits[0] == '1':
            return f"({digits[1:4]}) {digits[4:7]}-{digits[7:]}"
        
        return ""

    def save_results(self, businesses, filename="google_results.json"):
        """Сохранить результаты"""
        with open(filename, 'w', encoding='utf-8') as f:
            json.dump(businesses, f, indent=2, ensure_ascii=False)
        
        print(f"💾 Результаты сохранены в {filename}")

def main():
    print("🔍 GOOGLE HELPER - Обработка Google ссылок")
    print("=" * 50)
    
    helper = GoogleHelper()
    
    choice = input("\n1. Показать инструкции\n2. Обработать ссылки\nВыбор: ")
    
    if choice == "1":
        helper.show_instructions()
    elif choice == "2":
        businesses = helper.process_google_links()
        if businesses:
            helper.save_results(businesses)
            print(f"\n🎉 Найдено {len(businesses)} бизнесов с телефонами!")
        else:
            print("\n❌ Бизнесы с телефонами не найдены")
    else:
        print("❌ Неверный выбор")

if __name__ == "__main__":
    main() 