# ⚡ БЫСТРЫЙ СТАРТ - Оптимизированный парсер

## 🎯 Цель: Максимум телефонов за минимум времени

### Что изменилось:
- ❌ Убран медленный Selenium
- ✅ Только быстрые HTTP запросы
- ✅ Гибридный подход: OSM + Google
- ✅ Фокус на телефоны (цель: 80%+)

## 🚀 Запуск

### Вариант 1: Быстрый автоматический сбор
```bash
python optimized_scraper.py
```

### Вариант 2: Дополнительный Google парсинг
```bash
python google_helper.py
```

## 📋 Процесс работы

### 1. Автоматический сбор (optimized_scraper.py)
- Быстрый сбор из OSM (продуктивные регионы)
- Агрессивный поиск телефонов
- Если < 80% с телефонами → инструкции для Google

### 2. Google парсинг (при необходимости)
1. Откройте google.com
2. Поиск: "scrap metal recycling [город]"
3. Перейдите на страницы 2-5
4. F12 → Console → вставьте JavaScript
5. Сохраните JSON как `google_links.json`
6. Запустите `python google_helper.py`

## 🔧 JavaScript для Google (скопируйте в консоль)
```javascript
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
```

## 📊 Результат

### Файлы:
- `optimized_contacts_YYYYMMDD_HHMMSS.csv` - Основные данные
- `contact_report_YYYYMMDD_HHMMSS.txt` - Отчет по контактам

### Критерии успеха:
- ✅ 80%+ бизнесов с телефонами
- ⚡ Время сбора < 5 минут
- 📞 Готовые контакты для outreach

## 🎯 Стратегия

### Почему страницы 2-5 Google?
- Компании в топе не нуждаются в дополнительном продвижении
- Страницы 2-5 = потенциальные клиенты
- Меньше конкуренции за контакты

### Приоритеты:
1. 📞 Телефоны (критично)
2. 📧 Email (важно)
3. 🌐 Сайты (полезно)
4. 🏭 Материалы (необязательно)

## 🔧 Настройка

### Изменить целевой процент телефонов:
```python
# В optimized_scraper.py
self.MIN_PHONES_PERCENTAGE = 80  # Изменить на нужный
```

### Добавить города для поиска:
```python
# В optimized_scraper.py, секция productive_bboxes
# Добавить координаты нужных городов
```

## 🐛 Устранение проблем

### Мало контактов из OSM?
- Используйте Google парсинг
- Проверьте интернет-соединение

### JavaScript не работает?
- Обновите браузер
- Используйте Chrome/Firefox
- Отключите блокировщики рекламы

### Ошибки парсинга сайтов?
- Проверьте файлы логов
- Некоторые сайты могут блокировать запросы

## 🎉 Результат

После успешного сбора у вас будет:
- Список компаний с телефонами
- Готовая база для холодных звонков
- Приоритизированные контакты 