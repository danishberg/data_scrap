from parsers.news_api_collector import NewsAPICollector

n = NewsAPICollector()
articles = n.get_metal_industry_news(10)
print(f'Got {len(articles)} articles')
for a in articles[:3]:
    print(f'{a["title"][:50]}...')
