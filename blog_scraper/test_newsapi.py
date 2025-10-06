import os
os.environ['NEWS_API_KEY'] = '69440137cd9348df91fbca2eb9aa5eb0'

from parsers.news_api_collector import NewsAPICollector

n = NewsAPICollector()
print(f"Using {len(n.metal_queries)} queries:")
for i, q in enumerate(n.metal_queries[:3]):
    print(f"  {i+1}. {q}")

articles = n.get_metal_industry_news(20)
print(f'\nGot {len(articles)} metal industry articles')
for a in articles[:10]:
    title = a.get('title', 'No title')
    print(f'- {title[:80]}...')
