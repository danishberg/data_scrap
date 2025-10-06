#!/usr/bin/env python3
"""
Script to check the outputs of the blog scraper system
"""

import os
import glob
import json

def check_outputs():
    """Check all generated outputs"""

    # Find all JSON files in blog_database
    json_files = []
    for root, dirs, files in os.walk('blog_database'):
        for file in files:
            if file.endswith('.json'):
                json_files.append(os.path.join(root, file))

    print('Found JSON files:')
    for f in sorted(json_files):
        print(f'  {f}')

    # Check the most recent stage 1 file
    stage1_files = [f for f in json_files if 'stage_1' in f and 'raw_urls' in f]
    if stage1_files:
        latest = max(stage1_files, key=lambda x: os.path.getmtime(x))
        print(f'\nLatest stage 1 file: {latest}')

        try:
            with open(latest, 'r', encoding='utf-8') as f:
                data = json.load(f)
            print(f'Total URLs collected: {data.get("total_urls", 0)}')
            print(f'Sources: {data.get("sources", {})}')
            print(f'Sample URLs:')
            for i, url in enumerate(data.get('urls', [])[:3]):
                print(f'  {i+1}. {url}')
        except Exception as e:
            print(f'Error reading file: {e}')
    else:
        print('No stage 1 files found')

    # Check stage 2 files
    stage2_files = [f for f in json_files if 'stage_2' in f and 'segmented' in f]
    if stage2_files:
        latest = max(stage2_files, key=lambda x: os.path.getmtime(x))
        print(f'\nLatest stage 2 file: {latest}')

        try:
            with open(latest, 'r', encoding='utf-8') as f:
                data = json.load(f)
            print(f'Articles parsed: {data.get("total_articles_parsed", 0)}')
            print(f'Articles saved: {data.get("total_articles_saved", 0)}')
            print(f'Articles processed: {data.get("total_articles_processed", 0)}')
            print(f'Categories: {data.get("category_distribution", {})}')
        except Exception as e:
            print(f'Error reading stage 2 file: {e}')
    else:
        print('No stage 2 files found')

    # Check stage 3 files
    stage3_files = [f for f in json_files if 'stage_3' in f and 'generated' in f]
    if stage3_files:
        latest = max(stage3_files, key=lambda x: os.path.getmtime(x))
        print(f'\nLatest stage 3 file: {latest}')

        try:
            with open(latest, 'r', encoding='utf-8') as f:
                data = json.load(f)
            print(f'Posts generated: {data.get("total_posts_generated", 0)}')
            print(f'Digest generated: {data.get("digest_generated", False)}')
        except Exception as e:
            print(f'Error reading stage 3 file: {e}')
    else:
        print('No stage 3 files found')

if __name__ == "__main__":
    check_outputs()
