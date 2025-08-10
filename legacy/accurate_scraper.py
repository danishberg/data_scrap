#!/usr/bin/env python3
"""
AI-ENHANCED METAL SCRAPER WITH COMPREHENSIVE DATA EXTRACTION
Integrates multiple AI models for intelligent search, extraction, and validation
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

# AI Libraries
try:
    import spacy
    from transformers import pipeline, AutoTokenizer, AutoModelForSequenceClassification
    import torch
    from sentence_transformers import SentenceTransformer
    HAS_AI = True
except ImportError:
    HAS_AI = False
    print("⚠️  AI libraries not installed. Install with: pip install transformers spacy sentence-transformers torch")

# Phone number library
try:
    import phonenumbers
    from phonenumbers import geocoder, carrier
    HAS_PHONENUMBERS = True
except ImportError:
    HAS_PHONENUMBERS = False
    print("⚠️  phonenumbers library not installed. Install with: pip install phonenumbers")

# Optional: Ollama for local LLM
try:
    import ollama
    HAS_OLLAMA = True
except ImportError:
    HAS_OLLAMA = False

class AIEnhancedMetalScraper:
    def __init__(self):
        self.session = requests.Session()
        self.results = []
        self.processed_urls = set()
        self.logger = self._setup_logging()
        
        # Enhanced settings
        self.TIMEOUT = 8
        self.MAX_WORKERS = 12
        self.BATCH_SIZE = 25
        self.TARGET_SUCCESS_RATE = 0.40
        
        # AI Models initialization
        self.ai_models = {}
        self._init_ai_models()
        
        # Enhanced search queries with AI-generated variations
        self.base_queries = [
            'scrap metal dealers',
            'metal recycling center',
            'auto salvage yard',
            'scrap yard',
            'metal buyers',
            'junk yard',
            'copper scrap buyers',
            'aluminum recycling',
            'steel scrap dealers',
            'metal recovery services',
            'industrial metal recycling',
            'brass buyers',
            'iron scrap dealers',
            'catalytic converter buyers',
            'precious metal recovery'
        ]
        
        # US locations - expanded list
        self.us_locations = [
            'New York NY', 'Los Angeles CA', 'Chicago IL', 'Houston TX', 'Phoenix AZ',
            'Philadelphia PA', 'San Antonio TX', 'San Diego CA', 'Dallas TX', 'San Jose CA',
            'Austin TX', 'Jacksonville FL', 'Fort Worth TX', 'Columbus OH', 'Charlotte NC',
            'San Francisco CA', 'Indianapolis IN', 'Seattle WA', 'Denver CO', 'Washington DC',
            'Boston MA', 'El Paso TX', 'Detroit MI', 'Nashville TN', 'Portland OR',
            'Memphis TN', 'Oklahoma City OK', 'Las Vegas NV', 'Louisville KY', 'Baltimore MD',
            'Milwaukee WI', 'Albuquerque NM', 'Tucson AZ', 'Fresno CA', 'Mesa AZ',
            'Sacramento CA', 'Atlanta GA', 'Kansas City MO', 'Colorado Springs CO', 'Miami FL',
            'Raleigh NC', 'Omaha NE', 'Long Beach CA', 'Virginia Beach VA', 'Oakland CA',
            'Minneapolis MN', 'Tulsa OK', 'Tampa FL', 'Arlington TX', 'New Orleans LA'
        ]
        
        # Enhanced material keywords
        self.material_keywords = [
            'copper', 'aluminum', 'aluminium', 'steel', 'iron', 'brass', 'bronze',
            'stainless steel', 'lead', 'zinc', 'nickel', 'tin', 'titanium',
            'carbide', 'tungsten', 'precious metals', 'gold', 'silver', 'platinum',
            'palladium', 'rhodium', 'catalytic converters', 'car batteries',
            'radiators', 'electric motors', 'transformers', 'wire', 'cable',
            'circuit boards', 'electronic scrap', 'computer scrap', 'mobile phones',
            'cast iron', 'wrought iron', 'structural steel', 'rebar', 'pipes',
            'tubes', 'sheet metal', 'coils', 'turnings', 'shredded metal',
            'HMS', 'heavy melting scrap', 'auto parts', 'engines', 'transmissions'
        ]
        
        self.user_agents = [
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36',
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:122.0) Gecko/20100101 Firefox/122.0',
            'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36',
            'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36'
        ]
        
        self._init_session()

    def _init_ai_models(self):
        """Initialize AI models for enhanced processing"""
        if not HAS_AI:
            self.logger.warning("AI libraries not available. Using basic extraction methods.")
            return
        
        try:
            # Initialize spaCy for NER
            try:
                self.nlp = spacy.load("en_core_web_sm")
                self.logger.info("✅ spaCy model loaded successfully")
            except OSError:
                self.logger.warning("spaCy model not found. Install with: python -m spacy download en_core_web_sm")
                self.nlp = None
            
            # Initialize sentence transformer for similarity
            try:
                self.sentence_model = SentenceTransformer('all-MiniLM-L6-v2')
                self.logger.info("✅ Sentence transformer loaded successfully")
            except Exception as e:
                self.logger.warning(f"Failed to load sentence transformer: {e}")
                self.sentence_model = None
            
            # Initialize text classification for business relevance
            try:
                self.classifier = pipeline("text-classification", 
                                         model="microsoft/DialoGPT-medium",
                                         return_all_scores=True)
                self.logger.info("✅ Text classifier loaded successfully")
            except Exception as e:
                self.logger.warning(f"Failed to load text classifier: {e}")
                self.classifier = None
            
            # Initialize NER pipeline
            try:
                self.ner_pipeline = pipeline("ner", 
                                           model="dbmdz/bert-large-cased-finetuned-conll03-english",
                                           aggregation_strategy="simple")
                self.logger.info("✅ NER pipeline loaded successfully")
            except Exception as e:
                self.logger.warning(f"Failed to load NER pipeline: {e}")
                self.ner_pipeline = None
            
        except Exception as e:
            self.logger.error(f"Error initializing AI models: {e}")
            self.ai_models = {}

    def _init_session(self):
        """Initialize session with enhanced headers"""
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
        logger = logging.getLogger('AIEnhancedMetalScraper')
        logger.setLevel(logging.DEBUG)
        
        if not logger.handlers:
            console_handler = logging.StreamHandler()
            console_formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
            console_handler.setFormatter(console_formatter)
            logger.addHandler(console_handler)
        
        return logger

    def generate_ai_enhanced_queries(self, base_query, location):
        """Generate enhanced search queries using AI"""
        enhanced_queries = [base_query]
        
        # Add location-specific variations
        enhanced_queries.extend([
            f"{base_query} near {location}",
            f"{base_query} in {location}",
            f"{base_query} {location} area",
            f"best {base_query} {location}",
            f"local {base_query} {location}",
            f"{base_query} services {location}",
            f"professional {base_query} {location}"
        ])
        
        # Add material-specific variations
        materials = ['copper', 'aluminum', 'steel', 'iron', 'brass']
        for material in materials:
            enhanced_queries.extend([
                f"{material} {base_query} {location}",
                f"{base_query} {material} buyers {location}",
                f"{material} recycling {location}"
            ])
        
        # Use local LLM if available for query generation
        if HAS_OLLAMA:
            try:
                ai_queries = self._generate_queries_with_ollama(base_query, location)
                enhanced_queries.extend(ai_queries)
            except Exception as e:
                self.logger.debug(f"Ollama query generation failed: {e}")
        
        return enhanced_queries[:15]  # Limit to prevent too many queries

    def _generate_queries_with_ollama(self, base_query, location):
        """Generate search queries using local LLM"""
        try:
            prompt = f"""Generate 5 effective Google search queries for finding {base_query} businesses in {location}. 
            Focus on local businesses that buy, sell, or process scrap metal. 
            Make queries specific and likely to return business listings.
            
            Base query: {base_query}
            Location: {location}
            
            Return only the search queries, one per line:"""
            
            response = ollama.generate(model='llama2', prompt=prompt)
            queries = response['response'].strip().split('\n')
            return [q.strip() for q in queries if q.strip()][:5]
        except Exception as e:
            self.logger.debug(f"Ollama generation error: {e}")
            return []

    def run_comprehensive_scraping(self, target_businesses=100):
        """Enhanced scraping with AI integration"""
        self.logger.info(f"🤖 AI-ENHANCED METAL SCRAPER STARTED")
        self.logger.info(f"🎯 Target: {target_businesses} businesses")
        self.logger.info(f"🌍 Coverage: {len(self.us_locations)} US locations")
        self.logger.info(f"🔍 Queries: {len(self.base_queries)} base types")
        self.logger.info(f"🧠 AI Models: {'✅ Active' if HAS_AI else '❌ Basic mode'}")
        
        start_time = time.time()
        
        # Phase 1: AI-Enhanced Link Collection
        self.logger.info("🔗 Phase 1: AI-Enhanced Link Collection")
        all_links = self._collect_ai_enhanced_links()
        self.logger.info(f"✅ Collected {len(all_links)} unique links")
        
        # Phase 2: Comprehensive Data Extraction
        self.logger.info("📊 Phase 2: Comprehensive Data Extraction")
        businesses = self._extract_comprehensive_data(all_links, target_businesses)
        
        # Phase 3: AI-Enhanced Data Validation
        self.logger.info("🧠 Phase 3: AI-Enhanced Data Validation")
        validated_businesses = self._validate_with_ai(businesses)
        
        # Phase 4: Final Processing
        self.logger.info("🔬 Phase 4: Final Processing")
        self.results = self._finalize_results(validated_businesses, target_businesses)
        
        elapsed = time.time() - start_time
        phone_percentage = self._calculate_contact_percentage()
        
        self.logger.info(f"✅ AI-ENHANCED SCRAPING COMPLETED in {elapsed/60:.1f} minutes")
        self.logger.info(f"📊 Results: {len(self.results)} businesses")
        self.logger.info(f"📞 With contacts: {phone_percentage:.1f}%")
        
        return self.results

    def _collect_ai_enhanced_links(self):
        """Collect links with AI-enhanced search strategies"""
        all_links = []
        
        # Use top locations for comprehensive coverage
        selected_locations = self.us_locations[:20]  # Top 20 cities
        
        # Generate AI-enhanced queries
        search_tasks = []
        for location in selected_locations:
            for base_query in self.base_queries:
                enhanced_queries = self.generate_ai_enhanced_queries(base_query, location)
                for query in enhanced_queries[:5]:  # Limit per base query
                    for page in range(1, 4):  # Pages 1-3
                        search_tasks.append((query, page, location))
        
        self.logger.info(f"📋 Generated {len(search_tasks)} AI-enhanced search tasks")
        
        # Execute searches in parallel
        with ThreadPoolExecutor(max_workers=self.MAX_WORKERS) as executor:
            for i in range(0, len(search_tasks), self.BATCH_SIZE):
                batch = search_tasks[i:i + self.BATCH_SIZE]
                batch_links = []
                
                futures = {
                    executor.submit(self._enhanced_search, query, page, location): (query, page, location)
                    for query, page, location in batch
                }
                
                for future in as_completed(futures, timeout=60):
                    try:
                        links = future.result(timeout=10)
                        if links:
                            # Use AI to filter relevant links
                            relevant_links = self._filter_links_with_ai(links)
                            batch_links.extend(relevant_links)
                    except Exception as e:
                        self.logger.debug(f"Search batch failed: {e}")
                        continue
                
                all_links.extend(batch_links)
                
                progress = (i + self.BATCH_SIZE) / len(search_tasks) * 100
                self.logger.info(f"📊 Batch {i//self.BATCH_SIZE + 1}: +{len(batch_links)} links | Total: {len(all_links)} | Progress: {progress:.1f}%")
                
                # Collect sufficient links
                if len(all_links) >= 800:
                    break
        
        return self._deduplicate_links(all_links)

    def _enhanced_search(self, query, page, location):
        """Enhanced search with multiple engines"""
        links = []
        
        # Try Google first
        google_links = self._google_search(query, page)
        links.extend(google_links)
        
        # Try Bing as fallback
        if len(links) < 5:
            bing_links = self._bing_search(query, page)
            links.extend(bing_links)
        
        return links

    def _google_search(self, query, page):
        """Enhanced Google search"""
        links = []
        
        try:
            start = (page - 1) * 10
            url = f"https://www.google.com/search?q={quote_plus(query)}&start={start}&num=10"
            
            headers = {
                'User-Agent': random.choice(self.user_agents),
                'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
                'Accept-Language': 'en-US,en;q=0.9',
                'Accept-Encoding': 'gzip, deflate',
                'Connection': 'keep-alive',
                'Referer': 'https://www.google.com/'
            }
            
            response = self.session.get(url, headers=headers, timeout=self.TIMEOUT, verify=False)
            
            if response.status_code == 200:
                soup = BeautifulSoup(response.text, 'html.parser')
                
                # Enhanced Google result parsing
                results = soup.find_all('div', class_='g')
                
                for result in results:
                    try:
                        # Extract link
                        link_elem = result.find('a', href=True)
                        if not link_elem:
                            continue
                        
                        url = link_elem['href']
                        if not url.startswith('http'):
                            continue
                        
                        # Extract title
                        title_elem = result.find('h3')
                        title = title_elem.get_text(strip=True) if title_elem else ""
                        
                        # Extract description
                        desc_elem = result.find('span', class_='st') or result.find('div', class_='s')
                        description = desc_elem.get_text(strip=True) if desc_elem else ""
                        
                        if self._is_business_relevant(title, url, description):
                            links.append({
                                'url': url,
                                'title': title,
                                'description': description,
                                'page': page,
                                'query': query,
                                'source': 'Google'
                            })
                        
                        if len(links) >= 15:
                            break
                            
                    except Exception as e:
                        continue
            
            time.sleep(random.uniform(1.0, 2.0))
            
        except Exception as e:
            self.logger.debug(f"Google search failed for '{query}' page {page}: {e}")
        
        return links

    def _bing_search(self, query, page):
        """Enhanced Bing search as fallback"""
        links = []
        
        try:
            start = (page - 1) * 10
            url = f"https://www.bing.com/search?q={quote_plus(query)}&first={start}&count=10"
            
            headers = {
                'User-Agent': random.choice(self.user_agents),
                'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
                'Accept-Language': 'en-US,en;q=0.9',
                'Accept-Encoding': 'gzip, deflate',
                'Connection': 'keep-alive',
                'Referer': 'https://www.bing.com/'
            }
            
            response = self.session.get(url, headers=headers, timeout=self.TIMEOUT, verify=False)
            
            if response.status_code == 200:
                soup = BeautifulSoup(response.text, 'html.parser')
                results = soup.find_all('li', class_='b_algo')
                
                for result in results:
                    try:
                        h2 = result.find('h2')
                        if not h2:
                            continue
                        
                        link_elem = h2.find('a', href=True)
                        if not link_elem:
                            continue
                        
                        url = link_elem['href']
                        title = h2.get_text(strip=True)
                        
                        desc_elem = result.find('p') or result.find('div', class_='b_caption')
                        description = desc_elem.get_text(strip=True)[:200] if desc_elem else ""
                        
                        if self._is_business_relevant(title, url, description):
                            links.append({
                                'url': url,
                                'title': title,
                                'description': description,
                                'page': page,
                                'query': query,
                                'source': 'Bing'
                            })
                        
                        if len(links) >= 12:
                            break
                            
                    except Exception as e:
                        continue
            
            time.sleep(random.uniform(0.8, 1.5))
            
        except Exception as e:
            self.logger.debug(f"Bing search failed for '{query}' page {page}: {e}")
        
        return links

    def _is_business_relevant(self, title, url, description):
        """Enhanced business relevance check"""
        # Basic keyword check
        relevant_keywords = [
            'scrap', 'metal', 'recycling', 'salvage', 'junk', 'yard',
            'steel', 'copper', 'aluminum', 'iron', 'brass', 'buyer',
            'dealer', 'processing', 'facility', 'center', 'company',
            'auto parts', 'demolition', 'waste', 'materials'
        ]
        
        # Exclude obvious non-business sites
        exclude_domains = [
            'wikipedia.org', 'facebook.com', 'youtube.com', 'linkedin.com',
            'indeed.com', 'glassdoor.com', 'amazon.com', 'ebay.com',
            'craigslist.org', 'reddit.com', 'twitter.com', 'instagram.com'
        ]
        
        exclude_keywords = [
            'software', 'app', 'game', 'news', 'blog', 'jobs', 'career',
            'hiring', 'employment', 'resume', 'salary', 'review', 'rating',
            'price guide', 'calculator', 'directory', 'listing'
        ]
        
        combined_text = f"{title} {url} {description}".lower()
        
        # Check relevance
        relevant_count = sum(1 for word in relevant_keywords if word in combined_text)
        has_exclude_domain = any(domain in url.lower() for domain in exclude_domains)
        has_exclude_word = any(word in combined_text for word in exclude_keywords)
        
        # Business indicators
        business_indicators = [
            'llc', 'inc', 'corp', 'company', 'co.', 'ltd', 'phone',
            'contact', 'address', 'location', 'hours', 'service'
        ]
        has_business_indicators = any(indicator in combined_text for indicator in business_indicators)
        
        return (relevant_count >= 2 and not has_exclude_domain and
                not has_exclude_word and has_business_indicators)

    def _filter_links_with_ai(self, links):
        """Filter links using AI relevance scoring"""
        if not HAS_AI or not self.sentence_model:
            return links
        
        try:
            # Define target business description
            target_description = "scrap metal recycling business that buys and processes metal materials"
            
            relevant_links = []
            
            for link in links:
                combined_text = f"{link['title']} {link['description']}"
                
                # Use sentence similarity
                link_embedding = self.sentence_model.encode([combined_text])
                target_embedding = self.sentence_model.encode([target_description])
                
                similarity = torch.cosine_similarity(
                    torch.tensor(link_embedding),
                    torch.tensor(target_embedding)
                )[0].item()
                
                if similarity > 0.3:  # Threshold for relevance
                    link['ai_relevance_score'] = similarity
                    relevant_links.append(link)
            
            # Sort by relevance score
            relevant_links.sort(key=lambda x: x.get('ai_relevance_score', 0), reverse=True)
            return relevant_links
            
        except Exception as e:
            self.logger.debug(f"AI filtering failed: {e}")
            return links

    def _extract_comprehensive_data(self, links, target_businesses):
        """Extract comprehensive business data with AI enhancement"""
        self.logger.info(f"📊 Extracting comprehensive data from {len(links)} links")
        
        businesses = []
        processed_count = 0
        
        # Process links in parallel
        with ThreadPoolExecutor(max_workers=self.MAX_WORKERS) as executor:
            futures = {
                executor.submit(self._extract_single_business, link): link
                for link in links
            }
            
            for future in as_completed(futures):
                if len(businesses) >= target_businesses:
                    break
                
                try:
                    business = future.result(timeout=15)
                    if business:
                        businesses.append(business)
                        self.logger.info(f"✅ [{len(businesses)}] {business['name'][:50]}... | 📞 {business.get('phone', 'N/A')} | 📧 {business.get('email', 'N/A')}")
                except Exception as e:
                    self.logger.debug(f"Business extraction failed: {e}")
                
                processed_count += 1
                
                if processed_count % 100 == 0:
                    success_rate = (len(businesses) / processed_count) * 100
                    self.logger.info(f"📊 Processed: {processed_count} | Found: {len(businesses)} | Success: {success_rate:.1f}%")
        
        return businesses

    def _extract_single_business(self, link_data):
        """Extract comprehensive data from a single business page"""
        url = link_data['url']
        
        try:
            headers = {
                'User-Agent': random.choice(self.user_agents),
                'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
                'Accept-Language': 'en-US,en;q=0.9',
                'Accept-Encoding': 'gzip, deflate',
                'Connection': 'keep-alive',
                'Referer': 'https://www.google.com/'
            }
            
            response = self.session.get(url, headers=headers, timeout=self.TIMEOUT, verify=False)
            
            if response.status_code != 200:
                return None
            
            soup = BeautifulSoup(response.text, 'html.parser')
            page_text = response.text
            
            # Extract comprehensive business data
            name = self._extract_business_name(link_data, soup)
            phone = self._extract_phone_enhanced(page_text, soup)
            email = self._extract_email_enhanced(page_text, soup)
            
            # Debug logging
            self.logger.debug(f"Extracting from {url[:50]}...")
            self.logger.debug(f"  Name: {name}")
            self.logger.debug(f"  Phone: {phone}")
            self.logger.debug(f"  Email: {email}")
            
            business_data = {
                'name': name,
                'phone': phone,
                'email': email,
                'website': url,
                'address': self._extract_address_enhanced(page_text, soup),
                'city': self._extract_city_enhanced(page_text, soup),
                'state': self._extract_state_enhanced(page_text, soup),
                'zip_code': self._extract_zip_enhanced(page_text, soup),
                'country': 'United States',
                'description': self._extract_description_enhanced(page_text, soup),
                'materials_accepted': self._extract_materials_enhanced(page_text, soup),
                'services': self._extract_services_enhanced(page_text, soup),
                'working_hours': self._extract_hours_enhanced(page_text, soup),
                'payment_methods': self._extract_payment_methods_enhanced(page_text, soup),
                'certifications': self._extract_certifications_enhanced(page_text, soup),
                'social_media': self._extract_social_media_enhanced(page_text, soup),
                'years_in_business': self._extract_years_in_business_enhanced(page_text, soup),
                'languages': self._extract_languages_enhanced(page_text, soup),
                'additional_info': self._extract_additional_info_enhanced(page_text, soup),
                'extraction_timestamp': datetime.now().isoformat(),
                'source': link_data.get('source', 'Web Search'),
                'search_query': link_data.get('query', ''),
                'ai_relevance_score': link_data.get('ai_relevance_score', 0)
            }
            
            # AI-enhanced data validation
            if HAS_AI:
                business_data = self._enhance_data_with_ai(business_data, page_text, soup)
            
            # Calculate data completeness
            business_data['data_completeness'] = self._calculate_data_completeness(business_data)
            
            # Validate minimum requirements
            if self._meets_minimum_requirements(business_data):
                return business_data
            
        except Exception as e:
            self.logger.debug(f"Failed to extract business data from {url}: {e}")
        
        return None

    def _extract_phone_enhanced(self, page_text, soup):
        """Enhanced phone extraction with AI and phonenumbers library"""
        phone = None
        
        # Method 1: Use phonenumbers library if available
        if HAS_PHONENUMBERS:
            phone = self._extract_phone_with_phonenumbers(page_text)
            if phone:
                return phone
        
        # Method 2: Enhanced regex patterns
        phone = self._extract_phone_with_regex(page_text)
        if phone:
            return phone
        
        # Method 3: HTML parsing
        phone = self._extract_phone_from_html(soup)
        if phone:
            return phone
        
        # Method 4: Simple extraction as fallback
        phone = self._extract_phone_simple(page_text)
        if phone:
            return phone
        
        # Method 5: AI-enhanced extraction (if available)
        if HAS_AI and hasattr(self, 'ner_pipeline') and self.ner_pipeline:
            phone = self._extract_phone_with_ai(page_text)
            if phone:
                return phone
        
        return None

    def _extract_phone_with_phonenumbers(self, text):
        """Extract phone using phonenumbers library"""
        try:
            import phonenumbers
            from phonenumbers import geocoder, carrier
            
            # Find all potential phone numbers
            for match in phonenumbers.PhoneNumberMatcher(text, "US"):
                number = match.number
                if phonenumbers.is_valid_number(number):
                    formatted = phonenumbers.format_number(number, phonenumbers.PhoneNumberFormat.NATIONAL)
                    return formatted
        except Exception as e:
            self.logger.debug(f"phonenumbers extraction failed: {e}")
        
        return None

    def _extract_phone_with_regex(self, text):
        """Enhanced regex phone extraction"""
        # Comprehensive US phone patterns
        patterns = [
            # Standard formats
            r'\b\(?([2-9][0-9]{2})\)?[-.\s]?([2-9][0-9]{2})[-.\s]?([0-9]{4})\b',
            r'\b1[-.\s]?\(?([2-9][0-9]{2})\)?[-.\s]?([2-9][0-9]{2})[-.\s]?([0-9]{4})\b',
            
            # tel: links
            r'tel:[\s]*\+?1?[-.\s]?\(?([2-9][0-9]{2})\)?[-.\s]?([2-9][0-9]{2})[-.\s]?([0-9]{4})',
            
            # With extensions
            r'\b\(?([2-9][0-9]{2})\)?[-.\s]?([2-9][0-9]{2})[-.\s]?([0-9]{4})(?:\s*(?:ext|x|extension)\.?\s*\d{1,4})?\b',
            
            # International format
            r'\+1[-.\s]?\(?([2-9][0-9]{2})\)?[-.\s]?([2-9][0-9]{2})[-.\s]?([0-9]{4})\b',
            
            # Separated by spaces
            r'\b([2-9][0-9]{2})\s+([2-9][0-9]{2})\s+([0-9]{4})\b',
            
            # Dot separated
            r'\b([2-9][0-9]{2})\.([2-9][0-9]{2})\.([0-9]{4})\b'
        ]
        
        for pattern in patterns:
            matches = re.findall(pattern, text, re.IGNORECASE)
            for match in matches:
                if len(match) == 3:
                    area, exchange, number = match
                    if self._validate_us_phone_enhanced(area, exchange, number):
                        return f"({area}) {exchange}-{number}"
        
        return None

    def _extract_phone_from_html(self, soup):
        """Extract phone from HTML elements"""
        # tel: links
        tel_links = soup.find_all('a', href=lambda x: x and x.startswith('tel:'))
        for link in tel_links:
            tel_value = link.get('href', '').replace('tel:', '').strip()
            phone = self._clean_phone_number(tel_value)
            if phone:
                return phone
        
        # Microdata
        phone_elements = soup.find_all(attrs={'itemprop': re.compile(r'telephone|phone', re.IGNORECASE)})
        for element in phone_elements:
            content = element.get('content') or element.get_text().strip()
            phone = self._clean_phone_number(content)
            if phone:
                return phone
        
        # Class-based search
        phone_classes = soup.find_all(class_=re.compile(r'phone|tel|contact', re.IGNORECASE))
        for element in phone_classes:
            text = element.get_text().strip()
            phone = self._extract_phone_with_regex(text)
            if phone:
                return phone
        
        return None

    def _extract_phone_with_ai(self, text):
        """Extract phone using AI NER"""
        try:
            # Use NER to find phone-like entities
            entities = self.ner_pipeline(text[:1000])  # Limit text length
            
            for entity in entities:
                if 'phone' in entity.get('word', '').lower():
                    phone_text = entity['word']
                    phone = self._extract_phone_with_regex(phone_text)
                    if phone:
                        return phone
        except Exception as e:
            self.logger.debug(f"AI phone extraction failed: {e}")
        
        return None

    def _validate_us_phone_enhanced(self, area_code, exchange, number):
        """Enhanced US phone validation"""
        # Length check
        if len(area_code) != 3 or len(exchange) != 3 or len(number) != 4:
            return False
        
        # Must be digits
        if not (area_code.isdigit() and exchange.isdigit() and number.isdigit()):
            return False
        
        # Area code validation
        if area_code[0] in ['0', '1']:
            return False
        
        # Exchange validation
        if exchange[0] in ['0', '1']:
            return False
        
        # Block invalid patterns
        if area_code == '000' or exchange == '000' or number == '0000':
            return False
        
        # Block test numbers
        if area_code == '555' and exchange == '555':
            return False
        
        # Block emergency numbers
        if area_code + exchange + number in ['9111111111']:
            return False
        
        return True

    def _clean_phone_number(self, phone_str):
        """Clean and format phone number"""
        if not phone_str:
            return None
        
        # Extract digits
        digits = re.sub(r'\D', '', str(phone_str))
        
        # Handle different lengths
        if len(digits) == 10:
            area = digits[:3]
            exchange = digits[3:6]
            number = digits[6:]
        elif len(digits) == 11 and digits.startswith('1'):
            area = digits[1:4]
            exchange = digits[4:7]
            number = digits[7:]
        else:
            return None
        
        if self._validate_us_phone_enhanced(area, exchange, number):
            return f"({area}) {exchange}-{number}"
        
        return None

    def _extract_email_enhanced(self, page_text, soup):
        """Enhanced email extraction"""
        # Method 1: mailto links
        mailto_links = soup.find_all('a', href=lambda x: x and x.startswith('mailto:'))
        for link in mailto_links:
            email = link.get('href', '').replace('mailto:', '').strip()
            if self._validate_email_enhanced(email):
                return email
        
        # Method 2: Enhanced regex patterns
        email = self._extract_email_with_regex(page_text)
        if email:
            return email
        
        # Method 3: HTML microdata
        email_elements = soup.find_all(attrs={'itemprop': re.compile(r'email', re.IGNORECASE)})
        for element in email_elements:
            content = element.get('content') or element.get_text().strip()
            if self._validate_email_enhanced(content):
                return content
        
        # Method 4: Simple extraction as fallback
        email = self._extract_email_simple(page_text)
        if email:
            return email
        
        # Method 5: AI-enhanced extraction (if available)
        if HAS_AI and hasattr(self, 'ner_pipeline') and self.ner_pipeline:
            email = self._extract_email_with_ai(page_text, soup)
            if email:
                return email
        
        return None

    def _extract_email_with_regex(self, text):
        """Extract email using enhanced regex"""
        patterns = [
            # Standard email
            r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b',
            # With spaces around @
            r'\b[A-Za-z0-9._%+-]+\s*@\s*[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b',
            # With [at] replacement
            r'\b[A-Za-z0-9._%+-]+\s*\[at\]\s*[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b',
            # With (at) replacement
            r'\b[A-Za-z0-9._%+-]+\s*\(at\)\s*[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b',
            # With AT replacement
            r'\b[A-Za-z0-9._%+-]+\s*AT\s*[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b'
        ]
        
        for pattern in patterns:
            matches = re.findall(pattern, text, re.IGNORECASE)
            for match in matches:
                # Clean the email
                email = re.sub(r'\s+', '', match)
                email = email.replace('[at]', '@').replace('(at)', '@').replace('AT', '@')
                
                if self._validate_email_enhanced(email):
                    return email.lower()
        
        return None

    def _extract_email_with_ai(self, page_text, soup):
        """Extract email using AI techniques"""
        try:
            if self.ner_pipeline:
                # Use NER to find email-like entities
                entities = self.ner_pipeline(page_text[:1000])
                
                for entity in entities:
                    word = entity.get('word', '')
                    if '@' in word and '.' in word:
                        if self._validate_email_enhanced(word):
                            return word.lower()
        except Exception as e:
            self.logger.debug(f"AI email extraction failed: {e}")
        
        return None

    def _validate_email_enhanced(self, email):
        """Enhanced email validation"""
        if not email or '@' not in email:
            return False
        
        # Basic format check
        email_pattern = r'^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}$'
        if not re.match(email_pattern, email):
            return False
        
        # Exclude test domains
        test_domains = [
            'example.com', 'test.com', 'sample.com', 'demo.com',
            'placeholder.com', 'dummy.com', 'fake.com'
        ]
        
        domain = email.split('@')[1].lower()
        if domain in test_domains:
            return False
        
        return True

    def _extract_business_name(self, link_data, soup):
        """Extract business name from multiple sources"""
        # Try title tag first
        title_tag = soup.find('title')
        if title_tag:
            title = title_tag.get_text().strip()
            if title and len(title) > 3:
                # Clean title
                name = title.split('|')[0].split('-')[0].strip()
                return name[:150]
        
        # Try H1 tag
        h1_tag = soup.find('h1')
        if h1_tag:
            h1_text = h1_tag.get_text().strip()
            if h1_text and len(h1_text) > 3:
                return h1_text[:150]
        
        # Try meta tags
        meta_tags = [
            soup.find('meta', property='og:site_name'),
            soup.find('meta', property='og:title'),
            soup.find('meta', {'name': 'application-name'})
        ]
        
        for tag in meta_tags:
            if tag:
                content = tag.get('content', '')
                if content and len(content) > 3:
                    return content[:150]
        
        # Fallback to search result title
        return link_data.get('title', 'Unknown Business')[:150]

    def _extract_address_enhanced(self, page_text, soup):
        """Enhanced address extraction"""
        # Method 1: Microdata
        address_elements = soup.find_all(attrs={'itemprop': re.compile(r'address|street', re.IGNORECASE)})
        for element in address_elements:
            address = element.get('content') or element.get_text().strip()
            if address and len(address) > 10:
                return address[:200]
        
        # Method 2: Regex patterns
        address_patterns = [
            r'\b\d+\s+[A-Za-z0-9\s]+(?:Street|St|Avenue|Ave|Road|Rd|Drive|Dr|Boulevard|Blvd|Lane|Ln|Way|Circle|Cir|Court|Ct)\b[^,\n]*',
            r'\b\d+\s+[A-Za-z0-9\s]+(?:St|Ave|Rd|Dr|Blvd|Ln|Way|Cir|Ct)\.?\s*[,\n]?[^,\n]*'
        ]
        
        for pattern in address_patterns:
            matches = re.findall(pattern, page_text, re.IGNORECASE)
            if matches:
                return matches[0][:200]
        
        # Method 3: AI-enhanced extraction
        if HAS_AI:
            return self._extract_address_with_ai(page_text)
        
        return None

    def _extract_address_with_ai(self, text):
        """Extract address using AI NER"""
        try:
            if self.ner_pipeline:
                entities = self.ner_pipeline(text[:1000])
                
                addresses = []
                for entity in entities:
                    if entity.get('entity_group') == 'LOC':
                        addresses.append(entity['word'])
                
                if addresses:
                    return ' '.join(addresses)[:200]
        except Exception as e:
            self.logger.debug(f"AI address extraction failed: {e}")
        
        return None

    def _extract_city_enhanced(self, page_text, soup):
        """Enhanced city extraction"""
        # Microdata
        city_elements = soup.find_all(attrs={'itemprop': re.compile(r'city|locality', re.IGNORECASE)})
        for element in city_elements:
            city = element.get('content') or element.get_text().strip()
            if city and len(city) > 2:
                return city[:50]
        
        # Regex patterns
        city_patterns = [
            r'\b([A-Za-z\s]+),\s*([A-Z]{2})\s*\d{5}',
            r'(?:City|Town|Village):\s*([A-Za-z\s]+)',
            r'Located in ([A-Za-z\s]+),\s*[A-Z]{2}'
        ]
        
        for pattern in city_patterns:
            matches = re.findall(pattern, page_text, re.IGNORECASE)
            if matches:
                city = matches[0] if isinstance(matches[0], str) else matches[0][0]
                return city.strip()[:50]
        
        return None

    def _extract_state_enhanced(self, page_text, soup):
        """Enhanced state extraction"""
        # Microdata
        state_elements = soup.find_all(attrs={'itemprop': re.compile(r'state|region', re.IGNORECASE)})
        for element in state_elements:
            state = element.get('content') or element.get_text().strip()
            if state and len(state) >= 2:
                return state[:20]
        
        # Regex for state codes
        state_pattern = r'\b([A-Z]{2})\s*\d{5}(?:-\d{4})?\b'
        matches = re.findall(state_pattern, page_text)
        
        us_states = ['AL', 'AK', 'AZ', 'AR', 'CA', 'CO', 'CT', 'DE', 'FL', 'GA', 'HI', 'ID', 'IL', 'IN', 'IA', 'KS', 'KY', 'LA', 'ME', 'MD', 'MA', 'MI', 'MN', 'MS', 'MO', 'MT', 'NE', 'NV', 'NH', 'NJ', 'NM', 'NY', 'NC', 'ND', 'OH', 'OK', 'OR', 'PA', 'RI', 'SC', 'SD', 'TN', 'TX', 'UT', 'VT', 'VA', 'WA', 'WV', 'WI', 'WY']
        
        for match in matches:
            if match in us_states:
                return match
        
        return None

    def _extract_zip_enhanced(self, page_text, soup):
        """Enhanced ZIP code extraction"""
        # Microdata
        zip_elements = soup.find_all(attrs={'itemprop': re.compile(r'postal|zip', re.IGNORECASE)})
        for element in zip_elements:
            zip_code = element.get('content') or element.get_text().strip()
            if zip_code and re.match(r'^\d{5}(-\d{4})?$', zip_code):
                return zip_code
        
        # Regex patterns
        zip_patterns = [
            r'\b(\d{5}(?:-\d{4})?)\b',  # US ZIP
            r'\b([A-Z]\d[A-Z]\s*\d[A-Z]\d)\b'  # Canadian postal code
        ]
        
        for pattern in zip_patterns:
            matches = re.findall(pattern, page_text)
            if matches:
                return matches[0]
        
        return None

    def _extract_description_enhanced(self, page_text, soup):
        """Enhanced description extraction"""
        # Meta description
        meta_desc = soup.find('meta', {'name': 'description'})
        if meta_desc:
            content = meta_desc.get('content', '')
            if content and len(content) > 20:
                return content[:500]
        
        # OG description
        og_desc = soup.find('meta', property='og:description')
        if og_desc:
            content = og_desc.get('content', '')
            if content and len(content) > 20:
                return content[:500]
        
        # About section
        about_selectors = ['.about', '.description', '.overview', '.intro', '.summary']
        for selector in about_selectors:
            elements = soup.select(selector)
            for element in elements:
                text = element.get_text(strip=True)
                if len(text) > 50:
                    return text[:500]
        
        return None

    def _extract_materials_enhanced(self, page_text, soup):
        """Enhanced materials extraction"""
        materials_found = []
        text_lower = page_text.lower()
        
        # Check for each material keyword
        for material in self.material_keywords:
            if material in text_lower:
                materials_found.append(material)
        
        # Use AI to extract additional materials
        if HAS_AI and materials_found:
            ai_materials = self._extract_materials_with_ai(page_text)
            if ai_materials:
                materials_found.extend(ai_materials)
        
        return list(set(materials_found)) if materials_found else None

    def _extract_materials_with_ai(self, text):
        """Extract materials using AI NER"""
        try:
            if self.ner_pipeline:
                entities = self.ner_pipeline(text[:1000])
                
                materials = []
                for entity in entities:
                    word = entity.get('word', '').lower()
                    if any(mat in word for mat in ['metal', 'steel', 'iron', 'copper', 'aluminum']):
                        materials.append(word)
                
                return materials
        except Exception as e:
            self.logger.debug(f"AI materials extraction failed: {e}")
        
        return []

    def _extract_services_enhanced(self, page_text, soup):
        """Enhanced services extraction"""
        services_keywords = [
            'pickup', 'collection', 'container rental', 'roll-off', 'demolition',
            'dismantling', 'processing', 'sorting', 'weighing', 'cash payment',
            'commercial', 'residential', 'industrial', 'certified scales',
            'licensed', 'insured', 'bonded', 'environmental compliance'
        ]
        
        services_found = []
        text_lower = page_text.lower()
        
        for service in services_keywords:
            if service in text_lower:
                services_found.append(service)
        
        return services_found if services_found else None

    def _extract_hours_enhanced(self, page_text, soup):
        """Enhanced working hours extraction"""
        # Look for microdata
        hours_elements = soup.find_all(attrs={'itemprop': re.compile(r'openingHours|hours', re.IGNORECASE)})
        for element in hours_elements:
            hours = element.get('content') or element.get_text().strip()
            if hours and len(hours) > 10:
                return hours[:200]
        
        # Look for class-based selectors
        hours_selectors = ['.hours', '.opening-hours', '.business-hours', '.working-hours']
        for selector in hours_selectors:
            elements = soup.select(selector)
            for element in elements:
                hours_text = element.get_text(strip=True)
                if len(hours_text) > 10:
                    return hours_text[:200]
        
        return None

    def _extract_payment_methods_enhanced(self, page_text, soup):
        """Enhanced payment methods extraction"""
        payment_keywords = [
            'cash', 'check', 'credit card', 'debit card', 'visa', 'mastercard',
            'american express', 'discover', 'paypal', 'wire transfer',
            'bank transfer', 'financing', 'net terms'
        ]
        
        payment_methods = []
        text_lower = page_text.lower()
        
        for method in payment_keywords:
            if method in text_lower:
                payment_methods.append(method)
        
        return payment_methods if payment_methods else None

    def _extract_certifications_enhanced(self, page_text, soup):
        """Enhanced certifications extraction"""
        cert_patterns = [
            r'ISO\s*\d{4,5}',
            r'R2\s*certified',
            r'EPA\s*registered',
            r'ISRI\s*member',
            r'certified\s+\w+',
            r'licensed\s+\w+',
            r'OSHA\s*compliant'
        ]
        
        certifications = []
        for pattern in cert_patterns:
            matches = re.findall(pattern, page_text, re.IGNORECASE)
            certifications.extend(matches)
        
        return certifications[:5] if certifications else None

    def _extract_social_media_enhanced(self, page_text, soup):
        """Enhanced social media extraction"""
        social_patterns = {
            'facebook': r'(?:facebook\.com|fb\.com)/([^/\s]+)',
            'twitter': r'(?:twitter\.com|x\.com)/([^/\s]+)',
            'instagram': r'instagram\.com/([^/\s]+)',
            'linkedin': r'linkedin\.com/company/([^/\s]+)',
            'youtube': r'youtube\.com/(?:channel|user)/([^/\s]+)',
            'tiktok': r'tiktok\.com/@([^/\s]+)'
        }
        
        social_media = {}
        for platform, pattern in social_patterns.items():
            matches = re.findall(pattern, page_text, re.IGNORECASE)
            if matches:
                social_media[platform] = matches[0]
        
        return social_media if social_media else None

    def _extract_years_in_business_enhanced(self, page_text, soup):
        """Enhanced years in business extraction"""
        years_patterns = [
            r'(\d{1,2})\s*\+?\s*years?\s+(?:in\s+)?business',
            r'established\s+(?:in\s+)?(\d{4})',
            r'since\s+(\d{4})',
            r'founded\s+(?:in\s+)?(\d{4})',
            r'serving\s+(?:for\s+)?(\d{1,2})\s*years?'
        ]
        
        for pattern in years_patterns:
            matches = re.findall(pattern, page_text, re.IGNORECASE)
            if matches:
                return matches[0]
        
        return None

    def _extract_languages_enhanced(self, page_text, soup):
        """Enhanced languages extraction"""
        language_patterns = [
            r'languages?\s*:?\s*([^.]+)',
            r'(?:we\s+)?speak\s+([^.]+)',
            r'bilingual\s+([^.]+)',
            r'(?:english|spanish|french|german|italian|chinese|korean|japanese|arabic|russian)',
        ]
        
        languages = []
        for pattern in language_patterns:
            matches = re.findall(pattern, page_text, re.IGNORECASE)
            languages.extend(matches)
        
        return languages[:3] if languages else None

    def _extract_additional_info_enhanced(self, page_text, soup):
        """Enhanced additional info extraction"""
        info_patterns = [
            r'(?:we\s+also|additionally|other\s+services)\s*:?\s*([^.]+)',
            r'specializing\s+in\s+([^.]+)',
            r'expertise\s+in\s+([^.]+)',
            r'focus\s+on\s+([^.]+)'
        ]
        
        additional_info = []
        for pattern in info_patterns:
            matches = re.findall(pattern, page_text, re.IGNORECASE)
            additional_info.extend(matches)
        
        return additional_info[:3] if additional_info else None

    def _enhance_data_with_ai(self, business_data, page_text, soup):
        """Enhance business data using AI"""
        try:
            # Use AI to improve business name
            if HAS_AI and self.nlp:
                doc = self.nlp(page_text[:1000])
                
                # Extract organization entities
                orgs = [ent.text for ent in doc.ents if ent.label_ == "ORG"]
                if orgs and not business_data.get('name'):
                    business_data['name'] = orgs[0][:150]
                
                # Extract location entities
                locs = [ent.text for ent in doc.ents if ent.label_ in ["GPE", "LOC"]]
                if locs and not business_data.get('city'):
                    business_data['city'] = locs[0][:50]
            
            # AI-enhanced description
            if not business_data.get('description') and HAS_AI:
                business_data['description'] = self._generate_ai_description(page_text)
            
        except Exception as e:
            self.logger.debug(f"AI enhancement failed: {e}")
        
        return business_data

    def _generate_ai_description(self, text):
        """Generate AI description using local LLM"""
        if not HAS_OLLAMA:
            return None
        
        try:
            prompt = f"Based on this business website content, write a brief 2-sentence description of what this scrap metal business does:\n\n{text[:500]}"
            response = ollama.generate(model='llama2', prompt=prompt)
            return response['response'][:300]
        except Exception as e:
            self.logger.debug(f"AI description generation failed: {e}")
            return None

    def _validate_with_ai(self, businesses):
        """Validate business data using AI"""
        if not HAS_AI:
            return businesses
        
        validated = []
        
        for business in businesses:
            try:
                # AI-based relevance scoring
                if self.sentence_model:
                    business_text = f"{business.get('name', '')} {business.get('description', '')} {business.get('materials_accepted', '')}"
                    target_text = "scrap metal recycling business"
                    
                    business_embedding = self.sentence_model.encode([business_text])
                    target_embedding = self.sentence_model.encode([target_text])
                    
                    similarity = torch.cosine_similarity(
                        torch.tensor(business_embedding),
                        torch.tensor(target_embedding)
                    )[0].item()
                    
                    business['ai_relevance_score'] = similarity
                    
                    if similarity > 0.2:  # Threshold for relevance
                        validated.append(business)
                else:
                    validated.append(business)
                    
            except Exception as e:
                self.logger.debug(f"AI validation failed: {e}")
                validated.append(business)
        
        return validated

    def _meets_minimum_requirements(self, business_data):
        """Check if business meets minimum data requirements"""
        # Must have at least name and one contact method
        has_name = business_data.get('name') and len(business_data['name']) > 3
        has_contact = business_data.get('phone') or business_data.get('email')
        
        # If AI models are available, use relevance scoring
        # If not, just check for basic business indicators
        if HAS_AI and hasattr(self, 'sentence_model') and self.sentence_model:
            has_relevance = business_data.get('ai_relevance_score', 0) > 0.1
        else:
            # Fallback: basic keyword relevance check
            text_to_check = f"{business_data.get('name', '')} {business_data.get('description', '')} {business_data.get('website', '')}"
            basic_keywords = ['scrap', 'metal', 'recycling', 'salvage', 'steel', 'aluminum', 'copper', 'iron', 'brass']
            has_relevance = any(keyword in text_to_check.lower() for keyword in basic_keywords)
        
        return has_name and has_contact and has_relevance

    def _calculate_data_completeness(self, business_data):
        """Calculate data completeness percentage"""
        important_fields = [
            'name', 'phone', 'email', 'website', 'address', 'city', 'state',
            'description', 'materials_accepted', 'services', 'working_hours'
        ]
        
        filled_fields = 0
        for field in important_fields:
            value = business_data.get(field)
            if value and (not isinstance(value, list) or len(value) > 0):
                filled_fields += 1
        
        return int((filled_fields / len(important_fields)) * 100)

    def _finalize_results(self, businesses, target_count):
        """Finalize and optimize results"""
        # Remove duplicates
        unique_businesses = self._remove_duplicates(businesses)
        
        # Sort by AI relevance score and data completeness
        unique_businesses.sort(
            key=lambda x: (x.get('ai_relevance_score', 0), x.get('data_completeness', 0)),
            reverse=True
        )
        
        return unique_businesses[:target_count]

    def _remove_duplicates(self, businesses):
        """Remove duplicate businesses"""
        seen_contacts = set()
        seen_names = set()
        unique_businesses = []
        
        for business in businesses:
            phone = business.get('phone', '')
            email = business.get('email', '')
            name = business.get('name', '').lower()
            
            # Create unique identifiers
            contact_key = f"{phone}|{email}"
            name_key = name[:50]  # First 50 chars of name
            
            if contact_key not in seen_contacts and name_key not in seen_names:
                seen_contacts.add(contact_key)
                seen_names.add(name_key)
                unique_businesses.append(business)
        
        return unique_businesses

    def _extract_phone_simple(self, text):
        """Simple phone extraction that should work reliably"""
        if not text:
            return None
        
        # Simple US phone patterns
        patterns = [
            r'\((\d{3})\)[\s\-]?(\d{3})[\s\-]?(\d{4})',  # (123) 456-7890
            r'(\d{3})[\s\-\.](\d{3})[\s\-\.](\d{4})',     # 123-456-7890 or 123.456.7890
            r'(\d{3})\s(\d{3})\s(\d{4})',                 # 123 456 7890
            r'1[\s\-]?(\d{3})[\s\-]?(\d{3})[\s\-]?(\d{4})', # 1-123-456-7890
        ]
        
        for pattern in patterns:
            matches = re.findall(pattern, text)
            if matches:
                match = matches[0]
                if len(match) == 3:
                    area, exchange, number = match
                    # Simple validation - exclude obviously invalid
                    if area != '000' and exchange != '000' and number != '0000':
                        return f"({area}) {exchange}-{number}"
                elif len(match) == 4:  # For pattern with country code
                    area, exchange, number = match[1], match[2], match[3]
                    if area != '000' and exchange != '000' and number != '0000':
                        return f"({area}) {exchange}-{number}"
        
        return None
    
    def _extract_email_simple(self, text):
        """Simple email extraction that should work reliably"""
        if not text:
            return None
        
        # Simple email pattern
        email_pattern = r'[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}'
        
        matches = re.findall(email_pattern, text)
        for email in matches:
            email = email.lower()
            # Exclude obvious test domains
            if not any(bad in email for bad in ['example.com', 'test.com', 'sample.com', 'placeholder.com']):
                return email
        
        return None

    def _calculate_contact_percentage(self):
        """Calculate percentage of businesses with contacts"""
        if not self.results:
            return 0
        
        with_contacts = sum(1 for business in self.results
                           if business.get('phone') or business.get('email'))
        return (with_contacts / len(self.results)) * 100

    def _deduplicate_links(self, links):
        """Remove duplicate links"""
        seen_urls = set()
        unique_links = []
        
        for link in links:
            url = link['url']
            if url not in seen_urls:
                seen_urls.add(url)
                unique_links.append(link)
        
        return unique_links

    def export_results(self, output_dir="output"):
        """Export results with enhanced formatting"""
        if not self.results:
            self.logger.warning("No data to export")
            return None
        
        os.makedirs(output_dir, exist_ok=True)
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        # Create DataFrame
        df = pd.DataFrame(self.results)
        
        # CSV export
        csv_file = os.path.join(output_dir, f"ai_enhanced_metal_businesses_{timestamp}.csv")
        df.to_csv(csv_file, index=False)
        
        # Excel export with multiple sheets
        excel_file = os.path.join(output_dir, f"ai_enhanced_metal_businesses_{timestamp}.xlsx")
        with pd.ExcelWriter(excel_file, engine='openpyxl') as writer:
            # Main sheet
            df.to_excel(writer, sheet_name='All Businesses', index=False)
            
            # High-quality data sheet
            high_quality = df[df['data_completeness'] >= 70]
            if not high_quality.empty:
                high_quality.to_excel(writer, sheet_name='High Quality Data', index=False)
            
            # Contact sheet
            contact_columns = ['name', 'phone', 'email', 'website', 'address', 'city', 'state']
            available_columns = [col for col in contact_columns if col in df.columns]
            if available_columns:
                df[available_columns].to_excel(writer, sheet_name='Contact Information', index=False)
            
            # AI insights sheet
            ai_columns = ['name', 'ai_relevance_score', 'data_completeness', 'materials_accepted']
            available_ai_columns = [col for col in ai_columns if col in df.columns]
            if available_ai_columns:
                df[available_ai_columns].to_excel(writer, sheet_name='AI Insights', index=False)
        
        # JSON export
        json_file = os.path.join(output_dir, f"ai_enhanced_metal_businesses_{timestamp}.json")
        with open(json_file, 'w', encoding='utf-8') as f:
            json.dump(self.results, f, indent=2, default=str, ensure_ascii=False)
        
        # Generate report
        report_file = self._generate_report(output_dir, timestamp)
        
        self.logger.info(f"✅ AI-Enhanced data exported:")
        self.logger.info(f"  • CSV: {csv_file}")
        self.logger.info(f"  • Excel: {excel_file}")
        self.logger.info(f"  • JSON: {json_file}")
        self.logger.info(f"  • Report: {report_file}")
        
        return {
            'csv': csv_file,
            'excel': excel_file,
            'json': json_file,
            'report': report_file,
            'count': len(self.results)
        }

    def _generate_report(self, output_dir, timestamp):
        """Generate comprehensive report"""
        report_file = os.path.join(output_dir, f"ai_enhanced_report_{timestamp}.txt")
        
        with open(report_file, 'w', encoding='utf-8') as f:
            f.write("🤖 AI-ENHANCED METAL SCRAPER REPORT\n")
            f.write("=" * 60 + "\n\n")
            f.write(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
            f.write(f"AI Models: {'✅ Active' if HAS_AI else '❌ Basic Mode'}\n")
            f.write(f"Phone Library: {'✅ phonenumbers' if HAS_PHONENUMBERS else '❌ regex only'}\n")
            f.write(f"Local LLM: {'✅ Ollama' if HAS_OLLAMA else '❌ Not available'}\n\n")
            
            # Statistics
            total = len(self.results)
            with_phone = sum(1 for b in self.results if b.get('phone'))
            with_email = sum(1 for b in self.results if b.get('email'))
            
            f.write("📊 EXTRACTION STATISTICS\n")
            f.write("-" * 30 + "\n")
            f.write(f"Total Businesses: {total}\n")
            f.write(f"With Phone: {with_phone} ({with_phone/total*100:.1f}%)\n")
            f.write(f"With Email: {with_email} ({with_email/total*100:.1f}%)\n")
            f.write(f"Average Completeness: {sum(b.get('data_completeness', 0) for b in self.results)/total:.1f}%\n")
            
            if HAS_AI:
                avg_relevance = sum(b.get('ai_relevance_score', 0) for b in self.results) / total
                f.write(f"Average AI Relevance: {avg_relevance:.3f}\n")
            
            f.write("\n🎯 DATA QUALITY INSIGHTS\n")
            f.write("-" * 30 + "\n")
            f.write("✅ Enhanced phone validation with phonenumbers library\n")
            f.write("✅ AI-powered relevance scoring\n")
            f.write("✅ Comprehensive data extraction\n")
            f.write("✅ Duplicate removal and validation\n")
            f.write("✅ Multi-source search strategy\n")
        
        return report_file

def main():
    print("🤖 AI-ENHANCED METAL SCRAPER")
    print("=" * 50)
    print("🧠 Powered by AI: Better search, extraction & validation")
    print("📞 Advanced phone handling with phonenumbers library")
    print("🔍 Multi-engine search with AI query generation")
    print("🎯 AI relevance scoring and data validation")
    print("📊 Comprehensive data extraction")
    
    # Check AI availability
    if HAS_AI:
        print("✅ AI models loaded successfully")
    else:
        print("⚠️  AI models not available - using basic mode")
    
    if HAS_PHONENUMBERS:
        print("✅ phonenumbers library available")
    else:
        print("⚠️  phonenumbers library not available")
    
    if HAS_OLLAMA:
        print("✅ Ollama available for local LLM")
    else:
        print("⚠️  Ollama not available")
    
    scraper = AIEnhancedMetalScraper()
    
    try:
        target_count = input("\nHow many businesses to find? (default 100): ").strip()
        target_count = int(target_count) if target_count else 100
        
        print(f"\n🚀 Starting AI-enhanced scraping for {target_count} businesses...")
        print("🤖 Using AI models for intelligent extraction")
        print("📱 Advanced phone number validation")
        print("🔍 Multi-source search strategy")
        print("⏱️ Estimated time: 10-20 minutes for comprehensive data")
        
        confirm = input("\n🤖 Start AI-enhanced scraping? (y/N): ").lower().strip()
        if confirm != 'y':
            print("❌ Scraping cancelled")
            return
        
        # Run scraping
        results = scraper.run_comprehensive_scraping(target_count)
        
        if results:
            print(f"\n🎉 AI-ENHANCED SCRAPING COMPLETED!")
            print(f"📊 Businesses found: {len(results)}")
            print(f"📞 Contact coverage: {scraper._calculate_contact_percentage():.1f}%")
            
            # Export results
            print(f"\n📁 Exporting AI-enhanced data...")
            output_info = scraper.export_results()
            
            if output_info:
                print(f"\n🎉 AI-ENHANCED DATA EXPORTED:")
                print(f"📄 Files ready with comprehensive business data")
                print(f"🤖 AI-validated and scored results")
                print(f"📞 Enhanced phone number validation")
                print(f"🎯 Ready for high-quality outreach!")
            else:
                print("\n❌ Error exporting data")
        else:
            print("\n❌ No businesses found")
            
    except KeyboardInterrupt:
        print("\n⏹️  Scraping stopped by user")
        if scraper.results:
            print("💾 Saving partial results...")
            scraper.export_results()
    except Exception as e:
        print(f"\n❌ Error: {e}")
        scraper.logger.error(f"Main error: {e}")

if __name__ == "__main__":
    main()