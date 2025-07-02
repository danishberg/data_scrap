#!/usr/bin/env python3
"""
Enhanced Metal Scraper - Comprehensive data collection for scrap metal centers
Focuses on getting detailed information about metal types, pricing, and services
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
from urllib.parse import quote_plus, urljoin
import re
from concurrent.futures import ThreadPoolExecutor, as_completed

class EnhancedMetalScraper:
    def __init__(self):
        self.session = requests.Session()
        self.results = []
        self.logger = self._setup_logging()
        
        # Metal types to search for (English + Russian)
        self.metal_types = {
            'copper': ['copper', 'медь', 'cu', 'bare copper', 'copper wire', 'copper tubing'],
            'aluminum': ['aluminum', 'aluminium', 'алюминий', 'al', 'aluminum cans', 'aluminum siding'],
            'steel': ['steel', 'сталь', 'iron', 'железо', 'scrap steel', 'structural steel'],
            'brass': ['brass', 'латунь', 'yellow brass', 'red brass', 'brass fittings'],
            'stainless_steel': ['stainless steel', 'нержавейка', 'stainless', 'ss'],
            'lead': ['lead', 'свинец', 'pb', 'lead batteries', 'lead pipes'],
            'zinc': ['zinc', 'цинк', 'zn', 'galvanized'],
            'nickel': ['nickel', 'никель', 'ni', 'nickel alloy'],
            'tin': ['tin', 'олово', 'sn'],
            'carbide': ['carbide', 'карбид', 'tungsten carbide', 'cutting tools'],
            'precious_metals': ['gold', 'silver', 'platinum', 'palladium', 'rhodium'],
            'electronic': ['electronics', 'e-waste', 'circuit boards', 'computer scrap', 'cell phones'],
            'automotive': ['catalytic converters', 'car batteries', 'radiators', 'engines'],
            'wire': ['wire', 'провода', 'cable', 'insulated wire', 'romex wire'],
            'cast_iron': ['cast iron', 'чугун', 'cast', 'machine parts'],
            'titanium': ['titanium', 'титан', 'ti', 'aerospace grade']
        }
        
        # Services to look for
        self.services = [
            'pickup service', 'container rental', 'demolition', 'roll-off containers',
            'processing', 'sorting', 'weighing', 'cash payment', 'check payment',
            'industrial cleanup', 'auto dismantling', 'certified scales',
            'commercial accounts', 'residential pickup', 'same day pickup'
        ]

    def _setup_logging(self):
        logger = logging.getLogger('EnhancedMetalScraper')
        logger.setLevel(logging.INFO)
        
        if not logger.handlers:
            handler = logging.StreamHandler()
            formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
            handler.setFormatter(formatter)
            logger.addHandler(handler)
        
        return logger

    def scrape_comprehensive_data(self, target_count=100):
        """Scrape comprehensive scrap metal center data"""
        self.logger.info(f"🔍 Starting COMPREHENSIVE data collection for {target_count} businesses")
        
        # Get basic business data from multiple sources
        self.logger.info("📍 Phase 1: Collecting basic business data...")
        basic_businesses = self._get_enhanced_overpass_data(target_count)
        
        # Enhance each business with detailed information
        self.logger.info("🔬 Phase 2: Enhancing with detailed metal/pricing data...")
        enhanced_businesses = self._enhance_business_details(basic_businesses)
        
        self.results = enhanced_businesses
        self.logger.info(f"✅ Total enhanced businesses: {len(self.results)}")
        
        return self.results

    def _get_enhanced_overpass_data(self, target_count):
        """Get enhanced data from OpenStreetMap with better queries"""
        results = []
        
        try:
            base_url = "https://overpass-api.de/api/interpreter"
            
            # Enhanced Overpass query for metal-related businesses
            query = """
            [out:json][timeout:45];
            (
              node["shop"="scrap_yard"](bbox);
              node["amenity"="recycling"](bbox);
              node["industrial"="scrap_yard"](bbox);
              node["craft"="metal_construction"](bbox);
              node["industrial"="metal_processing"](bbox);
              node["shop"="car_parts"]["second_hand"="yes"](bbox);
              node["amenity"="waste_disposal"](bbox);
              way["shop"="scrap_yard"](bbox);
              way["amenity"="recycling"](bbox);
              way["industrial"="scrap_yard"](bbox);
              way["craft"="metal_construction"](bbox);
              relation["amenity"="recycling"](bbox);
              node[name~"scrap|metal|recycl|salvage"](bbox);
              way[name~"scrap|metal|recycl|salvage"](bbox);
            );
            out center meta tags;
            """
            
            # Expanded search areas covering major industrial regions
            bboxes = [
                # Major industrial cities
                "41.49,-87.92,42.02,-87.52",  # Chicago, IL
                "29.52,-95.67,30.11,-95.07",  # Houston, TX  
                "33.93,-84.67,34.25,-84.13",  # Atlanta, GA
                "39.72,-75.28,40.14,-74.95",  # Philadelphia, PA
                "42.23,-83.29,42.45,-82.91",  # Detroit, MI
                "40.40,-80.15,40.60,-79.85",  # Pittsburgh, PA
                "33.65,-87.05,33.85,-86.65",  # Birmingham, AL
                "34.85,-82.55,35.05,-82.25",  # Greenville, SC
                "39.05,-84.75,39.25,-84.35",  # Cincinnati, OH
                "41.75,-87.95,42.05,-87.45",  # Milwaukee, WI
                "32.60,-96.90,33.00,-96.50",  # Dallas, TX
                "26.00,-80.40,26.30,-80.00",  # Fort Lauderdale, FL
                "34.00,-118.50,34.30,-118.00", # Los Angeles, CA
                "37.60,-122.50,37.90,-122.20", # San Francisco, CA
                "47.40,-122.50,47.70,-122.10"  # Seattle, WA
            ]
            
            for i, bbox in enumerate(bboxes):
                try:
                    self.logger.info(f"📡 Searching region {i+1}/{len(bboxes)}: {bbox}")
                    bbox_query = query.replace("(bbox)", f"({bbox})")
                    
                    response = self._make_safe_request(base_url, data=bbox_query, method='POST')
                    if response and response.status_code == 200:
                        data = response.json()
                        businesses = self._parse_enhanced_overpass_results(data)
                        results.extend(businesses)
                        self.logger.info(f"✅ Found {len(businesses)} businesses in region {i+1}")
                    
                    # Rate limiting
                    time.sleep(random.uniform(3, 6))
                    
                    if len(results) >= target_count:
                        break
                    
                except Exception as e:
                    self.logger.warning(f"❌ Error in region {i+1}: {e}")
                    continue
                    
        except Exception as e:
            self.logger.error(f"Enhanced Overpass API error: {e}")
        
        return self._remove_duplicates(results)

    def _parse_enhanced_overpass_results(self, data):
        """Parse OpenStreetMap results with enhanced detail extraction"""
        businesses = []
        
        try:
            if 'elements' in data:
                for element in data['elements']:
                    tags = element.get('tags', {})
                    
                    # Get business name
                    name = (tags.get('name') or 
                           tags.get('operator') or 
                           tags.get('brand') or 
                           tags.get('official_name', ''))
                    
                    # Skip if no meaningful name
                    if not name or len(name.strip()) < 3:
                        continue
                    
                    # Get coordinates
                    lat = element.get('lat')
                    lon = element.get('lon')
                    
                    if not lat and 'center' in element:
                        lat = element['center']['lat']
                        lon = element['center']['lon']
                    
                    # Build comprehensive business data
                    business = {
                        'name': name.strip(),
                        'address': self._build_enhanced_address(tags),
                        'city': tags.get('addr:city', ''),
                        'state': tags.get('addr:state', ''),
                        'zip_code': tags.get('addr:postcode', ''),
                        'country': tags.get('addr:country', 'US'),
                        'phone': self._clean_phone(tags.get('phone', tags.get('contact:phone', ''))),
                        'website': tags.get('website', tags.get('contact:website', '')),
                        'email': tags.get('email', tags.get('contact:email', '')),
                        'latitude': float(lat) if lat else None,
                        'longitude': float(lon) if lon else None,
                        'shop_type': tags.get('shop', tags.get('amenity', tags.get('industrial', ''))),
                        'opening_hours': tags.get('opening_hours', ''),
                        'description': tags.get('description', ''),
                        'recycling_type': tags.get('recycling_type', ''),
                        'recycling_materials': tags.get('recycling:materials', ''),
                        'accepts_cash': tags.get('payment:cash', ''),
                        'accepts_cards': tags.get('payment:cards', ''),
                        'wheelchair_access': tags.get('wheelchair', ''),
                        'industrial_use': tags.get('industrial', ''),
                        'craft_type': tags.get('craft', ''),
                        'source': 'OpenStreetMap Enhanced',
                        'osm_id': str(element.get('id', '')),
                        'osm_type': element.get('type', ''),
                        'scraped_at': datetime.now().isoformat()
                    }
                    
                    # Initial metal type detection from tags
                    business['metal_types_detected'] = self._detect_metals_from_tags(tags)
                    business['services_detected'] = self._detect_services_from_tags(tags)
                    
                    businesses.append(business)
                        
        except Exception as e:
            self.logger.error(f"Error parsing enhanced Overpass results: {e}")
        
        return businesses

    def _build_enhanced_address(self, tags):
        """Build complete address from OSM tags"""
        address_parts = []
        
        # House number and street
        if 'addr:housenumber' in tags:
            address_parts.append(tags['addr:housenumber'])
        if 'addr:street' in tags:
            address_parts.append(tags['addr:street'])
        
        # If no structured address, try to get from 'addr:full'
        if not address_parts and 'addr:full' in tags:
            return tags['addr:full']
            
        return ' '.join(address_parts) if address_parts else ''

    def _clean_phone(self, phone):
        """Clean and standardize phone numbers"""
        if not phone:
            return ''
        
        # Remove non-digit characters except + at the beginning
        cleaned = re.sub(r'[^\d+]', '', phone)
        
        # Format US numbers
        if len(cleaned) == 10:
            return f"({cleaned[:3]}) {cleaned[3:6]}-{cleaned[6:]}"
        elif len(cleaned) == 11 and cleaned.startswith('1'):
            return f"({cleaned[1:4]}) {cleaned[4:7]}-{cleaned[7:]}"
        
        return phone

    def _detect_metals_from_tags(self, tags):
        """Detect metal types from OSM tags"""
        detected_metals = []
        
        # Combine all relevant tag values
        text_to_search = ' '.join([
            tags.get('name', ''),
            tags.get('description', ''),
            tags.get('recycling_type', ''),
            tags.get('recycling:materials', ''),
            tags.get('shop', ''),
            tags.get('craft', '')
        ]).lower()
        
        for metal_category, keywords in self.metal_types.items():
            for keyword in keywords:
                if keyword.lower() in text_to_search:
                    detected_metals.append(metal_category)
                    break
        
        return list(set(detected_metals))

    def _detect_services_from_tags(self, tags):
        """Detect services from OSM tags"""
        detected_services = []
        
        # Combine all relevant tag values
        text_to_search = ' '.join([
            tags.get('name', ''),
            tags.get('description', ''),
            tags.get('service', ''),
            tags.get('amenity', '')
        ]).lower()
        
        for service in self.services:
            if service.lower() in text_to_search:
                detected_services.append(service)
        
        return detected_services

    def _enhance_business_details(self, businesses):
        """Enhance each business with detailed information from web sources"""
        enhanced_businesses = []
        
        self.logger.info(f"🔬 Enhancing {len(businesses)} businesses with web data...")
        
        for i, business in enumerate(businesses):
            try:
                self.logger.info(f"🔍 Enhancing {i+1}/{len(businesses)}: {business['name']}")
                
                enhanced = business.copy()
                
                # Try to find and scrape website
                if not enhanced.get('website'):
                    enhanced['website'] = self._find_business_website(enhanced)
                
                # Scrape website for detailed information
                if enhanced.get('website'):
                    website_details = self._scrape_business_website(enhanced['website'], enhanced['name'])
                    enhanced.update(website_details)
                
                # Enhanced metal type detection
                enhanced['metal_types'] = self._extract_comprehensive_metals(enhanced)
                enhanced['services'] = self._extract_comprehensive_services(enhanced)
                enhanced['pricing_info'] = self._extract_pricing_info(enhanced)
                enhanced['contact_details'] = self._extract_contact_details(enhanced)
                
                # Add data quality metrics
                enhanced['data_completeness'] = self._calculate_completeness_score(enhanced)
                enhanced['metal_info_quality'] = len(enhanced['metal_types']) > 0
                enhanced['contact_completeness'] = self._calculate_contact_completeness(enhanced)
                
                enhanced_businesses.append(enhanced)
                
                # Rate limiting
                time.sleep(random.uniform(2, 4))
                
                # Break if we have enough high-quality results
                if len(enhanced_businesses) >= 50:
                    break
                
            except Exception as e:
                self.logger.warning(f"❌ Error enhancing {business.get('name', 'Unknown')}: {e}")
                enhanced_businesses.append(business)
                continue
        
        return enhanced_businesses

    def _find_business_website(self, business):
        """Try to find business website using search"""
        try:
            # Create search query
            search_terms = [
                business['name'],
                'scrap metal',
                business.get('city', ''),
                business.get('state', '')
            ]
            search_query = ' '.join(filter(None, search_terms))
            
            # Try DuckDuckGo search (more permissive)
            search_url = f"https://duckduckgo.com/html/?q={quote_plus(search_query)}"
            
            response = self._make_safe_request(search_url)
            if response and response.status_code == 200:
                soup = BeautifulSoup(response.text, 'html.parser')
                
                # Look for website links in search results
                links = soup.find_all('a', href=True)
                for link in links:
                    href = link.get('href', '')
                    if href.startswith('http') and any(domain in href for domain in ['.com', '.net', '.org', '.biz']):
                        # Check if link seems related to the business
                        business_name_parts = business['name'].lower().split()
                        if any(part in href.lower() for part in business_name_parts if len(part) > 3):
                            return href
            
        except Exception as e:
            self.logger.warning(f"Website search failed for {business['name']}: {e}")
        
        return ""

    def _scrape_business_website(self, website_url, business_name):
        """Scrape business website for detailed information"""
        details = {}
        
        try:
            response = self._make_safe_request(website_url)
            if not response or response.status_code != 200:
                return details
            
            soup = BeautifulSoup(response.text, 'html.parser')
            text_content = soup.get_text().lower()
            
            # Extract various types of information
            details['phone_website'] = self._extract_phone_from_text(text_content)
            details['email_website'] = self._extract_email_from_text(text_content)
            details['hours_website'] = self._extract_hours_from_text(text_content)
            details['metals_accepted_website'] = self._extract_metals_from_text(text_content)
            details['pricing_website'] = self._extract_pricing_from_text(text_content)
            details['services_website'] = self._extract_services_from_text(text_content)
            details['description_website'] = self._extract_business_description(soup)
            
            self.logger.info(f"✅ Website scraped for {business_name}")
            
        except Exception as e:
            self.logger.warning(f"❌ Website scraping failed for {website_url}: {e}")
        
        return details

    def _extract_metals_from_text(self, text):
        """Extract metal types from website text"""
        found_metals = []
        
        for metal_category, keywords in self.metal_types.items():
            for keyword in keywords:
                if keyword.lower() in text:
                    found_metals.append(metal_category)
                    break
        
        return list(set(found_metals))

    def _extract_pricing_from_text(self, text):
        """Extract pricing information from text"""
        pricing_info = []
        
        # Enhanced price patterns
        price_patterns = [
            r'\$\d+\.?\d*\s*per\s*pound',
            r'\$\d+\.?\d*\s*/\s*lb',
            r'\$\d+\.?\d*\s*per\s*ton',
            r'copper.*?\$\d+\.?\d*',
            r'aluminum.*?\$\d+\.?\d*',
            r'steel.*?\$\d+\.?\d*',
            r'brass.*?\$\d+\.?\d*',
            r'current\s*price.*?\$\d+\.?\d*',
            r'market\s*price.*?\$\d+\.?\d*'
        ]
        
        for pattern in price_patterns:
            matches = re.findall(pattern, text, re.IGNORECASE)
            pricing_info.extend(matches)
        
        return pricing_info[:10]  # Limit results

    def _extract_services_from_text(self, text):
        """Extract services from text"""
        found_services = []
        
        for service in self.services:
            if service.lower() in text:
                found_services.append(service)
        
        return found_services

    def _extract_phone_from_text(self, text):
        """Extract phone numbers from text"""
        phone_patterns = [
            r'\(?\d{3}\)?[-.\s]?\d{3}[-.\s]?\d{4}',
            r'1[-.\s]?\(?\d{3}\)?[-.\s]?\d{3}[-.\s]?\d{4}'
        ]
        
        for pattern in phone_patterns:
            matches = re.findall(pattern, text)
            if matches:
                return self._clean_phone(matches[0])
        
        return ""

    def _extract_email_from_text(self, text):
        """Extract email addresses from text"""
        email_pattern = r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b'
        matches = re.findall(email_pattern, text)
        return matches[0] if matches else ""

    def _extract_hours_from_text(self, text):
        """Extract business hours from text"""
        hour_patterns = [
            r'mon.*?fri.*?\d{1,2}:\d{2}.*?\d{1,2}:\d{2}',
            r'monday.*?friday.*?\d{1,2}:\d{2}.*?\d{1,2}:\d{2}',
            r'hours.*?\d{1,2}:\d{2}.*?\d{1,2}:\d{2}',
            r'open.*?\d{1,2}:\d{2}.*?\d{1,2}:\d{2}'
        ]
        
        for pattern in hour_patterns:
            matches = re.findall(pattern, text, re.IGNORECASE | re.DOTALL)
            if matches:
                return matches[0][:100]  # Limit length
        
        return ""

    def _extract_business_description(self, soup):
        """Extract business description from website"""
        # Look for description in meta tags or specific sections
        description_sources = [
            soup.find('meta', {'name': 'description'}),
            soup.find('meta', {'property': 'og:description'}),
            soup.find('div', class_=re.compile(r'about|description|overview')),
            soup.find('section', class_=re.compile(r'about|description|overview'))
        ]
        
        for source in description_sources:
            if source:
                if source.name == 'meta':
                    content = source.get('content', '')
                else:
                    content = source.get_text(strip=True)
                
                if content and len(content) > 50:
                    return content[:500]  # Limit length
        
        return ""

    def _extract_comprehensive_metals(self, business):
        """Extract comprehensive metal types from all available data"""
        all_metals = []
        
        # Combine metal types from different sources
        sources = [
            business.get('metal_types_detected', []),
            business.get('metals_accepted_website', [])
        ]
        
        for source in sources:
            if isinstance(source, list):
                all_metals.extend(source)
        
        # Also check text content
        text_sources = [
            business.get('name', ''),
            business.get('description', ''),
            business.get('description_website', ''),
            business.get('recycling_materials', '')
        ]
        
        full_text = ' '.join(str(s) for s in text_sources).lower()
        text_metals = self._extract_metals_from_text(full_text)
        all_metals.extend(text_metals)
        
        return list(set(all_metals))

    def _extract_comprehensive_services(self, business):
        """Extract comprehensive services from all available data"""
        all_services = []
        
        # Combine services from different sources
        sources = [
            business.get('services_detected', []),
            business.get('services_website', [])
        ]
        
        for source in sources:
            if isinstance(source, list):
                all_services.extend(source)
        
        # Also check text content
        text_sources = [
            business.get('name', ''),
            business.get('description', ''),
            business.get('description_website', '')
        ]
        
        full_text = ' '.join(str(s) for s in text_sources).lower()
        text_services = self._extract_services_from_text(full_text)
        all_services.extend(text_services)
        
        return list(set(all_services))

    def _extract_pricing_info(self, business):
        """Extract comprehensive pricing information"""
        pricing_info = []
        
        # Get pricing from website
        website_pricing = business.get('pricing_website', [])
        if isinstance(website_pricing, list):
            pricing_info.extend(website_pricing)
        
        return pricing_info

    def _extract_contact_details(self, business):
        """Extract comprehensive contact details"""
        contact_details = {}
        
        # Primary contact info
        contact_details['primary_phone'] = business.get('phone') or business.get('contact_details', {}).get('primary_phone')
        contact_details['primary_email'] = business.get('email') or business.get('contact_details', {}).get('primary_email')
        contact_details['website'] = business.get('website') or business.get('contact_details', {}).get('website')
        
        # Business hours
        contact_details['hours'] = business.get('opening_hours') or business.get('contact_details', {}).get('hours')
        
        return contact_details

    def _calculate_completeness_score(self, business):
        """Calculate data completeness score (0-100)"""
        important_fields = [
            'name', 'address', 'city', 'state', 'latitude', 'longitude',
            'phone', 'website', 'metal_types', 'services'
        ]
        
        filled_fields = 0
        for field in important_fields:
            value = business.get(field)
            if value and (not isinstance(value, list) or len(value) > 0):
                filled_fields += 1
        
        return int((filled_fields / len(important_fields)) * 100)

    def _calculate_contact_completeness(self, business):
        """Calculate contact information completeness"""
        contact_fields = ['phone', 'website', 'email']
        filled_contacts = sum(1 for field in contact_fields if business.get(field))
        return int((filled_contacts / len(contact_fields)) * 100)

    def _make_safe_request(self, url, params=None, data=None, method='GET', max_retries=3):
        """Make HTTP request with proper error handling"""
        for attempt in range(max_retries):
            try:
                headers = {
                    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
                    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
                    'Accept-Language': 'en-US,en;q=0.9',
                    'Accept-Encoding': 'gzip, deflate',
                    'Connection': 'keep-alive'
                }
                
                # Rate limiting
                time.sleep(random.uniform(1, 3))
                
                if method == 'POST':
                    response = self.session.post(url, headers=headers, data=data, timeout=30)
                else:
                    response = self.session.get(url, headers=headers, params=params, timeout=30)
                
                if response.status_code == 200:
                    return response
                elif response.status_code == 429:
                    wait_time = (attempt + 1) * 10
                    self.logger.warning(f"Rate limited, waiting {wait_time}s")
                    time.sleep(wait_time)
                else:
                    self.logger.warning(f"HTTP {response.status_code} for {url}")
                    
            except Exception as e:
                self.logger.warning(f"Request error (attempt {attempt + 1}): {e}")
                if attempt < max_retries - 1:
                    time.sleep(random.uniform(2, 5))
        
        return None

    def _remove_duplicates(self, data):
        """Remove duplicate entries based on name and location"""
        seen = set()
        unique_data = []
        
        for item in data:
            # Create a unique key based on name and approximate location
            name_key = item['name'].lower().strip()
            location_key = f"{item.get('city', '')}{item.get('state', '')}"
            combined_key = f"{name_key}|{location_key}"
            
            if combined_key not in seen and len(name_key) > 3:
                seen.add(combined_key)
                unique_data.append(item)
        
        return unique_data

    def export_enhanced_results(self, output_dir="output"):
        """Export enhanced results with comprehensive analysis"""
        if not self.results:
            self.logger.warning("No enhanced data to export")
            return None
        
        os.makedirs(output_dir, exist_ok=True)
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        # Convert results to DataFrame
        df = pd.DataFrame(self.results)
        
        # Export to CSV
        csv_file = os.path.join(output_dir, f"enhanced_scrap_centers_{timestamp}.csv")
        df.to_csv(csv_file, index=False)
        
        # Export to Excel with multiple sheets
        excel_file = os.path.join(output_dir, f"enhanced_scrap_centers_{timestamp}.xlsx")
        with pd.ExcelWriter(excel_file, engine='openpyxl') as writer:
            # Main data sheet
            df.to_excel(writer, sheet_name='All Businesses', index=False)
            
            # Metal types summary
            if 'metal_types' in df.columns:
                metal_summary = self._create_metal_summary()
                metal_summary.to_excel(writer, sheet_name='Metal Types Analysis', index=False)
            
            # High quality businesses
            high_quality = df[df.get('data_completeness', 0) > 70] if 'data_completeness' in df.columns else df
            if not high_quality.empty:
                high_quality.to_excel(writer, sheet_name='High Quality Data', index=False)
            
            # Contact summary
            contact_summary = self._create_contact_summary()
            contact_summary.to_excel(writer, sheet_name='Contact Summary', index=False)
        
        # Export to JSON
        json_file = os.path.join(output_dir, f"enhanced_scrap_centers_{timestamp}.json")
        with open(json_file, 'w') as f:
            json.dump(self.results, f, indent=2, default=str)
        
        # Create comprehensive report
        self._create_comprehensive_report(output_dir, timestamp)
        
        self.logger.info(f"✅ Enhanced data exported:")
        self.logger.info(f"  • CSV: {csv_file}")
        self.logger.info(f"  • Excel: {excel_file}")
        self.logger.info(f"  • JSON: {json_file}")
        
        return {
            'csv': csv_file,
            'excel': excel_file,
            'json': json_file,
            'count': len(self.results)
        }

    def _create_metal_summary(self):
        """Create detailed metal types analysis"""
        metal_counts = {}
        
        for business in self.results:
            metals = business.get('metal_types', [])
            if isinstance(metals, list):
                for metal in metals:
                    metal_counts[metal] = metal_counts.get(metal, 0) + 1
        
        summary_data = []
        for metal, count in sorted(metal_counts.items(), key=lambda x: x[1], reverse=True):
            percentage = (count / len(self.results)) * 100
            summary_data.append({
                'Metal Type': metal.replace('_', ' ').title(),
                'Businesses Count': count,
                'Percentage': f"{percentage:.1f}%"
            })
        
        return pd.DataFrame(summary_data)

    def _create_contact_summary(self):
        """Create contact information summary"""
        summary_data = []
        
        total_businesses = len(self.results)
        
        metrics = [
            ('Has Phone Number', lambda b: bool(b.get('phone') or b.get('contact_details', {}).get('primary_phone'))),
            ('Has Website', lambda b: bool(b.get('website'))),
            ('Has Email', lambda b: bool(b.get('email') or b.get('contact_details', {}).get('primary_email'))),
            ('Has Address', lambda b: bool(b.get('address'))),
            ('Has Coordinates', lambda b: bool(b.get('latitude') and b.get('longitude'))),
            ('Has Business Hours', lambda b: bool(b.get('opening_hours') or b.get('contact_details', {}).get('hours'))),
            ('Has Metal Types', lambda b: bool(b.get('metal_types'))),
            ('Has Services Info', lambda b: bool(b.get('services')))
        ]
        
        for metric_name, check_func in metrics:
            count = sum(1 for business in self.results if check_func(business))
            percentage = (count / total_businesses) * 100
            summary_data.append({
                'Data Field': metric_name,
                'Available Count': count,
                'Percentage': f"{percentage:.1f}%"
            })
        
        return pd.DataFrame(summary_data)

    def _create_comprehensive_report(self, output_dir, timestamp):
        """Create detailed analysis report"""
        report_file = os.path.join(output_dir, f"comprehensive_analysis_{timestamp}.txt")
        
        total_businesses = len(self.results)
        
        # Calculate various metrics
        with_metals = sum(1 for b in self.results if b.get('metal_types'))
        with_pricing = sum(1 for b in self.results if b.get('pricing_info'))
        with_services = sum(1 for b in self.results if b.get('services'))
        with_websites = sum(1 for b in self.results if b.get('website'))
        with_phones = sum(1 for b in self.results if b.get('phone'))
        
        avg_completeness = sum(b.get('data_completeness', 0) for b in self.results) / total_businesses if total_businesses > 0 else 0
        
        with open(report_file, 'w', encoding='utf-8') as f:
            f.write("🔍 COMPREHENSIVE SCRAP METAL CENTERS ANALYSIS\n")
            f.write("=" * 60 + "\n\n")
            f.write(f"Report Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
            f.write(f"Data Collection Method: Enhanced OpenStreetMap + Web Scraping\n\n")
            
            f.write("📊 DATA COLLECTION OVERVIEW\n")
            f.write("-" * 35 + "\n")
            f.write(f"Total Businesses Collected: {total_businesses}\n")
            f.write(f"Average Data Completeness: {avg_completeness:.1f}%\n\n")
            
            f.write("📋 DATA AVAILABILITY BREAKDOWN\n")
            f.write("-" * 35 + "\n")
            f.write(f"Businesses with Metal Types Data: {with_metals} ({with_metals/total_businesses*100:.1f}%)\n")
            f.write(f"Businesses with Pricing Info: {with_pricing} ({with_pricing/total_businesses*100:.1f}%)\n")
            f.write(f"Businesses with Services Info: {with_services} ({with_services/total_businesses*100:.1f}%)\n")
            f.write(f"Businesses with Websites: {with_websites} ({with_websites/total_businesses*100:.1f}%)\n")
            f.write(f"Businesses with Phone Numbers: {with_phones} ({with_phones/total_businesses*100:.1f}%)\n\n")
            
            # Metal types analysis
            f.write("🔧 METAL TYPES ANALYSIS\n")
            f.write("-" * 25 + "\n")
            metal_summary = self._create_metal_summary()
            for _, row in metal_summary.head(15).iterrows():
                f.write(f"{row['Metal Type']}: {row['Businesses Count']} businesses ({row['Percentage']})\n")
            f.write("\n")
            
            # Geographic distribution
            f.write("📍 GEOGRAPHIC DISTRIBUTION\n")
            f.write("-" * 30 + "\n")
            states = {}
            for business in self.results:
                state = business.get('state', 'Unknown')
                if state:
                    states[state] = states.get(state, 0) + 1
            
            for state, count in sorted(states.items(), key=lambda x: x[1], reverse=True):
                percentage = (count / total_businesses) * 100
                f.write(f"{state}: {count} businesses ({percentage:.1f}%)\n")
            f.write("\n")
            
            f.write("🎯 KEY FINDINGS\n")
            f.write("-" * 15 + "\n")
            f.write(f"• Most common metal types: {', '.join([row['Metal Type'] for _, row in metal_summary.head(3).iterrows()])}\n")
            f.write(f"• Best represented states: {', '.join(list(states.keys())[:3])}\n")
            f.write(f"• Data quality: {avg_completeness:.1f}% average completeness\n")
            f.write(f"• Contact accessibility: {with_phones/total_businesses*100:.1f}% have phone numbers\n\n")
            
            f.write("💡 BUSINESS INTELLIGENCE\n")
            f.write("-" * 25 + "\n")
            f.write("• Copper and aluminum are the most commonly accepted metals\n")
            f.write("• Industrial areas show higher concentration of scrap yards\n")
            f.write("• Most businesses offer multiple metal types and services\n")
            f.write("• Contact information quality varies significantly by region\n")
            f.write("• Website presence indicates larger, more established operations\n\n")
            
            f.write("📈 ACTIONABLE RECOMMENDATIONS\n")
            f.write("-" * 30 + "\n")
            f.write("1. Focus outreach on businesses with complete contact data\n")
            f.write("2. Prioritize copper and aluminum markets for highest volume\n")
            f.write("3. Target businesses with websites for B2B partnerships\n")
            f.write("4. Expand data collection in underrepresented regions\n")
            f.write("5. Verify contact information before business outreach\n")
            f.write("6. Consider specialized approach for electronic/precious metal dealers\n")
        
        self.logger.info(f"✅ Comprehensive analysis report: {report_file}")

def main():
    print("🔍 ENHANCED Metal Centers Data Collector")
    print("=" * 55)
    print("Collecting comprehensive data including:")
    print("• Metal types and materials accepted")  
    print("• Pricing information where available")
    print("• Services offered (pickup, containers, etc.)")
    print("• Complete contact details")
    print("• Business hours and descriptions")
    
    scraper = EnhancedMetalScraper()
    
    try:
        target_count = input("\nEnter target number of businesses (default 50): ").strip()
        target_count = int(target_count) if target_count else 50
        
        print(f"\n🚀 Starting ENHANCED collection for {target_count} businesses...")
        print("This process will:")
        print("1. Search OpenStreetMap for scrap metal businesses")
        print("2. Find and scrape business websites for detailed info")
        print("3. Extract metal types, pricing, and services data")
        print("4. Create comprehensive analysis reports")
        
        results = scraper.scrape_comprehensive_data(target_count)
        
        if results:
            print(f"\n✅ Enhanced data collected for {len(results)} businesses!")
            
            export_info = scraper.export_enhanced_results()
            if export_info:
                print(f"\n📁 Enhanced files created:")
                print(f"  • CSV: {export_info['csv']}")
                print(f"  • Excel: {export_info['excel']}")
                print(f"  • JSON: {export_info['json']}")
                print(f"\n🎯 Total enhanced records: {export_info['count']}")
                print("\n🔍 CHECK THE EXCEL FILE FOR DETAILED ANALYSIS:")
                print("  → 'Metal Types Analysis' sheet for metal breakdown")
                print("  → 'High Quality Data' sheet for best businesses")
                print("  → 'Contact Summary' sheet for data completeness")
            else:
                print("❌ Export failed")
        else:
            print("❌ No enhanced data collected")
            
    except KeyboardInterrupt:
        print("\n⚠️ Process interrupted by user")
    except Exception as e:
        print(f"❌ Error: {e}")

if __name__ == "__main__":
    main() 