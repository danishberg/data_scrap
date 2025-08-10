#!/usr/bin/env python3
"""
Massive Data Collection Script for Scrap Metal Centers
Target: 20,000-100,000+ comprehensive business records
"""

import os
import sys
import time
import logging
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
from collections import defaultdict

# Add current directory to Python path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from config import Config
from main import ScrapMetalScraper
from data_exporter import DataExporter
from signal_handler import setup_signal_handling
from utils import DataProcessor

class MassiveDataCollector:
    """Specialized collector for massive data collection (20K+ entries)"""
    
    def __init__(self):
        self.logger = self._setup_logging()
        self.scraper = ScrapMetalScraper()
        self.data_exporter = DataExporter()
        self.data_processor = DataProcessor()
        self.signal_handler = None
        self.all_results = []
        self.stats = defaultdict(int)
        
    def _setup_logging(self):
        """Setup enhanced logging for massive collection"""
        logging.basicConfig(
            level=getattr(logging, Config.LOG_LEVEL),
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler(Config.LOG_FILE),
                logging.StreamHandler()
            ]
        )
        return logging.getLogger(__name__)
    
    def run_massive_collection(self, target_count=20000):
        """Run massive data collection targeting specified number of entries"""
        
        self.logger.info("🚀 STARTING MASSIVE DATA COLLECTION")
        self.logger.info(f"🎯 Target: {target_count:,} business records")
        self.logger.info("🌍 Coverage: All English-speaking countries")
        self.logger.info("📊 Data Fields: 15+ comprehensive business attributes")
        
        # Setup signal handling
        self.signal_handler = setup_signal_handling(
            logger=self.logger,
            cleanup_functions=[self._cleanup]
        )
        
        start_time = datetime.now()
        
        try:
            # Phase 1: Broad geographic coverage
            self.logger.info("\n📍 PHASE 1: Geographic Coverage Collection")
            phase1_results = self._collect_by_geography(target_count // 3)
            self.all_results.extend(phase1_results)
            self.stats['phase1_count'] = len(phase1_results)
            
            if self._should_stop():
                return self._finalize_results()
            
            # Phase 2: Industry-specific terms
            self.logger.info("\n🔧 PHASE 2: Industry-Specific Collection") 
            phase2_results = self._collect_by_industry_terms(target_count // 3)
            self.all_results.extend(phase2_results)
            self.stats['phase2_count'] = len(phase2_results)
            
            if self._should_stop():
                return self._finalize_results()
            
            # Phase 3: Deep material-specific search
            self.logger.info("\n⚙️ PHASE 3: Material-Specific Deep Search")
            phase3_results = self._collect_by_materials(target_count // 3)
            self.all_results.extend(phase3_results)
            self.stats['phase3_count'] = len(phase3_results)
            
            # Phase 4: Data enhancement and validation
            self.logger.info("\n✨ PHASE 4: Data Enhancement & Validation")
            enhanced_results = self._enhance_and_validate()
            
            # Final processing
            final_results = self._finalize_results()
            
            end_time = datetime.now()
            duration = end_time - start_time
            
            self._print_final_stats(final_results, duration)
            
            return final_results
            
        except KeyboardInterrupt:
            self.logger.info("\n⚠️ Collection interrupted by user")
            return self._finalize_results()
        except Exception as e:
            self.logger.error(f"❌ Massive collection failed: {e}")
            return self._finalize_results()
    
    def _collect_by_geography(self, target_count):
        """Phase 1: Collect by comprehensive geographic coverage"""
        results = []
        locations_per_batch = 10
        
        # Use all available locations from config
        all_locations = Config.DEFAULT_LOCATIONS
        self.logger.info(f"📍 Scanning {len(all_locations)} locations worldwide")
        
        # Primary scraping sources for maximum coverage
        priority_sources = ['google_search', 'google_maps', 'yellowpages', 'yelp']
        
        # Basic search terms for broad coverage
        basic_terms = [
            'scrap metal recycling',
            'metal recycling centers', 
            'scrap yards',
            'metal dealers',
            'пункт приема металлолома'
        ]
        
        total_combinations = len(all_locations) * len(priority_sources) * len(basic_terms)
        completed = 0
        
        self.logger.info(f"🔄 Processing {total_combinations:,} search combinations")
        
        with ThreadPoolExecutor(max_workers=Config.MAX_WORKERS) as executor:
            futures = []
            
            for location in all_locations:
                if self._should_stop() or len(results) >= target_count:
                    break
                    
                for source in priority_sources:
                    if self._should_stop() or len(results) >= target_count:
                        break
                        
                    for term in basic_terms:
                        if self._should_stop() or len(results) >= target_count:
                            break
                        
                        future = executor.submit(
                            self._execute_search,
                            source, term, location, 50  # 50 results per search
                        )
                        futures.append(future)
            
            # Process results as they complete
            for future in as_completed(futures):
                if self._should_stop():
                    break
                
                try:
                    batch_results = future.result()
                    if batch_results:
                        results.extend(batch_results)
                        completed += 1
                        
                        # Progress update
                        if completed % 10 == 0:
                            self.logger.info(
                                f"📊 Phase 1 Progress: {len(results):,} results "
                                f"({completed}/{len(futures)} searches completed)"
                            )
                        
                        # Stop if we hit target
                        if len(results) >= target_count:
                            self.logger.info(f"🎯 Phase 1 target reached: {len(results):,} results")
                            break
                            
                except Exception as e:
                    self.logger.error(f"Search failed: {e}")
        
        self.logger.info(f"✅ Phase 1 completed: {len(results):,} results collected")
        return results
    
    def _collect_by_industry_terms(self, target_count):
        """Phase 2: Industry-specific comprehensive search"""
        results = []
        
        # All search terms from config for comprehensive coverage
        industry_terms = Config.SEARCH_TERMS
        
        # Focus on major metropolitan areas for deeper search
        major_cities = [
            'New York, NY', 'Los Angeles, CA', 'Chicago, IL', 'Houston, TX',
            'Toronto, ON', 'Vancouver, BC', 'London, UK', 'Manchester, UK',
            'Sydney, NSW', 'Melbourne, VIC', 'Auckland, NZ', 'Dublin, Ireland'
        ]
        
        sources = ['google_search', 'google_maps', 'yellowpages', 'yelp']
        
        self.logger.info(f"🔍 Deep industry search: {len(industry_terms)} terms × {len(major_cities)} cities")
        
        with ThreadPoolExecutor(max_workers=Config.MAX_WORKERS) as executor:
            futures = []
            
            for city in major_cities:
                if self._should_stop() or len(results) >= target_count:
                    break
                    
                for term in industry_terms:
                    if self._should_stop() or len(results) >= target_count:
                        break
                        
                    for source in sources:
                        if self._should_stop() or len(results) >= target_count:
                            break
                        
                        future = executor.submit(
                            self._execute_search,
                            source, term, city, 30  # 30 results per search
                        )
                        futures.append(future)
            
            # Process results
            completed = 0
            for future in as_completed(futures):
                if self._should_stop():
                    break
                
                try:
                    batch_results = future.result()
                    if batch_results:
                        results.extend(batch_results)
                        completed += 1
                        
                        if completed % 20 == 0:
                            self.logger.info(
                                f"📊 Phase 2 Progress: {len(results):,} results "
                                f"({completed}/{len(futures)} searches)"
                            )
                        
                        if len(results) >= target_count:
                            break
                            
                except Exception as e:
                    self.logger.error(f"Industry search failed: {e}")
        
        self.logger.info(f"✅ Phase 2 completed: {len(results):,} results collected")
        return results
    
    def _collect_by_materials(self, target_count):
        """Phase 3: Material-specific deep search"""
        results = []
        
        # Comprehensive material-based searches
        material_terms = [
            f"{material} recycling" for material in Config.MATERIAL_TYPES
        ] + [
            f"{material} scrap dealers" for material in Config.MATERIAL_TYPES[:10]
        ] + [
            f"{material} buyers" for material in Config.MATERIAL_TYPES[:10]
        ]
        
        # Regional coverage
        regions = [
            'California', 'Texas', 'Florida', 'New York', 'Pennsylvania',
            'Ontario', 'Quebec', 'British Columbia', 'England', 'Scotland',
            'New South Wales', 'Victoria', 'Queensland'
        ]
        
        sources = ['google_search', 'yellowpages']  # Focus on text-rich sources
        
        self.logger.info(f"⚙️ Material-specific search: {len(material_terms)} terms × {len(regions)} regions")
        
        with ThreadPoolExecutor(max_workers=Config.MAX_WORKERS) as executor:
            futures = []
            
            for region in regions:
                if self._should_stop() or len(results) >= target_count:
                    break
                    
                for term in material_terms:
                    if self._should_stop() or len(results) >= target_count:
                        break
                        
                    for source in sources:
                        if self._should_stop() or len(results) >= target_count:
                            break
                        
                        future = executor.submit(
                            self._execute_search,
                            source, term, region, 25  # 25 results per search
                        )
                        futures.append(future)
            
            # Process results
            completed = 0
            for future in as_completed(futures):
                if self._should_stop():
                    break
                
                try:
                    batch_results = future.result()
                    if batch_results:
                        results.extend(batch_results)
                        completed += 1
                        
                        if completed % 15 == 0:
                            self.logger.info(
                                f"📊 Phase 3 Progress: {len(results):,} results "
                                f"({completed}/{len(futures)} searches)"
                            )
                        
                        if len(results) >= target_count:
                            break
                            
                except Exception as e:
                    self.logger.error(f"Material search failed: {e}")
        
        self.logger.info(f"✅ Phase 3 completed: {len(results):,} results collected")
        return results
    
    def _execute_search(self, source, term, location, limit):
        """Execute a single search operation"""
        try:
            if self._should_stop():
                return []
            
            scraper_class = self.scraper.scrapers.get(source)
            if not scraper_class:
                return []
            
            scraper = scraper_class()
            results = scraper.scrape(
                search_term=term,
                location=location,
                limit=limit
            )
            
            # Add metadata
            for result in results:
                result['search_source'] = source
                result['search_term'] = term
                result['search_location'] = location
                result['collection_timestamp'] = datetime.now().isoformat()
            
            return results
            
        except Exception as e:
            self.logger.error(f"Search failed ({source}, {term}, {location}): {e}")
            return []
    
    def _enhance_and_validate(self):
        """Phase 4: Enhance data quality and add missing fields"""
        self.logger.info("🔍 Enhancing data quality...")
        
        enhanced_count = 0
        
        for result in self.all_results:
            try:
                # Enhance phone numbers
                if result.get('phone_primary'):
                    enhanced_phone = self.data_processor.extract_and_format_phones(
                        result['phone_primary']
                    )
                    if enhanced_phone:
                        result['phone_formatted'] = enhanced_phone[0]
                
                # Enhance email validation
                if result.get('email_primary'):
                    if self.data_processor.validate_email(result['email_primary']):
                        result['email_validated'] = True
                
                # Add data completeness score
                result['data_completeness_score'] = self._calculate_completeness_score(result)
                
                # Add verification status
                result['verification_status'] = self._get_verification_status(result)
                
                enhanced_count += 1
                
                if enhanced_count % 1000 == 0:
                    self.logger.info(f"📊 Enhanced {enhanced_count:,} records...")
                    
            except Exception as e:
                self.logger.error(f"Enhancement failed for record: {e}")
        
        self.logger.info(f"✅ Enhanced {enhanced_count:,} records")
        return self.all_results
    
    def _calculate_completeness_score(self, result):
        """Calculate data completeness score (0-100)"""
        total_fields = 15  # Target comprehensive fields
        completed_fields = 0
        
        key_fields = [
            'name', 'phone_primary', 'address_full', 'city', 'country',
            'email_primary', 'website', 'working_hours', 'materials',
            'latitude', 'longitude', 'description', 'business_type',
            'phone_secondary', 'social_media'
        ]
        
        for field in key_fields:
            if result.get(field):
                completed_fields += 1
        
        return int((completed_fields / total_fields) * 100)
    
    def _get_verification_status(self, result):
        """Determine verification status"""
        score = result.get('data_completeness_score', 0)
        
        if score >= 80:
            return 'verified'
        elif score >= 60:
            return 'partial'
        else:
            return 'basic'
    
    def _finalize_results(self):
        """Remove duplicates and finalize dataset"""
        self.logger.info("🔄 Finalizing dataset...")
        
        # Remove duplicates
        unique_results = self.scraper._remove_duplicates(self.all_results)
        
        # Sort by data completeness score
        unique_results.sort(
            key=lambda x: x.get('data_completeness_score', 0),
            reverse=True
        )
        
        self.logger.info(f"📊 Final dataset: {len(unique_results):,} unique records")
        
        # Export in all formats
        if unique_results:
            self.data_exporter.export_data(unique_results)
            self._create_comprehensive_excel(unique_results)
        
        return unique_results
    
    def _create_comprehensive_excel(self, results):
        """Create comprehensive Excel file with all data"""
        try:
            import pandas as pd
            from datetime import datetime
            
            # Flatten all data for Excel
            flattened_data = []
            
            for result in results:
                flat_record = {}
                
                # Basic information
                flat_record['Business_Name'] = result.get('name', '')
                flat_record['Phone_Primary'] = result.get('phone_primary', '')
                flat_record['Phone_Secondary'] = result.get('phone_secondary', '')
                flat_record['Email_Primary'] = result.get('email_primary', '')
                flat_record['Email_Secondary'] = result.get('email_secondary', '')
                flat_record['Website'] = result.get('website', '')
                
                # Location details
                flat_record['Address_Full'] = result.get('address_full', '')
                flat_record['Street_Address'] = result.get('street_address', '')
                flat_record['City'] = result.get('city', '')
                flat_record['State_Region'] = result.get('state_region', '')
                flat_record['Postal_Code'] = result.get('postal_code', '')
                flat_record['Country'] = result.get('country', '')
                flat_record['Latitude'] = result.get('latitude', '')
                flat_record['Longitude'] = result.get('longitude', '')
                
                # Business details
                flat_record['Description'] = result.get('description', '')
                flat_record['Business_Type'] = result.get('business_type', '')
                flat_record['Working_Hours'] = str(result.get('working_hours', ''))
                
                # Materials and services
                materials = result.get('materials', [])
                flat_record['Materials_Accepted'] = ', '.join(materials) if materials else ''
                flat_record['Materials_Count'] = len(materials)
                
                # Social media
                flat_record['Facebook'] = result.get('facebook_url', '')
                flat_record['Twitter'] = result.get('twitter_url', '')
                flat_record['Instagram'] = result.get('instagram_url', '')
                flat_record['LinkedIn'] = result.get('linkedin_url', '')
                flat_record['WhatsApp'] = result.get('whatsapp_number', '')
                flat_record['Telegram'] = result.get('telegram_contact', '')
                
                # Quality metrics
                flat_record['Data_Completeness_Score'] = result.get('data_completeness_score', 0)
                flat_record['Verification_Status'] = result.get('verification_status', 'basic')
                flat_record['Source'] = result.get('search_source', '')
                flat_record['Search_Term'] = result.get('search_term', '')
                flat_record['Search_Location'] = result.get('search_location', '')
                flat_record['Collection_Date'] = result.get('collection_timestamp', '')
                
                flattened_data.append(flat_record)
            
            # Create DataFrame
            df = pd.DataFrame(flattened_data)
            
            # Generate filename
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            filename = f"comprehensive_scrap_centers_{len(results)}records_{timestamp}.xlsx"
            filepath = Config.OUTPUT_DIR / filename
            
            # Export to Excel with formatting
            with pd.ExcelWriter(filepath, engine='openpyxl') as writer:
                df.to_excel(writer, sheet_name='Scrap_Metal_Centers', index=False)
                
                # Get workbook and worksheet
                workbook = writer.book
                worksheet = writer.sheets['Scrap_Metal_Centers']
                
                # Auto-adjust column widths
                for column in worksheet.columns:
                    max_length = 0
                    column_letter = column[0].column_letter
                    for cell in column:
                        try:
                            if len(str(cell.value)) > max_length:
                                max_length = len(str(cell.value))
                        except:
                            pass
                    adjusted_width = min(max_length + 2, 50)
                    worksheet.column_dimensions[column_letter].width = adjusted_width
            
            self.logger.info(f"📊 Comprehensive Excel file created: {filename}")
            self.logger.info(f"📁 Location: {filepath}")
            self.logger.info(f"📈 Records: {len(results):,}")
            self.logger.info(f"📋 Fields: {len(df.columns)}")
            
        except Exception as e:
            self.logger.error(f"Excel creation failed: {e}")
    
    def _should_stop(self):
        """Check if collection should stop"""
        return self.signal_handler and self.signal_handler.should_exit()
    
    def _cleanup(self):
        """Cleanup resources"""
        self.logger.info("🧹 Cleaning up resources...")
        if hasattr(self.scraper, '_cleanup_scrapers'):
            self.scraper._cleanup_scrapers()
    
    def _print_final_stats(self, results, duration):
        """Print comprehensive final statistics"""
        self.logger.info("\n" + "="*60)
        self.logger.info("📊 MASSIVE COLLECTION COMPLETED")
        self.logger.info("="*60)
        self.logger.info(f"🎯 Total Records Collected: {len(results):,}")
        self.logger.info(f"⏱️ Total Duration: {duration}")
        self.logger.info(f"📍 Phase 1 (Geography): {self.stats.get('phase1_count', 0):,}")
        self.logger.info(f"🔧 Phase 2 (Industry): {self.stats.get('phase2_count', 0):,}")
        self.logger.info(f"⚙️ Phase 3 (Materials): {self.stats.get('phase3_count', 0):,}")
        
        # Data quality stats
        verified_count = sum(1 for r in results if r.get('verification_status') == 'verified')
        partial_count = sum(1 for r in results if r.get('verification_status') == 'partial')
        
        self.logger.info(f"✅ Verified Records: {verified_count:,}")
        self.logger.info(f"⚠️ Partial Records: {partial_count:,}")
        
        # Geographic distribution
        countries = defaultdict(int)
        for result in results:
            country = result.get('country', 'Unknown')
            countries[country] += 1
        
        self.logger.info("\n🌍 Geographic Distribution:")
        for country, count in sorted(countries.items(), key=lambda x: x[1], reverse=True)[:10]:
            self.logger.info(f"   {country}: {count:,}")
        
        self.logger.info("="*60)

def main():
    """Main function for massive data collection"""
    print("🔧 MASSIVE SCRAP METAL DATA COLLECTION")
    print("🎯 Target: 20,000-100,000+ comprehensive business records")
    print("=" * 60)
    
    collector = MassiveDataCollector()
    
    try:
        # Get target count from user
        target_input = input("Enter target number of records (default 20000): ").strip()
        target_count = int(target_input) if target_input.isdigit() else 20000
        
        print(f"\n🚀 Starting collection for {target_count:,} records...")
        print("⚡ Press Ctrl+C to stop and export current results")
        print()
        
        results = collector.run_massive_collection(target_count)
        
        print(f"\n✅ Collection completed: {len(results):,} records")
        print("📊 Check the output directory for comprehensive Excel file")
        
    except KeyboardInterrupt:
        print("\n⚠️ Collection interrupted by user")
    except Exception as e:
        print(f"\n❌ Collection failed: {e}")

if __name__ == '__main__':
    main() 