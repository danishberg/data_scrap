#!/usr/bin/env python3
"""
Scrap Metal Centers Data Collection Application

This application scrapes data about scrap metal and recycling centers
from various sources across English-speaking countries.
"""

import os
import sys
import argparse
import logging
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed

# Add current directory to Python path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from config import Config
from data_exporter import DataExporter, create_summary_report
from scrapers.google_maps_scraper import GoogleMapsScraper, GoogleSearchScraper
from scrapers.yellowpages_scraper import YellowPagesScraper, YellowPagesCanadaScraper
from scrapers.yelp_scraper import YelpScraper
from signal_handler import setup_signal_handling, handle_keyboard_interrupt

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('scraping.log'),
        logging.StreamHandler()
    ]
)

class ScrapMetalScraper:
    def __init__(self):
        self.logger = logging.getLogger(__name__)
        self.scrapers = {
            'google_maps': GoogleMapsScraper,
            'google_search': GoogleSearchScraper,
            'yellowpages': YellowPagesScraper,
            'yellowpages_ca': YellowPagesCanadaScraper,
            'yelp': YelpScraper
        }
        self.data_exporter = DataExporter()
        self.all_results = []
        self.signal_handler = None
        self.active_scrapers = []
    
    def run_scraping(self, sources=None, search_terms=None, locations=None, limit_per_source=100):
        """Run the complete scraping process with signal handling"""
        self.logger.info("Starting scrap metal centers data collection")
        
        # Setup signal handling
        self.signal_handler = setup_signal_handling(
            logger=self.logger,
            cleanup_functions=[self._cleanup_scrapers]
        )
        
        try:
            # Use default values if not provided
            sources = sources or list(self.scrapers.keys())
            search_terms = search_terms or Config.SEARCH_TERMS
            locations = locations or self._get_default_locations()
            
            self.logger.info(f"Sources: {sources}")
            self.logger.info(f"Search terms: {search_terms}")
            self.logger.info(f"Locations: {locations}")
            
            # Run scraping for each combination
            scraping_tasks = []
            for source in sources:
                for search_term in search_terms:
                    for location in locations:
                        task = {
                            'source': source,
                            'search_term': search_term,
                            'location': location,
                            'limit': limit_per_source
                        }
                        scraping_tasks.append(task)
            
            self.logger.info(f"Total scraping tasks: {len(scraping_tasks)}")
            
            # Execute scraping tasks with interruption checking
            with ThreadPoolExecutor(max_workers=3) as executor:
                future_to_task = {}
                
                for task in scraping_tasks:
                    if self.signal_handler and self.signal_handler.should_exit():
                        self.logger.info("Stopping due to interrupt signal")
                        break
                    
                    future = executor.submit(self._execute_scraping_task, task)
                    future_to_task[future] = task
                
                for future in as_completed(future_to_task):
                    if self.signal_handler and self.signal_handler.should_exit():
                        self.logger.info("Cancelling remaining tasks due to interrupt")
                        for f in future_to_task:
                            f.cancel()
                        break
                    
                    task = future_to_task[future]
                    try:
                        results = future.result()
                        if results:
                            self.all_results.extend(results)
                            self.logger.info(f"Completed {task['source']} - {task['search_term']} - {task['location']}: {len(results)} results")
                    except Exception as e:
                        self.logger.error(f"Error in task {task}: {e}")
            
            # Check if we should continue with export
            if self.signal_handler and self.signal_handler.should_exit():
                self.logger.info("Export cancelled due to interrupt")
                return []
            
            # Remove duplicates
            unique_results = self._remove_duplicates(self.all_results)
            self.logger.info(f"Total unique results: {len(unique_results)}")
            
            # Export data
            if unique_results:
                self.data_exporter.export_data(unique_results)
                create_summary_report(unique_results)
            else:
                self.logger.warning("No data collected!")
            
            return unique_results
            
        except KeyboardInterrupt:
            self.logger.info("Scraping interrupted by user")
            return []
        finally:
            self._cleanup_scrapers()
    
    def _execute_scraping_task(self, task):
        """Execute a single scraping task with interrupt checking"""
        try:
            if self.signal_handler and self.signal_handler.should_exit():
                return []
            
            scraper_class = self.scrapers[task['source']]
            scraper = scraper_class()
            self.active_scrapers.append(scraper)
            
            results = scraper.scrape(
                search_term=task['search_term'],
                location=task['location'],
                limit=task['limit']
            )
            
            return results
            
        except KeyboardInterrupt:
            self.logger.info(f"Task {task['source']} interrupted")
            return []
        except Exception as e:
            self.logger.error(f"Error executing scraping task {task}: {e}")
            return []
        finally:
            if scraper in self.active_scrapers:
                self.active_scrapers.remove(scraper)
    
    def _get_default_locations(self):
        """Get default locations for each target country"""
        locations = {
            'US': [
                'New York, NY', 'Los Angeles, CA', 'Chicago, IL', 'Houston, TX',
                'Phoenix, AZ', 'Philadelphia, PA', 'San Antonio, TX', 'San Diego, CA',
                'Dallas, TX', 'San Jose, CA', 'Austin, TX', 'Jacksonville, FL',
                'Fort Worth, TX', 'Columbus, OH', 'Charlotte, NC', 'San Francisco, CA',
                'Indianapolis, IN', 'Seattle, WA', 'Denver, CO', 'Washington, DC'
            ],
            'CA': [
                'Toronto, ON', 'Montreal, QC', 'Vancouver, BC', 'Calgary, AB',
                'Ottawa, ON', 'Edmonton, AB', 'Mississauga, ON', 'Winnipeg, MB',
                'Quebec City, QC', 'Hamilton, ON', 'Brampton, ON', 'Surrey, BC'
            ],
            'GB': [
                'London', 'Birmingham', 'Manchester', 'Glasgow', 'Liverpool',
                'Bristol', 'Sheffield', 'Leeds', 'Edinburgh', 'Leicester'
            ],
            'AU': [
                'Sydney, NSW', 'Melbourne, VIC', 'Brisbane, QLD', 'Perth, WA',
                'Adelaide, SA', 'Gold Coast, QLD', 'Newcastle, NSW', 'Canberra, ACT'
            ],
            'NZ': [
                'Auckland', 'Wellington', 'Christchurch', 'Hamilton', 'Tauranga'
            ],
            'IE': [
                'Dublin', 'Cork', 'Limerick', 'Galway', 'Waterford'
            ],
            'ZA': [
                'Johannesburg', 'Cape Town', 'Durban', 'Pretoria', 'Port Elizabeth'
            ]
        }
        
        # Flatten all locations
        all_locations = []
        for country_locations in locations.values():
            all_locations.extend(country_locations)
        
        return all_locations
    
    def _remove_duplicates(self, results):
        """Remove duplicate entries based on name and address"""
        seen = set()
        unique_results = []
        
        for result in results:
            # Create a key for deduplication
            name = result.get('name', '').lower().strip()
            address = result.get('full_address', '').lower().strip()
            phone = result.get('phone_primary', '').strip()
            
            # Create composite key
            key = f"{name}|{address}|{phone}"
            
            if key not in seen and name:  # Only add if name exists
                seen.add(key)
                unique_results.append(result)
        
        return unique_results
    
    def _cleanup_scrapers(self):
        """Cleanup active scrapers"""
        self.logger.info("Cleaning up active scrapers...")
        for scraper in self.active_scrapers:
            try:
                scraper.cleanup()
            except Exception as e:
                self.logger.error(f"Error cleaning up scraper: {e}")
        self.active_scrapers.clear()

@handle_keyboard_interrupt
def main():
    """Main entry point for the application"""
    parser = argparse.ArgumentParser(description='Scrap Metal Centers Data Collection')
    
    parser.add_argument('--sources', nargs='+', 
                       choices=['google_maps', 'google_search', 'yellowpages', 'yellowpages_ca', 'yelp'],
                       help='Scraping sources to use')
    
    parser.add_argument('--search-terms', nargs='+', 
                       help='Custom search terms')
    
    parser.add_argument('--locations', nargs='+',
                       help='Custom locations to search')
    
    parser.add_argument('--limit', type=int, default=100,
                       help='Limit per source (default: 100)')
    
    parser.add_argument('--output-dir', 
                       help='Output directory for results')
    
    parser.add_argument('--config-file',
                       help='Path to configuration file')
    
    args = parser.parse_args()
    
    # Update configuration if output directory specified
    if args.output_dir:
        Config.OUTPUT_DIR = args.output_dir
    
    # Create scraper instance
    scraper = ScrapMetalScraper()
    
    # Run scraping
    try:
        results = scraper.run_scraping(
            sources=args.sources,
            search_terms=args.search_terms,
            locations=args.locations,
            limit_per_source=args.limit
        )
        
        print(f"\n{'='*50}")
        print(f"SCRAPING COMPLETED SUCCESSFULLY!")
        print(f"Total unique results: {len(results)}")
        print(f"Output directory: {Config.OUTPUT_DIR}")
        print(f"{'='*50}")
        
    except KeyboardInterrupt:
        print("\nScraping interrupted by user")
        sys.exit(1)
    except Exception as e:
        print(f"Error during scraping: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main() 