#!/usr/bin/env python3
"""
Configuration settings for Scrap Metal Centers Data Collection Application
Enhanced for comprehensive data collection (20,000+ entries)
"""

import os
from pathlib import Path
from dotenv import load_dotenv

load_dotenv()

class Config:
    # Base directory
    BASE_DIR = Path(__file__).parent.absolute()
    
    # Database configuration
    DATABASE_URL = os.getenv('DATABASE_URL', f'sqlite:///{BASE_DIR}/scrap_metal_centers.db')
    
    # Scraping behavior - optimized for large-scale collection
    REQUEST_DELAY = float(os.getenv('REQUEST_DELAY', '1.0'))  # Reduced for faster collection
    MAX_RETRIES = int(os.getenv('MAX_RETRIES', '5'))  # Increased for reliability
    TIMEOUT = int(os.getenv('TIMEOUT', '45'))  # Increased timeout
    BATCH_SIZE = int(os.getenv('BATCH_SIZE', '100'))  # Process in batches
    MAX_WORKERS = int(os.getenv('MAX_WORKERS', '8'))  # More parallel workers
    
    # Browser settings
    HEADLESS_BROWSER = os.getenv('HEADLESS_BROWSER', 'True').lower() == 'true'
    BROWSER_TYPE = os.getenv('BROWSER_TYPE', 'chrome')  # chrome, firefox, edge
    
    # Output configuration
    OUTPUT_FORMAT = os.getenv('OUTPUT_FORMAT', 'all')  # csv, excel, json, database, all
    OUTPUT_DIR = Path(os.getenv('OUTPUT_DIR', BASE_DIR / 'output'))
    
    # Ensure output directory exists
    OUTPUT_DIR.mkdir(exist_ok=True)
    
    # Enhanced target countries for maximum coverage
    TARGET_COUNTRIES = [
        'United States', 'USA', 'US',
        'Canada', 'CA', 'CAN',
        'United Kingdom', 'UK', 'GB', 'England', 'Scotland', 'Wales',
        'Australia', 'AU', 'AUS',
        'New Zealand', 'NZ', 'NZL',
        'Ireland', 'IE', 'IRL',
        'South Africa', 'ZA', 'RSA'
    ]
    
    # Comprehensive search terms for maximum data collection
    SEARCH_TERMS = [
        # Primary terms
        'scrap metal recycling centers',
        'metal recycling facilities',
        'scrap yards',
        'scrap metal dealers',
        'metal waste recycling',
        'scrap metal buyers',
        'metal recycling companies',
        'junk yards metal',
        'auto recycling centers',
        'car recycling centers',
        'vehicle recycling',
        
        # Specific metal types
        'copper recycling centers',
        'aluminum recycling',
        'steel recycling facilities',
        'iron recycling',
        'brass recycling',
        'bronze recycling',
        'lead recycling',
        'zinc recycling',
        'nickel recycling',
        'stainless steel recycling',
        
        # Russian terms (пункт приема)
        'пункт приема металлолома',
        'прием металлолома',
        'утилизация металла',
        'скупка металла',
        'металлолом',
        
        # Equipment and specialty
        'catalytic converter recycling',
        'radiator recycling',
        'wire recycling',
        'appliance recycling metal',
        'construction metal recycling',
        'industrial metal recycling',
        'commercial scrap metal',
        'residential metal pickup',
        
        # Alternative terms
        'metal salvage yards',
        'metal scrap dealers',
        'metal waste management',
        'metal recovery centers',
        'secondary metal processing',
        'metal commodity trading',
        'scrap metal processing',
        'metal recycling depot',
        'metal collection centers',
        'metal disposal services'
    ]
    
    # Comprehensive major cities and regions for maximum coverage
    DEFAULT_LOCATIONS = [
        # United States - Major cities and regions
        'New York, NY', 'Los Angeles, CA', 'Chicago, IL', 'Houston, TX',
        'Phoenix, AZ', 'Philadelphia, PA', 'San Antonio, TX', 'San Diego, CA',
        'Dallas, TX', 'San Jose, CA', 'Austin, TX', 'Jacksonville, FL',
        'Fort Worth, TX', 'Columbus, OH', 'Charlotte, NC', 'San Francisco, CA',
        'Indianapolis, IN', 'Seattle, WA', 'Denver, CO', 'Washington, DC',
        'Boston, MA', 'El Paso, TX', 'Detroit, MI', 'Nashville, TN',
        'Portland, OR', 'Memphis, TN', 'Oklahoma City, OK', 'Las Vegas, NV',
        'Louisville, KY', 'Baltimore, MD', 'Milwaukee, WI', 'Albuquerque, NM',
        'Tucson, AZ', 'Fresno, CA', 'Sacramento, CA', 'Kansas City, MO',
        'Mesa, AZ', 'Atlanta, GA', 'Omaha, NE', 'Colorado Springs, CO',
        'Raleigh, NC', 'Miami, FL', 'Long Beach, CA', 'Virginia Beach, VA',
        'Oakland, CA', 'Minneapolis, MN', 'Tampa, FL', 'Tulsa, OK',
        'Arlington, TX', 'New Orleans, LA', 'Wichita, KS', 'Cleveland, OH',
        
        # US States for broader coverage
        'Alabama', 'Alaska', 'Arizona', 'Arkansas', 'California', 'Colorado',
        'Connecticut', 'Delaware', 'Florida', 'Georgia', 'Hawaii', 'Idaho',
        'Illinois', 'Indiana', 'Iowa', 'Kansas', 'Kentucky', 'Louisiana',
        'Maine', 'Maryland', 'Massachusetts', 'Michigan', 'Minnesota',
        'Mississippi', 'Missouri', 'Montana', 'Nebraska', 'Nevada',
        'New Hampshire', 'New Jersey', 'New Mexico', 'New York',
        'North Carolina', 'North Dakota', 'Ohio', 'Oklahoma', 'Oregon',
        'Pennsylvania', 'Rhode Island', 'South Carolina', 'South Dakota',
        'Tennessee', 'Texas', 'Utah', 'Vermont', 'Virginia', 'Washington',
        'West Virginia', 'Wisconsin', 'Wyoming',
        
        # Canada - Major cities and provinces
        'Toronto, ON', 'Montreal, QC', 'Calgary, AB', 'Ottawa, ON',
        'Edmonton, AB', 'Mississauga, ON', 'Winnipeg, MB', 'Vancouver, BC',
        'Brampton, ON', 'Hamilton, ON', 'Quebec City, QC', 'Surrey, BC',
        'Laval, QC', 'Halifax, NS', 'London, ON', 'Markham, ON',
        'Vaughan, ON', 'Gatineau, QC', 'Saskatoon, SK', 'Longueuil, QC',
        'Kitchener, ON', 'Burnaby, BC', 'Windsor, ON', 'Regina, SK',
        'Richmond, BC', 'Richmond Hill, ON', 'Oakville, ON', 'Burlington, ON',
        'Sherbrooke, QC', 'Oshawa, ON', 'Saguenay, QC', 'Lévis, QC',
        'Barrie, ON', 'Abbotsford, BC', 'Coquitlam, BC', 'Trois-Rivières, QC',
        
        # Canada Provinces
        'Ontario', 'Quebec', 'British Columbia', 'Alberta', 'Manitoba',
        'Saskatchewan', 'Nova Scotia', 'New Brunswick', 'Newfoundland and Labrador',
        'Prince Edward Island', 'Northwest Territories', 'Yukon', 'Nunavut',
        
        # United Kingdom
        'London', 'Birmingham', 'Manchester', 'Glasgow', 'Liverpool',
        'Leeds', 'Sheffield', 'Edinburgh', 'Bristol', 'Cardiff',
        'Leicester', 'Coventry', 'Bradford', 'Belfast', 'Nottingham',
        'Kingston upon Hull', 'Newcastle upon Tyne', 'Stoke-on-Trent',
        'Southampton', 'Derby', 'Portsmouth', 'Brighton', 'Plymouth',
        'Northampton', 'Reading', 'Luton', 'Wolverhampton', 'Bolton',
        'Aberdeen', 'Bournemouth', 'Norwich', 'Swindon', 'Swansea',
        
        # Australia
        'Sydney, NSW', 'Melbourne, VIC', 'Brisbane, QLD', 'Perth, WA',
        'Adelaide, SA', 'Gold Coast, QLD', 'Newcastle, NSW', 'Canberra, ACT',
        'Sunshine Coast, QLD', 'Wollongong, NSW', 'Hobart, TAS', 'Geelong, VIC',
        'Townsville, QLD', 'Cairns, QLD', 'Darwin, NT', 'Toowoomba, QLD',
        'Ballarat, VIC', 'Bendigo, VIC', 'Albury, NSW', 'Launceston, TAS',
        
        # New Zealand
        'Auckland', 'Wellington', 'Christchurch', 'Hamilton', 'Tauranga',
        'Napier-Hastings', 'Dunedin', 'Palmerston North', 'Nelson', 'Rotorua',
        'New Plymouth', 'Whangarei', 'Invercargill', 'Whanganui', 'Gisborne',
        
        # Ireland
        'Dublin', 'Cork', 'Limerick', 'Galway', 'Waterford', 'Drogheda',
        'Dundalk', 'Swords', 'Bray', 'Navan', 'Ennis', 'Tralee',
        'Carlow', 'Newbridge', 'Naas', 'Athlone', 'Portlaoise', 'Mullingar',
        
        # South Africa
        'Johannesburg', 'Cape Town', 'Durban', 'Pretoria', 'Port Elizabeth',
        'Bloemfontein', 'East London', 'Pietermaritzburg', 'Kimberley',
        'Polokwane', 'Nelspruit', 'George', 'Rustenburg', 'Witbank'
    ]
    
    # Comprehensive material types for enhanced detection
    MATERIAL_TYPES = [
        # Base metals
        'copper', 'aluminum', 'steel', 'iron', 'brass', 'bronze',
        'lead', 'zinc', 'nickel', 'tin', 'titanium', 'magnesium',
        
        # Stainless steel grades
        'stainless steel', '304 stainless', '316 stainless', '430 stainless',
        
        # Specialty metals
        'catalytic converters', 'radiators', 'car batteries',
        'electric motors', 'transformers', 'wire harnesses',
        'appliances', 'air conditioners', 'computers', 'electronics',
        
        # Industrial materials
        'structural steel', 'rebar', 'sheet metal', 'pipe',
        'tubing', 'cable', 'wire', 'insulated wire', 'romex',
        
        # Automotive
        'auto parts', 'car bodies', 'engines', 'transmissions',
        'wheels', 'rims', 'exhaust systems', 'fuel tanks',
        
        # Construction
        'demolition steel', 'construction debris', 'beams',
        'plates', 'angle iron', 'channel iron', 'I-beams',
        
        # Precious metals
        'gold', 'silver', 'platinum', 'palladium', 'rhodium',
        
        # Electronic components
        'circuit boards', 'computer towers', 'servers',
        'cell phones', 'tablets', 'printers', 'monitors'
    ]
    
    # Enhanced data fields to collect (15+ comprehensive fields)
    DATA_FIELDS = {
        'basic_info': [
            'name', 'description', 'category', 'business_type',
            'website', 'email_primary', 'email_secondary',
            'phone_primary', 'phone_secondary', 'fax'
        ],
        'location': [
            'address_full', 'street_address', 'city', 'state_region',
            'postal_code', 'country', 'latitude', 'longitude',
            'service_area', 'delivery_pickup'
        ],
        'business_details': [
            'working_hours', 'established_year', 'employee_count',
            'certifications', 'licenses', 'insurance_info',
            'payment_methods', 'minimum_quantity', 'container_provided'
        ],
        'materials_pricing': [
            'materials_accepted', 'materials_not_accepted',
            'current_prices', 'price_per_pound', 'price_per_ton',
            'price_last_updated', 'bulk_discounts', 'price_quotes'
        ],
        'services': [
            'pickup_service', 'demolition_service', 'container_rental',
            'sorting_service', 'weighing_service', 'cash_payment',
            'check_payment', 'industrial_contracts', 'residential_service'
        ],
        'social_media': [
            'facebook_url', 'twitter_url', 'instagram_url', 'linkedin_url',
            'youtube_url', 'whatsapp_number', 'telegram_contact',
            'google_business_profile'
        ],
        'quality_metrics': [
            'reviews_count', 'average_rating', 'bbb_rating',
            'google_rating', 'yelp_rating', 'verification_status',
            'data_completeness_score', 'last_verified'
        ]
    }
    
    # Geocoding configuration
    GOOGLE_MAPS_API_KEY = os.getenv('GOOGLE_MAPS_API_KEY', '')
    ENABLE_GEOCODING = True
    
    # Rate limiting for large-scale scraping
    RATE_LIMIT = {
        'requests_per_minute': 30,
        'requests_per_hour': 1000,
        'requests_per_day': 10000
    }
    
    # Output configuration for large datasets
    EXCEL_MAX_ROWS = 1000000  # Excel can handle up to 1M rows
    CSV_CHUNK_SIZE = 10000   # Process CSV in chunks
    JSON_PRETTY_PRINT = False  # Compact JSON for large files
    
    # Logging configuration
    LOG_LEVEL = os.getenv('LOG_LEVEL', 'INFO')
    LOG_FILE = BASE_DIR / 'scraping.log'
    
    # Performance settings for large-scale collection
    ENABLE_CACHING = True
    CACHE_EXPIRY_HOURS = 24
    DATABASE_BATCH_SIZE = 1000
    MEMORY_LIMIT_MB = 2048

    # User agents rotation
    USE_ROTATING_USER_AGENTS = os.getenv('USE_ROTATING_USER_AGENTS', 'True').lower() == 'true'
    
    # Google Maps API (optional, for geocoding)
    ENABLE_GEOCODING = True
    
    # Output configuration
    OUTPUT_FORMAT = os.getenv('OUTPUT_FORMAT', 'both')  # csv, excel, database, both
    OUTPUT_DIR = os.getenv('OUTPUT_DIR', 'output')
    
    # Countries to scrape (ISO country codes)
    TARGET_COUNTRIES = [
        'US',  # United States
        'CA',  # Canada
        'GB',  # United Kingdom
        'AU',  # Australia
        'NZ',  # New Zealand
        'IE',  # Ireland
        'ZA',  # South Africa
    ]
    
    # Search terms in different languages
    SEARCH_TERMS = [
        'scrap metal recycling centers',
        'metal recycling facilities',
        'scrap yards',
        'scrap metal dealers',
        'metal waste recycling',
        'scrap metal buyers',
        'metal recycling companies',
        'junk yards metal',
        'auto recycling centers',
        'copper recycling centers',
        'aluminum recycling',
        'steel recycling facilities'
    ]
    
    # Material types to look for
    MATERIAL_TYPES = [
        'copper', 'aluminum', 'steel', 'iron', 'brass', 'bronze',
        'stainless steel', 'lead', 'zinc', 'nickel', 'tin',
        'automotive parts', 'appliances', 'electronics', 'wire',
        'cable', 'radiators', 'catalytic converters', 'batteries'
    ] 