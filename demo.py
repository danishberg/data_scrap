#!/usr/bin/env python3
"""
Demo script for the Scrap Metal Centers scraping application
This demonstrates the application with sample data.
"""

import os
import sys
import json
from datetime import datetime

# Add current directory to Python path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from config import Config
from data_exporter import DataExporter, create_summary_report
from models import DatabaseManager

def create_sample_data():
    """Create sample scrap metal center data for demonstration"""
    sample_data = [
        {
            'name': 'ABC Metal Recycling',
            'website': 'https://abcmetalrecycling.com',
            'full_address': '123 Industrial Way, New York, NY 10001',
            'street_address': '123 Industrial Way',
            'city': 'New York',
            'state_region': 'NY',
            'postal_code': '10001',
            'country': 'US',
            'latitude': 40.7128,
            'longitude': -74.0060,
            'phone_primary': '(212) 555-0123',
            'phone_secondary': '(212) 555-0124',
            'email_primary': 'info@abcmetalrecycling.com',
            'facebook_url': 'https://facebook.com/abcmetalrecycling',
            'working_hours': {
                'monday': '8:00 AM - 5:00 PM',
                'tuesday': '8:00 AM - 5:00 PM',
                'wednesday': '8:00 AM - 5:00 PM',
                'thursday': '8:00 AM - 5:00 PM',
                'friday': '8:00 AM - 5:00 PM',
                'saturday': '9:00 AM - 3:00 PM',
                'sunday': 'Closed'
            },
            'description': 'Full-service metal recycling facility accepting all types of ferrous and non-ferrous metals.',
            'materials': ['copper', 'aluminum', 'steel', 'iron', 'brass', 'stainless steel'],
            'source_url': 'https://example.com/demo',
            'verification_status': 'demo'
        },
        {
            'name': 'Metro Scrap Yards',
            'website': 'https://metroscrapyards.com',
            'full_address': '456 Recycling Blvd, Los Angeles, CA 90210',
            'street_address': '456 Recycling Blvd',
            'city': 'Los Angeles',
            'state_region': 'CA',
            'postal_code': '90210',
            'country': 'US',
            'latitude': 34.0522,
            'longitude': -118.2437,
            'phone_primary': '(323) 555-0456',
            'email_primary': 'contact@metroscrapyards.com',
            'website': 'https://metroscrapyards.com',
            'working_hours': {
                'monday': '7:00 AM - 6:00 PM',
                'tuesday': '7:00 AM - 6:00 PM',
                'wednesday': '7:00 AM - 6:00 PM',
                'thursday': '7:00 AM - 6:00 PM',
                'friday': '7:00 AM - 6:00 PM',
                'saturday': '8:00 AM - 4:00 PM',
                'sunday': 'Closed'
            },
            'description': 'Automotive and industrial metal recycling with competitive prices.',
            'materials': ['automotive parts', 'copper', 'aluminum', 'catalytic converters', 'radiators'],
            'source_url': 'https://example.com/demo',
            'verification_status': 'demo'
        },
        {
            'name': 'Green Metal Solutions',
            'website': 'https://greenmetalsolutions.ca',
            'full_address': '789 Recycling Ave, Toronto, ON M5H 2N2',
            'street_address': '789 Recycling Ave',
            'city': 'Toronto',
            'state_region': 'ON',
            'postal_code': 'M5H 2N2',
            'country': 'CA',
            'latitude': 43.6532,
            'longitude': -79.3832,
            'phone_primary': '(416) 555-0789',
            'email_primary': 'info@greenmetalsolutions.ca',
            'twitter_url': 'https://twitter.com/greenmetalsol',
            'working_hours': {
                'monday': '8:00 AM - 5:00 PM',
                'tuesday': '8:00 AM - 5:00 PM',
                'wednesday': '8:00 AM - 5:00 PM',
                'thursday': '8:00 AM - 5:00 PM',
                'friday': '8:00 AM - 5:00 PM',
                'saturday': 'Closed',
                'sunday': 'Closed'
            },
            'description': 'Eco-friendly metal recycling with focus on electronic waste and precious metals.',
            'materials': ['electronics', 'e-waste', 'copper', 'aluminum', 'precious metals'],
            'source_url': 'https://example.com/demo',
            'verification_status': 'demo'
        },
        {
            'name': 'Australian Metal Exchange',
            'website': 'https://ausmetalexchange.com.au',
            'full_address': '321 Steel Street, Sydney, NSW 2000',
            'street_address': '321 Steel Street',
            'city': 'Sydney',
            'state_region': 'NSW',
            'postal_code': '2000',
            'country': 'AU',
            'latitude': -33.8688,
            'longitude': 151.2093,
            'phone_primary': '+61 2 9555 0321',
            'email_primary': 'contact@ausmetalexchange.com.au',
            'linkedin_url': 'https://linkedin.com/company/ausmetalexchange',
            'working_hours': {
                'monday': '7:00 AM - 4:00 PM',
                'tuesday': '7:00 AM - 4:00 PM',
                'wednesday': '7:00 AM - 4:00 PM',
                'thursday': '7:00 AM - 4:00 PM',
                'friday': '7:00 AM - 4:00 PM',
                'saturday': 'Closed',
                'sunday': 'Closed'
            },
            'description': 'Large-scale metal recycling facility serving the Sydney metropolitan area.',
            'materials': ['steel', 'iron', 'copper', 'aluminum', 'zinc', 'lead'],
            'source_url': 'https://example.com/demo',
            'verification_status': 'demo'
        },
        {
            'name': 'London Scrap Metal Co',
            'website': 'https://londonscrapmetal.co.uk',
            'full_address': '654 Industrial Park, London E1 6AN',
            'street_address': '654 Industrial Park',
            'city': 'London',
            'state_region': 'England',
            'postal_code': 'E1 6AN',
            'country': 'GB',
            'latitude': 51.5074,
            'longitude': -0.1278,
            'phone_primary': '+44 20 7555 0654',
            'email_primary': 'enquiries@londonscrapmetal.co.uk',
            'whatsapp_number': '+44 7700 900654',
            'working_hours': {
                'monday': '8:00 AM - 5:00 PM',
                'tuesday': '8:00 AM - 5:00 PM',
                'wednesday': '8:00 AM - 5:00 PM',
                'thursday': '8:00 AM - 5:00 PM',
                'friday': '8:00 AM - 5:00 PM',
                'saturday': '9:00 AM - 1:00 PM',
                'sunday': 'Closed'
            },
            'description': 'Family-owned metal recycling business serving London and surrounding areas.',
            'materials': ['copper', 'brass', 'aluminum', 'lead', 'stainless steel', 'cast iron'],
            'source_url': 'https://example.com/demo',
            'verification_status': 'demo'
        }
    ]
    
    return sample_data

def demonstrate_export_functionality():
    """Demonstrate the data export functionality"""
    print("Scrap Metal Centers Application - Demo")
    print("=" * 50)
    
    # Override config for demo
    Config.OUTPUT_DIR = "demo_output"
    
    # Create sample data
    sample_data = create_sample_data()
    print(f"Created {len(sample_data)} sample scrap metal centers")
    
    # Initialize data exporter
    exporter = DataExporter()
    
    # Export data in all formats
    print("\nExporting data in multiple formats...")
    exporter.export_data(sample_data, "demo_scrap_centers")
    
    # Create summary report
    create_summary_report(sample_data, Config.OUTPUT_DIR)
    
    print(f"\nDemo completed! Check the '{Config.OUTPUT_DIR}' directory for:")
    print("- CSV file with tabular data")
    print("- Excel file with formatted spreadsheet")
    print("- JSON file with complete structured data")
    print("- SQLite database with normalized data")
    print("- Summary report with statistics")
    
    return sample_data

def demonstrate_database_functionality():
    """Demonstrate database operations"""
    print("\nDemonstrating database functionality...")
    
    # Initialize database
    db_manager = DatabaseManager(Config.DATABASE_URL)
    
    try:
        # Add sample data to database
        sample_data = create_sample_data()
        
        for center_data in sample_data:
            # Add center to database
            center = db_manager.add_scrap_center(center_data)
            
            if center and center_data.get('materials'):
                # Add materials
                for material_name in center_data['materials']:
                    material = db_manager.get_or_create_material(material_name)
                    if material not in center.materials:
                        center.materials.append(material)
                
                # Commit the relationships
                db_manager.session.commit()
        
        # Query data from database
        all_centers = db_manager.get_all_centers()
        print(f"Successfully stored {len(all_centers)} centers in database")
        
        # Show sample center data
        if all_centers:
            sample_center = all_centers[0]
            print(f"\nSample center from database:")
            print(f"Name: {sample_center.name}")
            print(f"Location: {sample_center.city}, {sample_center.country}")
            print(f"Materials: {[m.name for m in sample_center.materials]}")
        
    finally:
        db_manager.close()

if __name__ == "__main__":
    print("Starting Scrap Metal Centers Application Demo...")
    
    # Demonstrate export functionality
    sample_data = demonstrate_export_functionality()
    
    # Demonstrate database functionality
    demonstrate_database_functionality()
    
    print("\n" + "=" * 50)
    print("✅ Demo completed successfully!")
    print("\nThe application is ready for production use.")
    print("\nTo scrape real data, run:")
    print("python main.py")
    print("\nTo see all options:")
    print("python main.py --help") 