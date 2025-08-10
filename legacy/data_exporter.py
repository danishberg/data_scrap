import os
import json
import pandas as pd
import csv
from datetime import datetime
from config import Config
from models import DatabaseManager

class DataExporter:
    def __init__(self, output_dir=None):
        self.output_dir = output_dir or Config.OUTPUT_DIR
        self.ensure_output_directory()
    
    def ensure_output_directory(self):
        """Create output directory if it doesn't exist"""
        if not os.path.exists(self.output_dir):
            os.makedirs(self.output_dir)
    
    def export_data(self, data, filename_prefix="scrap_centers"):
        """Export data in the configured format(s)"""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        if Config.OUTPUT_FORMAT in ['csv', 'both']:
            self.export_to_csv(data, f"{filename_prefix}_{timestamp}.csv")
        
        if Config.OUTPUT_FORMAT in ['excel', 'both']:
            self.export_to_excel(data, f"{filename_prefix}_{timestamp}.xlsx")
        
        if Config.OUTPUT_FORMAT in ['database', 'both']:
            self.export_to_database(data)
        
        # Always export as JSON for backup
        self.export_to_json(data, f"{filename_prefix}_{timestamp}.json")
    
    def export_to_csv(self, data, filename):
        """Export data to CSV file"""
        filepath = os.path.join(self.output_dir, filename)
        
        if not data:
            print(f"No data to export to {filename}")
            return
        
        # Flatten nested data
        flattened_data = self._flatten_data(data)
        
        # Create DataFrame
        df = pd.DataFrame(flattened_data)
        
        # Export to CSV
        df.to_csv(filepath, index=False, encoding='utf-8')
        print(f"Data exported to CSV: {filepath}")
    
    def export_to_excel(self, data, filename):
        """Export data to Excel file"""
        filepath = os.path.join(self.output_dir, filename)
        
        if not data:
            print(f"No data to export to {filename}")
            return
        
        # Flatten nested data
        flattened_data = self._flatten_data(data)
        
        # Create DataFrame
        df = pd.DataFrame(flattened_data)
        
        # Export to Excel with formatting
        with pd.ExcelWriter(filepath, engine='xlsxwriter') as writer:
            df.to_excel(writer, sheet_name='Scrap Metal Centers', index=False)
            
            # Get workbook and worksheet
            workbook = writer.book
            worksheet = writer.sheets['Scrap Metal Centers']
            
            # Add formatting
            header_format = workbook.add_format({
                'bold': True,
                'text_wrap': True,
                'valign': 'top',
                'fg_color': '#D7E4BC',
                'border': 1
            })
            
            # Write headers with formatting
            for col_num, value in enumerate(df.columns.values):
                worksheet.write(0, col_num, value, header_format)
                worksheet.set_column(col_num, col_num, 20)  # Set column width
        
        print(f"Data exported to Excel: {filepath}")
    
    def export_to_json(self, data, filename):
        """Export data to JSON file"""
        filepath = os.path.join(self.output_dir, filename)
        
        with open(filepath, 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=2, ensure_ascii=False, default=str)
        
        print(f"Data exported to JSON: {filepath}")
    
    def export_to_database(self, data):
        """Export data to database"""
        db_manager = DatabaseManager(Config.DATABASE_URL)
        
        try:
            for center_data in data:
                # Prepare center data for database
                db_center_data = self._prepare_center_for_db(center_data)
                
                # Add center to database
                center = db_manager.add_scrap_center(db_center_data)
                
                if center and center_data.get('materials'):
                    # Add materials
                    for material_name in center_data['materials']:
                        material = db_manager.get_or_create_material(material_name)
                        if material not in center.materials:
                            center.materials.append(material)
                
                # Add prices if available
                if center and center_data.get('prices'):
                    for price_data in center_data['prices']:
                        db_manager.add_material_price(
                            center.id,
                            price_data.get('material_name'),
                            price_data
                        )
            
            print(f"Data exported to database: {Config.DATABASE_URL}")
            
        finally:
            db_manager.close()
    
    def _flatten_data(self, data):
        """Flatten nested data for CSV/Excel export"""
        flattened = []
        
        for item in data:
            flat_item = {}
            
            for key, value in item.items():
                if isinstance(value, dict):
                    # Flatten dictionary fields (like working_hours)
                    for sub_key, sub_value in value.items():
                        flat_item[f"{key}_{sub_key}"] = sub_value
                elif isinstance(value, list):
                    # Convert lists to comma-separated strings
                    flat_item[key] = ', '.join(str(v) for v in value)
                else:
                    flat_item[key] = value
            
            flattened.append(flat_item)
        
        return flattened
    
    def _prepare_center_for_db(self, center_data):
        """Prepare center data for database insertion"""
        db_data = {}
        
        # Map fields to database columns
        field_mapping = {
            'name': 'name',
            'website': 'website',
            'full_address': 'full_address',
            'street_address': 'street_address',
            'city': 'city',
            'state': 'state_region',
            'state_region': 'state_region',
            'postal_code': 'postal_code',
            'country': 'country',
            'latitude': 'latitude',
            'longitude': 'longitude',
            'phone_primary': 'phone_primary',
            'phone_secondary': 'phone_secondary',
            'email_primary': 'email_primary',
            'email_secondary': 'email_secondary',
            'facebook_url': 'facebook_url',
            'twitter_url': 'twitter_url',
            'instagram_url': 'instagram_url',
            'linkedin_url': 'linkedin_url',
            'whatsapp_number': 'whatsapp_number',
            'telegram_contact': 'telegram_contact',
            'description': 'description',
            'source_url': 'source_url'
        }
        
        for source_field, db_field in field_mapping.items():
            if source_field in center_data:
                db_data[db_field] = center_data[source_field]
        
        # Handle JSON fields
        if 'working_hours' in center_data:
            db_data['working_hours'] = json.dumps(center_data['working_hours'])
        
        if 'services_offered' in center_data:
            db_data['services_offered'] = json.dumps(center_data['services_offered'])
        
        return db_data

def create_summary_report(data, output_dir=None):
    """Create a summary report of the scraped data"""
    if not data:
        return
    
    output_dir = output_dir or Config.OUTPUT_DIR
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filepath = os.path.join(output_dir, f"summary_report_{timestamp}.txt")
    
    # Calculate statistics
    total_centers = len(data)
    centers_with_phone = sum(1 for item in data if item.get('phone_primary'))
    centers_with_email = sum(1 for item in data if item.get('email_primary'))
    centers_with_website = sum(1 for item in data if item.get('website'))
    centers_with_address = sum(1 for item in data if item.get('full_address'))
    centers_with_coordinates = sum(1 for item in data if item.get('latitude') and item.get('longitude'))
    
    # Count by country
    countries = {}
    for item in data:
        country = item.get('country', 'Unknown')
        countries[country] = countries.get(country, 0) + 1
    
    # Count materials
    all_materials = []
    for item in data:
        if item.get('materials'):
            all_materials.extend(item['materials'])
    
    material_counts = {}
    for material in all_materials:
        material_counts[material] = material_counts.get(material, 0) + 1
    
    # Write report
    with open(filepath, 'w', encoding='utf-8') as f:
        f.write("SCRAP METAL CENTERS DATA COLLECTION SUMMARY\n")
        f.write("=" * 50 + "\n\n")
        f.write(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n")
        
        f.write("OVERALL STATISTICS:\n")
        f.write(f"Total Centers Found: {total_centers}\n")
        f.write(f"Centers with Phone: {centers_with_phone} ({centers_with_phone/total_centers*100:.1f}%)\n")
        f.write(f"Centers with Email: {centers_with_email} ({centers_with_email/total_centers*100:.1f}%)\n")
        f.write(f"Centers with Website: {centers_with_website} ({centers_with_website/total_centers*100:.1f}%)\n")
        f.write(f"Centers with Address: {centers_with_address} ({centers_with_address/total_centers*100:.1f}%)\n")
        f.write(f"Centers with Coordinates: {centers_with_coordinates} ({centers_with_coordinates/total_centers*100:.1f}%)\n\n")
        
        f.write("DISTRIBUTION BY COUNTRY:\n")
        for country, count in sorted(countries.items(), key=lambda x: x[1], reverse=True):
            f.write(f"{country}: {count} centers\n")
        f.write("\n")
        
        f.write("TOP MATERIALS HANDLED:\n")
        for material, count in sorted(material_counts.items(), key=lambda x: x[1], reverse=True)[:20]:
            f.write(f"{material}: {count} centers\n")
    
    print(f"Summary report created: {filepath}") 