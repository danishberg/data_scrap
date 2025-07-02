import re
import json
import time
import random
from urllib.parse import urlparse, urljoin
import phonenumbers
from email_validator import validate_email, EmailNotValidError
from geopy.geocoders import Nominatim
from geopy.exc import GeocoderTimedOut, GeocoderServiceError
import googlemaps
from fake_useragent import UserAgent
from config import Config

class DataProcessor:
    def __init__(self):
        self.ua = UserAgent()
        if Config.GOOGLE_MAPS_API_KEY:
            self.gmaps = googlemaps.Client(key=Config.GOOGLE_MAPS_API_KEY)
        else:
            self.gmaps = None
        self.geocoder = Nominatim(user_agent="scrap_metal_scraper")
    
    def get_random_user_agent(self):
        """Get a random user agent"""
        if Config.USE_ROTATING_USER_AGENTS:
            return self.ua.random
        return "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
    
    def clean_text(self, text):
        """Clean and normalize text"""
        if not text:
            return ""
        # Remove extra whitespace and newlines
        text = ' '.join(text.split())
        # Remove special characters but keep important punctuation
        text = re.sub(r'[^\w\s\-.,()&@#]', '', text)
        return text.strip()
    
    def extract_phone_numbers(self, text):
        """Extract phone numbers from text"""
        if not text:
            return []
        
        phones = []
        # Common phone number patterns
        patterns = [
            r'\b\d{3}[-.]?\d{3}[-.]?\d{4}\b',  # US format
            r'\(\d{3}\)\s*\d{3}[-.]?\d{4}',   # (XXX) XXX-XXXX
            r'\+\d{1,3}[-.\s]?\d{3,14}',      # International
            r'\b\d{10,}\b'                     # Simple digits
        ]
        
        for pattern in patterns:
            matches = re.findall(pattern, text)
            for match in matches:
                try:
                    # Parse and format phone number
                    parsed = phonenumbers.parse(match, "US")
                    if phonenumbers.is_valid_number(parsed):
                        formatted = phonenumbers.format_number(parsed, phonenumbers.PhoneNumberFormat.NATIONAL)
                        phones.append(self.clean_text(formatted))
                except:
                    # If parsing fails, keep original if it looks like a phone
                    if len(re.sub(r'[^\d]', '', match)) >= 10:
                        phones.append(self.clean_text(match))
        
        return list(set(phones))  # Remove duplicates
    
    def extract_emails(self, text):
        """Extract email addresses from text"""
        if not text:
            return []
        
        email_pattern = r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b'
        emails = re.findall(email_pattern, text)
        
        valid_emails = []
        for email in emails:
            try:
                # Validate email
                validated = validate_email(email)
                valid_emails.append(validated.email)
            except EmailNotValidError:
                pass
        
        return list(set(valid_emails))
    
    def extract_social_media_links(self, text, base_url=""):
        """Extract social media links"""
        if not text:
            return {}
        
        social_patterns = {
            'facebook': [
                r'facebook\.com/[^\s<>"\']+',
                r'fb\.com/[^\s<>"\']+',
                r'fb\.me/[^\s<>"\']+',
                r'm\.facebook\.com/[^\s<>"\']+'
            ],
            'twitter': [
                r'twitter\.com/[^\s<>"\']+',
                r'x\.com/[^\s<>"\']+',
                r't\.co/[^\s<>"\']+',
                r'mobile\.twitter\.com/[^\s<>"\']+'
            ],
            'instagram': [
                r'instagram\.com/[^\s<>"\']+',
                r'instagr\.am/[^\s<>"\']+'
            ],
            'linkedin': [
                r'linkedin\.com/[^\s<>"\']+',
                r'lnkd\.in/[^\s<>"\']+'
            ],
            'whatsapp': [
                r'wa\.me/[^\s<>"\']+',
                r'whatsapp\.com/[^\s<>"\']+',
                r'api\.whatsapp\.com/[^\s<>"\']+',
                r'chat\.whatsapp\.com/[^\s<>"\']+',
                r'web\.whatsapp\.com/[^\s<>"\']+',
            ],
            'telegram': [
                r't\.me/[^\s<>"\']+',
                r'telegram\.me/[^\s<>"\']+',
                r'telegram\.org/[^\s<>"\']+',
                r'tg://[^\s<>"\']+',
            ]
        }
        
        social_links = {}
        for platform, patterns in social_patterns.items():
            for pattern in patterns:
                matches = re.findall(pattern, text, re.IGNORECASE)
                if matches:
                    # Clean and format URL
                    url = matches[0]
                    if not url.startswith('http'):
                        url = 'https://' + url
                    social_links[platform] = url
                    break
        
        return social_links
    
    def extract_working_hours(self, text):
        """Extract working hours from text"""
        if not text:
            return {}
        
        # Common patterns for business hours
        patterns = [
            r'(monday|mon|m)[\s-:]*(\d{1,2}:?\d{0,2})\s*(am|pm)?\s*[-to]*\s*(\d{1,2}:?\d{0,2})\s*(am|pm)?',
            r'(tuesday|tue|t)[\s-:]*(\d{1,2}:?\d{0,2})\s*(am|pm)?\s*[-to]*\s*(\d{1,2}:?\d{0,2})\s*(am|pm)?',
            r'(wednesday|wed|w)[\s-:]*(\d{1,2}:?\d{0,2})\s*(am|pm)?\s*[-to]*\s*(\d{1,2}:?\d{0,2})\s*(am|pm)?',
            r'(thursday|thu|th)[\s-:]*(\d{1,2}:?\d{0,2})\s*(am|pm)?\s*[-to]*\s*(\d{1,2}:?\d{0,2})\s*(am|pm)?',
            r'(friday|fri|f)[\s-:]*(\d{1,2}:?\d{0,2})\s*(am|pm)?\s*[-to]*\s*(\d{1,2}:?\d{0,2})\s*(am|pm)?',
            r'(saturday|sat|s)[\s-:]*(\d{1,2}:?\d{0,2})\s*(am|pm)?\s*[-to]*\s*(\d{1,2}:?\d{0,2})\s*(am|pm)?',
            r'(sunday|sun|su)[\s-:]*(\d{1,2}:?\d{0,2})\s*(am|pm)?\s*[-to]*\s*(\d{1,2}:?\d{0,2})\s*(am|pm)?',
        ]
        
        hours = {}
        text_lower = text.lower()
        
        # Check for closed days
        if 'closed' in text_lower:
            closed_patterns = [
                r'(monday|mon|m)[\s-:]*closed',
                r'(tuesday|tue|t)[\s-:]*closed',
                r'(wednesday|wed|w)[\s-:]*closed',
                r'(thursday|thu|th)[\s-:]*closed',
                r'(friday|fri|f)[\s-:]*closed',
                r'(saturday|sat|s)[\s-:]*closed',
                r'(sunday|sun|su)[\s-:]*closed',
            ]
            
            day_mapping = {
                'monday': 'monday', 'mon': 'monday', 'm': 'monday',
                'tuesday': 'tuesday', 'tue': 'tuesday', 't': 'tuesday',
                'wednesday': 'wednesday', 'wed': 'wednesday', 'w': 'wednesday',
                'thursday': 'thursday', 'thu': 'thursday', 'th': 'thursday',
                'friday': 'friday', 'fri': 'friday', 'f': 'friday',
                'saturday': 'saturday', 'sat': 'saturday', 's': 'saturday',
                'sunday': 'sunday', 'sun': 'sunday', 'su': 'sunday',
            }
            
            for pattern in closed_patterns:
                matches = re.findall(pattern, text_lower)
                for match in matches:
                    day = day_mapping.get(match, match)
                    hours[day] = 'Closed'
        
        # Extract operating hours
        hours_pattern = r'(\w+)[\s-:]*(\d{1,2}:?\d{0,2})\s*(am|pm)?\s*[-to]*\s*(\d{1,2}:?\d{0,2})\s*(am|pm)?'
        matches = re.findall(hours_pattern, text_lower, re.IGNORECASE)
        
        day_mapping = {
            'monday': 'monday', 'mon': 'monday',
            'tuesday': 'tuesday', 'tue': 'tuesday',
            'wednesday': 'wednesday', 'wed': 'wednesday',
            'thursday': 'thursday', 'thu': 'thursday',
            'friday': 'friday', 'fri': 'friday',
            'saturday': 'saturday', 'sat': 'saturday',
            'sunday': 'sunday', 'sun': 'sunday',
        }
        
        for match in matches:
            day_raw, start_time, start_period, end_time, end_period = match
            day = day_mapping.get(day_raw.lower())
            if day:
                start = f"{start_time} {start_period}" if start_period else start_time
                end = f"{end_time} {end_period}" if end_period else end_time
                hours[day] = f"{start} - {end}"
        
        return hours
    
    def extract_materials(self, text):
        """Extract accepted materials from text"""
        if not text:
            return []
        
        text_lower = text.lower()
        found_materials = []
        
        for material in Config.MATERIAL_TYPES:
            if material.lower() in text_lower:
                found_materials.append(material)
        
        # Additional material detection patterns
        metal_patterns = [
            r'(scrap\s+)?metal',
            r'ferrous',
            r'non-ferrous',
            r'precious\s+metal',
            r'alloy',
            r'cast\s+iron',
            r'wrought\s+iron',
            r'galvanized',
            r'sheet\s+metal',
            r'pipe',
            r'tubing',
            r'wire',
            r'cable',
            r'electronic\s+waste',
            r'e-waste',
            r'automotive\s+parts',
            r'appliance',
            r'hvac',
            r'industrial\s+metal'
        ]
        
        for pattern in metal_patterns:
            if re.search(pattern, text_lower):
                material_match = re.search(pattern, text_lower).group()
                if material_match not in [m.lower() for m in found_materials]:
                    found_materials.append(material_match.title())
        
        return list(set(found_materials))
    
    def geocode_address(self, address):
        """Get coordinates for an address"""
        if not address:
            return None, None
        
        try:
            # Try Google Maps first if API key is available
            if self.gmaps:
                try:
                    geocode_result = self.gmaps.geocode(address)
                    if geocode_result:
                        location = geocode_result[0]['geometry']['location']
                        return location['lat'], location['lng']
                except Exception as e:
                    print(f"Google Maps geocoding failed: {e}")
            
            # Fallback to Nominatim
            time.sleep(1)  # Rate limiting
            location = self.geocoder.geocode(address, timeout=10)
            if location:
                return location.latitude, location.longitude
                
        except (GeocoderTimedOut, GeocoderServiceError) as e:
            print(f"Geocoding failed for {address}: {e}")
        
        return None, None
    
    def parse_address(self, address):
        """Parse address into components"""
        if not address:
            return {}
        
        # Try to parse address using Google Maps if available
        if self.gmaps:
            try:
                geocode_result = self.gmaps.geocode(address)
                if geocode_result:
                    components = geocode_result[0]['address_components']
                    parsed = {}
                    
                    for component in components:
                        types = component['types']
                        if 'street_number' in types:
                            parsed['street_number'] = component['long_name']
                        elif 'route' in types:
                            parsed['street_name'] = component['long_name']
                        elif 'locality' in types:
                            parsed['city'] = component['long_name']
                        elif 'administrative_area_level_1' in types:
                            parsed['state'] = component['short_name']
                        elif 'country' in types:
                            parsed['country'] = component['short_name']
                        elif 'postal_code' in types:
                            parsed['postal_code'] = component['long_name']
                    
                    # Combine street number and name
                    if 'street_number' in parsed and 'street_name' in parsed:
                        parsed['street_address'] = f"{parsed['street_number']} {parsed['street_name']}"
                    
                    return parsed
            except Exception as e:
                print(f"Address parsing failed: {e}")
        
        # Simple regex-based parsing as fallback
        address_parts = {}
        
        # Extract postal code
        postal_pattern = r'\b\d{5}(-\d{4})?\b'  # US ZIP code
        postal_match = re.search(postal_pattern, address)
        if postal_match:
            address_parts['postal_code'] = postal_match.group()
        
        # Extract state (2-letter codes)
        state_pattern = r'\b[A-Z]{2}\b'
        state_matches = re.findall(state_pattern, address)
        if state_matches:
            address_parts['state'] = state_matches[-1]  # Take the last one
        
        return address_parts
    
    def validate_url(self, url, base_url=""):
        """Validate and normalize URL"""
        if not url:
            return None
        
        try:
            # Handle relative URLs
            if url.startswith('/') and base_url:
                url = urljoin(base_url, url)
            elif not url.startswith(('http://', 'https://')):
                url = 'https://' + url
            
            # Basic URL validation
            parsed = urlparse(url)
            if parsed.netloc:
                return url
        except Exception:
            pass
        
        return None
    
    def add_delay(self):
        """Add random delay between requests"""
        delay = random.uniform(Config.REQUEST_DELAY * 0.5, Config.REQUEST_DELAY * 1.5)
        time.sleep(delay) 