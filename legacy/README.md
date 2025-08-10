# 🔧 Scrap Metal Centers Data Collection Application

A comprehensive Python application for collecting data about scrap metal and recycling centers across English-speaking countries. Features both command-line and web interfaces with real-time monitoring.

## 🚀 Quick Start (One Command!)

### Easiest Way to Start:
```bash
python start.py
```

This single command will:
- ✅ Check and install dependencies automatically
- ✅ Start the web server
- ✅ Open your browser to the interface
- ✅ Provide full scraping control in one place

### Alternative Launch Options:
```bash
# Default web interface
python launcher.py

# Specific modes
python launcher.py web    # Web interface
python launcher.py cli    # Command-line interface
python launcher.py demo   # Run demonstration
python launcher.py test   # Run tests
```

## 📋 Features

### 🌐 Web Interface (Recommended)
- **Real-time monitoring** with live progress bars
- **Visual configuration** of sources, locations, search terms
- **Live log streaming** to see scraping progress
- **Start/Stop controls** with intuitive buttons
- **Download results** in CSV, Excel, JSON formats
- **Responsive design** works on any device

### 💻 Command-Line Interface
- **Full programmatic control** with command-line arguments
- **Proper signal handling** - Ctrl+C works correctly
- **Parallel processing** for efficient scraping
- **Detailed logging** and progress reporting

### 🎯 Data Collection
- **Multi-source scraping**: Google Search, Google Maps, Yellow Pages, Yelp
- **Comprehensive data**: Business info, contacts, materials, pricing
- **Geographic coverage**: USA, Canada, UK, Australia, New Zealand, Ireland, South Africa
- **Contact extraction**: Phones, emails, social media, WhatsApp, Telegram
- **Material tracking**: Types of metals accepted and pricing information

## 📦 Installation

### Requirements
- Python 3.8+
- Internet connection
- Web browser (for web interface)

### Setup
```bash
# Clone or download the application
cd data_scrap

# Install dependencies (automatic with start.py)
pip install -r requirements.txt

# Start the application
python start.py
```

## 🎮 Usage

### Web Interface Usage
1. Run `python start.py`
2. Browser opens automatically to http://localhost:5000
3. Configure your scraping parameters:
   - Select sources (Google Search, Google Maps, Yellow Pages, Yelp)
   - Enter search terms (pre-filled with relevant terms)
   - Specify locations to search
   - Set limit per source
4. Click "Start Scraping" 
5. Monitor real-time progress and logs
6. Download results when complete

### Command-Line Usage
```bash
# Basic usage
python launcher.py cli

# Custom configuration
python launcher.py cli --sources google_search yellowpages --limit 50

# Specific locations
python launcher.py cli --locations "New York, NY" "Los Angeles, CA"

# Get help
python launcher.py cli --help
```

### Available Sources
- `google_search` - Google Search results
- `google_maps` - Google Maps business listings
- `yellowpages` - Yellow Pages (US)
- `yellowpages_ca` - Yellow Pages Canada
- `yelp` - Yelp business listings

## 📊 Output Formats

Results are automatically exported in multiple formats:
- **CSV** - Spreadsheet compatible
- **Excel** - .xlsx format with formatting
- **JSON** - Machine-readable format
- **SQLite Database** - Relational data storage
- **Summary Report** - Human-readable overview

## 🗂️ Data Fields Collected

### Business Information
- Business name and description
- Website URL
- Category and industry type

### Location Data
- Full address
- Separate fields: street, city, state/region, postal code, country
- Coordinates (latitude, longitude) via geocoding

### Contact Information
- Primary and secondary phone numbers
- Primary and secondary email addresses
- Social media profiles (Facebook, Twitter, Instagram, LinkedIn)
- Messaging apps (WhatsApp, Telegram)

### Business Details
- Working hours (structured by day of week)
- Materials accepted (types of scrap metal)
- Pricing information (when available)
- Verification status and data quality scores

### Metadata
- Source of information
- Scraping timestamp
- Data completeness metrics

## 🛠️ Configuration

### Environment Variables
Create a `.env` file to customize settings:
```env
REQUEST_DELAY=2
MAX_RETRIES=3
TIMEOUT=30
HEADLESS_BROWSER=true
OUTPUT_FORMAT=all
```

### Search Customization
Edit `config.py` to modify:
- Target countries and regions
- Search terms and keywords
- Material types to track
- Output preferences

## 🧪 Testing and Demo

### Run Demo
```bash
python launcher.py demo
```
Creates sample data to test the application functionality.

### Run Tests
```bash
python launcher.py test
```
Validates scraper functionality and data processing.

## 🔧 Architecture

### Core Components
- **`start.py`** - Simple startup script
- **`launcher.py`** - Multi-mode launcher
- **`web_ui.py`** - Flask web interface with real-time updates
- **`main.py`** - Command-line interface with signal handling
- **`signal_handler.py`** - Graceful shutdown management

### Scrapers
- **`base_scraper.py`** - Abstract base class
- **`google_maps_scraper.py`** - Google services integration
- **`yellowpages_scraper.py`** - Yellow Pages scrapers
- **`yelp_scraper.py`** - Yelp business data

### Data Management
- **`models.py`** - Database schema and ORM
- **`utils.py`** - Data processing utilities
- **`data_exporter.py`** - Export functionality
- **`config.py`** - Application configuration

## 🚨 Important Notes

### Rate Limiting
The application includes built-in rate limiting to respect website terms of service:
- Configurable delays between requests
- Retry mechanisms for failed requests
- User-agent rotation for ethical scraping

### Ctrl+C Signal Handling
Both interfaces properly handle interruption:
- **Web Interface**: Use the "Stop Scraping" button or Ctrl+C in terminal
- **Command-Line**: Ctrl+C for immediate graceful shutdown
- Automatic cleanup of browser processes and active connections

### Legal Compliance
- Scrapes only publicly available information
- Respects robots.txt files
- Includes delays to avoid overwhelming servers
- For business research and lead generation purposes

## 📈 Performance

### Optimization Features
- **Parallel processing** with configurable worker threads
- **Duplicate detection** and removal
- **Data caching** to avoid re-scraping
- **Incremental updates** for large datasets
- **Memory-efficient** streaming for large result sets

### Scalability
- Handles thousands of results efficiently
- Configurable limits to manage resources
- Background processing for web interface
- Database storage for large datasets

## 🤝 Support

### Troubleshooting
1. **Dependencies Issues**: Run `pip install -r requirements.txt`
2. **Browser Won't Open**: Manually navigate to http://localhost:5000
3. **Scraping Errors**: Check internet connection and try different sources
4. **Ctrl+C Not Working**: Use the web interface stop button instead

### Getting Help
- Check the console logs for detailed error messages
- Use the demo mode to verify installation
- Try different scraping sources if one fails
- Reduce the limit per source for testing

---

**Built for business research and lead generation in the recycling industry.**
**Ethical scraping practices with proper rate limiting and respect for website terms of service.** 