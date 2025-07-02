#!/usr/bin/env python3
"""
Web UI for Scrap Metal Centers Data Collection Application
A Flask-based web interface for easy configuration and monitoring.
"""

import os
import sys
import json
import threading
import time
from datetime import datetime
from flask import Flask, render_template_string, request, jsonify, send_file
from flask_socketio import SocketIO, emit

# Add current directory to Python path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from config import Config
from main import ScrapMetalScraper
from models import DatabaseManager

app = Flask(__name__)
app.config['SECRET_KEY'] = 'scrap_metal_scraper_secret_key'
socketio = SocketIO(app, cors_allowed_origins="*")

# Global variables for scraping control
scraping_thread = None
scraping_status = {
    'running': False,
    'progress': 0,
    'current_task': '',
    'results_count': 0,
    'start_time': None,
    'logs': []
}

class WebUILogger:
    """Logger that emits to web UI"""
    def __init__(self):
        self.logs = []
    
    def info(self, message):
        log_entry = {
            'level': 'info',
            'message': message,
            'timestamp': datetime.now().strftime('%H:%M:%S')
        }
        self.logs.append(log_entry)
        scraping_status['logs'].append(log_entry)
        socketio.emit('log_update', log_entry)
    
    def error(self, message):
        log_entry = {
            'level': 'error',
            'message': message,
            'timestamp': datetime.now().strftime('%H:%M:%S')
        }
        self.logs.append(log_entry)
        scraping_status['logs'].append(log_entry)
        socketio.emit('log_update', log_entry)

web_logger = WebUILogger()

@app.route('/')
def index():
    """Main dashboard page"""
    return render_template_string(INDEX_TEMPLATE)

# HTML Template
INDEX_TEMPLATE = '''
<!DOCTYPE html>
<html>
<head>
    <title>Scrap Metal Centers Scraper</title>
    <link href="https://cdn.jsdelivr.net/npm/bootstrap@5.1.3/dist/css/bootstrap.min.css" rel="stylesheet">
    <script src="https://cdnjs.cloudflare.com/ajax/libs/socket.io/4.0.1/socket.io.js"></script>
    <style>
        .status-running { color: #28a745; font-weight: bold; }
        .status-stopped { color: #dc3545; font-weight: bold; }
        .log-info { color: #17a2b8; }
        .log-error { color: #dc3545; }
        .logs-container { height: 300px; overflow-y: scroll; background: #f8f9fa; padding: 10px; border: 1px solid #dee2e6; }
    </style>
</head>
<body>
    <nav class="navbar navbar-dark bg-dark">
        <div class="container">
            <span class="navbar-brand">🔧 Scrap Metal Centers Scraper</span>
        </div>
    </nav>
    
    <div class="container mt-4">
        <div class="row">
            <div class="col-md-8">
                <div class="card">
                    <div class="card-header">
                        <h3>Scraping Control Panel</h3>
                    </div>
                    <div class="card-body">
                        <form id="scrapingForm">
                            <div class="row">
                                <div class="col-md-6">
                                    <label class="form-label">Sources:</label>
                                    <div class="form-check">
                                        <input class="form-check-input" type="checkbox" value="google_search" id="source1" checked>
                                        <label class="form-check-label" for="source1">Google Search</label>
                                    </div>
                                    <div class="form-check">
                                        <input class="form-check-input" type="checkbox" value="google_maps" id="source2">
                                        <label class="form-check-label" for="source2">Google Maps</label>
                                    </div>
                                    <div class="form-check">
                                        <input class="form-check-input" type="checkbox" value="yellowpages" id="source3">
                                        <label class="form-check-label" for="source3">Yellow Pages</label>
                                    </div>
                                    <div class="form-check">
                                        <input class="form-check-input" type="checkbox" value="yelp" id="source4">
                                        <label class="form-check-label" for="source4">Yelp</label>
                                    </div>
                                </div>
                                
                                <div class="col-md-6">
                                    <div class="mb-3">
                                        <label class="form-label">Search Terms:</label>
                                        <textarea class="form-control" id="searchTerms" rows="3">scrap metal recycling
metal recycling centers
scrap yards</textarea>
                                    </div>
                                    
                                    <div class="mb-3">
                                        <label class="form-label">Locations:</label>
                                        <textarea class="form-control" id="locations" rows="3">New York, NY
Los Angeles, CA
Chicago, IL</textarea>
                                    </div>
                                    
                                    <div class="mb-3">
                                        <label class="form-label">Limit per source:</label>
                                        <input type="number" class="form-control" id="limit" value="10" min="1" max="1000">
                                    </div>
                                </div>
                            </div>
                            
                            <div class="d-grid gap-2">
                                <button type="submit" class="btn btn-primary" id="startBtn">Start Scraping</button>
                                <button type="button" class="btn btn-danger" id="stopBtn" disabled>Stop Scraping</button>
                            </div>
                        </form>
                        
                        <div class="mt-4">
                            <div class="d-flex justify-content-between">
                                <span id="currentTask">Ready to start</span>
                                <span id="progressText">0%</span>
                            </div>
                            <div class="progress">
                                <div class="progress-bar" id="progressBar" style="width: 0%"></div>
                            </div>
                        </div>
                    </div>
                </div>
            </div>
            
            <div class="col-md-4">
                <div class="card">
                    <div class="card-header">
                        <h5>Status</h5>
                    </div>
                    <div class="card-body">
                        <p><strong>Status:</strong> <span id="status" class="status-stopped">Stopped</span></p>
                        <p><strong>Results:</strong> <span id="resultsCount">0</span></p>
                        <p><strong>Start Time:</strong> <span id="startTime">-</span></p>
                    </div>
                </div>
                
                <div class="card mt-3">
                    <div class="card-header">
                        <h5>Live Logs</h5>
                    </div>
                    <div class="card-body">
                        <div id="logs" class="logs-container"></div>
                    </div>
                </div>
            </div>
        </div>
        
        <div class="row mt-4">
            <div class="col-12">
                <div class="card">
                    <div class="card-header">
                        <h5>Download Results</h5>
                    </div>
                    <div class="card-body">
                        <div class="row">
                            <div class="col-md-3">
                                <a href="/api/download/csv" class="btn btn-outline-primary w-100">Download CSV</a>
                            </div>
                            <div class="col-md-3">
                                <a href="/api/download/xlsx" class="btn btn-outline-success w-100">Download Excel</a>
                            </div>
                            <div class="col-md-3">
                                <a href="/api/download/json" class="btn btn-outline-info w-100">Download JSON</a>
                            </div>
                            <div class="col-md-3">
                                <button class="btn btn-outline-secondary w-100" onclick="loadResults()">View Results</button>
                            </div>
                        </div>
                    </div>
                </div>
            </div>
        </div>
    </div>

    <script src="https://cdn.jsdelivr.net/npm/bootstrap@5.1.3/dist/js/bootstrap.bundle.min.js"></script>
    <script>
        const socket = io();
        
        document.getElementById('scrapingForm').addEventListener('submit', function(e) {
            e.preventDefault();
            startScraping();
        });
        
        document.getElementById('stopBtn').addEventListener('click', stopScraping);
        
        function startScraping() {
            const sources = Array.from(document.querySelectorAll('input[type="checkbox"]:checked')).map(cb => cb.value);
            const searchTerms = document.getElementById('searchTerms').value.split('\\n').filter(term => term.trim());
            const locations = document.getElementById('locations').value.split('\\n').filter(loc => loc.trim());
            const limit = parseInt(document.getElementById('limit').value);
            
            fetch('/api/start_scraping', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ sources, search_terms: searchTerms, locations, limit })
            })
            .then(response => response.json())
            .then(data => {
                if (data.error) {
                    alert(data.error);
                } else {
                    updateUI(true);
                }
            });
        }
        
        function stopScraping() {
            fetch('/api/stop_scraping', { method: 'POST' })
            .then(response => response.json())
            .then(data => console.log(data.message));
        }
        
        function updateUI(running) {
            document.getElementById('startBtn').disabled = running;
            document.getElementById('stopBtn').disabled = !running;
            document.getElementById('status').textContent = running ? 'Running' : 'Stopped';
            document.getElementById('status').className = running ? 'status-running' : 'status-stopped';
        }
        
        function loadResults() {
            fetch('/api/results')
            .then(response => response.json())
            .then(data => {
                if (data.results && data.results.length > 0) {
                    alert(`Found ${data.count} results. Use download buttons to get the full data.`);
                } else {
                    alert('No results available yet. Run scraping first.');
                }
            });
        }
        
        // Socket events
        socket.on('status_update', function(status) {
            updateUI(status.running);
            document.getElementById('currentTask').textContent = status.current_task;
            document.getElementById('progressText').textContent = status.progress + '%';
            document.getElementById('progressBar').style.width = status.progress + '%';
            document.getElementById('resultsCount').textContent = status.results_count;
            
            if (status.start_time) {
                document.getElementById('startTime').textContent = new Date(status.start_time).toLocaleTimeString();
            }
        });
        
        socket.on('log_update', function(log) {
            const logsDiv = document.getElementById('logs');
            const logEntry = document.createElement('div');
            logEntry.className = 'log-' + log.level;
            logEntry.innerHTML = `<small>${log.timestamp}</small> ${log.message}`;
            logsDiv.appendChild(logEntry);
            logsDiv.scrollTop = logsDiv.scrollHeight;
        });
    </script>
</body>
</html>
'''

@app.route('/api/start_scraping', methods=['POST'])
def start_scraping():
    """Start scraping process"""
    global scraping_thread, scraping_status
    
    if scraping_status['running']:
        return jsonify({'error': 'Scraping is already running'}), 400
    
    # Get parameters from request
    data = request.get_json()
    sources = data.get('sources', ['google_search'])
    search_terms = data.get('search_terms', ['scrap metal recycling'])
    locations = data.get('locations', ['New York, NY'])
    limit = data.get('limit', 10)
    
    # Reset status
    scraping_status.update({
        'running': True,
        'progress': 0,
        'current_task': 'Initializing...',
        'results_count': 0,
        'start_time': datetime.now().isoformat(),
        'logs': []
    })
    
    # Start scraping in background thread
    scraping_thread = threading.Thread(
        target=run_scraping_background,
        args=(sources, search_terms, locations, limit)
    )
    scraping_thread.daemon = True
    scraping_thread.start()
    
    return jsonify({'message': 'Scraping started successfully'})

@app.route('/api/stop_scraping', methods=['POST'])
def stop_scraping():
    """Stop scraping process"""
    global scraping_status
    
    scraping_status['running'] = False
    scraping_status['current_task'] = 'Stopping...'
    
    socketio.emit('status_update', scraping_status)
    web_logger.info('Scraping stopped by user')
    
    return jsonify({'message': 'Scraping stop requested'})

@app.route('/api/status')
def get_status():
    """Get current scraping status"""
    return jsonify(scraping_status)

@app.route('/api/results')
def get_results():
    """Get scraping results"""
    try:
        # Get latest results from output directory
        output_dir = Config.OUTPUT_DIR
        if os.path.exists(output_dir):
            files = [f for f in os.listdir(output_dir) if f.endswith('.json')]
            if files:
                latest_file = max(files, key=lambda f: os.path.getctime(os.path.join(output_dir, f)))
                with open(os.path.join(output_dir, latest_file), 'r', encoding='utf-8') as f:
                    results = json.load(f)
                return jsonify({'results': results, 'count': len(results)})
        
        return jsonify({'results': [], 'count': 0})
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/download/<format>')
def download_results(format):
    """Download results in specified format"""
    try:
        output_dir = Config.OUTPUT_DIR
        if not os.path.exists(output_dir):
            return jsonify({'error': 'No results available'}), 404
        
        # Find latest file of requested format
        extension = f'.{format.lower()}'
        files = [f for f in os.listdir(output_dir) if f.endswith(extension)]
        
        if not files:
            return jsonify({'error': f'No {format} files available'}), 404
        
        latest_file = max(files, key=lambda f: os.path.getctime(os.path.join(output_dir, f)))
        file_path = os.path.join(output_dir, latest_file)
        
        return send_file(file_path, as_attachment=True)
    
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/database_stats')
def get_database_stats():
    """Get database statistics"""
    try:
        db_manager = DatabaseManager(Config.DATABASE_URL)
        
        centers = db_manager.get_all_centers()
        
        stats = {
            'total_centers': len(centers),
            'countries': {},
            'materials': {},
            'with_phone': 0,
            'with_email': 0,
            'with_website': 0,
            'with_coordinates': 0
        }
        
        for center in centers:
            # Count by country
            country = center.country or 'Unknown'
            stats['countries'][country] = stats['countries'].get(country, 0) + 1
            
            # Count materials
            for material in center.materials:
                stats['materials'][material.name] = stats['materials'].get(material.name, 0) + 1
            
            # Count completeness
            if center.phone_primary:
                stats['with_phone'] += 1
            if center.email_primary:
                stats['with_email'] += 1
            if center.website:
                stats['with_website'] += 1
            if center.latitude and center.longitude:
                stats['with_coordinates'] += 1
        
        db_manager.close()
        return jsonify(stats)
    
    except Exception as e:
        return jsonify({'error': str(e)}), 500

def run_scraping_background(sources, search_terms, locations, limit):
    """Run scraping in background thread"""
    global scraping_status
    
    try:
        scraper = ScrapMetalScraper()
        
        # Monkey patch logger to use web logger
        scraper.logger = web_logger
        
        web_logger.info(f'Starting scraping with sources: {sources}')
        web_logger.info(f'Search terms: {search_terms}')
        web_logger.info(f'Locations: {locations}')
        web_logger.info(f'Limit per source: {limit}')
        
        # Calculate total tasks for progress tracking
        total_tasks = len(sources) * len(search_terms) * len(locations)
        completed_tasks = 0
        
        results = []
        
        for source in sources:
            if not scraping_status['running']:
                break
                
            for search_term in search_terms:
                if not scraping_status['running']:
                    break
                    
                for location in locations:
                    if not scraping_status['running']:
                        break
                    
                    scraping_status['current_task'] = f'Scraping {source} for "{search_term}" in {location}'
                    socketio.emit('status_update', scraping_status)
                    
                    try:
                        # Create scraper instance for this source
                        scraper_class = scraper.scrapers.get(source)
                        if scraper_class:
                            source_scraper = scraper_class()
                            source_scraper.logger = web_logger
                            
                            task_results = source_scraper.scrape(
                                search_term=search_term,
                                location=location,
                                limit=limit
                            )
                            
                            results.extend(task_results)
                            scraping_status['results_count'] = len(results)
                            
                            web_logger.info(f'Completed {source} - found {len(task_results)} results')
                    
                    except Exception as e:
                        web_logger.error(f'Error in {source}: {str(e)}')
                    
                    completed_tasks += 1
                    scraping_status['progress'] = int((completed_tasks / total_tasks) * 100)
                    socketio.emit('status_update', scraping_status)
        
        if scraping_status['running']:
            # Remove duplicates
            unique_results = scraper._remove_duplicates(results)
            scraping_status['results_count'] = len(unique_results)
            
            # Export results
            if unique_results:
                scraping_status['current_task'] = 'Exporting results...'
                socketio.emit('status_update', scraping_status)
                
                scraper.data_exporter.export_data(unique_results)
                web_logger.info(f'Exported {len(unique_results)} unique results')
            
            scraping_status['current_task'] = 'Completed'
            scraping_status['progress'] = 100
            web_logger.info('Scraping completed successfully!')
        
    except Exception as e:
        web_logger.error(f'Scraping failed: {str(e)}')
        scraping_status['current_task'] = f'Error: {str(e)}'
    
    finally:
        scraping_status['running'] = False
        socketio.emit('status_update', scraping_status)

@socketio.on('connect')
def handle_connect():
    """Handle client connection"""
    emit('status_update', scraping_status)

def create_templates():
    """Create HTML templates for the web UI"""
    os.makedirs('templates', exist_ok=True)
    
    # Base template
    with open('templates/base.html', 'w', encoding='utf-8') as f:
        f.write('''<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>{% block title %}Scrap Metal Centers Scraper{% endblock %}</title>
    <link href="https://cdn.jsdelivr.net/npm/bootstrap@5.1.3/dist/css/bootstrap.min.css" rel="stylesheet">
    <script src="https://cdn.jsdelivr.net/npm/bootstrap@5.1.3/dist/js/bootstrap.bundle.min.js"></script>
    <script src="https://cdnjs.cloudflare.com/ajax/libs/socket.io/4.0.1/socket.io.js"></script>
    <style>
        .status-running { color: #28a745; }
        .status-stopped { color: #dc3545; }
        .log-info { color: #17a2b8; }
        .log-error { color: #dc3545; }
        .progress-container { margin: 20px 0; }
        .results-card { margin: 10px 0; }
    </style>
</head>
<body>
    <nav class="navbar navbar-expand-lg navbar-dark bg-dark">
        <div class="container">
            <a class="navbar-brand" href="/">🔧 Scrap Metal Centers Scraper</a>
            <div class="navbar-nav">
                <a class="nav-link" href="/">Dashboard</a>
                <a class="nav-link" href="/config">Configuration</a>
                <a class="nav-link" href="/results">Results</a>
            </div>
        </div>
    </nav>
    
    <div class="container mt-4">
        {% block content %}{% endblock %}
    </div>
    
    {% block scripts %}{% endblock %}
</body>
</html>''')
    
    # Index template
    with open('templates/index.html', 'w', encoding='utf-8') as f:
        f.write('''{% extends "base.html" %}

{% block content %}
<div class="row">
    <div class="col-md-8">
        <div class="card">
            <div class="card-header">
                <h3>Scraping Control Panel</h3>
            </div>
            <div class="card-body">
                <form id="scrapingForm">
                    <div class="row">
                        <div class="col-md-6">
                            <label class="form-label">Sources:</label>
                            <div class="form-check">
                                <input class="form-check-input" type="checkbox" value="google_search" id="source1" checked>
                                <label class="form-check-label" for="source1">Google Search</label>
                            </div>
                            <div class="form-check">
                                <input class="form-check-input" type="checkbox" value="google_maps" id="source2">
                                <label class="form-check-label" for="source2">Google Maps</label>
                            </div>
                            <div class="form-check">
                                <input class="form-check-input" type="checkbox" value="yellowpages" id="source3">
                                <label class="form-check-label" for="source3">Yellow Pages</label>
                            </div>
                            <div class="form-check">
                                <input class="form-check-input" type="checkbox" value="yelp" id="source4">
                                <label class="form-check-label" for="source4">Yelp</label>
                            </div>
                        </div>
                        
                        <div class="col-md-6">
                            <div class="mb-3">
                                <label class="form-label">Search Terms:</label>
                                <textarea class="form-control" id="searchTerms" rows="3">scrap metal recycling
metal recycling centers
scrap yards</textarea>
                            </div>
                            
                            <div class="mb-3">
                                <label class="form-label">Locations:</label>
                                <textarea class="form-control" id="locations" rows="3">New York, NY
Los Angeles, CA
Chicago, IL</textarea>
                            </div>
                            
                            <div class="mb-3">
                                <label class="form-label">Limit per source:</label>
                                <input type="number" class="form-control" id="limit" value="10" min="1" max="1000">
                            </div>
                        </div>
                    </div>
                    
                    <div class="d-grid gap-2">
                        <button type="submit" class="btn btn-primary" id="startBtn">Start Scraping</button>
                        <button type="button" class="btn btn-danger" id="stopBtn" disabled>Stop Scraping</button>
                    </div>
                </form>
                
                <div class="progress-container">
                    <div class="d-flex justify-content-between">
                        <span id="currentTask">Ready to start</span>
                        <span id="progressText">0%</span>
                    </div>
                    <div class="progress">
                        <div class="progress-bar" id="progressBar" style="width: 0%"></div>
                    </div>
                </div>
            </div>
        </div>
    </div>
    
    <div class="col-md-4">
        <div class="card">
            <div class="card-header">
                <h5>Status</h5>
            </div>
            <div class="card-body">
                <p><strong>Status:</strong> <span id="status" class="status-stopped">Stopped</span></p>
                <p><strong>Results:</strong> <span id="resultsCount">0</span></p>
                <p><strong>Start Time:</strong> <span id="startTime">-</span></p>
            </div>
        </div>
        
        <div class="card mt-3">
            <div class="card-header">
                <h5>Live Logs</h5>
            </div>
            <div class="card-body" style="height: 300px; overflow-y: scroll;">
                <div id="logs"></div>
            </div>
        </div>
    </div>
</div>
{% endblock %}

{% block scripts %}
<script>
const socket = io();
let isRunning = false;

// Form submission
document.getElementById('scrapingForm').addEventListener('submit', function(e) {
    e.preventDefault();
    startScraping();
});

// Stop button
document.getElementById('stopBtn').addEventListener('click', stopScraping);

function startScraping() {
    const sources = Array.from(document.querySelectorAll('input[type="checkbox"]:checked')).map(cb => cb.value);
    const searchTerms = document.getElementById('searchTerms').value.split('\\n').filter(term => term.trim());
    const locations = document.getElementById('locations').value.split('\\n').filter(loc => loc.trim());
    const limit = parseInt(document.getElementById('limit').value);
    
    fetch('/api/start_scraping', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ sources, search_terms: searchTerms, locations, limit })
    })
    .then(response => response.json())
    .then(data => {
        if (data.error) {
            alert(data.error);
        } else {
            updateUI(true);
        }
    });
}

function stopScraping() {
    fetch('/api/stop_scraping', { method: 'POST' })
    .then(response => response.json())
    .then(data => {
        console.log(data.message);
    });
}

function updateUI(running) {
    isRunning = running;
    document.getElementById('startBtn').disabled = running;
    document.getElementById('stopBtn').disabled = !running;
    document.getElementById('status').textContent = running ? 'Running' : 'Stopped';
    document.getElementById('status').className = running ? 'status-running' : 'status-stopped';
}

// Socket events
socket.on('status_update', function(status) {
    updateUI(status.running);
    document.getElementById('currentTask').textContent = status.current_task;
    document.getElementById('progressText').textContent = status.progress + '%';
    document.getElementById('progressBar').style.width = status.progress + '%';
    document.getElementById('resultsCount').textContent = status.results_count;
    
    if (status.start_time) {
        document.getElementById('startTime').textContent = new Date(status.start_time).toLocaleTimeString();
    }
});

socket.on('log_update', function(log) {
    const logsDiv = document.getElementById('logs');
    const logEntry = document.createElement('div');
    logEntry.className = 'log-' + log.level;
    logEntry.innerHTML = `<small>${log.timestamp}</small> ${log.message}`;
    logsDiv.appendChild(logEntry);
    logsDiv.scrollTop = logsDiv.scrollHeight;
});
</script>
{% endblock %}''')
    
    # Results template
    with open('templates/results.html', 'w', encoding='utf-8') as f:
        f.write('''{% extends "base.html" %}

{% block content %}
<div class="row">
    <div class="col-md-4">
        <div class="card">
            <div class="card-header">
                <h5>Database Statistics</h5>
            </div>
            <div class="card-body">
                <div id="dbStats">Loading...</div>
            </div>
        </div>
        
        <div class="card mt-3">
            <div class="card-header">
                <h5>Download Results</h5>
            </div>
            <div class="card-body">
                <div class="d-grid gap-2">
                    <a href="/api/download/csv" class="btn btn-outline-primary">Download CSV</a>
                    <a href="/api/download/xlsx" class="btn btn-outline-success">Download Excel</a>
                    <a href="/api/download/json" class="btn btn-outline-info">Download JSON</a>
                </div>
            </div>
        </div>
    </div>
    
    <div class="col-md-8">
        <div class="card">
            <div class="card-header">
                <h5>Latest Results <span id="resultCount" class="badge bg-primary">0</span></h5>
            </div>
            <div class="card-body">
                <div id="results">Loading...</div>
            </div>
        </div>
    </div>
</div>
{% endblock %}

{% block scripts %}
<script>
// Load database stats
fetch('/api/database_stats')
.then(response => response.json())
.then(data => {
    const statsDiv = document.getElementById('dbStats');
    if (data.error) {
        statsDiv.innerHTML = '<p class="text-danger">Error loading stats</p>';
    } else {
        statsDiv.innerHTML = `
            <p><strong>Total Centers:</strong> ${data.total_centers}</p>
            <p><strong>With Phone:</strong> ${data.with_phone}</p>
            <p><strong>With Email:</strong> ${data.with_email}</p>
            <p><strong>With Website:</strong> ${data.with_website}</p>
            <p><strong>With Coordinates:</strong> ${data.with_coordinates}</p>
        `;
    }
});

// Load results
fetch('/api/results')
.then(response => response.json())
.then(data => {
    const resultsDiv = document.getElementById('results');
    const countSpan = document.getElementById('resultCount');
    
    if (data.error) {
        resultsDiv.innerHTML = '<p class="text-danger">Error loading results</p>';
    } else {
        countSpan.textContent = data.count;
        
        if (data.results.length === 0) {
            resultsDiv.innerHTML = '<p>No results yet. Start scraping to see data here.</p>';
        } else {
            resultsDiv.innerHTML = data.results.slice(0, 10).map(result => `
                <div class="card results-card">
                    <div class="card-body">
                        <h6 class="card-title">${result.name || 'Unknown'}</h6>
                        <p class="card-text">
                            <small class="text-muted">
                                ${result.city || ''}, ${result.country || ''}<br>
                                ${result.phone_primary || 'No phone'}<br>
                                ${result.email_primary || 'No email'}
                            </small>
                        </p>
                        ${result.materials ? `<p><strong>Materials:</strong> ${result.materials.join(', ')}</p>` : ''}
                    </div>
                </div>
            `).join('');
            
            if (data.results.length > 10) {
                resultsDiv.innerHTML += `<p class="text-muted">Showing first 10 of ${data.count} results. Download files for complete data.</p>`;
            }
        }
    }
});
</script>
{% endblock %}''')
    
    # Config template
    with open('templates/config.html', 'w', encoding='utf-8') as f:
        f.write('''{% extends "base.html" %}

{% block content %}
<div class="card">
    <div class="card-header">
        <h3>Application Configuration</h3>
    </div>
    <div class="card-body">
        <div class="row">
            <div class="col-md-6">
                <h5>Scraping Settings</h5>
                <table class="table table-sm">
                    <tr><td>Request Delay</td><td>{{ config.REQUEST_DELAY }}s</td></tr>
                    <tr><td>Max Retries</td><td>{{ config.MAX_RETRIES }}</td></tr>
                    <tr><td>Timeout</td><td>{{ config.TIMEOUT }}s</td></tr>
                    <tr><td>Headless Browser</td><td>{{ config.HEADLESS_BROWSER }}</td></tr>
                    <tr><td>Browser Type</td><td>{{ config.BROWSER_TYPE }}</td></tr>
                </table>
            </div>
            
            <div class="col-md-6">
                <h5>Output Settings</h5>
                <table class="table table-sm">
                    <tr><td>Output Format</td><td>{{ config.OUTPUT_FORMAT }}</td></tr>
                    <tr><td>Output Directory</td><td>{{ config.OUTPUT_DIR }}</td></tr>
                    <tr><td>Database URL</td><td>{{ config.DATABASE_URL }}</td></tr>
                </table>
            </div>
        </div>
        
        <div class="row mt-4">
            <div class="col-md-6">
                <h5>Target Countries</h5>
                <ul class="list-group">
                    {% for country in config.TARGET_COUNTRIES %}
                    <li class="list-group-item">{{ country }}</li>
                    {% endfor %}
                </ul>
            </div>
            
            <div class="col-md-6">
                <h5>Search Terms</h5>
                <ul class="list-group" style="max-height: 200px; overflow-y: auto;">
                    {% for term in config.SEARCH_TERMS %}
                    <li class="list-group-item">{{ term }}</li>
                    {% endfor %}
                </ul>
            </div>
        </div>
        
        <div class="alert alert-info mt-4">
            <strong>Note:</strong> To modify these settings, edit the <code>config.py</code> file or use environment variables in a <code>.env</code> file.
        </div>
    </div>
</div>
{% endblock %}''')

if __name__ == '__main__':
    # Create templates directory and files
    create_templates()
    
    print("Starting Scrap Metal Centers Web UI...")
    print("Access the application at: http://localhost:5000")
    print("Press Ctrl+C to stop the server")
    
    try:
        socketio.run(app, host='0.0.0.0', port=5000, debug=False)
    except KeyboardInterrupt:
        print("\nWeb UI server stopped") 