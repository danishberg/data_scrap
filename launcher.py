#!/usr/bin/env python3
"""
Launcher script for Scrap Metal Centers Application
Provides options to run in command-line or web UI mode.
"""

import os
import sys
import argparse
import subprocess
import webbrowser
import time
import threading

def check_dependencies():
    """Check if all required dependencies are installed"""
    try:
        import flask
        import flask_socketio
        print("✓ All web dependencies available")
        return True
    except ImportError as e:
        print(f"✗ Missing web dependencies: {e}")
        print("Please install web dependencies: pip install flask flask-socketio eventlet")
        return False

def run_command_line():
    """Run the command-line version"""
    print("Starting command-line scraper...")
    subprocess.run([sys.executable, "main.py"] + sys.argv[2:])

def run_web_ui():
    """Run the web UI version with automatic browser opening"""
    if not check_dependencies():
        print("Cannot start web UI without required dependencies.")
        return
    
    print("🚀 Starting integrated web interface...")
    print("📋 This will:")
    print("   • Start the web server")
    print("   • Open your browser automatically")
    print("   • Provide full scraping control in one interface")
    print()
    
    # Start web server in background
    def start_server():
        subprocess.run([sys.executable, "web_ui.py"])
    
    server_thread = threading.Thread(target=start_server)
    server_thread.daemon = True
    server_thread.start()
    
    # Wait a moment for server to start
    print("⏳ Starting web server...")
    time.sleep(3)
    
    # Open browser automatically
    url = "http://localhost:5000"
    print(f"🌐 Opening browser at {url}")
    try:
        webbrowser.open(url)
        print("✅ Browser opened successfully!")
    except Exception as e:
        print(f"⚠️  Could not open browser automatically: {e}")
        print(f"📱 Please manually open: {url}")
    
    print()
    print("🎛️  Web Interface Features:")
    print("   • Real-time scraping control and monitoring")
    print("   • Live progress tracking and logs")
    print("   • Configure sources, locations, and search terms")
    print("   • Download results in multiple formats")
    print("   • Start/stop scraping with buttons")
    print()
    print("⚡ Press Ctrl+C here to stop the web server")
    print("=" * 50)
    
    try:
        # Keep the main thread alive to handle Ctrl+C
        server_thread.join()
    except KeyboardInterrupt:
        print("\n🛑 Shutting down web server...")
        print("✅ Server stopped successfully!")

def run_demo():
    """Run the demo script"""
    print("Running demo...")
    subprocess.run([sys.executable, "demo.py"])

def run_test():
    """Run the test script"""
    print("Running tests...")
    subprocess.run([sys.executable, "test_scraper.py"])

def run_massive():
    """Run massive data collection (20K+ entries)"""
    print("🚀 Starting massive data collection...")
    print("🎯 Target: 20,000-100,000+ comprehensive records")
    subprocess.run([sys.executable, "massive_scraper.py"])

def run_integrated():
    """Run integrated web interface (default mode)"""
    run_web_ui()

def main():
    parser = argparse.ArgumentParser(
        description="Scrap Metal Centers Data Collection Application",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python launcher.py                        # Start integrated web interface (recommended)
  python launcher.py web                    # Start web UI
  python launcher.py cli                    # Start command-line interface
  python launcher.py cli --sources google_search --limit 20
  python launcher.py massive                # Massive collection (20K+ records)
  python launcher.py demo                   # Run demo
  python launcher.py test                   # Run tests
        """
    )
    
    parser.add_argument(
        'mode',
        choices=['web', 'cli', 'demo', 'test', 'massive'],
        nargs='?',
        default='web',
        help='Application mode (default: web)'
    )
    
    # Add a special case for web mode (no additional args)
    if len(sys.argv) > 1 and sys.argv[1] == 'web':
        run_web_ui()
        return
    
    if len(sys.argv) > 1 and sys.argv[1] == 'demo':
        run_demo()
        return
        
    if len(sys.argv) > 1 and sys.argv[1] == 'test':
        run_test()
        return
        
    if len(sys.argv) > 1 and sys.argv[1] == 'massive':
        run_massive()
        return
    
    args, remaining = parser.parse_known_args()
    
    if args.mode == 'cli':
        # Pass remaining arguments to main.py
        sys.argv = ['main.py'] + remaining
        run_command_line()
    elif args.mode == 'web':
        run_web_ui()
    elif args.mode == 'demo':
        run_demo()
    elif args.mode == 'test':
        run_test()
    elif args.mode == 'massive':
        run_massive()

if __name__ == '__main__':
    print("🔧 Scrap Metal Centers Data Collection Application")
    print("=" * 50)
    
    if len(sys.argv) == 1:
        print("🚀 Starting integrated web interface (default mode)")
        print("💡 For other options:")
        print("  python launcher.py web    # Web interface")
        print("  python launcher.py cli    # Command-line interface")
        print("  python launcher.py demo   # Run demonstration")
        print("  python launcher.py test   # Run tests")
        print()
        run_integrated()
    else:
        main() 