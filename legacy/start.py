#!/usr/bin/env python3
"""
Simple Start Script for Scrap Metal Centers Application
Just run: python start.py
"""

import os
import sys
import subprocess
import webbrowser
import time
import threading

def check_dependencies():
    """Check if all required dependencies are installed"""
    try:
        import flask
        import flask_socketio
        return True
    except ImportError:
        print("🔧 Installing required web dependencies...")
        try:
            subprocess.check_call([sys.executable, "-m", "pip", "install", "flask", "flask-socketio", "eventlet"])
            print("✅ Dependencies installed successfully!")
            return True
        except subprocess.CalledProcessError:
            print("❌ Failed to install dependencies automatically.")
            print("Please run: pip install flask flask-socketio eventlet")
            return False

def start_server_background():
    """Start the web server in background"""
    subprocess.run([sys.executable, "web_ui.py"])

def main():
    print("🔧 Scrap Metal Centers Data Collection Application")
    print("=" * 55)
    print()
    
    # Check dependencies
    if not check_dependencies():
        return
    
    print("🚀 Starting integrated web application...")
    print()
    print("📋 Features:")
    print("   ✓ Real-time scraping control")
    print("   ✓ Live progress monitoring")
    print("   ✓ Visual configuration interface")
    print("   ✓ Download results in multiple formats")
    print("   ✓ Start/Stop controls with buttons")
    print()
    
    # Start web server in background thread
    server_thread = threading.Thread(target=start_server_background)
    server_thread.daemon = True
    server_thread.start()
    
    # Wait for server to start
    print("⏳ Starting web server...")
    time.sleep(3)
    
    # Open browser automatically
    url = "http://localhost:5000"
    print(f"🌐 Opening web interface at {url}")
    
    try:
        webbrowser.open(url)
        print("✅ Browser opened successfully!")
    except Exception as e:
        print(f"⚠️  Browser didn't open automatically")
        print(f"📱 Please manually open: {url}")
    
    print()
    print("🎛️  Use the web interface to:")
    print("   • Configure scraping sources and locations")
    print("   • Start and monitor scraping progress")
    print("   • View real-time logs and results")
    print("   • Download data in CSV, Excel, or JSON format")
    print()
    print("⚡ Press Ctrl+C here to stop the application")
    print("=" * 55)
    
    try:
        # Keep main thread alive
        while server_thread.is_alive():
            time.sleep(1)
    except KeyboardInterrupt:
        print("\n🛑 Shutting down application...")
        print("✅ Application stopped successfully!")
        
        # Kill any remaining Python processes
        try:
            if sys.platform == "win32":
                subprocess.run(["taskkill", "/F", "/IM", "python.exe"], 
                             capture_output=True)
        except:
            pass

if __name__ == '__main__':
    main() 