#!/usr/bin/env python3
"""
Status Checker for Massive Data Collection
Shows real-time progress and completion status
"""

import os
import time
import psutil
from datetime import datetime
from pathlib import Path

def check_processes():
    """Check if Python scraping processes are running"""
    python_processes = []
    for proc in psutil.process_iter(['pid', 'name', 'cmdline']):
        try:
            if proc.info['name'] == 'python.exe':
                cmdline = ' '.join(proc.info['cmdline']) if proc.info['cmdline'] else ''
                if 'massive_scraper' in cmdline or 'launcher.py massive' in cmdline:
                    python_processes.append(proc.info['pid'])
        except (psutil.NoSuchProcess, psutil.AccessDenied):
            pass
    return python_processes

def check_output_files():
    """Check for generated output files"""
    output_dir = Path('output')
    current_dir = Path('.')
    
    files_found = {
        'excel': list(current_dir.glob('*.xlsx')) + list(output_dir.glob('*.xlsx')) if output_dir.exists() else list(current_dir.glob('*.xlsx')),
        'csv': list(current_dir.glob('*.csv')) + list(output_dir.glob('*.csv')) if output_dir.exists() else list(current_dir.glob('*.csv')),
        'json': list(current_dir.glob('*.json')) + list(output_dir.glob('*.json')) if output_dir.exists() else list(current_dir.glob('*.json'))
    }
    
    return files_found

def get_latest_log_entries(n=10):
    """Get latest log entries"""
    log_file = Path('scraping.log')
    if log_file.exists():
        try:
            with open(log_file, 'r', encoding='utf-8') as f:
                lines = f.readlines()
                return lines[-n:] if lines else []
        except Exception:
            return []
    return []

def main():
    print("🔧 Massive Data Collection - Status Checker")
    print("=" * 50)
    
    while True:
        print(f"\n📅 Status Check: {datetime.now().strftime('%H:%M:%S')}")
        
        # Check processes
        processes = check_processes()
        if processes:
            print(f"🔄 RUNNING: {len(processes)} scraping process(es) active")
            print(f"   Process IDs: {', '.join(map(str, processes))}")
        else:
            print("✅ COMPLETED: No scraping processes running")
        
        # Check output files
        files = check_output_files()
        total_files = sum(len(file_list) for file_list in files.values())
        
        if total_files > 0:
            print(f"📄 OUTPUT FILES GENERATED: {total_files} total")
            for file_type, file_list in files.items():
                if file_list:
                    print(f"   {file_type.upper()}: {len(file_list)} files")
                    for file in file_list:
                        size_mb = file.stat().st_size / (1024*1024)
                        print(f"     • {file.name} ({size_mb:.1f} MB)")
        else:
            print("📄 OUTPUT FILES: None generated yet")
        
        # Show recent log activity
        recent_logs = get_latest_log_entries(3)
        if recent_logs:
            print("📋 RECENT ACTIVITY:")
            for log in recent_logs:
                # Clean up the log line and show only relevant parts
                clean_log = log.strip()
                if any(keyword in clean_log.lower() for keyword in ['progress', 'completed', 'phase', 'results', 'records']):
                    # Extract just the message part
                    if ' - ' in clean_log:
                        parts = clean_log.split(' - ')
                        if len(parts) >= 3:
                            timestamp = parts[0]
                            message = ' - '.join(parts[2:])
                            print(f"   {timestamp.split()[1]}: {message}")
        
        # Status summary
        if not processes and total_files > 0:
            print("\n🎉 COLLECTION COMPLETE!")
            print("📊 Your data is ready in the files listed above")
            break
        elif not processes and total_files == 0:
            print("\n⚠️ Collection stopped but no files generated")
            print("💡 Try running: python launcher.py massive")
            break
        else:
            print(f"\n⏳ Collection in progress... (checking again in 30 seconds)")
            print("💡 Press Ctrl+C to stop monitoring")
        
        try:
            time.sleep(30)
        except KeyboardInterrupt:
            print("\n👋 Status monitoring stopped")
            break

if __name__ == '__main__':
    main() 