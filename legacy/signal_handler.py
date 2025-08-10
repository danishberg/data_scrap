#!/usr/bin/env python3
"""
Signal handler for graceful shutdown of the scraping application
"""

import signal
import sys
import threading
import time
from typing import Optional, Callable

class GracefulKiller:
    """Handles SIGINT and SIGTERM for graceful shutdown"""
    
    def __init__(self, logger=None):
        self.kill_now = threading.Event()
        self.logger = logger
        self.original_sigint = signal.signal(signal.SIGINT, self._handle_signal)
        self.original_sigterm = signal.signal(signal.SIGTERM, self._handle_signal)
        self.cleanup_functions = []
        
    def _handle_signal(self, signum, frame):
        """Handle shutdown signals"""
        signal_name = "SIGINT" if signum == signal.SIGINT else "SIGTERM"
        
        if self.logger:
            self.logger.info(f"Received {signal_name}, initiating graceful shutdown...")
        else:
            print(f"\nReceived {signal_name}, initiating graceful shutdown...")
        
        # Set the kill flag
        self.kill_now.set()
        
        # Run cleanup functions
        for cleanup_func in self.cleanup_functions:
            try:
                cleanup_func()
            except Exception as e:
                if self.logger:
                    self.logger.error(f"Error in cleanup function: {e}")
                else:
                    print(f"Error in cleanup function: {e}")
        
        # Give some time for cleanup
        time.sleep(1)
        
        if self.logger:
            self.logger.info("Shutdown complete")
        else:
            print("Shutdown complete")
        
        sys.exit(0)
    
    def add_cleanup_function(self, func: Callable):
        """Add a function to be called during cleanup"""
        self.cleanup_functions.append(func)
    
    def should_exit(self) -> bool:
        """Check if application should exit"""
        return self.kill_now.is_set()
    
    def restore_signals(self):
        """Restore original signal handlers"""
        signal.signal(signal.SIGINT, self.original_sigint)
        signal.signal(signal.SIGTERM, self.original_sigterm)

class InterruptibleScraper:
    """Base class for scrapers that can be interrupted gracefully"""
    
    def __init__(self, logger=None):
        self.killer = GracefulKiller(logger)
        self.logger = logger
        self.active_sessions = []
        self.active_drivers = []
        
        # Register cleanup
        self.killer.add_cleanup_function(self.cleanup)
    
    def should_continue(self) -> bool:
        """Check if scraping should continue"""
        return not self.killer.should_exit()
    
    def register_session(self, session):
        """Register a requests session for cleanup"""
        self.active_sessions.append(session)
    
    def register_driver(self, driver):
        """Register a Selenium driver for cleanup"""
        self.active_drivers.append(driver)
    
    def cleanup(self):
        """Cleanup active sessions and drivers"""
        if self.logger:
            self.logger.info("Cleaning up active connections...")
        
        # Close requests sessions
        for session in self.active_sessions:
            try:
                session.close()
            except Exception as e:
                if self.logger:
                    self.logger.error(f"Error closing session: {e}")
        
        # Close Selenium drivers
        for driver in self.active_drivers:
            try:
                driver.quit()
            except Exception as e:
                if self.logger:
                    self.logger.error(f"Error closing driver: {e}")
        
        self.active_sessions.clear()
        self.active_drivers.clear()
    
    def safe_sleep(self, duration: float):
        """Sleep that can be interrupted"""
        start_time = time.time()
        while time.time() - start_time < duration:
            if self.should_continue():
                time.sleep(0.1)
            else:
                break
    
    def safe_request(self, session, *args, **kwargs):
        """Make a request that can be interrupted"""
        if not self.should_continue():
            return None
        
        try:
            # Set a shorter timeout for responsiveness
            kwargs.setdefault('timeout', 10)
            return session.request(*args, **kwargs)
        except KeyboardInterrupt:
            raise
        except Exception as e:
            if self.logger:
                self.logger.error(f"Request failed: {e}")
            return None

def setup_signal_handling(logger=None, cleanup_functions=None):
    """Setup signal handling for the application"""
    killer = GracefulKiller(logger)
    
    if cleanup_functions:
        for func in cleanup_functions:
            killer.add_cleanup_function(func)
    
    return killer

def handle_keyboard_interrupt(func):
    """Decorator to handle keyboard interrupts gracefully"""
    def wrapper(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except KeyboardInterrupt:
            print("\nOperation interrupted by user (Ctrl+C)")
            sys.exit(1)
        except Exception as e:
            print(f"Error: {e}")
            sys.exit(1)
    
    return wrapper 