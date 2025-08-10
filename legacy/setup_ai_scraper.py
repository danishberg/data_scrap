#!/usr/bin/env python3
"""
Automated Setup Script for AI-Enhanced Metal Scraper
This script installs all necessary dependencies and configures the environment
"""

import os
import sys
import subprocess
import platform
import logging
from pathlib import Path

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def run_command(command, description=""):
    """Run a shell command and handle errors"""
    try:
        logger.info(f"Running: {description or command}")
        result = subprocess.run(command, shell=True, capture_output=True, text=True)
        
        if result.returncode == 0:
            logger.info(f"✅ Success: {description or command}")
            return True
        else:
            logger.error(f"❌ Failed: {description or command}")
            logger.error(f"Error: {result.stderr}")
            return False
    except Exception as e:
        logger.error(f"❌ Exception running command: {e}")
        return False

def check_python_version():
    """Check if Python version is compatible"""
    version = sys.version_info
    if version.major < 3 or (version.major == 3 and version.minor < 8):
        logger.error("❌ Python 3.8 or higher is required")
        return False
    
    logger.info(f"✅ Python {version.major}.{version.minor}.{version.micro} detected")
    return True

def install_basic_requirements():
    """Install basic requirements"""
    logger.info("📦 Installing basic requirements...")
    
    # Upgrade pip first
    if not run_command("python -m pip install --upgrade pip", "Upgrading pip"):
        return False
    
    # Install core requirements first
    core_packages = [
        "requests>=2.31.0",
        "beautifulsoup4>=4.12.0",
        "pandas>=2.0.0",
        "lxml>=4.9.0",
        "openpyxl>=3.1.0",
        "phonenumbers>=8.13.0"
    ]
    
    logger.info("Installing core packages...")
    for package in core_packages:
        if not run_command(f"pip install {package}", f"Installing {package}"):
            logger.warning(f"Failed to install {package}, continuing...")
    
    # Try to install AI packages (optional)
    ai_packages = [
        "torch>=2.2.0",
        "transformers>=4.35.0",
        "spacy>=3.7.0",
        "sentence-transformers>=2.2.0"
    ]
    
    logger.info("Installing AI packages (optional)...")
    for package in ai_packages:
        if not run_command(f"pip install {package}", f"Installing {package}"):
            logger.warning(f"Failed to install {package}, AI features may be limited")
    
    return True

def install_spacy_model():
    """Install spaCy English model"""
    logger.info("🧠 Installing spaCy English model...")
    
    if not run_command("python -m spacy download en_core_web_sm", "Installing spaCy model"):
        logger.warning("⚠️  spaCy model installation failed. You can install it manually later.")
        return False
    
    return True

def install_ollama():
    """Install Ollama (optional)"""
    logger.info("🤖 Installing Ollama (optional)...")
    
    system = platform.system().lower()
    
    if system == "linux" or system == "darwin":  # Linux or macOS
        command = "curl -fsSL https://ollama.ai/install.sh | sh"
    elif system == "windows":
        logger.info("⚠️  For Windows, please download Ollama from https://ollama.ai/download")
        return False
    else:
        logger.warning("⚠️  Unsupported system for Ollama auto-install")
        return False
    
    if not run_command(command, "Installing Ollama"):
        logger.warning("⚠️  Ollama installation failed. You can install it manually later.")
        return False
    
    return True

def setup_ollama_model():
    """Setup Ollama model"""
    logger.info("🦙 Setting up Ollama model...")
    
    # Check if Ollama is available
    if not run_command("ollama --version", "Checking Ollama"):
        logger.warning("⚠️  Ollama not available. Skipping model setup.")
        return False
    
    # Pull llama2 model
    if not run_command("ollama pull llama2", "Pulling Llama2 model"):
        logger.warning("⚠️  Failed to pull Llama2 model. You can do this manually later.")
        return False
    
    return True

def create_test_script():
    """Create a test script to verify installation"""
    test_script = """#!/usr/bin/env python3
import sys
import importlib

def test_import(module_name, description=""):
    try:
        importlib.import_module(module_name)
        print(f"✅ {description or module_name} - OK")
        return True
    except ImportError as e:
        print(f"❌ {description or module_name} - FAILED: {e}")
        return False

def main():
    print("🧪 Testing AI-Enhanced Scraper Dependencies\\n")
    
    all_good = True
    
    # Basic dependencies
    all_good &= test_import("requests", "HTTP Requests")
    all_good &= test_import("bs4", "BeautifulSoup")
    all_good &= test_import("pandas", "Pandas")
    all_good &= test_import("openpyxl", "Excel Support")
    
    # AI dependencies
    all_good &= test_import("transformers", "Transformers")
    all_good &= test_import("torch", "PyTorch")
    all_good &= test_import("spacy", "spaCy")
    all_good &= test_import("sentence_transformers", "Sentence Transformers")
    
    # Phone number processing
    all_good &= test_import("phonenumbers", "Phone Numbers")
    
    # Optional dependencies
    test_import("ollama", "Ollama (Optional)")
    
    # Test spaCy model
    try:
        import spacy
        nlp = spacy.load("en_core_web_sm")
        print("✅ spaCy English model - OK")
    except Exception as e:
        print(f"❌ spaCy English model - FAILED: {e}")
        all_good = False
    
    print("\\n" + "="*50)
    if all_good:
        print("🎉 All dependencies installed successfully!")
        print("🚀 You can now run: python accurate_scraper.py")
    else:
        print("⚠️  Some dependencies failed to install.")
        print("📖 Check the AI_SETUP_GUIDE.md for troubleshooting.")
    
    return all_good

if __name__ == "__main__":
    main()
"""
    
    with open("test_installation.py", "w") as f:
        f.write(test_script)
    
    logger.info("✅ Created test script: test_installation.py")

def create_quick_start_script():
    """Create a quick start script"""
    quick_start = """#!/usr/bin/env python3
\"\"\"
Quick Start Script for AI-Enhanced Metal Scraper
\"\"\"

from accurate_scraper import AIEnhancedMetalScraper
import os

def main():
    print("🤖 AI-Enhanced Metal Scraper - Quick Start")
    print("=" * 50)
    
    # Initialize scraper
    scraper = AIEnhancedMetalScraper()
    
    # Get user input
    try:
        target = input("How many businesses to scrape? (default: 50): ").strip()
        target = int(target) if target else 50
    except ValueError:
        target = 50
    
    print(f"\\n🚀 Starting scraper for {target} businesses...")
    print("⏱️  This may take 10-20 minutes for comprehensive results")
    
    # Run scraper
    try:
        results = scraper.run_comprehensive_scraping(target)
        
        if results:
            print(f"\\n✅ Successfully scraped {len(results)} businesses!")
            
            # Export results
            output_info = scraper.export_results()
            
            print(f"\\n📁 Files exported:")
            print(f"  📊 Excel: {output_info.get('excel', 'N/A')}")
            print(f"  📄 CSV: {output_info.get('csv', 'N/A')}")
            print(f"  📋 JSON: {output_info.get('json', 'N/A')}")
            print(f"  📈 Report: {output_info.get('report', 'N/A')}")
            
            # Show sample results
            print(f"\\n🎯 Sample Results:")
            for i, business in enumerate(results[:5]):
                print(f"  {i+1}. {business.get('name', 'N/A')}")
                print(f"     📞 {business.get('phone', 'N/A')}")
                print(f"     📧 {business.get('email', 'N/A')}")
                print(f"     🌐 {business.get('website', 'N/A')}")
                print()
            
            print("🎉 Scraping completed successfully!")
            
        else:
            print("❌ No businesses found. Try adjusting search parameters.")
            
    except KeyboardInterrupt:
        print("\\n⏹️  Scraping interrupted by user")
        
    except Exception as e:
        print(f"\\n❌ Error: {e}")
        print("📖 Check the AI_SETUP_GUIDE.md for troubleshooting")

if __name__ == "__main__":
    main()
"""
    
    with open("quick_start.py", "w") as f:
        f.write(quick_start)
    
    logger.info("✅ Created quick start script: quick_start.py")

def main():
    """Main setup function"""
    print("🤖 AI-Enhanced Metal Scraper Setup")
    print("=" * 50)
    
    # Check Python version
    if not check_python_version():
        sys.exit(1)
    
    # Install basic requirements
    if not install_basic_requirements():
        logger.error("❌ Failed to install basic requirements")
        sys.exit(1)
    
    # Install spaCy model
    install_spacy_model()
    
    # Ask about Ollama installation
    if sys.stdin.isatty():  # Only ask if running interactively
        install_ollama_choice = input("\\nInstall Ollama for local LLM support? (y/N): ").lower().strip()
        if install_ollama_choice == 'y':
            if install_ollama():
                setup_ollama_model()
    
    # Create helper scripts
    create_test_script()
    create_quick_start_script()
    
    print("\\n" + "=" * 50)
    print("🎉 Setup completed!")
    print("\\n📋 Next steps:")
    print("  1. Run: python test_installation.py (to verify setup)")
    print("  2. Run: python quick_start.py (for quick start)")
    print("  3. Run: python accurate_scraper.py (for full scraper)")
    print("\\n📖 Read AI_SETUP_GUIDE.md for detailed documentation")
    
    # Run test automatically
    print("\\n🧪 Running installation test...")
    try:
        import subprocess
        result = subprocess.run([sys.executable, "test_installation.py"], 
                              capture_output=True, text=True)
        print(result.stdout)
        if result.stderr:
            print(result.stderr)
    except Exception as e:
        logger.error(f"Failed to run test: {e}")

if __name__ == "__main__":
    main() 