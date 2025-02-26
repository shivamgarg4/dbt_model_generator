#!/usr/bin/env python
"""
Launcher for the DBT Model Generator application

This script checks the Python version, installs required packages,
creates necessary directories, and launches the application.
"""
import os
import sys
import subprocess
import logging
import importlib.util
import traceback
from datetime import datetime
import tkinter as tk

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('launcher.log'),
        logging.StreamHandler(sys.stdout)
    ]
)

logger = logging.getLogger()

def check_python_version():
    """Check if the Python version is compatible"""
    required_version = (3, 8)
    current_version = sys.version_info
    
    logger.info(f"Python version check: {current_version.major}.{current_version.minor}")
    
    if current_version.major < required_version[0] or \
       (current_version.major == required_version[0] and current_version.minor < required_version[1]):
        logger.error(f"Python {required_version[0]}.{required_version[1]} or higher is required")
        return False
        
    logger.info(f"Python version check passed: {current_version.major}.{current_version.minor}")
    return True

def install_required_packages():
    """Install required packages from requirements.txt"""
    logger.info("Checking installed packages...")
    
    try:
        # Check if requirements.txt exists
        if not os.path.exists('requirements.txt'):
            logger.error("requirements.txt not found")
            return False
            
        # Read requirements
        with open('requirements.txt', 'r') as f:
            requirements = [line.strip() for line in f if line.strip() and not line.startswith('#')]
            
        # Check if packages are installed
        missing_packages = []
        for package in requirements:
            package_name = package.split('==')[0].split('>=')[0].strip()
            if not importlib.util.find_spec(package_name):
                missing_packages.append(package)
                
        # Install missing packages
        if missing_packages:
            logger.info(f"Installing missing packages: {', '.join(missing_packages)}")
            subprocess.check_call([sys.executable, '-m', 'pip', 'install'] + missing_packages)
            logger.info("Package installation completed")
        else:
            logger.info("All required packages are installed")
            
        return True
    except Exception as e:
        logger.error(f"Error installing packages: {str(e)}")
        return False

def create_directories():
    """Create necessary directories"""
    directories = ['dags', 'models', 'jobs', 'mappings', 'data']
    
    try:
        for directory in directories:
            if not os.path.exists(directory):
                os.makedirs(directory)
                logger.info(f"Created directory: {directory}")
                
        logger.info("Created necessary directories")
        return True
    except Exception as e:
        logger.error(f"Error creating directories: {str(e)}")
        return False

def setup_script_files():
    """Run the setup_scripts.py to create any missing script files"""
    setup_script = os.path.join(os.getcwd(), 'setup_scripts.py')
    if not os.path.exists(setup_script):
        logger.error("setup_scripts.py not found")
        return False
    
    logger.info("Setting up script files...")
    try:
        subprocess.run(
            [sys.executable, setup_script],
            check=True,
            capture_output=True,
            text=True
        )
        logger.info("Script files setup completed successfully")
        return True
    except Exception as e:
        logger.error(f"Error setting up script files: {str(e)}")
        return False

def run_application():
    """Run the main application"""
    try:
        # Import the main module from the correct file
        import dag_generator_app
        
        # Run the application
        logger.info("Starting DBT Model Generator application...")
        # Create the root window and start the application
        root = tk.Tk()
        app = dag_generator_app.DAGGeneratorApp(root)
        root.mainloop()
        logger.info("Application closed.")
        return True
    except Exception as e:
        logger.error(f"Error running application: {str(e)}")
        logger.error(traceback.format_exc())
        return False

def main():
    """Main entry point"""
    logger.info("=" * 50)
    logger.info("DBT Model Generator Launcher")
    logger.info("=" * 50)
    
    # Check Python version
    if not check_python_version():
        sys.exit(1)
        
    # Install required packages
    if not install_required_packages():
        sys.exit(1)
        
    # Create necessary directories
    if not create_directories():
        sys.exit(1)
        
    # Set up script files
    if not setup_script_files():
        sys.exit(1)
        
    # Run the application
    if not run_application():
        sys.exit(1)
        
    sys.exit(0)

if __name__ == "__main__":
    main() 
