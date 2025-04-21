import os
import sys
import subprocess
import logging
import winreg
import platform
import shutil


def setup_logging():
    """Configure logging for the application."""
    log_dir = os.path.join(os.path.dirname(__file__), 'logs')
    os.makedirs(log_dir, exist_ok=True)
    log_file = os.path.join(log_dir, 'app_launcher.log')

    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(log_file),
            logging.StreamHandler(sys.stdout)
        ]
    )
    return logging.getLogger(__name__)


def check_python_version():
    """
    Check if the Python version meets minimum requirements.

    Returns:
        bool: True if version is compatible, False otherwise
    """
    min_version = (3, 8, 0)
    current_version = sys.version_info

    if current_version < min_version:
        logging.error(
            f"Python version {current_version} is not supported. Minimum required: {'.'.join(map(str, min_version))}")
        return False
    return True


def create_virtual_environment():
    """
    Create a virtual environment with robust error handling.

    Returns:
        str: Path to virtual environment or None if creation fails
    """
    try:
        # Determine base directory
        base_dir = os.path.dirname(os.path.abspath(__file__))
        venv_path = os.path.join(base_dir, 'venv')

        # Remove existing venv if corrupted
        if os.path.exists(venv_path):
            shutil.rmtree(venv_path)

        # Create new virtual environment
        subprocess.run([sys.executable, '-m', 'venv', venv_path], check=True)

        logging.info(f"Virtual environment created successfully at {venv_path}")
        return venv_path

    except Exception as e:
        logging.error(f"Failed to create virtual environment: {e}")
        return None


def install_required_packages(venv_path):
    """
    Install required packages in the virtual environment.

    Args:
        venv_path (str): Path to the virtual environment

    Returns:
        bool: True if installation successful, False otherwise
    """
    try:
        # Path to Python and pip in the virtual environment
        python_path = os.path.join(venv_path, 'Scripts', 'python')
        pip_path = os.path.join(venv_path, 'Scripts', 'pip')

        # Path to requirements file
        requirements_path = os.path.join(os.path.dirname(__file__), 'requirements.txt')

        # Install requirements
        subprocess.run([pip_path, 'install', '-r', requirements_path], check=True)

        logging.info("All required packages installed successfully")
        return True

    except subprocess.CalledProcessError as e:
        logging.error(f"Package installation failed: {e}")
        return False


def check_system_compatibility():
    """
    Perform system compatibility checks.

    Returns:
        bool: True if system is compatible, False otherwise
    """
    try:
        # Check OS
        current_os = platform.system()
        if current_os != 'Windows':
            logging.error(f"Unsupported operating system: {current_os}")
            return False

        # Check available disk space (minimum 2GB)
        total, used, free = shutil.disk_usage(os.path.dirname(__file__))
        if free < 2 * (1024 ** 3):  # 2GB in bytes
            logging.error(f"Insufficient disk space: {free / (1024 ** 3):.2f} GB available")
            return False

        return True

    except Exception as e:
        logging.error(f"System compatibility check failed: {e}")
        return False


def run_application(venv_path):
    """
    Run the main application using the virtual environment.

    Args:
        venv_path (str): Path to the virtual environment

    Returns:
        bool: True if application runs successfully, False otherwise
    """
    try:
        python_path = os.path.join(venv_path, 'Scripts', 'python')
        main_script = os.path.join(os.path.dirname(__file__), 'dag_generator_app.py')

        # Run the application
        result = subprocess.run([python_path, main_script], capture_output=True, text=True)

        if result.returncode != 0:
            logging.error(f"Application failed with error: {result.stderr}")
            return False

        logging.info("Application ran successfully")
        return True

    except Exception as e:
        logging.error(f"Failed to run application: {e}")
        return False


def main():
    """Main launcher function with comprehensive error handling."""
    logger = setup_logging()

    try:
        # Perform initial checks
        if not check_python_version():
            sys.exit(1)

        if not check_system_compatibility():
            sys.exit(1)

        # Create virtual environment
        venv_path = create_virtual_environment()
        if not venv_path:
            sys.exit(1)

        # Install packages
        if not install_required_packages(venv_path):
            sys.exit(1)

        # Run application
        if not run_application(venv_path):
            sys.exit(1)

    except Exception as e:
        logger.error(f"Unexpected error in launcher: {e}")
        sys.exit(1)


if __name__ == '__main__':
    main()
