"""Application configuration"""

# UI Configuration
UI_CONFIG = {
    'WINDOW_SIZE': '1024x768',
    'THEME': 'breeze',
    'FONTS': {
        'HEADER': ('Segoe UI', 24, 'bold'),
        'SUBHEADER': ('Segoe UI', 12),
        'NORMAL': ('Segoe UI', 10)
    },
    'COLORS': {
        'PRIMARY': '#3daee9',
        'SECONDARY': '#31363b',
        'BACKGROUND': '#eff0f1',
        'TEXT': '#232627',
        'ACCENT': '#27ae60'
    }
}

# File paths
PATHS = {
    'MODELS': 'models',
    'DAGS': 'dags',
    'JOBS': 'jobs',
    'CONFIG': 'config',
    'HISTORY': 'file_history.json'
} 