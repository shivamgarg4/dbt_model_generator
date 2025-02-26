"""Resource manager for lazy loading and caching"""
from functools import lru_cache
import json
import os

class ResourceManager:
    @lru_cache(maxsize=32)
    def load_json(self, path):
        with open(path, 'r') as f:
            return json.load(f)
            
    def save_json(self, path, data):
        with open(path, 'w') as f:
            json.dump(data, f, indent=4) 