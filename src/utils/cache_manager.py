import json
import hashlib
from pathlib import Path
from datetime import datetime, timedelta
from typing import Any, Optional

class CacheManager:
    def __init__(self, cache_dir: str = "data/cache"):
        self.cache_dir = Path(cache_dir)
        self.cache_dir.mkdir(parents=True, exist_ok=True)
        
    def _get_cache_key(self, key: str) -> str:
        """Generate cache filename from key"""
        return hashlib.md5(key.encode()).hexdigest()
    
    def get(self, key: str, max_age_hours: int = 24) -> Optional[Any]:
        """Get cached value if exists and not expired"""
        cache_file = self.cache_dir / f"{self._get_cache_key(key)}.json"
        
        if not cache_file.exists():
            return None
            
        try:
            with open(cache_file, 'r') as f:
                data = json.load(f)
                
            # Check expiration
            cached_time = datetime.fromisoformat(data['timestamp'])
            if datetime.now() - cached_time > timedelta(hours=max_age_hours):
                cache_file.unlink()  # Delete expired cache
                return None
                
            return data['value']
            
        except Exception:
            return None
    
    def set(self, key: str, value: Any):
        """Cache a value"""
        cache_file = self.cache_dir / f"{self._get_cache_key(key)}.json"
        
        data = {
            'timestamp': datetime.now().isoformat(),
            'key': key,
            'value': value
        }
        
        with open(cache_file, 'w') as f:
            json.dump(data, f)
    
    def clear(self):
        """Clear all cache"""
        for cache_file in self.cache_dir.glob("*.json"):
            cache_file.unlink()