import json
import os
from functools import lru_cache
from typing import List


@lru_cache(maxsize=1)
def get_ph_remote_keywords() -> List[str]:
    """
    从配置文件读取菲律宾偏远地区关键字列表。

    Returns:
        ['mindanao', 'visayas', 'cebu', 'iloilo', ...]
    """
    config_dir = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    config_path = os.path.join(config_dir, 'config', 'ph_remote_keywords.json')

    with open(config_path, 'r', encoding='utf-8') as f:
        data = json.load(f)

    return data.get('ph_remote_keywords', [])
