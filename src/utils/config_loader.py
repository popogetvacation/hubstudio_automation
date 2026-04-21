"""
配置加载工具
"""
import yaml
import os


def load_label_mapping() -> dict:
    """
    加载标签名到ID的映射

    Returns:
        标签名到ID的映射字典
        {
            "同单多件": "1001",
            "高频复购": "1002",
            ...
        }
    """
    config_path = os.path.join(
        os.path.dirname(os.path.dirname(os.path.dirname(__file__))),
        'config',
        'bigseller_label_mapping.yaml'
    )

    if not os.path.exists(config_path):
        return {}

    with open(config_path, 'r', encoding='utf-8') as f:
        config = yaml.safe_load(f)

    return config.get('label_mapping', {})


def get_config_dir() -> str:
    """
    获取配置文件所在目录

    Returns:
        配置文件目录路径
    """
    return os.path.join(
        os.path.dirname(os.path.dirname(os.path.dirname(__file__))),
        'config'
    )
