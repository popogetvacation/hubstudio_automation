"""
日志工具模块
"""
import logging
import os
from datetime import datetime
from typing import Optional


def setup_logger(name: str = "hubstudio_automation",
                 level: str = "INFO",
                 log_format: Optional[str] = None,
                 log_file: Optional[str] = None) -> logging.Logger:
    """
    设置日志记录器

    Args:
        name: 日志记录器名称
        level: 日志级别
        log_format: 日志格式
        log_file: 日志文件路径

    Returns:
        配置好的日志记录器
    """
    logger = logging.getLogger(name)

    # 设置日志级别
    log_level = getattr(logging, level.upper(), logging.INFO)
    logger.setLevel(log_level)

    # 清除已有的处理器
    logger.handlers.clear()

    # 默认格式
    if log_format is None:
        log_format = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"

    formatter = logging.Formatter(log_format)

    # 控制台处理器
    console_handler = logging.StreamHandler()
    console_handler.setLevel(log_level)
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)

    # 文件处理器
    if log_file:
        # 确保日志目录存在
        log_dir = os.path.dirname(log_file)
        if log_dir and not os.path.exists(log_dir):
            os.makedirs(log_dir)

        file_handler = logging.FileHandler(log_file, encoding='utf-8')
        file_handler.setLevel(log_level)
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)

    return logger


# 默认日志记录器
default_logger = setup_logger()
