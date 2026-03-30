"""
HubStudio 自动化框架
"""
from .api import HubStudioClient, BrowserInfo, EnvironmentInfo
from .browser import (
    EnvironmentManager,
    EnvironmentStatus,
    HubStudioSeleniumDriver
)
from .scheduler import ConcurrentScheduler, TaskStatus, TaskResult
from .network import BrowserRequest
from .database import AccessDatabase

__version__ = '1.0.0'

__all__ = [
    'HubStudioClient',
    'BrowserInfo',
    'EnvironmentInfo',
    'EnvironmentManager',
    'EnvironmentStatus',
    'HubStudioSeleniumDriver',
    'ConcurrentScheduler',
    'TaskStatus',
    'TaskResult',
    'BrowserRequest',
    'AccessDatabase'
]
