"""
任务模块
支持按环境分组执行不同任务
"""
from .task_base import BaseTask, TaskRunner, TaskResult, TaskFactory, TaskStatus, GroupTaskResult
from .bigseller_task import BigSellerTask

# 确保任务被注册到 TaskFactory
__all__ = [
    'BaseTask',
    'TaskRunner',
    'TaskResult',
    'TaskStatus',
    'TaskFactory',
    'GroupTaskResult',
    'BigSellerTask'
]