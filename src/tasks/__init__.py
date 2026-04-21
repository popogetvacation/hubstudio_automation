"""
任务模块
支持按环境分组执行不同任务
"""
from .task_base import BaseTask, TaskRunner, TaskResult, TaskFactory, TaskStatus, GroupTaskResult
from .bigseller_task import BigSellerTask
from .bigseller_order_label_task import BigSellerOrderLabelTask

# 导入其他任务模块以触发注册
from . import shopee_all_order_task
from . import tiktok_order_task
from . import lazada_order_task

# 确保任务被注册到 TaskFactory
__all__ = [
    'BaseTask',
    'TaskRunner',
    'TaskResult',
    'TaskStatus',
    'TaskFactory',
    'GroupTaskResult',
    'BigSellerTask',
    'BigSellerOrderLabelTask'
]