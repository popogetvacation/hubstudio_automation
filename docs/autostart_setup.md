# 开机自动启动配置指南

## 概述

本指南说明如何配置 Windows 开机自动启动 HubStudio 和调度器。

## 快速开始

### 安装自动启动

1. **以管理员身份运行** `scripts\setup_autostart.bat`
2. 等待配置完成，看到成功提示

### 验证配置

1. 打开"任务计划程序"（Win+R 输入 `taskschd.msc`）
2. 查看任务列表中是否有：
   - `AutoStart_HubStudio`
   - `AutoStart_Scheduler`
3. 右键点击任务 → "运行"测试

### 重启测试

1. 重启计算机
2. 登录后等待约 2 分钟
3. 检查任务管理器：
   - 应该看到 `Hubstudio.exe` 进程
   - 应该看到 `python.exe` 进程（运行 run_scheduler.py）

## 启动顺序

- **10 秒后**: 启动 HubStudio
- **90 秒后**: 启动调度器（确保 HubStudio API 已就绪）

## 日志文件

- `logs\scheduler_startup.log` - 调度器启动日志
- `logs\scheduler.log` - 调度器运行日志
- `logs\automation.log` - 任务执行日志

## 故障排查

### HubStudio 未启动

1. 检查路径是否正确：`config\settings.yaml` 中的 `executable_path`
2. 手动运行 `scripts\start_hubstudio.bat` 查看错误信息
3. 确认 HubStudio 已正确安装

### 调度器未启动

1. 检查 HubStudio API 是否可用：访问 `http://127.0.0.1:6873`
2. 手动运行 `scripts\start_scheduler.bat` 查看错误信息
3. 检查 Python 环境和依赖是否正确安装

### 任务计划未执行

1. 打开"任务计划程序"查看任务状态
2. 检查"历史记录"选项卡查看执行日志
3. 确认任务触发器设置正确（登录时触发）

## 禁用自动启动

运行 `scripts\remove_autostart.bat` 移除任务计划。

## 手动管理

### 查看任务

```cmd
schtasks /Query /TN "AutoStart_HubStudio"
schtasks /Query /TN "AutoStart_Scheduler"
```

### 手动运行任务

```cmd
schtasks /Run /TN "AutoStart_HubStudio"
schtasks /Run /TN "AutoStart_Scheduler"
```

### 禁用任务

```cmd
schtasks /Change /TN "AutoStart_HubStudio" /DISABLE
schtasks /Change /TN "AutoStart_Scheduler" /DISABLE
```

### 启用任务

```cmd
schtasks /Change /TN "AutoStart_HubStudio" /ENABLE
schtasks /Change /TN "AutoStart_Scheduler" /ENABLE
```

## 技术细节

- **进程检测**: start_hubstudio.bat 会检查进程避免重复启动
- **API 健康检查**: 使用 curl 轮询确认 HubStudio API 就绪
- **重试机制**: 任务计划内置失败重试（HubStudio: 1分钟/3次，Scheduler: 2分钟/3次）
- **权限**: 需要管理员权限创建任务，但任务以当前用户身份运行
