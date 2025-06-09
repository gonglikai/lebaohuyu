# 游戏数据ETL清洗工具

一个专为游戏事件数据设计的高效数据清洗工具。

## 项目概述

这个工具专注于游戏事件数据的基础清洗工作，包括去重、空值处理、格式标准化和数据验证。采用分块处理技术，支持处理GB级别的大文件而不会造成内存溢出。

### 主要功能
数据去重: 删除完全重复的记录和基于EventID的重复记录
空值处理: 移除关键字段为空的无效记录
格式标准化: 统一文本字段格式，转换时间戳格式
数据验证: 过滤无效的事件类型和设备类型
内存优化: 分块处理大文件，避免内存不足
性能监控: 实时显示处理进度和性能统计

## 适用场景

游戏公司的数据工程师进行日常数据清洗
数据分析师准备干净的数据集进行分析
需要处理大规模用户行为数据的团队

## 支持的数据格式

### 输入要求
CSV文件必须包含以下列：
```
EventID        - 事件唯一标识符
PlayerID       - 玩家ID
EventTimestamp - 事件时间戳 (格式: YYYY-MM-DD HH:MM:SS)
EventType      - 事件类型
EventDetails   - 事件详细信息 (可选)
DeviceType     - 设备类型
Location       - 地理位置
```

### 示例数据
```csv
EventID,PlayerID,EventTimestamp,EventType,EventDetails,DeviceType,Location
E100000,P100000,2023-01-02 06:17:11,Login,Method: Email,PC,China
E100001,P100000,2023-01-06 03:39:59,Login,Method: Email,PC,China
E100002,P100000,2023-01-06 18:51:54,InAppPurchase,"Amount: $99.99, Item: Pack10",PC,China
```

### 支持的有效值
事件类型: Login, Logout, LevelComplete, InAppPurchase, SocialInteraction, SessionStart, SessionEnd
设备类型: Android, iOS, PC
地理位置: 无限制，但建议使用标准地区名称


### 环境要求
```bash
Python >= 3.7
pandas >= 1.0.0
```

### 基本使用
按提示输入
   ```
   输入CSV文件路径 (默认: game_events_100k_180days.csv): your_data.csv
   输出CSV文件路径 (默认: cleaned_your_data.csv): cleaned_data.csv
   确认开始清洗? (y/n): y
   ```

