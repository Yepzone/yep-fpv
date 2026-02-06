# FPV Scanner 工具集

FPV 视频数据扫描、验证和管理工具集。

## 📁 目录结构

```
scanner/
├── scan/                    # 扫描模块 - 从 OSS 扫描数据并导入数据库
│   ├── info_scan.py        # 扫描视频文件信息（主扫描脚本）
│   ├── metadata_scan.py    # 扫描元数据
│   └── export_formatted_csv.py  # 导出格式化的 CSV 文件
│
├── validate/                # 验证模块 - 验证数据完整性，支持自动修复
│   ├── verify_session_segments.py  # 验证 session segment 规则 ⭐
│   └── verify_device_duration.py   # 验证设备时长和文件大小
│
├── tools/                   # 工具模块 - 辅助工具和诊断脚本
│   ├── diagnose_session.py        # 诊断特定 session 的 OSS 文件
│   ├── fix_front_filesize.py      # 手动修复 front 文件大小
│   ├── analyze_segment_count.py   # 分析 segment 数量分布
│   ├── add_devices.py             # 添加设备到数据库
│   └── test_devices.py            # 测试设备连接
│
└── ExportedCSV/             # CSV 导出目录
```

## 🚀 快速开始

### 1. 扫描数据

从 OSS 扫描视频文件信息并导入数据库：

```bash
# 扫描指定设备和日期范围
python -m scanner.scan.info_scan --device-id b1e0 --start-date 2026-01-19 --end-date 2026-01-19

# 扫描所有设备
python -m scanner.scan.info_scan --start-date 2026-01-19 --end-date 2026-01-19
```

### 2. 导出 CSV

从数据库导出格式化的 CSV 文件：

```bash
# 导出指定日期范围
python -m scanner.scan.export_formatted_csv --start-date 2026-01-19 --end-date 2026-01-19

# 导出所有数据
python -m scanner.scan.export_formatted_csv --all
```

### 3. 验证数据（推荐）⭐

验证数据完整性，自动检测并修复问题：

```bash
# 验证并自动修复（推荐）
python -m scanner.validate.verify_session_segments --auto-fix

# 只验证不修复
python -m scanner.validate.verify_session_segments --no-fix

# 验证指定文件
python -m scanner.validate.verify_session_segments --file formatted_xxx.csv --auto-fix
```

### 4. 验证设备时长

验证指定设备的时长和文件大小：

```bash
# 验证所有设备
python -m scanner.validate.verify_device_duration

# 验证指定设备
python -m scanner.validate.verify_device_duration --device b1e0
```

## 📋 完整工作流程

### 日常扫描和验证流程

```bash
# 1. 扫描数据（从 OSS 导入数据库）
python -m scanner.scan.info_scan --start-date 2026-01-19 --end-date 2026-01-19

# 2. 导出 CSV
python -m scanner.scan.export_formatted_csv --start-date 2026-01-19 --end-date 2026-01-19

# 3. 验证数据（自动修复问题）
python -m scanner.validate.verify_session_segments --auto-fix

# 4. 如果有修复，重新导出 CSV
python -m scanner.scan.export_formatted_csv --start-date 2026-01-19 --end-date 2026-01-19
```

### 问题诊断流程

如果发现数据异常：

```bash
# 1. 诊断特定 session
python -m scanner.tools.diagnose_session
# （需要在脚本中配置 device_id 和 session_id）

# 2. 分析 segment 数量分布
python -m scanner.tools.analyze_segment_count

# 3. 手动修复 front 文件大小
python -m scanner.tools.fix_front_filesize
# （需要在脚本中配置 device_id 和 session_id）
```

## 🔧 核心功能详解

### 扫描模块 (scan/)

#### info_scan.py - 主扫描脚本

从 OSS 扫描视频文件信息并导入数据库。

**功能：**
- 扫描指定设备和日期范围的视频文件
- 获取文件大小、路径等信息
- 自动配对 down 和 front 摄像头
- 插入或更新数据库记录

**使用示例：**
```bash
# 基本用法
python -m scanner.scan.info_scan --device-id b1e0 --start-date 2026-01-19 --end-date 2026-01-19

# 调试模式（只处理前5条）
python -m scanner.scan.info_scan --device-id b1e0 --start-date 2026-01-19 --end-date 2026-01-19 --debug
```

#### export_formatted_csv.py - 导出 CSV

从数据库导出符合目标格式的 CSV 文件。

**功能：**
- 导出指定日期范围的数据
- 自动计算时长（基于 1200MB/10分钟）
- 生成视频链接
- 包含任务描述等字段

**使用示例：**
```bash
# 导出指定日期
python -m scanner.scan.export_formatted_csv --start-date 2026-01-19 --end-date 2026-01-19

# 导出所有数据
python -m scanner.scan.export_formatted_csv --all

# 指定输出文件名
python -m scanner.scan.export_formatted_csv --start-date 2026-01-19 --end-date 2026-01-19 --output my_export.csv
```

### 验证模块 (validate/)

#### verify_session_segments.py - Session 验证（推荐）⭐

验证 session 的 segment 规则，支持自动修复。

**验证规则：**
1. Segment 从 0 开始连续递增
2. 非最后 segment 文件大小约为 1200MB (1100-1300MB)
3. 最后 segment 文件大小 ≤ 1210MB

**自动修复功能：**
- 检测到 front_file_size_bytes = 0 时，自动从 OSS 读取并更新
- 修复后提示重新导出 CSV

**使用示例：**
```bash
# 验证并自动修复（推荐）
python -m scanner.validate.verify_session_segments --auto-fix

# 只验证不修复
python -m scanner.validate.verify_session_segments --no-fix

# 验证指定文件
python -m scanner.validate.verify_session_segments --file formatted_xxx.csv --auto-fix
```

**配置方式：**

编辑脚本末尾的配置区域：
```python
CSV_FILE = "formatted_2026-01-19.csv"  # 要验证的文件
AUTO_FIX = True  # 是否自动修复
```

#### verify_device_duration.py - 设备时长验证

验证指定设备的时长和文件大小是否匹配。

**功能：**
- 验证 filesize 和时长的对应关系（1200MB/10分钟）
- 输出每个设备的总时长和总文件大小
- 支持指定设备或验证所有设备

**使用示例：**
```bash
# 验证所有设备
python -m scanner.validate.verify_device_duration

# 验证指定设备
python -m scanner.validate.verify_device_duration --device b1e0

# 验证指定文件
python -m scanner.validate.verify_device_duration --file formatted_xxx.csv --device b1e0
```

### 工具模块 (tools/)

#### diagnose_session.py - Session 诊断

诊断特定 session 的 OSS 文件，检查文件是否存在、大小是否正确。

**使用方式：**
1. 编辑脚本配置 `DEVICE_ID` 和 `SESSION_ID`
2. 运行：`python -m scanner.tools.diagnose_session`

#### fix_front_filesize.py - 手动修复

手动修复特定 session 的 front 文件大小。

**使用方式：**
1. 编辑脚本配置 `DEVICE_ID`、`SESSION_ID` 和 `DRY_RUN`
2. 运行：`python -m scanner.tools.fix_front_filesize`

#### analyze_segment_count.py - Segment 分析

分析历史数据中 segment 数量的分布情况。

**使用方式：**
1. 编辑脚本配置 `CSV_FILE`
2. 运行：`python -m scanner.tools.analyze_segment_count`

## 🐛 常见问题

### Q1: 验证发现 front 文件大小为 0

**原因：** 扫描时 OSS API 获取文件大小失败

**解决方案：**
```bash
# 方法1：使用自动修复（推荐）
python -m scanner.validate.verify_session_segments --auto-fix

# 方法2：手动修复
python -m scanner.tools.fix_front_filesize
# （需要在脚本中配置 device_id 和 session_id）

# 方法3：重新扫描
python -m scanner.scan.info_scan --device-id 6ea2 --start-date 2026-01-19 --end-date 2026-01-19
```

### Q2: Segment 不连续

**原因：** 数据库中有重复的 segment 记录

**解决方案：**
1. 检查数据库中的重复记录
2. 手动删除重复记录
3. 重新扫描该 session

### Q3: 时长和文件大小不匹配

**原因：** 
- 设备录制码率不同
- 只有单个摄像头录制
- front 文件大小为 0

**解决方案：**
```bash
# 1. 验证并自动修复
python -m scanner.validate.verify_session_segments --auto-fix

# 2. 检查设备配置
python -m scanner.validate.verify_device_duration --device <device_id>

# 3. 诊断特定 session
python -m scanner.tools.diagnose_session
```

## 📝 配置说明

### 环境变量

在项目根目录的 `.env` 文件中配置：

```env
# 数据库配置
PG_HOST=your_host
PG_DATABASE=your_database
PG_USER=your_user
PG_PASSWORD=your_password
PG_PORT=5432

# OSS 配置
OSS_ACCESS_KEY_ID=your_access_key
OSS_ACCESS_KEY_SECRET=your_secret
OSS_ENDPOINT=your_endpoint
OSS_BUCKET_NAME=your_bucket
```

### 脚本配置

大多数脚本支持在文件末尾的配置区域修改默认参数：

```python
# ========================================
# 配置区域
# ========================================
CSV_FILE = "formatted_xxx.csv"
DEVICE_FILTER = None
AUTO_FIX = True
# ========================================
```

## 🔗 相关文档

- [Lark Bot 服务](../lark_bots/README.md) - 飞书机器人集成
- [数据库结构](../DB_misc/README.md) - 数据库表结构说明

## 📄 许可证

内部项目，仅供团队使用。
