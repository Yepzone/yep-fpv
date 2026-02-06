# 目录结构迁移指南

## 📦 新目录结构

为了更好的代码组织，我们重新规划了 scanner 目录结构：

### 旧结构 → 新结构

```
scanner/
├── info_scan.py              → scanner/scan/info_scan.py
├── metadata_scan.py          → scanner/scan/metadata_scan.py
├── export_formatted_csv.py   → scanner/scan/export_formatted_csv.py
│
├── verify_session_segments.py → scanner/validate/verify_session_segments.py
├── verify_device_duration.py  → scanner/validate/verify_device_duration.py
│
├── diagnose_session.py       → scanner/tools/diagnose_session.py
├── fix_front_filesize.py     → scanner/tools/fix_front_filesize.py
├── analyze_segment_count.py  → scanner/tools/analyze_segment_count.py
├── merge_csv_columns.py      → scanner/tools/merge_csv_columns.py (已删除)
├── add_devices.py            → scanner/tools/add_devices.py
└── test_devices.py           → scanner/tools/test_devices.py
```

## 🔄 命令更新

### 扫描命令

**旧命令：**
```bash
python -m scanner.info_scan --device-id b1e0 --start-date 2026-01-19
python -m scanner.export_formatted_csv --start-date 2026-01-19
```

**新命令：**
```bash
python -m scanner.scan.info_scan --device-id b1e0 --start-date 2026-01-19
python -m scanner.scan.export_formatted_csv --start-date 2026-01-19
```

### 验证命令

**旧命令：**
```bash
python -m scanner.verify_session_segments
python -m scanner.verify_device_duration
```

**新命令：**
```bash
python -m scanner.validate.verify_session_segments --auto-fix
python -m scanner.validate.verify_device_duration
```

### 工具命令

**旧命令：**
```bash
python -m scanner.diagnose_session
python -m scanner.fix_front_filesize
```

**新命令：**
```bash
python -m scanner.tools.diagnose_session
python -m scanner.tools.fix_front_filesize
```

## 📝 脚本更新

如果你有自己的脚本或定时任务使用了旧的命令，需要更新：

### Bash 脚本

**旧的 scan_fpv.sh：**
```bash
uv run -m scanner.info_scan --device-id $DEVICE_ID --start-date $DATE
```

**新的 scan_fpv.sh：**
```bash
uv run -m scanner.scan.info_scan --device-id $DEVICE_ID --start-date $DATE
```

### Crontab

**旧配置：**
```cron
0 2 * * * cd /app && uv run -m scanner.info_scan --start-date $(date +\%Y-\%m-\%d)
```

**新配置：**
```cron
0 2 * * * cd /app && uv run -m scanner.scan.info_scan --start-date $(date +\%Y-\%m-\%d)
```

### Python 导入

**旧导入：**
```python
from scanner.info_scan import scan_device
from scanner.export_formatted_csv import export_csv
```

**新导入：**
```python
from scanner.scan.info_scan import scan_device
from scanner.scan.export_formatted_csv import export_csv
```

## ✨ 新功能

### 自动修复功能

验证脚本现在支持自动修复：

```bash
# 验证并自动修复问题
python -m scanner.validate.verify_session_segments --auto-fix

# 只验证不修复
python -m scanner.validate.verify_session_segments --no-fix
```

### 模块化导入

现在可以按模块导入：

```python
# 导入扫描模块
from scanner.scan import info_scan, export_formatted_csv

# 导入验证模块
from scanner.validate import verify_session_segments

# 导入工具模块
from scanner.tools import diagnose_session
```

## 🔍 检查清单

迁移后请检查：

- [ ] 更新所有脚本中的命令路径
- [ ] 更新 crontab 定时任务
- [ ] 更新 Docker 启动命令
- [ ] 更新文档和 README
- [ ] 测试所有功能是否正常

## 📚 相关文档

- [Scanner README](README.md) - 完整的使用文档
- [主项目 README](../README.md) - 项目概述
