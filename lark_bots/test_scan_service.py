"""
测试扫库命令解析
"""

import re
from dataclasses import dataclass
from datetime import datetime, date
from typing import Optional


@dataclass
class ScanCommand:
    """扫库命令参数"""
    device_id: str
    start_date: date
    end_date: date
    
    def __str__(self):
        return f"设备={self.device_id}, 日期={self.start_date}~{self.end_date}"


def parse_scan_command(text: str) -> Optional[ScanCommand]:
    """解析扫库命令消息"""
    text = text.strip()
    
    if not text.lower().startswith("/scan"):
        return None
    
    args_text = text[5:].strip()
    
    if not args_text:
        return None
    
    # 尝试解析 --key value 格式
    device_match = re.search(r'--device[=\s]+(\S+)', args_text)
    start_match = re.search(r'--start[=\s]+(\d{4}-\d{2}-\d{2})', args_text)
    end_match = re.search(r'--end[=\s]+(\d{4}-\d{2}-\d{2})', args_text)
    
    if device_match:
        device_id = device_match.group(1)
        try:
            start_date = datetime.strptime(start_match.group(1), "%Y-%m-%d").date() if start_match else date.today()
            end_date = datetime.strptime(end_match.group(1), "%Y-%m-%d").date() if end_match else start_date
            return ScanCommand(device_id=device_id, start_date=start_date, end_date=end_date)
        except ValueError:
            return None
    
    # 尝试解析位置参数格式
    parts = args_text.split()
    
    if len(parts) < 1:
        return None
    
    device_id = parts[0]
    
    # 验证 device_id (应该是数字或简短的字母数字组合，不应该包含 "date" 等关键词)
    if not re.match(r'^[\w-]+$', device_id) or 'date' in device_id.lower():
        return None
    
    try:
        if len(parts) == 1:
            start_date = date.today()
            end_date = date.today()
        elif len(parts) == 2:
            start_date = datetime.strptime(parts[1], "%Y-%m-%d").date()
            end_date = start_date
        else:
            start_date = datetime.strptime(parts[1], "%Y-%m-%d").date()
            end_date = datetime.strptime(parts[2], "%Y-%m-%d").date()
        
        return ScanCommand(device_id=device_id, start_date=start_date, end_date=end_date)
    
    except ValueError:
        return None


def test_parse_scan_command():
    """测试命令解析"""
    
    # 测试用例
    test_cases = [
        # (输入, 期望结果)
        ("/scan 7393 2025-01-15", ScanCommand("7393", date(2025, 1, 15), date(2025, 1, 15))),
        ("/scan 7393 2025-01-01 2025-01-15", ScanCommand("7393", date(2025, 1, 1), date(2025, 1, 15))),
        ("/SCAN 7393 2025-01-15", ScanCommand("7393", date(2025, 1, 15), date(2025, 1, 15))),  # 大写
        ("/scan --device 7393 --start 2025-01-01 --end 2025-01-15", ScanCommand("7393", date(2025, 1, 1), date(2025, 1, 15))),
        ("/scan --device=7393 --start=2025-01-01", ScanCommand("7393", date(2025, 1, 1), date(2025, 1, 1))),
        
        # 无效命令
        ("hello", None),
        ("/scan", None),
        ("/scan invalid-date", None),
        ("/scan 7393 not-a-date", None),
    ]
    
    print("=" * 60)
    print("测试扫库命令解析")
    print("=" * 60)
    
    passed = 0
    failed = 0
    
    for input_text, expected in test_cases:
        result = parse_scan_command(input_text)
        
        # 比较结果
        if expected is None:
            success = result is None
        else:
            success = (
                result is not None and
                result.device_id == expected.device_id and
                result.start_date == expected.start_date and
                result.end_date == expected.end_date
            )
        
        status = "✓" if success else "✗"
        print(f"\n{status} 输入: {input_text!r}")
        print(f"  期望: {expected}")
        print(f"  结果: {result}")
        
        if success:
            passed += 1
        else:
            failed += 1
    
    print("\n" + "=" * 60)
    print(f"测试结果: {passed} 通过, {failed} 失败")
    print("=" * 60)
    
    return failed == 0


if __name__ == "__main__":
    import sys
    success = test_parse_scan_command()
    sys.exit(0 if success else 1)
