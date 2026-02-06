"""
设备时长验证脚本
================
验证指定设备的文件大小和时长是否匹配，并输出总时长和总大小

使用示例:
  python -m scanner.validate.verify_device_duration
  python -m scanner.validate.verify_device_duration --file formatted_xxx.csv --device b1e0
  python -m scanner.validate.verify_device_duration --file formatted_xxx.csv --device all
"""

import argparse
import csv
import re
import sys
from pathlib import Path
from collections import defaultdict

# 项目根目录
project_root = Path(__file__).parent.parent.parent  # scanner/validate/verify_device_duration.py -> scanner/validate -> scanner -> 项目根目录
sys.path.insert(0, str(project_root))

EXPORTED_CSV_DIR = project_root / "ExportedCSV"


def parse_filesize(filesize_str: str) -> float:
    """解析文件大小字符串，返回 MB 数值"""
    if not filesize_str:
        return 0.0
    match = re.search(r'([\d.]+)\s*MB', filesize_str)
    if match:
        return float(match.group(1))
    return 0.0


def verify_device_duration(csv_file: str, device_filter: str = None):
    """
    验证设备的时长和文件大小
    
    参数:
        csv_file: CSV文件名
        device_filter: 设备ID过滤（None表示所有设备）
    """
    csv_path = EXPORTED_CSV_DIR / csv_file
    
    if not csv_path.exists():
        print(f"✗ 文件不存在: {csv_path}")
        return
    
    print("=" * 80)
    print(f"设备时长验证")
    print("=" * 80)
    print(f"文件: {csv_file}")
    print(f"设备过滤: {device_filter if device_filter else '全部设备'}")
    print()
    
    # 读取CSV文件
    with open(csv_path, 'r', encoding='utf-8') as f:
        # 检测是否有表头
        first_line = f.readline()
        f.seek(0)
        
        has_header = '采集日期' in first_line or 'session_id' in first_line
        
        if has_header:
            reader = csv.DictReader(f)
            rows = list(reader)
        else:
            # 无表头，手动构建字典
            reader = csv.reader(f)
            rows = []
            for row in reader:
                if len(row) >= 9:
                    rows.append({
                        '采集日期': row[0],
                        '采集时间': row[1],
                        '设备ID': row[2],
                        '段落号': row[3],
                        '向下镜头视频链接': row[4],
                        '向前镜头视频链接': row[5],
                        'session_id': row[6],
                        'filesize': row[7],
                        '时长': row[8]
                    })
    
    if not rows:
        print("✗ 文件为空或格式错误")
        return
    
    print(f"✓ 读取 {len(rows)} 条记录")
    print()
    
    # 按设备分组统计
    device_stats = defaultdict(lambda: {
        'count': 0,
        'total_size_mb': 0.0,
        'total_duration_min': 0,
        'match_count': 0,
        'mismatch_count': 0,
        'mismatches': []
    })
    
    # 时长误差容忍度（分钟）
    DURATION_TOLERANCE = 1
    
    # 处理每一行
    for idx, row in enumerate(rows, start=1):
        device_id = row.get('设备ID', '')
        filesize_str = row.get('filesize', '')
        duration_str = row.get('时长', '')
        session_id = row.get('session_id', '')
        segment = row.get('段落号', '')
        
        # 设备过滤
        if device_filter and device_filter.lower() != 'all' and device_id != device_filter:
            continue
        
        # 解析数据
        size_mb = parse_filesize(filesize_str)
        try:
            duration_min = int(duration_str) if duration_str else 0
        except ValueError:
            duration_min = 0
        
        # 计算预期时长（基于 1200MB/10分钟）
        expected_duration = round((size_mb / 1200.0) * 10)
        duration_diff = abs(expected_duration - duration_min)
        
        # 统计
        stats = device_stats[device_id]
        stats['count'] += 1
        stats['total_size_mb'] += size_mb
        stats['total_duration_min'] += duration_min
        
        if duration_diff <= DURATION_TOLERANCE:
            stats['match_count'] += 1
        else:
            stats['mismatch_count'] += 1
            if len(stats['mismatches']) < 5:  # 只保存前5个不匹配示例
                stats['mismatches'].append({
                    'row': idx,
                    'session_id': session_id,
                    'segment': segment,
                    'filesize': filesize_str,
                    'actual_duration': duration_min,
                    'expected_duration': expected_duration,
                    'diff': duration_diff
                })
    
    # 输出结果
    if not device_stats:
        print(f"⚠ 未找到设备 '{device_filter}' 的数据")
        return
    
    # 按设备ID排序
    sorted_devices = sorted(device_stats.items())
    
    print("=" * 80)
    print("验证结果")
    print("=" * 80)
    print()
    
    for device_id, stats in sorted_devices:
        print(f"【设备: {device_id}】")
        print(f"  记录数:       {stats['count']} 条")
        print(f"  总文件大小:   {stats['total_size_mb']:.2f} MB ({stats['total_size_mb']/1024:.2f} GB)")
        print(f"  总时长:       {stats['total_duration_min']} 分钟 ({stats['total_duration_min']/60:.2f} 小时)")
        print()
        
        # 验证结果
        match_rate = (stats['match_count'] / stats['count'] * 100) if stats['count'] > 0 else 0
        print(f"  验证结果:")
        print(f"    ✓ 匹配:     {stats['match_count']} 条 ({match_rate:.1f}%)")
        print(f"    ✗ 不匹配:   {stats['mismatch_count']} 条 ({100-match_rate:.1f}%)")
        
        # 显示不匹配示例
        if stats['mismatches']:
            print(f"\n  不匹配示例（前{len(stats['mismatches'])}条）:")
            for i, mismatch in enumerate(stats['mismatches'], 1):
                print(f"    {i}. 行{mismatch['row']}: {mismatch['session_id']} 段{mismatch['segment']}")
                print(f"       文件大小: {mismatch['filesize']}")
                print(f"       实际时长: {mismatch['actual_duration']} 分钟")
                print(f"       预期时长: {mismatch['expected_duration']} 分钟 (误差: {mismatch['diff']} 分钟)")
        
        print()
        print("-" * 80)
        print()
    
    # 总计
    if len(device_stats) > 1:
        total_count = sum(s['count'] for s in device_stats.values())
        total_size = sum(s['total_size_mb'] for s in device_stats.values())
        total_duration = sum(s['total_duration_min'] for s in device_stats.values())
        total_match = sum(s['match_count'] for s in device_stats.values())
        total_mismatch = sum(s['mismatch_count'] for s in device_stats.values())
        
        print("=" * 80)
        print("【总计】")
        print("=" * 80)
        print(f"  设备数:       {len(device_stats)} 个")
        print(f"  记录数:       {total_count} 条")
        print(f"  总文件大小:   {total_size:.2f} MB ({total_size/1024:.2f} GB)")
        print(f"  总时长:       {total_duration} 分钟 ({total_duration/60:.2f} 小时)")
        print()
        
        match_rate = (total_match / total_count * 100) if total_count > 0 else 0
        print(f"  验证结果:")
        print(f"    ✓ 匹配:     {total_match} 条 ({match_rate:.1f}%)")
        print(f"    ✗ 不匹配:   {total_mismatch} 条 ({100-match_rate:.1f}%)")
        print()
    
    # 结论
    print("=" * 80)
    print("结论:")
    print("=" * 80)
    
    all_match = all(s['mismatch_count'] == 0 for s in device_stats.values())
    if all_match:
        print("✅ 所有设备的文件大小和时长完全匹配！")
    else:
        print("⚠️  部分设备存在文件大小和时长不匹配的情况")
        print("   建议检查不匹配的记录")
    
    print("=" * 80)


def parse_args():
    parser = argparse.ArgumentParser(
        description="验证设备的文件大小和时长",
        formatter_class=argparse.RawDescriptionHelpFormatter
    )
    
    parser.add_argument(
        "--file", "-f",
        dest="csv_file",
        help="CSV文件名（在ExportedCSV目录下）"
    )
    parser.add_argument(
        "--device", "-d",
        dest="device",
        help="设备ID（不指定则验证所有设备，使用 'all' 也表示所有设备）"
    )
    
    return parser.parse_args()


def main():
    """主函数"""
    # ========================================
    # 配置区域：在这里指定要验证的文件和设备
    # ========================================
    CSV_FILE = "formatted_2026-01-18_2026-01-20_20260122_121420.csv"  # 要验证的CSV文件名
    DEVICE_FILTER = None  # 设备过滤：None=所有设备, "b1e0"=指定设备, "all"=所有设备
    # ========================================
    
    args = parse_args()
    
    # 使用命令行参数或默认值
    csv_file = args.csv_file or CSV_FILE
    device = args.device or DEVICE_FILTER
    
    print()
    print("设备时长验证工具")
    print()
    
    verify_device_duration(csv_file, device)


if __name__ == "__main__":
    main()
