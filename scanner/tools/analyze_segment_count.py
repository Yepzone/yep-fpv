"""
分析 Session Segment 数量分布
==============================
统计历史数据中每个 session 的 segment 数量

使用示例:
  python -m scanner.analyze_segment_count
"""

import csv
import sys
from pathlib import Path
from collections import defaultdict, Counter

# 项目根目录
project_root = Path(__file__).parent.parent.parent  # scanner/tools/analyze_segment_count.py -> scanner/tools -> scanner -> 项目根目录
sys.path.insert(0, str(project_root))

EXPORTED_CSV_DIR = project_root / "ExportedCSV"


def analyze_segment_count(csv_file: str):
    """
    分析 segment 数量分布
    
    参数:
        csv_file: CSV文件名
    """
    csv_path = EXPORTED_CSV_DIR / csv_file
    
    if not csv_path.exists():
        print(f"✗ 文件不存在: {csv_path}")
        return
    
    print("=" * 80)
    print(f"Segment 数量分析")
    print("=" * 80)
    print(f"文件: {csv_file}")
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
                        'session_id': row[6],
                        '段落号': row[3],
                        '设备ID': row[2]
                    })
    
    if not rows:
        print("✗ 文件为空或格式错误")
        return
    
    print(f"✓ 读取 {len(rows)} 条记录")
    print()
    
    # 按 session_id 统计 segment 数量
    session_segments = defaultdict(lambda: {'count': 0, 'device': '', 'max_segment': -1})
    
    for row in rows:
        session_id = row.get('session_id', '')
        segment_str = row.get('段落号', '')
        device_id = row.get('设备ID', '')
        
        try:
            segment = int(segment_str)
        except ValueError:
            segment = -1
        
        session_segments[session_id]['count'] += 1
        session_segments[session_id]['device'] = device_id
        if segment > session_segments[session_id]['max_segment']:
            session_segments[session_id]['max_segment'] = segment
    
    # 统计 segment 数量分布
    segment_count_distribution = Counter()
    max_segment_distribution = Counter()
    
    for session_id, info in session_segments.items():
        segment_count_distribution[info['count']] += 1
        max_segment_distribution[info['max_segment']] += 1
    
    # 输出结果
    print("=" * 80)
    print("Segment 数量分布（每个 session 有多少条记录）")
    print("=" * 80)
    
    for count in sorted(segment_count_distribution.keys()):
        sessions = segment_count_distribution[count]
        percentage = sessions / len(session_segments) * 100
        print(f"  {count} 个 segment: {sessions} 个 session ({percentage:.1f}%)")
    
    print()
    print("=" * 80)
    print("最大 Segment 编号分布（segment 从 0 到 n）")
    print("=" * 80)
    
    for max_seg in sorted(max_segment_distribution.keys()):
        sessions = max_segment_distribution[max_seg]
        percentage = sessions / len(session_segments) * 100
        print(f"  最大 segment = {max_seg}: {sessions} 个 session ({percentage:.1f}%)")
    
    print()
    print("=" * 80)
    print("Segment 数量 > 5 的 session 详情")
    print("=" * 80)
    
    large_sessions = [(sid, info) for sid, info in session_segments.items() if info['count'] > 5]
    large_sessions.sort(key=lambda x: x[1]['count'], reverse=True)
    
    if large_sessions:
        print(f"\n找到 {len(large_sessions)} 个 session 的 segment 数量 > 5:\n")
        for session_id, info in large_sessions[:20]:  # 只显示前20个
            print(f"  {session_id} (设备: {info['device']})")
            print(f"    - Segment 数量: {info['count']}")
            print(f"    - 最大 segment: {info['max_segment']}")
    else:
        print("\n✓ 没有 session 的 segment 数量 > 5")
    
    print()
    print("=" * 80)
    print("统计总结")
    print("=" * 80)
    print(f"总 session 数: {len(session_segments)}")
    print(f"总记录数: {len(rows)}")
    print(f"平均每个 session 的 segment 数: {len(rows) / len(session_segments):.2f}")
    print(f"最多 segment 的 session: {max(session_segments.items(), key=lambda x: x[1]['count'])[1]['count']} 个")
    print(f"最大的 segment 编号: {max(session_segments.items(), key=lambda x: x[1]['max_segment'])[1]['max_segment']}")
    print("=" * 80)


def main():
    """主函数"""
    # ========================================
    # 配置区域：在这里指定要分析的文件
    # ========================================
    CSV_FILE = "大乱斗.csv"  # 历史数据
    # ========================================
    
    print()
    print("Segment 数量分析工具")
    print()
    
    analyze_segment_count(CSV_FILE)


if __name__ == "__main__":
    main()
