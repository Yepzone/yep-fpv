"""
Session Segment 验证脚本
========================
验证每个 session 的 segment 规则：
1. segment 从 0 开始连续递增
2. 非最后一个 segment 的文件大小应约为 1200MB
3. 最后一个 segment 的文件大小应小于 1200MB

支持自动修复功能：
- 检测到 front_file_size_bytes = 0 时，自动从 OSS 读取并更新

使用示例:
  python -m scanner.validate.verify_session_segments
  python -m scanner.validate.verify_session_segments --auto-fix  # 自动修复问题
"""

import csv
import re
import sys
import os
import argparse
from pathlib import Path
from collections import defaultdict
from dotenv import load_dotenv
import oss2
import psycopg2

# 项目根目录
project_root = Path(__file__).parent.parent.parent  # scanner/validate/verify_session_segments.py -> scanner/validate -> scanner -> 项目根目录
sys.path.insert(0, str(project_root))

# 加载环境变量
load_dotenv(project_root / ".env")

EXPORTED_CSV_DIR = project_root / "ExportedCSV"


def parse_filesize(filesize_str: str) -> float:
    """解析文件大小字符串，返回 MB 数值"""
    if not filesize_str:
        return 0.0
    match = re.search(r'([\d.]+)\s*MB', filesize_str)
    if match:
        return float(match.group(1))
    return 0.0


def get_db_connection():
    """获取数据库连接"""
    try:
        conn = psycopg2.connect(
            host=os.getenv("PG_HOST"),
            database=os.getenv("PG_DATABASE"),
            user=os.getenv("PG_USER"),
            password=os.getenv("PG_PASSWORD"),
            port=os.getenv("PG_PORT", 5432)
        )
        return conn
    except Exception as e:
        print(f"✗ 无法连接到数据库: {e}")
        return None


def get_oss_bucket():
    """获取 OSS bucket 连接"""
    try:
        auth = oss2.Auth(
            os.getenv("OSS_ACCESS_KEY_ID"),
            os.getenv("OSS_ACCESS_KEY_SECRET")
        )
        bucket = oss2.Bucket(
            auth,
            os.getenv("OSS_ENDPOINT"),
            os.getenv("OSS_BUCKET_NAME")
        )
        return bucket
    except Exception as e:
        print(f"✗ 无法连接到 OSS: {e}")
        return None


def fix_session_front_filesize(device_id: str, session_id: str, bucket, conn) -> bool:
    """
    修复特定 session 的 front 文件大小
    
    返回: 是否成功修复
    """
    try:
        # 列出该 session 的所有 front 文件
        prefix = f"{device_id}/{session_id}/segments/"
        
        front_files = {}
        for obj in oss2.ObjectIterator(bucket, prefix=prefix):
            filename = obj.key.split("/")[-1]
            
            # 只处理 front mp4 文件
            if "front" in filename and filename.endswith('.mp4'):
                # 提取 segment 编号
                match = re.search(r'_(\d+)\.mp4$', filename)
                if match:
                    segment_number = match.group(1)
                    front_files[segment_number] = obj.size
        
        if not front_files:
            return False
        
        # 查询数据库中需要更新的记录
        with conn.cursor() as cur:
            cur.execute("""
                SELECT segment_number
                FROM fpv.segments
                WHERE session_id = %s AND (front_file_size_bytes = 0 OR front_file_size_bytes IS NULL)
                ORDER BY segment_number
            """, (session_id,))
            
            segments_to_fix = [row[0] for row in cur.fetchall()]
        
        if not segments_to_fix:
            return False
        
        # 执行更新
        updates = []
        for segment_number in segments_to_fix:
            segment_str = str(segment_number).zfill(4)
            if segment_str in front_files:
                updates.append((front_files[segment_str], session_id, segment_number))
        
        if updates:
            with conn.cursor() as cur:
                cur.executemany("""
                    UPDATE fpv.segments
                    SET front_file_size_bytes = %s
                    WHERE session_id = %s AND segment_number = %s
                """, updates)
                conn.commit()
            
            print(f"      ✓ 已修复 {len(updates)} 个 segment 的 front 文件大小")
            return True
        
        return False
        
    except Exception as e:
        print(f"      ✗ 修复失败: {e}")
        conn.rollback()
        return False


def verify_session_segments(csv_file: str, auto_fix: bool = False):
    """
    验证 session 的 segment 规则
    
    参数:
        csv_file: CSV文件名
    """
    csv_path = EXPORTED_CSV_DIR / csv_file
    
    if not csv_path.exists():
        print(f"✗ 文件不存在: {csv_path}")
        return
    
    print("=" * 80)
    print(f"Session Segment 验证")
    print("=" * 80)
    print(f"文件: {csv_file}")
    print(f"自动修复: {'启用' if auto_fix else '禁用'}")
    print()
    
    # 如果启用自动修复，连接数据库和 OSS
    conn = None
    bucket = None
    if auto_fix:
        conn = get_db_connection()
        bucket = get_oss_bucket()
        if not conn or not bucket:
            print("⚠️  无法连接到数据库或OSS，自动修复功能已禁用")
            auto_fix = False
        else:
            print("✓ 已连接到数据库和OSS，自动修复功能已启用")
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
    
    # 按 session_id 分组
    sessions = defaultdict(list)
    
    for row in rows:
        session_id = row.get('session_id', '')
        segment_str = row.get('段落号', '')
        filesize_str = row.get('filesize', '')
        device_id = row.get('设备ID', '')
        
        try:
            segment = int(segment_str)
        except ValueError:
            segment = -1
        
        size_mb = parse_filesize(filesize_str)
        
        sessions[session_id].append({
            'segment': segment,
            'size_mb': size_mb,
            'filesize_str': filesize_str,
            'device_id': device_id
        })
    
    print(f"✓ 共 {len(sessions)} 个 session")
    print()
    
    # 验证每个 session
    print("=" * 80)
    print("验证结果")
    print("=" * 80)
    print()
    
    # 统计
    total_sessions = len(sessions)
    valid_sessions = 0
    invalid_sessions = 0
    
    # 问题分类
    issues = {
        'segment_gap': [],      # segment 不连续
        'segment_order': [],    # segment 顺序错误
        'size_too_small': [],   # 非最后 segment 文件太小
        'last_too_large': [],   # 最后 segment 文件太大
    }
    
    # 文件大小阈值
    NORMAL_SIZE_MIN = 1100  # MB，非最后 segment 的最小值（重点检查是否远小于1200）
    NORMAL_SIZE_MAX = 1300  # MB，非最后 segment 的最大值
    LAST_SIZE_MAX = 1210    # MB，最后 segment 的最大值（允许多一点点）
    
    for session_id, segments in sorted(sessions.items()):
        # 按 segment 排序
        segments.sort(key=lambda x: x['segment'])
        
        device_id = segments[0]['device_id']
        has_issue = False
        session_issues = []
        
        # 验证1: segment 应该从 0 开始连续递增
        expected_segments = list(range(len(segments)))
        actual_segments = [s['segment'] for s in segments]
        
        if actual_segments != expected_segments:
            has_issue = True
            if actual_segments[0] != 0:
                session_issues.append(f"segment 不从 0 开始（实际: {actual_segments[0]}）")
                issues['segment_order'].append(session_id)
            else:
                # 检查是否有间隔
                for i in range(len(actual_segments) - 1):
                    if actual_segments[i+1] != actual_segments[i] + 1:
                        session_issues.append(f"segment 不连续: {actual_segments[i]} -> {actual_segments[i+1]}")
                        issues['segment_gap'].append(session_id)
                        break
        
        # 验证2: 非最后一个 segment 应该约为 1200MB（重点检查）
        for i, seg in enumerate(segments[:-1]):  # 除了最后一个
            if seg['size_mb'] < NORMAL_SIZE_MIN:
                has_issue = True
                
                # 检查是否是 front_file_size = 0 的问题
                if auto_fix and seg['size_mb'] > 0 and seg['size_mb'] < 700:
                    # 可能是只有 down 文件大小，front 为 0
                    print(f"   🔧 检测到可能的 front 文件大小缺失，尝试修复...")
                    if fix_session_front_filesize(device_id, session_id, bucket, conn):
                        session_issues.append(f"segment {seg['segment']} 文件太小: {seg['filesize_str']} (已尝试修复)")
                    else:
                        session_issues.append(f"⚠️ segment {seg['segment']} 文件太小: {seg['filesize_str']} (修复失败)")
                else:
                    session_issues.append(f"⚠️ segment {seg['segment']} 文件太小: {seg['filesize_str']} (应约 1200MB)")
                
                if session_id not in issues['size_too_small']:
                    issues['size_too_small'].append(session_id)
            elif seg['size_mb'] > NORMAL_SIZE_MAX:
                has_issue = True
                session_issues.append(f"segment {seg['segment']} 文件过大: {seg['filesize_str']} (应约 1200MB)")
                if session_id not in issues['size_too_small']:
                    issues['size_too_small'].append(session_id)
        
        # 验证3: 最后一个 segment 应该小于 1200MB
        if len(segments) > 0:
            last_seg = segments[-1]
            if last_seg['size_mb'] >= LAST_SIZE_MAX:
                has_issue = True
                session_issues.append(f"最后 segment {last_seg['segment']} 文件过大: {last_seg['filesize_str']} (应 < 1200MB)")
                issues['last_too_large'].append(session_id)
        
        # 输出结果
        if has_issue:
            invalid_sessions += 1
            print(f"❌ {session_id} (设备: {device_id})")
            print(f"   Segments: {len(segments)} 个 {actual_segments}")
            for issue in session_issues:
                print(f"   - {issue}")
            
            # 显示所有 segment 的大小
            print(f"   文件大小:")
            for seg in segments:
                marker = "✓" if seg == segments[-1] and seg['size_mb'] < LAST_SIZE_MAX else \
                         "✓" if seg != segments[-1] and NORMAL_SIZE_MIN <= seg['size_mb'] <= NORMAL_SIZE_MAX else "✗"
                print(f"     {marker} segment {seg['segment']}: {seg['filesize_str']}")
            print()
        else:
            valid_sessions += 1
    
    # 输出统计
    print("=" * 80)
    print("统计结果")
    print("=" * 80)
    print(f"总 session 数:     {total_sessions}")
    print(f"✓ 验证通过:        {valid_sessions} ({valid_sessions/total_sessions*100:.1f}%)")
    print(f"✗ 验证失败:        {invalid_sessions} ({invalid_sessions/total_sessions*100:.1f}%)")
    print()
    
    if invalid_sessions > 0:
        print("问题分类:")
        if issues['segment_order']:
            print(f"  - Segment 顺序错误: {len(issues['segment_order'])} 个")
        if issues['segment_gap']:
            print(f"  - Segment 不连续:   {len(issues['segment_gap'])} 个")
        if issues['size_too_small']:
            print(f"  - ⚠️ 非最后segment文件大小异常: {len(issues['size_too_small'])} 个 (重点关注)")
        if issues['last_too_large']:
            print(f"  - 最后segment文件过大: {len(issues['last_too_large'])} 个")
    
    print()
    print("=" * 80)
    print("结论:")
    print("=" * 80)
    
    if invalid_sessions == 0:
        print("✅ 所有 session 的 segment 规则验证通过！")
        print("   - 所有 segment 从 0 开始连续递增")
        print("   - 非最后 segment 文件大小约为 1200MB (1100-1300MB)")
        print("   - 最后 segment 文件大小 ≤ 1210MB")
    else:
        print(f"⚠️  发现 {invalid_sessions} 个 session 存在问题")
        if issues['size_too_small']:
            print(f"\n   🔍 重点关注：{len(issues['size_too_small'])} 个 session 的非最后segment文件大小异常")
            print("      这些segment应该约为1200MB，但实际远小于或远大于此值")
            if auto_fix:
                print("      已尝试自动修复，建议重新导出CSV验证结果")
        print("\n   请检查上述标记为 ❌ 的 session")
    
    print("=" * 80)
    
    # 清理连接
    if conn:
        conn.close()


def main():
    """主函数"""
    # ========================================
    # 配置区域：在这里指定要验证的文件
    # ========================================
    CSV_FILE = "formatted_2026-01-19_2026-01-19_20260122_144132.csv"  # 修复后的文件
    AUTO_FIX = True  # 是否自动修复问题（改为 False 禁用自动修复）
    # ========================================
    
    # 支持命令行参数
    parser = argparse.ArgumentParser(description="验证 Session Segment 规则")
    parser.add_argument("--file", "-f", help="CSV文件名")
    parser.add_argument("--auto-fix", action="store_true", help="启用自动修复")
    parser.add_argument("--no-fix", action="store_true", help="禁用自动修复")
    args = parser.parse_args()
    
    # 命令行参数优先
    csv_file = args.file or CSV_FILE
    if args.auto_fix:
        auto_fix = True
    elif args.no_fix:
        auto_fix = False
    else:
        auto_fix = AUTO_FIX
    
    print()
    print("Session Segment 验证工具")
    print()
    
    verify_session_segments(csv_file, auto_fix)


if __name__ == "__main__":
    main()
