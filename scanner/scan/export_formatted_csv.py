"""
导出格式化CSV脚本
================
从数据库导出符合目标格式的CSV文件，支持三种格式：
1. raw (原始云格式) - OSS路径，带updated_at
2. internal (奥特内部采集) - HTTP视频链接，简洁格式
3. scale (规模采集重启) - 完整QA列，用于规模采集项目

使用示例:
  uv run -m scanner.scan.export_formatted_csv --start-date 2026-01-12 --end-date 2026-01-13
  uv run -m scanner.scan.export_formatted_csv --format scale --all
"""

import argparse
import csv
import logging
import os
import sys
from datetime import datetime, date
from pathlib import Path

project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

import psycopg2
from psycopg2 import extras
from dotenv import load_dotenv

load_dotenv(project_root / ".env")

EXPORTED_CSV_DIR = project_root / "ExportedCSV"
VIDEO_BASE_URL = "http://localhost:8082/faster?path="

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)


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
        logging.info(f"✓ 成功连接到数据库: {os.getenv('PG_DATABASE')}@{os.getenv('PG_HOST')}")
        return conn
    except Exception as e:
        logging.error(f"✗ 无法连接到数据库: {e}")
        raise e


def build_video_url(device_id: str, session_id: str, segment_number: str, camera_type: str) -> str:
    """构建HTTP视频链接"""
    seg_num = segment_number.zfill(4)
    return f"{VIDEO_BASE_URL}{device_id}/{session_id}/segments/{device_id}-{camera_type}_sbs_{seg_num}.mp4"


def translate_task_description(task_desc: str) -> str:
    """
    将英文任务描述翻译为中文
    """
    if not task_desc or not task_desc.strip():
        return ""
    
    # 官方映射表
    task_translation_map = {
        "fold clothes": "叠衣服",
        "clear the table": "收拾桌面",
        "organize books": "书本收纳",
        "lace shoes": "穿鞋带",
        "organize shoe cabinet": "整理鞋柜",
        "organize medicine cabinet": "药箱收纳",
        "arrange dishes and utensils": "整理碗筷",
        "wipe dishes with a cloth": "用抹布擦拭碗盘",
        "organize toiletries": "整理洗漱用品",
        "organize documents": "整理文件",
        "organize snacks, condiments, or toys": "整理零食、调料或玩具",
        "install batteries": "电池安装",
        
        # 兼容旧格式
        "folding clothes": "叠衣服",
        "folding": "叠衣服",
        "cleaning dishes": "整理碗筷",
        "desk organizing": "收拾桌面",
    }
    
    task_lower = task_desc.lower().strip()
    
    # 精确匹配
    if task_lower in task_translation_map:
        return task_translation_map[task_lower]
    
    # 模糊匹配
    for english_key, chinese_value in task_translation_map.items():
        if english_key in task_lower or task_lower in english_key:
            return chinese_value
    
    # 没匹配到返回原文
    return task_desc


# 默认审批人列表
DEFAULT_APPROVERS = ["邹子扬", "谢文敏", "吴镔", "向伟", "向文杰", "王炜龙", "李虹霖", "刘涛萌", "张浩春"]


def prompt_approvers_config() -> dict:
    """
    交互式询问用户本次导出的审批人分配（数字选择方式）
    返回: {审批人名: 权重} 字典
    """
    print("\n" + "=" * 60)
    print("审批人分配配置")
    print("=" * 60)
    print("可选审批人:")
    for i, name in enumerate(DEFAULT_APPROVERS, 1):
        print(f"  {i}. {name}")
    print(f"  0. 跳过分配")
    print("-" * 60)
    
    while True:
        user_input = input("请选择审批人 (多选用逗号分隔，如 1,8,9): ").strip()
        
        if not user_input or user_input == "0":
            print("⚠ 跳过审批人分配，审批人列将为空")
            return {}
        
        try:
            selected = {}
            indices = [x.strip() for x in user_input.split(",")]
            
            for idx_str in indices:
                idx = int(idx_str)
                if idx < 1 or idx > len(DEFAULT_APPROVERS):
                    print(f"✗ 无效选项: {idx}")
                    continue
                name = DEFAULT_APPROVERS[idx - 1]
                selected[name] = 1  # 默认权重1
            
            if not selected:
                print("⚠ 未选择有效的审批人，请重新输入")
                continue
            
            # 询问是否需要自定义权重
            print(f"\n已选择: {', '.join(selected.keys())}")
            custom = input("是否自定义权重? (y/N): ").strip().lower()
            
            if custom in ['y', 'yes']:
                print("请为每人输入权重 (直接回车默认为1):")
                for name in selected.keys():
                    w = input(f"  {name} 的权重: ").strip()
                    if w:
                        selected[name] = int(w)
            
            # 确认
            total_weight = sum(selected.values())
            print(f"\n最终分配方案:")
            for name, weight in selected.items():
                pct = (weight / total_weight * 100) if total_weight > 0 else 0
                print(f"  - {name}: 权重 {weight} ({pct:.1f}%)")
            
            confirm = input("\n确认? (Y/n): ").strip().lower()
            if confirm in ['', 'y', 'yes']:
                return selected
            else:
                print("请重新选择\n")
                
        except ValueError as e:
            print(f"✗ 输入格式错误，请输入数字")
            print("示例: 1,8,9\n")


def assign_approvers(num_rows: int, approvers_config: dict = None) -> list:
    """
    按权重随机分配审批人
    
    Args:
        num_rows: 需要分配的行数
        approvers_config: {审批人名: 权重} 字典，如果为空则返回空列表
    """
    import random
    
    if not approvers_config:
        return [""] * num_rows
    
    approvers_list = list(approvers_config.keys())
    weights_list = list(approvers_config.values())
    total_weight = sum(weights_list)
    
    # 计算每人分配数量
    exact_counts = [num_rows * w / total_weight for w in weights_list]
    assigned_counts = [int(count) for count in exact_counts]
    
    # 处理余数
    remainders = [exact - assigned for exact, assigned in zip(exact_counts, assigned_counts)]
    remaining_slots = num_rows - sum(assigned_counts)
    
    if remaining_slots > 0:
        indices_sorted = sorted(range(len(remainders)), key=lambda i: remainders[i], reverse=True)
        for i in range(remaining_slots):
            assigned_counts[indices_sorted[i]] += 1
    
    # 生成随机序列
    assignment_list = []
    for name, count in zip(approvers_list, assigned_counts):
        assignment_list.extend([name] * count)
    
    random.shuffle(assignment_list)
    return assignment_list


def build_oss_path(bucket_name: str, device_id: str, session_id: str, segment_number: str, camera_type: str) -> str:
    """构建OSS路径"""
    seg_num = segment_number.zfill(4)
    return f"oss://{bucket_name}/{device_id}/{session_id}/segments/{device_id}-{camera_type}_sbs_{seg_num}.mp4"


# ==============================================================================
# 格式定义
# ==============================================================================

FORMAT_RAW = "raw"           # 原始云格式
FORMAT_INTERNAL = "internal" # 奥特内部采集
FORMAT_SCALE = "scale"       # 规模采集重启

FORMAT_NAMES = {
    FORMAT_RAW: "原始云格式",
    FORMAT_INTERNAL: "奥特内部采集", 
    FORMAT_SCALE: "规模采集重启",
}

# 各格式的列定义
COLUMNS_RAW = [
    "updated_at", "date", "time", "device_id", "segment_number",
    "down_oss_path", "front_oss_path", "session_id", 
    "down_file_size_bytes", "front_file_size_bytes"
]

COLUMNS_INTERNAL = [
    "采集日期", "采集时间", "设备ID", "段落号",
    "向下镜头视频链接", "向前镜头视频链接",
    "session_id", "filesize", "时长",
    "审批人", "任务描述", "操作员姿态", "头部移动",
    "向下摄像头手部位置", "数据状态", "时间标注", "NOTE"
]

COLUMNS_SCALE = [
    "采集日期", "采集时间", "设备ID", "段落序号",
    "向下镜头视频链接", "向前镜头视频链接",
    "session_id", "filesize", "原始上送时长", "任务描述",
    "运营端不合格时长", "算法端可用数据时长",
    "审批人", "审批状态", "数据状态", "不合格时间标注", "不合格时长", "NOTE",
    "操作员姿态", "头部移动", "向下摄像头手部位置", "其余数据标签", "LET PT"
]


def export_csv(
    conn,
    output_filename: str,
    export_format: str,
    start_date: date = None,
    end_date: date = None,
    export_all: bool = False,
    time_adjust: bool = False,
    approvers_config: dict = None
) -> int:
    """导出CSV文件
    
    Args:
        time_adjust: 是否对时间+8小时（b852设备除外）
        approvers_config: 审批人分配配置 {姓名: 权重}
    """
    EXPORTED_CSV_DIR.mkdir(exist_ok=True)
    output_path = EXPORTED_CSV_DIR / output_filename

    query = """
        SELECT 
            seg.updated_at,
            sess.collect_date,
            sess.collect_time,
            sess.device_id,
            seg.segment_number,
            sess.session_id,
            seg.down_file_size_bytes,
            seg.front_file_size_bytes,
            sess.task_description,
            dev.mb_per_10min
        FROM fpv.segments seg
        JOIN fpv.sessions sess ON seg.session_id = sess.session_id
        JOIN fpv.devices dev ON sess.device_id = dev.device_id
    """
    
    params = []
    conditions = []
    
    if not export_all:
        if start_date:
            conditions.append("sess.collect_date >= %s")
            params.append(start_date)
        if end_date:
            conditions.append("sess.collect_date <= %s")
            params.append(end_date)
    
    if conditions:
        query += " WHERE " + " AND ".join(conditions)
    
    query += " ORDER BY sess.collect_date DESC, sess.collect_time DESC, seg.segment_number"

    logging.info("正在查询数据库...")
    
    try:
        with conn.cursor(cursor_factory=extras.RealDictCursor) as cur:
            cur.execute(query, params)
            rows = cur.fetchall()
            
            if not rows:
                logging.warning("⚠ 没有查询到数据")
                return 0
            
            logging.info(f"✓ 查询到 {len(rows)} 条记录")
            
            # 根据格式选择列
            if export_format == FORMAT_RAW:
                fieldnames = COLUMNS_RAW
            elif export_format == FORMAT_SCALE:
                fieldnames = COLUMNS_SCALE
            else:
                fieldnames = COLUMNS_INTERNAL
            
            logging.info(f"正在写入CSV ({FORMAT_NAMES[export_format]}): {output_path}")
            
            bucket_name = os.getenv("OSS_BUCKET_NAME", "we-fpv-sh-new")
            
            # 规模采集格式：预先过滤并分配审批人
            valid_rows = rows
            approver_list = []
            if export_format == FORMAT_SCALE:
                # 先过滤时长为0的
                valid_rows = []
                for row in rows:
                    down_size = row['down_file_size_bytes'] or 0
                    front_size = row['front_file_size_bytes'] or 0
                    total_size_mb = (down_size + front_size) / (1024 * 1024)
                    duration = round((total_size_mb / 1200.0) * 10)
                    if duration > 0:
                        valid_rows.append(row)
                
                logging.info(f"  过滤后有效记录: {len(valid_rows)} 条 (过滤掉 {len(rows) - len(valid_rows)} 条时长为0的)")
                
                # 分配审批人
                if valid_rows:
                    approver_list = assign_approvers(len(valid_rows), approvers_config)
            
            with open(output_path, 'w', newline='', encoding='utf-8') as csvfile:
                writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
                writer.writeheader()
                
                row_idx = 0
                for row in valid_rows:
                    device_id = row['device_id']
                    session_id = row['session_id']
                    segment_number = row['segment_number']
                    
                    # 计算文件大小和时长
                    down_size = row['down_file_size_bytes'] or 0
                    front_size = row['front_file_size_bytes'] or 0
                    total_size_mb = (down_size + front_size) / (1024 * 1024)
                    filesize_str = f"{total_size_mb:.2f} MB"
                    mb_per_10min = 1200.0
                    duration = round((total_size_mb / mb_per_10min) * 10)
                    
                    # 时间处理
                    collect_date = row['collect_date']
                    collect_time = row['collect_time']
                    
                    # 规模采集格式：根据选项+8小时（b852除外）
                    if export_format == FORMAT_SCALE and time_adjust and device_id != 'b852':
                        from datetime import timedelta
                        dt = datetime.combine(collect_date, collect_time)
                        dt_adjusted = dt + timedelta(hours=8)
                        collect_date = dt_adjusted.date()
                        collect_time = dt_adjusted.time()
                    
                    if export_format == FORMAT_RAW:
                        # 原始云格式 - 原汁原味，不做任何处理
                        csv_row = {
                            "updated_at": str(row['updated_at']),
                            "date": str(row['collect_date']),
                            "time": str(row['collect_time']),
                            "device_id": device_id,
                            "segment_number": segment_number.zfill(4),
                            "down_oss_path": build_oss_path(bucket_name, device_id, session_id, segment_number, "down"),
                            "front_oss_path": build_oss_path(bucket_name, device_id, session_id, segment_number, "front"),
                            "session_id": session_id,
                            "down_file_size_bytes": row['down_file_size_bytes'] or 0,
                            "front_file_size_bytes": row['front_file_size_bytes'] or 0,
                        }
                    elif export_format == FORMAT_SCALE:
                        # 规模采集重启格式
                        approver = approver_list[row_idx] if row_idx < len(approver_list) else ""
                        row_idx += 1
                        
                        csv_row = {
                            "采集日期": str(collect_date),
                            "采集时间": str(collect_time),
                            "设备ID": device_id,
                            "段落序号": int(segment_number),
                            "向下镜头视频链接": build_video_url(device_id, session_id, segment_number, "down"),
                            "向前镜头视频链接": build_video_url(device_id, session_id, segment_number, "front"),
                            "session_id": session_id,
                            "filesize": filesize_str,
                            "原始上送时长": duration,
                            "任务描述": translate_task_description(row['task_description'] or ""),
                            "运营端不合格时长": "",
                            "算法端可用数据时长": "",
                            "审批人": approver,
                            "审批状态": "待审批",
                            "数据状态": "",
                            "不合格时间标注": "",
                            "不合格时长": "",
                            "NOTE": "",
                            "操作员姿态": "",
                            "头部移动": "",
                            "向下摄像头手部位置": "",
                            "其余数据标签": "",
                            "LET PT": "",
                        }
                    else:
                        # 奥特内部采集格式
                        csv_row = {
                            "采集日期": str(row['collect_date']),
                            "采集时间": str(row['collect_time']),
                            "设备ID": device_id,
                            "段落号": int(segment_number),
                            "向下镜头视频链接": build_video_url(device_id, session_id, segment_number, "down"),
                            "向前镜头视频链接": build_video_url(device_id, session_id, segment_number, "front"),
                            "session_id": session_id,
                            "filesize": filesize_str,
                            "时长": duration,
                            "审批人": "",
                            "任务描述": translate_task_description(row['task_description'] or ""),
                            "操作员姿态": "",
                            "头部移动": "",
                            "向下摄像头手部位置": "",
                            "数据状态": "",
                            "时间标注": "",
                            "NOTE": "",
                        }
                    
                    writer.writerow(csv_row)
            
            file_size = output_path.stat().st_size
            file_size_mb = file_size / (1024 * 1024)
            
            logging.info(f"\n✓ CSV导出成功!")
            logging.info(f"  - 格式: {FORMAT_NAMES[export_format]}")
            logging.info(f"  - 文件路径: {output_path}")
            logging.info(f"  - 导出行数: {len(valid_rows)}")
            logging.info(f"  - 文件大小: {file_size_mb:.2f} MB")
            
            return len(valid_rows)
            
    except Exception as e:
        logging.error(f"✗ 导出失败: {e}", exc_info=True)
        return 0


def parse_args():
    parser = argparse.ArgumentParser(
        description="从数据库导出CSV文件，支持多种格式",
        formatter_class=argparse.RawDescriptionHelpFormatter
    )
    
    parser.add_argument(
        "--format", "-f",
        dest="format",
        choices=["raw", "internal", "scale"],
        default="internal",
        help="导出格式: raw=原始云格式, internal=奥特内部采集, scale=规模采集重启 (默认: internal)"
    )
    parser.add_argument(
        "--start-date",
        dest="start_date",
        help="开始日期，格式: YYYY-MM-DD"
    )
    parser.add_argument(
        "--end-date",
        dest="end_date",
        help="结束日期，格式: YYYY-MM-DD"
    )
    parser.add_argument(
        "--all",
        dest="export_all",
        action="store_true",
        help="导出所有数据（忽略日期筛选）"
    )
    parser.add_argument(
        "--output", "-o",
        dest="output",
        help="输出文件名（默认自动生成）"
    )
    parser.add_argument(
        "--time-adjust",
        dest="time_adjust",
        action="store_true",
        help="时间+8小时转换（b852设备除外）"
    )
    
    return parser.parse_args()


def main():
    args = parse_args()
    
    # 默认配置
    DEFAULT_EXPORT_ALL = False
    DEFAULT_START_DATE = "2026-01-15"
    DEFAULT_END_DATE = "2026-01-15"
    
    start_date = None
    end_date = None
    
    if not args.start_date and not args.end_date and not args.export_all:
        if DEFAULT_EXPORT_ALL:
            args.export_all = True
        else:
            args.start_date = DEFAULT_START_DATE
            args.end_date = DEFAULT_END_DATE
    
    if args.start_date:
        try:
            start_date = datetime.strptime(args.start_date, "%Y-%m-%d").date()
        except ValueError:
            logging.error("✗ 开始日期格式错误，请使用 YYYY-MM-DD")
            return
    
    if args.end_date:
        try:
            end_date = datetime.strptime(args.end_date, "%Y-%m-%d").date()
        except ValueError:
            logging.error("✗ 结束日期格式错误，请使用 YYYY-MM-DD")
            return
    
    # 生成文件名
    if args.output:
        output_filename = args.output
    else:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        format_prefix = {
            FORMAT_RAW: "raw",
            FORMAT_INTERNAL: "internal", 
            FORMAT_SCALE: "scale",
        }[args.format]
        
        if args.export_all:
            output_filename = f"{format_prefix}_all_{timestamp}.csv"
        elif start_date and end_date:
            output_filename = f"{format_prefix}_{start_date}_{end_date}_{timestamp}.csv"
        else:
            output_filename = f"{format_prefix}_{timestamp}.csv"
    
    logging.info("=" * 60)
    logging.info("导出配置:")
    logging.info(f"  - 格式: {FORMAT_NAMES[args.format]}")
    logging.info(f"  - 日期范围: {start_date or '无'} 至 {end_date or '无'}")
    logging.info(f"  - 导出全部: {'是' if args.export_all else '否'}")
    logging.info(f"  - 输出文件: {EXPORTED_CSV_DIR / output_filename}")
    logging.info("=" * 60)
    
    # 如果是 scale 格式，询问审批人分配
    approvers_config = None
    if args.format == FORMAT_SCALE:
        approvers_config = prompt_approvers_config()
    
    conn = None
    try:
        conn = get_db_connection()
        
        row_count = export_csv(
            conn,
            output_filename,
            args.format,
            start_date,
            end_date,
            args.export_all,
            args.time_adjust,
            approvers_config
        )
        
        if row_count > 0:
            logging.info(f"\n✓ 完成! 共导出 {row_count} 条记录")
        else:
            logging.warning("\n⚠ 没有数据被导出")
            
    except Exception as e:
        logging.error(f"✗ 程序执行失败: {e}", exc_info=True)
    finally:
        if conn:
            conn.close()
            logging.info("✓ 数据库连接已关闭")


if __name__ == "__main__":
    main()
