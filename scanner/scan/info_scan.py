"""
OSS视频数据扫描脚本 - 数据库版本
==================================
功能:
1. 扫描OSS上的metadata.json文件，写入fpv.sessions表
2. 扫描OSS上的视频段文件(*.mp4)，写入fpv.segments表
3. 自动管理设备配置(fpv.devices)
4. 支持增量扫描(基于updated_at)
5. 导出本次新增记录的CSV文件

使用示例:
  uv run -m scanner.scan.info_scan --device-id 7393 --start-date 2025-12-01 --end-date 2025-12-01
"""

import argparse
import csv
import json
import logging
import os
import re
import shutil
from collections import defaultdict
from datetime import datetime, date
from pathlib import Path
from typing import List, Dict, Tuple, Optional, Any, Set

import oss2
import psycopg2
from dotenv import load_dotenv
from psycopg2 import extras
from tenacity import (
    retry,
    stop_after_attempt,
    wait_exponential,
    retry_if_exception_type,
)

from utils.external_connections import get_oss_bucket

# ==============================================================================
# 配置与常量
# ==============================================================================

load_dotenv(override=False)

TEMP_METADATA_DIR = Path("./temp_metadata_files")
METADATA_FILENAME = "metadata.json"
DEFAULT_CSV_FILENAME = "oss_mp4_qa.csv"

# CSV导出目录 (固定路径)
EXPORTED_CSV_DIR = Path("./ExportedCSV")

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)


# 日志辅助函数
def log_section(title: str, level: int = 0):
    """打印分节标题"""
    if level == 0:
        logging.info("=" * 80)
        logging.info(f"{title}")
        logging.info("=" * 80)
    elif level == 1:
        logging.info("-" * 80)
        logging.info(f"{title}")
        logging.info("-" * 80)
    else:
        logging.info(f"\n{'  ' * (level - 2)}>>> {title}")


def log_stats(stats: Dict[str, int], prefix: str = ""):
    """打印统计信息"""
    for key, value in stats.items():
        logging.info(f"{prefix}{key}: {value}")


# ==============================================================================
# 数据库连接与操作
# ==============================================================================

def get_db_connection():
    """从环境变量获取配置并返回 PostgreSQL 连接。"""
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
        logging.error(f"✗ 无法连接到 PostgreSQL 数据库: {e}")
        raise e


def load_device_config(conn) -> Dict[str, Dict]:
    """
    从数据库加载设备配置。

    返回:
        {
            'device_id': {
                'mb_per_10min': 600.0,
                'skip_scan': False,
                'is_active': True
            }
        }
    """
    try:
        with conn.cursor(cursor_factory=extras.RealDictCursor) as cur:
            cur.execute("""
                        SELECT device_id, mb_per_10min, skip_scan, is_active
                        FROM fpv.devices
                        """)
            rows = cur.fetchall()

            config = {}
            active_count = 0
            skip_count = 0

            for row in rows:
                config[row['device_id']] = {
                    'mb_per_10min': float(row['mb_per_10min']),
                    'skip_scan': row['skip_scan'],
                    'is_active': row['is_active']
                }
                if row['is_active'] and not row['skip_scan']:
                    active_count += 1
                elif row['skip_scan']:
                    skip_count += 1

            logging.info(f"✓ 已加载设备配置:")
            logging.info(f"  - 总设备数: {len(config)}")
            logging.info(f"  - 活跃设备: {active_count}")
            logging.info(f"  - 跳过扫描: {skip_count}")
            logging.info(f"  - 非活跃设备: {len(config) - active_count - skip_count}")
            return config
    except Exception as e:
        logging.error(f"✗ 加载设备配置失败: {e}")
        return {}


def ensure_device_exists(conn, device_id: str, device_config: Dict) -> bool:
    """
    确保设备存在于数据库中，如果不存在则自动创建。

    返回:
        True 如果设备可用 (is_active=True, skip_scan=False)
    """
    if device_id in device_config:
        # 检查是否应该跳过
        if device_config[device_id]['skip_scan']:
            logging.debug(f"  ⊗ 设备 {device_id} 配置为跳过扫描")
            return False
        if not device_config[device_id]['is_active']:
            logging.debug(f"  ⊗ 设备 {device_id} 未启用")
            return False
        return True

    # 设备不存在，自动创建
    try:
        with conn.cursor() as cur:
            cur.execute("""
                        INSERT INTO fpv.devices (device_id, mb_per_10min, is_active, skip_scan)
                        VALUES (%s, 600.0, TRUE, FALSE) ON CONFLICT (device_id) DO NOTHING
                        """, (device_id,))
            conn.commit()

            # 更新本地配置缓存
            device_config[device_id] = {
                'mb_per_10min': 600.0,
                'skip_scan': False,
                'is_active': True
            }

            logging.info(f"  ✓ 新设备 {device_id} 已自动注册 (默认配置: 600MB/10min)")
            return True
    except Exception as e:
        logging.error(f"  ✗ 创建设备 {device_id} 失败: {e}")
        conn.rollback()
        return False


def is_session_exists(conn, session_id: str) -> bool:
    """检查会话是否已存在于数据库。"""
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT 1 FROM fpv.sessions WHERE session_id = %s LIMIT 1",
                (session_id,)
            )
            return cur.fetchone() is not None
    except Exception as e:
        logging.error(f"✗ 检查会话存在性失败: {e}")
        return False


def is_segment_exists(conn, session_id: str, segment_number: str) -> bool:
    """检查视频段是否已存在于数据库。"""
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT 1 FROM fpv.segments WHERE session_id = %s AND segment_number = %s LIMIT 1",
                (session_id, segment_number)
            )
            return cur.fetchone() is not None
    except Exception as e:
        logging.error(f"✗ 检查视频段存在性失败: {e}")
        return False


# ==============================================================================
# OSS 操作 (带重试)
# ==============================================================================

@retry(
    retry=retry_if_exception_type(Exception),
    stop=stop_after_attempt(5),
    wait=wait_exponential(multiplier=0.5, min=1, max=10),
)
def list_prefixes(bucket: oss2.Bucket, prefix: str, delimiter: str = "/") -> List[str]:
    """列出给定前缀下的所有目录。"""
    result = bucket.list_objects_v2(prefix=prefix, delimiter=delimiter, max_keys=1000)
    return result.prefix_list


@retry(
    retry=retry_if_exception_type(Exception),
    stop=stop_after_attempt(5),
    wait=wait_exponential(multiplier=0.5, min=1, max=10),
)
def list_objects(bucket: oss2.Bucket, prefix: str) -> List[str]:
    """列出给定前缀下的所有对象。"""
    objects = []
    continuation_token = ""

    while True:
        result = bucket.list_objects_v2(
            prefix=prefix,
            continuation_token=continuation_token,
            max_keys=1000
        )
        objects.extend([obj.key for obj in result.object_list])

        if not result.is_truncated:
            break
        continuation_token = result.next_continuation_token

    return objects


@retry(
    retry=retry_if_exception_type(Exception),
    stop=stop_after_attempt(5),
    wait=wait_exponential(multiplier=0.5, min=1, max=10),
)
def get_object_size(bucket: oss2.Bucket, object_key: str) -> int:
    """获取对象大小（字节）。"""
    try:
        result = bucket.head_object(object_key)
        return result.content_length
    except Exception as e:
        logging.warning(f"⚠ 无法获取对象大小 {object_key}: {e}")
        return 0


# ==============================================================================
# 辅助函数
# ==============================================================================

def parse_session_id(session_id: str) -> Tuple[str, str, Optional[date]]:
    """
    解析session_id以提取日期和时间。

    示例: session_20251028_051033_882605 -> ('2025-10-28', '05:10:33', date(2025, 10, 28))

    返回:
        元组 (日期字符串, 时间字符串, date对象)
    """
    match = re.match(r"^session_(\d{4})(\d{2})(\d{2})_(\d{2})(\d{2})(\d{2})_\d+$", session_id)
    if match:
        try:
            year, month, day = map(int, match.groups()[0:3])
            hour, minute, second = match.groups()[3:6]

            date_str = f"{year}-{month:02d}-{day:02d}"
            time_str = f"{hour}:{minute}:{second}"
            date_obj = date(year, month, day)
            return date_str, time_str, date_obj
        except ValueError as e:
            logging.warning(f"⚠ session_id {session_id} 中的日期/时间无效: {e}")
            return "", "", None
    return "", "", None


def is_date_in_range(
        session_date: Optional[date],
        start_date: Optional[date],
        end_date: Optional[date]
) -> bool:
    """检查给定日期是否在指定范围内。"""
    if session_date is None:
        return False
    if start_date is None and end_date is None:
        return True
    if start_date and session_date < start_date:
        return False
    if end_date and session_date > end_date:
        return False
    return True


def parse_mp4_filename(filename: str) -> Tuple[str, str]:
    """
    解析MP4文件名以提取摄像头类型和分段号。

    示例:
        stereo_cam0_sbs_0010.mp4 -> ('stereo_cam0', '0010')
        2ed0-front_sbs_0012.mp4 -> ('2ed0-front', '0012')
    """
    match = re.match(r"^(.+?)_sbs_(\d+)\.mp4$", filename)
    if match:
        return match.group(1), match.group(2)
    return "", ""


def get_nested_value(data: Dict[str, Any], path: str, default: Any = None) -> Any:
    """安全地获取嵌套字典中的值。"""
    keys = path.split('.')
    current = data
    for key in keys:
        if isinstance(current, dict) and key in current:
            current = current[key]
        else:
            return default
    return current


# ==============================================================================
# Metadata 扫描与处理
# ==============================================================================

def prepare_session_record(
        device_id: str,
        session_id: str,
        metadata: Dict[str, Any]
) -> Dict[str, Any]:
    """
    将metadata.json转换为数据库记录格式。
    """
    start_time_iso = metadata.get("start_time_utc_iso8601", "")
    end_time_iso = metadata.get("end_time_utc_iso8601", "")

    # 解析日期/时间
    collect_date_str, collect_time_str, _ = parse_session_id(session_id)

    # 提取嵌套字段
    task_info = metadata.get("task_info", {})
    camera_settings = metadata.get("camera_settings", {})
    device_info = metadata.get("device_info", {})

    # 操作员信息
    operator_height = task_info.get("operator_height")
    operator_info = {"operator_height": operator_height} if operator_height is not None else None

    # 采集地点
    collect_site = get_nested_value(metadata, "task_info.collect_site", default="N/A")

    # 摄像头数量
    num_cameras = len(camera_settings.get("stereo_cameras", []))

    return {
        "session_id": session_id,
        "device_id": device_id,
        "collect_date": collect_date_str,
        "collect_time": collect_time_str,
        "start_time_utc": start_time_iso,
        "end_time_utc": end_time_iso,
        "task_description": task_info.get("task_description"),
        "scene": task_info.get("scene"),
        "collect_site": collect_site,
        "operator_info": operator_info,
        "device_model": device_info.get("model"),
        "platform": device_info.get("platform"),
        "resolution": camera_settings.get("resolution"),
        "fps": camera_settings.get("fps"),
        "num_cameras": num_cameras,
        "raw_metadata_json": metadata,
    }


def insert_session(conn, record: Dict[str, Any]) -> bool:
    """
    将会话记录插入数据库。

    返回:
        True 如果插入成功, False 如果失败或重复
    """
    fields = [
        "session_id", "device_id", "collect_date", "collect_time",
        "start_time_utc", "end_time_utc", "task_description", "scene",
        "collect_site", "operator_info", "device_model", "platform",
        "resolution", "fps", "num_cameras", "raw_metadata_json"
    ]

    placeholders = ', '.join(['%s'] * len(fields))
    columns = ', '.join(fields)
    values = [record.get(field) for field in fields]

    # 处理JSONB字段
    values[fields.index("operator_info")] = psycopg2.extras.Json(record.get("operator_info"))
    values[fields.index("raw_metadata_json")] = psycopg2.extras.Json(record.get("raw_metadata_json"))

    try:
        with conn.cursor() as cur:
            insert_query = f"""
                INSERT INTO fpv.sessions ({columns})
                VALUES ({placeholders})
            """
            cur.execute(insert_query, values)
            conn.commit()
            return True
    except psycopg2.errors.UniqueViolation:
        conn.rollback()
        return False
    except Exception as e:
        logging.error(f"      ✗ 插入会话失败 {record['session_id']}: {e}")
        conn.rollback()
        return False


def scan_metadata(
        bucket: oss2.Bucket,
        conn,
        device_config: Dict,
        start_date: Optional[date] = None,
        end_date: Optional[date] = None,
        device_id_filter: Optional[str] = None,
        debug_mode: bool = False
) -> Dict[str, int]:
    """
    扫描OSS上的metadata.json文件并写入数据库。

    返回:
        统计字典
    """
    log_section("阶段 1: 扫描 Metadata", level=0)

    stats = {
        '扫描设备数': 0,
        '扫描会话数': 0,
        '新增会话数': 0,
        '已存在会话数': 0,
        '跳过会话数': 0,
        'metadata不存在': 0,
        '解析失败数': 0,
        '插入失败数': 0,
    }

    TEMP_METADATA_DIR.mkdir(exist_ok=True)

    logging.info("正在扫描OSS存储桶...")
    device_prefixes = list_prefixes(bucket, "")
    logging.info(f"✓ 发现 {len(device_prefixes)} 个设备前缀\n")

    for device_idx, device_prefix in enumerate(device_prefixes, 1):
        device_id = device_prefix.rstrip("/")

        # 设备筛选
        if device_id_filter and device_id != device_id_filter:
            logging.debug(f"跳过设备 {device_id} (不在筛选范围)")
            continue

        # 检查设备是否可用
        if not ensure_device_exists(conn, device_id, device_config):
            continue

        stats['扫描设备数'] += 1

        log_section(f"设备 [{device_idx}/{len(device_prefixes)}]: {device_id}", level=2)

        # 列出会话目录
        session_prefixes = list_prefixes(bucket, device_prefix)
        valid_sessions = [s for s in session_prefixes if s.rstrip("/").split("/")[-1].startswith("session_")]

        logging.info(f"  发现 {len(valid_sessions)} 个会话目录")

        session_stats = {
            'total': 0,
            'new': 0,
            'exists': 0,
            'skipped': 0,
            'no_metadata': 0,
            'parse_error': 0,
        }

        for session_idx, session_prefix in enumerate(valid_sessions, 1):
            session_id = session_prefix.rstrip("/").split("/")[-1]
            stats['扫描会话数'] += 1
            session_stats['total'] += 1

            # 日期筛选
            date_str, time_str, session_date = parse_session_id(session_id)
            if not is_date_in_range(session_date, start_date, end_date):
                session_stats['skipped'] += 1
                stats['跳过会话数'] += 1
                logging.debug(f"    [{session_idx}/{len(valid_sessions)}] ⊗ {session_id} - 不在日期范围")
                continue

            # 检查是否已存在
            if is_session_exists(conn, session_id):
                session_stats['exists'] += 1
                stats['已存在会话数'] += 1
                logging.debug(f"    [{session_idx}/{len(valid_sessions)}] ⊗ {session_id} - 已存在")
                continue

            # 下载metadata.json
            oss_key = f"{session_prefix}{METADATA_FILENAME}"
            local_path = TEMP_METADATA_DIR / f"{session_id}_{METADATA_FILENAME}"

            try:
                if not bucket.object_exists(oss_key):
                    session_stats['no_metadata'] += 1
                    stats['metadata不存在'] += 1
                    logging.warning(f"    [{session_idx}/{len(valid_sessions)}] ⚠ {session_id} - metadata不存在")
                    continue

                logging.info(f"    [{session_idx}/{len(valid_sessions)}] ⟳ {session_id} ({date_str} {time_str})")
                bucket.get_object_to_file(oss_key, str(local_path))

                # 解析JSON
                with open(local_path, 'r', encoding='utf-8') as f:
                    metadata = json.load(f)

                # 准备并插入记录
                record = prepare_session_record(device_id, session_id, metadata)
                if insert_session(conn, record):
                    session_stats['new'] += 1
                    stats['新增会话数'] += 1
                    logging.info(f"      ✓ 成功插入")
                else:
                    stats['插入失败数'] += 1
                    logging.warning(f"      ⚠ 插入失败")

                # 调试模式限制
                if debug_mode and stats['新增会话数'] >= 5:
                    logging.warning("\n⚠ 调试模式: 达到限制 (5条)，停止metadata扫描")
                    log_stats(session_stats, prefix="  当前设备统计 - ")
                    log_stats(stats, prefix="总体统计 - ")
                    return stats

            except json.JSONDecodeError as e:
                session_stats['parse_error'] += 1
                stats['解析失败数'] += 1
                logging.error(f"      ✗ JSON解析失败: {e}")
            except Exception as e:
                stats['解析失败数'] += 1
                logging.error(f"      ✗ 处理失败: {e}")

            finally:
                if local_path.exists():
                    os.remove(local_path)

        # 输出设备级统计
        if session_stats['total'] > 0:
            logging.info(f"\n  设备 {device_id} 统计:")
            logging.info(f"    - 总会话数: {session_stats['total']}")
            logging.info(f"    - 新增: {session_stats['new']}")
            logging.info(f"    - 已存在: {session_stats['exists']}")
            logging.info(f"    - 日期筛选跳过: {session_stats['skipped']}")
            logging.info(f"    - metadata不存在: {session_stats['no_metadata']}")
            logging.info(f"    - 解析错误: {session_stats['parse_error']}\n")

    log_section("Metadata 扫描完成", level=1)
    log_stats(stats, prefix="  ")

    return stats


# ==============================================================================
# Segments 扫描与处理
# ==============================================================================

def insert_segment(conn, record: Dict[str, Any]) -> bool:
    """
    将视频段记录插入数据库。

    返回:
        True 如果插入成功, False 如果失败或重复
    """
    fields = [
        "session_id", "segment_number",
        "down_file_name", "down_oss_path", "down_file_size_bytes",
        "front_file_name", "front_oss_path", "front_file_size_bytes"
    ]

    placeholders = ', '.join(['%s'] * len(fields))
    columns = ', '.join(fields)
    values = [record.get(field) for field in fields]

    try:
        with conn.cursor() as cur:
            insert_query = f"""
                INSERT INTO fpv.segments ({columns})
                VALUES ({placeholders})
            """
            cur.execute(insert_query, values)
            conn.commit()
            return True
    except psycopg2.errors.UniqueViolation:
        conn.rollback()
        return False
    except Exception as e:
        logging.error(f"        ✗ 插入段失败 {record['segment_number']}: {e}")
        conn.rollback()
        return False


def scan_segments(
        bucket: oss2.Bucket,
        bucket_name: str,
        conn,
        device_config: Dict,
        start_date: Optional[date] = None,
        end_date: Optional[date] = None,
        device_id_filter: Optional[str] = None,
        debug_mode: bool = False
) -> Tuple[Dict[str, int], Set[Tuple[str, str]]]:
    """
    扫描OSS上的视频段文件并写入数据库。
    处理所有符合条件的session（包括已存在的），只插入新的segments。

    返回:
        (统计字典, 新增的(session_id, segment_number)集合)
    """
    log_section("阶段 2: 扫描 Segments", level=0)

    stats = {
        '扫描设备数': 0,
        '扫描会话数': 0,
        '处理会话数': 0,
        '新增视频段数': 0,
        '已存在视频段数': 0,
        '跳过会话数': 0,
        '未配对段数': 0,
        '文件名无效数': 0,
        '会话不存在数': 0,
    }

    # 记录本次新增的 (session_id, segment_number)
    new_segments = set()

    logging.info("正在扫描OSS存储桶...\n")

    device_prefixes = list_prefixes(bucket, "")

    for device_idx, device_prefix in enumerate(device_prefixes, 1):
        device_id = device_prefix.rstrip("/")

        # 设备筛选
        if device_id_filter and device_id != device_id_filter:
            continue

        # 检查设备配置
        if device_id not in device_config:
            logging.debug(f"跳过设备 {device_id} (未在配置中)")
            continue

        if device_config[device_id]['skip_scan']:
            continue

        stats['扫描设备数'] += 1

        log_section(f"设备 [{device_idx}/{len(device_prefixes)}]: {device_id}", level=2)

        # 列出会话目录
        session_prefixes = list_prefixes(bucket, device_prefix)
        valid_sessions = [s for s in session_prefixes if s.rstrip("/").split("/")[-1].startswith("session_")]

        logging.info(f"  发现 {len(valid_sessions)} 个会话目录")

        session_stats = {
            'total': 0,
            'processed': 0,
            'skipped_no_session': 0,
            'skipped_date': 0,
            'new_segments': 0,
            'exists_segments': 0,
            'unpaired': 0,
        }

        for session_idx, session_prefix in enumerate(valid_sessions, 1):
            session_id = session_prefix.rstrip("/").split("/")[-1]
            stats['扫描会话数'] += 1
            session_stats['total'] += 1

            # 日期筛选
            date_str, time_str, session_date = parse_session_id(session_id)
            if not is_date_in_range(session_date, start_date, end_date):
                session_stats['skipped_date'] += 1
                stats['跳过会话数'] += 1
                logging.debug(f"    [{session_idx}/{len(valid_sessions)}] ⊗ {session_id} - 不在日期范围")
                continue

            # 必须先存在session记录
            if not is_session_exists(conn, session_id):
                session_stats['skipped_no_session'] += 1
                stats['会话不存在数'] += 1
                logging.warning(f"    [{session_idx}/{len(valid_sessions)}] ⚠ {session_id} - 会话不存在，跳过")
                continue

            # 处理所有符合条件的session（包括已存在的）
            logging.info(f"    [{session_idx}/{len(valid_sessions)}] ⟳ {session_id} ({date_str} {time_str})")
            session_stats['processed'] += 1
            stats['处理会话数'] += 1

            # 列出segments目录中的MP4文件
            segments_prefix = f"{session_prefix}segments/"
            try:
                objects = list_objects(bucket, segments_prefix)
            except Exception as e:
                logging.error(f"        ✗ 无法列出对象: {e}")
                continue

            mp4_objects = [obj for obj in objects if obj.endswith(".mp4")]
            logging.info(f"        发现 {len(mp4_objects)} 个MP4文件")

            if len(mp4_objects) == 0:
                continue

            # 按segment_number分组
            segments_dict = defaultdict(dict)
            invalid_filenames = 0

            for obj_key in mp4_objects:
                filename = obj_key.split("/")[-1]
                camera_type, segment_number = parse_mp4_filename(filename)

                if not camera_type or not segment_number:
                    invalid_filenames += 1
                    logging.warning(f"          ⚠ 文件名格式无效: {filename}")
                    continue

                oss_path = f"oss://{bucket_name}/{obj_key}"
                file_size_bytes = get_object_size(bucket, obj_key)

                camera_lower = camera_type.lower()
                if "down" in camera_lower:
                    segments_dict[segment_number]["down_file_name"] = filename
                    segments_dict[segment_number]["down_oss_path"] = oss_path
                    segments_dict[segment_number]["down_file_size_bytes"] = file_size_bytes
                elif "front" in camera_lower:
                    segments_dict[segment_number]["front_file_name"] = filename
                    segments_dict[segment_number]["front_oss_path"] = oss_path
                    segments_dict[segment_number]["front_file_size_bytes"] = file_size_bytes
                else:
                    invalid_filenames += 1
                    logging.warning(f"          ⚠ 未知摄像头类型: {filename}")

            if invalid_filenames > 0:
                stats['文件名无效数'] += invalid_filenames

            # 处理配对的段
            paired_new = 0
            paired_exists = 0
            unpaired = 0

            for segment_number, files in sorted(segments_dict.items()):
                # 必须同时有down和front
                if "down_file_name" not in files or "front_file_name" not in files:
                    unpaired += 1
                    down_status = "✓" if "down_file_name" in files else "✗"
                    front_status = "✓" if "front_file_name" in files else "✗"
                    logging.debug(f"          ⊗ 段 {segment_number} 未配对 (down:{down_status} front:{front_status})")
                    continue

                # 检查是否已存在
                if is_segment_exists(conn, session_id, segment_number):
                    paired_exists += 1
                    logging.debug(f"          ⊗ 段 {segment_number} 已存在")
                    continue

                # 准备记录
                record = {
                    "session_id": session_id,
                    "segment_number": segment_number,
                    "down_file_name": files["down_file_name"],
                    "down_oss_path": files["down_oss_path"],
                    "down_file_size_bytes": files["down_file_size_bytes"],
                    "front_file_name": files["front_file_name"],
                    "front_oss_path": files["front_oss_path"],
                    "front_file_size_bytes": files["front_file_size_bytes"],
                }

                # 插入数据库
                if insert_segment(conn, record):
                    paired_new += 1
                    new_segments.add((session_id, segment_number))  # 记录新增的segment
                    logging.debug(f"          ✓ 段 {segment_number} 已插入")

                # 调试模式限制
                if debug_mode and stats['新增视频段数'] + paired_new >= 5:
                    logging.warning("\n⚠ 调试模式: 达到限制 (5条)，停止segments扫描")
                    stats['新增视频段数'] += paired_new
                    stats['已存在视频段数'] += paired_exists
                    stats['未配对段数'] += unpaired
                    log_stats(stats, prefix="总体统计 - ")
                    return stats, new_segments

            session_stats['new_segments'] += paired_new
            session_stats['exists_segments'] += paired_exists
            session_stats['unpaired'] += unpaired
            stats['新增视频段数'] += paired_new
            stats['已存在视频段数'] += paired_exists
            stats['未配对段数'] += unpaired

            if paired_new + paired_exists + unpaired > 0:
                logging.info(f"        统计: 新增={paired_new}, 已存在={paired_exists}, 未配对={unpaired}")

        # 输出设备级统计
        if session_stats['processed'] > 0:
            logging.info(f"\n  设备 {device_id} 统计:")
            logging.info(f"    - 处理会话数: {session_stats['processed']}")
            logging.info(f"    - 新增视频段: {session_stats['new_segments']}")
            logging.info(f"    - 已存在视频段: {session_stats['exists_segments']}")
            logging.info(f"    - 未配对段: {session_stats['unpaired']}")
            logging.info(f"    - 跳过(会话不存在): {session_stats['skipped_no_session']}")
            logging.info(f"    - 跳过(日期筛选): {session_stats['skipped_date']}\n")

    log_section("Segments 扫描完成", level=1)
    log_stats(stats, prefix="  ")
    logging.info(f"\n  本次新增 segment 数量: {len(new_segments)}")

    return stats, new_segments


# ==============================================================================
# CSV 导出
# ==============================================================================

def export_to_csv(
        conn,
        output_filename: str,
        new_segments: Set[Tuple[str, str]]
) -> Dict[str, int]:
    """
    从数据库导出CSV文件到ExportedCSV目录。
    只导出本次新增的segments记录。

    参数:
        conn: 数据库连接
        output_filename: 输出文件名（不含路径）
        new_segments: 本次新增的(session_id, segment_number)集合

    返回:
        统计字典
    """
    log_section("阶段 3: 导出 CSV (仅本次新增记录)", level=0)

    stats = {
        '导出行数': 0,
    }

    if not new_segments:
        logging.warning("⚠ 本次没有新增segment记录，跳过CSV导出")
        return stats

    # 确保ExportedCSV目录存在
    EXPORTED_CSV_DIR.mkdir(exist_ok=True)

    # 完整的输出路径
    output_path = EXPORTED_CSV_DIR / output_filename

    try:
        logging.info(f"  本次新增 {len(new_segments)} 个segment，正在查询数据...")

        # 构建查询 - 只选择新增的(session_id, segment_number)
        # 由于可能有大量记录，我们使用临时表或者分批查询
        # 这里使用 VALUES 构造临时集合进行JOIN

        # 将 new_segments 转换为列表以便查询
        segments_list = list(new_segments)

        # 构建参数化查询
        # PostgreSQL支持使用 (session_id, segment_number) IN (VALUES ...)
        values_placeholders = ','.join(['(%s, %s)'] * len(segments_list))
        params = []
        for session_id, segment_number in segments_list:
            params.extend([session_id, segment_number])

        query = f"""
            SELECT * FROM fpv.segments_csv_export 
            WHERE (session_id, segment_number) IN (VALUES {values_placeholders})
            ORDER BY date DESC, time DESC, segment_number
        """

        logging.info("  正在查询数据...")
        with conn.cursor(cursor_factory=extras.RealDictCursor) as cur:
            cur.execute(query, params)
            rows = cur.fetchall()

            if not rows:
                logging.warning("⚠ 新增的segments在视图中没有对应数据")
                return stats

            logging.info(f"  查询到 {len(rows)} 行数据")

            # 写入CSV
            fieldnames = [
                "updated_at", "date", "time", "device_id", "segment_number", "approval_status",
                "down_oss_path", "front_oss_path", "session_id", "filesize", "estimated_duration"
            ]

            logging.info(f"  正在写入CSV文件: {output_path}")
            with open(output_path, 'w', newline='', encoding='utf-8') as csvfile:
                writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
                writer.writeheader()
                writer.writerows(rows)

            stats['导出行数'] = len(rows)

            # 计算文件大小
            file_size = output_path.stat().st_size
            file_size_mb = file_size / (1024 * 1024)

            logging.info(f"\n✓ CSV导出成功 (仅本次新增记录)")
            logging.info(f"  - 文件路径: {output_path}")
            logging.info(f"  - 新增segment数: {len(new_segments)}")
            logging.info(f"  - 导出行数: {len(rows):,}")
            logging.info(f"  - 文件大小: {file_size_mb:.2f} MB")

    except Exception as e:
        logging.error(f"✗ CSV导出失败: {e}", exc_info=True)

    return stats


# ==============================================================================
# Main
# ==============================================================================

def parse_args():
    parser = argparse.ArgumentParser(
        description="扫描OSS存储桶并写入PostgreSQL数据库",
        formatter_class=argparse.RawDescriptionHelpFormatter
    )

    # 扫描模式
    parser.add_argument(
        "--mode",
        choices=["all", "metadata", "segments"],
        default="all",
        help="扫描模式: all=metadata+segments, metadata=仅metadata, segments=仅segments (默认: all)"
    )

    # 日期筛选
    parser.add_argument(
        "--start-date",
        dest="start_date",
        help="开始日期，格式: YYYY-MM-DD (包含)"
    )
    parser.add_argument(
        "--end-date",
        dest="end_date",
        help="结束日期，格式: YYYY-MM-DD (包含)"
    )

    # 设备筛选
    parser.add_argument(
        "--device-id",
        dest="device_id",
        help="要扫描的设备ID (可选)"
    )

    # CSV导出
    parser.add_argument(
        "--export-csv",
        action="store_true",
        default=True,
        help="导出CSV文件 (默认: True, 仅导出本次新增记录)"
    )
    parser.add_argument(
        "--no-export-csv",
        dest="export_csv",
        action="store_false",
        help="不导出CSV文件"
    )
    parser.add_argument(
        "--csv-output",
        default=DEFAULT_CSV_FILENAME,
        help=f"CSV输出文件名 (默认: {DEFAULT_CSV_FILENAME}，将保存到ExportedCSV目录)"
    )

    # 调试模式
    parser.add_argument(
        "--debug",
        action="store_true",
        default=False,
        help="调试模式: 限制处理5条记录"
    )

    # 日志级别
    parser.add_argument(
        "--log-level",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        default="INFO",
        help="日志级别 (默认: INFO)"
    )

    return parser.parse_args()


def main():
    args = parse_args()

    # 设置日志级别
    logging.getLogger().setLevel(getattr(logging, args.log_level))

    # 解析日期
    start_date = None
    end_date = None

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

    # 默认日期范围
    if not start_date and not end_date:
        today = date.today()
        start_date = today
        end_date = today
        logging.info(f"未指定日期范围，默认使用今天: {today}\n")

    # 生成带时间戳的CSV文件名
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    base_name = args.csv_output.rsplit(".", 1)[0] if "." in args.csv_output else args.csv_output
    extension = args.csv_output.rsplit(".", 1)[1] if "." in args.csv_output else "csv"
    csv_filename = f"{base_name}_{timestamp}.{extension}"

    # 打印配置
    log_section("扫描配置", level=0)
    logging.info(f"  模式: {args.mode}")
    logging.info(f"  日期范围: {start_date} 至 {end_date}")
    logging.info(f"  设备筛选: {args.device_id or '全部'}")
    logging.info(f"  导出CSV: {'是 (仅本次新增记录)' if args.export_csv else '否'}")
    if args.export_csv:
        csv_full_path = EXPORTED_CSV_DIR / csv_filename
        logging.info(f"  CSV路径: {csv_full_path}")
    logging.info(f"  调试模式: {'是' if args.debug else '否'}")
    logging.info(f"  日志级别: {args.log_level}")

    # 连接资源
    bucket = None
    conn = None

    try:
        # 连接OSS
        logging.info("\n正在连接到OSS...")
        bucket = get_oss_bucket()
        bucket_name = bucket.bucket_name
        logging.info(f"✓ 成功连接到OSS存储桶: {bucket_name}\n")

        # 连接数据库
        logging.info("正在连接到数据库...")
        conn = get_db_connection()

        # 加载设备配置
        logging.info("\n正在加载设备配置...")
        device_config = load_device_config(conn)

        # 执行扫描
        all_stats = {}
        new_segments = set()

        # 扫描metadata
        if args.mode in ["all", "metadata"]:
            metadata_stats = scan_metadata(
                bucket, conn, device_config,
                start_date, end_date, args.device_id, args.debug
            )
            all_stats['metadata'] = metadata_stats

        # 扫描segments (处理所有符合条件的session，只插入新segment)
        if args.mode in ["all", "segments"]:
            segments_stats, new_segments = scan_segments(
                bucket, bucket_name, conn, device_config,
                start_date, end_date, args.device_id, args.debug
            )
            all_stats['segments'] = segments_stats

        # 导出CSV (只导出新增的segments)
        if args.export_csv and new_segments:
            csv_stats = export_to_csv(conn, csv_filename, new_segments)
            all_stats['csv'] = csv_stats
        elif args.export_csv and not new_segments:
            logging.warning("⚠ 没有新增segment记录，跳过CSV导出")

        # 总结
        log_section("扫描任务完成", level=0)

        if 'metadata' in all_stats:
            logging.info("Metadata 统计:")
            log_stats(all_stats['metadata'], prefix="  ")

        if 'segments' in all_stats:
            logging.info("\nSegments 统计:")
            log_stats(all_stats['segments'], prefix="  ")

        if 'csv' in all_stats:
            logging.info("\nCSV 导出统计:")
            log_stats(all_stats['csv'], prefix="  ")

        logging.info("\n" + "=" * 80)
        logging.info("✓ 所有任务执行完毕")
        logging.info("=" * 80)

    except Exception as e:
        logging.error(f"\n{'=' * 80}")
        logging.error(f"✗ 程序执行失败")
        logging.error(f"{'=' * 80}")
        logging.error(f"错误信息: {e}", exc_info=True)

    finally:
        # 清理资源
        if conn:
            conn.close()
            logging.info("\n✓ 数据库连接已关闭")

        if TEMP_METADATA_DIR.exists():
            shutil.rmtree(TEMP_METADATA_DIR)
            logging.info(f"✓ 临时目录已清理: {TEMP_METADATA_DIR}")


if __name__ == "__main__":
    main()