import argparse
import json
import logging
import os
import re
import shutil
from datetime import datetime, date
from pathlib import Path
from typing import List, Dict, Tuple, Optional, Any

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

# 假设这个模块存在并提供 oss2.Bucket 对象
from utils.external_connections import get_oss_bucket

# ==============================================================================
# 配置文件与常量
# ==============================================================================
# 加载环境变量 (包括 OSS 配置和 DB 配置)
load_dotenv(override=False)

TEMP_METADATA_DIR = Path("./temp_metadata_files")
METADATA_FILENAME = "metadata.json"
TABLE_FULL_NAME = "session_info.session_metadata"  # 目标表名

# 配置日志
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# 需要跳过的设备ID列表
SKIP_DEVICE_IDS = ["stereo_cam0", "test"]


# ==============================================================================
# 数据库连接与检查函数
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
        return conn
    except Exception as e:
        logging.error(f"无法连接到 PostgreSQL 数据库: {e}")
        # 如果连接失败，则抛出异常，阻止主程序运行
        raise e


def is_session_exists(conn, session_id: str) -> bool:
    """检查数据库中是否已存在该 session_id 的记录（利用 UNIQUE 约束）。"""
    try:
        with conn.cursor() as cur:
            # 检查 session_id 是否已存在
            cur.execute(
                f"SELECT 1 FROM {TABLE_FULL_NAME} WHERE session_id = %s LIMIT 1",
                (session_id,)
            )
            return cur.fetchone() is not None
    except Exception as e:
        logging.error(f"数据库检查失败: {e}")
        # 数据库出错时，返回 False，让后续插入逻辑去处理潜在的唯一性冲突
        return False


# ==============================================================================
# 辅助函数
# ==============================================================================

def parse_session_id(session_id: str) -> Tuple[str, str, Optional[date]]:
    """
    解析 session_id 以提取日期和时间。

    返回:
        元组 (格式化日期字符串, 格式化时间字符串, date对象或None)
    """
    # 模式: session_YYYYMMDD_HHMMSS_微秒
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
            logging.warning(f"session_id {session_id} 中的日期/时间无效: {e}")
            return "", "", None
    return "", "", None


def is_date_in_range(session_date: Optional[date], start_date: Optional[date], end_date: Optional[date]) -> bool:
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


@retry(
    retry=retry_if_exception_type(Exception),
    stop=stop_after_attempt(5),
    wait=wait_exponential(multiplier=0.5, min=1, max=10),
)
def list_prefixes(bucket: oss2.Bucket, prefix: str, delimiter: str = "/") -> List[str]:
    """列出给定前缀下的所有目录。"""
    result = bucket.list_objects_v2(prefix=prefix, delimiter=delimiter, max_keys=1000)
    return result.prefix_list


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
# 数据处理和映射函数
# ==============================================================================

def prepare_db_record(device_id: str, session_id: str, metadata: Dict[str, Any]) -> Dict[str, Any]:
    """
    将 JSON 元数据和路径信息转换为数据库记录格式。
    """
    start_time_iso = metadata.get("start_time_utc_iso8601", "")
    end_time_iso = metadata.get("end_time_utc_iso8601", "")

    # 解析日期/时间 (将用于 collect_date/collect_time)
    collect_date_str, collect_time_str, _ = parse_session_id(session_id)

    # 安全提取嵌套字段
    task_info = metadata.get("task_info", {})
    camera_settings = metadata.get("camera_settings", {})
    device_info = metadata.get("device_info", {})

    # 提取操作员信息 (operator_height)
    operator_height = task_info.get("operator_height")
    # 如果 operator_height 存在，则创建 operator_info 字典，否则为 None
    operator_info = {"operator_height": operator_height} if operator_height is not None else None

    # 采集地点字段 (collect_site)
    collect_site = get_nested_value(metadata, "task_info.collect_site", default="N/A")

    # 摄像头数量计算
    num_cameras = len(camera_settings.get("stereo_cameras", []))

    return {
        # 业务标识符
        "session_id": session_id,
        "device_id": device_id,

        # 时间信息 (新列名)
        "collect_date": collect_date_str,  # <--- 字段名已同步
        "collect_time": collect_time_str,  # <--- 字段名已同步
        "start_time_utc": start_time_iso,
        "end_time_utc": end_time_iso,

        # 任务/地点信息
        "task_description": task_info.get("task_description"),
        "scene": task_info.get("scene"),
        "collect_site": collect_site,
        "operator_info": operator_info,

        # 设备配置信息
        "device_model": device_info.get("model"),
        "platform": device_info.get("platform"),
        "resolution": camera_settings.get("resolution"),
        "fps": camera_settings.get("fps"),
        "num_cameras": num_cameras,

        # 原始 JSON
        "raw_metadata_json": metadata,
    }


def insert_into_db(conn, record: Dict[str, Any]):
    """将单个记录插入到 PostgreSQL 数据库。"""
    if conn is None:
        logging.error("数据库连接无效，跳过插入。")
        return

    # 字段列表 (同步更新 collect_date 和 collect_time)
    fields = [
        "session_id", "device_id", "collect_date", "collect_time", "start_time_utc", "end_time_utc",
        "task_description", "scene", "collect_site", "operator_info",
        "device_model", "platform", "resolution", "fps", "num_cameras", "raw_metadata_json"
    ]

    # 准备 values 占位符 (%s)
    placeholders = ', '.join(['%s'] * len(fields))
    columns = ', '.join(fields)

    # 从记录中提取对应的值
    values = [record.get(field) for field in fields]

    # 特别处理 JSONB 字段，使用 psycopg2.Json(value)
    # operator_info 和 raw_metadata_json
    values[fields.index("operator_info")] = psycopg2.extras.Json(record.get("operator_info"))
    values[fields.index("raw_metadata_json")] = psycopg2.extras.Json(record.get("raw_metadata_json"))

    try:
        with conn.cursor() as cur:
            insert_query = f"""
                INSERT INTO {TABLE_FULL_NAME} ({columns})
                VALUES ({placeholders});
            """
            cur.execute(insert_query, values)
            conn.commit()
            logging.info(f"  [DB SUCCESS] 已插入 {record['session_id']}")
    except psycopg2.errors.UniqueViolation:
        # 如果 session_id 唯一约束冲突，说明数据已存在，跳过
        logging.warning(f"  [DB DUPLICATE] 会话 {record['session_id']} 已存在。")
        conn.rollback()
    except Exception as e:
        logging.error(f"  [DB ERROR] 插入 {record['session_id']} 失败: {e}")
        conn.rollback()


# ==============================================================================
# 核心扫描逻辑
# ==============================================================================

def process_oss_metadata(
        bucket: oss2.Bucket,
        conn,
        start_date: Optional[date] = None,
        end_date: Optional[date] = None,
        device_id_filter: Optional[str] = None,
        debug_mode: bool = False
):
    """
    扫描OSS存储桶，定位 metadata.json 文件，并处理写入数据库。
    """
    processed_count = 0
    date_filter_active = start_date is not None or end_date is not None

    if debug_mode:
        logging.info("调试模式: 将处理限制为5个会话")
    if SKIP_DEVICE_IDS:
        logging.info(f"自动跳过以下设备: {', '.join(SKIP_DEVICE_IDS)}")

    # 1. 创建临时目录
    TEMP_METADATA_DIR.mkdir(exist_ok=True)
    logging.info(f"创建临时目录: {TEMP_METADATA_DIR}")

    logging.info("扫描存储桶中的设备ID...")
    device_prefixes = list_prefixes(bucket, "")

    for device_prefix in device_prefixes:
        device_id = device_prefix.rstrip("/")

        if device_id in SKIP_DEVICE_IDS:
            logging.info(f"\n跳过设备: {device_id} (在跳过列表中)")
            continue

        if device_id_filter and device_id != device_id_filter:
            continue

        logging.info(f"\n处理设备: {device_id}")

        # 2. 列出该设备下的所有 session 目录
        session_prefixes = list_prefixes(bucket, device_prefix)

        for session_prefix in session_prefixes:
            session_id = session_prefix.rstrip("/").split("/")[-1]

            if not session_id.startswith("session_"):
                continue

            # 从 session_id 中获取日期对象进行筛选
            date_str, time_str, session_date = parse_session_id(session_id)

            # 日期筛选
            if date_filter_active and not is_date_in_range(session_date, start_date, end_date):
                continue

            logging.info(f"    -> 检查会话: {session_id} ({date_str})")

            # 3. 检查数据库中是否已存在
            if is_session_exists(conn, session_id):
                logging.warning(f"    [SKIP] 会话 {session_id} 已存在于数据库。")
                continue

            # 4. 定位 OSS 文件
            oss_key = f"{session_prefix}{METADATA_FILENAME}"
            local_path = TEMP_METADATA_DIR / f"{session_id}_{METADATA_FILENAME}"

            # 5. 检查文件是否存在
            try:
                if not bucket.object_exists(oss_key):
                    logging.warning(f"    [SKIP] 文件 {oss_key} 不存在。")
                    continue

                # 6. 下载文件
                logging.info(f"    下载 {oss_key}...")
                bucket.get_object_to_file(oss_key, str(local_path))

                # 7. 解析 JSON 文件
                with open(local_path, 'r', encoding='utf-8') as f:
                    metadata = json.load(f)

                # 8. 数据准备和写入
                record = prepare_db_record(device_id, session_id, metadata)
                insert_into_db(conn, record)

                processed_count += 1

                # 9. 调试模式限制
                if debug_mode and processed_count >= 5:
                    logging.info("调试模式: 达到限制，提前停止扫描")
                    return

            except Exception as e:
                logging.error(f"    [ERROR] 处理 {oss_key} 失败: {e}")

            finally:
                # 10. 清理本地文件
                if local_path.exists():
                    os.remove(local_path)
                    # logging.info(f"    [CLEAN] 已删除本地文件 {local_path}。")

    logging.info(f"\n扫描完成! 总共处理并写入 {processed_count} 条记录。")


# ==============================================================================
# Main
# ==============================================================================

def parse_args():
    parser = argparse.ArgumentParser(
        description="扫描OSS存储桶中的 metadata.json 文件并写入 PostgreSQL。"
    )
    parser.add_argument("--debug", action="store_true", default=False, help="调试模式: 将处理限制为5个会话。")
    parser.add_argument("--start-date", dest="start_date", help="开始日期筛选，格式为YYYY-MM-DD (包含)")
    parser.add_argument("--end-date", dest="end_date", help="结束日期筛选，格式为YYYY-MM-DD (包含)")
    parser.add_argument("--device-id", dest="device_id", help="要扫描的设备ID (可选)")
    return parser.parse_args()


def main():
    args = parse_args()

    # 1. 日期解析和默认值
    start_date = None
    end_date = None
    if args.start_date:
        try:
            start_date = datetime.strptime(args.start_date, "%Y-%m-%d").date()
        except ValueError:
            logging.error("开始日期格式错误，请使用 YYYY-MM-DD。")
            return
    if args.end_date:
        try:
            end_date = datetime.strptime(args.end_date, "%Y-%m-%d").date()
        except ValueError:
            logging.error("结束日期格式错误，请使用 YYYY-MM-DD。")
            return

    # 默认使用今天的日期范围
    if not start_date and not end_date:
        today = date.today()
        start_date = today
        end_date = today
        logging.info(f"未指定日期范围，默认使用今天: {today}")

    # 2. 获取连接
    oss_bucket = None
    db_conn = None
    try:
        # 获取 OSS 存储桶连接
        oss_bucket = get_oss_bucket()
        logging.info(f"已连接到 OSS 存储桶: {oss_bucket.bucket_name}")

        # 获取数据库连接
        db_conn = get_db_connection()
        logging.info(f"已连接到 PostgreSQL 数据库。")

        # 3. 扫描和处理
        process_oss_metadata(
            oss_bucket,
            db_conn,
            start_date,
            end_date,
            args.device_id,
            args.debug
        )

    except Exception as e:
        logging.error(f"主程序运行失败: {e}")

    finally:
        # 4. 清理资源
        if db_conn:
            db_conn.close()
            logging.info("关闭数据库连接。")

        if TEMP_METADATA_DIR.exists():
            shutil.rmtree(TEMP_METADATA_DIR)
            logging.info(f"清理临时目录: {TEMP_METADATA_DIR}")

        logging.info("程序结束。")


if __name__ == "__main__":
    main()
