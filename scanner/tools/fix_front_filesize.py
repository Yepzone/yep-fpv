"""
修复 Front 文件大小
===================
直接从 OSS 读取 front 文件大小并更新到数据库

使用示例:
  python -m scanner.fix_front_filesize
"""

import os
import sys
import logging
from pathlib import Path
from dotenv import load_dotenv
import oss2
import psycopg2

# 项目根目录
project_root = Path(__file__).parent.parent.parent  # scanner/tools/fix_front_filesize.py -> scanner/tools -> scanner -> 项目根目录
sys.path.insert(0, str(project_root))

# 加载环境变量
load_dotenv(project_root / ".env")

# 日志配置
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
        logging.info(f"✓ 成功连接到数据库")
        return conn
    except Exception as e:
        logging.error(f"✗ 无法连接到数据库: {e}")
        raise


def fix_front_filesize(device_id: str, session_id: str, dry_run: bool = True):
    """
    修复特定 session 的 front 文件大小
    
    参数:
        device_id: 设备ID
        session_id: Session ID
        dry_run: 是否只是预览，不实际更新
    """
    print("=" * 80)
    print(f"修复 Front 文件大小")
    print("=" * 80)
    print(f"设备ID: {device_id}")
    print(f"Session ID: {session_id}")
    print(f"模式: {'预览模式（不会实际更新）' if dry_run else '更新模式'}")
    print()
    
    # 连接 OSS
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
        logging.info(f"✓ 已连接到 OSS")
    except Exception as e:
        logging.error(f"✗ 无法连接到 OSS: {e}")
        return
    
    # 连接数据库
    conn = get_db_connection()
    
    try:
        # 列出该 session 的所有 front 文件
        prefix = f"{device_id}/{session_id}/segments/"
        
        front_files = {}
        for obj in oss2.ObjectIterator(bucket, prefix=prefix):
            filename = obj.key.split("/")[-1]
            
            # 只处理 front mp4 文件
            if "front" in filename and filename.endswith('.mp4'):
                # 提取 segment 编号
                import re
                match = re.search(r'_(\d+)\.mp4$', filename)
                if match:
                    segment_number = match.group(1)
                    front_files[segment_number] = obj.size
        
        logging.info(f"✓ 找到 {len(front_files)} 个 front 文件")
        print()
        
        # 查询数据库中的记录
        with conn.cursor() as cur:
            cur.execute("""
                SELECT segment_number, front_file_size_bytes
                FROM fpv.segments
                WHERE session_id = %s
                ORDER BY segment_number
            """, (session_id,))
            
            db_records = cur.fetchall()
        
        logging.info(f"✓ 数据库中有 {len(db_records)} 条记录")
        print()
        
        # 对比并更新
        updates = []
        
        print("对比结果:")
        print("-" * 80)
        
        for segment_number, db_size in db_records:
            segment_str = str(segment_number).zfill(4)
            oss_size = front_files.get(segment_str, 0)
            
            if db_size != oss_size:
                updates.append((oss_size, session_id, segment_number))
                status = "需要更新" if not dry_run else "预览"
                print(f"Segment {segment_number}:")
                print(f"  数据库: {db_size / (1024*1024):.2f} MB")
                print(f"  OSS:    {oss_size / (1024*1024):.2f} MB")
                print(f"  状态:   {status}")
            else:
                print(f"Segment {segment_number}: ✓ 一致 ({oss_size / (1024*1024):.2f} MB)")
        
        print()
        print("=" * 80)
        
        if not updates:
            print("✓ 所有记录都已正确，无需更新")
        elif dry_run:
            print(f"⚠️  预览模式：发现 {len(updates)} 条记录需要更新")
            print("   如需实际更新，请设置 dry_run=False")
        else:
            # 执行更新
            print(f"正在更新 {len(updates)} 条记录...")
            
            with conn.cursor() as cur:
                cur.executemany("""
                    UPDATE fpv.segments
                    SET front_file_size_bytes = %s
                    WHERE session_id = %s AND segment_number = %s
                """, updates)
                conn.commit()
            
            logging.info(f"✓ 成功更新 {len(updates)} 条记录")
        
        print("=" * 80)
        
    except Exception as e:
        logging.error(f"✗ 处理失败: {e}")
        import traceback
        traceback.print_exc()
        conn.rollback()
    finally:
        conn.close()


def main():
    """主函数"""
    # ========================================
    # 配置区域
    # ========================================
    DEVICE_ID = "6ea2"
    SESSION_ID = "session_20260119_125037_597697"
    DRY_RUN = False  # 改为 False 以实际更新数据库
    # ========================================
    
    print()
    print("Front 文件大小修复工具")
    print()
    
    fix_front_filesize(DEVICE_ID, SESSION_ID, dry_run=DRY_RUN)


if __name__ == "__main__":
    main()
