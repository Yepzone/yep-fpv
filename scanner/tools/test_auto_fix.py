"""
测试自动修复功能
"""
import os
import sys
from pathlib import Path
from dotenv import load_dotenv
import psycopg2

project_root = Path(__file__).parent.parent.parent  # scanner/tools/test_auto_fix.py -> scanner/tools -> scanner -> 项目根目录
sys.path.insert(0, str(project_root))
load_dotenv(project_root / ".env")

# 连接数据库
conn = psycopg2.connect(
    host=os.getenv("PG_HOST"),
    database=os.getenv("PG_DATABASE"),
    user=os.getenv("PG_USER"),
    password=os.getenv("PG_PASSWORD"),
    port=os.getenv("PG_PORT", 5432)
)

# 将 front_file_size_bytes 改回 0 来测试
session_id = "session_20260119_125037_597697"

with conn.cursor() as cur:
    # 先查看当前值
    cur.execute("""
        SELECT segment_number, front_file_size_bytes / 1024.0 / 1024.0 as front_mb
        FROM fpv.segments
        WHERE session_id = %s
        ORDER BY segment_number
    """, (session_id,))
    
    print("当前值:")
    for row in cur.fetchall():
        print(f"  Segment {row[0]}: {row[1]:.2f} MB")
    
    # 改回 0
    cur.execute("""
        UPDATE fpv.segments
        SET front_file_size_bytes = 0
        WHERE session_id = %s
    """, (session_id,))
    conn.commit()
    
    print(f"\n✓ 已将 {session_id} 的 front_file_size_bytes 改回 0")
    print("现在可以测试自动修复功能了")

conn.close()
