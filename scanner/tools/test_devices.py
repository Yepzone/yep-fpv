#!/home/x/workspace/cleanbase/FPV_session_info/.venv/bin/python
import sys
import os

# 修复导入路径：添加项目根目录
project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))  # scanner/tools/test_devices.py -> scanner/tools -> scanner -> 项目根目录
sys.path.insert(0, project_root)

import psycopg2
from scanner.scan.info_scan import get_db_connection  # 使用正确的数据库连接函数

def check_database_status():
    conn = get_db_connection()  # 使用正确的连接函数
    try:
        with conn.cursor() as cur:
            # 检查所有设备
            cur.execute("SELECT device_id, is_active, skip_scan, created_at FROM fpv.devices ORDER BY created_at;")
            devices = cur.fetchall()
            print("=== 所有设备 ===")
            for device in devices:
                print(f"设备ID: {device[0]}, 活跃: {device[1]}, 跳过扫描: {device[2]}, 创建时间: {device[3]}")
            
            # 检查设备13fa
            cur.execute("SELECT * FROM fpv.devices WHERE device_id = '13fa';")
            device_13fa = cur.fetchone()
            print("\n=== 设备13fa详情 ===")
            if device_13fa:
                print(f"设备存在: {device_13fa}")
            else:
                print("设备13fa不存在！")
            
            # 检查各设备的会话数量
            cur.execute("SELECT device_id, COUNT(*) as session_count FROM fpv.sessions GROUP BY device_id;")
            sessions_by_device = cur.fetchall()
            print("\n=== 各设备的会话数量 ===")
            for device, count in sessions_by_device:
                print(f"{device}: {count}个会话")
            
            # 检查设备13fa的会话
            cur.execute("SELECT COUNT(*) FROM fpv.sessions WHERE device_id = '13fa';")
            session_count_13fa = cur.fetchone()[0]
            print(f"\n=== 设备13fa的会话数量 ===")
            print(f"设备13fa有 {session_count_13fa} 个会话")
            
    except Exception as e:
        print(f"查询数据库时出错: {e}")
    finally:
        conn.close()

if __name__ == "__main__":
    check_database_status()