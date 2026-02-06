"""
列出近期活跃设备
================
从数据库查询近期有数据的设备，按活跃时间分组显示

使用示例:
  uv run -m scanner.tools.list_active_devices
"""

import os
import sys
from datetime import date, timedelta
from pathlib import Path

import psycopg2
from psycopg2 import extras
from dotenv import load_dotenv

# 加载环境变量 (与其他脚本保持一致)
load_dotenv(override=False)


def get_db_connection():
    """获取数据库连接"""
    return psycopg2.connect(
        host=os.getenv("PG_HOST"),
        database=os.getenv("PG_DATABASE"),
        user=os.getenv("PG_USER"),
        password=os.getenv("PG_PASSWORD"),
        port=os.getenv("PG_PORT", 5432)
    )


def list_active_devices():
    """列出近期活跃设备"""
    today = date.today()
    one_week_ago = today - timedelta(days=7)
    
    query = """
        SELECT 
            device_id,
            collect_date,
            COUNT(DISTINCT session_id) as session_count
        FROM fpv.sessions
        WHERE collect_date >= %s
        GROUP BY device_id, collect_date
        ORDER BY collect_date DESC, session_count DESC
    """
    
    conn = None
    try:
        conn = get_db_connection()
        
        with conn.cursor(cursor_factory=extras.RealDictCursor) as cur:
            cur.execute(query, (one_week_ago,))
            rows = cur.fetchall()
        
        if not rows:
            print("最近一周没有活跃设备")
            return
        
        # 按日期分组显示
        print("=" * 60)
        print("📱 近期活跃设备")
        print("=" * 60)
        
        current_date = None
        for row in rows:
            collect_date = row['collect_date']
            device_id = row['device_id']
            session_count = row['session_count']
            
            # 计算距今天数
            days_ago = (today - collect_date).days
            
            if collect_date != current_date:
                current_date = collect_date
                # 日期标签
                if days_ago == 0:
                    label = "🔥 今天"
                elif days_ago == 1:
                    label = "📅 昨天"
                else:
                    label = f"📆 {days_ago}天前"
                
                print(f"\n{label} ({collect_date})")
                print("-" * 40)
            
            print(f"  {device_id:<12} {session_count} 个会话")
        
        print("\n" + "=" * 60)
        
    except Exception as e:
        print(f"查询失败: {e}")
    finally:
        if conn:
            conn.close()


if __name__ == "__main__":
    list_active_devices()
