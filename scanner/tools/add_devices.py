import psycopg2
import os

# 读取环境变量
host = os.getenv("PG_HOST")
database = os.getenv("PG_DATABASE")
user = os.getenv("PG_USER")
password = os.getenv("PG_PASSWORD")
port = os.getenv("PG_PORT")

# 连接到数据库
conn = psycopg2.connect(
    host=host,
    database=database,
    user=user,
    password=password,
    port=port
)

# 读取并执行SQL文件
with open('DB_misc/FPV-MetaManagement.sql', 'r', encoding='utf-8') as f:
    sql_content = f.read()

try:
    with conn.cursor() as cur:
        cur.execute(sql_content)
        conn.commit()
        print("数据库初始化成功！")
except Exception as e:
    print(f"执行SQL时出错: {e}")
    conn.rollback()
finally:
    conn.close()