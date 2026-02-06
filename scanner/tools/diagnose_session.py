"""
诊断特定 Session 的文件
=======================
直接从 OSS 读取文件信息，诊断为什么 front 文件大小获取失败

使用示例:
  python -m scanner.diagnose_session
"""

import os
import sys
from pathlib import Path
from dotenv import load_dotenv
import oss2

# 项目根目录
project_root = Path(__file__).parent.parent.parent  # scanner/tools/diagnose_session.py -> scanner/tools -> scanner -> 项目根目录
sys.path.insert(0, str(project_root))

# 加载环境变量
load_dotenv(project_root / ".env")


def diagnose_session(device_id: str, session_id: str):
    """
    诊断特定 session 的文件
    
    参数:
        device_id: 设备ID
        session_id: Session ID
    """
    print("=" * 80)
    print(f"Session 诊断")
    print("=" * 80)
    print(f"设备ID: {device_id}")
    print(f"Session ID: {session_id}")
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
        print(f"✓ 已连接到 OSS bucket: {os.getenv('OSS_BUCKET_NAME')}")
        print()
    except Exception as e:
        print(f"✗ 无法连接到 OSS: {e}")
        return
    
    # 列出该 session 的所有文件
    prefix = f"{device_id}/{session_id}/segments/"
    print(f"正在列出文件: {prefix}")
    print("-" * 80)
    
    try:
        files = []
        for obj in oss2.ObjectIterator(bucket, prefix=prefix):
            files.append(obj)
        
        print(f"✓ 找到 {len(files)} 个文件\n")
        
        # 按文件名排序
        files.sort(key=lambda x: x.key)
        
        # 分析每个文件
        down_files = []
        front_files = []
        
        for obj in files:
            filename = obj.key.split("/")[-1]
            
            # 跳过非 mp4 文件
            if not filename.endswith('.mp4'):
                continue
            
            # 获取文件大小
            try:
                # 方法1: 从列表结果获取
                size_from_list = obj.size
                
                # 方法2: 使用 head_object 获取
                head_result = bucket.head_object(obj.key)
                size_from_head = head_result.content_length
                
                size_mb = size_from_list / (1024 * 1024)
                
                camera_type = "down" if "down" in filename else "front" if "front" in filename else "unknown"
                
                if camera_type == "down":
                    down_files.append((filename, size_from_list, size_from_head))
                elif camera_type == "front":
                    front_files.append((filename, size_from_list, size_from_head))
                
                # 检查两种方法是否一致
                match_status = "✓" if size_from_list == size_from_head else "✗ 不一致!"
                
                print(f"{camera_type:6} | {filename:30} | {size_mb:8.2f} MB | {match_status}")
                
                if size_from_list != size_from_head:
                    print(f"       | 列表大小: {size_from_list}, head_object大小: {size_from_head}")
                
            except Exception as e:
                print(f"✗ 无法获取文件大小: {filename}")
                print(f"  错误: {e}")
        
        print()
        print("=" * 80)
        print("统计结果")
        print("=" * 80)
        print(f"Down 文件数: {len(down_files)}")
        print(f"Front 文件数: {len(front_files)}")
        
        if len(down_files) != len(front_files):
            print(f"\n⚠️  Down 和 Front 文件数量不匹配!")
        
        # 检查是否所有 front 文件大小都能正确获取
        front_size_issues = []
        for filename, size_list, size_head in front_files:
            if size_list == 0 or size_head == 0:
                front_size_issues.append(filename)
        
        if front_size_issues:
            print(f"\n⚠️  发现 {len(front_size_issues)} 个 front 文件大小为 0:")
            for f in front_size_issues:
                print(f"  - {f}")
        else:
            print(f"\n✓ 所有 front 文件大小都能正确获取")
        
        # 对比 down 和 front 的总大小
        total_down = sum(size for _, size, _ in down_files)
        total_front = sum(size for _, size, _ in front_files)
        
        print(f"\nDown 总大小: {total_down / (1024*1024):.2f} MB")
        print(f"Front 总大小: {total_front / (1024*1024):.2f} MB")
        print(f"总计: {(total_down + total_front) / (1024*1024):.2f} MB")
        
        print("=" * 80)
        
    except Exception as e:
        print(f"✗ 列出文件失败: {e}")
        import traceback
        traceback.print_exc()


def main():
    """主函数"""
    # ========================================
    # 配置区域
    # ========================================
    DEVICE_ID = "6ea2"
    SESSION_ID = "session_20260119_125037_597697"
    # ========================================
    
    print()
    print("Session 诊断工具")
    print()
    
    diagnose_session(DEVICE_ID, SESSION_ID)


if __name__ == "__main__":
    main()
