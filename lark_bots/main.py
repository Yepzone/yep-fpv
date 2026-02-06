import lark_oapi as lark
from lark_oapi.api. im. v1 import *
import json
import os
import argparse
import sys
from dotenv import load_dotenv


def upload_and_send_csv(client, file_path, receive_id, receive_id_type="chat_id"):
    """
    Upload and send a CSV file to Lark/Feishu
    
    Args:
        client:  Lark client instance
        file_path: Path to the CSV file
        receive_id: Receiver ID (open_id, user_id, or chat_id)
        receive_id_type: Type of receiver ID (default: "chat_id")
    """
    # Check if file exists
    if not os.path.exists(file_path):
        print(f"❌ Error: File not found: {file_path}")
        return False
    
    # --- STEP 1: Upload the CSV file ---
    print(f"📤 Uploading file: {file_path}")
    try:
        file_obj = open(file_path, "rb")
        
        # Extract filename from path
        file_name = os.path.basename(file_path)
        
        # Construct upload request
        upload_request = CreateFileRequest. builder() \
            .request_body(CreateFileRequestBody.builder()
                .file_type("stream")
                .file_name(file_name)
                .file(file_obj)
                .build()) \
            .build()
        
        upload_response = client.im.v1.file. create(upload_request)
        
        if not upload_response.success():
            print(f"❌ Upload failed: {upload_response.msg}")
            return False
        
        # Extract the file_key from the response
        file_key = upload_response.data.file_key
        print(f"✅ File uploaded successfully!  Key: {file_key}")
        
    except Exception as e:
        print(f"❌ Error during upload: {e}")
        return False
    finally:
        if 'file_obj' in locals():
            file_obj.close()
    
    # --- STEP 2: Send the Message ---
    print(f"📨 Sending message to {receive_id_type}: {receive_id}")
    try:
        content_dict = {"file_key": file_key}
        send_request = CreateMessageRequest.builder() \
            .receive_id_type(receive_id_type) \
            .request_body(CreateMessageRequestBody. builder()
                .receive_id(receive_id)
                .msg_type("file")
                .content(json. dumps(content_dict))
                .build()) \
            .build()
        
        send_response = client.im.v1.message.create(send_request)
        
        if send_response.success():
            print("✅ CSV sent successfully!")
            return True
        else:
            print(f"❌ Send failed: {send_response.msg}")
            return False
            
    except Exception as e: 
        print(f"❌ Error during send: {e}")
        return False


def main():
    # 加载 .env 文件到环境变量
    load_dotenv(override=False)

    # 获取脚本所在目录的绝对路径
    SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
    
    # Parse command line arguments
    parser = argparse.ArgumentParser(
        description='Upload and send CSV file to Lark/Feishu',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog='''
Examples:
  # 使用所有默认参数（从 .env 读取）
  python send_csv_to_lark.py
  
  # 只修改文件路径
  python send_csv_to_lark.py --file report.csv
  
  # 发送到不同的群
  python send_csv_to_lark.py --receive-id oc_另一个群ID
  
  # 完全自定义
  python send_csv_to_lark.py -f custom.csv -r ou_user123 -t open_id
        '''
    )
    
    # 所有参数都从 .env 读取默认值，命令行参数可覆盖
    parser. add_argument(
        '--file', '-f',
        default=os.path.join(SCRIPT_DIR, 'data.csv'),
        help=f'Path to the CSV file to send (default: {os.getenv("DEFAULT_CSV_FILE", "not set")})'
    )
    
    parser.add_argument(
        '--receive-id', '-r',
        default=os.getenv('DEFAULT_RECEIVE_ID'),
        help=f'Receiver ID (default: {os.getenv("DEFAULT_RECEIVE_ID", "not set")})'
    )
    
    parser.add_argument(
        '--receive-type', '-t',
        choices=['open_id', 'user_id', 'chat_id'],
        default=os.getenv('DEFAULT_RECEIVE_TYPE', 'chat_id'),
        help=f'Type of receiver ID (default:  {os.getenv("DEFAULT_RECEIVE_TYPE", "chat_id")})'
    )
    
    parser.add_argument(
        '--app-id',
        default=os.getenv('LARK_APP_ID'),
        help='Lark App ID (from LARK_APP_ID in .env)'
    )
    
    parser.add_argument(
        '--app-secret',
        default=os.getenv('LARK_APP_SECRET'),
        help='Lark App Secret (from LARK_APP_SECRET in .env)'
    )
    
    args = parser.parse_args()
    
    # 验证必需参数
    if not args.file:
        print("❌ Error: --file is required or set DEFAULT_CSV_FILE in . env")
        print("💡 Tip: Check your .env file or use:  python send_csv_to_lark.py --file data.csv")
        sys.exit(1)
    
    if not args.receive_id:
        print("❌ Error:  --receive-id is required or set DEFAULT_RECEIVE_ID in . env")
        print("💡 Tip: Check your .env file or use: python send_csv_to_lark.py --receive-id oc_xxx")
        sys.exit(1)
    
    if not args.app_id or not args.app_secret:
        print("❌ Error:  LARK_APP_ID and LARK_APP_SECRET are required in .env")
        print("💡 Tip: Check your .env file contains:")
        print("   LARK_APP_ID=cli_xxx")
        print("   LARK_APP_SECRET=xxx")
        sys.exit(1)
    
    # 打印使用的配置（方便调试）
    print("=" * 50)
    print("📋 Configuration:")
    print(f"   File: {args.file}")
    print(f"   Receive ID: {args.receive_id}")
    print(f"   Receive Type: {args.receive_type}")
    print(f"   App ID: {args. app_id[: 15]}...")
    print("=" * 50)
    
    # Initialize Lark Client
    print("🔧 Initializing Lark client...")
    client = lark.Client. builder() \
        .app_id(args.app_id) \
        .app_secret(args.app_secret) \
        .build()
    
    # Upload and send
    success = upload_and_send_csv(
        client=client,
        file_path=args.file,
        receive_id=args.receive_id,
        receive_id_type=args.receive_type
    )
    
    # Exit with appropriate code
    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()