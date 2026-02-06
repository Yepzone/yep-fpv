"""
Lark 群消息监听服务 - 轮询模式
===================================
功能:
1. 定时轮询 Lark 群消息
2. /scan - 扫库命令
3. /export - 导出CSV命令
4. @机器人 - 显示帮助信息
5. 执行期间拒绝新请求

启动服务:
  python -m lark_bots.scan_service
"""

import json
import logging
import os
import re
import subprocess
import sys
import threading
import time
from dataclasses import dataclass
from datetime import datetime, date
from typing import Optional, Set
from logging.handlers import RotatingFileHandler

import lark_oapi as lark
from lark_oapi.api.im.v1 import *
from dotenv import load_dotenv

# ==============================================================================
# 配置
# ==============================================================================

load_dotenv(override=False)  # 不覆盖已有环境变量（Docker传入的优先）

LARK_APP_ID = os.getenv("LARK_APP_ID")
LARK_APP_SECRET = os.getenv("LARK_APP_SECRET")
MONITORED_CHAT_ID = os.getenv("LARK_MONITORED_CHAT_ID")

# 轮询间隔（秒）
POLL_INTERVAL = int(os.getenv("POLL_INTERVAL", "3"))

# 日志配置
LOG_DIR = os.getenv("LOG_DIR", ".")
LOG_FILE = os.path.join(LOG_DIR, "lark_bot.log")

# 确保日志目录存在
os.makedirs(LOG_DIR, exist_ok=True)

# 配置日志：同时输出到控制台和文件
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# 控制台输出
console_handler = logging.StreamHandler()
console_handler.setFormatter(logging.Formatter('%(asctime)s [%(levelname)s] %(message)s', '%Y-%m-%d %H:%M:%S'))
logger.addHandler(console_handler)

# 文件输出（自动轮转，最大5MB，保留3个备份）
file_handler = RotatingFileHandler(LOG_FILE, maxBytes=5*1024*1024, backupCount=3, encoding='utf-8')
file_handler.setFormatter(logging.Formatter('%(asctime)s [%(levelname)s] %(message)s', '%Y-%m-%d %H:%M:%S'))
logger.addHandler(file_handler)

# 帮助信息
HELP_MESSAGE = """📖 FPV扫库机器人使用指南

🔍 扫库命令 /scan
扫描OSS并写入数据库，生成增量CSV

格式:
  /scan <设备ID> <日期>
  /scan <设备ID> <开始日期> <结束日期>

示例:
  /scan 7393 2025-01-15
  /scan 7393 2025-01-01 2025-01-15

📤 导出命令 /export
从数据库导出格式化CSV（含视频链接）

格式:
  /export <日期>
  /export <开始日期> <结束日期>
  /export all

示例:
  /export 2025-01-15
  /export 2025-01-01 2025-01-15
  /export all

💡 提示: 日期格式为 YYYY-MM-DD"""


# ==============================================================================
# 命令解析
# ==============================================================================

@dataclass
class ScanCommand:
    device_id: str
    start_date: date
    end_date: date
    
    def __str__(self):
        return f"设备={self.device_id}, 日期={self.start_date}~{self.end_date}"


@dataclass
class ExportCommand:
    start_date: Optional[date]
    end_date: Optional[date]
    export_all: bool = False
    
    def __str__(self):
        if self.export_all:
            return "导出全部数据"
        return f"日期={self.start_date}~{self.end_date}"


def parse_scan_command(text: str) -> Optional[ScanCommand]:
    """解析 /scan 命令"""
    text = text.strip()
    
    if not text.lower().startswith("/scan"):
        return None
    
    args_text = text[5:].strip()
    
    if not args_text:
        return None
    
    parts = args_text.split()
    if len(parts) < 1:
        return None
    
    device_id = parts[0]
    if not re.match(r'^[\w-]+$', device_id) or 'date' in device_id.lower():
        return None
    
    try:
        if len(parts) == 1:
            start_date = date.today()
            end_date = date.today()
        elif len(parts) == 2:
            start_date = datetime.strptime(parts[1], "%Y-%m-%d").date()
            end_date = start_date
        else:
            start_date = datetime.strptime(parts[1], "%Y-%m-%d").date()
            end_date = datetime.strptime(parts[2], "%Y-%m-%d").date()
        
        return ScanCommand(device_id=device_id, start_date=start_date, end_date=end_date)
    except ValueError:
        return None


def parse_export_command(text: str) -> Optional[ExportCommand]:
    """解析 /export 命令"""
    text = text.strip()
    
    if not text.lower().startswith("/export"):
        return None
    
    args_text = text[7:].strip()
    
    # /export all
    if args_text.lower() == "all":
        return ExportCommand(start_date=None, end_date=None, export_all=True)
    
    if not args_text:
        # 默认导出今天
        today = date.today()
        return ExportCommand(start_date=today, end_date=today)
    
    parts = args_text.split()
    
    try:
        if len(parts) == 1:
            start_date = datetime.strptime(parts[0], "%Y-%m-%d").date()
            end_date = start_date
        else:
            start_date = datetime.strptime(parts[0], "%Y-%m-%d").date()
            end_date = datetime.strptime(parts[1], "%Y-%m-%d").date()
        
        return ExportCommand(start_date=start_date, end_date=end_date)
    except ValueError:
        return None


# ==============================================================================
# Lark 客户端
# ==============================================================================

class LarkClient:
    def __init__(self, app_id: str, app_secret: str):
        self.client = lark.Client.builder() \
            .app_id(app_id) \
            .app_secret(app_secret) \
            .build()
    
    def send_text_message(self, chat_id: str, text: str) -> bool:
        try:
            content = json.dumps({"text": text})
            request = CreateMessageRequest.builder() \
                .receive_id_type("chat_id") \
                .request_body(CreateMessageRequestBody.builder()
                    .receive_id(chat_id)
                    .msg_type("text")
                    .content(content)
                    .build()) \
                .build()
            
            response = self.client.im.v1.message.create(request)
            
            if response.success():
                logger.info(f"✓ 消息发送成功")
                return True
            else:
                logger.error(f"✗ 消息发送失败: {response.msg}")
                return False
        except Exception as e:
            logger.error(f"✗ 发送消息异常: {e}")
            return False
    
    def send_file(self, chat_id: str, file_path: str) -> bool:
        if not os.path.exists(file_path):
            logger.error(f"✗ 文件不存在: {file_path}")
            return False
        
        try:
            with open(file_path, "rb") as f:
                upload_request = CreateFileRequest.builder() \
                    .request_body(CreateFileRequestBody.builder()
                        .file_type("stream")
                        .file_name(os.path.basename(file_path))
                        .file(f)
                        .build()) \
                    .build()
                
                upload_response = self.client.im.v1.file.create(upload_request)
                
                if not upload_response.success():
                    logger.error(f"✗ 文件上传失败: {upload_response.msg}")
                    return False
                
                file_key = upload_response.data.file_key
            
            content = json.dumps({"file_key": file_key})
            send_request = CreateMessageRequest.builder() \
                .receive_id_type("chat_id") \
                .request_body(CreateMessageRequestBody.builder()
                    .receive_id(chat_id)
                    .msg_type("file")
                    .content(content)
                    .build()) \
                .build()
            
            send_response = self.client.im.v1.message.create(send_request)
            
            if send_response.success():
                logger.info(f"✓ 文件发送成功")
                return True
            else:
                logger.error(f"✗ 文件发送失败: {send_response.msg}")
                return False
                
        except Exception as e:
            logger.error(f"✗ 发送文件异常: {e}")
            return False
    
    def get_chat_messages(self, chat_id: str, page_size: int = 20) -> list:
        try:
            request = ListMessageRequest.builder() \
                .container_id_type("chat") \
                .container_id(chat_id) \
                .sort_type("ByCreateTimeDesc") \
                .page_size(page_size) \
                .build()
            
            response = self.client.im.v1.message.list(request)
            
            if response.success() and response.data and response.data.items:
                return response.data.items
            else:
                if not response.success():
                    logger.debug(f"获取消息失败: {response.code} - {response.msg}")
                return []
        except Exception as e:
            logger.error(f"获取消息异常: {e}")
            return []


# ==============================================================================
# 任务执行器
# ==============================================================================

class TaskExecutor:
    """任务执行器 (线程安全)"""
    
    def __init__(self, lark_client: LarkClient):
        self.lark_client = lark_client
        self._lock = threading.Lock()
        self._is_running = False
        self._current_task: Optional[str] = None
    
    @property
    def is_running(self) -> bool:
        with self._lock:
            return self._is_running
    
    @property
    def current_task(self) -> Optional[str]:
        with self._lock:
            return self._current_task
    
    def try_execute_scan(self, command: ScanCommand, chat_id: str) -> bool:
        """尝试执行扫库任务"""
        with self._lock:
            if self._is_running:
                return False
            self._is_running = True
            self._current_task = f"扫库: {command}"
        
        thread = threading.Thread(target=self._run_scan, args=(command, chat_id), daemon=True)
        thread.start()
        return True
    
    def try_execute_export(self, command: ExportCommand, chat_id: str) -> bool:
        """尝试执行导出任务"""
        with self._lock:
            if self._is_running:
                return False
            self._is_running = True
            self._current_task = f"导出: {command}"
        
        thread = threading.Thread(target=self._run_export, args=(command, chat_id), daemon=True)
        thread.start()
        return True
    
    def _run_scan(self, command: ScanCommand, chat_id: str):
        """执行扫库脚本"""
        start_time = datetime.now()
        project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        
        try:
            logger.info(f"开始执行扫库: {command}")
            
            self.lark_client.send_text_message(
                chat_id,
                f"🚀 开始扫库\n"
                f"设备: {command.device_id}\n"
                f"日期: {command.start_date} ~ {command.end_date}"
            )
            
            cmd = [
                sys.executable, "-m", "scanner.scan.info_scan",
                "--device-id", command.device_id,
                "--start-date", str(command.start_date),
                "--end-date", str(command.end_date),
            ]
            
            logger.info(f"执行命令: {' '.join(cmd)}")
            
            result = subprocess.run(cmd, capture_output=True, text=True, cwd=project_root, encoding='utf-8', errors='replace')
            
            duration = (datetime.now() - start_time).total_seconds()
            
            if result.returncode == 0:
                logger.info(f"✓ 扫库完成，耗时 {duration:.1f}s")
                
                stats_msg = self._extract_stats(result.stdout)
                
                self.lark_client.send_text_message(
                    chat_id,
                    f"✅ 扫库完成\n"
                    f"设备: {command.device_id}\n"
                    f"日期: {command.start_date} ~ {command.end_date}\n"
                    f"耗时: {duration:.1f}秒\n"
                    f"{stats_msg}"
                )
                
                self._send_latest_csv(chat_id, project_root)
            else:
                logger.error(f"✗ 扫库失败: {result.stderr}")
                error_msg = result.stderr[-500:] if len(result.stderr) > 500 else result.stderr
                self.lark_client.send_text_message(
                    chat_id,
                    f"❌ 扫库失败\n设备: {command.device_id}\n错误: {error_msg}"
                )
        
        except Exception as e:
            logger.error(f"✗ 扫库异常: {e}", exc_info=True)
            self.lark_client.send_text_message(chat_id, f"❌ 扫库异常\n错误: {str(e)}")
        
        finally:
            with self._lock:
                self._is_running = False
                self._current_task = None
    
    def _run_export(self, command: ExportCommand, chat_id: str):
        """执行导出脚本"""
        start_time = datetime.now()
        project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        
        try:
            logger.info(f"开始执行导出: {command}")
            
            if command.export_all:
                desc = "全部数据"
            else:
                desc = f"{command.start_date} ~ {command.end_date}"
            
            self.lark_client.send_text_message(
                chat_id,
                f"📤 开始导出CSV\n日期范围: {desc}"
            )
            
            cmd = [sys.executable, "-m", "scanner.scan.export_formatted_csv"]
            
            if command.export_all:
                cmd.append("--all")
            else:
                cmd.extend(["--start-date", str(command.start_date)])
                cmd.extend(["--end-date", str(command.end_date)])
            
            logger.info(f"执行命令: {' '.join(cmd)}")
            
            result = subprocess.run(cmd, capture_output=True, text=True, cwd=project_root, encoding='utf-8', errors='replace')
            
            duration = (datetime.now() - start_time).total_seconds()
            
            if result.returncode == 0:
                logger.info(f"✓ 导出完成，耗时 {duration:.1f}s")
                
                # 提取导出行数
                row_match = re.search(r'导出行数:\s*(\d+)', result.stdout)
                row_count = row_match.group(1) if row_match else "未知"
                
                self.lark_client.send_text_message(
                    chat_id,
                    f"✅ 导出完成\n"
                    f"日期范围: {desc}\n"
                    f"导出记录: {row_count} 条\n"
                    f"耗时: {duration:.1f}秒"
                )
                
                self._send_latest_csv(chat_id, project_root, prefix="formatted")
            else:
                logger.error(f"✗ 导出失败: {result.stderr}")
                error_msg = result.stderr[-500:] if len(result.stderr) > 500 else result.stderr
                self.lark_client.send_text_message(
                    chat_id,
                    f"❌ 导出失败\n错误: {error_msg}"
                )
        
        except Exception as e:
            logger.error(f"✗ 导出异常: {e}", exc_info=True)
            self.lark_client.send_text_message(chat_id, f"❌ 导出异常\n错误: {str(e)}")
        
        finally:
            with self._lock:
                self._is_running = False
                self._current_task = None
    
    def _extract_stats(self, output: str) -> str:
        stats = []
        patterns = [
            (r'新增会话数:\s*(\d+)', '新增会话'),
            (r'新增视频段数:\s*(\d+)', '新增视频段'),
            (r'导出行数:\s*(\d+)', '导出记录'),
        ]
        for pattern, label in patterns:
            match = re.search(pattern, output)
            if match:
                stats.append(f"{label}: {match.group(1)}")
        return '\n'.join(stats) if stats else ""
    
    def _send_latest_csv(self, chat_id: str, project_root: str, prefix: str = ""):
        """发送最新生成的 CSV 文件"""
        csv_dir = os.path.join(project_root, "ExportedCSV")
        if not os.path.exists(csv_dir):
            return
        
        csv_files = [f for f in os.listdir(csv_dir) if f.endswith('.csv')]
        if prefix:
            csv_files = [f for f in csv_files if f.startswith(prefix)]
        
        if not csv_files:
            return
        
        latest_csv = max(csv_files, key=lambda f: os.path.getmtime(os.path.join(csv_dir, f)))
        csv_path = os.path.join(csv_dir, latest_csv)
        
        # 5分钟内生成的文件才发送
        if (datetime.now().timestamp() - os.path.getmtime(csv_path)) < 300:
            self.lark_client.send_file(chat_id, csv_path)


# ==============================================================================
# 消息轮询服务
# ==============================================================================

class MessagePoller:
    def __init__(self, lark_client: LarkClient, executor: TaskExecutor, chat_id: str):
        self.lark_client = lark_client
        self.executor = executor
        self.chat_id = chat_id
        self.processed_ids: Set[str] = set()
        self._running = False
    
    def start(self):
        self._running = True
        
        logger.info("初始化：获取现有消息...")
        messages = self.lark_client.get_chat_messages(self.chat_id, page_size=50)
        for msg in messages:
            self.processed_ids.add(msg.message_id)
        logger.info(f"已标记 {len(self.processed_ids)} 条历史消息")
        
        logger.info(f"开始轮询，间隔 {POLL_INTERVAL} 秒")
        
        while self._running:
            try:
                self._poll_once()
            except Exception as e:
                logger.error(f"轮询异常: {e}")
            
            time.sleep(POLL_INTERVAL)
    
    def stop(self):
        self._running = False
    
    def _poll_once(self):
        messages = self.lark_client.get_chat_messages(self.chat_id, page_size=10)
        
        for msg in messages:
            msg_id = msg.message_id
            
            if msg_id in self.processed_ids:
                continue
            
            self.processed_ids.add(msg_id)
            
            msg_type = msg.msg_type
            if msg_type != "text":
                continue
            
            try:
                content = json.loads(msg.body.content)
                text = content.get("text", "")
            except Exception as e:
                logger.debug(f"解析消息内容失败: {e}")
                continue
            
            logger.info(f"新消息: {text}")
            
            self._handle_message(text)
    
    def _handle_message(self, text: str):
        """处理消息"""
        text_lower = text.lower().strip()
        
        # 检查是否 @机器人 或请求帮助
        if "@" in text or text_lower in ["help", "帮助", "?", "？"]:
            self.lark_client.send_text_message(self.chat_id, HELP_MESSAGE)
            return
        
        # 解析 /scan 命令
        if text_lower.startswith("/scan"):
            scan_cmd = parse_scan_command(text)
            if scan_cmd is None:
                self.lark_client.send_text_message(
                    self.chat_id,
                    "❓ /scan 命令格式错误\n\n"
                    "正确格式:\n"
                    "/scan <设备ID> <日期>\n"
                    "/scan <设备ID> <开始日期> <结束日期>\n\n"
                    "示例:\n"
                    "/scan 7393 2025-01-15"
                )
                return
            
            logger.info(f"解析到扫库命令: {scan_cmd}")
            
            if self.executor.try_execute_scan(scan_cmd, self.chat_id):
                logger.info("扫库任务已启动")
            else:
                self.lark_client.send_text_message(
                    self.chat_id,
                    f"⏳ 正在执行任务中，请稍后再试\n当前任务: {self.executor.current_task}"
                )
            return
        
        # 解析 /export 命令
        if text_lower.startswith("/export"):
            export_cmd = parse_export_command(text)
            if export_cmd is None:
                self.lark_client.send_text_message(
                    self.chat_id,
                    "❓ /export 命令格式错误\n\n"
                    "正确格式:\n"
                    "/export <日期>\n"
                    "/export <开始日期> <结束日期>\n"
                    "/export all\n\n"
                    "示例:\n"
                    "/export 2025-01-15\n"
                    "/export all"
                )
                return
            
            logger.info(f"解析到导出命令: {export_cmd}")
            
            if self.executor.try_execute_export(export_cmd, self.chat_id):
                logger.info("导出任务已启动")
            else:
                self.lark_client.send_text_message(
                    self.chat_id,
                    f"⏳ 正在执行任务中，请稍后再试\n当前任务: {self.executor.current_task}"
                )
            return


# ==============================================================================
# Main
# ==============================================================================

def main():
    logger.info("=" * 60)
    logger.info("Lark 扫库机器人 (轮询模式)")
    logger.info("=" * 60)
    
    if not LARK_APP_ID or not LARK_APP_SECRET:
        logger.error("✗ 缺少 LARK_APP_ID 或 LARK_APP_SECRET")
        sys.exit(1)
    
    if not MONITORED_CHAT_ID:
        logger.error("✗ 缺少 LARK_MONITORED_CHAT_ID")
        sys.exit(1)
    
    logger.info(f"监听群 ID: {MONITORED_CHAT_ID}")
    logger.info(f"轮询间隔: {POLL_INTERVAL} 秒")
    
    lark_client = LarkClient(LARK_APP_ID, LARK_APP_SECRET)
    executor = TaskExecutor(lark_client)
    poller = MessagePoller(lark_client, executor, MONITORED_CHAT_ID)
    
    logger.info("-" * 60)
    logger.info("支持的命令:")
    logger.info("  /scan <设备ID> <日期>  - 扫库")
    logger.info("  /export <日期>         - 导出CSV")
    logger.info("  @机器人 或 help        - 显示帮助")
    logger.info("-" * 60)
    
    try:
        poller.start()
    except KeyboardInterrupt:
        logger.info("\n服务已停止")
        poller.stop()


if __name__ == "__main__":
    main()
