"""
简单测试 Lark WebSocket 消息接收
"""
import os
import json
import lark_oapi as lark
from lark_oapi.api.im.v1 import P2ImMessageReceiveV1
from dotenv import load_dotenv

load_dotenv(override=False)

LARK_APP_ID = os.getenv("LARK_APP_ID")
LARK_APP_SECRET = os.getenv("LARK_APP_SECRET")

print(f"APP_ID: {LARK_APP_ID[:10]}..." if LARK_APP_ID else "APP_ID: 未设置")
print(f"APP_SECRET: {LARK_APP_SECRET[:5]}..." if LARK_APP_SECRET else "APP_SECRET: 未设置")


def on_message(ctx: lark.EventContext, event: P2ImMessageReceiveV1):
    """收到消息时的回调"""
    print("\n" + "=" * 50)
    print("收到消息事件!")
    print("=" * 50)
    
    try:
        message = event.event.message
        print(f"消息ID: {message.message_id}")
        print(f"消息类型: {message.message_type}")
        print(f"Chat ID: {message.chat_id}")
        print(f"发送者: {event.event.sender.sender_id.open_id}")
        
        if message.message_type == "text":
            content = json.loads(message.content)
            print(f"消息内容: {content.get('text', '')}")
    except Exception as e:
        print(f"解析消息出错: {e}")
    
    print("=" * 50 + "\n")


# 创建事件处理器
event_handler = lark.EventDispatcherHandler.builder("", "") \
    .register_p2_im_message_receive_v1(on_message) \
    .build()

# 创建 WebSocket 客户端
ws_client = lark.ws.Client(
    LARK_APP_ID,
    LARK_APP_SECRET,
    event_handler=event_handler,
    log_level=lark.LogLevel.DEBUG  # 开启 DEBUG 日志
)

print("\n启动 WebSocket 监听...")
print("请在 Lark 群里发送任意消息测试\n")

ws_client.start()
