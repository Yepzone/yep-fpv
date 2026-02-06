import lark_oapi as lark
from lark_oapi.api.im.v1 import *

# 1. 初始化客户端（用你的 App ID 和 App Secret）
client = lark.Client.builder() \
    .app_id("cli_a9d1d08c73781e1b") \
    .app_secret("zOOlJhPBbxJswuIWsLf6khCRY6CojEk0") \
    .build()

def get_chat_list():
    """
    获取机器人所在的群聊列表，打印 chat_id 和群名称
    """
    # 构造请求
    request = ListChatRequest.builder() \
        .page_size(50) \
        .build()
    
    # 发起请求
    response = client. im.v1.chat.list(request)
    
    # 检查是否成功
    if not response.success():
        print(f"获取群聊列表失败: {response.code} - {response.msg}")
        return
    
    # 打印所有群聊信息
    print("=" * 60)
    print("机器人所在的群聊列表：")
    print("=" * 60)
    
    if not response.data.items:
        print("未找到任何群聊，请确保：")
        print("1. 机器人已加入至少一个群聊")
        print("2. 机器人拥有 'im:chat' 相关权限")
        return
    
    for idx, chat in enumerate(response.data.items, 1):
        print(f"\n【群聊 {idx}】")
        print(f"  群名称: {chat.name if chat.name else '(无名称)'}")
        print(f"  Chat ID: {chat.chat_id}")
        print(f"  群描述: {chat.description if chat.description else '(无描述)'}")
        print("-" * 60)
    
    print("\n✅ 复制上面的 Chat ID，填入你的发送脚本即可！")

if __name__ == "__main__": 
    get_chat_list()