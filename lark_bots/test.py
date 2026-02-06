import lark_oapi as lark
from lark_oapi.api.im.v1 import *
import json
import os
# 1. Initialize Client
client = lark.Client.builder() \
    .app_id("cli_a9d1d72a4b381e1a") \
    .app_secret("QJMhcECCGDp2C7y8i3tLxw6DkiaGdLVX") \
    .build()
def upload_and_send_csv(file_path, receive_id,receive_id_type="open_id"):
    # --- STEP 1: Upload the CSV file ---
    # We must open the file in binary mode ('rb')
    file_obj = open(file_path, "rb")
    # Construct upload request
    upload_request = CreateFileRequest.builder() \
        .request_body(CreateFileRequestBody.builder()
            .file_type("stream")   # 'stream' is often used for generic files
            .file_name("report.csv")
            .file(file_obj)
            .build()) \
        .build()
    upload_response = client.im.v1.file.create(upload_request)
    if not upload_response.success():
        print(f"Upload failed: {upload_response.msg}")
        return
    # Extract the file_key from the response
    file_key = upload_response.data.file_key
    print(f"File uploaded successfully! Key: {file_key}")
    # --- STEP 2: Send the Message ---
    # The content must be a JSON string containing the file_key
    content_dict = {"file_key": file_key}
    send_request = CreateMessageRequest.builder() \
        .receive_id_type(receive_id_type) \
        .request_body(CreateMessageRequestBody.builder()
            .receive_id(receive_id)
            .msg_type("file")  # Important: msg_type is 'file'
            .content(json.dumps(content_dict))
            .build()) \
        .build()
    send_response = client.im.v1.message.create(send_request)
    if send_response.success():
        print("CSV sent successfully!")
    else:
        print(f"Send failed: {send_response.msg}")
# Usage
# receive_id can be open_id, user_id, or chat_id (if you change receive_id_type above)
csv_file = os.path.join(os.path.dirname(__file__), "data.csv")
upload_and_send_csv(csv_file, "oc_99e7de1f2f508f08d155428a4cf56a8c", receive_id_type="chat_id")