import os
import sys

import oss2
import psycopg2
from dotenv import load_dotenv

load_dotenv(override=True)

TELEOP_DB_HOST = os.getenv("TELEOP_DB_HOST", "localhost")
TELEOP_DB_NAME = os.getenv("TELEOP_DB_NAME")
TELEOP_DB_PASSWORD = os.getenv("TELEOP_DB_PASSWORD")
TELEOP_DB_PORT = int(os.getenv("TELEOP_DB_PORT", "5432"))
TELEOP_DB_USER = os.getenv("TELEOP_DB_USER")

OSS_ACCESS_KEY_ID = os.getenv("OSS_ACCESS_KEY_ID")
OSS_ACCESS_KEY_SECRET = os.getenv("OSS_ACCESS_KEY_SECRET")
OSS_BUCKET_NAME = os.getenv("OSS_BUCKET_NAME")
OSS_ENDPOINT = os.getenv("OSS_ENDPOINT")
OSS_REGION = os.getenv("OSS_REGION")

SLACK_TOKEN = os.getenv("SLACK_TOKEN")

LARK_APP_ID = os.getenv("LARK_APP_ID")
LARK_APP_SECRET = os.getenv("LARK_APP_SECRET")
LARK_FOLDER_TOKEN = os.getenv("LARK_FOLDER_TOKEN")


def get_db_conn():
    return psycopg2.connect(
        host=TELEOP_DB_HOST,
        port=TELEOP_DB_PORT,
        dbname=TELEOP_DB_NAME,
        user=TELEOP_DB_USER,
        password=TELEOP_DB_PASSWORD,
        connect_timeout=10,
    )


def get_oss_bucket():
    if not all(
            [
                OSS_ACCESS_KEY_ID,
                OSS_ACCESS_KEY_SECRET,
                OSS_BUCKET_NAME,
                OSS_ENDPOINT,
                OSS_REGION,
            ]
    ):
        print("Missing OSS credentials or bucket/env.", file=sys.stderr)
        sys.exit(2)

    auth = oss2.Auth(OSS_ACCESS_KEY_ID, OSS_ACCESS_KEY_SECRET)
    return oss2.Bucket(
        auth=auth, endpoint=OSS_ENDPOINT, bucket_name=OSS_BUCKET_NAME, region=OSS_REGION
    )
