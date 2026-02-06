FROM python:3.11-slim

# 安装系统依赖
RUN apt-get update && apt-get install -y libpq-dev gcc && rm -rf /var/lib/apt/lists/*

# 设置时区
ENV TZ=Asia/Shanghai
RUN ln -snf /usr/share/zoneinfo/$TZ /etc/localtime && echo $TZ > /etc/timezone

WORKDIR /app
COPY . .

# 创建目录（用于挂载）
RUN mkdir -p /app/logs /app/output

# 设置环境变量默认值
ENV LOG_DIR=/app/logs
ENV OUTPUT_DIR=/app/output

# 安装依赖
RUN pip install uv && uv sync

# 默认启动命令（定时任务用）
# 可通过 docker run 时指定不同命令覆盖
CMD ["./scan_fpv.sh"]

# 使用示例:
# 方式1 - 定时扫库（一次性执行，配合 crontab）:
#   docker run --rm --env-file .env fpv-scanner:latest
#   docker run --rm --env-file .env fpv-scanner:latest ./scan_fpv.sh 2025-01-01 2025-01-15
#
# 方式2 - Lark机器人服务（持续运行）:
#   docker run -d --name fpv-lark-bot --env-file .env fpv-scanner:latest uv run python -m lark_bots.scan_service