#!/bin/bash
set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
echo "=== FPV扫描器便携式部署 ==="
echo "部署目录: $SCRIPT_DIR"

# 检查Docker
if ! docker --version >/dev/null 2>&1; then
    echo "错误：Docker未安装"
    exit 1
fi

# 检查镜像文件是否存在
if [ ! -f "$SCRIPT_DIR/fpv-scanner.tar.gz" ]; then
    echo "错误：未找到镜像文件 fpv-scanner.tar.gz"
    echo "请确保镜像文件与部署脚本在同一目录"
    exit 1
fi

# 检查环境文件
if [ ! -f "$SCRIPT_DIR/.env" ]; then
    echo "警告：未找到 .env 文件，将使用容器内默认配置"
fi

# 创建宿主机目录
echo "创建数据目录..."
sudo mkdir -p /opt/fpv-scanner/{logs,csv,backup}

# 加载镜像
echo "加载Docker镜像..."
docker load -i "$SCRIPT_DIR/fpv-scanner.tar.gz"

# 清理旧容器
echo "清理旧版本..."
docker stop fpv-scanner 2>/dev/null || true
docker rm fpv-scanner 2>/dev/null || true

# 运行新容器
echo "启动FPV扫描器..."
if [ -f "$SCRIPT_DIR/.env" ]; then
    docker run -d \
        --name fpv-scanner \
        -v /opt/fpv-scanner/logs:/app/logs \
        -v /opt/fpv-scanner/csv:/app/output \
        --env-file "$SCRIPT_DIR/.env" \
        fpv-scanner:latest
else
    docker run -d \
        --name fpv-scanner \
        -v /opt/fpv-scanner/logs:/app/logs \
        -v /opt/fpv-scanner/csv:/app/output \
        fpv-scanner:latest
fi

echo "部署完成！"
echo "查看日志: docker logs -f fpv-scanner"
echo "停止容器: docker stop fpv-scanner"
echo "启动容器: docker start fpv-scanner"
echo "删除容器: docker rm fpv-scanner"