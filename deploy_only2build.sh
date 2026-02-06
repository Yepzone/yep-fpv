#!/bin/bash
set -e

echo "=== FPV扫描器Docker部署 ==="

# 检查Docker
if ! docker --version >/dev/null 2>&1; then
    echo "错误：Docker未安装"
    exit 1
fi

# 创建宿主机目录
echo "创建数据目录..."
sudo mkdir -p /opt/fpv-scanner/{logs,csv,backup}

# 构建镜像
echo "构建Docker镜像并压缩..."
docker build -t fpv-scanner:latest .
docker save -o fpv-scanner.tar fpv-scanner:latest
gzip fpv-scanner.tar

# 清理旧容器
echo "清理旧版本..."
docker stop fpv-scanner 2>/dev/null || true
docker rm fpv-scanner 2>/dev/null || true

