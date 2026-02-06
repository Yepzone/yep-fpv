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
echo "构建Docker镜像..."
docker build -t fpv-scanner:latest .

# 清理旧容器
echo "清理旧版本..."
docker stop fpv-scanner 2>/dev/null || true
docker rm fpv-scanner 2>/dev/null || true

# 运行新容器
echo "启动FPV扫描器..."
docker run -d \
    --name fpv-scanner \
    -v /opt/fpv-scanner/logs:/app/logs \
    -v /opt/fpv-scanner/csv:/app/output \
    --env-file .env \
    fpv-scanner:latest  

echo "部署完成！"
echo "查看日志: docker logs -f fpv-scanner"