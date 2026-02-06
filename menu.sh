#!/bin/bash
# ==============================================================================
# FPV 数据管理工具 - 交互式菜单
# ==============================================================================

# 颜色定义
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
CYAN='\033[0;36m'
NC='\033[0m' # No Color

# 分隔线
LINE="━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"

# 打印带颜色的标题
print_header() {
    echo -e "${CYAN}${LINE}${NC}"
    echo -e "${CYAN}   🎮 FPV 数据管理工具${NC}"
    echo -e "${CYAN}${LINE}${NC}"
    echo ""
}

# 打印菜单选项
print_menu() {
    echo -e "${CYAN}⭐ 常用操作${NC}"
    echo -e "  ${GREEN}1)${NC} 🔍 扫描数据               - 扫描OSS并写入数据库"
    echo -e "  ${GREEN}2)${NC} 📤 导出CSV                - 导出带视频链接的CSV"
    echo ""
    echo -e "${YELLOW}── 其他功能 ──${NC}"
    echo -e "  ${GREEN}3)${NC} 仅扫描Metadata            ${GREEN}6)${NC} 启动Lark监听服务"
    echo -e "  ${GREEN}4)${NC} 验证设备时长              ${GREEN}7)${NC} 发送CSV到Lark群"
    echo -e "  ${GREEN}5)${NC} 验证Session Segments      ${GREEN}8)${NC} 添加设备"
    echo ""
    echo -e "  ${RED}0)${NC} 退出"
    echo ""
    echo -e "${LINE}"
}

# 读取日期输入
read_date() {
    local prompt=$1
    local default=$2
    local result
    
    if [ -n "$default" ]; then
        read -p "$prompt [$default]: " result
        result=${result:-$default}
    else
        read -p "$prompt: " result
    fi
    echo "$result"
}

# 读取设备ID
read_device_id() {
    local device_id
    read -p "设备ID (留空扫描所有设备): " device_id
    echo "$device_id"
}

# 显示活跃设备列表
show_active_devices() {
    echo -e "\n${CYAN}正在查询近期活跃设备...${NC}\n"
    uv run -m scanner.tools.list_active_devices 2>/dev/null || echo -e "${YELLOW}(无法连接数据库，跳过设备列表)${NC}"
}

# 1. 扫描数据 (info_scan)
do_info_scan() {
    echo -e "\n${CYAN}=== 扫描数据 (info_scan) ===${NC}"
    
    # 先显示活跃设备
    show_active_devices
    
    echo ""
    local device_id=$(read_device_id)
    local today=$(date +%Y-%m-%d)
    local start_date=$(read_date "开始日期 (YYYY-MM-DD)" "$today")
    local end_date=$(read_date "结束日期 (YYYY-MM-DD)" "$start_date")
    
    echo ""
    echo -e "${YELLOW}执行命令:${NC}"
    
    local cmd="uv run -m scanner.scan.info_scan --start-date $start_date --end-date $end_date"
    if [ -n "$device_id" ]; then
        cmd="$cmd --device-id $device_id"
    fi
    
    echo -e "${GREEN}$cmd${NC}\n"
    read -p "确认执行? (y/n) " confirm
    
    if [ "$confirm" = "y" ] || [ "$confirm" = "Y" ]; then
        eval $cmd
    fi
}

# 2. 仅扫描Metadata
do_metadata_scan() {
    echo -e "\n${CYAN}=== 仅扫描Metadata ===${NC}\n"
    
    local device_id=$(read_device_id)
    local today=$(date +%Y-%m-%d)
    local start_date=$(read_date "开始日期 (YYYY-MM-DD)" "$today")
    local end_date=$(read_date "结束日期 (YYYY-MM-DD)" "$start_date")
    
    echo ""
    echo -e "${YELLOW}执行命令:${NC}"
    
    local cmd="uv run -m scanner.scan.metadata_scan --start-date $start_date --end-date $end_date"
    if [ -n "$device_id" ]; then
        cmd="$cmd --device-id $device_id"
    fi
    
    echo -e "${GREEN}$cmd${NC}\n"
    read -p "确认执行? (y/n) " confirm
    
    if [ "$confirm" = "y" ] || [ "$confirm" = "Y" ]; then
        eval $cmd
    fi
}

# 3. 导出格式化CSV
do_export_csv() {
    echo -e "\n${CYAN}=== 导出CSV ===${NC}\n"
    
    echo -e "${YELLOW}选择导出格式:${NC}"
    echo -e "  ${GREEN}1)${NC} 规模采集重启    - 完整QA列，含审批状态 ${CYAN}(推荐)${NC}"
    echo "  2) 奥特内部采集    - HTTP视频链接，简洁格式"
    echo "  3) 原始云格式      - OSS路径，带updated_at"
    read -p "选择格式 (1/2/3) [1]: " format_choice
    format_choice=${format_choice:-1}
    
    local time_adjust=""
    if [ "$format_choice" = "1" ]; then
        echo ""
        echo -e "${YELLOW}时间调整 (b852设备不受影响):${NC}"
        echo "  1) 保留原始时间"
        echo "  2) 所有设备+8小时 (UTC转北京时间)"
        read -p "选择 (1/2) [1]: " time_choice
        time_choice=${time_choice:-1}
        if [ "$time_choice" = "2" ]; then
            time_adjust="--time-adjust"
        fi
    fi
    
    echo ""
    echo -e "${YELLOW}选择导出范围:${NC}"
    echo "  1) 指定日期范围"
    echo "  2) 导出全部数据"
    read -p "选择 (1/2): " export_choice
    
    local cmd="uv run -m scanner.scan.export_formatted_csv"
    
    # 格式参数
    case $format_choice in
        1) cmd="$cmd --format scale $time_adjust" ;;
        2) cmd="$cmd --format internal" ;;
        3) cmd="$cmd --format raw" ;;
    esac
    
    # 日期范围
    if [ "$export_choice" = "2" ]; then
        cmd="$cmd --all"
    else
        local today=$(date +%Y-%m-%d)
        local start_date=$(read_date "开始日期 (YYYY-MM-DD)" "$today")
        local end_date=$(read_date "结束日期 (YYYY-MM-DD)" "$start_date")
        cmd="$cmd --start-date $start_date --end-date $end_date"
    fi
    
    echo ""
    echo -e "${YELLOW}执行命令:${NC}"
    echo -e "${GREEN}$cmd${NC}\n"
    read -p "确认执行? (y/n) " confirm
    
    if [ "$confirm" = "y" ] || [ "$confirm" = "Y" ]; then
        eval $cmd
    fi
}

# 4. 验证设备时长
do_verify_duration() {
    echo -e "\n${CYAN}=== 验证设备时长 ===${NC}\n"
    
    # 列出可用的CSV文件
    echo "ExportedCSV 目录中的文件:"
    if [ -d "ExportedCSV" ]; then
        ls -lt ExportedCSV/*.csv 2>/dev/null | head -10
    else
        echo -e "${RED}ExportedCSV 目录不存在${NC}"
        return
    fi
    
    echo ""
    read -p "输入CSV文件名: " csv_file
    read -p "设备ID (留空验证所有设备): " device_id
    
    local cmd="uv run -m scanner.validate.verify_device_duration --file $csv_file"
    if [ -n "$device_id" ]; then
        cmd="$cmd --device $device_id"
    fi
    
    echo ""
    echo -e "${YELLOW}执行命令:${NC}"
    echo -e "${GREEN}$cmd${NC}\n"
    read -p "确认执行? (y/n) " confirm
    
    if [ "$confirm" = "y" ] || [ "$confirm" = "Y" ]; then
        eval $cmd
    fi
}

# 5. 验证Session Segments
do_verify_segments() {
    echo -e "\n${CYAN}=== 验证Session Segments ===${NC}\n"
    
    # 列出可用的CSV文件
    echo "ExportedCSV 目录中的文件:"
    if [ -d "ExportedCSV" ]; then
        ls -lt ExportedCSV/*.csv 2>/dev/null | head -10
    else
        echo -e "${RED}ExportedCSV 目录不存在${NC}"
        return
    fi
    
    echo ""
    read -p "输入CSV文件名: " csv_file
    read -p "启用自动修复? (y/n) [n]: " auto_fix
    
    local cmd="uv run -m scanner.validate.verify_session_segments --file $csv_file"
    if [ "$auto_fix" = "y" ] || [ "$auto_fix" = "Y" ]; then
        cmd="$cmd --auto-fix"
    else
        cmd="$cmd --no-fix"
    fi
    
    echo ""
    echo -e "${YELLOW}执行命令:${NC}"
    echo -e "${GREEN}$cmd${NC}\n"
    read -p "确认执行? (y/n) " confirm
    
    if [ "$confirm" = "y" ] || [ "$confirm" = "Y" ]; then
        eval $cmd
    fi
}

# 6. 启动Lark监听服务
do_start_lark_service() {
    echo -e "\n${CYAN}=== 启动Lark监听服务 ===${NC}\n"
    
    echo -e "${YELLOW}注意: 这是一个长期运行的服务，按 Ctrl+C 停止${NC}\n"
    
    local cmd="uv run -m lark_bots.scan_service"
    
    echo -e "${YELLOW}执行命令:${NC}"
    echo -e "${GREEN}$cmd${NC}\n"
    read -p "确认启动? (y/n) " confirm
    
    if [ "$confirm" = "y" ] || [ "$confirm" = "Y" ]; then
        eval $cmd
    fi
}

# 7. 发送CSV到Lark群
do_send_csv_to_lark() {
    echo -e "\n${CYAN}=== 发送CSV到Lark群 ===${NC}\n"
    
    # 列出可用的CSV文件
    echo "ExportedCSV 目录中的文件:"
    if [ -d "ExportedCSV" ]; then
        ls -lt ExportedCSV/*.csv 2>/dev/null | head -10
    else
        echo -e "${RED}ExportedCSV 目录不存在${NC}"
        return
    fi
    
    echo ""
    read -p "输入CSV文件名: " csv_file
    
    local file_path="ExportedCSV/$csv_file"
    if [ ! -f "$file_path" ]; then
        echo -e "${RED}文件不存在: $file_path${NC}"
        return
    fi
    
    local cmd="uv run -m lark_bots.main --file $file_path"
    
    echo ""
    echo -e "${YELLOW}执行命令:${NC}"
    echo -e "${GREEN}$cmd${NC}\n"
    read -p "确认发送? (y/n) " confirm
    
    if [ "$confirm" = "y" ] || [ "$confirm" = "Y" ]; then
        eval $cmd
    fi
}

# 8. 添加设备
do_add_device() {
    echo -e "\n${CYAN}=== 添加设备 ===${NC}\n"
    
    read -p "设备ID: " device_id
    read -p "MB/10分钟 [600]: " mb_per_10min
    mb_per_10min=${mb_per_10min:-600}
    
    local cmd="uv run -m scanner.tools.add_devices --device-id $device_id --mb-per-10min $mb_per_10min"
    
    echo ""
    echo -e "${YELLOW}执行命令:${NC}"
    echo -e "${GREEN}$cmd${NC}\n"
    read -p "确认执行? (y/n) " confirm
    
    if [ "$confirm" = "y" ] || [ "$confirm" = "Y" ]; then
        eval $cmd
    fi
}

# 9. 诊断Session
do_diagnose_session() {
    echo -e "\n${CYAN}=== 诊断Session ===${NC}\n"
    
    read -p "Session ID: " session_id
    
    if [ -z "$session_id" ]; then
        echo -e "${RED}Session ID 不能为空${NC}"
        return
    fi
    
    local cmd="uv run -m scanner.tools.diagnose_session --session-id $session_id"
    
    echo ""
    echo -e "${YELLOW}执行命令:${NC}"
    echo -e "${GREEN}$cmd${NC}\n"
    read -p "确认执行? (y/n) " confirm
    
    if [ "$confirm" = "y" ] || [ "$confirm" = "Y" ]; then
        eval $cmd
    fi
}

# 主循环
main() {
    while true; do
        print_header
        print_menu
        
        read -p "请选择操作 [0-9]: " choice
        
        case $choice in
            1) do_info_scan ;;
            2) do_export_csv ;;
            3) do_metadata_scan ;;
            4) do_verify_duration ;;
            5) do_verify_segments ;;
            6) do_start_lark_service ;;
            7) do_send_csv_to_lark ;;
            8) do_add_device ;;
            0) 
                echo -e "\n${GREEN}再见! 👋${NC}\n"
                exit 0
                ;;
            *)
                echo -e "\n${RED}无效选项，请重新选择${NC}"
                ;;
        esac
        
        echo ""
        read -p "按回车键继续..."
    done
}

# 运行主程序
main
