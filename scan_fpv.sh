#!/bin/bash
# set -e

cd "$(dirname "$0")"

# 使用环境变量配置路径（支持Docker挂载卷）
LOG_DIR="${LOG_DIR:-.}"                    # 日志目录，默认当前目录
OUTPUT_DIR="${OUTPUT_DIR:-.}"              # 输出目录，默认当前目录  
TMP_DIR="${TMP_DIR:-/tmp}"                 # 临时目录，默认系统tmp


# 两个日志文件（使用环境变量配置的路径）
DETAIL_LOG="$LOG_DIR/scan_log.txt"
SUMMARY_LOG="$LOG_DIR/scan_summary.log"
TMP_OUT="$TMP_DIR/scan_python_tmp.out"
MAX_DETAIL_LINES=50000
MAX_SUMMARY_LINES=10000

mkdir -p "$LOG_DIR" "$OUTPUT_DIR"

# 参数处理
DEF_START_DATE=$(date -d "8 days ago" +%Y-%m-%d)
DEF_END_DATE=$(date -d "7 days ago" +%Y-%m-%d)

if [[ $# -ge 2 ]]; then
    START_DATE="$1"
    END_DATE="$2"
else
    START_DATE="$DEF_START_DATE"
    END_DATE="$DEF_END_DATE"
fi

RUN_TS=$(date "+%Y-%m-%d %H:%M:%S")
START_TIME=$(date +%s)

# PY_CMD="uv run -m scanner.scan.info_scan --start-date $START_DATE --end-date $END_DATE --log-level INFO"
PY_CMD="uv run -m scanner.scan.info_scan --start-date 2026-1-15 --end-date 2026-1-15 --log-level INFO"


# ========== 运行Python脚本 ==========
echo "[$RUN_TS] 开始扫描:   $START_DATE 至 $END_DATE"
$PY_CMD 2>&1 | tee "$TMP_OUT"

# 捕获Python脚本的退出码
PY_EXIT_CODE=${PIPESTATUS[0]}

# ========== 计算耗时 ==========
END_TIME=$(date +%s)
DURATION=$((END_TIME - START_TIME))
MINUTES=$((DURATION / 60))
SECONDS=$((DURATION % 60))

# ========== 提取统计信息 ==========
# 提取各设备的新增会话数（metadata扫描）

METADATA_NEW_COUNT=0
METADATA_DEVICES=""

# 修复：定义 new_count_pattern 并匹配 "新增:  " 或 "新增会话数:  "
device_pattern='(>>>)?[[:space:]]*设备[[:space:]]*\[([0-9]+)/[0-9]+\]:[[:space:]]*([a-zA-Z0-9_-]+)'
new_count_pattern='新增(会话数)?:[[:space:]]+([0-9]+)'

while IFS= read -r line; do
    if [[ "$line" =~ $device_pattern ]]; then
        CURRENT_DEVICE="${BASH_REMATCH[3]}"
    fi

    if [[ "$line" =~ $new_count_pattern ]]; then
        NEW_COUNT="${BASH_REMATCH[2]}"  # 注意改成第2个捕获组
        NEW_COUNT=$(echo "$NEW_COUNT" | tr -d '\r\n[:space:]' | grep -o '[0-9]*')
        test -z "$NEW_COUNT" && NEW_COUNT=0
        if [[ $NEW_COUNT -gt 0 ]]; then
            METADATA_NEW_COUNT=$((METADATA_NEW_COUNT + NEW_COUNT))
            METADATA_DEVICES="${METADATA_DEVICES}    - 设备 ${CURRENT_DEVICE}:   ${NEW_COUNT} 个会话\n"
        fi
    fi
done < "$TMP_OUT"


SEGMENTS_NEW_COUNT=$(awk '/新增视频段数:[[:space:]]+[0-9]+/ {print $NF; exit}' "$TMP_OUT" | tr -d '\r\n[:space:]' | grep -o '[0-9]*')
SEGMENTS_EXISTS_COUNT=$(awk '/已存在视频段数:[[:space:]]+[0-9]+/ {print $NF; exit}' "$TMP_OUT" | tr -d '\r\n[:space:]' | grep -o '[0-9]*')


# 修复：匹配带 "- " 前缀和单个空格的格式
CSV_PATH=$(grep "文件路径:" "$TMP_OUT" | tail -1 | sed 's/.*文件路径:[[:space:]]*//' | tr -d '\r\n' | xargs)

CSV_ROWS=$(awk '/导出行数:[[:space:]]+[0-9]+/ {print $NF; exit}' "$TMP_OUT" | tr -d '\r\n[:space:]' | grep -o '[0-9]*')
test -z "$CSV_ROWS" && CSV_ROWS=0

CSV_SIZE=$(grep "文件大小:" "$TMP_OUT" | tail -1 | sed 's/.*文件大小:[[:space:]]*//' | tr -d '\r\n' | xargs)

ERROR_COUNT=$(grep -cE "ERROR|✗|Traceback|Exception" "$TMP_OUT" 2>/dev/null || echo "0")
ERROR_COUNT=$(echo "$ERROR_COUNT" | tr -d '\r\n[:space:]' | grep -o '[0-9]*')
test -z "$ERROR_COUNT" && ERROR_COUNT=0



# ========== 写入摘要日志（结构化） ==========
{
    echo "================================================================================"
    echo "扫描时间:  $RUN_TS"
    echo "日期范围: $START_DATE 至 $END_DATE"
    echo "执行命令: $PY_CMD"
    echo "================================================================================"
    echo ""

    # 执行状态
    if [[ $PY_EXIT_CODE -eq 0 ]]; then
        echo "【执行状态】✓ 成功"
    else
        echo "【执行状态】✗ 失败 (退出码: $PY_EXIT_CODE)"
    fi
    echo ""

    # Metadata 新增统计
    if [[ $METADATA_NEW_COUNT -gt 0 ]]; then
        echo "  新增会话总数: $METADATA_NEW_COUNT"
        echo -e "$METADATA_DEVICES"
    else
        echo "  (无新增会话)"
    fi
    echo ""

    # Segments 统计
    if [[ $SEGMENTS_NEW_COUNT -gt 0 ]]; then
        echo "  - 新增视频段:  $SEGMENTS_NEW_COUNT"
        echo "  - 已存在视频段: $SEGMENTS_EXISTS_COUNT"
    else
        echo "  (无新增视频段)"
    fi
    echo ""

    # CSV 导出
    if [[ -n "$CSV_PATH" ]] && [[ $CSV_ROWS -gt 0 ]]; then
        echo "  - 文件:  $CSV_PATH"
        echo "  - 行数: $CSV_ROWS"
        echo "  - 大小: $CSV_SIZE"
    else
        echo "  (无新增segment，未生成CSV)"
    fi
    echo ""

    # 错误统计和详细信息
    if [[ $ERROR_COUNT -gt 0 ]]; then
        echo "【错误统计】"
        echo "  发现 $ERROR_COUNT 个错误"
        echo ""
        echo "【错误详情】"

        # 提取所有 ERROR 行
        echo "  --- Python ERROR 日志 ---"
        grep -E "ERROR" "$TMP_OUT" | head -20 | sed 's/^/  /' || echo "  (无)"
        echo ""

        # 提取所有 Traceback 和 Exception
        echo "  --- Python 异常堆栈 ---"
        awk '
            /Traceback \(most recent call last\):/ { capture=1; buffer="" }
            capture { buffer = buffer $0 "\n" }
            /^[A-Za-z]+Error:/ && capture {
                print buffer $0
                capture=0
                count++
                if (count >= 3) exit
            }
        ' "$TMP_OUT" | sed 's/^/  /' || echo "  (无)"
        echo ""

        # 提取所有 ✗ 标记的行
        echo "  --- 操作失败项 ---"
        grep "✗" "$TMP_OUT" | head -20 | sed 's/^/  /' || echo "  (无)"
        echo ""
    fi

    # 总耗时
    echo "【总耗时】${MINUTES}分${SECONDS}秒"
    echo "================================================================================"
    echo ""

} >> "$SUMMARY_LOG"



# ========== 写入详细日志（带截断） ==========
{
    echo "[$RUN_TS] ==== 扫库起止:  $START_DATE 至 $END_DATE ===="
    echo "    执行命令: $PY_CMD"
    echo "    退出码: $PY_EXIT_CODE"
    echo "    总耗时: ${MINUTES}分${SECONDS}秒"
    echo ""
    
    # 🔧 加这4行
    echo "---- Python 完整输出 ----"
    cat "$TMP_OUT"
    echo ""
    echo "---- Python 输出结束 ----"
    echo ""
    
    # 原来的错误摘要部分可以删掉或保留
    if [[ $ERROR_COUNT -gt 0 ]]; then
        echo "---- 错误摘要 ----"
        grep -E "ERROR" "$TMP_OUT" | head -20 | sed 's/^/  /'
        echo "---- 错误摘要END ----"
    fi

    echo "---- 新增会话统计 ----"
    echo "Metadata新增:  $METADATA_NEW_COUNT"
    echo "Segments新增: $SEGMENTS_NEW_COUNT"
    echo "---- 新增会话统计END ----"

    echo "[$RUN_TS] ==== 扫库完成 ===="
    echo ""

} >> "$DETAIL_LOG"

# ========== 截断详细日志 ==========
if [[ -f "$DETAIL_LOG" ]]; then
    CUR_LINES=$(wc -l < "$DETAIL_LOG" || echo "0")

    # 修复：使用十进制比较
    if (( CUR_LINES > MAX_DETAIL_LINES )); then
        tail -n "$MAX_DETAIL_LINES" "$DETAIL_LOG" > "$DETAIL_LOG.tmp"
        mv "$DETAIL_LOG.tmp" "$DETAIL_LOG"
        echo "[$(date "+%Y-%m-%d %H:%M:%S")] [LOG CLEANUP] 日志自动精简到${MAX_DETAIL_LINES}行" >> "$DETAIL_LOG"
    fi
fi

# ========== 清理临时文件 ==========
rm -f "$TMP_OUT"


# ========== 发送CSV到飞书（如果有新增记录） ==========
if [[ $PY_EXIT_CODE -eq 0 ]] && [[ -n "$CSV_PATH" ]] && [[ -f "$CSV_PATH" ]] && (( CSV_ROWS > 0 )); then
    echo ""
    echo "正在发送CSV到飞书..."
    
    # 调用发送脚本，捕获输出
    LARK_SEND_LOG="/tmp/lark_send_$$.log"
    if uv run python lark_bots/main.py -f "$CSV_PATH" > "$LARK_SEND_LOG" 2>&1; then
        # 发送成功
        SEND_STATUS="✓ CSV已成功发送到飞书群"
        echo "  $SEND_STATUS"
        
        # 记录到摘要日志
        {
            echo "【飞书通知】"
            echo "  $SEND_STATUS"
            echo "  - 文件:  $(basename "$CSV_PATH")"
            echo "  - 大小: $CSV_SIZE"
            echo ""
        } >> "$SUMMARY_LOG"
        
    else
        # 发送失败（但不影响主流程）
        SEND_STATUS="⚠ CSV发送失败，请检查飞书配置或网络连接"
        echo "  $SEND_STATUS"
        
        # 记录到摘要日志和详细日志
        {
            echo "【飞书通知】"
            echo "  $SEND_STATUS"
            echo "  - 错误日志见: $LARK_SEND_LOG"
            echo ""
        } >> "$SUMMARY_LOG"
        
        {
            echo "[$RUN_TS] ==== 飞书发送失败 ===="
            cat "$LARK_SEND_LOG"
            echo "[$RUN_TS] ==== 飞书发送日志结束 ===="
            echo ""
        } >> "$DETAIL_LOG"
    fi
    
    # 清理发送日志（成功的情况）
    [[ -f "$LARK_SEND_LOG" ]] && rm -f "$LARK_SEND_LOG"
    
else
    # 无CSV需要发送的情况
    if (( CSV_ROWS == 0 )); then
        echo ""
        echo "  ⊗ 本次无新增segment记录，跳过飞书通知"
    fi
fi



echo ""
echo "✓ 扫描完成！"
echo "  - 详细日志: $DETAIL_LOG"
echo "  - 摘要日志: $SUMMARY_LOG"

if [[ $PY_EXIT_CODE -ne 0 ]]; then
    echo "  ⚠ 警告: Python脚本执行失败 (退出码: $PY_EXIT_CODE)"
    echo "  请查看日志文件了解详情"
    exit $PY_EXIT_CODE
fi