#!/bin/bash
# ==============================================================================
# LandPPT 多源备份脚本 (WebDAV + S3/R2/C2)
# 修复：语法错误 & Synology C2 上传兼容性
# ==============================================================================

# 【配置 AWS】解决下载/上传时的寻址和分片问题
# export AWS_EC2_METADATA_DISABLED=true
# export AWS_CONFIG_FILE=/tmp/aws_config
# cat > "$AWS_CONFIG_FILE" <<'EOF'
# [default]
# s3 =
#     addressing_style = path
# EOF

# ----------------- 配置 -----------------

# 强制使用虚拟环境 Python (确保能加载 botocore)
PYTHON_EXEC="/opt/venv/bin/python"

# LandPPT 默认数据目录
DATA_DIR="."
DB_FILE="${DATA_DIR}/landppt.db"

# 备份间隔与保留数量
SYNC_INTERVAL="${SYNC_INTERVAL:-600}"
BACKUP_KEEP="${BACKUP_KEEP:-24}"

# 超时设置 (秒)
TIMEOUT_RESTORE="60"  # 获取列表和下载的超时时间
TIMEOUT_CMD="120"     # 上传超时时间

log() { echo "[Backup] $(date '+%Y-%m-%d %H:%M:%S') $*"; }

# ----------------- 检查函数 -----------------
has_webdav() { [[ -n "$WEBDAV_URL" && -n "$WEBDAV_USERNAME" && -n "$WEBDAV_PASSWORD" ]]; }
has_s3()     { [[ -n "$S3_ENDPOINT_URL" && -n "$S3_BUCKET" && -n "$S3_ACCESS_KEY_ID" ]]; }
has_s3_2()   { [[ -n "$S3_2_ENDPOINT_URL" && -n "$S3_2_BUCKET" && -n "$S3_2_ACCESS_KEY_ID" ]]; }

# 带超时的命令执行
run_with_timeout() {
    local t="$1"; shift
    if command -v timeout >/dev/null; then
        timeout "$t" "$@"
        local rc=$?
        [ $rc -eq 124 ] && log "错误: 操作超时 ($t秒)" && return 124
        return $rc
    else
        "$@"
    fi
}

# 解压数据库函数
extract_db() {
    local tar_path="$1"
    [ ! -f "$tar_path" ] && return 1
    
    mkdir -p "$DATA_DIR"
    
    $PYTHON_EXEC -c "
import sys, tarfile, os, shutil
try:
    with tarfile.open('$tar_path', 'r:gz') as tar:
        m = next((m for m in tar.getmembers() if m.name.endswith('landppt.db')), None)
        if m:
            m.name = os.path.basename(m.name)
            tar.extract(m, '/tmp/restore_tmp')
            shutil.move('/tmp/restore_tmp/'+m.name, '$DB_FILE')
            print('ok')
        else:
            sys.exit(1)
except:
    sys.exit(1)
" >/dev/null 2>&1
}

# ----------------- 1. 获取最新文件名逻辑 -----------------

# 获取 S3 最新文件名 (仅返回文件名)
get_s3_latest_name() {
    local ENDPOINT="$1" BUCKET="$2" ACCESS="$3" SECRET="$4"
    export AWS_ACCESS_KEY_ID="$ACCESS"
    export AWS_SECRET_ACCESS_KEY="$SECRET"
    export AWS_DEFAULT_REGION="auto"
    
    run_with_timeout 20 aws --endpoint-url "$ENDPOINT" s3 ls "s3://$BUCKET/" 2>/dev/null \
        | awk '{print $4}' | grep 'landppt_backup_' | sort | tail -n 1
}

# 获取 WebDAV 最新文件名 (仅返回文件名)
get_webdav_latest_name() {
    $PYTHON_EXEC -c "
import os, sys
from webdav3.client import Client
try:
    hostname = '$WEBDAV_URL'
    sub = os.environ.get('WEBDAV_BACKUP_PATH', '')
    if sub: hostname = hostname.rstrip('/') + '/' + sub.strip('/')
    opts = {
        'webdav_hostname': hostname,
        'webdav_login': '$WEBDAV_USERNAME',
        'webdav_password': '$WEBDAV_PASSWORD',
        'webdav_timeout': 15
    }
    client = Client(opts)
    files = [f for f in client.list() if f.startswith('landppt_backup_') and f.endswith('.tar.gz')]
    if files: print(sorted(files)[-1])
except: pass
"
}

# ----------------- 2. 下载逻辑 -----------------

download_s3_file() {
    local ENDPOINT="$1" BUCKET="$2" ACCESS="$3" SECRET="$4" FILE="$5" DL_PATH="$6"
    log "从 S3 下载: $FILE ..."
    export AWS_ACCESS_KEY_ID="$ACCESS"
    export AWS_SECRET_ACCESS_KEY="$SECRET"
    export AWS_DEFAULT_REGION="us-004"
    run_with_timeout "$TIMEOUT_RESTORE" aws --endpoint-url "$ENDPOINT" s3 cp "s3://$BUCKET/$FILE" "$DL_PATH" --quiet
}

download_webdav_file() {
    local FILE="$1" DL_PATH="$2"
    log "从 WebDAV 下载: $FILE ..."
    $PYTHON_EXEC -c "
import requests, os, sys
hostname = '$WEBDAV_URL'
sub = os.environ.get('WEBDAV_BACKUP_PATH', '')
if sub: hostname = hostname.rstrip('/') + '/' + sub.strip('/')
url = hostname + '/' + '$FILE'
try:
    with requests.get(url, auth=('$WEBDAV_USERNAME', '$WEBDAV_PASSWORD'), stream=True, timeout=60) as r:
        r.raise_for_status()
        with open('$DL_PATH', 'wb') as f:
            for chunk in r.iter_content(8192): f.write(chunk)
except: sys.exit(1)
"
}

# ----------------- 主启动恢复流程 -----------------

# 如果 DB 已存在，跳过恢复
if [ -f "$DB_FILE" ] && [ -s "$DB_FILE" ]; then
    log "本地数据库已存在，跳过恢复。"
else
    log "正在检查所有备份源的最新版本..."
    CANDIDATES_FILE="/tmp/backup_candidates.txt"
    > "$CANDIDATES_FILE"

    # 1. 检查 S3 (主)
    if has_s3; then
        F_S3=$(get_s3_latest_name "$S3_ENDPOINT_URL" "$S3_BUCKET" "$S3_ACCESS_KEY_ID" "$S3_SECRET_ACCESS_KEY")
        if [ -n "$F_S3" ]; then
            echo "$F_S3 S3_MAIN" >> "$CANDIDATES_FILE"
            log "发现 S3(主): $F_S3"
        fi
    fi

    # 2. 检查 S3 (备)
    if has_s3_2; then
        F_S3_2=$(get_s3_latest_name "$S3_2_ENDPOINT_URL" "$S3_2_BUCKET" "$S3_2_ACCESS_KEY_ID" "$S3_2_SECRET_ACCESS_KEY")
        if [ -n "$F_S3_2" ]; then
            echo "$F_S3_2 S3_SEC" >> "$CANDIDATES_FILE"
            log "发现 S3(备): $F_S3_2"
        fi
    fi

    # 3. 检查 WebDAV
    if has_webdav; then
        F_DAV=$(get_webdav_latest_name)
        if [ -n "$F_DAV" ]; then
            echo "$F_DAV WEBDAV" >> "$CANDIDATES_FILE"
            log "发现 WebDAV: $F_DAV"
        fi
    fi

    # 4. 决策：排序取最大值
    BEST_LINE=$(sort -r "$CANDIDATES_FILE" | head -n 1)
    
    if [ -n "$BEST_LINE" ]; then
        TARGET_FILE=$(echo "$BEST_LINE" | awk '{print $1}')
        SOURCE_TYPE=$(echo "$BEST_LINE" | awk '{print $2}')
        
        DL_FILE="/tmp/restore.tar.gz"
        rm -f "$DL_FILE"

        log ">>> 决定使用最新备份: $TARGET_FILE (来源: $SOURCE_TYPE)"
        
        SUCCESS=0
        case "$SOURCE_TYPE" in
            "S3_MAIN")
                download_s3_file "$S3_ENDPOINT_URL" "$S3_BUCKET" "$S3_ACCESS_KEY_ID" "$S3_SECRET_ACCESS_KEY" "$TARGET_FILE" "$DL_FILE"
                ;;
            "S3_SEC")
                download_s3_file "$S3_2_ENDPOINT_URL" "$S3_2_BUCKET" "$S3_2_ACCESS_KEY_ID" "$S3_2_SECRET_ACCESS_KEY" "$TARGET_FILE" "$DL_FILE"
                ;;
            "WEBDAV")
                download_webdav_file "$TARGET_FILE" "$DL_FILE"
                ;;
        esac
        
        if [ -f "$DL_FILE" ] && [ -s "$DL_FILE" ]; then
            extract_db "$DL_FILE"
            if [ $? -eq 0 ]; then
                log "恢复成功！"
                SUCCESS=1
                rm -f "$DL_FILE"
            fi
        fi
        
        if [ $SUCCESS -eq 0 ]; then
            log "错误: 尽管发现了文件，但下载或解压失败。"
        fi
    else
        log "未在任何源中找到备份文件，将启动全新实例。"
    fi
    rm -f "$CANDIDATES_FILE"
fi

# ----------------- 开启后台备份 (保持原逻辑) -----------------
(
    while true; do
        sleep "$SYNC_INTERVAL"
        
        if [ -f "$DB_FILE" ]; then
            TS=$(date +%Y%m%d_%H%M%S)
            BACKUP_NAME="landppt_backup_${TS}.tar.gz"
            TMP_BAK="/tmp/$BACKUP_NAME"
            
            # 打包
            tar -czf "$TMP_BAK" -C "$DATA_DIR" landppt.db 2>/dev/null
            
            # 上传 WebDAV
            if has_webdav; then
                run_with_timeout "$TIMEOUT_CMD" curl -s -f --connect-timeout 15 \
                    -u "$WEBDAV_USERNAME:$WEBDAV_PASSWORD" \
                    -T "$TMP_BAK" \
                    "${WEBDAV_URL%/}/${WEBDAV_BACKUP_PATH#/}/$BACKUP_NAME" >/dev/null 2>&1
            fi
            
            # 上传 S3 (主)
            if has_s3; then
                export AWS_ACCESS_KEY_ID="$S3_ACCESS_KEY_ID"
                export AWS_SECRET_ACCESS_KEY="$S3_SECRET_ACCESS_KEY"
                export AWS_DEFAULT_REGION="auto"
                run_with_timeout "$TIMEOUT_CMD" aws --endpoint-url "$S3_ENDPOINT_URL" s3 cp "$TMP_BAK" "s3://$S3_BUCKET/$BACKUP_NAME" >/dev/null 2>&1
                
                # S3 清理
                FILES=$(aws --endpoint-url "$S3_ENDPOINT_URL" s3 ls "s3://$S3_BUCKET/" | awk '{print $4}' | grep 'landppt_backup_' | sort)
                COUNT=$(echo "$FILES" | wc -l)
                if [ "$COUNT" -gt "$BACKUP_KEEP" ]; then
                    DEL=$(($COUNT - $BACKUP_KEEP))
                    echo "$FILES" | head -n "$DEL" | while read -r F; do
                        aws --endpoint-url "$S3_ENDPOINT_URL" s3 rm "s3://$S3_BUCKET/$F" --quiet
                    done
                fi
            fi

            # 上传 S3 (备)
            if has_s3_2; then
                export AWS_ACCESS_KEY_ID="$S3_2_ACCESS_KEY_ID"
                export AWS_SECRET_ACCESS_KEY="$S3_2_SECRET_ACCESS_KEY"
                export AWS_DEFAULT_REGION="auto"
                run_with_timeout "$TIMEOUT_CMD" aws --endpoint-url "$S3_2_ENDPOINT_URL" s3 cp "$TMP_BAK" "s3://$S3_2_BUCKET/$BACKUP_NAME" >/dev/null 2>&1

                # S3 清理
                FILES=$(aws --endpoint-url "$S3_2_ENDPOINT_URL" s3 ls "s3://$S3_2_BUCKET/" | awk '{print $4}' | grep 'landppt_backup_' | sort)
                COUNT=$(echo "$FILES" | wc -l)
                if [ "$COUNT" -gt "$BACKUP_KEEP" ]; then
                    DEL=$(($COUNT - $BACKUP_KEEP))
                    echo "$FILES" | head -n "$DEL" | while read -r F; do
                        aws --endpoint-url "$S3_2_ENDPOINT_URL" s3 rm "s3://$S3_2_BUCKET/$F" --quiet
                    done
                fi
            fi
            
            rm -f "$TMP_BAK"
            log "备份完成: $BACKUP_NAME"
        fi
    done
) &
