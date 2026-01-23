#!/bin/bash
# ==============================================================================
# landppt 多源备份脚本 (Final Fix)
# 修复：上传日志不可见问题、Region 设置问题、环境变量隔离
# ==============================================================================

# 1. 强制 Python 路径
PYTHON_EXEC="/opt/venv/bin/python"

# 2. 初始化 AWS 配置文件 (解决 addressing_style 问题)
export AWS_EC2_METADATA_DISABLED=true
export AWS_CONFIG_FILE=/tmp/aws_config
cat > "$AWS_CONFIG_FILE" <<'EOF'
[default]
s3 =
    addressing_style = path
EOF

# ----------------- 配置 -----------------
DATA_DIR="."
DB_FILE="${DATA_DIR}/landppt.db"
SYNC_INTERVAL="${SYNC_INTERVAL:-600}"
BACKUP_KEEP="${BACKUP_KEEP:-24}"
TIMEOUT_CMD="120"

# S3 区域默认值 (如果 .env 没填，这里作为兜底)
# Synology C2 通常需要指定具体区域，如 us-004
S3_REGION="${S3_REGION:-us-004}"
S3_2_REGION="${S3_2_REGION:-auto}"

log() { echo "[Backup] $(date '+%Y-%m-%d %H:%M:%S') $*"; }

# ----------------- 检查函数 -----------------
has_webdav() { [[ -n "$WEBDAV_URL" && -n "$WEBDAV_USERNAME" && -n "$WEBDAV_PASSWORD" ]]; }
has_s3()     { [[ -n "$S3_ENDPOINT_URL" && -n "$S3_BUCKET" && -n "$S3_ACCESS_KEY_ID" ]]; }
has_s3_2()   { [[ -n "$S3_2_ENDPOINT_URL" && -n "$S3_2_BUCKET" && -n "$S3_2_ACCESS_KEY_ID" ]]; }

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

# ----------------- 核心功能函数 -----------------

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

get_s3_latest_name() {
    local ENDPOINT="$1" BUCKET="$2" ACCESS="$3" SECRET="$4" REGION="$5"
    export AWS_ACCESS_KEY_ID="$ACCESS"
    export AWS_SECRET_ACCESS_KEY="$SECRET"
    export AWS_DEFAULT_REGION="$REGION"
    
    run_with_timeout 20 aws --endpoint-url "$ENDPOINT" s3 ls "s3://$BUCKET/" --no-verify-ssl 2>/dev/null \
        | awk '{print $4}' | grep 'landppt_backup_' | sort | tail -n 1
}

download_s3_file() {
    local ENDPOINT="$1" BUCKET="$2" ACCESS="$3" SECRET="$4" REGION="$5" FILE="$6" DL_PATH="$7"
    log "从 S3 下载: $FILE (Region: $REGION)..."
    export AWS_ACCESS_KEY_ID="$ACCESS"
    export AWS_SECRET_ACCESS_KEY="$SECRET"
    export AWS_DEFAULT_REGION="$REGION"
    
    run_with_timeout 60 aws --endpoint-url "$ENDPOINT" s3 cp "s3://$BUCKET/$FILE" "$DL_PATH" --no-progress
}

# 统一上传与清理函数 (包含详细日志)
upload_and_cleanup_s3() {
    local ENDPOINT="$1" BUCKET="$2" ACCESS="$3" SECRET="$4" REGION="$5" FILE_PATH="$6" REMOTE_NAME="$7"
    
    export AWS_ACCESS_KEY_ID="$ACCESS"
    export AWS_SECRET_ACCESS_KEY="$SECRET"
    export AWS_DEFAULT_REGION="$REGION"

    # 上传 (移除 >/dev/null 以便查看报错)
    run_with_timeout "$TIMEOUT_CMD" aws --endpoint-url "$ENDPOINT" s3 cp "$FILE_PATH" "s3://$BUCKET/$REMOTE_NAME" --no-progress
    local UPLOAD_RC=$?

    if [ $UPLOAD_RC -ne 0 ]; then
        log "错误: S3 上传失败 (Code: $UPLOAD_RC). Endpoint: $ENDPOINT"
    else
        # 清理旧文件
        local FILES=$(aws --endpoint-url "$ENDPOINT" s3 ls "s3://$BUCKET/" 2>/dev/null | awk '{print $4}' | grep 'landppt_backup_' | sort)
        local COUNT=$(echo "$FILES" | wc -l)
        if [ "$COUNT" -gt "$BACKUP_KEEP" ]; then
            local DEL_COUNT=$(($COUNT - $BACKUP_KEEP))
            # log "清理 S3 旧备份: $DEL_COUNT 个..."
            echo "$FILES" | head -n "$DEL_COUNT" | while read -r F; do
                [ -n "$F" ] && aws --endpoint-url "$ENDPOINT" s3 rm "s3://$BUCKET/$F" --quiet
            done
        fi
    fi
}

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

# ----------------- 启动恢复流程 -----------------
if [ -f "$DB_FILE" ] && [ -s "$DB_FILE" ]; then
    log "本地数据库已存在，跳过恢复。"
else
    log "正在检查所有备份源的最新版本..."
    CANDIDATES_FILE="/tmp/backup_candidates.txt"
    > "$CANDIDATES_FILE"

    if has_s3; then
        F_S3=$(get_s3_latest_name "$S3_ENDPOINT_URL" "$S3_BUCKET" "$S3_ACCESS_KEY_ID" "$S3_SECRET_ACCESS_KEY" "$S3_REGION")
        [ -n "$F_S3" ] && echo "$F_S3 S3_MAIN" >> "$CANDIDATES_FILE" && log "发现 S3(主): $F_S3"
    fi
    if has_s3_2; then
        F_S3_2=$(get_s3_latest_name "$S3_2_ENDPOINT_URL" "$S3_2_BUCKET" "$S3_2_ACCESS_KEY_ID" "$S3_2_SECRET_ACCESS_KEY" "$S3_2_REGION")
        [ -n "$F_S3_2" ] && echo "$F_S3_2 S3_SEC" >> "$CANDIDATES_FILE" && log "发现 S3(备): $F_S3_2"
    fi
    if has_webdav; then
        F_DAV=$(get_webdav_latest_name)
        [ -n "$F_DAV" ] && echo "$F_DAV WEBDAV" >> "$CANDIDATES_FILE" && log "发现 WebDAV: $F_DAV"
    fi

    BEST_LINE=$(sort -r "$CANDIDATES_FILE" | head -n 1)
    if [ -n "$BEST_LINE" ]; then
        TARGET_FILE=$(echo "$BEST_LINE" | awk '{print $1}')
        SOURCE_TYPE=$(echo "$BEST_LINE" | awk '{print $2}')
        DL_FILE="/tmp/restore.tar.gz"
        rm -f "$DL_FILE"
        log ">>> 决定使用最新备份: $TARGET_FILE (来源: $SOURCE_TYPE)"
        
        case "$SOURCE_TYPE" in
            "S3_MAIN") download_s3_file "$S3_ENDPOINT_URL" "$S3_BUCKET" "$S3_ACCESS_KEY_ID" "$S3_SECRET_ACCESS_KEY" "$S3_REGION" "$TARGET_FILE" "$DL_FILE" ;;
            "S3_SEC")  download_s3_file "$S3_2_ENDPOINT_URL" "$S3_2_BUCKET" "$S3_2_ACCESS_KEY_ID" "$S3_2_SECRET_ACCESS_KEY" "$S3_2_REGION" "$TARGET_FILE" "$DL_FILE" ;;
            "WEBDAV")  download_webdav_file "$TARGET_FILE" "$DL_FILE" ;;
        esac
        
        if [ -f "$DL_FILE" ] && [ -s "$DL_FILE" ]; then
            extract_db "$DL_FILE" && log "恢复成功！" && rm -f "$DL_FILE"
        else
            log "错误: 下载失败或文件为空。"
        fi
    else
        log "未在任何源中找到备份文件，启动全新实例。"
    fi
    rm -f "$CANDIDATES_FILE"
fi

# ----------------- 后台备份循环 -----------------
(
    while true; do
        sleep "$SYNC_INTERVAL"
        if [ -f "$DB_FILE" ]; then
            TS=$(date +%Y%m%d_%H%M%S)
            BACKUP_NAME="landppt_backup_${TS}.tar.gz"
            TMP_BAK="/tmp/$BACKUP_NAME"
            
            # 1. 打包
            tar -czf "$TMP_BAK" -C "$DATA_DIR" landppt.db 2>/dev/null
            
            # 2. 上传 WebDAV
            if has_webdav; then
                run_with_timeout "$TIMEOUT_CMD" curl -s -f --connect-timeout 15 \
                    -u "$WEBDAV_USERNAME:$WEBDAV_PASSWORD" -T "$TMP_BAK" \
                    "${WEBDAV_URL%/}/${WEBDAV_BACKUP_PATH#/}/$BACKUP_NAME" >/dev/null 2>&1
            fi
            
            # 3. 上传 S3 (主)
            if has_s3; then
                upload_and_cleanup_s3 "$S3_ENDPOINT_URL" "$S3_BUCKET" "$S3_ACCESS_KEY_ID" "$S3_SECRET_ACCESS_KEY" "$S3_REGION" "$TMP_BAK" "$BACKUP_NAME"
            fi

            # 4. 上传 S3 (备)
            if has_s3_2; then
                upload_and_cleanup_s3 "$S3_2_ENDPOINT_URL" "$S3_2_BUCKET" "$S3_2_ACCESS_KEY_ID" "$S3_2_SECRET_ACCESS_KEY" "$S3_2_REGION" "$TMP_BAK" "$BACKUP_NAME"
            fi
            
            rm -f "$TMP_BAK"
            log "备份周期结束: $BACKUP_NAME"
        fi
    done
) &
