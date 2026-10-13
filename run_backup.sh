#!/usr/bin/env bash
# Бекап очередей HRTECH, PRODUCTIV и HROPS.
#
# Переменные окружения берутся из tracker-backup.env, токены — из tracker-backup.tokens
# (оба файла рядом со скриптом; пример — *.example). Каталог бекапа — TRACKER_BACKUP_OUT
# из env-файла, по умолчанию ./backup рядом со скриптом.
#
#   ./run_backup.sh                 # инкрементальный бекап
#   ./run_backup.sh --full          # остальные аргументы передаются tracker_backup.py
#   ./run_backup.sh --limit 3 -v    # пробный прогон
#
# Пути к файлам можно переопределить: ENV_FILE=… TOKENS_FILE=… PYTHON=… ./run_backup.sh
set -euo pipefail

QUEUES="HRTECH,PRODUCTIV,HROPS"

DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ENV_FILE="${ENV_FILE:-$DIR/tracker-backup.env}"
TOKENS_FILE="${TOKENS_FILE:-$DIR/tracker-backup.tokens}"
PYTHON="${PYTHON:-$DIR/.venv/bin/python}"

die() { echo "run_backup: $*" >&2; exit 2; }

[[ -r "$ENV_FILE" ]] || die "не найден файл переменных $ENV_FILE (пример: tracker-backup.env.example)"
[[ -r "$TOKENS_FILE" ]] || die "не найден файл токенов $TOKENS_FILE (пример: tracker-backup.tokens.example)"
[[ -x "$PYTHON" ]] || die "не найден $PYTHON — создайте venv: python3 -m venv .venv && .venv/bin/pip install -r requirements.txt"

if [[ -n "$(find "$TOKENS_FILE" -perm -004)" ]]; then
    echo "run_backup: внимание: $TOKENS_FILE доступен на чтение всем пользователям, лучше chmod 600" >&2
fi

set -a
# shellcheck source=/dev/null
. "$ENV_FILE"
set +a

OUT="${TRACKER_BACKUP_OUT:-$DIR/backup}"
mkdir -p "$OUT"

# Не даём запустить второй бекап в тот же каталог, пока идёт первый (cron, ручной запуск).
# flock есть в Linux; в macOS — brew install flock, без него проверка пропускается.
if command -v flock >/dev/null 2>&1; then
    exec 9>"$OUT/.lock"
    flock -n 9 || { echo "run_backup: бекап в $OUT уже идёт, выходим" >&2; exit 0; }
fi

# exec: SIGTERM приходит прямо в Python (он корректно завершается), блокировка — через унаследованный fd 9
exec "$PYTHON" "$DIR/tracker_backup.py" --out "$OUT" --queues "$QUEUES" --tokens-file "$TOKENS_FILE" "$@"
