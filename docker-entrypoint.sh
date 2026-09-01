#!/bin/sh
set -eu

umask 077

permission_error() {
    echo "chathygiene data permissions are unsafe; stop the service and set the data directory to 0700 and existing database files to 0600" >&2
    exit 1
}

[ -d /data ] && [ ! -L /data ] || permission_error
[ "$(stat -c '%a' /data 2>/dev/null)" = "700" ] || permission_error

for database_file in \
    /data/chathygiene.db \
    /data/chathygiene.db-wal \
    /data/chathygiene.db-shm
do
    if [ -e "$database_file" ] || [ -L "$database_file" ]; then
        [ -f "$database_file" ] && [ ! -L "$database_file" ] || permission_error
        [ "$(stat -c '%a' "$database_file" 2>/dev/null)" = "600" ] || permission_error
    fi
done

exec "$@"
