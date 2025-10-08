#!/bin/sh
set -eu

SNIPPET_DIR="/shared/nginx/routers"
mkdir -p "$SNIPPET_DIR"

echo "[reloader] watching $SNIPPET_DIR"
while true; do
  sleep 2
  find "$SNIPPET_DIR" -type f -print0 2>/dev/null | xargs -0 stat -c "%n %Y" 2>/dev/null > /tmp/snap.new || true
  if [ -f /tmp/snap.old ]; then
    if ! cmp -s /tmp/snap.new /tmp/snap.old; then
      mv /tmp/snap.new /tmp/snap.old
      echo "[reloader] change detected -> nginx -t && nginx -s reload"
      if nginx -t; then
        nginx -s reload || true
      else
        echo "[reloader] nginx -t failed; not reloading"
      fi
    fi
  else
    mv /tmp/snap.new /tmp/snap.old || true
  fi
done
