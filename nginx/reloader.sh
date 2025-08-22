#!/bin/sh
# Watches router snippets and reloads nginx when they change.
SNIPPET_DIR="/shared/nginx/routers"
mkdir -p "$SNIPPET_DIR"

inotifywait() {
  # Busybox inotify-tools may not be present. Polling fallback every 2s.
  while true; do
    sleep 2
    find "$SNIPPET_DIR" -type f -print0 2>/dev/null | xargs -0 stat -c "%n %Y" 2>/dev/null > /tmp/snap.new
    if [ -f /tmp/snap.old ]; then
      if ! cmp -s /tmp/snap.new /tmp/snap.old; then
        mv /tmp/snap.new /tmp/snap.old
        nginx -s reload 2>/dev/null || true
      fi
    else
      mv /tmp/snap.new /tmp/snap.old
    fi
  done
}
inotifywait
