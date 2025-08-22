# nginx/Dockerfile
FROM nginx:1.25-alpine

# Nginx config
COPY nginx.conf /etc/nginx/nginx.conf
COPY conf.d/ /etc/nginx/conf.d/

# Hot reloader to pick up new snippet files
COPY reloader.sh /reloader.sh
RUN chmod +x /reloader.sh

# Don't try to create /shared here (on Fargate the root FS is read-only).
# The ephemeral /shared volume will be mounted by ECS; we mkdir at runtime in the entrypoint.

EXPOSE 8080
CMD ["/bin/sh", "-c", "mkdir -p /shared/nginx/routers && /reloader.sh & nginx -g 'daemon off;'"]
