# MinIO's own images left quay.io and Docker Hub; Bitnami's legacy build carries the same server.
FROM docker.io/bitnamilegacy/minio:2025.7.23
USER 0
ENV HOME=/root
ENTRYPOINT ["/opt/bitnami/minio/bin/minio"]
