# syntax=docker/dockerfile:1
FROM alpine:latest
ARG TARGETARCH
ENV PATH=/opt/bin:$PATH
COPY minio-${TARGETARCH} /opt/bin/minio
COPY docker-entrypoint.sh /usr/bin/docker-entrypoint.sh
ENTRYPOINT ["/usr/bin/docker-entrypoint.sh"]
VOLUME ["/data"]
CMD ["minio"]
