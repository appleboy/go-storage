# syntax=docker/dockerfile:1
# Recreate the pinned test fixture from the official historical release.
FROM ubuntu:24.04
ADD --checksum=sha256:f80ee0924802adcdf5ae03fd0bade3ff2d519372e8a1ff30e7e74456ee6896c6 --chmod=755 https://github.com/minio/minio/releases/download/RELEASE.2024-01-16T16-07-38Z/minio.linux-amd64.RELEASE.2024-01-16T16-07-38Z /usr/local/bin/minio
ENTRYPOINT ["/usr/local/bin/minio"]
