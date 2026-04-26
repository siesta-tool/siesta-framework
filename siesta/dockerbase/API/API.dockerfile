FROM siesta/spark:4.0.1

USER root

RUN apt-get update && apt-get install -y bash && rm -rf /var/lib/apt/lists/*

# Ensure UID 1001 has a username entry. Hadoop's UnixLoginModule requires this.
RUN getent passwd 1001 >/dev/null || echo 'spark:x:1001:0:spark user:/home/spark:/bin/bash' >> /etc/passwd

WORKDIR /workspace

# Install Python dependencies at build time as root so they land in system site-packages.
COPY siesta/requirements.txt /tmp/req/siesta/requirements.txt
COPY siesta/api/requirements.txt /tmp/req/siesta/api/requirements.txt
COPY siesta/core/requirements.txt /tmp/req/siesta/core/requirements.txt
COPY siesta/storage/S3/requirements.txt /tmp/req/siesta/storage/S3/requirements.txt
COPY siesta/modules/compare/requirements.txt /tmp/req/siesta/modules/compare/requirements.txt
COPY siesta/modules/index/requirements.txt /tmp/req/siesta/modules/index/requirements.txt
COPY siesta/modules/model/requirements.txt /tmp/req/siesta/modules/model/requirements.txt
COPY siesta/modules/mine/requirements.txt /tmp/req/siesta/modules/mine/requirements.txt

RUN /opt/bitnami/python/bin/python3 -m pip install --prefer-binary \
      -r /tmp/req/siesta/requirements.txt \
      -r /tmp/req/siesta/api/requirements.txt \
      -r /tmp/req/siesta/core/requirements.txt \
      -r /tmp/req/siesta/storage/S3/requirements.txt \
      -r /tmp/req/siesta/modules/compare/requirements.txt \
      -r /tmp/req/siesta/modules/index/requirements.txt \
      -r /tmp/req/siesta/modules/model/requirements.txt \
      -r /tmp/req/siesta/modules/mine/requirements.txt && \
    rm -rf /tmp/req

COPY siesta/dockerbase/API/entrypoint.sh /usr/local/bin/siesta-api-entrypoint.sh
RUN chmod 0755 /usr/local/bin/siesta-api-entrypoint.sh

# Pre-create the Ivy cache dir owned by the runtime user so the named volume
# inherits the correct ownership on first mount.
RUN mkdir -p /tmp/.ivy2 && chown -R 1001:0 /tmp/.ivy2

USER 1001

ENTRYPOINT ["/usr/local/bin/siesta-api-entrypoint.sh"]