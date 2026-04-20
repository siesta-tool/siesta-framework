FROM siesta/spark:4.0.1

USER root

RUN apt-get update && apt-get install -y bash && rm -rf /var/lib/apt/lists/*

# Ensure UID 1001 has a username entry. Hadoop's UnixLoginModule requires this.
RUN getent passwd 1001 >/dev/null || echo 'spark:x:1001:0:spark user:/home/spark:/bin/bash' >> /etc/passwd

WORKDIR /workspace

COPY siesta/dockerbase/API/entrypoint.sh /usr/local/bin/siesta-api-entrypoint.sh
RUN chmod 0755 /usr/local/bin/siesta-api-entrypoint.sh

USER 1001

ENTRYPOINT ["/usr/local/bin/siesta-api-entrypoint.sh"]