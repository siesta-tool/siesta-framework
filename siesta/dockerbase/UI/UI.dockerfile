FROM python:3.12-slim

WORKDIR /workspace/ui

COPY ui/requirements.txt /tmp/req/ui/requirements.txt
RUN pip install --no-cache-dir --prefer-binary -r /tmp/req/ui/requirements.txt && \
    rm -rf /tmp/req

COPY ui/ .

EXPOSE 8501

ENTRYPOINT ["streamlit", "run", "app.py", "--server.address=0.0.0.0", "--server.port=8501"]
