FROM python:3.11-slim

RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    postgresql-client \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

RUN pip install --no-cache-dir \
    pyarrow==15.0.2 \
    pandas==2.2.2 \
    psycopg2-binary==2.9.9 \
    requests==2.31.0

COPY load.sh /load.sh
COPY load_pg.py /load_pg.py
COPY loader_api.py /loader_api.py
RUN chmod +x /load.sh

ENTRYPOINT ["/load.sh"]
