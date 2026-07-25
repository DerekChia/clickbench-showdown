FROM python:3.11-slim
RUN pip install --no-cache-dir duckdb==1.1.0
COPY server.py /server.py
EXPOSE 9999
CMD ["python3", "/server.py"]
