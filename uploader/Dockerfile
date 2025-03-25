# uploader/Dockerfile
FROM python:3.12-slim

WORKDIR /app

COPY . /app

RUN pip install --no-cache-dir hdfs pyyaml

CMD ["python", "data_uploader.py"]
