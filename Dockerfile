FROM python:3.10-slim

WORKDIR /app
COPY . /app

ENV PYTHONPATH=/app
ENV PYTHONUNBUFFERED=1

RUN pip install --upgrade pip
RUN pip install pyspark==3.5.0 requests dagster pytest black flake8

CMD ["dagster", "job", "execute", "-f", "pipeline/pipeline_dagster.py", "-j", "data_pipeline"]
