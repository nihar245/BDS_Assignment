FROM apache/airflow:2.10.4-python3.11

USER root
RUN apt-get update \
	&& apt-get install -y --no-install-recommends git \
	&& apt-get clean \
	&& rm -rf /var/lib/apt/lists/*

COPY requirements.txt /requirements.txt
USER airflow
RUN pip install --no-cache-dir -r /requirements.txt
