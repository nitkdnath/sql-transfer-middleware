FROM apache/airflow:2.5.0
COPY requirements.txt /
COPY artifacts/mysql-connector-odbc_8.0.31-1debian11_amd64.deb /
USER root
RUN apt-get update \
    # && apt-get upgrade \
    && apt-get install dpkg-dev -y  --no-install-recommends\
    && dpkg -i /mysql-connector-odbc_8.0.31-1debian11_amd64.deb \
    && apt-get autoremove -yqq --purge \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*
USER airflow
RUN pip install --no-cache-dir -r /requirements.txt