FROM apache/airflow:2.10.3
LABEL maintainer="hungdv@fetek.vn"

USER root

RUN apt update && apt install unzip libaio1
RUN mkdir -p /opt/oracle && chown -R airflow: /opt/oracle

COPY ./libs/instantclient-basic-linux.x64-21.16.0.0.0dbru.zip /opt/oracle/
RUN cd /opt/oracle && unzip instantclient-basic-linux.x64-21.16.0.0.0dbru.zip
RUN echo /opt/oracle/instantclient_21_16 > /etc/ld.so.conf.d/oracle-instantclient.conf
RUN ldconfig

COPY --chown=airflow:root ./libs/requirements-local.txt ./requirements/

USER airflow
RUN pip install -r requirements/requirements-local.txt