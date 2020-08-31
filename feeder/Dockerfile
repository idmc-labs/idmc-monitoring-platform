FROM python:3.8.5-buster

WORKDIR /code/

RUN apt-get -y update
RUN apt-get install -y gcc g++ libgdal-dev gdal-bin --no-install-recommends && \
    apt-get clean -y && \
    python -m pip install --upgrade pip

ARG CPLUS_INCLUDE_PATH=/usr/include/gdal
ARG C_INCLUDE_PATH=/usr/include/gdal

COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

RUN touch /var/log/cron.log

ADD crontab /etc/cron.d/monitoring-cronjob
RUN chmod 0644 /etc/cron.d/monitoring-cronjob

RUN apt-get install -y cron

COPY . .

ENTRYPOINT ["/bin/sh", "/code/runner.sh"]
#CMD cron -f
