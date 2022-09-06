FROM python:3.8-slim-buster

ARG BROOK_APP_CONFIG_FILE

WORKDIR /app

COPY requirements.txt requirements.txt
RUN pip install -r requirements.txt

COPY src ./src

COPY $BROOK_APP_CONFIG_FILE ./config.ini
ENV BROOK_APP_CONFIG_FILE=/app/config.ini

ENTRYPOINT python
