FROM python:3.8-slim-buster

ARG CONFIG_FILE
ARG PYTHON_SCRIPT
ENV PYTHON_SCRIPT=$PYTHON_SCRIPT

WORKDIR /app

COPY requirements.txt requirements.txt
RUN pip install -r requirements.txt

COPY src ./src

COPY $CONFIG_FILE ./config.ini
ENV CONFIG_FILE=/app/config.ini
ENV PYTHONPATH=/app/src

CMD python3 -m src.${PYTHON_SCRIPT}
