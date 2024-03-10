FROM python:3.11.7

WORKDIR /app

COPY . /app

RUN pip3 install -r requirements.txt