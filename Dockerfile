FROM python:3.10-buster

WORKDIR /app

COPY requirements.txt /app/requirements.txt

RUN pip install --upgrade pip && \
		pip install -r requirements.txt

RUN	rm -f requirements.txt
