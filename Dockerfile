FROM python:3.8

SHELL ["/bin/bash", "-c"]

WORKDIR /var/www/html

RUN pip install --upgrade pip setuptools

RUN pip install pipenv

COPY Pipfile ./
#COPY Pipfile.lock ./

RUN pipenv install