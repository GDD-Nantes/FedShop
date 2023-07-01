FROM python:3.8.17-alpine3.18
WORKDIR /fedshop-proxy
COPY rsfb/proxy /fedshop-proxy
RUN pip install --no-cache flask flask-cors requests gunicorn