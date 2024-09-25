# Dockerfile
FROM python:3.12

WORKDIR /usr/app

COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

CMD ["dbt", "run"]
