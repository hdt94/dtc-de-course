FROM python:3.9.4

RUN apt-get install -qq wget

WORKDIR /app
COPY requirements.txt .
RUN pip install -r requirements.txt
COPY ingest_data.py .

ENTRYPOINT [ "python", "ingest_data.py" ]
