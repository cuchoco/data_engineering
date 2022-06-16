FROM python:3.9.1

RUN apt-get install wget
RUN pip --trusted-host pypi.org --trusted-host files.pythonhosted.org install pandas sqlalchemy psycopg2 pyarrow

WORKDIR /app
COPY ingest_data.py ingest_data.py

ENTRYPOINT [ "python", "ingest_data.py" ]