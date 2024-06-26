
FROM python:3.12-slim

COPY chord.py /app/chord.py

WORKDIR /app

ENTRYPOINT ["python", "chord.py"]