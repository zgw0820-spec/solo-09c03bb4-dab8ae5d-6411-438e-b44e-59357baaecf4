FROM python:3.12-slim

WORKDIR /srv
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY app ./app

EXPOSE 8000
# default service: the web coordinator; workers override the command
CMD ["uvicorn", "app.web.main:app", "--host", "0.0.0.0", "--port", "8000"]
