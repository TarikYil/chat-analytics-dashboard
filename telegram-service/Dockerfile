FROM python:3.10-slim
WORKDIR /app
COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt
COPY telegram_reporter.py ./
CMD ["python", "telegram_reporter.py"] 