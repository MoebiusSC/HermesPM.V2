FROM python:3.12-slim
WORKDIR /app
COPY app.py engine.py provider.py service.py /app/
COPY static /app/static
ENV PYTHONUNBUFFERED=1
CMD ["python", "app.py"]
