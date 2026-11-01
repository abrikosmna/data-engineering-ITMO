FROM python:3.12-slim
WORKDIR /app
COPY hw4/requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt
COPY src /app/src
COPY hw4/*.py /app/hw4/
ENV PYTHONUNBUFFERED=1 PYTHONPATH=/app/src
CMD ["python", "hw4/coordinator.py"]
