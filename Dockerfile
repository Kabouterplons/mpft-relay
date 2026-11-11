FROM python:3.12-slim
WORKDIR /app
COPY relay.py .
EXPOSE 12345
CMD ["python", "-u", "relay.py"]
