FROM python:3.14.0-slim
WORKDIR /app
COPY . .
RUN pip install -r requirements.txt
CMD python3 -u main.py