FROM python:3.13-slim
WORKDIR /app
COPY . .
RUN pip install poetry
RUN poetry install --no-root
CMD python3 main.py