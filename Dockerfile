FROM python:3.14.0-alpine3.22
WORKDIR /app
COPY . .
RUN pip install --only-binary=all -r requirements.txt
CMD python3 -u main.py