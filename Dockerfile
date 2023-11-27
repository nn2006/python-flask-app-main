FROM python:3.11.4-alpine3.18

WORKDIR /app
COPY templates/ /app/templates/
COPY db/ /app/db/
COPY ./requirements.txt /app/
COPY ./app.py /app/
RUN pip install -r requirements.txt
RUN pip install ldap3




CMD ["flask", "run", "--host", "0.0.0.0"]