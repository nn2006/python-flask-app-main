FROM python:3.11.4-alpine3.18

WORKDIR /python-flask-app-main
COPY templates/ /python-flask-app-main/templates/
COPY db/ /python-flask-app-main/db/
COPY ./requirements.txt /python-flask-app-main/
COPY ./app.py /python-flask-app-main/
RUN pip install -r requirements.txt
RUN pip install ldap3




CMD ["flask", "run", "--host", "0.0.0.0"]