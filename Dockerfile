FROM python:3


WORKDIR /usr/src/app

COPY requirements.txt ./
COPY run.sh ./
RUN pip install --no-cache-dir -r requirements.txt

COPY syncSQLErrors.py .

CMD [ "./run.sh" ]

