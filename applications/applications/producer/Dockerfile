FROM python:3.6
COPY . /
WORKDIR /
RUN pip install --no-cache-dir -r requirements.txt
#CMD python /app/producer.py
ENTRYPOINT [ "python" ]

CMD [ "app/producer.py" ]
