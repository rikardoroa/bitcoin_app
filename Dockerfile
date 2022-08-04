FROM python:3.9.7
LABEL AUTHOR="rikardoroa"
LABEL DESCRIPTION="A dockerfile container - muttdata challenge"
COPY . /app
WORKDIR  /app
RUN make /app
COPY  requirements.txt app/requirements.txt
RUN pip install --upgrade pip
RUN pip install -r app/requirements.txt
CMD [ "python", "./main.py"]