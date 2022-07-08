FROM amazon/aws-lambda-python:latest

LABEL maintainer="Alexis Lucattini"

RUN yum update -y && \
    yum install -y python3 python3-dev python3-pip gcc && \
    rm -Rf /var/cache/yum

COPY requirements.txt ./

RUN pip install -r requirements.txt

COPY lambda_code.py ./

CMD ["lambda_code.lambda_handler"]
