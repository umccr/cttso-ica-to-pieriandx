FROM amazon/aws-lambda-python:latest

LABEL maintainer="Alexis Lucattini"

RUN yum update -y && \
    yum install -y python3 python3-dev python3-pip gcc && \
    rm -Rf /var/cache/yum && \
    mkdir /opt/python_utils/

COPY lambda_code.py ./

COPY ../layers/lambda_utils ./lambda_utils

RUN ( \
  cd lambda_utils && \
  python setup.py install && \
  rm -rf lambda_utils \
)

CMD ["lambda_code.lambda_handler"]
