FROM amazon/aws-lambda-python:latest

LABEL maintainer="Alexis Lucattini"

# Attempting logic from https://stackoverflow.com/a/68071471/6946787
# Context is in directory above
ARG APP_ROOT=.
ARG THIS_DIR_NAME="get_metadata_from_portal_and_defaults_and_launch_validation_workflow"

RUN yum update -y && \
    yum install -y python3 python3-dev python3-pip gcc && \
    rm -Rf /var/cache/yum && \
    mkdir /opt/python_utils/

RUN pip install --upgrade pip && \
    pip install google-auth

COPY ${APP_ROOT}/${THIS_DIR_NAME}/lambda_code.py ./

COPY ${APP_ROOT}/layers/ ./layers/

RUN ( \
      cd layers && \
      python setup.py install \
    )

CMD ["lambda_code.lambda_handler"]
