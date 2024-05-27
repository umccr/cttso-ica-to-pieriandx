FROM amazon/aws-lambda-python:3.9.2023.11.10.17

LABEL maintainer="Alexis Lucattini"

# Attempting logic from https://stackoverflow.com/a/68071471/6946787
# Context is in directory above
ARG APP_ROOT=.
ARG THIS_DIR_NAME="get_metadata_from_portal_and_redcap_and_launch_clinical_workflow"

RUN yum update -y && \
    yum install -y python3 python3-dev python3-pip gcc && \
    rm -Rf /var/cache/yum && \
    mkdir /opt/python_utils/

RUN pip install --upgrade pip && \
    pip install google-auth

COPY ${APP_ROOT}/${THIS_DIR_NAME}/lambda_code.py ./

COPY ${APP_ROOT}/layers/ ./layers/

RUN ( \
      chmod +rx ./lambda_code.py && \
      cd layers && \
      python setup.py install \
    )

CMD ["lambda_code.lambda_handler"]
