FROM public.ecr.aws/lambda/provided:al2

# Args
ARG CURL_VERSION="7.84.0"

# Copy in bootstrap and function script
COPY bootstrap ${LAMBDA_RUNTIME_DIR}
COPY function.sh ${LAMBDA_TASK_ROOT}

# Convert to executables
RUN chmod 755 ${LAMBDA_RUNTIME_DIR}/bootstrap && \
    chmod 755 ${LAMBDA_TASK_ROOT}/function.sh

# Install the absolute latest version of curl
RUN yum update -y && \
    yum install -y \
      libssl-dev \
      autoconf \
      libtool \
      make \
      jq \
      wget \
      gzip \
      unzip \
      openssl \
      openssl-devel && \
    echo "Installing latest version of curl" 1>&2 && \
    wget "https://curl.se/download/curl-${CURL_VERSION}.tar.gz" && \
    tar -xf curl-${CURL_VERSION}.tar.gz && \
    ( \
      cd curl-${CURL_VERSION}/ && \
      autoreconf -fi && \
      ./configure \
        --with-ssl \
        --prefix /usr/bin && \
      make && \
      make install \
    ) && \
    rm -rf curl-${CURL_VERSION}/ && \
    echo "Installing the latest version of aws binary" 1>&2 && \
    curl --silent --output "awscliv2.zip" \
      "https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip" && \
    unzip -qq "awscliv2.zip" && \
    ./aws/install \

CMD ["function.handler"]