FROM quay.io/condaforge/mambaforge:4.10.3-7

# Set args
ARG CONDA_GROUP_NAME="cttso_ica_to_pieriandx_group"
ARG CONDA_GROUP_ID=1000
ARG CONDA_USER_NAME="cttso_ica_to_pieriandx_user"
ARG CONDA_USER_ID=1000
ARG CONDA_ENV_NAME="cttso-ica-to-pieriandx"
ARG SRC_TEMP_DIR="/cttso-ica-to-pieriandx-src-temp"

# Copy over for user
COPY . "${SRC_TEMP_DIR}/"

RUN export DEBIAN_FRONTEND=noninteractive && \
    echo "Updating apt" 1>&2 && \
    apt-get update -y -qq && \
    echo Installing jq 1>&2 && \
    apt-get install -y -qq \
      jq \
      rsync \
      curl \
      unzip && \
    echo "Cleaning up after apt installations" 1>&2 && \
    apt-get clean -y -qq && \
    echo Updating mamba 1>&2 && \
    mamba update --yes \
      --quiet \
      --name base \
      --channel defaults \
      mamba && \
    echo "Install aws cli" 1>&2 && \
    curl "https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip" --output "awscliv2.zip" && \
    unzip -qq "awscliv2.zip" && \
    ./aws/install && \
    rm -rf aws/ && \
    echo "Adding user groups" 1>&2 && \
    addgroup \
      --gid "${CONDA_GROUP_ID}" \
      "${CONDA_GROUP_NAME}" && \
    adduser \
      --disabled-password \
      --gid "${CONDA_GROUP_ID}" \
      --uid "${CONDA_USER_ID}" "${CONDA_USER_NAME}" && \
    echo "Moving and changing ownership of source docs" 1>&2 && \
    cp -r "${SRC_TEMP_DIR}/." "/home/${CONDA_USER_NAME}/cttso-ica-to-pieriandx-src/" && \
    chown -R "${CONDA_USER_ID}:${CONDA_GROUP_ID}" "/home/${CONDA_USER_NAME}/cttso-ica-to-pieriandx-src/" && \
    rm -rf  "${SRC_TEMP_DIR}"

# Switch to conda user
USER "${CONDA_USER_NAME}"
ENV USER="${CONDA_USER_NAME}"

# Add conda command
RUN echo "Adding in package and env paths to conda arc (now running under user: '${CONDA_USER_NAME}')" 1>&2 && \
    conda config --append pkgs_dirs "\$HOME/.conda/pkgs" && \
    conda config --append envs_dirs "\$HOME/.conda/envs" && \
    echo "Installing into a conda env" 1>&2 && \
    (  \
      cd "/home/${CONDA_USER_NAME}" && \
      mamba env create \
        --quiet \
        --name "${CONDA_ENV_NAME}" \
        --file "cttso-ica-to-pieriandx-src/cttso-ica-to-pieriandx-conda-env.yaml" \
    )

# Add cttso scripts to path
ENV PATH="/home/${CONDA_USER_NAME}/.conda/envs/${CONDA_ENV_NAME}/bin:${PATH}"

# Install setup
RUN echo "Installing utilities into conda env" 1>&2 && \
    ( \
      cd "/home/${CONDA_USER_NAME}/cttso-ica-to-pieriandx-src/" && \
      python3 setup.py install \
    ) && \
    echo "Ensure scripts are executable" 1>&2 && \
    chmod +x "/home/${CONDA_USER_NAME}/cttso-ica-to-pieriandx-src/scripts/*" && \
    echo "Adding reference csvs" 1>&2 && \
    rsync --archive \
      "/home/${CONDA_USER_NAME}/cttso-ica-to-pieriandx-src/references/" \
      "$(find "/home/${CONDA_USER_NAME}/.conda/envs/${CONDA_ENV_NAME}/" -type d -name "references")/"

# Add scripts to path
ENV PATH="/home/${CONDA_USER_NAME}/cttso-ica-to-pieriandx-src/scripts/:${PATH}"

# Change conda activate base to conda activate cttso-ica-to-pieriandx in bashrc
RUN sed --in-place \
      "s/conda activate base/conda activate ${CONDA_ENV_NAME}/" "/home/${CONDA_USER_NAME}/.bashrc"


ENTRYPOINT ["tini", "--"]
CMD [ "cttso-ica-to-pieriandx.py", "--help" ]