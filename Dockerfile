FROM docker.io/condaforge/mambaforge-pypy3:4.10.3-6

# Set args
ARG CONDA_GROUP_NAME="cttso_ica_to_pieriandx_group"
ARG CONDA_GROUP_ID=1000
ARG CONDA_USER_NAME="cttso_ica_to_pieriandx_user"
ARG CONDA_USER_ID=1000
ARG CONDA_ENV_NAME="cttso_ica_to_pieriandx"
ARG SRC_TEMP_DIR="/cttso-ica-to-pieriandx-src-temp/"

# Copy over for user
COPY . "${SRC_TEMP_DIR}"

RUN export DEBIAN_FRONTEND=noninteractive && \
    echo "Updating apt" 1>&2 && \
    apt-get update -y -q && \
    echo Installing jq 1>&2 && \
    apt-get install -y -q \
      jq && | \
    echo "Cleaning up after apt installations" 1>&2 && \
    apt-get clean -y && \
    echo Updating mamba 1>&2 && \
    mamba update --yes \
      --quiet \
      --name base \
      --channel defaults \
      mamba && \
    echo "Adding user groups" 1>&2 && \
    addgroup \
      --gid "${CONDA_GROUP_ID}" \
      "${CONDA_GROUP_NAME}" && \
    adduser \
      --disabled-password \
      --gid "${CONDA_GROUP_ID}" \
      --uid "${CONDA_USER_ID}" "${CONDA_USER_NAME}" && \
    echo "Moving and changing ownership of source docs"
    cp -r "${SRC_TEMP_DIR}." "/home/${CONDA_USER_NAME}/cttso-ica-to-pieraidx-src/" && \
    chown -R "${CONDA_USER_ID}:${CONDA_GROUP_ID}" "/home/${CONDA_USER_NAME}/cttso-ica-to-pieraidx-src/" && \
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
      mamba env create --file "cttso-ica-to-pieraidx-src/cttso-ica-to-pieriandx-conda-env.yaml" -y \
    )

# Add cttso scripts to path
ENV PATH="\$HOME/.conda/envs/cttso-ica-to-pieriandx/bin/:\$PATH"

# Install setup
RUN echo "Installing utilities into conda env" 1>&2 \
    ( \
      cd "/home/${CONDA_USER_NAME}/cttso-ica-to-pieraidx-src/" && \
      python setup.py install \
    )

CMD "cttso-ica-to-pieriandx.py"