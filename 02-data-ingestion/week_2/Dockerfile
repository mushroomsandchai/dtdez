# First-time build can take upto 10 mins.

FROM apache/airflow:3.0.2

ENV AIRFLOW_HOME=/opt/airflow

USER root
RUN apt-get update -qq && apt-get install vim -qqq
# git gcc g++ -qqq

COPY requirements.txt .
USER airflow
RUN pip install --no-cache-dir -r requirements.txt
USER root

# Ref: https://airflow.apache.org/docs/docker-stack/recipes.html

SHELL ["/bin/bash", "-o", "pipefail", "-e", "-u", "-x", "-c"]

ARG CLOUD_SDK_VERSION=322.0.0
ENV GCLOUD_HOME=/home/google-cloud-sdk

ENV PATH="${GCLOUD_HOME}/bin/:${PATH}"

USER root

RUN apt-get update -y && apt-get install -y curl gnupg \
  && echo "deb [signed-by=/usr/share/keyrings/cloud.google.gpg] http://packages.cloud.google.com/apt cloud-sdk main" \
     | tee -a /etc/apt/sources.list.d/google-cloud-sdk.list \
  && curl https://packages.cloud.google.com/apt/doc/apt-key.gpg \
     | apt-key --keyring /usr/share/keyrings/cloud.google.gpg add - \
  && apt-get update -y && apt-get install -y google-cloud-sdk

USER airflow

WORKDIR $AIRFLOW_HOME
USER root  
COPY --chown=root:root scripts/ /scripts/
RUN chmod -R +x /scripts

USER $AIRFLOW_UID