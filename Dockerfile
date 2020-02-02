#-------------------------------------------------------------------------------------------------------------
# Copyright (c) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License. See https://go.microsoft.com/fwlink/?linkid=2090316 for license information.
#-------------------------------------------------------------------------------------------------------------

FROM continuumio/miniconda3

# Avoid warnings by switching to noninteractive
ENV DEBIAN_FRONTEND=noninteractive

USER root

# Configure apt and install packages
RUN apt-get update \
        && apt-get -y install --no-install-recommends apt-utils dialog 2>&1 \
        #
        # Verify git, process tools, lsb-release (common in install instructions for CLIs) installed
        && apt-get -y install git iproute2 procps iproute2 lsb-release \
        #
        # Clean up
        && apt-get autoremove -y \
        && apt-get clean -y \
        && rm -rf /var/lib/apt/lists/*

# Install odbc and ms sql tools
# per https://docs.microsoft.com/en-us/sql/connect/odbc/linux-mac/installing-the-microsoft-odbc-driver-for-sql-server#microsoft-odbc-driver-17-for-sql-server
RUN apt-get update \
        && apt-get -y install curl gnupg locales \
        && apt-get -y upgrade \
        && curl https://packages.microsoft.com/keys/microsoft.asc | apt-key add - \
        && curl https://packages.microsoft.com/config/debian/10/prod.list > /etc/apt/sources.list.d/mssql-release.list \
        && apt-get update \
        && ACCEPT_EULA=Y apt-get -y install msodbcsql17 \
        # bcp and sqlcmd
        && ACCEPT_EULA=Y apt-get -y install mssql-tools \
        && apt-get -y install unixodbc-dev


# Needed for SqlCmd per https://github.com/Microsoft/mssql-docker/issues/163#issuecomment-364069729
RUN echo "nb_NO.UTF-8 UTF-8" > /etc/locale.gen \
        && echo "en_US.UTF-8 UTF-8" >> /etc/locale.gen \
        && locale-gen

# place sql tools in path
RUN if [ ! -f ~/.bashrc ]; then touch ~/.bashrc; fi \
        && echo 'export PATH="$PATH:/opt/mssql-tools/bin"' >> ~/.bashrc 

# per https://stackoverflow.com/a/25086628/6067848
RUN /bin/bash -c "source ~/.bashrc"

# Switch back to dialog for any ad-hoc use of apt-get
ENV DEBIAN_FRONTEND=dialog


# Conda and local install
COPY env.yml /tmp/conda-tmp/

# Local install BCPandas
RUN conda env update -n base -f /tmp/conda-tmp/env.yml \
        && rm -rf /tmp/conda-tmp
