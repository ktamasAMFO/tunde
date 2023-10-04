FROM debian:buster-slim

SHELL ["/bin/bash", "-c"]
RUN apt-get update
# install necessary software
# build-essential, zlib1g-dev and libffi-dev for building and installing python
# the ones in the second row are needed for pip to get packages over HTTPS
# the third row is needed to build the wheel for the mysqlclient python module
RUN apt-get -yq install wget build-essential zlib1g-dev libffi-dev \
    libreadline-gplv2-dev libncursesw5-dev libssl-dev libsqlite3-dev tk-dev libgdbm-dev libc6-dev libbz2-dev \
    libmariadb-dev-compat libmariadb-dev

# download, unzip and build python 3.7.3
RUN mkdir -p /opt/python
WORKDIR /opt/python
RUN wget https://www.python.org/ftp/python/3.7.3/Python-3.7.3.tgz
RUN tar -xvf Python-3.7.3.tgz
WORKDIR /opt/python/Python-3.7.3
# the --enable-optimizations flag makes the build time much longer, but hopefully makes the python implementation faster
RUN ./configure \
    --prefix=/opt/python/python3.7
#    --enable-optimizations
RUN make
RUN make install

# install virtualenv
WORKDIR /opt/python
RUN /opt/python/python3.7/bin/python3.7 -m ensurepip
RUN /opt/python/python3.7/bin/pip3 install virtualenv

# activate virtualenv and install wheel package
WORKDIR /opt/python
RUN /opt/python/python3.7/bin/virtualenv venv
RUN source venv/bin/activate
RUN /opt/python/python3.7/bin/pip3 install wheel

# copy over requirements.txt, create wheels in directory
COPY ./requirements.txt ./requirements.txt
RUN mkdir wheels
RUN /opt/python/python3.7/bin/pip3 wheel --wheel-dir=./wheels -r requirements.txt
RUN tar -czf wheels.tar wheels
