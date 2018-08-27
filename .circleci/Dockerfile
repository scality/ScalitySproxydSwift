FROM ubuntu:xenial

USER root

RUN apt-get update && apt-get install --yes \
    git \
    gcc \
    build-essential \
    autoconf \
    automake \
    libtool \
    zlib1g-dev \
    python2.7 \
    python-dev \
    python-pip \
    python-tox

RUN pip install coverage

RUN git clone https://github.com/openstack/liberasurecode.git && \
    cd liberasurecode && \
    ./autogen.sh && \
    ./configure && \
    make && \
    make install

RUN cd ~

RUN git clone https://github.com/openstack/pyeclib.git && \
    cd pyeclib && \
    pip install -U bindep -r test-requirements.txt && \
    python setup.py install && \
    echo '/usr/local/lib' >> /etc/ld.so.conf && \
    ldconfig
