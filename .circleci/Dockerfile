FROM ubuntu:xenial

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
    python-tox \
    locales \
    liberasurecode-dev \
  && pip install coverage \
  && git clone https://github.com/openstack/liberasurecode.git \
  && cd liberasurecode \
  && ./autogen.sh \
  && ./configure \
  && make \
  && make install \
  && cd ~ \
  && git clone https://github.com/openstack/pyeclib.git \
  && cd pyeclib \
  && pip install -U bindep -r test-requirements.txt \
  && python setup.py install \
  && echo '/usr/local/lib' >> /etc/ld.so.conf \
  && ldconfig \
  && sed -i -e 's/# en_US.UTF-8 UTF-8/en_US.UTF-8 UTF-8/' /etc/locale.gen \
  && locale-gen

ENV LANG en_US.UTF-8
ENV LANGUAGE en_US:en
ENV LC_ALL en_US.UTF-8
