FROM registry.access.redhat.com/rhosp13/openstack-swift-object
MAINTAINER Scality
LABEL name="scality/swift-scality-backend" vendor="Scality" version="0.4.4" release="1"

ADD https://github.com/scality/ScalitySproxydSwift/archive/swift-scality-backend-0.4.4.tar.gz /tmp/

USER root

RUN cd /tmp \
  && tar xzf swift-scality-backend-0.4.4.tar.gz \
  && cd ScalitySproxydSwift-swift-scality-backend-0.4.4 \
  && python setup.py install

USER stack

