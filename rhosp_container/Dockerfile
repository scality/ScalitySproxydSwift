FROM registry.access.redhat.com/rhosp13/openstack-swift-object
MAINTAINER Scality
LABEL name="scality/swift-scality-backend" \
      vendor="Scality" \
      version="0.4.4" \
      release="1" \
      summary="Swift object-server plugin for Scality RING" \
      description="This package implements a back-end for OpenStack Swift storing objects in a Scality RING installation."

COPY LICENSE /licenses/LICENSE.txt

ADD https://github.com/scality/ScalitySproxydSwift/archive/swift-scality-backend-0.4.4.tar.gz /tmp/

USER root

RUN cd /tmp \
  && tar xzf swift-scality-backend-0.4.4.tar.gz \
  && cd ScalitySproxydSwift-swift-scality-backend-0.4.4 \
  && python setup.py install

USER swift

