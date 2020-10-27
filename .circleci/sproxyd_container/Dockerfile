ARG SCAL_USERNAME
ARG SCAL_PASSWORD
ARG SCAL_URL
FROM ubuntu:trusty
ARG SCAL_USERNAME
ARG SCAL_PASSWORD
ARG SCAL_URL

EXPOSE 81

RUN apt-get update && apt-get install -y curl

RUN curl -o - "https://${SCAL_USERNAME}:${SCAL_PASSWORD}@${SCAL_URL}/centos/7/x86_64/scality.pub" | apt-key add -

RUN echo "deb [ arch=amd64 ] https://${SCAL_USERNAME}:${SCAL_PASSWORD}@${SCAL_URL}/ubuntu trusty scality/ring scality/thirdparty" > /etc/apt/sources.list.d/scality.list \
  && apt-get update \
  && apt-get install -y python3-requests scality-sproxyd scality-sproxyd-apache2

COPY sproxyd.conf /etc
COPY scality-sd.conf /etc/apache2/sites-enabled/

CMD service apache2 restart && sproxyd -c /etc/sproxyd.conf -dwl
