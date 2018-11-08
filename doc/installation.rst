Installation
============
This package depends on `Scality Sproxyd client`_, a Python client library for
Scality Sproxyd connector. It must be installed before installing this package.

.. _Scality Sproxyd client: https://github.com/scality/scality-sproxyd-client

0. Download and uncompress the source code of this project. All the releases for
   this project can be found here_.
   For now the recommended version to use is the latest of the 0.4 branch.

.. _here: https://github.com/scality/ScalitySproxydSwift/releases

1. Install this package:

   .. code-block:: console

       python setup.py install

2. Modify your Swift :file:`object-server.conf` to use the new object server:

   .. code-block:: ini

       [app:object-server]
       use = egg:swift_scality_backend#sproxyd_object

3. Set the Sproxy endpoint(s) to connect to in the
   ``[app:object-server]`` section in the same file. If your system supports it
   (anything running a Linux kernel newer than 2.6.17 does), also make sure to
   enable :c:func:`splice`, which enhances throughput and lowers CPU
   utilization:

   .. code-block:: ini

       [app:object-server]
       sproxyd_endpoints = http://172.24.4.3:81/proxy/bparc,http://172.24.4.4:81/proxy/bparc
       splice = yes

   If the IPs targetted by 'sproxyd_endpoints' require HTTP Basic Authentication,
   also set the following variables in the :file:`object-server.conf` file:

   .. code-block:: ini

       [app:object-server]
       sproxyd_endpoints = http://172.24.4.3:81/proxy/bparc,http://172.24.4.4:81/proxy/bparc
       splice = yes

       sproxyd_url_username = testusern@me
       sproxyd_url_password = test42pa@s?:sword

   Moreover, if the sproxyd endpoints use a self-signed certificate for HTTPS
   authentication, it is possible to suppress the warnings related to certificate
   verification by adding the path to the CA certificate bundle in :file:`object-server.conf`:

   .. code-block:: ini

       [app:object-server]
       sproxyd_endpoints = https://172.24.4.3:81/proxy/bparc

       sproxyd_url_cert_bundle = /home/scality/ca.crt

   Also, if the sproxyd endpoints require certificate authentication from clients,
   the client certificate and key should also be specified in this file:

   .. code-block:: ini

       [app:object-server]
       sproxyd_endpoints = https://172.24.4.3:81/proxy/bparc

       sproxyd_url_cert_bundle = /home/scality/ca.crt

       sproxyd_url_client_cert = /home/scality/client.crt
       sproxyd_url_client_key = /home/scality/client.key

**Warning: splicing cannot be used with SSL/TLS encryption, as it would read
an encrypted data stream instead of the expected raw data**

4. (optional, only on a multi-node Swift installation) The target architecture
   looks like:

   .. image:: _static/target-architecture.png

   Scality RING ensures data safety through replication or ARC (Scality’s EC
   mechanism), and thus OpenStack Swift should no longer manage this key aspect.
   As such, an OpenStack Swift installation that leverages a Scality RING
   back-end should be configured to store only a single replica of any object.
   To do that you would typically use the `swift-ring-builder` command to create
   or recreate a Swift object ring with a **number of replicas set to 1**.

   .. warning:: Be careful with `swift-ring-builder` as it can potentially make
     existing data unreachable. Always make a copy of `object.ring.gz` and
     `object.builder` files when in doubt.

   .. note:: In the diagram, note how any `proxy-server` process only talks to
     the `object-server` process that sits on the same server. This is an
     optimization that avoids one extra network hop. Be sure to take that into
     concideration when adding server while constructing the Swift object ring.
