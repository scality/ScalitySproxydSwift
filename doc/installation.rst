Installation
============
This package depends on `Scality Sproxyd client`_, a Python client library for Scality Sproxyd connector. It must
be installed before installing this package.

.. _Scality Sproxyd client: https://github.com/scality/scality-sproxyd-client

0. Download and uncompress the source code of this project. All the releases for this
   project can be found here_.
   For now the recommended version to use is the latest of the 0.3 branch.

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
