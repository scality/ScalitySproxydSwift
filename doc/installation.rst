Installation
============
This package depends on `Scality Sproxyd client`_, a Python client library for Scality Sproxyd connector. It must
be installed before installing this package.

.. _Scality Sproxyd client: https://github.com/scality/scality-sproxyd-client

1. Install this package:

   .. code-block:: console

       python setup.py install

2. Modify your Swift :file:`object-server.conf` to use the new object server:

   .. code-block:: ini

       [app:object-server]
       use = egg:swift_scality_backend#sproxyd_object

3. Set the Sproxy host(s) and Sproxy path to connect to in the
   ``[app:object-server]`` section in the same file. If your system supports it
   (anything running a Linux kernel newer than 2.6.17 does), also make sure to
   enable :c:func:`splice`, which enhances throughput and lowers CPU
   utilization:

   .. code-block:: ini

       [app:object-server]
       sproxyd_host = 172.24.4.3:81,172.24.4.4:81
       sproxyd_path = /proxy/chord
       splice = yes

4. Configure the webserver in front of Scality Sproxyd (usually Apache) to
   accept encoded slashes (`/`). Swift object name can contain one or several
   slashes that must be encoded before being sent to Sproxyd. To allow encoded
   slashes, edit your Apache configuration (or the Sproxyd virtual host) and
   add the following line:

    .. code-block:: apacheconf

       AllowEncodedSlashes NoDecode
