Scality object server backend for OpenStack Swift
==============================================

Installation
------------

1. Install the Sproxyd object server:
   ``sudo python setup.py install``

2. Modify your `object-server.conf` to use the new object server:
   ```
[app:object-server]
use = egg:swift_scality_backend#sproxyd_object
```

3. Set the Sproxy host and Sproxy path to connect to in the `[app:object-server]` section in the same file:
   ```
[app:object-server]
sproxyd_host = 172.24.4.3:81,172.24.4.4:81
sproxyd_path = /proxy/chord
```
