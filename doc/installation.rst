Installation
============
1. Install this package::

    sudo python setup.py install

2. Modify your Swift `object-server.conf` to use the new object server::

    [app:object-server]
    use = egg:swift_scality_backend#sproxyd_object

3. Set the Sproxy host(s) and Sproxy path to connect to in the
   `[app:object-server]` section in the same file. If your system supports it
   (anything running a Linux kernel newer than 2.6.17 does), also make sure to
   enable `splice`, which enhances throughput and lowers CPU utilization::

    [app:object-server]
    sproxyd_host = 172.24.4.3:81,172.24.4.4:81
    sproxyd_path = /proxy/chord
    splice = yes
