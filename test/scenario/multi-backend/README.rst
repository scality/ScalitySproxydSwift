=======================
Swift Multi-backend Lab
=======================

This is a scenario which leverage fabfiles to bootstrap a SAIO (Swift All In One) cluster, as well as a Scality RING. Further, it allows automated setup of a storage policy that targets the Scality RING, as well validation of configured storage policies.

The SAIO installation and the RING are intended to be installed on two different machines on the same network.

Requirements
------------
Two machines (or VMs) with Ubuntu 14 installed. Other Debian distributions may work, but has not been tested. One machine will host the SAIO installation, and the other will host a single node Scality RING. These machines will be referred to as ``SAIO_HOST``, and ``RING_HOST`` in the examples below. Make sure that there are no firewall rules blocking traffic between these machines.

Getting started
---------------
Install dependencies (``python-swiftclient`` and ``fabric``).

.. code-block:: console

    $ pip install -r requirements.txt

Bootstrap Scality RING. Pass the machine for installation with the ``-H`` parameter to ``fab``.

.. code-block:: console

    $ export SCAL_PASS="username:password"  # For getting scality packages.
    $ fab bootstrap.ring -H <RING_HOST>

Bootstrap SAIO by passing the target host for installation, and the user to run swift as. The user should not exist, and will be created.

.. code-block:: console

    $ fab bootstrap.swift:<USER> -H <SAIO_HOST>

The steps above can be done in parallel. Once the SAIO installation is finished, it is ready for use. To try it out, you can authenticate as user ``test:tester`` with password ``testing``. For details about the Swift installation you may issue:

.. code-block:: console

    $ swift -A http://<SAIO_HOST>:8080/auth/v1.0 -U test:tester -K testing info

Validating install
------------------
A basic test of creating a container, filling it with files from a local directory, and then validating the integrity of the uploaded files by re-downloading them, is possible through the fab validation tasks. A manifest over integrity checked files is created during this process, and will be written to the directory containing the source files. Make sure you have write access to this directory, or specify a path to where this file should be written by passing ``manifest_path`` as an argument, eg ``manifest_path=~/manifest``.

.. code-block:: console

    $ fab validate.container_integrity:<SAIO_HOST>,<CONTAINER>,<LOCAL_DIRECTORY>,\
        manifest_path=<WRITABLE_MANIFEST_PATH>

For creation, and validation of a container backed by a certain storage policy, append the name of the storage policy to the command:

.. code-block:: console

    $ fab validate.container_integrity:<SAIO_HOST>,<CONTAINER>,<LOCAL_DIRECTORY>,\
        <STORAGE_POLICY>

Installing The Scality Storage Policy
-------------------------------------
To put a storage policy in place which uses the Scality RING as backend, issue the ``bootstrap.scality_storage_policy`` fab task with the user running swift, and the endpoint of ``sproxyd``, eg ``http://sproxyd.local:81/proxy/bpchord``. Note that the ``sproxyd`` endpoint targeted should be configured "by path".

.. code-block:: console

    $ fab bootstrap.scality_storage_policy:<USER>,<SPROXYD_ENDPOINT> -H <SAIO_HOST>

The storage policy installed will be called ``scality``, and can be seen by issuing the ``swift info`` command and inspecting the ``policies`` property. To validate that the new policy is working properly, you can issue the ``validation.container_integrity`` fab mentioned above.

.. code-block:: console

    $ fab validate.container_integrity:<SAIO_HOST>,<CONTAINER>,<LOCAL_DIRECTORY>,\
        scality
