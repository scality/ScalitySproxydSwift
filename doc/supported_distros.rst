Supported Linux distributions
=============================
This Python package has been tested to install and run on Centos (6 and 7) and
Ubuntu (12.04 and 14.04). Other distributions should work too (as long as they
provide Python 2.6 or Python 2.7) but this has no been tested.

Since version 0.4.1, tests using Python 2.7 are no longer performed.

Our Continous Integration system
--------------------------------
This Python package is automatically tested in the Scality OpenStack CI. Unit
tests and functional tests are run periodically and on every code commit. The
automated tests are run on Centos 7, Ubuntu 12 and Ubuntu 14.

Though Centos 6 is supported, tests on Centos 6 are trigerred manually, "on demand".
