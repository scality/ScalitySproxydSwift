#!/usr/bin/env python
# Copyright (c) 2014 Scality
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
# implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import setuptools

setuptools.setup(
    name='swift-scality-backend',
    version='0.2.0',
    description='Scality Ring backend for OpenStack Swift',
    url='http://www.scality.com/',
    author='Scality Openstack Engineering Team',
    author_email='openstack-eng@scality.com',
    license='Apache License (2.0)',
    packages=['swift_scality_backend'],
    classifiers=[
        'License :: OSI Approved :: Apache Software License',
        'Operating System :: POSIX :: Linux',
        'Programming Language :: Python :: 2.7'],
    install_requires=['swift>=1.13.1', 'eventlet>=0.9.15'],
    entry_points={
        'paste.app_factory': [
            'sproxyd_object=swift_scality_backend.server:app_factory'],
    },
)
