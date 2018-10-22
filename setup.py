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

import distutils.spawn
import setuptools
import subprocess
import sys

import swift_scality_backend


def get_version():
    def has_git():
        return distutils.spawn.find_executable('git') is not None

    def is_git_clone():
        cmd = ['git', 'rev-parse', '--show-toplevel']

        proc = subprocess.Popen(
            cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)

        return proc.wait() == 0

    if hasattr(subprocess, 'check_output'):
        check_output = subprocess.check_output
    else:
        def check_output(*args, **kwargs):
            proc = subprocess.Popen(stdout=subprocess.PIPE, *args, **kwargs)
            output, _ = proc.communicate()
            rc = proc.poll()
            if rc != 0:
                raise subprocess.CalledProcessError(
                    rc, kwargs.get('args', args[0]), output=output)
            return output

    def get_git_version():
        prefix = 'swift-scality-backend-'
        cmd = ['git', 'describe', '--tags', '--dirty', '--always',
               '--match', '%s*' % prefix]

        result = check_output(cmd).strip()
        if sys.version_info >= (3, 0):
            result = str(result, 'utf-8')
        assert result.startswith(prefix)

        return result[len(prefix):]

    if has_git() and is_git_clone():
        return get_git_version()
    else:
        return '999'


setuptools.setup(
    name='swift-scality-backend',
    version=get_version(),
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
    install_requires=swift_scality_backend.__requires__,
    entry_points={
        'paste.app_factory': [
            'sproxyd_object=swift_scality_backend.server:app_factory'],
        'console_scripts': [
            'swift-scality-backend=swift_scality_backend.cli:main'],
    },
)
