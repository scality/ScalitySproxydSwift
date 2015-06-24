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


import uuid

import pytest
import swiftclient

import os_utils


def test_put_when_sproxyd_down(setup1):
    prefix = 'test_put_when_sproxyd_down_%s' % uuid.uuid4()
    container_name = '%s-container' % prefix
    obj_name = '%s-object' % prefix
    obj_content = '%s-object-content' % prefix

    swift_connection = setup1['swift_connection']
    headers = {'x-storage-policy': 'Policy-2'}

    with os_utils.create_container(swift_connection, container_name, headers):

        setup1['sproxyd_registry'].stop_process('sproxyd-2')

        with pytest.raises(swiftclient.ClientException) as excinfo:
            swift_connection.put_object(container_name, obj_name, obj_content)

        assert excinfo.value.http_status == 503


def test_get_when_sproxyd_down(setup1):
    prefix = 'test_get_when_sproxyd_down_%s' % uuid.uuid4()
    container_name = '%s-container' % prefix
    obj_name = '%s-object' % prefix
    obj_content = '%s-object-content' % prefix

    swift_connection = setup1['swift_connection']
    headers = {'x-storage-policy': 'Policy-2'}
    swift_connection.put_container(container_name, headers=headers)
    swift_connection.put_object(container_name, obj_name, obj_content)

    setup1['sproxyd_registry'].stop_process('sproxyd-2')

    with pytest.raises(swiftclient.ClientException) as excinfo:
        swift_connection.get_object(container_name, obj_name)

    assert excinfo.value.http_status == 503
