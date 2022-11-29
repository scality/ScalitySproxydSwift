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


import contextlib
import time
import uuid

import pytest
import scality_sproxyd_client.sproxyd_client

import os_utils
import sproxyd_utils


@pytest.mark.parametrize(
    "inputdict",
    [{'expected_sp': 'Policy-2',
      'sproxyd_name': 'sproxyd-2'},
     {'input_sp': 'Policy-3', 'expected_sp': 'Policy-3',
      'sproxyd_name': 'sproxyd-3'}],
    ids=["default-policy", "specific-policy"])
def test_create(setup1, inputdict):
    policy = 'default'
    if 'input_sp' in inputdict:
        policy = inputdict['input_sp']

    prefix = 'test_create_%s_%s' % (policy, uuid.uuid4())
    container_name = '%s-container' % prefix
    obj_name = '%s-object' % prefix
    obj_content = '%s-object-content' % prefix

    swift_connection = setup1['swift_connection']
    headers = dict()
    if policy != 'default':
        headers['x-storage-policy'] = policy

    with contextlib.nested(
            os_utils.create_container(
                swift_connection, container_name, headers),
            os_utils.create_object(
                swift_connection, container_name, obj_name, obj_content)):

        assert inputdict['expected_sp'] == \
            swift_connection.head_container(container_name)['x-storage-policy']

        swift_connection.put_object(container_name, obj_name, obj_content)
        swift_obj = swift_connection.get_object(container_name, obj_name)

        assert obj_content == swift_obj[1]

        sproxyd_url = setup1['sproxyd_registry'].get_config(
            inputdict['sproxyd_name']).url
        sd_client = scality_sproxyd_client.sproxyd_client.SproxydClient(
            [sproxyd_url])
        # FIXME : we might get dynamically this 'AUTH_' prefix from somewhere ?
        sd_obj_name = sproxyd_utils.get_sproxyd_object_name(
            "AUTH_%s" % setup1['tenant_id'], container_name, obj_name)
        sd_response = sd_client.get_object(sd_obj_name)
        sd_obj_content = sproxyd_utils.get_object_content(sd_response)

        assert obj_content == sd_obj_content


def test_read_fallback_to_write_ring_if_read_ring_respond_404(setup3):

    prefix = 'read_fallback_to_write_if_404_%s' % uuid.uuid4()
    container_name = '%s-container' % prefix
    obj_name = '%s-object' % prefix
    obj_content = '%s-object-content' % prefix

    swift_connection = setup3['swift_connection']
    headers = {'x-storage-policy': 'Policy-2'}

    with contextlib.nested(
            os_utils.create_container(
                swift_connection, container_name, headers),
            os_utils.create_object(
                swift_connection, container_name, obj_name, obj_content)):

        swift_obj = swift_connection.get_object(container_name, obj_name)
        assert obj_content == swift_obj[1]


def test_read_fallback_to_write_ring_when_read_ring_is_down(setup3):

    prefix = 'read_fallback_to_write_if_read_ring_down_%s' % uuid.uuid4()
    container_name = '%s-container' % prefix
    obj_name = '%s-object' % prefix
    obj_content = '%s-object-content' % prefix

    swift_connection = setup3['swift_connection']
    headers = {'x-storage-policy': 'Policy-2'}

    with contextlib.nested(
            os_utils.create_container(
                swift_connection, container_name, headers),
            os_utils.create_object(
                swift_connection, container_name, obj_name, obj_content)):

        setup3['sproxyd_registry'].stop_process('sproxyd-2')
        time.sleep(20)

        swift_obj = swift_connection.get_object(container_name, obj_name)
        assert obj_content == swift_obj[1]
