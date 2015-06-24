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


import pytest
import swift_scality_backend.policy_configuration as policy_configuration

import os_utils
import sproxyd_utils


def pytest_addoption(parser):
    parser.addoption("--host-ip", action="store")
    parser.addoption("--allow-encoded-slashes", action="store")
    parser.addoption("--os-auth-url", action="store")
    parser.addoption("--os-identity-api-version", action="store")
    parser.addoption("--os-demo-username", action="store")
    parser.addoption("--os-demo-password", action="store")
    parser.addoption("--os-demo-tenantname", action="store")
    parser.addoption("--os-admin-username", action="store")
    parser.addoption("--os-admin-password", action="store")
    parser.addoption("--os-admin-tenantname", action="store")
    parser.addoption("--sproxyd-numbers", action="store")


def get_demo_swift_connection(request):
    # FIXME : read user, password, etc... from somewhere ?
    return os_utils.get_swift_connection(
        authurl=request.config.getoption("--os-auth-url"),
        user=request.config.getoption("--os-demo-username"),
        key=request.config.getoption("--os-demo-password"),
        tenant_name=request.config.getoption("--os-demo-tenantname"),
        auth_version=request.config.getoption("--os-identity-api-version"))


def get_admin_keystone_client(request):
    # FIXME : read user, password, etc... from somewhere ?
    return os_utils.V2KeystoneClient(
        username=request.config.getoption("--os-admin-username"),
        password=request.config.getoption("--os-admin-password"),
        tenant_name=request.config.getoption("--os-admin-tenantname"),
        auth_url=request.config.getoption("--os-auth-url"),
        version=request.config.getoption("--os-identity-api-version"))


@pytest.yield_fixture(scope="session")
def configurations(request):
    # FIXME the port 4244 shouldbe configurable
    bootstraplist = "%s:%s" % (request.config.getoption("--host-ip"), 4244)
    allow_encoded_slashes = request.config.getoption("--allow-encoded-slashes")

    sproxyd_registry = sproxyd_utils.SproxydRegistry(
        int(request.config.getoption("--sproxyd-numbers")))
    sproxyd_registry.generate_confs(
        host='127.0.0.1',
        allow_encoded_slashes=allow_encoded_slashes,
        bootstraplist=bootstraplist)
    sproxyd_registry.start_processes()
    sproxyd_utils.restart_apache()

    tenant_id = get_admin_keystone_client(request).tenant_id

    os_registry = os_utils.OSRegistry()

    global_swift_config = os_registry.get_config('global_swift')
    global_swift_config.backup()
    global_swift_config_obj = global_swift_config.read()
    global_swift_config_obj.remove_default()
    global_swift_config_obj.set_storage_policy(2, 'Policy-2', default=False)
    global_swift_config_obj.set_storage_policy(3, 'Policy-3', default=False)
    global_swift_config.write(global_swift_config_obj)

    object_server_config = os_registry.get_config('object_server')
    object_server_config.backup()

    storage_policies_config = os_registry.get_config('storage_policies')
    storage_policies_config.backup()

    yield dict(
        sproxyd_registry=sproxyd_registry,
        os_registry=os_registry, tenant_id=tenant_id)

    global_swift_config.restore()
    object_server_config.restore()
    storage_policies_config.restore()
    os_registry.restart_services()

    sproxyd_registry.stop_processes()
    sproxyd_registry.delete_confs()
    sproxyd_utils.restart_apache()


@pytest.fixture()
def setup1(configurations, request):
    """Two different policies Policy-2 and Policy-3
    Two rings ringA (sproxyd-2) and ringB (sproxyd-3)
    ringA is the r+w ring for Policy-2
    ringB is the r+w ring for Policy-3
    Policy-2 is the default policy
    No location preferences
    """

    sproxyd_registry = configurations['sproxyd_registry']
    sproxyd_registry.respawn_processses()

    os_registry = configurations['os_registry']

    global_swift_config = os_registry.get_config('global_swift')
    global_swift_config_obj = global_swift_config.read()
    global_swift_config_obj.set_default(2)
    global_swift_config.write(global_swift_config_obj)

    object_server_config = os_registry.get_config('object_server')
    object_server_config_obj = object_server_config.read()
    object_server_config_obj.reset_location_preferences()
    object_server_config.write(object_server_config_obj)

    storage_policies_config = os_registry.get_config('storage_policies')
    ring_a = policy_configuration.Ring(
        'ringA', location='paris', endpoints=[sproxyd_registry.get_config(
            'sproxyd-2').url])
    ring_b = policy_configuration.Ring(
        'ringB', location='paris', endpoints=[sproxyd_registry.get_config(
            'sproxyd-3').url])
    policy_2 = policy_configuration.StoragePolicy(
        2, read_set=[ring_a], write_set=[ring_a])
    policy_3 = policy_configuration.StoragePolicy(
        3, read_set=[ring_b], write_set=[ring_b])
    sp_config_obj = policy_configuration.Configuration([policy_2, policy_3])
    storage_policies_config.write(sp_config_obj)

    os_registry.restart_services()

    sproxyd_registry.check_processes_running()

    swift_connection = get_demo_swift_connection(request)

    return dict(
        sp_config=sp_config_obj, sproxyd_registry=sproxyd_registry,
        swift_connection=swift_connection,
        tenant_id=configurations['tenant_id'])


@pytest.fixture()
def setup2(configurations, request):
    """One policy Policy-2
    Two rings ringA (sproxyd-2) and ringB (sproxyd-3)
    ringA and ringB are the are read rings for Policy-2
    ringA is the w ring for Policy-2
    ringA  is located in Paris
    ringB  is located in SF
    Policy-2 is the default policy
    Paris is the preferred location of the object server
    """

    sproxyd_registry = configurations['sproxyd_registry']
    sproxyd_registry.respawn_processses()

    os_registry = configurations['os_registry']

    global_swift_config = os_registry.get_config('global_swift')
    global_swift_config_obj = global_swift_config.read()
    global_swift_config_obj.set_default(2)
    global_swift_config.write(global_swift_config_obj)

    object_server_config = os_registry.get_config('object_server')
    object_server_config_obj = object_server_config.read()
    object_server_config_obj.set_location_preference(['Paris'])
    object_server_config.write(object_server_config_obj)

    storage_policies_config = os_registry.get_config('storage_policies')
    ring_a = policy_configuration.Ring(
        'ringA', location='Paris', endpoints=[sproxyd_registry.get_config(
            'sproxyd-2').url])
    ring_b = policy_configuration.Ring(
        'ringB', location='SF', endpoints=[sproxyd_registry.get_config(
            'sproxyd-3').url])
    policy_2 = policy_configuration.StoragePolicy(
        2, read_set=[ring_a, ring_b], write_set=[ring_a])
    sp_config_obj = policy_configuration.Configuration([policy_2])
    storage_policies_config.write(sp_config_obj)

    os_registry.restart_service('object_server')

    sproxyd_registry.check_processes_running()

    swift_connection = get_demo_swift_connection(request)

    return dict(
        sp_config=sp_config_obj, sproxyd_registry=sproxyd_registry,
        swift_connection=swift_connection,
        tenant_id=configurations['tenant_id'])


@pytest.fixture()
def setup3(configurations, request):
    """One policy Policy-2.
    Two rings ringA (sproxyd-2) and ringB (sproxyd-3)
    ringA is the read ring of Policy-2
    ringB is the write ring of Policy-2
    Policy-2 is the default policy
    No location preferences
    """

    sproxyd_registry = configurations['sproxyd_registry']
    sproxyd_registry.respawn_processses()

    os_registry = configurations['os_registry']

    global_swift_config = os_registry.get_config('global_swift')
    global_swift_config_obj = global_swift_config.read()
    global_swift_config_obj.set_default(2)
    global_swift_config.write(global_swift_config_obj)

    object_server_config = os_registry.get_config('object_server')
    object_server_config_obj = object_server_config.read()
    object_server_config_obj.reset_location_preferences()
    object_server_config.write(object_server_config_obj)

    storage_policies_config = os_registry.get_config('storage_policies')
    storage_policies_config.backup()
    ring_a = policy_configuration.Ring(
        'ringA', location='Paris', endpoints=[sproxyd_registry.get_config(
            'sproxyd-2').url])
    ring_b = policy_configuration.Ring(
        'ringB', location='Paris', endpoints=[sproxyd_registry.get_config(
            'sproxyd-3').url])
    policy_2 = policy_configuration.StoragePolicy(
        2, read_set=[ring_a], write_set=[ring_b])
    sp_config_obj = policy_configuration.Configuration([policy_2])
    storage_policies_config.write(sp_config_obj)

    os_registry.restart_service('object_server')

    sproxyd_registry.check_processes_running()

    swift_connection = get_demo_swift_connection(request)

    return dict(
        sp_config=sp_config_obj, sproxyd_registry=sproxyd_registry,
        swift_connection=swift_connection,
        tenant_id=configurations['tenant_id'])
