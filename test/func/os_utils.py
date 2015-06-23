import ConfigParser
import contextlib
import os
import shutil
import signal
import subprocess
import sys
import time

import keystoneclient.v2_0
import swiftclient
import swift_scality_backend.policy_configuration as policy_configuration


global_swift_config = '/etc/swift/swift.conf'
storage_policies_config = '/etc/swift/scality-storage-policies.ini'

object_server_config = '/etc/swift/object-server/1.conf'
proxy_server_config = '/etc/swift/proxy-server.conf'
container_server_config = '/etc/swift/container-server/1.conf'
account_server_config = '/etc/swift/account-server/1.conf'

object_server_bin = '/opt/stack/swift/bin/swift-object-server'
proxy_server_bin = '/opt/stack/swift/bin/swift-proxy-server'
container_server_bin = '/opt/stack/swift/bin/swift-container-server'
account_server_bin = '/opt/stack/swift/bin/swift-account-server'

object_server_name = 's-object'
proxy_server_name = 's-proxy'
container_server_name = 's-container'
account_server_name = 's-account'


class V2KeystoneClient(keystoneclient.v2_0.client.Client):

    def __init__(self, username, password, tenant_name, auth_url, version):
        assert version == '2.0', "This wrapper can only query keystone v2"
        super(V2KeystoneClient, self).__init__(
            username=username, password=password, tenant_name=tenant_name,
            auth_url=auth_url)

    def get_user(self, username):
        for user in self.users.list():
            if user.name == username:
                return user


def get_swift_connection(authurl, user, key, tenant_name, auth_version):
    storage_url, storage_token = swiftclient.Connection(
        authurl=authurl, user=user, key=key, tenant_name=tenant_name,
        auth_version=auth_version).get_auth()
    return swiftclient.Connection(
        preauthurl=storage_url, preauthtoken=storage_token,
        auth_version=auth_version, retries=2)


@contextlib.contextmanager
def create_container(swift_connection, container_name, headers, sleep=0):

    swift_connection.put_container(container_name, headers=headers)
    try:
        yield
        if sleep:
            time.sleep(sleep)
    finally:
        swift_connection.delete_container(container_name)


@contextlib.contextmanager
def create_object(
        swift_connection, container_name, object_name,
        object_content, sleep=0):

    swift_connection.put_object(container_name, object_name, object_content)
    try:
        yield
        if sleep:
            time.sleep(sleep)
    finally:
        swift_connection.delete_object(container_name, object_name)


class OSService(object):

    screen_cmd_tpml = ["screen -S stack -p %(service_name)s -X stuff ",
                       "'%(service_bin)s %(service_conf)s -v & ",
                       "echo $! >%(pid_file)s; fg ",
                       "|| echo \"%(service_name)s failed to start\" | ",
                       "tee -a ",
                       "\"/opt/stack/status/stack/%(service_name)s.failure\"",
                       "\n'"]

    def __init__(self, name, bean, conf_file):
        self.name = name
        self._bin = bean
        self.conf_file = conf_file
        self._pid_file = "/opt/stack/status/stack/%s.pid" % name

    def _create_screen_cmd(self):
        cmd = ''.join(self.screen_cmd_tpml)
        return cmd % dict(
            service_name=self.name, service_bin=self._bin,
            service_conf=self.conf_file, pid_file=self._pid_file)

    @property
    def pid(self):
        with open(self._pid_file, 'r') as f:
            return int(f.read())

    def start(self):
        subprocess.check_call(self._create_screen_cmd(), shell=True)

    def stop(self):
        os.kill(self.pid, signal.SIGTERM)

    def restart(self):
        self.stop()
        self.start()


class ObjectServerConfig(ConfigParser.SafeConfigParser):

    app_section = "app:object-server"
    location_option = 'scality_location_preferences'

    def reset_location_preferences(self):
        if self.has_option(self.app_section, self.location_option):
            self.remove_option(self.app_section, self.location_option)

    def set_location_preference(self, locations):
        self.set(self.app_section, self.location_option, ','.join(locations))

    def get_location_preference(self):
        self.set(self.app_section, self.location_option).split(',')


class GlobalSwiftConfig(ConfigParser.SafeConfigParser):

    def _storage_policy_section(self, index):
        return "storage-policy:%s" % index

    def set_storage_policy(self, index, name, default=False):
        section = self._storage_policy_section(index)
        if not self.has_section(section):
            self.add_section(section)
        self.set(section, 'name', name)
        if default:
            self.set(section, 'default', 'yes')
        else:
            if self.has_option(section, 'default'):
                self.remove_option(section, 'default')

    def remove_default(self):
        for index in range(10):
            section = self._storage_policy_section(index)
            if self.has_section(section) and \
               self.has_option(section, 'default'):
                self.remove_option(section, 'default')

    def set_default(self, index):
        self.remove_default()
        self.set(self._storage_policy_section(index), 'default', 'yes')

    def get_storage_policy(self, index):
        section = self._storage_policy_section(index)
        return (
            self.get(section, 'name'),
            self.get(section, 'default') == 'yes')

    def reset_storage_policies(self):
        for index in range(10):
            section = self._storage_policy_section(index)
            if self.has_section(section):
                self.remove_section(section)


class ConfigWrapperException(Exception):
    pass


class ConfigWrapper(object):

    def __init__(self, config_file, config_class):
        self.config_file = config_file
        self.config_class = config_class

    @property
    def config_file_backup(self):
        return self.config_file + '.bck'

    def read(self):
        with open(self.config_file, 'r') as f:
            if hasattr(self.config_class, 'read'):
                config_obj = self.config_class()
                config_obj.readfp(f)
            elif hasattr(self.config_class, 'from_stream'):
                config_obj = self.config_class.from_stream(f)
            else:
                raise ConfigWrapperException(
                    "Configuration class unkown : %s" % self.config_cass)
        return config_obj

    def write(self, config_obj):
        with open(self.config_file, 'w') as f:
            if hasattr(config_obj, 'write'):
                config_obj.write(f)
            elif hasattr(config_obj, 'to_stream'):
                config_obj.to_stream(f)
            else:
                raise ConfigWrapperException(
                    "Configuration object unkown : %s" % config_obj)

    def backup(self):
        if os.path.exists(self.config_file):
            shutil.copyfile(self.config_file, self.config_file_backup)

    def restore(self):
        if os.path.exists(self.config_file_backup):
            shutil.copyfile(self.config_file_backup, self.config_file)
            os.remove(self.config_file_backup)


class OSRegistry(object):

    service_names = ['object_server', 'proxy_server', 'container_server',
                     'account_server']

    config_names = service_names + ['storage_policies', 'global_swift']

    def __init__(self):

        self._services = dict()
        self._configs = dict()

        thismodule = sys.modules[__name__]

        for name in self.service_names:
            private_name = getattr(thismodule, '%s_name' % name)
            config = getattr(thismodule, '%s_config' % name)
            bean = getattr(thismodule, '%s_bin' % name)
            self._services[name] = OSService(
                name=private_name, bean=bean, conf_file=config)

        for name in self.config_names:
            config = getattr(thismodule, '%s_config' % name)
            if name in self.service_names:
                if name == 'object_server':
                    self._configs[name] = ConfigWrapper(
                        config, ObjectServerConfig)
                else:
                    self._configs[name] = ConfigWrapper(
                        config, ConfigParser.SafeConfigParser)
            else:
                if name == 'global_swift':
                    self._configs[name] = ConfigWrapper(
                        config, GlobalSwiftConfig)
                elif name == 'storage_policies':
                    self._configs[name] = ConfigWrapper(
                        config, policy_configuration.Configuration)

    @property
    def services(self):
        return self._services.values()

    def start_services(self):
        for service in self.services:
            service.start()

    def stop_services(self):
        for service in self.services:
            service.stop()

    def restart_services(self):
        for service in self.services:
            service.restart()

    def restart_service(self, name):
        self.get_service(name).restart()

    def get_service(self, name):
        return self._services[name]

    def get_config(self, name):
        return self._configs[name]
