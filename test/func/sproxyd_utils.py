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


from __future__ import with_statement
import os
import platform
import stat
import subprocess32
import tempfile
import urllib


ring_driver_conf_tmpl = """"ring_driver:0": {
        "by_path_service_id": "%(by_path_service_id)s",
        "by_path_cos": 0,
        "alias": "chord_path",
        "bstraplist": "%(bootstraplist)s",
        "by_path_enabled": true,
        "deferred_deletes_enabled_by_policy": false,
        "deferred_deletes_enabled_by_request": false,
        "deferred_writes_enabled_by_policy": false,
        "deferred_writes_enabled_by_request": false,
        "type": "chord"
     }"""

sproxyd_conf_tmpl = """{
    "general": {
        "ring": "MyRing",
        "conn_max": 10000,
        "conn_max_reuse": 100000,
        "consistent_reads": true,
        "consistent_writes": true,
        "max_proc_fd": 40960,
        "port": %(fcgi_port)s,
        "split_chunk_size": 33554432,
        "split_control_by_request": false,
        "split_enabled": true,
        "split_gc_cos": 2,
        "split_memory_limit": 671088640,
        "split_n_get_workers": 20,
        "split_n_io_workers": 20,
        "split_n_put_workers": 20,
        "split_threshold": 67108864
    },
    %(ring_driver)s
}"""


apache_vhost_conf_tmpl = """Listen %(vhost_port)s
    <VirtualHost *:%(vhost_port)s>
    DocumentRoot /var/www/%(name)s
    AllowEncodedSlashes %(allow_encoded_slash)s
    LimitRequestFieldSize 32766
    LimitRequestLine 32766
    </VirtualHost>
    FastCgiExternalServer /var/www/%(name)s/proxy -host 127.0.0.1:%(fcgi_port)s -flush -idle-timeout 300
    """


class SproxydProcess(object):

    def __init__(self, name, conf_file):
        self.name = name
        self.conf_file = conf_file
        self._p = None

    def __getattr__(self, attr):
        return getattr(self._p, attr)

    def start(self):
        self._f = open('/opt/stack/logs/%s.out' % self.name, 'a')
        self._p = subprocess32.Popen(
            ['sudo', '/usr/local/bin/sproxyd',  '-wdl', '-c', '%s' %
             self.conf_file, '-n', self.name,
             # Sproxyd is really not verbose by default, these flags provide
             # some sort of access log
             '-TLERROR,LWARNING,LRINGLL,LRINGLL'],
            stdout=self._f, stderr=subprocess32.STDOUT)

    def stop(self):
        if self._p.returncode is None:
            subprocess32.check_call(['sudo', 'kill', '%s' % self._p.pid])
        self.clean()

    def clean(self):
        self._f.close()


class SproxydRegistryError(Exception):
    pass


class SproxydRegistry(dict):

    base_vhosts_port = 80
    base_fcgi_port = 10000
    base_service_id = 192
    sproxyd_filename_tmpl = "sproxyd-%(number)s.conf"
    apache_sproxyd_filename_tmpl = "apache-sproxyd-%(number)s.conf"

    def __init__(self, number):
        self.number = number
        self._configs = dict()
        self._processes = dict()
        self._index = [(i, 'sproxyd-%s' % i) for i in range(2, self.number+2)]

    @property
    def configs(self):
        return self._configs.values()

    @property
    def processes(self):
        return self._processes.values()

    def get_config(self, name):
        return self._configs[name]

    def get_process(self, name):
        return self._processes[name]

    def generate_confs(
            self, host, allow_encoded_slashes, bootstraplist):
        for number, name in self._index:
            conf = SproxydConfiguration(
                name=name, host=host,
                vhost_port=self.base_vhosts_port+number,
                fcgi_port=self.base_fcgi_port+number,
                by_path_service_id=hex(self.base_service_id + number),
                allow_encoded_slashes=allow_encoded_slashes,
                bootstraplist=bootstraplist,
                sproxyd_conf_filename=self.sproxyd_filename_tmpl
                % dict(number=number),
                apacheconf_filename=self.apache_sproxyd_filename_tmpl
                % dict(number=number))
            conf.write()

            self._configs[name] = conf

    def delete_confs(self):
        for conf in self.configs:
            conf.delete()

    def start_processes(self):
        for name, conf in self._configs.items():
            p = SproxydProcess(
                name, conf.sproxyd_conf_path)
            self._processes[name] = p
            p.start()

        for process in self.processes:
            self._check_process_running(process)

    def _check_process_running(self, process):
        returncode = process.poll()
        if returncode is not None:
            process.clean()
            raise SproxydRegistryError(
                "The process %s, pid: %s exited with returncode :s ",
                (process.name, process.pid, process.returncode))

    def check_processes_running(self):
        for process in self.processes:
            self._check_process_running(process)

    def stop_processes(self):
        for process in self.processes:
            process.stop()

    def stop_process(self, name):
        self.get_process(name).stop()

    def respawn_processses(self):
        """ If a process is done, clean it and spawn a new one.
        Does nothing on still running processes.
        """
        for process in self.processes:
            returncode = process.poll()
            if returncode is not None:
                process.clean()
                process.start()


class SproxydConfiguration(object):

    def __init__(
            self, name, host, vhost_port, fcgi_port, by_path_service_id,
            allow_encoded_slashes, bootstraplist, sproxyd_conf_filename,
            apacheconf_filename):

        self.name = name
        self.host = host
        self.vhost_port = vhost_port
        self.fcgi_port = fcgi_port
        self.by_path_service_id = by_path_service_id
        self.allow_encoded_slashes = allow_encoded_slashes
        self.bootstraplist = bootstraplist
        self.sproxyd_conf_filename = sproxyd_conf_filename
        self.apacheconf_filename = apacheconf_filename

    def _get_apache_conf_directory(self):
        platform_name = platform.dist()[0]
        assert platform_name in ('centos', 'Ubuntu'), \
            "Unsupported platform : %s" % platform_name
        if platform_name == 'centos':
            return '/etc/httpd/conf.d'
        elif platform_name == 'Ubuntu':
            return '/etc/apache2/sites-enabled/'

    @property
    def sproxyd_conf_path(self):
        return os.path.join('/etc', self.sproxyd_conf_filename)

    @property
    def apache_conf_path(self):
        return os.path.join(
            self._get_apache_conf_directory(), self.apacheconf_filename)

    @property
    def url(self):
        return "http://%s:%s/proxy/chord_path" % (self.host, self.vhost_port)

    def _write_sproxyd_conf_file(self):
        ring_driver_conf = ring_driver_conf_tmpl % dict(
            by_path_service_id=self.by_path_service_id,
            bootstraplist=self.bootstraplist)
        main_conf = sproxyd_conf_tmpl % dict(
            fcgi_port=self.fcgi_port, ring_driver=ring_driver_conf)
        write_with_sudo_rwrr(main_conf, self.sproxyd_conf_path)

    def _write_apache_vhost_conf_file(self):
        conf = apache_vhost_conf_tmpl % dict(
            vhost_port=self.vhost_port, fcgi_port=self.fcgi_port,
            allow_encoded_slash=self.allow_encoded_slashes,
            name=self.name)
        write_with_sudo_rwrr(conf, self.apache_conf_path)

    def write(self):
        self._write_sproxyd_conf_file()
        self._write_apache_vhost_conf_file()

    def delete(self):
        subprocess32.check_call(['sudo', 'rm', self.sproxyd_conf_path])
        subprocess32.check_call(['sudo', 'rm', self.apache_conf_path])


def write_with_sudo(content, dest, perm_flag):
    """ Write 'content' string to dest
    using sudo in a subporcess.
    """
    fd, path = tempfile.mkstemp()
    os.chmod(path, perm_flag)
    with os.fdopen(fd, 'w') as f:
        f.write(content)
    subprocess32.check_call(['sudo', 'cp', path, dest])
    os.remove(path)


def write_with_sudo_rwrr(content, dest):
    write_with_sudo(
        content, dest,
        stat.S_IRUSR | stat.S_IWUSR | stat.S_IRGRP | stat.S_IROTH)


def restart_apache():
    platform_name = platform.dist()[0]
    assert platform_name in ('centos', 'Ubuntu'), \
        "Unsupported platform : %s" % platform_name
    if platform_name == 'centos':
        apache_bin = '/etc/init.d/httpd'
    elif platform_name == 'Ubuntu':
        apache_bin = '/etc/init.d/apache2'
    subprocess32.check_call(['sudo', apache_bin, 'restart'])


# FIXME : should be imported from swift_scality_backend
def get_sproxyd_object_name(account, container, obj):
    return '/'.join(urllib.quote(part, '')
                    for part in (account, container, obj))


def get_object_content(client_response):
    return ''.join([elem for elem in client_response[1]])
