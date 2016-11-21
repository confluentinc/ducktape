# Copyright 2014 Confluent Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from contextlib import contextmanager
import logging

from ducktape.utils.http_utils import HttpMixin
from ducktape.utils.util import wait_until
from ducktape.errors import DucktapeError


class RemoteAccountError(DucktapeError):
    """This exception is raised when an attempted action on a remote node fails.
    """
    def __init__(self, account, msg):
        self.account_str = str(account)
        self.msg = msg

    def __str__(self):
        return "%s: %s" % (self.account_str, self.msg)


class RemoteCommandError(RemoteAccountError):
    """This exception is raised when a process run by ssh*() returns a non-zero exit status.
    """
    def __init__(self, account, cmd, exit_status, msg):
        self.account_str = str(account)
        self.exit_status = exit_status
        self.cmd = cmd
        self.msg = msg

    def __str__(self):
        msg = "%s: Command '%s' returned non-zero exit status %d." % (self.account_str, self.cmd, self.exit_status)
        if self.msg:
            msg += " Remote error message: %s" % self.msg
        return msg


class RemoteAccount(HttpMixin):
    """RemoteAccount is the heart of interaction with cluster nodes,
    and every allocated cluster node has a reference to an instance of RemoteAccount.

    It wraps metadata such as ssh configs, and provides methods for file system manipulation and shell commands.

    Each operating system has its own RemoteAccount implementation.
    """

    linux = "linux"
    windows = "windows"

    def __init__(self, ssh_config, externally_routable_ip=None, logger=None):
        # Instance of RemoteAccountSSHConfig - use this instead of a dict, because we need the entire object to
        # be hashable
        self.ssh_config = ssh_config

        # We don't want to rely on the hostname (e.g. 'worker1') having been added to the driver host's /etc/hosts file.
        # But that means we need to distinguish between the hostname and the value of hostname we use for SSH commands.
        # We try to satisfy all use cases and keep things simple by
        #   a) storing the hostname the user probably expects (the "Host" value in .ssh/config)
        #   b) saving the real value we use for running the SSH command
        self.hostname = ssh_config.host
        self.ssh_hostname = ssh_config.hostname

        self.user = ssh_config.user
        self.externally_routable_ip = externally_routable_ip
        self._logger = logger

    @staticmethod
    def make_remote_account(ssh_config, externally_routable_ip=None):
        """Factory function for creating the correct RemoteAccount implementation."""

        # import here to avoid a circular import.
        from ducktape.cluster.linux_remoteaccount import LinuxRemoteAccount
        from ducktape.cluster.windows_remoteaccount import WindowsRemoteAccount

        if ssh_config.host and "windows" in ssh_config.host:
            return WindowsRemoteAccount(ssh_config=ssh_config,
                                        externally_routable_ip=externally_routable_ip)
        else:
            return LinuxRemoteAccount(ssh_config=ssh_config,
                                      externally_routable_ip=externally_routable_ip)

    def has_operating_system(self, operating_system):
        # import here to avoid a circular import. TODO: Is there a better way to do this?
        from ducktape.cluster.linux_remoteaccount import LinuxRemoteAccount
        from ducktape.cluster.windows_remoteaccount import WindowsRemoteAccount

        return (operating_system == RemoteAccount.linux and isinstance(self, LinuxRemoteAccount) or
                (operating_system == RemoteAccount.windows and isinstance(self, WindowsRemoteAccount)))

    @property
    def logger(self):
        if self._logger:
            return self._logger
        else:
            return logging.getLogger(__name__)

    @logger.setter
    def logger(self, logger):
        self._logger = logger

    def _log(self, level, msg, *args, **kwargs):
        msg = "%s: %s" % (str(self), msg)
        self.logger.log(level, msg, *args, **kwargs)

    @property
    def ssh_client(self):
        raise NotImplementedError

    @property
    def sftp_client(self):
        raise NotImplementedError

    def close(self):
        raise NotImplementedError

    def __str__(self):
        r = ""
        if self.user:
            r += self.user + "@"
        r += self.hostname
        return r

    def __repr__(self):
        return str(self.__dict__)

    def __eq__(self, other):
        return other is not None and self.__dict__ == other.__dict__

    def __hash__(self):
        return hash(tuple(sorted(self.__dict__.items())))

    @property
    def local(self):
        raise NotImplementedError

    def wait_for_http_service(self, port, headers, timeout=20, path='/'):
        """Wait until this service node is available/awake."""
        url = "http://%s:%s%s" % (self.externally_routable_ip, str(port), path)

        err_msg = "Timed out trying to contact service on %s. " % url + \
                            "Either the service failed to start, or there is a problem with the url."
        wait_until(lambda: self._can_ping_url(url, headers), timeout_sec=timeout, backoff_sec=.25, err_msg=err_msg)

    def _can_ping_url(self, url, headers):
        """See if we can successfully issue a GET request to the given url."""
        try:
            self.http_request(url, "GET", "", headers, timeout=.75)
            return True
        except:
            return False

    def ssh(self, cmd, allow_fail=False):
        raise NotImplementedError

    def ssh_capture(self, cmd, allow_fail=False, callback=None, combine_stderr=True, timeout_sec=None):
        raise NotImplementedError

    def ssh_output(self, cmd, allow_fail=False, combine_stderr=True, timeout_sec=None):
        raise NotImplementedError

    def alive(self, pid):
        raise NotImplementedError

    def signal(self, pid, sig, allow_fail=False):
        raise NotImplementedError

    def kill_process(self, process_grep_str, clean_shutdown=True, allow_fail=False):
        raise NotImplementedError

    def copy_between(self, src, dest, dest_node):
        raise NotImplementedError

    def scp_from(self, src, dest, recursive=False):
        raise NotImplementedError

    def copy_from(self, src, dest):
        raise NotImplementedError

    def scp_to(self, src, dest, recursive=False):
        raise NotImplementedError

    def copy_to(self, src, dest):
        raise NotImplementedError

    def islink(self, path):
        raise NotImplementedError

    def isdir(self, path):
        raise NotImplementedError

    def exists(self, path):
        raise NotImplementedError

    def isfile(self, path):
        raise NotImplementedError

    def open(self, path, mode='r'):
        raise NotImplementedError

    def create_file(self, path, contents):
        raise NotImplementedError

    def mkdir(self, path, mode=0755):
        raise NotImplementedError

    def mkdirs(self, path, mode=0755):
        raise NotImplementedError

    def remove(self, path, allow_fail=False):
        raise NotImplementedError

    def fetch_externally_routable_ip(self, is_aws):
        raise NotImplementedError

    @contextmanager
    def monitor_log(self, log):
        raise NotImplementedError
